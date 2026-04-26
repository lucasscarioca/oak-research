from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import uuid

import asyncpg
from httpx import ASGITransport, AsyncClient

from oakresearch import db as db_module
from oakresearch.main import app
from oakresearch.db import apply_migrations, bootstrap_instance


class Phase5And6HttpTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.database_url = os.environ.get(
            "TEST_DATABASE_URL",
            "postgresql://oakresearch:oakresearch@db:5432/oakresearch",
        )
        self.schema_name = f"test_{uuid.uuid4().hex}"
        self.pool = await asyncpg.create_pool(
            self.database_url,
            min_size=1,
            max_size=2,
            server_settings={"search_path": self.schema_name},
        )
        async with self.pool.acquire() as conn:
            await conn.execute(f'CREATE SCHEMA {self.schema_name}')
            await conn.execute(f'SET search_path TO {self.schema_name}')
            await apply_migrations(conn)
            await bootstrap_instance(conn)

        self.tempdir = tempfile.TemporaryDirectory()
        self.original_storage_dir = db_module.DEFAULT_STORAGE_DIR
        db_module.DEFAULT_STORAGE_DIR = Path(self.tempdir.name)
        app.state.pool = self.pool

    async def asyncTearDown(self) -> None:
        db_module.DEFAULT_STORAGE_DIR = self.original_storage_dir
        self.tempdir.cleanup()
        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            await conn.execute(f'DROP SCHEMA IF EXISTS {self.schema_name} CASCADE')
        await self.pool.close()

    async def create_authenticated_client(self) -> AsyncClient:
        client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
        response = await client.post(
            "/auth/onboarding",
            json={"username": "owner", "password": "secret", "confirm_password": "secret"},
        )
        self.assertEqual(response.status_code, 200)
        return client

    async def test_provider_config_http_flow(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)

        unauthenticated = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
        self.addAsyncCleanup(unauthenticated.aclose)
        response = await unauthenticated.get("/provider/config")
        self.assertEqual(response.status_code, 401)

        response = await client.get("/provider/config")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["validation_status"], "unknown")
        self.assertFalse(body["api_key_present"])
        self.assertNotIn("api_key_ciphertext", body)

        response = await client.post("/provider/config/test")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["validation_message"], "No API key configured")

        response = await client.put("/provider/config", json={"api_key": "   "})
        self.assertEqual(response.status_code, 400)

        with patch("oakresearch.main.validate_gemini_api_key", return_value=(False, "API key not valid. Please pass a valid API key.")):
            response = await client.put("/provider/config", json={"api_key": "invalid-key"})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["validation_status"], "invalid")
        self.assertTrue(body["api_key_present"])
        self.assertEqual(body["validation_message"], "API key not valid. Please pass a valid API key.")
        self.assertNotIn("api_key_ciphertext", body)
        self.assertNotIn("invalid-key", json.dumps(body))

        with patch("oakresearch.main.validate_gemini_api_key", return_value=(True, None)):
            response = await client.put("/provider/config", json={"api_key": "valid-key"})
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["validation_status"], "valid")
        self.assertTrue(body["api_key_present"])
        self.assertNotIn("api_key_ciphertext", body)
        self.assertIsNone(body.get("validation_message"))

        response = await client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["provider_configured"])

        with patch("oakresearch.main.validate_gemini_api_key", return_value=(True, None)):
            response = await client.post("/provider/config/test")
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["validation_status"], "valid")
        self.assertTrue(body["api_key_present"])
        self.assertNotIn("api_key_ciphertext", body)

    async def test_source_creation_http_flow(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)

        response = await client.post(
            "/sources",
            json={
                "source_type": "text",
                "title": "Text source",
                "content_text": "hello world",
                "metadata": {"origin": "manual"},
            },
        )
        self.assertEqual(response.status_code, 200)
        text_source = response.json()
        self.assertEqual(text_source["title"], "Text source")
        self.assertEqual(text_source["status"], "queued")
        self.assertEqual(text_source["job_status"], "queued")
        self.assertEqual(text_source["metadata"]["input_kind"], "text")
        self.assertEqual(text_source["metadata"]["origin"], "manual")
        self.assertEqual(Path(text_source["payload_uri"]).read_text(encoding="utf-8"), "hello world")

        response = await client.post(
            "/sources",
            json={
                "source_type": "pdf",
                "title": "Upload source",
                "content_base64": "cGRmIHBheWxvYWQ=",
                "original_name": "paper.pdf",
                "mime_type": "application/pdf",
                "metadata": {"origin": "upload"},
            },
        )
        self.assertEqual(response.status_code, 200)
        upload_source = response.json()
        self.assertEqual(upload_source["metadata"]["input_kind"], "upload")
        self.assertEqual(upload_source["metadata"]["original_name"], "paper.pdf")
        self.assertEqual(Path(upload_source["payload_uri"]).read_bytes(), b"pdf payload")

        response = await client.post(
            "/sources",
            json={
                "source_type": "url",
                "title": "URL source",
                "source_url": "https://example.com/article",
                "content_text": "Extracted text for the article",
                "metadata": {"origin": "manual"},
            },
        )
        self.assertEqual(response.status_code, 200)
        url_source = response.json()
        self.assertEqual(url_source["metadata"]["input_kind"], "url")
        self.assertTrue(url_source["metadata"]["has_fallback_text"])
        self.assertEqual(url_source["metadata"]["source_url"], "https://example.com/article")
        self.assertEqual(Path(url_source["payload_uri"]).read_text(encoding="utf-8"), "Extracted text for the article")

        response = await client.get("/sources")
        self.assertEqual(response.status_code, 200)
        sources = response.json()
        self.assertEqual(len(sources), 3)
        self.assertEqual(sources[0]["title"], "URL source")
        self.assertEqual(sources[0]["metadata"]["input_kind"], "url")
        self.assertEqual(sources[1]["metadata"]["input_kind"], "upload")

        response = await client.patch(f"/sources/{url_source['id']}", json={"title": "Renamed source"})
        self.assertEqual(response.status_code, 200)
        patched = response.json()
        self.assertEqual(patched["title"], "Renamed source")
        self.assertEqual(patched["metadata"]["input_kind"], "url")
        self.assertEqual(patched["status"], "queued")

        response = await client.patch("/sources/999999", json={"title": "Missing source"})
        self.assertEqual(response.status_code, 404)

    async def test_source_creation_rejects_missing_content_malformed_base64_and_missing_default_notebook(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)

        response = await client.post(
            "/sources",
            json={"source_type": "text", "title": "Missing content"},
        )
        self.assertEqual(response.status_code, 400)

        response = await client.post(
            "/sources",
            json={"source_type": "pdf", "title": "Bad upload", "content_base64": "not-base64"},
        )
        self.assertEqual(response.status_code, 400)

        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            notebook_id = await conn.fetchval("SELECT id FROM notebooks WHERE is_default = TRUE LIMIT 1")
            await conn.execute("UPDATE notebooks SET is_default = FALSE WHERE id = $1", notebook_id)

        response = await client.post(
            "/sources",
            json={"source_type": "text", "title": "No notebook", "content_text": "hello"},
        )
        self.assertEqual(response.status_code, 409)

    async def test_run_creation_blocks_without_provider_configuration(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)

        response = await client.post("/runs", json={"question": "Why is Gemini unavailable?"})
        self.assertEqual(response.status_code, 200)
        run = response.json()
        self.assertEqual(run["status"], "blocked")
        self.assertEqual(run["step_label"], "provider-not-ready")
        self.assertEqual(run["blocked_reason"], "Gemini provider configuration is not ready")

        response = await client.get("/runs")
        self.assertEqual(response.status_code, 200)
        runs = response.json()
        self.assertEqual(runs[0]["status"], "blocked")
        self.assertEqual(runs[0]["blocked_reason"], "Gemini provider configuration is not ready")

    async def test_run_creation_blocks_when_saved_valid_provider_key_is_unusable(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)

        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            await conn.execute(
                """
                UPDATE provider_configs
                SET validation_status = 'valid', api_key_ciphertext = 'not-valid-base64', validated_at = now()
                WHERE id = 1
                """
            )

        response = await client.post("/runs", json={"question": "Should this be blocked?"})
        self.assertEqual(response.status_code, 200)
        run = response.json()
        self.assertEqual(run["status"], "blocked")
        self.assertEqual(run["step_label"], "provider-not-ready")

    async def test_protected_routes_require_authentication(self) -> None:
        client = AsyncClient(transport=ASGITransport(app=app), base_url="http://test")
        self.addAsyncCleanup(client.aclose)

        response = await client.get("/provider/config")
        self.assertEqual(response.status_code, 401)
        response = await client.put("/provider/config", json={"api_key": "nope"})
        self.assertEqual(response.status_code, 401)
        response = await client.post("/provider/config/test")
        self.assertEqual(response.status_code, 401)
        response = await client.get("/sources")
        self.assertEqual(response.status_code, 401)
        response = await client.post("/sources", json={"title": "Nope"})
        self.assertEqual(response.status_code, 401)
        response = await client.patch("/sources/1", json={"title": "Nope"})
        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()
