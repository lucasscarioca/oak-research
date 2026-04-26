from __future__ import annotations

import base64
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import uuid

import asyncpg
from httpx import ASGITransport, AsyncClient

from oakresearch import db as db_module
from oakresearch.db import apply_migrations, bootstrap_instance
from oakresearch.ingestion import IngestionError, process_next_source_job_once, _validate_public_url
from oakresearch.main import app


class Phase7IngestionTest(unittest.IsolatedAsyncioTestCase):
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

    async def test_text_sources_are_processed_asynchronously(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)

        response = await client.post(
            "/sources",
            json={
                "source_type": "text",
                "title": "Text source",
                "content_text": "alpha paragraph\n\nbeta paragraph",
            },
        )
        self.assertEqual(response.status_code, 200)
        queued = response.json()
        self.assertEqual(queued["status"], "queued")
        self.assertEqual(queued["job_status"], "queued")

        processed = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(processed)
        self.assertEqual(processed["status"], "succeeded")

        response = await client.post(
            "/sources",
            json={
                "source_type": "text",
                "title": "Uploaded text source",
                "content_base64": base64.b64encode(b"uploaded paragraph\n\nmore uploaded text").decode("ascii"),
                "original_name": "notes.txt",
                "mime_type": "text/plain",
            },
        )
        self.assertEqual(response.status_code, 200)
        uploaded = response.json()
        self.assertEqual(uploaded["status"], "queued")
        self.assertEqual(uploaded["metadata"]["input_kind"], "upload")

        processed = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(processed)
        self.assertEqual(processed["status"], "succeeded")

        response = await client.get("/sources")
        self.assertEqual(response.status_code, 200)
        sources = response.json()
        source_by_id = {source["id"]: source for source in sources}
        self.assertEqual(source_by_id[queued["id"]]["status"], "succeeded")
        self.assertEqual(source_by_id[uploaded["id"]]["status"], "succeeded")
        self.assertEqual(source_by_id[uploaded["id"]]["job_status"], "succeeded")
        self.assertEqual(source_by_id[uploaded["id"]]["job_step_label"], "ingestion-complete")
        self.assertEqual(source_by_id[uploaded["id"]]["metadata"]["original_name"], "notes.txt")

        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            text_chunks = await conn.fetch(
                "SELECT chunk_index, chunk_text FROM source_chunks WHERE source_id = $1 ORDER BY chunk_index ASC",
                queued["id"],
            )
            upload_chunks = await conn.fetch(
                "SELECT chunk_index, chunk_text FROM source_chunks WHERE source_id = $1 ORDER BY chunk_index ASC",
                uploaded["id"],
            )
        self.assertGreaterEqual(len(text_chunks), 1)
        self.assertIn("alpha paragraph", text_chunks[0]["chunk_text"])
        self.assertGreaterEqual(len(upload_chunks), 1)
        self.assertIn("uploaded paragraph", upload_chunks[0]["chunk_text"])

    async def test_broken_url_fails_and_can_be_retried(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)

        response = await client.post(
            "/sources",
            json={
                "source_type": "url",
                "title": "Broken URL source",
                "source_url": "https://example.invalid/missing",
            },
        )
        self.assertEqual(response.status_code, 200)
        source = response.json()
        self.assertEqual(source["status"], "queued")

        with patch("oakresearch.ingestion._fetch_url_text", side_effect=IngestionError("Unable to fetch URL content from https://example.invalid/missing")):
            processed = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(processed)
        self.assertEqual(processed["status"], "failed")

        response = await client.get("/sources")
        self.assertEqual(response.status_code, 200)
        sources = response.json()
        self.assertEqual(sources[0]["status"], "failed")
        self.assertIn("Unable to fetch URL content", sources[0]["job_error_message"])

        response = await client.post(f"/sources/{sources[0]['id']}/retry")
        self.assertEqual(response.status_code, 200)
        retried = response.json()
        self.assertEqual(retried["status"], "queued")
        self.assertEqual(retried["job_status"], "queued")

        with patch("oakresearch.ingestion._fetch_url_text", return_value="Recovered article text"):
            processed = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(processed)
        self.assertEqual(processed["status"], "succeeded")

        response = await client.post(
            "/sources",
            json={
                "source_type": "url",
                "title": "Local host blocked",
                "source_url": "http://127.0.0.1:8000/health",
            },
        )
        self.assertEqual(response.status_code, 200)
        localhost_source = response.json()
        self.assertEqual(localhost_source["status"], "queued")

        processed = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(processed)
        self.assertEqual(processed["status"], "failed")
        self.assertIn("not public", processed["error"])

        response = await client.get("/sources")
        self.assertEqual(response.status_code, 200)
        sources = response.json()
        self.assertEqual(sources[0]["status"], "failed")
        self.assertIn("not public", sources[0]["job_error_message"])

        response = await client.post(f"/sources/{sources[0]['id']}/retry")
        self.assertEqual(response.status_code, 200)
        retried = response.json()
        self.assertEqual(retried["status"], "queued")
        self.assertEqual(retried["job_status"], "queued")

        with patch("oakresearch.ingestion._fetch_url_text", return_value="Recovered article text"):
            processed = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(processed)
        self.assertEqual(processed["status"], "succeeded")

        response = await client.get("/sources")
        self.assertEqual(response.status_code, 200)
        sources = response.json()
        self.assertEqual(sources[0]["status"], "succeeded")
        self.assertEqual(sources[0]["job_status"], "succeeded")

        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            jobs = await conn.fetch(
                "SELECT status, step_label FROM source_jobs WHERE source_id = $1 ORDER BY id ASC",
                sources[0]["id"],
            )
        self.assertEqual([job["status"] for job in jobs], ["failed", "succeeded"])
        self.assertEqual(jobs[-1]["step_label"], "ingestion-complete")

    async def test_url_validation_rejects_private_hostname_resolution(self) -> None:
        private_addrinfo = [(None, None, None, None, ("10.0.0.5", 443))]
        link_local_v6_addrinfo = [(None, None, None, None, ("fe80::1", 443, 0, 0))]
        mixed_addrinfo = [
            (None, None, None, None, ("2606:4700:4700::1111", 443, 0, 0)),
            (None, None, None, None, ("127.0.0.1", 443)),
        ]
        public_addrinfo = [(None, None, None, None, ("93.184.216.34", 443))]

        for addrinfo in (private_addrinfo, link_local_v6_addrinfo, mixed_addrinfo):
            with self.subTest(addrinfo=addrinfo), patch("oakresearch.ingestion.socket.getaddrinfo", return_value=addrinfo):
                with self.assertRaisesRegex(IngestionError, "non-public"):
                    _validate_public_url("https://example.com/article")

        with patch("oakresearch.ingestion.socket.getaddrinfo", return_value=public_addrinfo):
            _validate_public_url("https://example.com/article")

        with self.assertRaisesRegex(IngestionError, "not public"):
            _validate_public_url("http://[::1]/health")

    async def test_url_source_with_fallback_text_never_fetches_network(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)

        response = await client.post(
            "/sources",
            json={
                "source_type": "url",
                "title": "Fallback URL source",
                "source_url": "https://example.com/article",
                "content_text": "Stored fallback text should be ingested without fetching.",
            },
        )
        self.assertEqual(response.status_code, 200)
        source = response.json()

        with patch("oakresearch.ingestion._fetch_url_text", side_effect=AssertionError("network should not be fetched")):
            processed = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(processed)
        self.assertEqual(processed["status"], "succeeded")

        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            chunk_text = await conn.fetchval("SELECT chunk_text FROM source_chunks WHERE source_id = $1", source["id"])
        self.assertIn("Stored fallback text", chunk_text)


if __name__ == "__main__":
    unittest.main()
