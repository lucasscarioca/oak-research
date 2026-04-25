from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import uuid

import asyncpg
from httpx import ASGITransport, AsyncClient

from oakresearch import db as db_module
from oakresearch.answering import AnsweringError, process_next_run_job_once
from oakresearch.db import apply_migrations, bootstrap_instance
from oakresearch.ingestion import IngestionError, process_next_source_job_once
from oakresearch.main import app


class Phase12FailurePathTest(unittest.IsolatedAsyncioTestCase):
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

    async def configure_provider(self, client: AsyncClient) -> None:
        with patch("oakresearch.main.validate_gemini_api_key", return_value=(True, None)):
            response = await client.put("/provider/config", json={"api_key": "dummy-key"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["validation_status"], "valid")

    async def test_end_to_end_failure_path_verification(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)
        await self.configure_provider(client)

        broken_source_response = await client.post(
            "/sources",
            json={
                "source_type": "url",
                "title": "Broken URL source",
                "source_url": "https://example.invalid/missing",
            },
        )
        self.assertEqual(broken_source_response.status_code, 200)
        broken_source_id = broken_source_response.json()["id"]

        with patch(
            "oakresearch.ingestion._fetch_url_text",
            side_effect=IngestionError("Unable to fetch URL content from https://example.invalid/missing"),
        ):
            processed = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(processed)
        self.assertEqual(processed["status"], "failed")

        sources_response = await client.get("/sources")
        self.assertEqual(sources_response.status_code, 200)
        sources = sources_response.json()
        broken_source = next(item for item in sources if item["id"] == broken_source_id)
        self.assertEqual(broken_source["status"], "failed")
        self.assertIn("Unable to fetch URL content", broken_source["job_error_message"])

        diagnostics_response = await client.get("/diagnostics")
        self.assertEqual(diagnostics_response.status_code, 200)
        diagnostics = diagnostics_response.json()
        self.assertIn("Broken URL source", {item["label"] for item in diagnostics["recent_failures"]})
        self.assertIn(broken_source_id, [item["entity_id"] for item in diagnostics["recent_jobs"]])

        usable_source_response = await client.post(
            "/sources",
            json={
                "source_type": "text",
                "title": "Usable source",
                "content_text": "OakResearch stays usable after a broken URL fails.",
            },
        )
        self.assertEqual(usable_source_response.status_code, 200)
        usable_source_id = usable_source_response.json()["id"]
        processed = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(processed)
        self.assertEqual(processed["status"], "succeeded")

        async def fake_stream(*args, **kwargs):
            yield "OakResearch stays usable after a broken URL fails"
            yield " [1]."

        with patch("oakresearch.answering.embed_text", side_effect=AnsweringError("no embeddings")), patch(
            "oakresearch.answering.stream_gemini_text", side_effect=fake_stream
        ):
            run_response = await client.post(
                "/runs",
                json={"question": "What stays usable after a broken URL fails?"},
            )
            self.assertEqual(run_response.status_code, 200)
            run_id = run_response.json()["id"]
            processed = await process_next_run_job_once(self.pool)
            self.assertIsNotNone(processed)
            self.assertEqual(processed["status"], "succeeded")

        run_detail_response = await client.get(f"/runs/{run_id}")
        self.assertEqual(run_detail_response.status_code, 200)
        self.assertEqual(run_detail_response.json()["status"], "succeeded")
        self.assertGreaterEqual(len(run_detail_response.json()["answer"]["citations"]), 1)

        retry_response = await client.post(f"/sources/{broken_source_id}/retry")
        self.assertEqual(retry_response.status_code, 200)
        self.assertEqual(retry_response.json()["status"], "queued")

        with patch("oakresearch.ingestion._fetch_url_text", return_value="Recovered article text"):
            processed = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(processed)
        self.assertEqual(processed["status"], "succeeded")

        retry_detail_response = await client.get(f"/sources/{broken_source_id}")
        self.assertEqual(retry_detail_response.status_code, 200)
        retry_detail = retry_detail_response.json()
        self.assertEqual(retry_detail["status"], "succeeded")
        self.assertGreaterEqual(len(retry_detail["chunks"]), 1)

        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            jobs = await conn.fetch(
                "SELECT status FROM source_jobs WHERE source_id = $1 ORDER BY id ASC",
                broken_source_id,
            )
        self.assertEqual([job["status"] for job in jobs], ["failed", "succeeded"])
        self.assertGreaterEqual(len(jobs), 2)

        sources_response = await client.get("/sources")
        self.assertEqual(sources_response.status_code, 200)
        sources = sources_response.json()
        broken_source = next(item for item in sources if item["id"] == broken_source_id)
        self.assertEqual(broken_source["status"], "succeeded")
        usable_source = next(item for item in sources if item["id"] == usable_source_id)
        self.assertEqual(usable_source["status"], "succeeded")


if __name__ == "__main__":
    unittest.main()
