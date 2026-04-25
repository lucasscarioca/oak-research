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
from oakresearch.db import apply_migrations, bootstrap_instance
from oakresearch.ingestion import IngestionError, process_next_source_job_once
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

        response = await client.get("/sources")
        self.assertEqual(response.status_code, 200)
        sources = response.json()
        self.assertEqual(sources[0]["status"], "succeeded")
        self.assertEqual(sources[0]["job_status"], "succeeded")
        self.assertEqual(sources[0]["job_step_label"], "ingestion-complete")

        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            chunks = await conn.fetch(
                "SELECT chunk_index, chunk_text FROM source_chunks WHERE source_id = $1 ORDER BY chunk_index ASC",
                sources[0]["id"],
            )
        self.assertGreaterEqual(len(chunks), 1)
        self.assertIn("alpha paragraph", chunks[0]["chunk_text"])

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


if __name__ == "__main__":
    unittest.main()
