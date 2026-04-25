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
from oakresearch.db import apply_migrations, bootstrap_instance, mark_run_failed, mark_run_job_failed
from oakresearch.ingestion import IngestionError, process_next_source_job_once
from oakresearch.main import app


class Phase10DiagnosticsTest(unittest.IsolatedAsyncioTestCase):
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

    async def test_diagnostics_reports_provider_and_recent_failure_state(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)
        await self.configure_provider(client)

        response = await client.post(
            "/sources",
            json={
                "source_type": "text",
                "title": "OakResearch overview",
                "content_text": "OakResearch uses FastAPI, Postgres, and a worker service.",
            },
        )
        self.assertEqual(response.status_code, 200)
        await process_next_source_job_once(self.pool)

        response = await client.post(
            "/sources",
            json={
                "source_type": "url",
                "title": "Broken URL source",
                "source_url": "https://example.invalid/404",
            },
        )
        self.assertEqual(response.status_code, 200)
        with patch("oakresearch.ingestion.process_source_payload", side_effect=IngestionError("boom")):
            processed_source = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(processed_source)
        self.assertEqual(processed_source["status"], "failed")

        with patch("oakresearch.answering.embed_text", side_effect=AnsweringError("no embeddings")):
            response = await client.post(
                "/runs",
                json={"question": "What is the capital of Mars?"},
            )
            self.assertEqual(response.status_code, 200)
            blocked_run_id = response.json()["id"]
            processed_run = await process_next_run_job_once(self.pool)
        self.assertIsNotNone(processed_run)
        self.assertEqual(processed_run["status"], "blocked")

        response = await client.post(
            "/runs",
            json={"question": "Worker crashed mid-run"},
        )
        self.assertEqual(response.status_code, 200)
        failed_run_id = response.json()["id"]
        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            failed_job_id = await conn.fetchval(
                "SELECT id FROM jobs WHERE entity_id = $1 AND kind = 'run-question' ORDER BY id DESC LIMIT 1",
                failed_run_id,
            )
            await mark_run_failed(conn, run_id=failed_run_id, error_message="Worker crashed")
            await mark_run_job_failed(conn, int(failed_job_id), "Worker crashed")

        response = await client.get("/diagnostics")
        self.assertEqual(response.status_code, 200)
        diagnostics = response.json()

        self.assertEqual(diagnostics["provider_test_result"]["status"], "valid")
        self.assertEqual(diagnostics["provider_test_result"]["message"], "Saved key is validated")

        recent_job_labels = {item["label"] for item in diagnostics["recent_jobs"]}
        self.assertIn("OakResearch overview", recent_job_labels)
        self.assertIn("Broken URL source", recent_job_labels)
        self.assertIn("What is the capital of Mars?", recent_job_labels)
        self.assertIn("Worker crashed mid-run", recent_job_labels)

        failure_labels = {item["label"] for item in diagnostics["recent_failures"]}
        self.assertIn("Broken URL source", failure_labels)
        self.assertIn("What is the capital of Mars?", failure_labels)
        self.assertIn("Worker crashed mid-run", failure_labels)

        failed_run = next(item for item in diagnostics["recent_failures"] if item["job_id"] == failed_run_id)
        self.assertEqual(failed_run["status"], "failed")
        blocked_run = next(item for item in diagnostics["recent_failures"] if item["job_id"] == blocked_run_id)
        self.assertEqual(blocked_run["status"], "blocked")


if __name__ == "__main__":
    unittest.main()
