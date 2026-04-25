from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
import uuid

import asyncpg
from httpx import ASGITransport, AsyncClient

from oakresearch import db as db_module
from oakresearch.answering import DEFAULT_REFUSAL_MESSAGE
from oakresearch.db import apply_migrations, bootstrap_instance
from oakresearch.ingestion import process_next_source_job_once
from oakresearch.main import app


class Phase9RunsTest(unittest.IsolatedAsyncioTestCase):
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

    async def test_run_history_tracks_success_blocked_failed_and_rerun_attempts(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)
        await self.configure_provider(client)

        response = await client.post(
            "/sources",
            json={
                "source_type": "text",
                "title": "OakResearch overview",
                "content_text": "OakResearch uses FastAPI, Postgres, and a worker service. The notebook is self-hosted.",
            },
        )
        self.assertEqual(response.status_code, 200)
        await process_next_source_job_once(self.pool)

        response = await client.post(
            "/runs",
            json={
                "question": "What stack does OakResearch use?",
                "status": "succeeded",
                "step_label": "answer-complete",
                "answer": {
                    "answer_text": "OakResearch uses FastAPI and Postgres [1].",
                    "trace_summary": "Retrieved 1 chunk(s); Sources: OakResearch overview",
                    "model": "gemini-2.0-flash",
                    "citations": [
                        {
                            "source_id": 1,
                            "chunk_ref": "1:0",
                            "citation_text": "Chunk for OakResearch overview",
                            "citation_index": 0,
                        }
                    ],
                },
            },
        )
        self.assertEqual(response.status_code, 200)
        success_run_id = response.json()["id"]

        response = await client.post(
            "/runs",
            json={
                "question": "What is the capital of Mars?",
                "status": "blocked",
                "step_label": "grounding-insufficient",
                "blocked_reason": DEFAULT_REFUSAL_MESSAGE,
            },
        )
        self.assertEqual(response.status_code, 200)
        blocked_run_id = response.json()["id"]

        response = await client.post(
            "/runs",
            json={
                "question": "This run failed in the worker",
                "status": "failed",
                "step_label": "answer-generation-failed",
                "error_message": "Worker crashed",
            },
        )
        self.assertEqual(response.status_code, 200)
        failed_run_id = response.json()["id"]

        response = await client.post(
            "/runs",
            json={
                "question": "What stack does OakResearch use?",
                "status": "succeeded",
                "step_label": "answer-complete",
                "rerun_of_run_id": success_run_id,
                "answer": {
                    "answer_text": "OakResearch uses FastAPI and Postgres [1].",
                    "trace_summary": "Retrieved 1 chunk(s); Sources: OakResearch overview",
                    "model": "gemini-2.0-flash",
                    "citations": [
                        {
                            "source_id": 1,
                            "chunk_ref": "1:0",
                            "citation_text": "Chunk for OakResearch overview",
                            "citation_index": 0,
                        }
                    ],
                },
            },
        )
        self.assertEqual(response.status_code, 200)
        rerun_run_id = response.json()["id"]
        self.assertEqual(response.json()["rerun_of_run_id"], success_run_id)

        response = await client.get("/runs")
        self.assertEqual(response.status_code, 200)
        runs = response.json()
        statuses = {run["status"] for run in runs}
        self.assertIn("succeeded", statuses)
        self.assertIn("blocked", statuses)
        self.assertIn("failed", statuses)

        rerun_record = next(run for run in runs if run["id"] == rerun_run_id)
        self.assertEqual(rerun_record["rerun_of_run_id"], success_run_id)
        self.assertEqual(rerun_record["status"], "succeeded")

        blocked_record = next(run for run in runs if run["id"] == blocked_run_id)
        self.assertEqual(blocked_record["status"], "blocked")
        self.assertIsNotNone(blocked_record["blocked_reason"])

        failed_record = next(run for run in runs if run["id"] == failed_run_id)
        self.assertEqual(failed_record["status"], "failed")
        self.assertEqual(failed_record["error_message"], "Worker crashed")

        response = await client.get(f"/runs/{rerun_run_id}")
        self.assertEqual(response.status_code, 200)
        run = response.json()
        self.assertEqual(run["rerun_of_run_id"], success_run_id)
        self.assertEqual(run["status"], "succeeded")
        self.assertGreaterEqual(len(run["answer"]["citations"]), 1)


if __name__ == "__main__":
    unittest.main()
