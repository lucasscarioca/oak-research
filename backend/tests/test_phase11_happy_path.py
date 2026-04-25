from __future__ import annotations

import asyncio
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
from oakresearch.ingestion import process_next_source_job_once
from oakresearch.main import app


class Phase11HappyPathTest(unittest.IsolatedAsyncioTestCase):
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

    async def test_end_to_end_happy_path_verification(self) -> None:
        client = await self.create_authenticated_client()
        self.addAsyncCleanup(client.aclose)
        await self.configure_provider(client)

        auth_status = await client.get("/auth/status")
        self.assertEqual(auth_status.status_code, 200)
        self.assertTrue(auth_status.json()["authenticated"])

        notebook_response = await client.get("/notebooks/default")
        self.assertEqual(notebook_response.status_code, 200)
        notebook_id = notebook_response.json()["id"]

        source_response = await client.post(
            "/sources",
            json={
                "notebook_id": notebook_id,
                "source_type": "text",
                "title": "OakResearch overview",
                "content_text": "OakResearch uses FastAPI, Postgres, a worker service, and cited answers.",
            },
        )
        self.assertEqual(source_response.status_code, 200)
        source_id = source_response.json()["id"]

        source_job = await process_next_source_job_once(self.pool)
        self.assertIsNotNone(source_job)
        self.assertEqual(source_job["status"], "succeeded")

        source_detail = await client.get(f"/sources/{source_id}")
        self.assertEqual(source_detail.status_code, 200)
        self.assertGreaterEqual(len(source_detail.json()["chunks"]), 1)

        async def fake_stream(*args, **kwargs):
            yield "OakResearch uses FastAPI and Postgres"
            yield " [1]."

        with patch("oakresearch.answering.embed_text", side_effect=AnsweringError("no embeddings")), patch(
            "oakresearch.answering.stream_gemini_text", side_effect=fake_stream
        ):
            run_response = await client.post("/runs", json={"question": "What stack does OakResearch use?"})
            self.assertEqual(run_response.status_code, 200)
            run_id = run_response.json()["id"]

            streamed_tokens: list[str] = []

            async def consume_stream() -> None:
                async with client.stream("GET", f"/runs/{run_id}/stream") as stream_response:
                    self.assertEqual(stream_response.status_code, 200)
                    async for chunk in stream_response.aiter_text():
                        streamed_tokens.append(chunk)

            stream_task = asyncio.create_task(consume_stream())
            await asyncio.sleep(0.1)
            run_job = await process_next_run_job_once(self.pool)
            self.assertIsNotNone(run_job)
            self.assertEqual(run_job["status"], "succeeded")
            await asyncio.wait_for(stream_task, timeout=10)

        stream_text = "".join(streamed_tokens)
        self.assertIn("OakResearch uses FastAPI and Postgres", stream_text)
        self.assertIn("[1]", stream_text)

        run_detail_response = await client.get(f"/runs/{run_id}")
        self.assertEqual(run_detail_response.status_code, 200)
        run_detail = run_detail_response.json()
        self.assertEqual(run_detail["status"], "succeeded")
        self.assertGreaterEqual(len(run_detail["answer"]["citations"]), 1)

        citation_source_id = run_detail["answer"]["citations"][0]["source_id"]
        source_detail_response = await client.get(f"/sources/{citation_source_id}")
        self.assertEqual(source_detail_response.status_code, 200)
        self.assertEqual(source_detail_response.json()["id"], source_id)

        runs_response = await client.get("/runs")
        self.assertEqual(runs_response.status_code, 200)
        self.assertIn(run_id, [run["id"] for run in runs_response.json()])

        rerun_response = await client.post(
            "/runs",
            json={
                "question": "What stack does OakResearch use?",
                "rerun_of_run_id": run_id,
            },
        )
        self.assertEqual(rerun_response.status_code, 200)
        rerun_id = rerun_response.json()["id"]

        with patch("oakresearch.answering.embed_text", side_effect=AnsweringError("no embeddings")), patch(
            "oakresearch.answering.stream_gemini_text", side_effect=fake_stream
        ):
            rerun_job = await process_next_run_job_once(self.pool)
            self.assertIsNotNone(rerun_job)
            self.assertEqual(rerun_job["status"], "succeeded")

        rerun_detail_response = await client.get(f"/runs/{rerun_id}")
        self.assertEqual(rerun_detail_response.status_code, 200)
        self.assertEqual(rerun_detail_response.json()["rerun_of_run_id"], run_id)

        diagnostics_response = await client.get("/diagnostics")
        self.assertEqual(diagnostics_response.status_code, 200)
        diagnostics = diagnostics_response.json()
        self.assertEqual(diagnostics["provider_test_result"]["status"], "valid")
        self.assertIn("OakResearch overview", {job["label"] for job in diagnostics["recent_jobs"]})
        self.assertEqual(diagnostics["recent_failures"], [])


if __name__ == "__main__":
    unittest.main()
