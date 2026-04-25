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
from oakresearch.ingestion import process_next_source_job_once
from oakresearch.main import app


class Phase8AnsweringTest(unittest.IsolatedAsyncioTestCase):
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

    async def test_question_run_streams_answer_and_persists_citations(self) -> None:
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
        source_id = response.json()["id"]
        await process_next_source_job_once(self.pool)

        async def fake_stream(*args, **kwargs):
            yield "OakResearch uses FastAPI and Postgres"
            yield " [1]."

        with patch("oakresearch.answering.embed_text", side_effect=AnsweringError("no embeddings")), patch(
            "oakresearch.answering.stream_gemini_text", side_effect=fake_stream
        ):
            response = await client.post(
                "/runs",
                json={"question": "What stack does OakResearch use?"},
            )
            self.assertEqual(response.status_code, 200)
            run_id = response.json()["id"]
            processed = await process_next_run_job_once(self.pool)
            self.assertIsNotNone(processed)
            self.assertEqual(processed["status"], "succeeded")

        response = await client.get(f"/runs/{run_id}")
        self.assertEqual(response.status_code, 200)
        run = response.json()
        self.assertEqual(run["status"], "succeeded")
        self.assertIn("FastAPI", run["answer"]["answer_text"])
        self.assertGreaterEqual(len(run["answer"]["citations"]), 1)
        self.assertEqual(run["answer"]["citations"][0]["source_id"], source_id)

        async with client.stream("GET", f"/runs/{run_id}/stream") as stream_response:
            self.assertEqual(stream_response.status_code, 200)
            streamed = ""
            async for chunk in stream_response.aiter_text():
                streamed += chunk
        self.assertIn("FastAPI", streamed)
        self.assertIn("[1]", streamed)

        response = await client.get(f"/sources/{source_id}")
        self.assertEqual(response.status_code, 200)
        source = response.json()
        self.assertGreaterEqual(len(source["chunks"]), 1)

    async def test_weakly_grounded_question_refuses_to_answer(self) -> None:
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

        with patch("oakresearch.answering.embed_text", side_effect=AnsweringError("no embeddings")):
            response = await client.post(
                "/runs",
                json={"question": "What is the capital of Mars?"},
            )
            self.assertEqual(response.status_code, 200)
            run_id = response.json()["id"]
            processed = await process_next_run_job_once(self.pool)
            self.assertIsNotNone(processed)
            self.assertEqual(processed["status"], "blocked")

        response = await client.get(f"/runs/{run_id}")
        self.assertEqual(response.status_code, 200)
        run = response.json()
        self.assertEqual(run["status"], "blocked")
        self.assertIn("notebook sources", run["blocked_reason"])
        self.assertIn("enough grounded evidence", run["answer"]["answer_text"])
        self.assertEqual(run["answer"]["citations"], [])


if __name__ == "__main__":
    unittest.main()
