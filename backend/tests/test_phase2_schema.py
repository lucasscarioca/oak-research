from __future__ import annotations

import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path

import asyncpg

from oakresearch.db import apply_migrations, bootstrap_instance, get_bootstrap_state


class Phase2SchemaTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.database_url = os.environ.get(
            "TEST_DATABASE_URL",
            "postgresql://oakresearch:oakresearch@db:5432/oakresearch",
        )
        self.schema_name = f"test_{uuid.uuid4().hex}"
        self.pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=2)
        async with self.pool.acquire() as conn:
            await conn.execute(f'CREATE SCHEMA {self.schema_name}')
            await conn.execute(f'SET search_path TO {self.schema_name}')
            await apply_migrations(conn)
            await bootstrap_instance(conn)

    async def asyncTearDown(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            await conn.execute(f'DROP SCHEMA IF EXISTS {self.schema_name} CASCADE')
        await self.pool.close()

    async def test_bootstrap_creates_owner_and_default_notebook(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            state = await get_bootstrap_state(conn)
        self.assertTrue(state["bootstrap_complete"])
        self.assertIsNotNone(state["owner"])
        self.assertIsNotNone(state["default_notebook"])

    async def test_sources_and_runs_persist(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            notebook_id = await conn.fetchval("SELECT id FROM notebooks WHERE is_default = TRUE LIMIT 1")
            source_id = await conn.fetchval(
                """
                INSERT INTO sources (notebook_id, source_type, title, payload_uri, payload_sha256, metadata)
                VALUES ($1, 'text', 'Sample source', '/data/sample.md', 'abc123', '{}'::jsonb)
                RETURNING id
                """,
                notebook_id,
            )
            run_id = await conn.fetchval(
                """
                INSERT INTO runs (notebook_id, question, status, step_label, blocked_reason, error_message)
                VALUES ($1, 'What happened?', 'blocked', 'provider-missing', 'No provider', NULL)
                RETURNING id
                """,
                notebook_id,
            )
            answer_id = await conn.fetchval(
                """
                INSERT INTO answers (run_id, answer_text, trace_summary, model)
                VALUES ($1, 'Answer text', 'Trace summary', 'gemini-2.0')
                RETURNING id
                """,
                run_id,
            )
            await conn.execute(
                """
                INSERT INTO citations (answer_id, source_id, chunk_ref, citation_text, citation_index)
                VALUES ($1, $2, 'chunk-1', 'citation', 0)
                """,
                answer_id,
                source_id,
            )

        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            source_count = await conn.fetchval("SELECT count(*) FROM sources")
            run_count = await conn.fetchval("SELECT count(*) FROM runs")
            answer_count = await conn.fetchval("SELECT count(*) FROM answers")
            citation_count = await conn.fetchval("SELECT count(*) FROM citations")

        self.assertEqual(source_count, 1)
        self.assertEqual(run_count, 1)
        self.assertEqual(answer_count, 1)
        self.assertEqual(citation_count, 1)

    async def test_bootstrap_is_idempotent(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            first = await bootstrap_instance(conn)
            second = await bootstrap_instance(conn)
        self.assertEqual(first["owner"]["id"], second["owner"]["id"])
        self.assertEqual(first["default_notebook"]["id"], second["default_notebook"]["id"])


if __name__ == "__main__":
    unittest.main()
