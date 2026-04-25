from __future__ import annotations

import os
import unittest
from datetime import UTC, datetime
import uuid

import asyncpg

from oakresearch.db import (
    apply_migrations,
    bootstrap_instance,
    create_source,
    get_provider_api_key,
    get_provider_config,
    list_sources,
    update_provider_config,
    update_source_title,
)


class Phase5And6Test(unittest.IsolatedAsyncioTestCase):
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

    async def test_provider_config_round_trip(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            config = await get_provider_config(conn)
            self.assertIsNotNone(config)
            self.assertEqual(config["validation_status"], "unknown")
            updated = await update_provider_config(
                conn,
                provider_name="gemini",
                api_key="test-key",
                validation_status="valid",
                validated_at=datetime.now(UTC),
            )
            self.assertEqual(updated["validation_status"], "valid")
            self.assertEqual(await get_provider_api_key(conn), "test-key")

    async def test_source_creation_queues_job_and_allows_title_edits(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            notebook_id = await conn.fetchval("SELECT id FROM notebooks WHERE is_default = TRUE LIMIT 1")
            source = await create_source(
                conn,
                {
                    "notebook_id": notebook_id,
                    "source_type": "text",
                    "title": "Original title",
                    "payload_uri": "/data/sample.txt",
                    "payload_sha256": "abc123",
                    "metadata": {"input_kind": "text"},
                },
            )
            self.assertEqual(source["title"], "Original title")
            self.assertEqual(source["status"], "queued")
            self.assertEqual(source["job_status"], "queued")

            sources = await list_sources(conn)
            self.assertEqual(len(sources), 1)
            self.assertEqual(sources[0]["status"], "queued")

            renamed = await update_source_title(conn, source["id"], "Renamed title")
            self.assertIsNotNone(renamed)
            self.assertEqual(renamed["title"], "Renamed title")

            sources_after = await list_sources(conn)
            self.assertEqual(sources_after[0]["title"], "Renamed title")


if __name__ == "__main__":
    unittest.main()
