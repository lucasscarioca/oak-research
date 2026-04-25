from __future__ import annotations

import os
import unittest
import uuid

import asyncpg

from oakresearch.db import (
    apply_migrations,
    authenticate_user,
    bootstrap_instance,
    complete_onboarding,
    create_session,
    get_authenticated_user,
    hash_password,
    revoke_session,
    verify_password,
)


class Phase3AuthTest(unittest.IsolatedAsyncioTestCase):
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

    async def test_password_hash_round_trip(self) -> None:
        password_hash = hash_password("correct horse battery staple")
        self.assertTrue(verify_password("correct horse battery staple", password_hash))
        self.assertFalse(verify_password("not the password", password_hash))

    async def test_onboarding_creates_session_and_unlocks_auth(self) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(f'SET search_path TO {self.schema_name}')
            result = await complete_onboarding(conn, username="lucas", password="secret")
            self.assertTrue(result["instance"]["onboarding_complete"])
            user = await authenticate_user(conn, username="lucas", password="secret")
            self.assertIsNotNone(user)
            token = await create_session(conn, user["id"], user_agent="test-agent", ip_address="127.0.0.1")
            current_user = await get_authenticated_user(conn, token)
            self.assertIsNotNone(current_user)
            self.assertEqual(current_user["username"], "lucas")
            await revoke_session(conn, token)
            revoked_user = await get_authenticated_user(conn, token)
            self.assertIsNone(revoked_user)


if __name__ == "__main__":
    unittest.main()
