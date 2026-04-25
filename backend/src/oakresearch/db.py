from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import base64
import hashlib
import hmac
import secrets

import asyncpg

from .settings import get_settings

settings = get_settings()
MIGRATION_LOCK_ID = 842_740_921
MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
DEFAULT_OWNER_USERNAME = "owner"
DEFAULT_OWNER_PASSWORD_HASH = "unconfigured"
DEFAULT_NOTEBOOK_NAME = "Default notebook"
DEFAULT_PROVIDER_NAME = "gemini"
DEFAULT_STORAGE_DIR = Path(settings.storage_path)
DEFAULT_SESSION_COOKIE_NAME = "oakresearch_session"
DEFAULT_SESSION_TTL = timedelta(days=30)
PASSWORD_HASH_ALGORITHM = "pbkdf2_sha256"
PASSWORD_HASH_ITERATIONS = 200_000


def split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    for raw_statement in sql.split(";"):
        statement = raw_statement.strip()
        if statement:
            statements.append(statement)
    return statements


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        PASSWORD_HASH_ITERATIONS,
    )
    return "{}${}${}${}".format(
        PASSWORD_HASH_ALGORITHM,
        PASSWORD_HASH_ITERATIONS,
        base64.b64encode(salt).decode("ascii"),
        base64.b64encode(digest).decode("ascii"),
    )


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, iterations_text, salt_text, digest_text = password_hash.split("$", maxsplit=3)
    except ValueError:
        return False
    if algorithm != PASSWORD_HASH_ALGORITHM:
        return False

    try:
        iterations = int(iterations_text)
        salt = base64.b64decode(salt_text.encode("ascii"))
        expected_digest = base64.b64decode(digest_text.encode("ascii"))
    except (ValueError, UnicodeError):
        return False

    candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(candidate, expected_digest)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        settings.database_url,
        min_size=1,
        max_size=5,
    )


async def ensure_storage_dir() -> None:
    DEFAULT_STORAGE_DIR.mkdir(parents=True, exist_ok=True)


async def ensure_migrations_table(conn: asyncpg.Connection) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version text PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )


async def apply_migrations(conn: asyncpg.Connection) -> list[str]:
    async with conn.transaction():
        await ensure_migrations_table(conn)
        await conn.execute("SELECT pg_advisory_xact_lock($1)", MIGRATION_LOCK_ID)

        applied = {
            row["version"]
            for row in await conn.fetch("SELECT version FROM schema_migrations ORDER BY version")
        }
        applied_versions: list[str] = []

        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            version = path.stem.split("_", maxsplit=1)[0]
            if version in applied:
                continue

            sql = path.read_text(encoding="utf-8")
            for statement in split_sql_statements(sql):
                await conn.execute(statement)
            await conn.execute("INSERT INTO schema_migrations (version) VALUES ($1)", version)
            applied_versions.append(version)

        return applied_versions


async def bootstrap_instance(conn: asyncpg.Connection) -> dict[str, Any]:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_instance (
            id integer PRIMARY KEY,
            owner_user_id bigint NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
            default_notebook_id bigint NOT NULL REFERENCES notebooks(id) ON DELETE RESTRICT,
            onboarding_complete boolean NOT NULL DEFAULT false,
            bootstrap_version integer NOT NULL DEFAULT 1,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT app_instance_singleton CHECK (id = 1)
        )
        """
    )
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS provider_configs (
            id integer PRIMARY KEY,
            provider_name text NOT NULL,
            api_key_ciphertext text,
            validation_status text NOT NULL DEFAULT 'unknown',
            validated_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT provider_configs_singleton CHECK (id = 1)
        )
        """
    )

    owner = await conn.fetchrow(
        """
        INSERT INTO users (username, password_hash)
        VALUES ($1, $2)
        ON CONFLICT (username)
        DO UPDATE SET username = EXCLUDED.username
        RETURNING *
        """,
        DEFAULT_OWNER_USERNAME,
        DEFAULT_OWNER_PASSWORD_HASH,
    )
    assert owner is not None

    notebook = await conn.fetchrow(
        """
        INSERT INTO notebooks (owner_user_id, name, is_default)
        VALUES ($1, $2, TRUE)
        ON CONFLICT (owner_user_id, name)
        DO UPDATE SET is_default = EXCLUDED.is_default, updated_at = now()
        RETURNING *
        """,
        owner["id"],
        DEFAULT_NOTEBOOK_NAME,
    )
    assert notebook is not None

    await conn.execute(
        """
        UPDATE notebooks
        SET is_default = FALSE, updated_at = now()
        WHERE owner_user_id = $1 AND id <> $2 AND is_default = TRUE
        """,
        owner["id"],
        notebook["id"],
    )

    await conn.execute(
        """
        INSERT INTO provider_configs (id, provider_name, api_key_ciphertext, validation_status)
        VALUES (1, $1, NULL, 'unknown')
        ON CONFLICT (id)
        DO UPDATE SET provider_name = EXCLUDED.provider_name, updated_at = now()
        """,
        DEFAULT_PROVIDER_NAME,
    )

    await conn.execute(
        """
        INSERT INTO app_instance (id, owner_user_id, default_notebook_id, onboarding_complete, bootstrap_version)
        VALUES (1, $1, $2, FALSE, 1)
        ON CONFLICT (id)
        DO UPDATE SET owner_user_id = EXCLUDED.owner_user_id,
                      default_notebook_id = EXCLUDED.default_notebook_id,
                      onboarding_complete = COALESCE(app_instance.onboarding_complete, FALSE),
                      bootstrap_version = EXCLUDED.bootstrap_version,
                      updated_at = now()
        """,
        owner["id"],
        notebook["id"],
    )

    return await get_bootstrap_state(conn)


async def create_session(
    conn: asyncpg.Connection,
    user_id: int,
    *,
    user_agent: str | None = None,
    ip_address: str | None = None,
) -> str:
    token = secrets.token_urlsafe(32)
    await conn.execute(
        """
        INSERT INTO auth_sessions (user_id, session_token_hash, expires_at, user_agent, ip_address)
        VALUES ($1, $2, now() + $3::interval, $4, $5)
        """,
        user_id,
        token_hash(token),
        DEFAULT_SESSION_TTL,
        user_agent,
        ip_address,
    )
    return token


async def revoke_session(conn: asyncpg.Connection, token: str) -> None:
    await conn.execute(
        """
        UPDATE auth_sessions
        SET revoked_at = now()
        WHERE session_token_hash = $1 AND revoked_at IS NULL
        """,
        token_hash(token),
    )


async def get_authenticated_user(
    conn: asyncpg.Connection, token: str | None
) -> dict[str, Any] | None:
    if not token:
        return None

    row = await conn.fetchrow(
        """
        SELECT u.id, u.username, u.created_at
        FROM auth_sessions s
        JOIN users u ON u.id = s.user_id
        WHERE s.session_token_hash = $1
          AND s.revoked_at IS NULL
          AND s.expires_at > now()
        LIMIT 1
        """,
        token_hash(token),
    )
    if row is None:
        return None

    await conn.execute(
        """
        UPDATE auth_sessions
        SET last_seen_at = now()
        WHERE session_token_hash = $1
        """,
        token_hash(token),
    )
    return dict(row)


async def initialize_database(conn: asyncpg.Connection) -> dict[str, Any]:
    await apply_migrations(conn)
    await ensure_storage_dir()
    return await bootstrap_instance(conn)


async def get_bootstrap_state(conn: asyncpg.Connection) -> dict[str, Any]:
    latest_migration = await conn.fetchval(
        "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1"
    )
    owner = await conn.fetchrow(
        "SELECT * FROM users ORDER BY created_at ASC, id ASC LIMIT 1"
    )
    notebook = None
    if owner is not None:
        notebook = await conn.fetchrow(
            """
            SELECT *
            FROM notebooks
            WHERE owner_user_id = $1 AND is_default = TRUE
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            """,
            owner["id"],
        )

    provider_config = await conn.fetchrow("SELECT * FROM provider_configs WHERE id = 1")
    instance = await conn.fetchrow("SELECT * FROM app_instance WHERE id = 1")
    return {
        "bootstrap_complete": owner is not None and notebook is not None and instance is not None,
        "schema_version": latest_migration,
        "owner": dict(owner) if owner is not None else None,
        "default_notebook": dict(notebook) if notebook is not None else None,
        "provider_config": dict(provider_config) if provider_config is not None else None,
        "instance": dict(instance) if instance is not None else None,
        "onboarding_complete": bool(instance["onboarding_complete"]) if instance is not None else False,
    }


async def complete_onboarding(
    conn: asyncpg.Connection,
    *,
    username: str,
    password: str,
) -> dict[str, Any]:
    async with conn.transaction():
        owner = await conn.fetchrow(
            "SELECT * FROM users ORDER BY created_at ASC, id ASC LIMIT 1"
        )
        if owner is None:
            owner = await conn.fetchrow(
                """
                INSERT INTO users (username, password_hash)
                VALUES ($1, $2)
                RETURNING *
                """,
                username,
                hash_password(password),
            )
        else:
            owner = await conn.fetchrow(
                """
                UPDATE users
                SET username = $1,
                    password_hash = $2
                WHERE id = $3
                RETURNING *
                """,
                username,
                hash_password(password),
                owner["id"],
            )
        assert owner is not None

        instance = await conn.fetchrow(
            """
            UPDATE app_instance
            SET owner_user_id = $1,
                onboarding_complete = TRUE,
                updated_at = now()
            WHERE id = 1
            RETURNING *
            """,
            owner["id"],
        )
        assert instance is not None
        return {"owner": dict(owner), "instance": dict(instance)}


async def authenticate_user(
    conn: asyncpg.Connection,
    *,
    username: str,
    password: str,
) -> dict[str, Any] | None:
    user = await conn.fetchrow(
        "SELECT * FROM users WHERE username = $1 LIMIT 1",
        username,
    )
    if user is None:
        return None
    if not verify_password(password, user["password_hash"]):
        return None
    return dict(user)


async def list_sources(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT *
        FROM sources
        ORDER BY created_at DESC, id DESC
        """
    )
    return [dict(row) for row in rows]


async def create_source(conn: asyncpg.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    row = await conn.fetchrow(
        """
        INSERT INTO sources (notebook_id, source_type, title, payload_uri, payload_sha256, metadata)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        RETURNING *
        """,
        payload["notebook_id"],
        payload["source_type"],
        payload["title"],
        payload["payload_uri"],
        payload["payload_sha256"],
        json.dumps(payload.get("metadata", {})),
    )
    assert row is not None
    return dict(row)


async def list_runs(conn: asyncpg.Connection) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT r.*, a.answer_text, a.trace_summary, a.model
        FROM runs r
        LEFT JOIN answers a ON a.run_id = r.id
        ORDER BY r.created_at DESC, r.id DESC
        """
    )
    return [dict(row) for row in rows]


async def create_run(conn: asyncpg.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    async with conn.transaction():
        run = await conn.fetchrow(
            """
            INSERT INTO runs (
                notebook_id, question, status, step_label, blocked_reason,
                error_message, rerun_of_run_id, started_at, finished_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING *
            """,
            payload["notebook_id"],
            payload["question"],
            payload.get("status", "queued"),
            payload.get("step_label"),
            payload.get("blocked_reason"),
            payload.get("error_message"),
            payload.get("rerun_of_run_id"),
            payload.get("started_at"),
            payload.get("finished_at"),
        )
        assert run is not None

        answer_payload = payload.get("answer")
        if answer_payload is not None:
            answer = await conn.fetchrow(
                """
                INSERT INTO answers (run_id, answer_text, trace_summary, model)
                VALUES ($1, $2, $3, $4)
                RETURNING *
                """,
                run["id"],
                answer_payload["answer_text"],
                answer_payload.get("trace_summary"),
                answer_payload.get("model"),
            )
            assert answer is not None

            for index, citation_payload in enumerate(answer_payload.get("citations", [])):
                await conn.execute(
                    """
                    INSERT INTO citations (answer_id, source_id, chunk_ref, citation_text, citation_index)
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    answer["id"],
                    citation_payload["source_id"],
                    citation_payload.get("chunk_ref"),
                    citation_payload["citation_text"],
                    citation_payload.get("citation_index", index),
                )

    return await get_run(conn, run["id"])


async def get_run(conn: asyncpg.Connection, run_id: int) -> dict[str, Any] | None:
    run = await conn.fetchrow(
        """
        SELECT r.*, a.id AS answer_id, a.answer_text, a.trace_summary, a.model
        FROM runs r
        LEFT JOIN answers a ON a.run_id = r.id
        WHERE r.id = $1
        """,
        run_id,
    )
    if run is None:
        return None

    citation_rows = []
    answer_id = run["answer_id"]
    if answer_id is not None:
        rows = await conn.fetch(
            """
            SELECT *
            FROM citations
            WHERE answer_id = $1
            ORDER BY citation_index ASC, id ASC
            """,
            answer_id,
        )
        citation_rows = [dict(row) for row in rows]

    data = dict(run)
    answer_id_value = data.pop("answer_id", None)
    if answer_id_value is not None:
        data["answer"] = {
            "id": answer_id_value,
            "answer_text": data.pop("answer_text", None),
            "trace_summary": data.pop("trace_summary", None),
            "model": data.pop("model", None),
            "citations": citation_rows,
        }
    else:
        data.pop("answer_text", None)
        data.pop("trace_summary", None)
        data.pop("model", None)
    return data
