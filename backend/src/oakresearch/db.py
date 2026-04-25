from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import asyncpg
from cryptography.fernet import Fernet, InvalidToken

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


def _secret_cipher() -> Fernet:
    key_material = hashlib.sha256(settings.app_secret.encode("utf-8")).digest()
    return Fernet(base64.urlsafe_b64encode(key_material))


def encode_secret(value: str) -> str:
    return _secret_cipher().encrypt(value.encode("utf-8")).decode("ascii")


def decode_secret(value: str) -> str:
    try:
        return _secret_cipher().decrypt(value.encode("ascii")).decode("utf-8")
    except (InvalidToken, ValueError, UnicodeError) as exc:
        raise ValueError("Invalid encrypted secret") from exc


def validate_gemini_api_key(api_key: str, *, timeout: float = 10.0) -> tuple[bool, str | None]:
    normalized = api_key.strip()
    if not normalized:
        return False, "API key is required"

    url = f"https://generativelanguage.googleapis.com/v1beta/models?{urllib.parse.urlencode({'key': normalized})}"
    request = urllib.request.Request(url, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            response.read()
        return True, None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(detail)
            message = payload.get("error", {}).get("message") or detail
        except json.JSONDecodeError:
            message = detail or str(exc)
        return False, message
    except Exception as exc:  # pragma: no cover - network failures surface in UI
        return False, str(exc)


async def create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        settings.database_url,
        min_size=1,
        max_size=5,
    )


async def ensure_storage_dir() -> None:
    DEFAULT_STORAGE_DIR.mkdir(parents=True, exist_ok=True)


def normalize_json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def store_source_payload(*, source_type: str, data: bytes, original_name: str | None = None) -> dict[str, str]:
    payload_sha256 = hashlib.sha256(data).hexdigest()
    suffix_map = {
        "pdf": ".pdf",
        "text": ".txt",
        "markdown": ".md",
        "url": ".txt",
    }
    suffix = suffix_map.get(source_type, Path(original_name or "").suffix or ".bin")
    target_dir = DEFAULT_STORAGE_DIR / "sources" / payload_sha256[:2]
    target_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{payload_sha256}{suffix}"
    payload_path = target_dir / file_name
    payload_path.write_bytes(data)
    return {"payload_uri": str(payload_path), "payload_sha256": payload_sha256}


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


async def get_provider_config(conn: asyncpg.Connection) -> dict[str, Any] | None:
    row = await conn.fetchrow("SELECT * FROM provider_configs WHERE id = 1")
    return dict(row) if row is not None else None


async def get_provider_api_key(conn: asyncpg.Connection) -> str | None:
    row = await get_provider_config(conn)
    if row is None:
        return None
    ciphertext = row.get("api_key_ciphertext")
    if not ciphertext:
        return None
    try:
        return decode_secret(ciphertext)
    except ValueError:
        return None


async def update_provider_config(
    conn: asyncpg.Connection,
    *,
    provider_name: str,
    api_key: str | None,
    validation_status: str,
    validated_at: datetime | None,
) -> dict[str, Any]:
    encoded_key = encode_secret(api_key) if api_key else None
    row = await conn.fetchrow(
        """
        INSERT INTO provider_configs (
            id, provider_name, api_key_ciphertext, validation_status, validated_at
        )
        VALUES (1, $1, $2, $3, $4)
        ON CONFLICT (id)
        DO UPDATE SET provider_name = EXCLUDED.provider_name,
                      api_key_ciphertext = EXCLUDED.api_key_ciphertext,
                      validation_status = EXCLUDED.validation_status,
                      validated_at = EXCLUDED.validated_at,
                      updated_at = now()
        RETURNING *
        """,
        provider_name,
        encoded_key,
        validation_status,
        validated_at,
    )
    assert row is not None
    return dict(row)


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
        SELECT
            s.*,
            sj.id AS latest_job_id,
            sj.status AS job_status,
            sj.step_label AS job_step_label,
            sj.error_message AS job_error_message,
            sj.started_at AS job_started_at,
            sj.finished_at AS job_finished_at
        FROM sources s
        LEFT JOIN LATERAL (
            SELECT *
            FROM source_jobs
            WHERE source_id = s.id
            ORDER BY created_at DESC, id DESC
            LIMIT 1
        ) sj ON TRUE
        ORDER BY s.created_at DESC, s.id DESC
        """
    )
    items: list[dict[str, Any]] = []
    for row in rows:
        data = dict(row)
        data["metadata"] = normalize_json_value(data.get("metadata"))
        data["status"] = data.get("job_status") or "untracked"
        items.append(data)
    return items


async def queue_source_job(conn: asyncpg.Connection, source_id: int) -> dict[str, Any]:
    async with conn.transaction():
        job = await conn.fetchrow(
            """
            INSERT INTO source_jobs (source_id, status, step_label)
            VALUES ($1, 'queued', 'queued-for-ingestion')
            RETURNING *
            """,
            source_id,
        )
        assert job is not None
        await conn.execute(
            """
            INSERT INTO source_job_items (job_id, item_index, status, step_label)
            VALUES ($1, 0, 'queued', 'queued-for-ingestion')
            """,
            job["id"],
        )
    return dict(job)


async def create_source(conn: asyncpg.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    async with conn.transaction():
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

    await queue_source_job(conn, int(row["id"]))
    return await get_source(conn, row["id"])


async def claim_next_source_job(conn: asyncpg.Connection) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        WITH next_job AS (
            SELECT id
            FROM source_jobs
            WHERE status = 'queued'
            ORDER BY created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        ), claimed AS (
            UPDATE source_jobs
            SET status = 'running',
                step_label = 'processing-source',
                started_at = COALESCE(started_at, now()),
                updated_at = now()
            WHERE id IN (SELECT id FROM next_job)
            RETURNING *
        )
        SELECT * FROM claimed
        """
    )
    if row is None:
        return None

    await conn.execute(
        """
        UPDATE source_job_items
        SET status = 'running',
            step_label = 'processing-source',
            started_at = COALESCE(started_at, now()),
            updated_at = now()
        WHERE job_id = $1
        """,
        row["id"],
    )
    return dict(row)


async def update_source_job_step(conn: asyncpg.Connection, job_id: int, step_label: str) -> None:
    await conn.execute(
        """
        UPDATE source_jobs
        SET step_label = $2,
            updated_at = now()
        WHERE id = $1
        """,
        job_id,
        step_label,
    )
    await conn.execute(
        """
        UPDATE source_job_items
        SET step_label = $2,
            updated_at = now()
        WHERE job_id = $1
        """,
        job_id,
        step_label,
    )


async def complete_source_job(
    conn: asyncpg.Connection,
    *,
    job_id: int,
    source_id: int,
    chunks: list[dict[str, Any]],
) -> None:
    async with conn.transaction():
        await conn.execute("DELETE FROM source_chunks WHERE source_id = $1", source_id)
        for chunk in chunks:
            await conn.execute(
                """
                INSERT INTO source_chunks (source_id, job_id, chunk_index, chunk_text, chunk_hash)
                VALUES ($1, $2, $3, $4, $5)
                """,
                source_id,
                job_id,
                chunk["chunk_index"],
                chunk["chunk_text"],
                chunk["chunk_hash"],
            )
        await conn.execute(
            """
            UPDATE source_jobs
            SET status = 'succeeded',
                step_label = 'ingestion-complete',
                finished_at = now(),
                updated_at = now(),
                error_message = NULL
            WHERE id = $1
            """,
            job_id,
        )
        await conn.execute(
            """
            UPDATE source_job_items
            SET status = 'succeeded',
                step_label = 'ingestion-complete',
                finished_at = now(),
                updated_at = now(),
                error_message = NULL
            WHERE job_id = $1
            """,
            job_id,
        )


async def fail_source_job(conn: asyncpg.Connection, *, job_id: int, error_message: str) -> None:
    async with conn.transaction():
        await conn.execute(
            """
            UPDATE source_jobs
            SET status = 'failed',
                step_label = 'ingestion-failed',
                error_message = $2,
                finished_at = now(),
                updated_at = now()
            WHERE id = $1
            """,
            job_id,
            error_message,
        )
        await conn.execute(
            """
            UPDATE source_job_items
            SET status = 'failed',
                step_label = 'ingestion-failed',
                error_message = $2,
                finished_at = now(),
                updated_at = now()
            WHERE job_id = $1
            """,
            job_id,
            error_message,
        )


async def get_source(conn: asyncpg.Connection, source_id: int) -> dict[str, Any] | None:
    rows = await conn.fetch(
        """
        SELECT
            s.*,
            sj.id AS latest_job_id,
            sj.status AS job_status,
            sj.step_label AS job_step_label,
            sj.error_message AS job_error_message,
            sj.started_at AS job_started_at,
            sj.finished_at AS job_finished_at
        FROM sources s
        LEFT JOIN LATERAL (
            SELECT *
            FROM source_jobs
            WHERE source_id = s.id
            ORDER BY created_at DESC, id DESC
            LIMIT 1
        ) sj ON TRUE
        WHERE s.id = $1
        LIMIT 1
        """,
        source_id,
    )
    if not rows:
        return None
    data = dict(rows[0])
    data["metadata"] = normalize_json_value(data.get("metadata"))
    data["status"] = data.get("job_status") or "untracked"
    return data


async def list_source_chunks_for_notebook(conn: asyncpg.Connection, notebook_id: int) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            sc.id,
            sc.source_id,
            sc.chunk_index,
            sc.chunk_text,
            sc.chunk_hash,
            sc.embedding,
            s.title AS source_title,
            s.source_type
        FROM source_chunks sc
        JOIN sources s ON s.id = sc.source_id
        WHERE s.notebook_id = $1
        ORDER BY sc.source_id ASC, sc.chunk_index ASC
        """,
        notebook_id,
    )
    return [dict(row) for row in rows]


async def ensure_source_chunk_embeddings(
    conn: asyncpg.Connection,
    provider_api_key: str,
    chunks: list[dict[str, Any]],
) -> None:
    from .answering import embed_text

    for chunk in chunks:
        if chunk.get("embedding") is not None:
            continue
        embedding = await embed_text(provider_api_key, str(chunk["chunk_text"]))
        await conn.execute(
            """
            UPDATE source_chunks
            SET embedding = $2::vector
            WHERE id = $1
            """,
            chunk["id"],
            "[" + ",".join(f"{float(value):.8f}" for value in embedding) + "]",
        )
        chunk["embedding"] = embedding


async def get_source_detail(conn: asyncpg.Connection, source_id: int) -> dict[str, Any] | None:
    source = await get_source(conn, source_id)
    if source is None:
        return None

    chunks = await conn.fetch(
        """
        SELECT id, source_id, job_id, chunk_index, chunk_text, chunk_hash, created_at
        FROM source_chunks
        WHERE source_id = $1
        ORDER BY chunk_index ASC
        """,
        source_id,
    )
    source["chunks"] = [dict(chunk) for chunk in chunks]
    return source


async def append_run_event(
    conn: asyncpg.Connection,
    run_id: int,
    *,
    event_type: str,
    event_text: str | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        """
        INSERT INTO run_events (run_id, event_type, event_text, payload)
        VALUES ($1, $2, $3, $4::jsonb)
        RETURNING *
        """,
        run_id,
        event_type,
        event_text,
        json.dumps(payload or {}),
    )
    assert row is not None
    return dict(row)


async def list_run_events(
    conn: asyncpg.Connection,
    run_id: int,
    *,
    after_id: int = 0,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT *
        FROM run_events
        WHERE run_id = $1 AND id > $2
        ORDER BY id ASC
        """,
        run_id,
        after_id,
    )
    return [dict(row) for row in rows]


async def enqueue_run_job(conn: asyncpg.Connection, run_id: int) -> dict[str, Any]:
    job = await conn.fetchrow(
        """
        INSERT INTO jobs (kind, entity_type, entity_id, status, step_label, payload)
        VALUES ('run-question', 'run', $1, 'queued', 'queued-for-answering', '{}'::jsonb)
        RETURNING *
        """,
        run_id,
    )
    assert job is not None
    return dict(job)


async def claim_next_run_job(conn: asyncpg.Connection) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        WITH next_job AS (
            SELECT id
            FROM jobs
            WHERE kind = 'run-question' AND status = 'queued'
            ORDER BY created_at ASC, id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        ), claimed AS (
            UPDATE jobs
            SET status = 'running',
                step_label = 'retrieving-relevant-chunks',
                started_at = COALESCE(started_at, now()),
                updated_at = now()
            WHERE id IN (SELECT id FROM next_job)
            RETURNING *
        )
        SELECT * FROM claimed
        """
    )
    if row is None:
        return None
    return dict(row)


async def mark_run_job_running(conn: asyncpg.Connection, job_id: int, *, step_label: str) -> None:
    await conn.execute(
        """
        UPDATE jobs
        SET status = 'running',
            step_label = $2,
            started_at = COALESCE(started_at, now()),
            updated_at = now()
        WHERE id = $1
        """,
        job_id,
        step_label,
    )


async def mark_run_job_succeeded(conn: asyncpg.Connection, job_id: int, *, step_label: str) -> None:
    await conn.execute(
        """
        UPDATE jobs
        SET status = 'succeeded',
            step_label = $2,
            finished_at = now(),
            updated_at = now(),
            error_message = NULL
        WHERE id = $1
        """,
        job_id,
        step_label,
    )


async def mark_run_job_failed(conn: asyncpg.Connection, job_id: int, error_message: str) -> None:
    await conn.execute(
        """
        UPDATE jobs
        SET status = 'failed',
            step_label = 'answer-failed',
            error_message = $2,
            finished_at = now(),
            updated_at = now()
        WHERE id = $1
        """,
        job_id,
        error_message,
    )


async def mark_run_running(conn: asyncpg.Connection, *, run_id: int, step_label: str) -> None:
    await conn.execute(
        """
        UPDATE runs
        SET status = 'running',
            step_label = $2,
            started_at = COALESCE(started_at, now()),
            updated_at = now(),
            blocked_reason = NULL,
            error_message = NULL
        WHERE id = $1
        """,
        run_id,
        step_label,
    )


async def mark_run_blocked(conn: asyncpg.Connection, *, run_id: int, blocked_reason: str) -> None:
    await conn.execute(
        """
        UPDATE runs
        SET status = 'blocked',
            step_label = 'grounding-insufficient',
            blocked_reason = $2,
            finished_at = now(),
            updated_at = now(),
            error_message = NULL
        WHERE id = $1
        """,
        run_id,
        blocked_reason,
    )


async def mark_run_failed(conn: asyncpg.Connection, *, run_id: int, error_message: str) -> None:
    await conn.execute(
        """
        UPDATE runs
        SET status = 'failed',
            step_label = 'answer-failed',
            error_message = $2,
            finished_at = now(),
            updated_at = now()
        WHERE id = $1
        """,
        run_id,
        error_message,
    )


async def mark_run_succeeded(conn: asyncpg.Connection, *, run_id: int, step_label: str = 'answer-complete') -> None:
    await conn.execute(
        """
        UPDATE runs
        SET status = 'succeeded',
            step_label = $2,
            finished_at = now(),
            updated_at = now(),
            blocked_reason = NULL,
            error_message = NULL
        WHERE id = $1
        """,
        run_id,
        step_label,
    )


async def complete_run(
    conn: asyncpg.Connection,
    *,
    run_id: int,
    status: str,
    step_label: str,
    blocked_reason: str | None,
    answer_text: str,
    trace_summary: str | None,
    model: str | None,
    citations: list[dict[str, Any]],
) -> None:
    async with conn.transaction():
        if status == 'blocked':
            await conn.execute(
                """
                UPDATE runs
                SET status = $2,
                    step_label = $3,
                    blocked_reason = $4,
                    finished_at = now(),
                    updated_at = now(),
                    error_message = NULL
                WHERE id = $1
                """,
                run_id,
                status,
                step_label,
                blocked_reason,
            )
        else:
            await conn.execute(
                """
                UPDATE runs
                SET status = $2,
                    step_label = $3,
                    blocked_reason = NULL,
                    finished_at = now(),
                    updated_at = now(),
                    error_message = NULL
                WHERE id = $1
                """,
                run_id,
                status,
                step_label,
            )

        answer = await conn.fetchrow(
            """
            INSERT INTO answers (run_id, answer_text, trace_summary, model)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (run_id)
            DO UPDATE SET answer_text = EXCLUDED.answer_text,
                          trace_summary = EXCLUDED.trace_summary,
                          model = EXCLUDED.model,
                          updated_at = now()
            RETURNING *
            """,
            run_id,
            answer_text,
            trace_summary,
            model,
        )
        assert answer is not None
        await conn.execute("DELETE FROM citations WHERE answer_id = $1", answer["id"])
        for index, citation in enumerate(citations):
            await conn.execute(
                """
                INSERT INTO citations (answer_id, source_id, chunk_ref, citation_text, citation_index)
                VALUES ($1, $2, $3, $4, $5)
                """,
                answer["id"],
                citation["source_id"],
                citation.get("chunk_ref"),
                citation["citation_text"],
                citation.get("citation_index", index),
            )


async def update_source_title(conn: asyncpg.Connection, source_id: int, title: str) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        UPDATE sources
        SET title = $2,
            updated_at = now()
        WHERE id = $1
        RETURNING *
        """,
        source_id,
        title,
    )
    return dict(row) if row is not None else None


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


async def list_recent_jobs(conn: asyncpg.Connection, *, limit: int = 10) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT *
        FROM (
            SELECT
                'source'::text AS job_kind,
                'source'::text AS entity_type,
                sj.id AS job_id,
                sj.source_id AS entity_id,
                s.title AS label,
                sj.status,
                sj.step_label,
                sj.error_message,
                sj.created_at,
                sj.started_at,
                sj.finished_at
            FROM source_jobs sj
            JOIN sources s ON s.id = sj.source_id
            UNION ALL
            SELECT
                'run'::text AS job_kind,
                'run'::text AS entity_type,
                r.id AS job_id,
                r.id AS entity_id,
                r.question AS label,
                r.status,
                r.step_label,
                r.error_message,
                r.created_at,
                r.started_at,
                r.finished_at
            FROM runs r
        ) items
        ORDER BY created_at DESC, job_id DESC
        LIMIT $1
        """,
        limit,
    )
    return [dict(row) for row in rows]


async def list_recent_failures(conn: asyncpg.Connection, *, limit: int = 10) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT *
        FROM (
            SELECT
                'source'::text AS job_kind,
                'source'::text AS entity_type,
                sj.id AS job_id,
                sj.source_id AS entity_id,
                s.title AS label,
                sj.status,
                sj.step_label,
                sj.error_message,
                sj.created_at,
                sj.started_at,
                sj.finished_at
            FROM source_jobs sj
            JOIN sources s ON s.id = sj.source_id
            WHERE sj.status = 'failed'
            UNION ALL
            SELECT
                'run'::text AS job_kind,
                'run'::text AS entity_type,
                r.id AS job_id,
                r.id AS entity_id,
                r.question AS label,
                r.status,
                r.step_label,
                r.error_message,
                r.created_at,
                r.started_at,
                r.finished_at
            FROM runs r
            WHERE r.status IN ('failed', 'blocked')
        ) items
        ORDER BY created_at DESC, job_id DESC
        LIMIT $1
        """,
        limit,
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
        if answer_payload is None and payload.get("status", "queued") == "queued":
            await enqueue_run_job(conn, int(run["id"]))
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
