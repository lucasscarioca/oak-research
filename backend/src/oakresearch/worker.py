from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from typing import Any

import asyncpg
from fastapi import FastAPI, Request

from .db import create_pool, get_bootstrap_state, initialize_database
from .ingestion import process_next_source_job_once
from .settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


async def check_worker_database(request: Request) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    try:
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
            state = await get_bootstrap_state(conn)
    except Exception as exc:  # pragma: no cover - surfaced in health response
        return {"status": "degraded", "detail": str(exc)}

    return {
        "status": "ok",
        "bootstrap_complete": state["bootstrap_complete"],
        "schema_version": state["schema_version"],
    }


async def ingestion_loop(app: FastAPI) -> None:
    pool: asyncpg.Pool = app.state.pool
    while True:
        try:
            processed = await process_next_source_job_once(pool)
            if processed is None:
                await asyncio.sleep(2.0)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - resilient background worker loop
            logger.exception("Unhandled ingestion worker failure")
            await asyncio.sleep(2.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting OakResearch worker in %s", settings.environment)
    pool = await create_pool()
    app.state.pool = pool
    async with pool.acquire() as conn:
        app.state.bootstrap_state = await initialize_database(conn)
    worker_task = asyncio.create_task(ingestion_loop(app))
    try:
        yield
    finally:
        worker_task.cancel()
        with suppress(asyncio.CancelledError):
            await worker_task
        await pool.close()
        logger.info("Shutting down OakResearch worker")


app = FastAPI(title="OakResearch Worker", lifespan=lifespan)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "worker", "status": "running"}


@app.get("/health")
async def health(request: Request) -> dict[str, Any]:
    database = await check_worker_database(request)
    overall = "ok" if database["status"] == "ok" else "degraded"
    return {"service": "worker", "status": overall, "database": database}


@app.get("/ready")
async def ready(request: Request) -> dict[str, bool | str | dict[str, Any]]:
    database = await check_worker_database(request)
    return {
        "service": "worker",
        "ready": database["status"] == "ok" and database.get("bootstrap_complete", False),
        "database": database,
    }
