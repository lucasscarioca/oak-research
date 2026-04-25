from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import asyncpg
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .db import (
    create_pool,
    create_run,
    create_source,
    get_bootstrap_state,
    get_run,
    initialize_database,
    list_runs,
    list_sources,
)
from .settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class CitationCreate(BaseModel):
    source_id: int
    citation_text: str
    chunk_ref: str | None = None
    citation_index: int | None = None


class SourceCreate(BaseModel):
    notebook_id: int | None = None
    source_type: str = Field(default="text")
    title: str
    payload_uri: str
    payload_sha256: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunAnswerCreate(BaseModel):
    answer_text: str
    trace_summary: str | None = None
    model: str | None = None
    citations: list[CitationCreate] = Field(default_factory=list)


class RunCreate(BaseModel):
    notebook_id: int | None = None
    question: str
    status: str = Field(default="queued")
    step_label: str | None = None
    blocked_reason: str | None = None
    error_message: str | None = None
    rerun_of_run_id: int | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    answer: RunAnswerCreate | None = None


async def fetch_bootstrap_state(request: Request) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        return await get_bootstrap_state(conn)


async def check_database(request: Request) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    try:
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
            state = await get_bootstrap_state(conn)
    except Exception as exc:  # pragma: no cover - surfaced in health response
        return {"status": "degraded", "detail": str(exc)}

    return {"status": "ok", "bootstrap_complete": state["bootstrap_complete"], "schema_version": state["schema_version"]}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting %s API in %s", settings.app_name, settings.environment)
    pool = await create_pool()
    app.state.pool = pool
    async with pool.acquire() as conn:
        app.state.bootstrap_state = await initialize_database(conn)
    yield
    await pool.close()
    logger.info("Shutting down %s API", settings.app_name)


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "api", "name": settings.app_name, "status": "running"}


@app.get("/health")
async def health(request: Request) -> dict[str, object]:
    database = await check_database(request)
    overall = "ok" if database["status"] == "ok" else "degraded"
    return {
        "service": "api",
        "status": overall,
        "environment": settings.environment,
        "database": database,
        "provider_configured": bool(settings.gemini_api_key.strip()),
    }


@app.get("/ready")
async def ready(request: Request) -> dict[str, object]:
    database = await check_database(request)
    bootstrap_complete = database.get("bootstrap_complete", False)
    return {
        "service": "api",
        "ready": database["status"] == "ok" and bootstrap_complete,
        "database": database,
    }


@app.get("/bootstrap/status")
async def bootstrap_status(request: Request) -> dict[str, Any]:
    return await fetch_bootstrap_state(request)


@app.get("/owner")
async def owner(request: Request) -> dict[str, Any]:
    state = await fetch_bootstrap_state(request)
    if state["owner"] is None:
        raise HTTPException(status_code=404, detail="Owner not initialized")
    return state["owner"]


@app.get("/notebooks/default")
async def default_notebook(request: Request) -> dict[str, Any]:
    state = await fetch_bootstrap_state(request)
    if state["default_notebook"] is None:
        raise HTTPException(status_code=404, detail="Default notebook not initialized")
    return state["default_notebook"]


@app.get("/sources")
async def get_sources(request: Request) -> list[dict[str, Any]]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        return await list_sources(conn)


@app.post("/sources")
async def create_source_record(request: Request, payload: SourceCreate) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    state = await fetch_bootstrap_state(request)
    notebook_id = payload.notebook_id or (state["default_notebook"]["id"] if state["default_notebook"] else None)
    if notebook_id is None:
        raise HTTPException(status_code=409, detail="Default notebook is not available")

    async with pool.acquire() as conn:
        return await create_source(
            conn,
            {
                "notebook_id": notebook_id,
                "source_type": payload.source_type,
                "title": payload.title,
                "payload_uri": payload.payload_uri,
                "payload_sha256": payload.payload_sha256,
                "metadata": payload.metadata,
            },
        )


@app.get("/runs")
async def get_runs(request: Request) -> list[dict[str, Any]]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        return await list_runs(conn)


@app.get("/runs/{run_id}")
async def get_run_record(request: Request, run_id: int) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        run = await get_run(conn, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@app.post("/runs")
async def create_run_record(request: Request, payload: RunCreate) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    state = await fetch_bootstrap_state(request)
    notebook_id = payload.notebook_id or (state["default_notebook"]["id"] if state["default_notebook"] else None)
    if notebook_id is None:
        raise HTTPException(status_code=409, detail="Default notebook is not available")

    run_payload: dict[str, Any] = {
        "notebook_id": notebook_id,
        "question": payload.question,
        "status": payload.status,
        "step_label": payload.step_label,
        "blocked_reason": payload.blocked_reason,
        "error_message": payload.error_message,
        "rerun_of_run_id": payload.rerun_of_run_id,
        "started_at": payload.started_at,
        "finished_at": payload.finished_at,
    }
    if payload.answer is not None:
        run_payload["answer"] = {
            "answer_text": payload.answer.answer_text,
            "trace_summary": payload.answer.trace_summary,
            "model": payload.answer.model,
            "citations": [citation.model_dump() for citation in payload.answer.citations],
        }

    async with pool.acquire() as conn:
        return await create_run(conn, run_payload)
