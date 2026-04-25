from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .db import (
    DEFAULT_SESSION_COOKIE_NAME,
    authenticate_user,
    complete_onboarding,
    create_pool,
    create_run,
    create_session,
    create_source,
    get_authenticated_user,
    get_bootstrap_state,
    get_run,
    initialize_database,
    list_runs,
    list_sources,
    revoke_session,
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


class AuthCredentials(BaseModel):
    username: str
    password: str


class OnboardingCreate(AuthCredentials):
    confirm_password: str


async def fetch_bootstrap_state(request: Request) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        return await get_bootstrap_state(conn)


def serialize_user(user: dict[str, Any] | None) -> dict[str, Any] | None:
    if user is None:
        return None
    return {
        "id": user["id"],
        "username": user["username"],
        "created_at": user.get("created_at"),
    }


async def current_user(request: Request) -> dict[str, Any] | None:
    pool: asyncpg.Pool = request.app.state.pool
    token = request.cookies.get(DEFAULT_SESSION_COOKIE_NAME)
    async with pool.acquire() as conn:
        return await get_authenticated_user(conn, token)


async def require_authentication(request: Request) -> dict[str, Any]:
    user = await current_user(request)
    if user is None:
        raise HTTPException(status_code=401, detail="Authentication required")
    return user


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
    allow_origins=[settings.web_origin],
    allow_credentials=True,
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


@app.get("/auth/status")
async def auth_status(request: Request) -> dict[str, Any]:
    state = await fetch_bootstrap_state(request)
    user = await current_user(request)
    return {
        "onboarding_required": not state.get("onboarding_complete", False),
        "authenticated": user is not None,
        "user": serialize_user(user),
    }


@app.post("/auth/onboarding")
async def onboarding(request: Request, response: Response, payload: OnboardingCreate) -> dict[str, Any]:
    state = await fetch_bootstrap_state(request)
    if state.get("onboarding_complete", False):
        raise HTTPException(status_code=409, detail="Onboarding already completed")
    if payload.password != payload.confirm_password:
        raise HTTPException(status_code=400, detail="Passwords do not match")

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        result = await complete_onboarding(conn, username=payload.username, password=payload.password)
        token = await create_session(
            conn,
            result["owner"]["id"],
            user_agent=request.headers.get("user-agent"),
            ip_address=request.client.host if request.client else None,
        )

    response.set_cookie(
        key=DEFAULT_SESSION_COOKIE_NAME,
        value=token,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="lax",
        max_age=int(30 * 24 * 60 * 60),
        path="/",
    )
    return {
        "authenticated": True,
        "user": serialize_user(result["owner"]),
        "onboarding_required": False,
    }


@app.post("/auth/login")
async def login(request: Request, response: Response, payload: AuthCredentials) -> dict[str, Any]:
    state = await fetch_bootstrap_state(request)
    if not state.get("onboarding_complete", False):
        raise HTTPException(status_code=409, detail="Onboarding required")

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        user = await authenticate_user(conn, username=payload.username, password=payload.password)
        if user is None:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        token = await create_session(
            conn,
            user["id"],
            user_agent=request.headers.get("user-agent"),
            ip_address=request.client.host if request.client else None,
        )

    response.set_cookie(
        key=DEFAULT_SESSION_COOKIE_NAME,
        value=token,
        httponly=True,
        secure=settings.cookie_secure,
        samesite="lax",
        max_age=int(30 * 24 * 60 * 60),
        path="/",
    )
    return {"authenticated": True, "user": serialize_user(user)}


@app.post("/auth/logout")
async def logout(request: Request, response: Response) -> dict[str, Any]:
    token = request.cookies.get(DEFAULT_SESSION_COOKIE_NAME)
    pool: asyncpg.Pool = request.app.state.pool
    if token:
        async with pool.acquire() as conn:
            await revoke_session(conn, token)
    response.delete_cookie(key=DEFAULT_SESSION_COOKIE_NAME, path="/")
    return {"authenticated": False}


@app.get("/bootstrap/status")
async def bootstrap_status(request: Request, _: dict[str, Any] = Depends(require_authentication)) -> dict[str, Any]:
    state = await fetch_bootstrap_state(request)
    state["owner"] = serialize_user(state.get("owner"))
    provider_config = state.get("provider_config")
    if provider_config is not None:
        state["provider_config"] = {
            "id": provider_config["id"],
            "provider_name": provider_config["provider_name"],
            "validation_status": provider_config["validation_status"],
            "validated_at": provider_config["validated_at"],
            "created_at": provider_config["created_at"],
            "updated_at": provider_config["updated_at"],
        }
    return state


@app.get("/owner")
async def owner(request: Request, _: dict[str, Any] = Depends(require_authentication)) -> dict[str, Any]:
    state = await fetch_bootstrap_state(request)
    owner = serialize_user(state["owner"])
    if owner is None:
        raise HTTPException(status_code=404, detail="Owner not initialized")
    return owner


@app.get("/notebooks/default")
async def default_notebook(request: Request, _: dict[str, Any] = Depends(require_authentication)) -> dict[str, Any]:
    state = await fetch_bootstrap_state(request)
    if state["default_notebook"] is None:
        raise HTTPException(status_code=404, detail="Default notebook not initialized")
    return state["default_notebook"]


@app.get("/sources")
async def get_sources(request: Request, _: dict[str, Any] = Depends(require_authentication)) -> list[dict[str, Any]]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        return await list_sources(conn)


@app.post("/sources")
async def create_source_record(
    request: Request,
    payload: SourceCreate,
    _: dict[str, Any] = Depends(require_authentication),
) -> dict[str, Any]:
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
async def get_runs(request: Request, _: dict[str, Any] = Depends(require_authentication)) -> list[dict[str, Any]]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        return await list_runs(conn)


@app.get("/runs/{run_id}")
async def get_run_record(
    request: Request,
    run_id: int,
    _: dict[str, Any] = Depends(require_authentication),
) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        run = await get_run(conn, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@app.post("/runs")
async def create_run_record(
    request: Request,
    payload: RunCreate,
    _: dict[str, Any] = Depends(require_authentication),
) -> dict[str, Any]:
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
