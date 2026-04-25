from __future__ import annotations

import asyncio
import base64
import binascii
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import asyncpg
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
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
    get_source_detail,
    list_run_events,
    get_provider_config,
    get_authenticated_user,
    get_bootstrap_state,
    get_provider_api_key,
    get_run,
    get_source,
    initialize_database,
    queue_source_job,
    list_runs,
    list_sources,
    revoke_session,
    store_source_payload,
    update_provider_config,
    update_source_title,
    validate_gemini_api_key,
)
from .settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class CitationCreate(BaseModel):
    source_id: int
    citation_text: str
    chunk_ref: str | None = None
    citation_index: int | None = None


class ProviderConfigUpdate(BaseModel):
    api_key: str


class SourceCreate(BaseModel):
    notebook_id: int | None = None
    source_type: str = Field(default="text")
    title: str
    source_url: str | None = None
    content_base64: str | None = None
    content_text: str | None = None
    original_name: str | None = None
    mime_type: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourceTitleUpdate(BaseModel):
    title: str


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


def serialize_provider_config(provider_config: dict[str, Any] | None) -> dict[str, Any] | None:
    if provider_config is None:
        return None
    return {
        "provider_name": provider_config["provider_name"],
        "validation_status": provider_config["validation_status"],
        "validated_at": provider_config.get("validated_at"),
        "created_at": provider_config.get("created_at"),
        "updated_at": provider_config.get("updated_at"),
        "api_key_present": bool(provider_config.get("api_key_ciphertext")),
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

    provider_config = state.get("provider_config")
    provider_configured = bool(provider_config and provider_config.get("validation_status") == "valid")
    return {
        "status": "ok",
        "bootstrap_complete": state["bootstrap_complete"],
        "schema_version": state["schema_version"],
        "provider_configured": provider_configured,
    }


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
        "provider_configured": bool(database.get("provider_configured")),
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


@app.get("/provider/config")
async def provider_config(request: Request, _: dict[str, Any] = Depends(require_authentication)) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        config = await get_provider_config(conn)
    return serialize_provider_config(config) or {
        "provider_name": "gemini",
        "validation_status": "unknown",
        "validated_at": None,
        "created_at": None,
        "updated_at": None,
        "api_key_present": False,
    }


@app.put("/provider/config")
async def save_provider_config(
    request: Request,
    payload: ProviderConfigUpdate,
    _: dict[str, Any] = Depends(require_authentication),
) -> dict[str, Any]:
    if not payload.api_key.strip():
        raise HTTPException(status_code=400, detail="API key is required")

    validation_ok, validation_error = await asyncio.to_thread(validate_gemini_api_key, payload.api_key)
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        config = await update_provider_config(
            conn,
            provider_name="gemini",
            api_key=payload.api_key,
            validation_status="valid" if validation_ok else "invalid",
            validated_at=datetime.now(UTC) if validation_ok else None,
        )

    response = serialize_provider_config(config) or {}
    response["validation_message"] = validation_error
    return response


@app.post("/provider/config/test")
async def test_provider_config(
    request: Request,
    _: dict[str, Any] = Depends(require_authentication),
) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        api_key = await get_provider_api_key(conn)
        if not api_key:
            config = await update_provider_config(
                conn,
                provider_name="gemini",
                api_key=None,
                validation_status="invalid",
                validated_at=None,
            )
            return {
                **(serialize_provider_config(config) or {}),
                "validation_message": "No API key configured",
            }
        validation_ok, validation_error = await asyncio.to_thread(validate_gemini_api_key, api_key)
        config = await update_provider_config(
            conn,
            provider_name="gemini",
            api_key=api_key,
            validation_status="valid" if validation_ok else "invalid",
            validated_at=datetime.now(UTC) if validation_ok else None,
        )

    response = serialize_provider_config(config) or {}
    response["validation_message"] = validation_error
    return response


@app.get("/bootstrap/status")
async def bootstrap_status(request: Request, _: dict[str, Any] = Depends(require_authentication)) -> dict[str, Any]:
    state = await fetch_bootstrap_state(request)
    state["owner"] = serialize_user(state.get("owner"))
    state["provider_config"] = serialize_provider_config(state.get("provider_config"))
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


@app.get("/sources/{source_id}")
async def get_source_record(
    request: Request,
    source_id: int,
    _: dict[str, Any] = Depends(require_authentication),
) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        source = await get_source_detail(conn, source_id)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found")
    return source


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

    fallback_text = payload.content_text.strip() if payload.content_text is not None else None
    if payload.content_base64:
        try:
            raw_bytes = base64.b64decode(payload.content_base64, validate=True)
        except (ValueError, binascii.Error):
            raise HTTPException(status_code=400, detail="content_base64 must be valid base64")
        input_kind = "upload"
    elif payload.source_url:
        raw_text = fallback_text or payload.source_url
        raw_bytes = raw_text.encode("utf-8")
        input_kind = "url"
    elif fallback_text:
        raw_bytes = fallback_text.encode("utf-8")
        input_kind = "text"
    else:
        raise HTTPException(status_code=400, detail="Source content is required")

    stored_payload = store_source_payload(
        source_type=payload.source_type,
        data=raw_bytes,
        original_name=payload.original_name,
    )
    metadata = {
        **payload.metadata,
        "input_kind": input_kind,
        "original_name": payload.original_name,
        "mime_type": payload.mime_type,
        "source_url": payload.source_url,
        "has_fallback_text": bool(fallback_text and payload.source_url),
    }

    async with pool.acquire() as conn:
        return await create_source(
            conn,
            {
                "notebook_id": notebook_id,
                "source_type": payload.source_type,
                "title": payload.title,
                **stored_payload,
                "metadata": metadata,
            },
        )


@app.patch("/sources/{source_id}")
async def update_source_record(
    request: Request,
    source_id: int,
    payload: SourceTitleUpdate,
    _: dict[str, Any] = Depends(require_authentication),
) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        source = await update_source_title(conn, source_id, payload.title)
        if source is None:
            raise HTTPException(status_code=404, detail="Source not found")
        return await get_source(conn, source_id) or source


@app.post("/sources/{source_id}/retry")
async def retry_source_record(
    request: Request,
    source_id: int,
    _: dict[str, Any] = Depends(require_authentication),
) -> dict[str, Any]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        source = await get_source(conn, source_id)
        if source is None:
            raise HTTPException(status_code=404, detail="Source not found")
        if source.get("status") != "failed":
            raise HTTPException(status_code=409, detail="Source is not failed")
        await queue_source_job(conn, source_id)
        return await get_source(conn, source_id) or source


@app.get("/runs")
async def get_runs(request: Request, _: dict[str, Any] = Depends(require_authentication)) -> list[dict[str, Any]]:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        return await list_runs(conn)


@app.get("/runs/{run_id}/stream")
async def stream_run_record(
    request: Request,
    run_id: int,
    _: dict[str, Any] = Depends(require_authentication),
) -> StreamingResponse:
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        if await get_run(conn, run_id) is None:
            raise HTTPException(status_code=404, detail="Run not found")

    async def event_stream():
        last_event_id = 0
        emitted_tokens = False
        while True:
            async with pool.acquire() as conn:
                run = await get_run(conn, run_id)
                if run is None:
                    break
                events = await list_run_events(conn, run_id, after_id=last_event_id)
            for event in events:
                last_event_id = event["id"]
                event_text = event.get("event_text") or ""
                if event_text:
                    emitted_tokens = True
                    yield event_text
            if run["status"] in {"succeeded", "failed", "blocked"}:
                if not emitted_tokens:
                    answer = run.get("answer") or {}
                    answer_text = answer.get("answer_text")
                    if answer_text:
                        yield answer_text
                break
            await asyncio.sleep(0.35)

    return StreamingResponse(event_stream(), media_type="text/plain; charset=utf-8")


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
        provider_config = await get_provider_config(conn)
        if payload.answer is None and (provider_config is None or provider_config.get("validation_status") != "valid"):
            raise HTTPException(status_code=409, detail="Gemini provider configuration is not ready")
        return await create_run(conn, run_payload)
