from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


async def check_database() -> dict[str, str]:
    try:
        conn = await asyncpg.connect(settings.database_url, timeout=5)
    except Exception as exc:  # pragma: no cover - surfaced in health response
        return {"status": "degraded", "detail": str(exc)}

    try:
        await conn.execute("SELECT 1")
    finally:
        await conn.close()

    return {"status": "ok"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting %s API in %s", settings.app_name, settings.environment)
    yield
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
async def health() -> dict[str, object]:
    database = await check_database()
    overall = "ok" if database["status"] == "ok" else "degraded"
    return {
        "service": "api",
        "status": overall,
        "environment": settings.environment,
        "database": database,
        "provider_configured": bool(settings.gemini_api_key.strip()),
    }


@app.get("/ready")
async def ready() -> dict[str, object]:
    database = await check_database()
    return {
        "service": "api",
        "ready": database["status"] == "ok",
        "database": database,
    }
