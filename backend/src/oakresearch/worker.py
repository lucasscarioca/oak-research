from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting OakResearch worker in %s", settings.environment)
    yield
    logger.info("Shutting down OakResearch worker")


app = FastAPI(title="OakResearch Worker", lifespan=lifespan)


@app.get("/")
async def root() -> dict[str, str]:
    return {"service": "worker", "status": "running"}


@app.get("/health")
async def health() -> dict[str, str]:
    return {"service": "worker", "status": "ok"}


@app.get("/ready")
async def ready() -> dict[str, bool | str]:
    return {"service": "worker", "ready": True}
