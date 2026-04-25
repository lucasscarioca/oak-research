from __future__ import annotations

import hashlib
import html
import re
from pathlib import Path
from typing import Any

import asyncpg
import httpx
from pypdf import PdfReader

from .db import (
    claim_next_source_job,
    complete_source_job,
    fail_source_job,
    get_source,
    normalize_json_value,
    update_source_job_step,
)

USER_AGENT = "OakResearch/1.0"
DEFAULT_REQUEST_TIMEOUT = 20.0
DEFAULT_CHUNK_SIZE = 1200
DEFAULT_CHUNK_OVERLAP = 160


class IngestionError(RuntimeError):
    pass


def _payload_path(source: dict[str, Any]) -> Path:
    return Path(source["payload_uri"])


def _source_metadata(source: dict[str, Any]) -> dict[str, Any]:
    metadata = normalize_json_value(source.get("metadata"))
    return metadata if isinstance(metadata, dict) else {}


def _normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _html_to_text(document: str) -> str:
    without_scripts = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", document, flags=re.IGNORECASE | re.DOTALL)
    stripped = re.sub(r"<[^>]+>", " ", without_scripts)
    return _normalize_whitespace(html.unescape(stripped))


def _read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise IngestionError(f"Unable to decode text payload at {path}") from exc


def _read_pdf_file(path: Path) -> str:
    try:
        with path.open("rb") as handle:
            reader = PdfReader(handle)
            pages = [page.extract_text() or "" for page in reader.pages]
    except Exception as exc:  # pragma: no cover - library/network edge cases
        raise IngestionError(f"Unable to parse PDF payload at {path}") from exc

    text = "\n\n".join(page.strip() for page in pages if page and page.strip())
    if not text.strip():
        raise IngestionError("PDF did not contain extractable text")
    return text


async def _fetch_url_text(url: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT, follow_redirects=True, headers={"User-Agent": USER_AGENT}) as client:
            response = await client.get(url)
            response.raise_for_status()
    except Exception as exc:
        raise IngestionError(f"Unable to fetch URL content from {url}") from exc

    content_type = response.headers.get("content-type", "").lower()
    body = response.text
    if "html" in content_type or "<html" in body.lower():
        text = _html_to_text(body)
    else:
        text = body.strip()

    if not text:
        raise IngestionError("URL response did not contain extractable text")
    return text


async def extract_source_text(source: dict[str, Any]) -> tuple[str, str]:
    metadata = _source_metadata(source)
    source_type = str(source.get("source_type") or "text")
    input_kind = str(metadata.get("input_kind") or source_type)
    payload_path = _payload_path(source)

    if source_type == "pdf" or input_kind == "upload":
        return _read_pdf_file(payload_path), "parsed-pdf"

    if input_kind == "url":
        if metadata.get("has_fallback_text"):
            return _read_text_file(payload_path), "used-fallback-text"

        source_url = metadata.get("source_url")
        if not source_url:
            source_url = _read_text_file(payload_path)
        return await _fetch_url_text(str(source_url)), "fetched-url"

    return _read_text_file(payload_path), "parsed-text"


def chunk_text(text: str, *, chunk_size: int = DEFAULT_CHUNK_SIZE, overlap: int = DEFAULT_CHUNK_OVERLAP) -> list[str]:
    normalized = re.sub(r"\r\n?", "\n", text).strip()
    if not normalized:
        return []

    paragraphs = [
        _normalize_whitespace(paragraph)
        for paragraph in re.split(r"\n\s*\n", normalized)
        if _normalize_whitespace(paragraph)
    ]
    if not paragraphs:
        return []

    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        if len(paragraph) > chunk_size:
            if current:
                chunks.append(current)
                current = ""
            start = 0
            while start < len(paragraph):
                end = min(len(paragraph), start + chunk_size)
                chunks.append(paragraph[start:end].strip())
                if end >= len(paragraph):
                    break
                start = max(end - overlap, start + 1)
            continue

        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if len(candidate) <= chunk_size:
            current = candidate
            continue

        if current:
            chunks.append(current)
        current = paragraph

    if current:
        chunks.append(current)

    return [chunk for chunk in chunks if chunk]


def chunk_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


async def process_source_payload(source: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    extracted_text, step_label = await extract_source_text(source)
    chunks = chunk_text(extracted_text)
    if not chunks:
        raise IngestionError("Source did not contain any ingestible content")

    chunk_payloads = [
        {
            "chunk_index": index,
            "chunk_text": chunk,
            "chunk_hash": chunk_hash(chunk),
        }
        for index, chunk in enumerate(chunks)
    ]
    return step_label, chunk_payloads


async def process_next_source_job_once(pool: asyncpg.Pool) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        job = await claim_next_source_job(conn)
        if job is None:
            return None
        source = await get_source(conn, job["source_id"])
        if source is None:
            await fail_source_job(conn, job_id=job["id"], error_message="Source no longer exists")
            return {"job_id": job["id"], "status": "failed"}

    try:
        step_label, chunks = await process_source_payload(source)
        async with pool.acquire() as conn:
            await update_source_job_step(conn, job["id"], step_label)
            await complete_source_job(conn, job_id=job["id"], source_id=source["id"], chunks=chunks)
        return {"job_id": job["id"], "source_id": source["id"], "status": "succeeded", "step_label": step_label}
    except Exception as exc:
        async with pool.acquire() as conn:
            await fail_source_job(conn, job_id=job["id"], error_message=str(exc))
        return {"job_id": job["id"], "source_id": source["id"], "status": "failed", "error": str(exc)}
