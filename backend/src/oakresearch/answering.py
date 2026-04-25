from __future__ import annotations

import asyncio
import json
import logging
import math
import re
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable

import asyncpg
import httpx

from .db import (
    append_run_event,
    claim_next_run_job,
    complete_run,
    ensure_source_chunk_embeddings,
    get_bootstrap_state,
    get_provider_api_key,
    get_run,
    list_source_chunks_for_notebook,
    mark_run_blocked,
    mark_run_failed,
    mark_run_running,
    mark_run_succeeded,
    mark_run_job_failed,
    mark_run_job_running,
    mark_run_job_succeeded,
    normalize_json_value,
)
from .settings import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()
DEFAULT_REQUEST_TIMEOUT = 30.0
DEFAULT_RETRIEVAL_LIMIT = 6
DEFAULT_REFUSAL_MESSAGE = (
    "I don’t have enough grounded evidence in the notebook sources to answer that confidently. "
    "Please add a more relevant source or ask a narrower question."
)


class AnsweringError(RuntimeError):
    pass


@dataclass(slots=True)
class RetrievedChunk:
    id: int
    source_id: int
    source_title: str
    source_type: str
    chunk_index: int
    chunk_text: str
    chunk_hash: str
    score: float
    retrieval_mode: str
    source_ref: str


_WORD_RE = re.compile(r"[A-Za-z0-9_]+")


def _tokenize(text: str) -> set[str]:
    return {token.lower() for token in _WORD_RE.findall(text)}


def _chunk_snippet(text: str, *, limit: int = 180) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 1].rstrip()}…"


def _vector_literal(values: list[float]) -> str:
    return "[" + ",".join(f"{value:.8f}" for value in values) + "]"


async def embed_text(api_key: str, text: str, *, model: str | None = None) -> list[float]:
    normalized = text.strip()
    if not normalized:
        return []

    embedding_model = model or settings.gemini_embedding_model
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{embedding_model}:embedContent"
    payload = {
        "content": {"parts": [{"text": normalized}]},
    }
    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": api_key.strip(),
    }
    async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT) as client:
        response = await client.post(url, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json()
    if data.get("embedding", {}).get("values"):
        return [float(value) for value in data["embedding"]["values"]]
    embeddings = data.get("embeddings") or []
    if embeddings:
        return [float(value) for value in embeddings[0].get("values", [])]
    raise AnsweringError("Embedding response did not include vector values")


async def stream_gemini_text(
    api_key: str,
    *,
    prompt: str,
    model: str | None = None,
) -> AsyncIterator[str]:
    generation_model = model or settings.gemini_model
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{generation_model}:streamGenerateContent?alt=sse"
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt}],
            }
        ],
        "generationConfig": {
            "temperature": 0.2,
            "topP": 0.9,
            "maxOutputTokens": 1024,
        },
    }
    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": api_key.strip(),
    }
    async with httpx.AsyncClient(timeout=DEFAULT_REQUEST_TIMEOUT * 4) as client:
        async with client.stream("POST", url, headers=headers, json=payload) as response:
            response.raise_for_status()
            async for line in response.aiter_lines():
                if not line.startswith("data:"):
                    continue
                raw = line.removeprefix("data:").strip()
                if not raw or raw == "[DONE]":
                    continue
                try:
                    event = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                candidates = event.get("candidates") or []
                if not candidates:
                    continue
                content = candidates[0].get("content") or {}
                parts = content.get("parts") or []
                for part in parts:
                    text = part.get("text")
                    if text:
                        yield text


async def _lexical_rank(question: str, chunks: list[dict[str, Any]], *, limit: int) -> list[RetrievedChunk]:
    question_tokens = _tokenize(question)
    ranked: list[RetrievedChunk] = []
    for chunk in chunks:
        chunk_tokens = _tokenize(str(chunk["chunk_text"]))
        if not chunk_tokens:
            continue
        overlap = len(question_tokens & chunk_tokens)
        score = overlap / math.sqrt(len(chunk_tokens)) if overlap else 0.0
        ranked.append(
            RetrievedChunk(
                id=int(chunk["id"]),
                source_id=int(chunk["source_id"]),
                source_title=str(chunk["source_title"]),
                source_type=str(chunk["source_type"]),
                chunk_index=int(chunk["chunk_index"]),
                chunk_text=str(chunk["chunk_text"]),
                chunk_hash=str(chunk["chunk_hash"]),
                score=score,
                retrieval_mode="lexical",
                source_ref=f"{chunk['source_id']}:{chunk['chunk_index']}",
            )
        )
    ranked.sort(key=lambda item: (-item.score, item.source_id, item.chunk_index))
    return ranked[:limit]


async def retrieve_relevant_chunks(
    conn: asyncpg.Connection,
    *,
    notebook_id: int,
    question: str,
    provider_api_key: str,
    limit: int = DEFAULT_RETRIEVAL_LIMIT,
) -> list[RetrievedChunk]:
    chunks = await list_source_chunks_for_notebook(conn, notebook_id)
    if not chunks:
        return []

    try:
        question_embedding = await embed_text(provider_api_key, question, model=settings.gemini_embedding_model)
    except Exception as exc:
        logger.warning("Falling back to lexical retrieval after question embedding failure: %s", exc)
        return await _lexical_rank(question, chunks, limit=limit)

    try:
        await ensure_source_chunk_embeddings(conn, provider_api_key, chunks)
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
                s.source_type,
                (sc.embedding <=> $2::vector) AS score
            FROM source_chunks sc
            JOIN sources s ON s.id = sc.source_id
            WHERE s.notebook_id = $1
              AND sc.embedding IS NOT NULL
            ORDER BY sc.embedding <=> $2::vector ASC, sc.source_id ASC, sc.chunk_index ASC
            LIMIT $3
            """,
            notebook_id,
            _vector_literal(question_embedding),
            limit,
        )
    except Exception as exc:
        logger.warning("Falling back to lexical retrieval after vector search failure: %s", exc)
        return await _lexical_rank(question, chunks, limit=limit)

    ranked: list[RetrievedChunk] = []
    for row in rows:
        data = dict(row)
        ranked.append(
            RetrievedChunk(
                id=int(data["id"]),
                source_id=int(data["source_id"]),
                source_title=str(data["source_title"]),
                source_type=str(data["source_type"]),
                chunk_index=int(data["chunk_index"]),
                chunk_text=str(data["chunk_text"]),
                chunk_hash=str(data["chunk_hash"]),
                score=float(data["score"]),
                retrieval_mode="vector",
                source_ref=f"{data['source_id']}:{data['chunk_index']}",
            )
        )

    if ranked:
        return ranked
    return await _lexical_rank(question, chunks, limit=limit)


def _grounding_strength(chunks: list[RetrievedChunk]) -> float:
    if not chunks:
        return 0.0
    if chunks[0].retrieval_mode == "vector":
        best_distance = min(chunk.score for chunk in chunks)
        return max(0.0, 1.0 - best_distance)
    return max(chunk.score for chunk in chunks)


def _build_prompt(question: str, chunks: list[RetrievedChunk]) -> str:
    context_lines = []
    for index, chunk in enumerate(chunks, start=1):
        context_lines.append(
            "\n".join(
                [
                    f"[{index}] Source: {chunk.source_title} (ref {chunk.source_ref})",
                    f"Chunk excerpt: {_chunk_snippet(chunk.chunk_text, limit=800)}",
                ]
            )
        )

    context_block = "\n\n".join(context_lines)
    return (
        "You are OakResearch, a grounded research notebook assistant. "
        "Answer using only the notebook context below. "
        "If the context is insufficient, say exactly: "
        f"\"{DEFAULT_REFUSAL_MESSAGE}\". "
        "When you use a fact from a context item, cite it inline with a bracketed number like [1] or [2]. "
        "Do not invent sources or claim certainty without support.\n\n"
        f"Question: {question.strip()}\n\n"
        f"Notebook context:\n{context_block}\n\n"
        "Answer in concise paragraphs and include inline citations."
    )


async def _emit_tokens(
    conn: asyncpg.Connection,
    run_id: int,
    tokens: AsyncIterator[str],
    *,
    on_token: Callable[[str], Awaitable[None]] | None = None,
) -> str:
    parts: list[str] = []
    async for token in tokens:
        if not token:
            continue
        parts.append(token)
        await append_run_event(conn, run_id, event_type="token", event_text=token)
        if on_token is not None:
            await on_token(token)
    return "".join(parts)


async def generate_grounded_answer(
    conn: asyncpg.Connection,
    *,
    run_id: int,
    question: str,
    chunks: list[RetrievedChunk],
    provider_api_key: str,
    on_token: Callable[[str], Awaitable[None]] | None = None,
) -> dict[str, Any]:
    if not chunks:
        refusal = DEFAULT_REFUSAL_MESSAGE
        await append_run_event(conn, run_id, event_type="status", event_text="refused")
        if on_token is not None:
            await on_token(refusal)
        return {
            "answer_text": refusal,
            "trace_summary": "No source chunks were available for retrieval.",
            "citations": [],
            "refused": True,
        }

    prompt = _build_prompt(question, chunks)
    text = await _emit_tokens(conn, run_id, stream_gemini_text(provider_api_key, prompt=prompt), on_token=on_token)
    answer_text = text.strip() or DEFAULT_REFUSAL_MESSAGE

    citation_matches = [int(match) for match in re.findall(r"\[(\d+)\]", answer_text)]
    citations: list[dict[str, Any]] = []
    seen_indices: set[int] = set()
    for citation_index in citation_matches:
        if citation_index in seen_indices:
            continue
        seen_indices.add(citation_index)
        if 1 <= citation_index <= len(chunks):
            chunk = chunks[citation_index - 1]
            citations.append(
                {
                    "source_id": chunk.source_id,
                    "chunk_ref": chunk.source_ref,
                    "citation_text": _chunk_snippet(chunk.chunk_text, limit=220),
                    "citation_index": citation_index - 1,
                }
            )

    if not citations and answer_text != DEFAULT_REFUSAL_MESSAGE:
        best_chunk = chunks[0]
        citations.append(
            {
                "source_id": best_chunk.source_id,
                "chunk_ref": best_chunk.source_ref,
                "citation_text": _chunk_snippet(best_chunk.chunk_text, limit=220),
                "citation_index": 0,
            }
        )

    trace_summary = "; ".join(
        [
            f"Retrieved {len(chunks)} chunk(s)",
            "Sources: " + ", ".join(dict.fromkeys(chunk.source_title for chunk in chunks[:3])),
        ]
    )
    refused = answer_text == DEFAULT_REFUSAL_MESSAGE
    return {
        "answer_text": answer_text,
        "trace_summary": trace_summary,
        "citations": citations,
        "refused": refused,
    }


async def process_next_run_job_once(pool: asyncpg.Pool) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        job = await claim_next_run_job(conn)
        if job is None:
            return None
        run = await get_run(conn, int(job["entity_id"]))
        if run is None:
            await mark_run_job_failed(conn, int(job["id"]), "Run no longer exists")
            return {"job_id": job["id"], "status": "failed"}

        try:
            provider_api_key = await get_provider_api_key(conn)
            if not provider_api_key:
                await mark_run_blocked(
                    conn,
                    run_id=int(run["id"]),
                    blocked_reason="Gemini provider key is not configured",
                )
                await mark_run_job_failed(conn, int(job["id"]), "Gemini provider key is not configured")
                return {"job_id": job["id"], "run_id": run["id"], "status": "blocked"}

            await mark_run_running(conn, run_id=int(run["id"]), step_label="retrieving-relevant-chunks")
            await mark_run_job_running(conn, int(job["id"]), step_label="retrieving-relevant-chunks")
            chunks = await retrieve_relevant_chunks(
                conn,
                notebook_id=int(run["notebook_id"]),
                question=str(run["question"]),
                provider_api_key=provider_api_key,
            )

            if _grounding_strength(chunks) < 0.15:
                refusal = DEFAULT_REFUSAL_MESSAGE
                await append_run_event(conn, int(run["id"]), event_type="status", event_text="blocked")
                await complete_run(
                    conn,
                    run_id=int(run["id"]),
                    status="blocked",
                    step_label="grounding-insufficient",
                    blocked_reason="Insufficient grounding in notebook sources",
                    answer_text=refusal,
                    trace_summary="Grounding was insufficient; refused to answer.",
                    model=settings.gemini_model,
                    citations=[],
                )
                await mark_run_job_succeeded(conn, int(job["id"]), step_label="grounding-insufficient")
                return {"job_id": job["id"], "run_id": run["id"], "status": "blocked"}

            await mark_run_running(conn, run_id=int(run["id"]), step_label="generating-answer")
            await mark_run_job_running(conn, int(job["id"]), step_label="generating-answer")

            async def _on_token(token: str) -> None:
                await asyncio.sleep(0)

            answer = await generate_grounded_answer(
                conn,
                run_id=int(run["id"]),
                question=str(run["question"]),
                chunks=chunks,
                provider_api_key=provider_api_key,
                on_token=_on_token,
            )
            await complete_run(
                conn,
                run_id=int(run["id"]),
                status="succeeded" if not answer["refused"] else "blocked",
                step_label="answer-complete" if not answer["refused"] else "grounding-insufficient",
                blocked_reason=None if not answer["refused"] else "Insufficient grounding in notebook sources",
                answer_text=answer["answer_text"],
                trace_summary=answer["trace_summary"],
                model=settings.gemini_model,
                citations=answer["citations"],
            )
            await mark_run_job_succeeded(conn, int(job["id"]), step_label="answer-complete")
            return {
                "job_id": job["id"],
                "run_id": run["id"],
                "status": "succeeded" if not answer["refused"] else "blocked",
            }
        except Exception as exc:
            logger.exception("Unhandled run worker failure")
            await mark_run_failed(conn, run_id=int(run["id"]), error_message=str(exc))
            await mark_run_job_failed(conn, int(job["id"]), str(exc))
            return {"job_id": job["id"], "run_id": run["id"], "status": "failed", "error": str(exc)}
