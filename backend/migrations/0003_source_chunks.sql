-- OakResearch source chunk storage for ingestion

CREATE TABLE IF NOT EXISTS source_chunks (
    id bigserial PRIMARY KEY,
    source_id bigint NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    job_id bigint NOT NULL REFERENCES source_jobs(id) ON DELETE CASCADE,
    chunk_index integer NOT NULL,
    chunk_text text NOT NULL,
    chunk_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_id, job_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS source_chunks_source_id_idx
    ON source_chunks (source_id, chunk_index ASC);

CREATE INDEX IF NOT EXISTS source_chunks_job_id_idx
    ON source_chunks (job_id, chunk_index ASC);
