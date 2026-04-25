-- OakResearch answering pipeline storage

CREATE EXTENSION IF NOT EXISTS vector;

ALTER TABLE source_chunks
    ADD COLUMN IF NOT EXISTS embedding vector(768);

CREATE INDEX IF NOT EXISTS source_chunks_embedding_idx
    ON source_chunks USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE TABLE IF NOT EXISTS run_events (
    id bigserial PRIMARY KEY,
    run_id bigint NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    event_type text NOT NULL,
    event_text text,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS run_events_run_id_idx
    ON run_events (run_id, id ASC);
