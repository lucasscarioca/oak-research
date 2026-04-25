-- OakResearch initial persistence schema

CREATE TABLE IF NOT EXISTS schema_migrations (
    version text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS users (
    id bigserial PRIMARY KEY,
    username text NOT NULL UNIQUE,
    password_hash text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS notebooks (
    id bigserial PRIMARY KEY,
    owner_user_id bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name text NOT NULL,
    is_default boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (owner_user_id, name)
);

CREATE UNIQUE INDEX IF NOT EXISTS notebooks_one_default_per_owner_idx
    ON notebooks (owner_user_id)
    WHERE is_default;

CREATE TABLE IF NOT EXISTS provider_configs (
    id integer PRIMARY KEY,
    provider_name text NOT NULL,
    api_key_ciphertext text,
    validation_status text NOT NULL DEFAULT 'unknown',
    validated_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT provider_configs_singleton CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS app_instance (
    id integer PRIMARY KEY,
    owner_user_id bigint NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    default_notebook_id bigint NOT NULL REFERENCES notebooks(id) ON DELETE RESTRICT,
    bootstrap_version integer NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT app_instance_singleton CHECK (id = 1)
);

CREATE TABLE IF NOT EXISTS sources (
    id bigserial PRIMARY KEY,
    notebook_id bigint NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    source_type text NOT NULL,
    title text NOT NULL,
    payload_uri text NOT NULL,
    payload_sha256 text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_jobs (
    id bigserial PRIMARY KEY,
    source_id bigint NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    status text NOT NULL,
    step_label text,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT source_jobs_status_check CHECK (status IN ('queued', 'running', 'succeeded', 'failed'))
);

CREATE TABLE IF NOT EXISTS source_job_items (
    id bigserial PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES source_jobs(id) ON DELETE CASCADE,
    item_index integer NOT NULL,
    status text NOT NULL,
    step_label text,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT source_job_items_status_check CHECK (status IN ('queued', 'running', 'succeeded', 'failed'))
);

CREATE TABLE IF NOT EXISTS jobs (
    id bigserial PRIMARY KEY,
    kind text NOT NULL,
    entity_type text NOT NULL,
    entity_id bigint,
    status text NOT NULL,
    step_label text,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT jobs_status_check CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'blocked'))
);

CREATE TABLE IF NOT EXISTS runs (
    id bigserial PRIMARY KEY,
    notebook_id bigint NOT NULL REFERENCES notebooks(id) ON DELETE CASCADE,
    question text NOT NULL,
    status text NOT NULL,
    step_label text,
    blocked_reason text,
    error_message text,
    rerun_of_run_id bigint REFERENCES runs(id) ON DELETE SET NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    started_at timestamptz,
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT runs_status_check CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'blocked'))
);

CREATE TABLE IF NOT EXISTS answers (
    id bigserial PRIMARY KEY,
    run_id bigint NOT NULL UNIQUE REFERENCES runs(id) ON DELETE CASCADE,
    answer_text text NOT NULL,
    trace_summary text,
    model text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS citations (
    id bigserial PRIMARY KEY,
    answer_id bigint NOT NULL REFERENCES answers(id) ON DELETE CASCADE,
    source_id bigint NOT NULL REFERENCES sources(id) ON DELETE RESTRICT,
    chunk_ref text,
    citation_text text NOT NULL,
    citation_index integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sources_notebook_id_idx ON sources (notebook_id, created_at DESC);
CREATE INDEX IF NOT EXISTS source_jobs_source_id_idx ON source_jobs (source_id, created_at DESC);
CREATE INDEX IF NOT EXISTS source_job_items_job_id_idx ON source_job_items (job_id, item_index ASC);
CREATE INDEX IF NOT EXISTS jobs_entity_idx ON jobs (entity_type, entity_id, created_at DESC);
CREATE INDEX IF NOT EXISTS runs_notebook_id_idx ON runs (notebook_id, created_at DESC);
CREATE INDEX IF NOT EXISTS citations_answer_id_idx ON citations (answer_id, citation_index ASC);
