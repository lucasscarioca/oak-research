-- OakResearch authentication/session schema

ALTER TABLE app_instance
    ADD COLUMN IF NOT EXISTS onboarding_complete boolean NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS auth_sessions (
    id bigserial PRIMARY KEY,
    user_id bigint NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_token_hash text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    last_seen_at timestamptz,
    user_agent text,
    ip_address text
);

CREATE INDEX IF NOT EXISTS auth_sessions_user_id_idx
    ON auth_sessions (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS auth_sessions_expires_at_idx
    ON auth_sessions (expires_at ASC);
