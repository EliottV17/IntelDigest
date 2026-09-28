CREATE TABLE IF NOT EXISTS jobs (
    id         UUID PRIMARY KEY,
    url        TEXT        NOT NULL,
    status     TEXT        NOT NULL DEFAULT 'pending',
    result     JSONB,
    error      TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
