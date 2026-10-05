CREATE TABLE IF NOT EXISTS jobs (
    id uuid PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    fingerprint text NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    state text NOT NULL DEFAULT 'queued' CHECK (state IN ('queued', 'running', 'succeeded', 'dead')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 10),
    run_at timestamptz NOT NULL DEFAULT now(),
    lease_token uuid,
    lease_until timestamptz,
    last_error text,
    result jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((state = 'running') = (lease_token IS NOT NULL AND lease_until IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs (run_at, created_at, id) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS jobs_expired ON jobs (lease_until) WHERE state = 'running';

-- The demo's durable side effect shares the database. Its primary key makes a
-- replay safe even if a worker crashes after writing it but before acknowledging.
CREATE TABLE IF NOT EXISTS report_results (
    job_id uuid PRIMARY KEY REFERENCES jobs(id),
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
