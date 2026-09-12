-- Schema shipped in v0.4.1 (commit 8181c4391b8828029c0e1e6e04439f74a671786c).
-- Deliberately independent of current embedded migrations.

-- 00000000000000_create_apalis
CREATE SCHEMA IF NOT EXISTS apalis;

CREATE TABLE IF NOT EXISTS apalis.workers (
    id TEXT NOT NULL,
    worker_type TEXT NOT NULL,
    storage_name TEXT NOT NULL,
    layers TEXT NOT NULL DEFAULT '',
    last_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    started_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id, worker_type)
);

CREATE INDEX IF NOT EXISTS workers_worker_type_last_seen_idx
    ON apalis.workers(worker_type, last_seen DESC);

CREATE TABLE IF NOT EXISTS apalis.jobs (
    job BYTEA NOT NULL,
    id TEXT NOT NULL PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 25,
    run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    last_result JSONB,
    lock_at TIMESTAMP WITH TIME ZONE,
    lock_by TEXT,
    done_at TIMESTAMP WITH TIME ZONE,
    priority INTEGER NOT NULL DEFAULT 0,
    metadata JSONB,
    idempotency_key TEXT,
    CONSTRAINT jobs_status_check
        CHECK (status IN ('Pending', 'Queued', 'Running', 'Done', 'Failed', 'Killed')),
    CONSTRAINT jobs_attempts_check CHECK (attempts >= 0),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts > 0),
    CONSTRAINT jobs_attempts_lte_max_attempts_check CHECK (attempts <= max_attempts),
    CONSTRAINT jobs_priority_check CHECK (priority >= 0),
    CONSTRAINT jobs_lock_by_worker_type_fkey
        FOREIGN KEY (lock_by, job_type) REFERENCES apalis.workers(id, worker_type)
);

CREATE INDEX IF NOT EXISTS jobs_dequeue_idx
    ON apalis.jobs(job_type, priority DESC, run_at ASC, id)
    WHERE status IN ('Pending', 'Failed');
CREATE INDEX IF NOT EXISTS jobs_list_by_queue_idx
    ON apalis.jobs(job_type, status, done_at DESC, run_at DESC);
CREATE INDEX IF NOT EXISTS jobs_list_all_idx
    ON apalis.jobs(status, done_at DESC, run_at DESC);
CREATE INDEX IF NOT EXISTS jobs_locked_by_queue_idx
    ON apalis.jobs(job_type, lock_by)
    WHERE lock_by IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_job_type_run_at_idx
    ON apalis.jobs(job_type, run_at);
CREATE INDEX IF NOT EXISTS jobs_run_at_idx
    ON apalis.jobs(run_at);
CREATE INDEX IF NOT EXISTS jobs_job_type_done_at_idx
    ON apalis.jobs(job_type, done_at)
    WHERE done_at IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
    ON apalis.jobs(job_type, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE OR REPLACE FUNCTION apalis.get_jobs(
    worker_id TEXT,
    v_job_type TEXT,
    v_job_count INTEGER DEFAULT 5
) RETURNS SETOF apalis.jobs AS $$
BEGIN
    RETURN QUERY
    WITH next_jobs AS (
        SELECT id
        FROM apalis.jobs
        WHERE (status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts))
            AND run_at <= now()
            AND job_type = v_job_type
        ORDER BY priority DESC, run_at ASC
        LIMIT v_job_count
        FOR UPDATE SKIP LOCKED
    )
    UPDATE apalis.jobs
    SET status = 'Queued',
        lock_by = worker_id,
        lock_at = date_trunc('second', now())
    FROM next_jobs
    WHERE apalis.jobs.id = next_jobs.id
    RETURNING apalis.jobs.*;
END;
$$ LANGUAGE plpgsql VOLATILE;

DROP TRIGGER IF EXISTS notify_workers ON apalis.jobs;
DROP FUNCTION IF EXISTS apalis.notify_new_jobs;

CREATE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.run_at <= now() THEN
        PERFORM pg_notify(
            'apalis::job::insert',
            json_build_object(
                'job_type', NEW.job_type,
                'id', NEW.id,
                'run_at', NEW.run_at
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_workers
AFTER INSERT ON apalis.jobs
FOR EACH ROW EXECUTE FUNCTION apalis.notify_new_jobs();


-- 20260521000000_harden_apalis_sql
DROP INDEX IF EXISTS apalis.workers_id_idx;
DROP INDEX IF EXISTS apalis.unique_worker_id;
DROP INDEX IF EXISTS apalis.workers_worker_type_idx;
DROP INDEX IF EXISTS apalis.workers_last_seen_idx;
DROP INDEX IF EXISTS apalis.jobs_id_idx;
DROP INDEX IF EXISTS apalis.unique_job_id;
DROP INDEX IF EXISTS apalis.jobs_status_idx;
DROP INDEX IF EXISTS apalis.jobs_lock_by_idx;
DROP INDEX IF EXISTS apalis.jobs_job_type_idx;
DROP INDEX IF EXISTS apalis.idx_jobs_idempotency_key;
-- Drop the legacy `WHERE lock_by IS NOT NULL` form so the strengthened
-- partial index below (with the `status IN ('Running','Queued')` predicate)
-- is created; `CREATE INDEX IF NOT EXISTS` is a no-op if a same-named index
-- with a different predicate is still present.
DROP INDEX IF EXISTS apalis.jobs_locked_by_queue_idx;

ALTER TABLE apalis.jobs DROP CONSTRAINT IF EXISTS jobs_lock_by_fkey;
ALTER TABLE apalis.jobs DROP CONSTRAINT IF EXISTS jobs_lock_by_worker_type_fkey;

UPDATE apalis.jobs AS jobs
SET status = 'Pending',
    lock_by = NULL,
    lock_at = NULL
WHERE lock_by IS NOT NULL
    AND NOT EXISTS (
        SELECT 1
        FROM apalis.workers AS workers
        WHERE workers.id = jobs.lock_by
            AND workers.worker_type = jobs.job_type
    );

UPDATE apalis.jobs
SET lock_at = date_trunc('second', lock_at)
WHERE lock_at IS NOT NULL;

ALTER TABLE apalis.workers DROP CONSTRAINT IF EXISTS workers_pkey CASCADE;
ALTER TABLE apalis.workers
    ADD CONSTRAINT workers_pkey PRIMARY KEY (id, worker_type);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_status_check'
            AND connamespace = 'apalis'::regnamespace
    ) THEN
        ALTER TABLE apalis.jobs
            ADD CONSTRAINT jobs_status_check
            CHECK (status IN ('Pending', 'Queued', 'Running', 'Done', 'Failed', 'Killed'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_attempts_check'
            AND connamespace = 'apalis'::regnamespace
    ) THEN
        ALTER TABLE apalis.jobs
            ADD CONSTRAINT jobs_attempts_check CHECK (attempts >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_max_attempts_check'
            AND connamespace = 'apalis'::regnamespace
    ) THEN
        ALTER TABLE apalis.jobs
            ADD CONSTRAINT jobs_max_attempts_check CHECK (max_attempts > 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_attempts_lte_max_attempts_check'
            AND connamespace = 'apalis'::regnamespace
    ) THEN
        ALTER TABLE apalis.jobs
            ADD CONSTRAINT jobs_attempts_lte_max_attempts_check
            CHECK (attempts <= max_attempts);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'jobs_priority_check'
            AND connamespace = 'apalis'::regnamespace
    ) THEN
        ALTER TABLE apalis.jobs
            ADD CONSTRAINT jobs_priority_check CHECK (priority IS NULL OR priority >= 0);
    END IF;
END $$;

UPDATE apalis.jobs
SET priority = 0
WHERE priority IS NULL;

ALTER TABLE apalis.jobs
    ALTER COLUMN priority SET DEFAULT 0,
    ALTER COLUMN priority SET NOT NULL;

ALTER TABLE apalis.jobs
    ADD CONSTRAINT jobs_lock_by_worker_type_fkey
    FOREIGN KEY (lock_by, job_type) REFERENCES apalis.workers(id, worker_type);

CREATE INDEX IF NOT EXISTS workers_worker_type_last_seen_idx
    ON apalis.workers(worker_type, last_seen DESC);
CREATE INDEX IF NOT EXISTS jobs_dequeue_idx
    ON apalis.jobs(job_type, priority DESC, run_at ASC, id)
    WHERE status IN ('Pending', 'Failed');
CREATE INDEX IF NOT EXISTS jobs_list_by_queue_idx
    ON apalis.jobs(job_type, status, done_at DESC, run_at DESC);
CREATE INDEX IF NOT EXISTS jobs_list_all_idx
    ON apalis.jobs(status, done_at DESC, run_at DESC);
-- `reenqueue_orphaned_blocking` joins `jobs` to `workers` on `(lock_by,
-- job_type)` and filters `status IN ('Running', 'Queued')`. A `WHERE lock_by
-- IS NOT NULL` partial index lets PostgreSQL skip rows without a lock owner,
-- but it still has to re-check every locked row's status. Adding the status
-- predicate to the partial index makes the orphan-recovery scan strictly
-- index-only.
CREATE INDEX IF NOT EXISTS jobs_locked_by_queue_idx
    ON apalis.jobs(job_type, lock_by)
    WHERE lock_by IS NOT NULL
        AND status IN ('Running', 'Queued');
CREATE INDEX IF NOT EXISTS jobs_job_type_run_at_idx
    ON apalis.jobs(job_type, run_at);
CREATE INDEX IF NOT EXISTS jobs_run_at_idx
    ON apalis.jobs(run_at);
CREATE INDEX IF NOT EXISTS jobs_job_type_done_at_idx
    ON apalis.jobs(job_type, done_at)
    WHERE done_at IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
    ON apalis.jobs(job_type, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE OR REPLACE FUNCTION apalis.get_jobs(
    worker_id TEXT,
    v_job_type TEXT,
    v_job_count INTEGER DEFAULT 5
) RETURNS SETOF apalis.jobs AS $$
BEGIN
    RETURN QUERY
    WITH next_jobs AS (
        SELECT id
        FROM apalis.jobs
        WHERE (status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts))
            AND run_at <= now()
            AND job_type = v_job_type
        ORDER BY priority DESC, run_at ASC
        LIMIT v_job_count
        FOR UPDATE SKIP LOCKED
    )
    UPDATE apalis.jobs
    SET status = 'Queued',
        lock_by = worker_id,
        lock_at = date_trunc('second', now())
    FROM next_jobs
    WHERE apalis.jobs.id = next_jobs.id
    RETURNING apalis.jobs.*;
END;
$$ LANGUAGE plpgsql VOLATILE;


-- 20260521000001_statement_level_notify
-- Replace the per-row NOTIFY trigger with a statement-level trigger that emits
-- at most one NOTIFY per (queue, statement) — one bulk `push_tasks` of N jobs
-- previously generated N NOTIFY events (each parsed by every listener); now
-- it generates one NOTIFY per `job_type` with the inserted ids batched in the
-- payload.
--
-- Wire format (forward-compatible): `{job_type, ids: [...]}`. The Rust
-- listener also accepts the legacy `{job_type, id}` form so a partial rollout
-- (one migration applied but not the other, or a third-party tool emitting
-- the old shape) keeps working.

DROP TRIGGER IF EXISTS notify_workers ON apalis.jobs;
DROP FUNCTION IF EXISTS apalis.notify_new_jobs();

-- PostgreSQL caps `pg_notify` payloads at NOTIFY_PAYLOAD_MAX_LENGTH (~8000
-- bytes); exceeding it aborts the whole transaction. A 26-char Ulid plus
-- JSON quoting/comma is ~29 bytes, so we chunk ids into groups of 100
-- (~2.9 KB) before emitting. A bulk `push_tasks` of thousands still emits
-- one NOTIFY per ~100 jobs instead of one per row — a 10–100× reduction.
CREATE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT job_type, json_agg(id) AS ids
        FROM (
            SELECT
                job_type,
                id,
                (row_number() OVER (PARTITION BY job_type ORDER BY id) - 1) / 100
                    AS chunk
            FROM new_jobs
            WHERE run_at <= now()
        ) sub
        GROUP BY job_type, chunk
    LOOP
        PERFORM pg_notify(
            'apalis::job::insert',
            json_build_object(
                'job_type', rec.job_type,
                'ids', rec.ids
            )::text
        );
    END LOOP;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_workers
AFTER INSERT ON apalis.jobs
REFERENCING NEW TABLE AS new_jobs
FOR EACH STATEMENT EXECUTE FUNCTION apalis.notify_new_jobs();


-- 20260521000002_worker_lease_token
-- Per-registration lease token for workers.
--
-- Previously `keep_alive` was gated only on `(id, worker_type)`, so any
-- process that knew the (deterministic) worker id pair could refresh a
-- foreign worker's heartbeat and indefinitely block `reenqueue_orphaned`
-- from reclaiming its jobs. With a per-registration random token, only the
-- process holding the token issued at `register_worker` time can refresh
-- the heartbeat.
--
-- The column is nullable so existing rows (and code paths that have not yet
-- adopted the token API) continue to work; new rows produced by
-- `register_worker_blocking` always populate it.

ALTER TABLE apalis.workers
    ADD COLUMN IF NOT EXISTS lease_token TEXT;


-- 20260521000003_queue_stats_snapshot
-- Materialized snapshot of per-queue statistics.
--
-- `list_queues` and `metrics` scan the full `apalis.jobs` table with 20+
-- `FILTER` aggregates per call (`src/queries/admin.rs`). On busy queues this
-- is O(rows) per dashboard request and an easy DoS vector. Pre-aggregating
-- into a materialized view lets callers query a small fixed-cost table and
-- decide how often the data is refreshed (via `refresh_queue_stats_snapshot`
-- below — exposed through the Rust API).
--
-- The view is created `WITH NO DATA` so the migration is fast and idempotent;
-- the first `REFRESH` populates it. A unique index on `job_type` enables
-- `REFRESH MATERIALIZED VIEW CONCURRENTLY` so refreshes don't block readers.

CREATE MATERIALIZED VIEW IF NOT EXISTS apalis.queue_stats_snapshot AS
SELECT
    job_type,
    COUNT(*) AS total_jobs,
    COUNT(*) FILTER (WHERE status = 'Running') AS running_jobs,
    COUNT(*) FILTER (WHERE status = 'Pending') AS pending_jobs,
    COUNT(*) FILTER (WHERE status = 'Queued') AS queued_jobs,
    COUNT(*) FILTER (WHERE status = 'Failed') AS failed_jobs,
    COUNT(*) FILTER (WHERE status = 'Done') AS done_jobs,
    COUNT(*) FILTER (WHERE status = 'Killed') AS killed_jobs,
    COUNT(*) FILTER (WHERE status IN ('Pending', 'Queued', 'Running')) AS active_jobs,
    COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 hour') AS jobs_past_hour,
    COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '24 hours') AS jobs_past_day,
    COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '7 days') AS jobs_past_7_days,
    MAX(run_at) AS most_recent_run_at,
    MAX(done_at) FILTER (WHERE done_at IS NOT NULL) AS most_recent_done_at,
    now() AS snapshot_at
FROM apalis.jobs
GROUP BY job_type
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS queue_stats_snapshot_job_type_idx
    ON apalis.queue_stats_snapshot (job_type);


-- 20260521000004_dequeue_index_tighten
-- Tighten `jobs_dequeue_idx` so terminal `Failed` rows (attempts at
-- max_attempts) do not pile up at the head of the dequeue index ordering
-- and force `SKIP LOCKED` to walk past them on every fetch. The partial
-- predicate matches the status/attempts filter used by `fetch_next` /
-- `queue_by_id` / `apalis.get_jobs`, so terminal rows are excluded from the
-- index entirely. Note `run_at <= now()` is NOT covered: `run_at` is a sort
-- column here (after the unconstrained `priority DESC`), so it remains a
-- residual filter — future-scheduled rows with a higher priority are still
-- walked and filtered out on every fetch. That trade-off is deliberate: it
-- keeps one index serving both the ordering and the predicate, and
-- delay-heavy/high-priority workloads were not measured to need a second
-- index. Revisit only with workload evidence.
DROP INDEX IF EXISTS apalis.jobs_dequeue_idx;
CREATE INDEX IF NOT EXISTS jobs_dequeue_idx
    ON apalis.jobs(job_type, priority DESC, run_at ASC, id)
    WHERE status = 'Pending'
       OR (status = 'Failed' AND attempts < max_attempts);

-- Pin the `apalis.get_jobs` plpgsql function's search_path so a caller
-- with a hostile `search_path` (per CVE-2018-1058 patterns) cannot
-- shadow `now()`/operators referenced inside the function. The body
-- already uses schema-qualified `apalis.jobs`, so this is defensive
-- hardening only.
CREATE OR REPLACE FUNCTION apalis.get_jobs(
    worker_id TEXT,
    v_job_type TEXT,
    v_job_count INTEGER DEFAULT 5
) RETURNS SETOF apalis.jobs AS $$
BEGIN
    RETURN QUERY
    WITH next_jobs AS (
        SELECT id
        FROM apalis.jobs
        WHERE (status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts))
            AND run_at <= now()
            AND job_type = v_job_type
        ORDER BY priority DESC, run_at ASC
        LIMIT v_job_count
        FOR UPDATE SKIP LOCKED
    )
    UPDATE apalis.jobs
    SET status = 'Queued',
        lock_by = worker_id,
        lock_at = date_trunc('second', now())
    FROM next_jobs
    WHERE apalis.jobs.id = next_jobs.id
    RETURNING apalis.jobs.*;
END;
$$ LANGUAGE plpgsql VOLATILE
   SECURITY INVOKER
   SET search_path = pg_catalog, apalis;


-- 20260521000005_notify_new_jobs_search_path
-- Pin the `apalis.notify_new_jobs` plpgsql function's `search_path` so a
-- caller with a hostile `search_path` (per CVE-2018-1058 patterns) cannot
-- shadow `now()`, `pg_notify()`, `json_agg()`, `json_build_object()`, or the
-- `row_number()` window function referenced inside the body. The function is
-- attached to a statement-level INSERT trigger and runs as the inserting
-- role; `SECURITY INVOKER` is the default but is spelled out here to mirror
-- `apalis.get_jobs` and document the intent. Bodies remain unchanged.
CREATE OR REPLACE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER
SECURITY INVOKER
SET search_path = pg_catalog, apalis
AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN
        SELECT job_type, json_agg(id) AS ids
        FROM (
            SELECT
                job_type,
                id,
                (row_number() OVER (PARTITION BY job_type ORDER BY id) - 1) / 100
                    AS chunk
            FROM new_jobs
            WHERE run_at <= now()
        ) sub
        GROUP BY job_type, chunk
    LOOP
        PERFORM pg_notify(
            'apalis::job::insert',
            json_build_object(
                'job_type', rec.job_type,
                'ids', rec.ids
            )::text
        );
    END LOOP;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;


-- 20260521000006_notify_new_jobs_fast_path
-- Single-row fast-path for `apalis.notify_new_jobs`. The statement-level
-- trigger ran `row_number() OVER (PARTITION BY job_type ORDER BY id)` and
-- `json_agg` against the `new_jobs` transition table on every INSERT — even
-- the overwhelmingly common single-row path (`push_tasks` with one task).
-- Branch on the cardinality of `new_jobs` so a one-row insert pays a single
-- `pg_notify` plus a `COUNT(*)` filter instead of a window + aggregate.
--
-- SECURITY/search_path: this CREATE OR REPLACE re-asserts the same
-- `SECURITY INVOKER, SET search_path = pg_catalog, apalis` hardening as
-- migration `…000005_notify_new_jobs_search_path`. Without it, this REPLACE
-- would silently revert the trigger to the default search_path and undo
-- that earlier defense.

CREATE OR REPLACE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER
SECURITY INVOKER
SET search_path = pg_catalog, apalis
AS $$
DECLARE
    rec RECORD;
    cutoff TIMESTAMPTZ := now();
    single_row apalis.jobs;
    new_jobs_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO new_jobs_count FROM new_jobs WHERE run_at <= cutoff;
    IF new_jobs_count = 0 THEN
        RETURN NULL;
    END IF;

    IF new_jobs_count = 1 THEN
        SELECT * INTO single_row FROM new_jobs WHERE run_at <= cutoff LIMIT 1;
        PERFORM pg_notify(
            'apalis::job::insert',
            json_build_object(
                'job_type', single_row.job_type,
                'ids', json_build_array(single_row.id)
            )::text
        );
        RETURN NULL;
    END IF;

    FOR rec IN
        SELECT job_type, json_agg(id) AS ids
        FROM (
            SELECT
                job_type,
                id,
                (row_number() OVER (PARTITION BY job_type ORDER BY id) - 1) / 100
                    AS chunk
            FROM new_jobs
            WHERE run_at <= cutoff
        ) sub
        GROUP BY job_type, chunk
    LOOP
        PERFORM pg_notify(
            'apalis::job::insert',
            json_build_object(
                'job_type', rec.job_type,
                'ids', rec.ids
            )::text
        );
    END LOOP;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
