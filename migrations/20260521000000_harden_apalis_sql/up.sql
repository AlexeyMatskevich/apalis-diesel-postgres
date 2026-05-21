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
