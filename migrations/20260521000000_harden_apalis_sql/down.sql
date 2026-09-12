-- Downgrade cannot represent the same worker id in multiple queues.
-- Refuse before DDL instead of silently deleting registrations.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM apalis.workers GROUP BY id HAVING count(*) > 1) THEN
        RAISE EXCEPTION 'cannot downgrade: worker ids occur in multiple queues; resolve registrations explicitly first';
    END IF;
END $$;

ALTER TABLE apalis.jobs DROP CONSTRAINT IF EXISTS jobs_lock_by_worker_type_fkey;
ALTER TABLE apalis.jobs DROP CONSTRAINT IF EXISTS jobs_status_check;
ALTER TABLE apalis.jobs DROP CONSTRAINT IF EXISTS jobs_attempts_check;
ALTER TABLE apalis.jobs DROP CONSTRAINT IF EXISTS jobs_max_attempts_check;
ALTER TABLE apalis.jobs DROP CONSTRAINT IF EXISTS jobs_attempts_lte_max_attempts_check;
ALTER TABLE apalis.jobs DROP CONSTRAINT IF EXISTS jobs_priority_check;

DROP INDEX IF EXISTS apalis.workers_worker_type_last_seen_idx;
DROP INDEX IF EXISTS apalis.jobs_dequeue_idx;
DROP INDEX IF EXISTS apalis.jobs_list_by_queue_idx;
DROP INDEX IF EXISTS apalis.jobs_list_all_idx;
DROP INDEX IF EXISTS apalis.jobs_locked_by_queue_idx;
DROP INDEX IF EXISTS apalis.jobs_job_type_run_at_idx;
DROP INDEX IF EXISTS apalis.jobs_run_at_idx;
DROP INDEX IF EXISTS apalis.jobs_job_type_done_at_idx;
DROP INDEX IF EXISTS apalis.idx_jobs_idempotency_key;

ALTER TABLE apalis.jobs ALTER COLUMN priority DROP NOT NULL;

ALTER TABLE apalis.workers DROP CONSTRAINT IF EXISTS workers_pkey;
ALTER TABLE apalis.workers
    ADD CONSTRAINT workers_pkey PRIMARY KEY (id);

ALTER TABLE apalis.jobs
    ADD CONSTRAINT jobs_lock_by_fkey
    FOREIGN KEY (lock_by) REFERENCES apalis.workers(id);

CREATE INDEX IF NOT EXISTS workers_id_idx ON apalis.workers(id);
CREATE UNIQUE INDEX IF NOT EXISTS unique_worker_id ON apalis.workers(id);
CREATE INDEX IF NOT EXISTS workers_worker_type_idx ON apalis.workers(worker_type);
CREATE INDEX IF NOT EXISTS workers_last_seen_idx ON apalis.workers(last_seen);
CREATE INDEX IF NOT EXISTS jobs_id_idx ON apalis.jobs(id);
CREATE UNIQUE INDEX IF NOT EXISTS unique_job_id ON apalis.jobs(id);
CREATE INDEX IF NOT EXISTS jobs_status_idx ON apalis.jobs(status);
CREATE INDEX IF NOT EXISTS jobs_lock_by_idx ON apalis.jobs(lock_by);
CREATE INDEX IF NOT EXISTS jobs_job_type_idx ON apalis.jobs(job_type);
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
    ON apalis.jobs(job_type, idempotency_key);

CREATE OR REPLACE FUNCTION apalis.get_jobs(
    worker_id TEXT,
    v_job_type TEXT,
    v_job_count INTEGER DEFAULT 5
) RETURNS SETOF apalis.jobs AS $$
BEGIN
    -- Match native claim/recovery ordering even for this trusted, token-free API.
    -- The FK's implicit worker KEY SHARE would otherwise happen after job locks.
    PERFORM 1 FROM apalis.workers AS worker
    WHERE worker.id = worker_id AND worker.worker_type = v_job_type
    FOR SHARE;

    RETURN QUERY
    -- Keep the limit on one candidate set, even when the UPDATE plan rescans it.
    WITH candidates AS MATERIALIZED (
        SELECT id
        FROM apalis.jobs
        WHERE (status IN ('Pending', 'Failed') AND attempts < max_attempts)
            AND run_at <= statement_timestamp()
            AND job_type = v_job_type
        ORDER BY priority DESC, run_at ASC
        LIMIT v_job_count
        FOR UPDATE SKIP LOCKED
    )
    UPDATE apalis.jobs AS job
    SET status = 'Queued',
        lock_by = worker_id,
        lock_at = date_trunc('second', statement_timestamp()),
        done_at = NULL
    FROM candidates
    WHERE job.id = candidates.id
    RETURNING job.*;
END;
$$ LANGUAGE plpgsql VOLATILE
   SECURITY INVOKER
   SET search_path = pg_catalog, apalis, pg_temp;
