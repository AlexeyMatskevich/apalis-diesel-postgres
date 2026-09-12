DROP INDEX IF EXISTS apalis.jobs_dequeue_idx;
CREATE INDEX IF NOT EXISTS jobs_dequeue_idx
    ON apalis.jobs(job_type, priority DESC, run_at ASC, id)
    WHERE status IN ('Pending', 'Failed');

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
    WITH next_jobs AS (
        SELECT id
        FROM apalis.jobs
        WHERE (status IN ('Pending', 'Failed') AND attempts < max_attempts)
            AND run_at <= statement_timestamp()
            AND job_type = v_job_type
        ORDER BY priority DESC, run_at ASC
        LIMIT v_job_count
        FOR UPDATE SKIP LOCKED
    )
    UPDATE apalis.jobs
    SET status = 'Queued',
        lock_by = worker_id,
        lock_at = date_trunc('second', statement_timestamp()),
        done_at = NULL
    FROM next_jobs
    WHERE apalis.jobs.id = next_jobs.id
    RETURNING apalis.jobs.*;
END;
$$ LANGUAGE plpgsql VOLATILE
   SECURITY INVOKER
   SET search_path = pg_catalog, apalis, pg_temp;
