-- Renew heartbeats without waiting for native or compatibility claims.
CREATE OR REPLACE FUNCTION apalis.get_jobs(
    worker_id TEXT,
    v_job_type TEXT,
    v_job_count INTEGER DEFAULT 5
) RETURNS SETOF apalis.jobs AS $$
BEGIN
    -- Preserve worker-before-job ordering and fence takeover/deletion,
    -- while permitting the independent last_seen heartbeat UPDATE.
    PERFORM 1 FROM apalis.workers AS worker
    WHERE worker.id = worker_id AND worker.worker_type = v_job_type
    FOR KEY SHARE;

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
