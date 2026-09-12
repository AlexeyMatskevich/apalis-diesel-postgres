-- Repair active rows which an earlier nullable compatibility argument allowed.
-- Each lost active execution consumes one attempt, bounded by max_attempts.
UPDATE apalis.jobs AS jobs
SET attempts = CASE WHEN status IN ('Queued', 'Running')
                    THEN CASE WHEN attempts < max_attempts THEN attempts + 1 ELSE max_attempts END
                    ELSE attempts END,
    status = CASE WHEN status IN ('Queued', 'Running')
                  THEN CASE WHEN attempts < max_attempts - 1 THEN 'Pending' ELSE 'Killed' END
                  ELSE status END,
    last_result = CASE WHEN status IN ('Queued', 'Running')
                       THEN jsonb_build_object('Err', 'Worker ownership was lost during schema upgrade')
                       ELSE last_result END,
    done_at = CASE WHEN status IN ('Queued', 'Running')
                   THEN CASE WHEN attempts < max_attempts - 1 THEN NULL ELSE statement_timestamp() END
                   ELSE done_at END,
    lock_by = NULL,
    lock_at = NULL
WHERE status IN ('Queued', 'Running') AND lock_by IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                   WHERE conrelid = 'apalis.jobs'::regclass
                     AND conname = 'jobs_active_owner_check') THEN
        ALTER TABLE apalis.jobs ADD CONSTRAINT jobs_active_owner_check
            CHECK (status NOT IN ('Queued', 'Running') OR lock_by IS NOT NULL);
    END IF;
END;
$$;

-- Renew heartbeats without waiting for native or compatibility claims.
CREATE OR REPLACE FUNCTION apalis.get_jobs(
    worker_id TEXT,
    v_job_type TEXT,
    v_job_count INTEGER DEFAULT 5
) RETURNS SETOF apalis.jobs AS $$
BEGIN
    IF worker_id IS NULL THEN
        RAISE EXCEPTION 'worker_id must not be null' USING ERRCODE = '22004';
    END IF;

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
