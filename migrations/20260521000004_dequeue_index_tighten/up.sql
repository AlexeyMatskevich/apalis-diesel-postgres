-- Tighten `jobs_dequeue_idx` so terminal `Failed` rows (attempts at
-- max_attempts) do not pile up at the head of the dequeue index ordering
-- and force `SKIP LOCKED` to walk past them on every fetch. The new
-- predicate exactly matches the WHERE clause used by `fetch_next` /
-- `queue_by_id` / `apalis.get_jobs`, so the planner can do a pure index
-- scan with no residual filter on the dequeue path.
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
