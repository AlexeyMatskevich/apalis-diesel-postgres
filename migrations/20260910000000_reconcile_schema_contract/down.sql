-- Restore the released 0.4.1 generation: its dequeue index predicate and its
-- definitions of `apalis.get_jobs` and `apalis.notify_new_jobs`, as installed
-- by that release rather than as the edited historical migrations now define
-- them, so a database installed by 0.4.1 returns to exactly its own schema.
DROP INDEX IF EXISTS apalis.jobs_dequeue_idx;
CREATE INDEX IF NOT EXISTS jobs_dequeue_idx
    ON apalis.jobs(job_type, priority DESC, run_at ASC, id)
    WHERE status = 'Pending'
       OR (status = 'Failed' AND attempts < max_attempts);

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
