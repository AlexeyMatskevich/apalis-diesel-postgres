-- Reconcile deployed schemas without replaying historical table/key DDL.
-- Never infer or reconstruct data lost by an older migration.
DROP INDEX IF EXISTS apalis.jobs_dequeue_idx;
CREATE INDEX jobs_dequeue_idx ON apalis.jobs(job_type, priority DESC, run_at, id)
    WHERE status IN ('Pending', 'Failed') AND attempts < max_attempts;

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

-- Single-row fast-path for `apalis.notify_new_jobs`. The statement-level
-- trigger ran `row_number() OVER (PARTITION BY job_type ORDER BY id)` and
-- `json_agg` against the `new_jobs` transition table on every INSERT — even
-- the overwhelmingly common single-row path (`push_tasks` with one task).
-- Branch on the cardinality of `new_jobs` so a one-row insert pays a single
-- `pg_notify` plus a `COUNT(*)` filter instead of a window + aggregate.
--
-- SECURITY/search_path: this CREATE OR REPLACE re-asserts the same
-- `SECURITY INVOKER, SET search_path = pg_catalog, apalis, pg_temp` hardening
-- as migration `…000005_notify_new_jobs_search_path`. Without it, this REPLACE
-- would silently revert the trigger to the default search_path and undo
-- that earlier defense.

CREATE OR REPLACE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER
SECURITY INVOKER
SET search_path = pg_catalog, apalis, pg_temp
AS $$
DECLARE
    rec RECORD;
    cutoff TIMESTAMPTZ := statement_timestamp();
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
