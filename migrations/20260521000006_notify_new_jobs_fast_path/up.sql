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
