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
