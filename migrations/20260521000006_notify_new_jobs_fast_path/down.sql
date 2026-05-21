-- Revert to the byte-equivalent body installed by migration
-- `…000005_notify_new_jobs_search_path`. Keep `SET search_path = pg_catalog,
-- apalis` and the `$$ LANGUAGE plpgsql;` trailer so `down`-then-`up` returns
-- the database to the exact state that `000005` left.
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
