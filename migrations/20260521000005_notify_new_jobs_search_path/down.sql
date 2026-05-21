-- Revert to the un-hardened function body (no SET search_path, no explicit
-- SECURITY INVOKER). The trigger is unchanged across the up/down boundary.
CREATE OR REPLACE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER AS $$
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
