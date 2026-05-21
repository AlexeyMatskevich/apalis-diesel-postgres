-- Replace the per-row NOTIFY trigger with a statement-level trigger that emits
-- at most one NOTIFY per (queue, statement) — one bulk `push_tasks` of N jobs
-- previously generated N NOTIFY events (each parsed by every listener); now
-- it generates one NOTIFY per `job_type` with the inserted ids batched in the
-- payload.
--
-- Wire format (forward-compatible): `{job_type, ids: [...]}`. The Rust
-- listener also accepts the legacy `{job_type, id}` form so a partial rollout
-- (one migration applied but not the other, or a third-party tool emitting
-- the old shape) keeps working.

DROP TRIGGER IF EXISTS notify_workers ON apalis.jobs;
DROP FUNCTION IF EXISTS apalis.notify_new_jobs();

-- PostgreSQL caps `pg_notify` payloads at NOTIFY_PAYLOAD_MAX_LENGTH (~8000
-- bytes); exceeding it aborts the whole transaction. A 26-char Ulid plus
-- JSON quoting/comma is ~29 bytes, so we chunk ids into groups of 100
-- (~2.9 KB) before emitting. A bulk `push_tasks` of thousands still emits
-- one NOTIFY per ~100 jobs instead of one per row — a 10–100× reduction.
CREATE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER AS $$
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

CREATE TRIGGER notify_workers
AFTER INSERT ON apalis.jobs
REFERENCING NEW TABLE AS new_jobs
FOR EACH STATEMENT EXECUTE FUNCTION apalis.notify_new_jobs();
