-- Materialized snapshot of per-queue statistics.
--
-- `list_queues` and `metrics` scan the full `apalis.jobs` table with 20+
-- `FILTER` aggregates per call (`src/queries/admin.rs`). On busy queues this
-- is O(rows) per dashboard request and an easy DoS vector. Pre-aggregating
-- into a materialized view lets callers query a small fixed-cost table and
-- decide how often the data is refreshed (via `refresh_queue_stats_snapshot`
-- below — exposed through the Rust API).
--
-- The view is created `WITH NO DATA` so the migration is fast and idempotent;
-- the first `REFRESH` populates it. A unique index on `job_type` enables
-- `REFRESH MATERIALIZED VIEW CONCURRENTLY` so refreshes don't block readers.

CREATE MATERIALIZED VIEW IF NOT EXISTS apalis.queue_stats_snapshot AS
SELECT
    job_type,
    COUNT(*) AS total_jobs,
    COUNT(*) FILTER (WHERE status = 'Running') AS running_jobs,
    COUNT(*) FILTER (WHERE status = 'Pending') AS pending_jobs,
    COUNT(*) FILTER (WHERE status = 'Queued') AS queued_jobs,
    COUNT(*) FILTER (WHERE status = 'Failed') AS failed_jobs,
    COUNT(*) FILTER (WHERE status = 'Done') AS done_jobs,
    COUNT(*) FILTER (WHERE status = 'Killed') AS killed_jobs,
    COUNT(*) FILTER (WHERE status IN ('Pending', 'Queued', 'Running')) AS active_jobs,
    COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 hour') AS jobs_past_hour,
    COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '24 hours') AS jobs_past_day,
    COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '7 days') AS jobs_past_7_days,
    MAX(run_at) AS most_recent_run_at,
    MAX(done_at) FILTER (WHERE done_at IS NOT NULL) AS most_recent_done_at,
    now() AS snapshot_at
FROM apalis.jobs
GROUP BY job_type
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS queue_stats_snapshot_job_type_idx
    ON apalis.queue_stats_snapshot (job_type);
