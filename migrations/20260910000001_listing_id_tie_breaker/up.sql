-- Extend listing indexes to cover the complete, deterministic page order.
-- Transactional setup holds these DDL locks until the entire upgrade commits.
DROP INDEX IF EXISTS apalis.jobs_list_by_queue_idx;
CREATE INDEX jobs_list_by_queue_idx
    ON apalis.jobs(job_type, status, done_at DESC, run_at DESC, id DESC);

DROP INDEX IF EXISTS apalis.jobs_list_all_idx;
CREATE INDEX jobs_list_all_idx
    ON apalis.jobs(status, done_at DESC, run_at DESC, id DESC);
