-- Restore the dequeue index of the preceding generation. The function bodies
-- this migration re-asserted are identical to their predecessors and stay.
DROP INDEX IF EXISTS apalis.jobs_dequeue_idx;
CREATE INDEX IF NOT EXISTS jobs_dequeue_idx
    ON apalis.jobs(job_type, priority DESC, run_at ASC, id)
    WHERE status = 'Pending'
       OR (status = 'Failed' AND attempts < max_attempts);
