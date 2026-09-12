-- Repair only ownership that the legacy id-only FK allowed across queues.
-- Active lost executions consume an attempt; terminal history is never revived.
ALTER TABLE apalis.jobs DROP CONSTRAINT IF EXISTS fk_worker_lock_by;
ALTER TABLE apalis.jobs DROP CONSTRAINT IF EXISTS jobs_lock_by_fkey;

UPDATE apalis.jobs AS jobs
SET attempts = CASE WHEN status IN ('Queued', 'Running')
                    THEN CASE WHEN attempts < max_attempts THEN attempts + 1 ELSE max_attempts END
                    ELSE attempts END,
    status = CASE WHEN status IN ('Queued', 'Running')
                  THEN CASE WHEN attempts < max_attempts - 1 THEN 'Pending' ELSE 'Killed' END
                  ELSE status END,
    last_result = CASE WHEN status IN ('Queued', 'Running')
                       THEN jsonb_build_object('Err', 'Worker ownership was lost during schema upgrade')
                       ELSE last_result END,
    done_at = CASE WHEN status IN ('Queued', 'Running')
                   THEN CASE WHEN attempts < max_attempts - 1 THEN NULL ELSE statement_timestamp() END
                   ELSE done_at END,
    lock_by = NULL,
    lock_at = NULL
WHERE (status IN ('Queued', 'Running') OR lock_by IS NOT NULL)
  AND NOT EXISTS (
      SELECT 1 FROM apalis.workers AS workers
      WHERE workers.id = jobs.lock_by AND workers.worker_type = jobs.job_type
  );
