CREATE SCHEMA IF NOT EXISTS apalis;

CREATE TABLE IF NOT EXISTS apalis.workers (
    id TEXT NOT NULL,
    worker_type TEXT NOT NULL,
    storage_name TEXT NOT NULL,
    layers TEXT NOT NULL DEFAULT '',
    last_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    started_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id, worker_type)
);

CREATE INDEX IF NOT EXISTS workers_worker_type_last_seen_idx
    ON apalis.workers(worker_type, last_seen DESC);

CREATE TABLE IF NOT EXISTS apalis.jobs (
    job BYTEA NOT NULL,
    id TEXT NOT NULL PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'Pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 25,
    run_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
    last_result JSONB,
    lock_at TIMESTAMP WITH TIME ZONE,
    lock_by TEXT,
    done_at TIMESTAMP WITH TIME ZONE,
    priority INTEGER NOT NULL DEFAULT 0,
    metadata JSONB,
    idempotency_key TEXT,
    CONSTRAINT jobs_status_check
        CHECK (status IN ('Pending', 'Queued', 'Running', 'Done', 'Failed', 'Killed')),
    CONSTRAINT jobs_attempts_check CHECK (attempts >= 0),
    CONSTRAINT jobs_max_attempts_check CHECK (max_attempts > 0),
    CONSTRAINT jobs_attempts_lte_max_attempts_check CHECK (attempts <= max_attempts),
    CONSTRAINT jobs_priority_check CHECK (priority >= 0),
    CONSTRAINT jobs_lock_by_worker_type_fkey
        FOREIGN KEY (lock_by, job_type) REFERENCES apalis.workers(id, worker_type)
);

CREATE INDEX IF NOT EXISTS jobs_dequeue_idx
    ON apalis.jobs(job_type, priority DESC, run_at ASC, id)
    WHERE status IN ('Pending', 'Failed');
CREATE INDEX IF NOT EXISTS jobs_list_by_queue_idx
    ON apalis.jobs(job_type, status, done_at DESC, run_at DESC);
CREATE INDEX IF NOT EXISTS jobs_list_all_idx
    ON apalis.jobs(status, done_at DESC, run_at DESC);
CREATE INDEX IF NOT EXISTS jobs_locked_by_queue_idx
    ON apalis.jobs(job_type, lock_by)
    WHERE lock_by IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_job_type_run_at_idx
    ON apalis.jobs(job_type, run_at);
CREATE INDEX IF NOT EXISTS jobs_run_at_idx
    ON apalis.jobs(run_at);
CREATE INDEX IF NOT EXISTS jobs_job_type_done_at_idx
    ON apalis.jobs(job_type, done_at)
    WHERE done_at IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_idempotency_key
    ON apalis.jobs(job_type, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE OR REPLACE FUNCTION apalis.get_jobs(
    worker_id TEXT,
    v_job_type TEXT,
    v_job_count INTEGER DEFAULT 5
) RETURNS SETOF apalis.jobs AS $$
BEGIN
    RETURN QUERY
    WITH next_jobs AS (
        SELECT id
        FROM apalis.jobs
        WHERE (status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts))
            AND run_at <= now()
            AND job_type = v_job_type
        ORDER BY priority DESC, run_at ASC
        LIMIT v_job_count
        FOR UPDATE SKIP LOCKED
    )
    UPDATE apalis.jobs
    SET status = 'Queued',
        lock_by = worker_id,
        lock_at = date_trunc('second', now())
    FROM next_jobs
    WHERE apalis.jobs.id = next_jobs.id
    RETURNING apalis.jobs.*;
END;
$$ LANGUAGE plpgsql VOLATILE;

DROP TRIGGER IF EXISTS notify_workers ON apalis.jobs;
DROP FUNCTION IF EXISTS apalis.notify_new_jobs;

CREATE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.run_at <= now() THEN
        PERFORM pg_notify(
            'apalis::job::insert',
            json_build_object(
                'job_type', NEW.job_type,
                'id', NEW.id,
                'run_at', NEW.run_at
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_workers
AFTER INSERT ON apalis.jobs
FOR EACH ROW EXECUTE FUNCTION apalis.notify_new_jobs();
