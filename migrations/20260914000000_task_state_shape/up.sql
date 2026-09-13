-- Close the claim shape for every writer: a Pending row carries no owner
-- columns, and an active row carries a complete claim, owner and timestamp.
-- Repair legacy rows first; terminal history is never rewritten.

-- A Pending row runs nowhere. Owner columns on it are the history of a claim
-- that already ended, and they mislead ownership listings.
UPDATE apalis.jobs
SET lock_by = NULL, lock_at = NULL
WHERE status = 'Pending' AND (lock_by IS NOT NULL OR lock_at IS NOT NULL);

-- An active row without a claim timestamp cannot be acknowledged: the
-- acknowledgement predicate needs the complete claim. Treat the execution as
-- lost, like an ownerless one: consume one attempt, bounded by max_attempts.
UPDATE apalis.jobs AS jobs
SET attempts = CASE WHEN attempts < max_attempts THEN attempts + 1 ELSE max_attempts END,
    status = CASE WHEN attempts < max_attempts - 1 THEN 'Pending' ELSE 'Killed' END,
    last_result = jsonb_build_object('Err', 'Claim timestamp was missing during schema upgrade'),
    done_at = CASE WHEN attempts < max_attempts - 1 THEN NULL ELSE statement_timestamp() END,
    lock_by = NULL,
    lock_at = NULL
WHERE status IN ('Queued', 'Running') AND lock_at IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint
                   WHERE conrelid = 'apalis.jobs'::regclass
                     AND conname = 'jobs_state_shape_check') THEN
        ALTER TABLE apalis.jobs ADD CONSTRAINT jobs_state_shape_check
            CHECK ((status <> 'Pending' OR (lock_by IS NULL AND lock_at IS NULL))
               AND (status NOT IN ('Queued', 'Running') OR (lock_by IS NOT NULL AND lock_at IS NOT NULL)));
    END IF;
END;
$$;
