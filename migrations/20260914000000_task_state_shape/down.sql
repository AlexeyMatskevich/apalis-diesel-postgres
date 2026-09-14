-- Rows repaired by the upgrade keep their repaired history; only the
-- constraint is removed.
ALTER TABLE apalis.jobs DROP CONSTRAINT IF EXISTS jobs_state_shape_check;
DROP INDEX IF EXISTS apalis.jobs_job_type_lock_by_idx;
