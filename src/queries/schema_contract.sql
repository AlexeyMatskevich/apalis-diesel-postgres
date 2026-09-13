-- Catalog shape, not a checksum of arbitrary function bodies. $1 checks just
-- the common columns of a recognized legacy generation (lease/snapshot absent
-- and nullable priority permitted). $2 is the generation whose objects are
-- required: 11 selects the final indexes and function bodies, 13 adds the
-- active-owner constraint, 14 adds the state-shape constraint.
WITH expected_columns(table_name, column_name, type_name, required) AS (VALUES
    ('jobs', 'job', 'bytea', true),
    ('jobs', 'id', 'text', true),
    ('jobs', 'job_type', 'text', true),
    ('jobs', 'status', 'text', true),
    ('jobs', 'attempts', 'integer', true),
    ('jobs', 'max_attempts', 'integer', true),
    ('jobs', 'run_at', 'timestamp with time zone', true),
    ('jobs', 'last_result', 'jsonb', false),
    ('jobs', 'lock_at', 'timestamp with time zone', false),
    ('jobs', 'lock_by', 'text', false),
    ('jobs', 'done_at', 'timestamp with time zone', false),
    ('jobs', 'priority', 'integer', true),
    ('jobs', 'metadata', 'jsonb', false),
    ('jobs', 'idempotency_key', 'text', false),
    ('workers', 'id', 'text', true),
    ('workers', 'worker_type', 'text', true),
    ('workers', 'storage_name', 'text', true),
    ('workers', 'layers', 'text', true),
    ('workers', 'last_seen', 'timestamp with time zone', true),
    ('workers', 'started_at', 'timestamp with time zone', false),
    ('workers', 'lease_token', 'text', false),
    ('queue_stats_snapshot', 'job_type', 'text', false),
    ('queue_stats_snapshot', 'total_jobs', 'bigint', false),
    ('queue_stats_snapshot', 'running_jobs', 'bigint', false),
    ('queue_stats_snapshot', 'pending_jobs', 'bigint', false),
    ('queue_stats_snapshot', 'queued_jobs', 'bigint', false),
    ('queue_stats_snapshot', 'failed_jobs', 'bigint', false),
    ('queue_stats_snapshot', 'done_jobs', 'bigint', false),
    ('queue_stats_snapshot', 'killed_jobs', 'bigint', false),
    ('queue_stats_snapshot', 'active_jobs', 'bigint', false),
    ('queue_stats_snapshot', 'jobs_past_hour', 'bigint', false),
    ('queue_stats_snapshot', 'jobs_past_day', 'bigint', false),
    ('queue_stats_snapshot', 'jobs_past_7_days', 'bigint', false),
    ('queue_stats_snapshot', 'most_recent_run_at', 'timestamp with time zone', false),
    ('queue_stats_snapshot', 'most_recent_done_at', 'timestamp with time zone', false),
    ('queue_stats_snapshot', 'snapshot_at', 'timestamp with time zone', false)
), column_problems AS (
 SELECT 'column ' || e.table_name || '.' || e.column_name AS problem
 FROM expected_columns e
 LEFT JOIN pg_namespace n ON n.nspname = 'apalis'
 LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = e.table_name
 LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = e.column_name AND a.attnum > 0 AND NOT a.attisdropped
 WHERE (NOT $1 OR (e.table_name != 'queue_stats_snapshot' AND e.column_name != 'lease_token'))
 AND (a.attname IS NULL OR format_type(a.atttypid, a.atttypmod) != e.type_name
      OR (a.attnotnull != e.required AND NOT ($1 AND e.column_name = 'priority'))
      OR c.relkind != CASE WHEN e.table_name = 'queue_stats_snapshot' THEN 'm'::"char" ELSE 'r'::"char" END)
), expected_constraints(table_name, constraint_name, definition) AS (VALUES
 ('jobs', 'jobs_active_owner_check', $$CHECK (((status <> ALL (ARRAY['Queued'::text, 'Running'::text])) OR (lock_by IS NOT NULL)))$$),
 ('jobs', 'jobs_state_shape_check', $$CHECK ((((status <> 'Pending'::text) OR ((lock_by IS NULL) AND (lock_at IS NULL))) AND ((status <> ALL (ARRAY['Queued'::text, 'Running'::text])) OR ((lock_by IS NOT NULL) AND (lock_at IS NOT NULL)))))$$),
 ('jobs', 'jobs_pkey', 'PRIMARY KEY (id)'),
 ('workers', 'workers_pkey', 'PRIMARY KEY (id, worker_type)'),
 ('jobs', 'jobs_lock_by_worker_type_fkey', 'FOREIGN KEY (lock_by, job_type) REFERENCES apalis.workers(id, worker_type)'),
 ('jobs', 'jobs_attempts_check', 'CHECK ((attempts >= 0))'),
 ('jobs', 'jobs_max_attempts_check', 'CHECK ((max_attempts > 0))'),
 ('jobs', 'jobs_attempts_lte_max_attempts_check', 'CHECK ((attempts <= max_attempts))'),
 ('jobs', 'jobs_priority_check', 'CHECK ((priority >= 0))'),
 ('jobs', 'jobs_status_check', $$CHECK ((status = ANY (ARRAY['Pending'::text, 'Queued'::text, 'Running'::text, 'Done'::text, 'Failed'::text, 'Killed'::text])))$$)
), constraint_problems AS (
 SELECT 'constraint ' || e.table_name || '.' || e.constraint_name AS problem
 FROM expected_constraints e
 LEFT JOIN pg_constraint c ON c.conrelid = to_regclass('apalis.' || e.table_name) AND c.conname = e.constraint_name
 WHERE NOT $1 AND ($2 >= 13 OR e.constraint_name != 'jobs_active_owner_check')
  AND ($2 >= 14 OR e.constraint_name != 'jobs_state_shape_check')
  AND (c.oid IS NULL OR NOT c.convalidated OR c.condeferrable
  OR (pg_get_constraintdef(c.oid) != e.definition
    AND NOT (e.constraint_name = 'jobs_priority_check' AND pg_get_constraintdef(c.oid) = 'CHECK (((priority IS NULL) OR (priority >= 0)))')))
), expected_indexes(index_name, definition) AS (VALUES
 ('workers_worker_type_last_seen_idx', 'CREATE INDEX workers_worker_type_last_seen_idx ON apalis.workers USING btree (worker_type, last_seen DESC)'),
 ('jobs_list_by_queue_idx', CASE WHEN $2 >= 11
    THEN 'CREATE INDEX jobs_list_by_queue_idx ON apalis.jobs USING btree (job_type, status, done_at DESC, run_at DESC, id DESC)'
    ELSE 'CREATE INDEX jobs_list_by_queue_idx ON apalis.jobs USING btree (job_type, status, done_at DESC, run_at DESC)' END),
 ('jobs_list_all_idx', CASE WHEN $2 >= 11
    THEN 'CREATE INDEX jobs_list_all_idx ON apalis.jobs USING btree (status, done_at DESC, run_at DESC, id DESC)'
    ELSE 'CREATE INDEX jobs_list_all_idx ON apalis.jobs USING btree (status, done_at DESC, run_at DESC)' END),
 ('jobs_locked_by_queue_idx', $$CREATE INDEX jobs_locked_by_queue_idx ON apalis.jobs USING btree (job_type, lock_by) WHERE ((lock_by IS NOT NULL) AND (status = ANY (ARRAY['Running'::text, 'Queued'::text])))$$),
 ('jobs_job_type_run_at_idx', 'CREATE INDEX jobs_job_type_run_at_idx ON apalis.jobs USING btree (job_type, run_at)'),
 ('jobs_run_at_idx', 'CREATE INDEX jobs_run_at_idx ON apalis.jobs USING btree (run_at)'),
 ('jobs_job_type_done_at_idx', 'CREATE INDEX jobs_job_type_done_at_idx ON apalis.jobs USING btree (job_type, done_at) WHERE (done_at IS NOT NULL)'),
 ('idx_jobs_idempotency_key', 'CREATE UNIQUE INDEX idx_jobs_idempotency_key ON apalis.jobs USING btree (job_type, idempotency_key) WHERE (idempotency_key IS NOT NULL)'),
 ('queue_stats_snapshot_job_type_idx', 'CREATE UNIQUE INDEX queue_stats_snapshot_job_type_idx ON apalis.queue_stats_snapshot USING btree (job_type)'),
 ('jobs_dequeue_idx', CASE WHEN $2 >= 11
    THEN $$CREATE INDEX jobs_dequeue_idx ON apalis.jobs USING btree (job_type, priority DESC, run_at, id) WHERE ((status = ANY (ARRAY['Pending'::text, 'Failed'::text])) AND (attempts < max_attempts))$$
    ELSE $$CREATE INDEX jobs_dequeue_idx ON apalis.jobs USING btree (job_type, priority DESC, run_at, id) WHERE ((status = 'Pending'::text) OR ((status = 'Failed'::text) AND (attempts < max_attempts)))$$ END)
), index_problems AS (
 SELECT 'index ' || e.index_name AS problem FROM expected_indexes e
 LEFT JOIN pg_index i ON i.indexrelid = to_regclass('apalis.' || e.index_name)
 WHERE NOT $1 AND (i.indexrelid IS NULL OR NOT i.indisvalid OR NOT i.indisready
                  OR pg_get_indexdef(i.indexrelid) != e.definition)
), function_problems AS (
 SELECT 'function ' || signature AS problem
 FROM (VALUES ('apalis.get_jobs(text,text,integer)', true, 'apalis.jobs'),
              ('apalis.notify_new_jobs()', false, 'trigger')) e(signature, returns_set, return_type)
 LEFT JOIN pg_proc p ON p.oid = to_regprocedure(e.signature)
 WHERE NOT $1 AND (p.oid IS NULL OR p.prosecdef OR p.proretset != e.returns_set
     OR p.prorettype != to_regtype(e.return_type) OR p.provolatile != 'v'
     OR p.proconfig IS NULL OR NOT (p.proconfig @> ARRAY['search_path=pg_catalog, apalis, pg_temp']
                  OR (NOT ($2 >= 11) AND p.proconfig @> ARRAY['search_path=pg_catalog, apalis'])))
), trigger_problems AS (
 SELECT 'trigger notify_workers' AS problem WHERE NOT $1 AND NOT EXISTS (
 SELECT 1 FROM pg_trigger t WHERE t.tgrelid = to_regclass('apalis.jobs')
    AND t.tgname = 'notify_workers' AND t.tgtype = 4 AND t.tgenabled = 'O'
    AND t.tgfoid = to_regprocedure('apalis.notify_new_jobs()')
    AND t.tgnewtable = 'new_jobs' AND t.tgoldtable IS NULL
    AND t.tgqual IS NULL AND t.tgnargs = 0)
)
SELECT problem FROM column_problems UNION ALL SELECT problem FROM constraint_problems
UNION ALL SELECT problem FROM index_problems UNION ALL SELECT problem FROM function_problems
UNION ALL SELECT problem FROM trigger_problems ORDER BY problem
