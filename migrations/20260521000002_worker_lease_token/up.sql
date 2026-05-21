-- Per-registration lease token for workers.
--
-- Previously `keep_alive` was gated only on `(id, worker_type)`, so any
-- process that knew the (deterministic) worker id pair could refresh a
-- foreign worker's heartbeat and indefinitely block `reenqueue_orphaned`
-- from reclaiming its jobs. With a per-registration random token, only the
-- process holding the token issued at `register_worker` time can refresh
-- the heartbeat.
--
-- The column is nullable so existing rows (and code paths that have not yet
-- adopted the token API) continue to work; new rows produced by
-- `register_worker_blocking` always populate it.

ALTER TABLE apalis.workers
    ADD COLUMN IF NOT EXISTS lease_token TEXT;
