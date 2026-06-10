# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While
the crate is pre-1.0, a minor version bump may carry breaking changes.

## [0.4.0]

### Fixed

- A claimed row whose payload failed to decode was stranded in `Running` for
  as long as the claiming worker kept heartbeating: ack needs a decoded task,
  and orphan recovery only reclaims rows of stale workers. The decode stage
  now releases such rows through the normal retry budget (`Failed` with the
  decode error in `last_result`, terminal `Killed` once attempts are
  exhausted), guarded by the exact claim epoch (`lock_by`, `lock_at`,
  `attempts`) so a delayed release never touches a row that was acked, swept,
  or re-claimed in the meantime.
- The checked-in Diesel schema (`src/schema.rs`) was missing the
  `workers.lease_token` column added by migration 20260521000002; typed
  queries against `apalis.workers` could not reference it. New specs pin
  `schema.rs` against `information_schema` for both tables so the next
  migration cannot leave the typed schema stale silently.
- CI's postgres job ran only 3 of the 11 integration test binaries; the
  newer spec suites (outbox SAVEPOINT semantics, concurrent reenqueue,
  migration concurrency, the `spec_queries_*` SQL contracts) were
  compile-checked but never executed as release gates. The job now runs
  `--tests`, which also gates any future test binary automatically.
- Concurrent `reenqueue_orphaned` sweeps could double-apply to the same stale
  row under READ COMMITTED (EvalPlanQual re-check): burning an extra attempt,
  prematurely killing a job, or flipping an already-acked row back to
  `Pending`. The sweep now repeats the status predicate on the outer UPDATE
  and claims candidates with `FOR UPDATE OF jobs SKIP LOCKED`, so competing
  sweeps skip each other instead of queueing and re-applying.
- The shared notify listener returned its pooled connection to r2d2 without
  `UNLISTEN`, so the next pool user inherited the subscription and
  notifications accumulated unread in libpq's receive buffer. Every listener
  exit now removes the subscription before the connection is recycled.
- `Debug` output of `PgAck` (and therefore of the public `PgMiddleware`
  returned by `Backend::middleware()`) printed the per-process `lease_token`
  verbatim; it is now redacted, matching `PostgresStorage`'s `Debug`.
- `with_codec` rebuilt the sink from scratch, silently dropping buffered
  tasks and any in-flight flush; both now carry over (the buffer holds
  codec-independent compact tasks).
- With both `tokio` and `ntex` features enabled, calling the backend from the
  ntex executor panicked inside `tokio::task::spawn_blocking`; the backend
  now falls back to ntex's blocking pool when no Tokio runtime is present.

### Changed (breaking)

- `PgFetcher`'s phantom `_marker` field is no longer public; construct the
  marker fetcher via `Default` instead.

### Changed

- Pool-path enqueue batches without an `idempotency_key` skip the
  conflict-recovery machinery (transaction wrapper, `RETURNING`
  materialization, key copies) on the sink's hot flush path — no conflict is
  possible without a key. The outbox path (`push_with_conn` /
  `push_task_with_conn`) keeps every batch inside `conn.transaction(...)`:
  a failing INSERT — idempotency conflict or a PK violation on a
  caller-supplied task id — rolls back only the batch's SAVEPOINT and never
  aborts the caller's outer transaction.
- apalis RC dependencies are pinned exactly (`=…-rc.9`): cargo treats
  prereleases as caret-compatible, so an unpinned requirement would let
  `cargo update` pull a breaking `rc.10` silently. Dropped the unused direct
  `pin-project` dependency and trimmed production tokio features to `rt`.
- The admin worker registration dropped its unreachable `AlreadyRegistered`
  branch (the statement always upserts; dashboards re-registering an
  existing worker is the expected idempotent case).
- The `Sink` impl on `PostgresStorage` no longer requires
  `Args: Send + Sync + 'static`.
- The notify-driven fetcher pipeline composition is shared between
  `PgNotify` and `SharedFetcher` (`notify_backed_compact_stream`), removing
  the duplication that had already let the two LISTEN loops drift.

### Documentation

- The handler/fan-out examples (`README.md`, `examples/worker.rs`,
  `examples/worker-ntex.rs`) now run business transactions on a separate
  backend pool injected via `Data<PgPool>` instead of `storage.pool()`,
  matching the "Connection pool isolation" guidance they previously
  contradicted.
- `Error::IdempotencyConflict` recovery guidance now explains that
  `conflicting_keys` also covers intra-batch duplicates: deduplicate (keep
  one task per conflicting key) instead of dropping every task with the key.
- The dequeue-index migration comment no longer claims the partial predicate
  "exactly matches" the fetch WHERE clause: `run_at <= now()` remains a
  residual filter, and the trade-off is now documented.
- README runtime-feature matrix corrected: building with no runtime feature
  is a compile error, and the tokio/ntex precedence is runtime-aware.

## [0.3.0]

### Changed (breaking)

- Idempotency-key conflicts on enqueue now return a dedicated, typed error
  variant instead of the stringly-typed `Error::InvalidArgument(
  "idempotency_key conflict: …")`. The new variant is:

  ```rust
  Error::IdempotencyConflict { job_type: String, conflicting_keys: Vec<String>, total: usize }
  ```

  `conflicting_keys` lists exactly which keys collided — against stored rows
  or between tasks in the same batch — so a batch caller can deduplicate
  (keep one task per conflicting key) and re-enqueue the rest.

  Match the variant to tell a benign duplicate apart from a real failure,
  rather than matching the message text (which could change in any release):

  ```rust
  match storage.push_task_with_conn(conn, task) {
      Ok(id) => { /* enqueued */ }
      Err(Error::IdempotencyConflict { .. }) => { /* duplicate — swallow it */ }
      Err(other) => return Err(other),
  }
  ```

  Storage behavior is unchanged: the conflict still rolls back the whole
  enqueue batch via SAVEPOINT — one duplicate undoes *every* row in the
  batch, not just the colliding one — while a surrounding transaction stays
  alive so business writes can still commit. Every other
  `Error::InvalidArgument` case (queue-name / metadata / idempotency-key
  length caps, unreachable `run_at`) is unchanged.

### Notes

- `Error` is `#[non_exhaustive]`, so future variants are not a breaking change
  for downstreams that already include a wildcard match arm.

## [0.2.0] and earlier

See the git history for changes before this changelog was introduced.
