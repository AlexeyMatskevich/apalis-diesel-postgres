# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). While
the crate is pre-1.0, a minor version bump may carry breaking changes.

## [Unreleased]

### Added

- Batch outbox enqueue on a caller-supplied connection: `push_batch_with_conn`
  and `push_tasks_with_conn` insert many tasks in one round trip inside the
  caller's own transaction, mirroring the single-task `push_with_conn` /
  `push_task_with_conn` and sharing their SAVEPOINT-per-batch conflict
  semantics.

### Changed (breaking)

- `lock_task` and `lock_task_in_queue` now take `&PgTaskId` instead of `&Ulid`,
  so callers pass the storage's own task-id type rather than a raw ULID.
- Storage constructors and builders now borrow the pool (`&PgPool`) instead of
  taking it by value, so one pool can seed several storages without a `.clone()`
  at every call site.
- `Error::AlreadyRegistered` is now a struct variant that also carries the queue
  it collided on — `AlreadyRegistered { worker_id, queue }` — so the error
  identifies which queue's registration was already held, not just the worker.
- `PgSink` is now a crate-internal type. It was never meant to be named directly
  (construct sinks through `PostgresStorage`), so it no longer appears in the
  public API.

### Changed

- The encoded task payload is capped at 1 MiB (`MAX_JOB_PAYLOAD_LEN`): a larger
  payload is rejected up front with `Error::InvalidArgument` instead of being
  written to the row unbounded.
- The `ListWorkers` admin trait no longer requires `Args: Sync`.

### Fixed

- `database_hint`'s structured foreign-key match required both
  `constraint_name` and `table_name == Some("jobs")` to fire; some drivers
  report the constraint but leave `table_name` unset, so the "register the
  worker for this queue before locking or acknowledging jobs" hint silently
  fell through to the locale-dependent message match (and could be lost
  entirely on non-English PostgreSQL servers). `constraint_name` alone is
  already specific to this schema's `jobs.lock_by` FK, so the structured match
  now fires whenever the constraint name matches and `table_name` is either
  absent or `"jobs"`; only a `table_name` naming some other table rules it
  out.
- Under the `ntex` feature, a dropped ack or sink-flush future could cancel a
  still-queued blocking database write: ntex-rt's blocking pool skips a closure
  whose result receiver was already dropped, so a cancelled future could strand
  a row in `Running` under a healthy heartbeat, or lose tasks a flush had
  already drained from its buffer. `run_blocking` now detaches the submission
  onto a background task that owns the receiver, so the write always runs to
  completion; when no ntex runtime is entered it falls back to the inline
  blocking pool, which runs the closure synchronously and is inherently
  drop-safe.
- The polling fetcher rebuilt its poll strategy on every cycle. Because
  `MultiStrategy::poll_strategy` drains its shared strategy list, every rebuild
  after the first saw an empty strategy and collapsed to a hardcoded 100 ms
  fallback — silently discarding the configured interval and backoff after a
  single cycle. The strategy is now built once per fetcher and polled for its
  whole lifetime (it still reads the live task count, so backoff keeps
  adapting). Draining a batch always returns to the configured strategy instead
  of issuing its own follow-up fetch, so a user-supplied rate limiter or
  readiness gate is never bypassed.
- A `JobRow` whose `metadata` column held a non-object JSON value (an array,
  string, number, or bool) panicked while converting into `TaskRow`; non-object
  metadata is now coerced to absent instead of panicking.
- `attempts + 1` was evaluated in `i32` on the fail and re-enqueue paths, so a
  row that reached `i32::MAX` attempts would overflow; the increment is now
  promoted to `bigint` (`attempts::bigint + 1`) before the `max_attempts`
  comparison.
- The orphan-recovery sweep (`reenqueue_orphaned`) reclaimed every eligible
  stale row in a single `UPDATE`, so a large backlog produced an unbounded
  transaction. Each sweep now reclaims at most 1000 rows and drains the
  remainder on subsequent sweeps.
- `setup()` released the migration advisory lock exactly once, but PostgreSQL
  session advisory locks are reentrant: a pooled connection that already held a
  leaked copy (from a prior `setup()` whose release failed while its session
  stayed alive) would return to the pool still holding the lock, letting the
  next `setup()` re-enter it without blocking and defeating the serialization.
  `setup()` now drains every hold the backend has on the lock — unlocking while
  `pg_locks` still reports it held, which also avoids the "lock is not held"
  server warning an unlock-until-`false` loop would emit — and confirms the
  connection owns zero copies before returning.

### Performance

- The decode stage carries the worker id as `Arc<str>` and the ack path forwards
  the per-process lease token as `Arc<str>`, replacing a `String` allocation on
  every decoded row and every acknowledgement with a refcount bump.
- The worker heartbeat defers its queue-name allocation to the error path, so
  the common success path does not allocate it.

### Documentation

- `list_tasks` and `list_all_tasks` now document the `OFFSET` pagination cost:
  the query still scans and discards the skipped rows, so deep pages grow
  linearly more expensive.

## [0.4.1]

### Fixed

- `list_queues()` (the `ListQueues` admin trait) silently returned an empty
  `stats` list for any queue with no completed jobs: in that state the
  per-queue `AVG_JOB_DURATION_MINS` aggregate is SQL `NULL`, which serialized
  to a JSON `null` that cannot decode into `apalis_core::Statistic` (whose
  `value` is a non-optional `String`); the whole `Vec<Statistic>` decode then
  failed and was defaulted to empty, dropping *every* stat for the queue. The
  `queue_stats` CTE now `COALESCE`s null stat values to `"0"` (matching the
  single-stat metrics path), so a queue with jobs always reports its full
  stat set. A new `spec_queries_admin` scenario pins the `PENDING_JOBS` /
  `TOTAL_JOBS` titles so the regression cannot recur unobserved.

### Changed

- The unscoped `lock_task` no longer lists "or in another queue" in its
  `TaskNotFound` hint: that entry point does not filter by `job_type`, so a task
  in another queue is locked rather than reported missing. `lock_task_in_queue`
  keeps the queue-aware hint.

### Documentation

- Every public `Result`-returning function now carries an `# Errors` section
  (`build_pool`, `build_pool_with`, `setup`, `verify_schema`,
  `refresh_queue_stats_snapshot`, `lock_task`, `lock_task_in_queue`), matching
  the convention already used by the outbox `push_*_with_conn` methods.
- `MIGRATIONS` and the `schema` module are now documented, and the crate enables
  `#![warn(missing_docs)]` and `#![warn(rustdoc::broken_intra_doc_links)]` so doc
  coverage and intra-doc links cannot silently regress.
- README links to `examples/*` and `CONTRIBUTING.md` are now absolute GitHub
  URLs: as relative links they 404 when the README is rendered as the crate's
  docs.rs landing page. Added an MSRV note (Rust 1.88) and removed an `unwrap()`
  from the `push_task_with_conn` example.
- `Cargo.toml` gained `[package.metadata.docs.rs]` (`all-features = true`,
  `--cfg docsrs`) so docs.rs documents the `ntex` path alongside `tokio`.
- `CONTRIBUTING.md` no longer lists `--no-default-features` check/test commands:
  building without a runtime feature is an intentional `compile_error!`, so
  those commands could never pass.

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

[Unreleased]: https://github.com/AlexeyMatskevich/apalis-diesel-postgres/compare/v0.4.1...HEAD
[0.4.1]: https://github.com/AlexeyMatskevich/apalis-diesel-postgres/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/AlexeyMatskevich/apalis-diesel-postgres/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/AlexeyMatskevich/apalis-diesel-postgres/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/AlexeyMatskevich/apalis-diesel-postgres/compare/v0.1.1...v0.2.0
