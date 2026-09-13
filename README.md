# apalis-diesel-postgres

[![crates.io](https://img.shields.io/crates/v/apalis-diesel-postgres.svg)](https://crates.io/crates/apalis-diesel-postgres)
[![docs.rs](https://img.shields.io/docsrs/apalis-diesel-postgres)](https://docs.rs/apalis-diesel-postgres)

PostgreSQL storage backend for [Apalis](https://github.com/apalis-dev/apalis)
implemented with Diesel and `r2d2`.

The crate's headline feature is a **transactional enqueue API** —
[`PostgresStorage::push_with_conn`](#transactional-enqueue-outbox-pattern) — that
lets you insert a job in the same `PgConnection` transaction as your business
data. If the business transaction commits, the job is enqueued; if it rolls
back, no job is enqueued and no `LISTEN/NOTIFY` is delivered. That removes the
classic "resource written, job missing" split-brain without a separate outbox
table.

The crate accepts any `diesel::r2d2::Pool<ConnectionManager<PgConnection>>` —
you keep using the pool you already wire up — and ships everything else apalis
needs: migrations, polling/notify-driven dequeue, locking, ack, retries,
orphan re-enqueue, admin queries, and `MakeShared` for many-queue setups.

## Status

Targets the Apalis 1.0 release candidate: `apalis-core 1.0.0-rc.9`,
`apalis-sql 1.0.0-rc.9`, `apalis-codec 0.1.0-rc.9`, `diesel >=2.3.13`.
Uses the Apalis SQL tables (`apalis.jobs`, `apalis.workers`). The
[0.4.1 to 0.5.0 upgrade guide](https://github.com/AlexeyMatskevich/apalis-diesel-postgres/blob/v0.5.0/docs/upgrading.md#upgrading-from-041-to-050)
covers Rust API changes, schema migration, deployment order and rollback limits.

MSRV: Rust 1.88.

The SQL used by this backend requires PostgreSQL 14 or later. CI currently tests
PostgreSQL 18. PostgreSQL 12/13 are incompatible with the metrics queries;
PostgreSQL 14–17 have not been validated by the current CI matrix.

## Installation

```toml
[dependencies]
apalis-diesel-postgres = { version = "0.5", features = ["tokio"] }
diesel = { version = "2.3.13", features = ["postgres", "r2d2", "chrono", "serde_json"] }
serde = { version = "1", features = ["derive"] }
```

Runtime features (pick one):

- `tokio` (default) — Diesel work runs on `tokio::task::spawn_blocking`.
- `ntex` — Diesel work runs on `ntex_rt::spawn_blocking`. Enable with
  `--no-default-features --features ntex`.
- no feature — compile error: without a runtime every Diesel query would
  execute inline on the async caller and stall the executor.

If both `tokio` and `ntex` are enabled, `tokio` wins while a Tokio runtime
is present; outside one (e.g. on the ntex executor) the work falls back to
ntex's blocking pool. Treat `--all-features` as a compatibility check, not
a runtime shape.

## Quick start

Build a pool, run the migrations once at startup, create a storage. The
worker side (poll/lock/ack) follows the regular apalis APIs — see the
[Running an apalis worker](#running-an-apalis-worker) section below for a
full end-to-end wiring.

```rust,no_run
# async fn run() -> Result<(), apalis_diesel_postgres::Error> {
use apalis_diesel_postgres::{Config, PostgresStorage, build_pool, setup};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct SendEmail {
    to: String,
}

let pool = build_pool("postgres://127.0.0.1:5432/app")?;
setup(&pool).await?;

let storage = PostgresStorage::<SendEmail>::new_with_config(
    &pool,
    &Config::new("emails"),
);
# let _ = storage;
# Ok(())
# }
```

To enqueue tasks from a request handler atomically with business data, see
the [outbox section](#transactional-enqueue-outbox-pattern). To run the
example end-to-end against a real database, see
[`examples/outbox.rs`](https://github.com/AlexeyMatskevich/apalis-diesel-postgres/blob/master/examples/outbox.rs):

```sh
DATABASE_URL=postgres://127.0.0.1:5432/apalis_diesel_postgres \
    cargo run --example outbox --features tokio
```

## Running an apalis worker

Add `apalis` to your `Cargo.toml`; it re-exports `WorkerBuilder`, `Worker`,
`Data` and the `BoxDynError` alias used by handlers. `PostgresStorage<T>`
satisfies apalis's `Backend + Send + Sync` requirement, so it slots straight
into `WorkerBuilder::backend(...)`.

```rust,no_run
# use apalis::prelude::*;
# use apalis_diesel_postgres::{Config, PostgresStorage, build_pool, setup};
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct SendEmail { to: String }
# async fn handle_email(job: SendEmail) -> Result<(), BoxDynError> {
#     println!("sending to {}", job.to);
#     Ok(())
# }
# async fn run() -> Result<(), BoxDynError> {
let pool = build_pool("postgres://127.0.0.1:5432/app")?;
setup(&pool).await?;

let storage: PostgresStorage<SendEmail> =
    PostgresStorage::new_with_config(&pool, &Config::new("emails"));

WorkerBuilder::new("emails-worker")
    .backend(storage)
    .build(handle_email)
    .run()
    .await?;
# Ok(())
# }
```

### Calling `push_with_conn` from inside a handler

The outbox pattern isn't limited to HTTP handlers — the same transactional
guarantees apply when one job needs to enqueue a follow-up job atomically
with its own database writes. Inject the follow-up queue's storage **and a
separate business pool** via `Data<...>`, hop onto the blocking pool, and
share a `&mut PgConnection` between the business write and `push_with_conn`.
The transaction's connection comes from the business pool, not from
`storage.pool()` — handler transactions on the apalis pool would compete
with fetch/ack/heartbeat and can trigger the cascade described under
[Connection pool isolation](#connection-pool-isolation):

```rust,no_run
# use apalis::prelude::*;
# use apalis_diesel_postgres::{Error as PgError, PgPool, PostgresStorage};
# use diesel::Connection;
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct SendEmail { to: String }
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct LogActivity { kind: String, target: String }
async fn handle_email(
    job: SendEmail,
    backend_pool: Data<PgPool>,
    activity: Data<PostgresStorage<LogActivity>>,
) -> Result<(), BoxDynError> {
    let backend_pool = (*backend_pool).clone();
    let activity = (*activity).clone();
    let to = job.to.clone();

    tokio::task::spawn_blocking(move || -> Result<(), PgError> {
        // Business pool, NOT activity.pool(): the apalis pool stays
        // dedicated to fetch/ack/heartbeat/listener work.
        let mut conn = backend_pool.get().map_err(PgError::Pool)?;
        conn.transaction(|c| {
            // Your business write goes here — same connection, same txn.
            activity.push_with_conn(c, LogActivity {
                kind: "email_sent".to_owned(),
                target: to,
            })?;
            Ok::<_, PgError>(())
        })
    })
    .await??;
    Ok(())
}
```

Wire the follow-up storage and the business pool into the worker via
`.data(...)`:

```rust,no_run
# use apalis::prelude::*;
# use apalis_diesel_postgres::{PgPool, PostgresStorage};
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct SendEmail { to: String }
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct LogActivity { kind: String, target: String }
# async fn handle_email(
#     _: SendEmail,
#     _: Data<PgPool>,
#     _: Data<PostgresStorage<LogActivity>>,
# ) -> Result<(), BoxDynError> { Ok(()) }
# fn wire(
#     backend_pool: PgPool,
#     emails: PostgresStorage<SendEmail>,
#     activity: PostgresStorage<LogActivity>,
# ) {
let worker = WorkerBuilder::new("emails-worker")
    .backend(emails)
    .data(backend_pool)
    .data(activity)
    .build(handle_email);
# let _ = worker;
# }
```

End-to-end runnable example: [`examples/worker.rs`](https://github.com/AlexeyMatskevich/apalis-diesel-postgres/blob/master/examples/worker.rs).

```sh
DATABASE_URL=postgres://127.0.0.1:5432/apalis_diesel_postgres \
    cargo run --example worker --features tokio
```

## Transactional enqueue (outbox pattern)

When a request handler must persist a resource (an order, a user, a file
upload) **and** enqueue a follow-up job (send confirmation email, kick off
processing), the two writes have to either both happen or both not happen.
If they live in different transactions, you get the classic split-brain:

- Resource written, job missing → silent loss of work.
- Job written, resource rolled back → consumer wakes up, fails to find the
  row, retries forever.

`PostgresStorage::push_with_conn` lets you insert the apalis task on the
**same** `&mut PgConnection` your handler is already using, so the task
INSERT is part of your business transaction. If the transaction commits, the
job is enqueued; if it rolls back, no job is enqueued and no `NOTIFY` is
delivered. There is no manual outbox table to drain.

```rust,no_run
# use apalis_diesel_postgres::{Config, PgPool, PgTaskId, PostgresStorage};
# use diesel::{Connection, RunQueryDsl, sql_query, PgConnection};
# use diesel::r2d2::{ConnectionManager, Pool};
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct SendConfirmationEmail { order_id: i64 }
# async fn create_order(
#     backend_pool: PgPool,
#     storage: PostgresStorage<SendConfirmationEmail>,
#     order_id: i64,
# ) -> Result<PgTaskId, Box<dyn std::error::Error + Send + Sync>> {
let task_id = tokio::task::spawn_blocking(move || {
    let mut conn = backend_pool.get()?;
    conn.transaction::<_, Box<dyn std::error::Error + Send + Sync>, _>(|c| {
        // Business write — your service's own table.
        sql_query("INSERT INTO orders (id, status) VALUES ($1, 'pending')")
            .bind::<diesel::sql_types::BigInt, _>(order_id)
            .execute(c)?;

        // Apalis enqueue — same connection, same transaction.
        let id = storage.push_with_conn(c, SendConfirmationEmail { order_id })?;
        Ok(id)
    })
})
.await??;
# Ok(task_id)
# }
```

Key points:

- `backend_pool` is **your** service's pool, separate from the pool you hand
  to `PostgresStorage`. See [Connection pool isolation](#connection-pool-isolation).
- The whole block runs inside `tokio::task::spawn_blocking` (or
  `ntex_rt::spawn_blocking` on the `ntex` runtime — see the Runtime features
  section above) — `push_with_conn` is synchronous and would otherwise stall
  the runtime.
- `NOTIFY` fires when the outer transaction commits, so listeners only
  observe committed work.

### `push_task_with_conn` — full control

`push_with_conn(args)` is the ergonomic path: auto Ulid, default scheduling.
For `idempotency_key`, `priority`, `run_at` (delayed run), `max_attempts`,
custom `metadata`, or a pre-allocated `task_id`, build a [`PgTask<Args>`]
and call `push_task_with_conn`:

```rust,no_run
# use apalis_diesel_postgres::{PgTask, PgTaskId, PostgresStorage};
# use diesel::PgConnection;
# use std::time::{SystemTime, UNIX_EPOCH};
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct Reminder { order_id: i64 }
# fn enqueue(
#     conn: &mut PgConnection,
#     storage: &PostgresStorage<Reminder>,
#     order_id: i64,
# ) -> Result<PgTaskId, apalis_diesel_postgres::Error> {
let mut task = PgTask::<Reminder>::new(Reminder { order_id });
task.parts.idempotency_key = Some(format!("reminder:{order_id}"));
task.parts.run_at = SystemTime::now()
    .duration_since(UNIX_EPOCH)
    .unwrap_or_default() // now() predates UNIX_EPOCH only on a broken clock
    .as_secs()
    + 24 * 3600; // tomorrow

storage.push_task_with_conn(conn, task)
# }
```

### Constraints

- **Synchronous** — wrap in `tokio::task::spawn_blocking` from async code so
  the whole transaction stays on one blocking task.
- **Don't reuse this connection** for unrelated apalis operations
  (fetch/ack/heartbeat) — those live on the apalis pool.
- **Idempotency conflict** rolls back only the apalis batch via SAVEPOINT;
  the outer transaction stays alive. Decide whether to commit your business
  writes or roll the whole transaction back when you get
  `Err(Error::IdempotencyConflict { .. })` — match the variant, not the
  message text. One duplicate key rolls back the *whole* enqueue batch, not
  just the colliding row.
- **No outer transaction** → Diesel auto-commits the INSERT; the call still
  works, but you lose the outbox guarantee.

## Connection pool isolation

**Do not share the apalis pool with your HTTP request handlers or other
unrelated workloads.** Each listener holds one connection for its lifetime;
fetch, acknowledgement and heartbeat borrow connections for their individual
operations. If the application
exhausts the pool under load, the fetcher and heartbeat stall; lifecycle
marks the worker dead and re-enqueues its in-flight tasks, which produces
more load on the same pool — a cascading failure that is hard to recover
from while it is happening.

Run two separate `r2d2` pools against the same PostgreSQL database — one
for your web service, one for apalis — and size them independently:

```rust,no_run
use apalis_diesel_postgres::{Config, PostgresStorage, build_pool_with};

// Web/backend pool — sized for request concurrency.
let backend_pool = build_pool_with(
    "postgres://127.0.0.1:5432/app",
    |b| b.max_size(20).connection_timeout(std::time::Duration::from_secs(2)),
)?;

// Apalis pool — sized for worker concurrency + lifecycle + listeners.
// Rough rule: worker_concurrency + 2 + listeners.
let apalis_pool = build_pool_with(
    "postgres://127.0.0.1:5432/app",
    |b| b.max_size(8).connection_timeout(std::time::Duration::from_secs(2)),
)?;

let storage = PostgresStorage::<()>::new_with_config(&apalis_pool, &Config::new("emails"));
// Use `backend_pool.get()` + `storage.push_with_conn(conn, args)` from
// request handlers to enqueue inside business transactions.
# Ok::<_, Box<dyn std::error::Error>>(())
```

Recommendations:

- Set a short `connection_timeout` (1–3 s) on both pools so a starved pool
  fails loudly instead of hanging request handlers.
- Set `statement_timeout` on the session via your connection setup if
  workloads need it.
- Monitor pool saturation via `pool.state()` (`connections`,
  `idle_connections`) on both pools.

## Storage modes

```rust
use apalis_diesel_postgres::{Config, PostgresStorage, SharedPostgresStorage};
use diesel::{PgConnection, r2d2::{ConnectionManager, Pool}};
type PgPool = Pool<ConnectionManager<PgConnection>>;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct EmailJob { to: String }

fn build(pool: PgPool) {
    let config = Config::new("emails");

    // Polling fetcher (the default). Queue name = `Args` type name.
    let _polling = PostgresStorage::<EmailJob>::new(&pool);

    // Polling fetcher with an explicit queue name (use this for stable queues).
    let _polling = PostgresStorage::<EmailJob>::new_with_config(&pool, &config);

    // Polling + LISTEN/NOTIFY wakeups (lower latency, dedicated connection).
    let _notify = PostgresStorage::<EmailJob>::new_with_notify(&pool, &config);

    // One listener shared across many queues, registered via apalis `MakeShared`.
    let _shared: SharedPostgresStorage = SharedPostgresStorage::new(&pool);
}
```

`new_with_notify` and `SharedPostgresStorage` use `LISTEN
"apalis::job::insert"` to wake workers on insert. Polling stays as a
fallback. Each notify-mode worker stream owns a listener that pins one extra
pooled connection. Shared storage shares one listener connection among its
subscribers. Size the apalis pool for these connections.

## Polling strategies and worker recovery

`PostgresStorage::new(pool)` recreates the default polling strategy for every
consumer. The upstream `Config` stores one-shot strategy streams: cloning an
arbitrary config does not clone those streams. When reusing custom configuration
across workers, supply a factory so each consumer gets its own strategy:

```rust
use std::time::Duration;
use apalis_core::backend::poll_strategy::IntervalStrategy;
use apalis_diesel_postgres::{Config, PgPool, PostgresStorage};

fn storage(pool: &PgPool) -> PostgresStorage<String> {
    PostgresStorage::new_with_config(pool, &Config::new("emails"))
        .with_poll_strategy_factory(|| IntervalStrategy::new(Duration::from_millis(100)))
}
```

This also applies to notification and shared storage built from a `Config`.
An exhausted strategy produces `PollStrategyExhausted` and ends its polling
branch. It cannot authorize further polling. A finite custom strategy must
explicitly define how polling continues; creating fresh configuration or using
the factory is the upgrade path for previously shared one-shot strategies.

A claimed row whose payload fails to decode is released through the retry
budget before the batch continues; a release that keeps failing after a few
seconds of retries retires the worker registration so orphan recovery can
reclaim the row and the rest of that batch.
Each claim is acknowledged at most once. An in-process re-dispatch of an
already acknowledged claim, for example by a retry layer placed outside the
backend middleware, is refused with `AlreadyAcknowledged` before the handler
runs again; the persisted retry budget schedules the next attempt, and the
worker registration stays active.
If automatic acknowledgement loses its result through a database or
serialization error, the affected worker registration is retired locally.
A claim whose transaction produced tasks but whose commit cannot be confirmed
returns `ClaimOutcomeUnknown` and also retires the registration. This includes
connection loss and a panic in user instrumentation after commit; the original
error remains available as its source. A typed server rejection whose outcome
is known (such as a serialization or constraint failure) instead returns the
ordinary database error and does not retire the local registration. A manual
stream consumer can continue after that error; Apalis treats stream errors as
fatal. Errors before a claim and empty fetches also do not retire the local
registration. Low-level token-free callers
must resolve `ClaimOutcomeUnknown` before renewing that worker's heartbeat.

A registration created through the admin `RegisterWorker` trait carries no
lease token and has no heartbeat: its liveness is the time of its last
`register_worker` call. Callers that claim through `lock_task`,
`lock_task_in_queue` or `apalis.get_jobs` under such a name must re-register
within `reenqueue_orphaned_after`, or their `Running` and `Queued` tasks are
recovered as orphans. While that registration is fresh, a worker stream
registering the same name receives `AlreadyRegistered`; once it is stale, the
stream takes the name over and recovers its claims first.
Dropping a polling stream after it has yielded its registration also retires
that registration. A stream whose registration failed, for example with
`AlreadyRegistered` or a pool error, owns no claim; dropping it leaves the
name free, so a clone of the same storage can register once the holder is
stale.
The retired worker stops claiming new tasks and refreshing its heartbeat;
other worker names sharing the storage remain independent. This is a fail-stop
policy: with Apalis `Worker::run` or a monitored worker, the resulting stream
error drops that worker's pending service futures, including other handlers and
acknowledgements already in progress. There is no automatic graceful drain after
an acknowledgement failure. An independently retained acknowledgement may still
complete; submitted blocking SQL may also finish after its waiter is dropped.
Dropping a stream that has never been polled leaves the registration active.
To restart, construct fresh storage with a fresh `Config` or a poll strategy
factory. Cloning retired storage preserves its retired local registration;
reusing a consumed `Config` strategy can immediately exhaust polling. A fresh
storage token cannot take over a still-fresh registration with the same name:
restart may need to wait for the stale deadline. After the last committed
heartbeat expires, another worker can recover unfinished tasks.
Recovery counts a lost attempt and respects the retry budget. Taking over the
same worker name recovers all of that registration's claims before renewal;
this can be a large transaction after a large in-flight batch.
Storage clones retain one local liveness record per distinct worker name until
all clones are dropped. Bound the set of worker names for long-lived storage.

Canceling a temporary `stream.next()` wait leaves the stream and its buffered
tasks intact. Canceling an enqueue/fetch/ack waiter after its blocking operation
has been submitted does not cancel the SQL or commit. Stop the worker gracefully
when possible, and treat an interrupted enqueue result as potentially committed.
Manual acknowledgers must retain their completion obligation or stop their
worker heartbeat if acknowledgement cannot be completed.

Enqueue deduplication is scoped to `(queue, idempotency_key)` while the row
exists. It does not prevent repeated delivery or repeated external handler
effects after a crash, retirement or unknown acknowledgement outcome. Handlers
need their own idempotency for those effects. No exactly-once execution is
provided.

## Buffered enqueue completion

`Sink::start_send` and `SinkExt::feed` accept tasks into the local buffer. A
successful `flush` or `close` confirms that all accepted tasks have been written,
including tasks buffered while an earlier batch was still in flight. Dropping a
temporary flush waiter leaves that work in the sink; another flush can finish it.

A failed batch returns its original error once. That sink then returns
`Error::SinkFailed` from readiness, flush and close. Changing its codec preserves
the failed state; cloning storage creates a fresh, empty sink. Keep submitted task
IDs and idempotency keys so that an uncertain database outcome can be reconciled
before resubmitting through a fresh sink. The backend does not automatically replay
a failed batch. `SinkBufferFull` before a task is accepted is a recoverable
capacity error and does not fail the pipeline.

## Operational boundaries

Use trusted PostgreSQL roles for schema changes and direct table access. Lease
tokens fence cooperating worker instances; callers with the pool or table write
privileges can bypass the public worker protocol. Configure libpq TLS explicitly
for the deployment, including certificate and hostname verification where
required. The crate does not override connection-string TLS policy.

On macOS with MIT Kerberos 1.22.1, process exit can race with background r2d2
connection establishment and trigger a native assertion.
Dropping storage or a pool does not join an in-flight connection attempt.
This native shutdown issue remains unresolved. A successful Cargo exit code
alone does not establish that native shutdown was clean.
For deployments using neither GSS authentication nor GSS encryption,
`PGGSSENCMODE=disable` avoids the affected credential-probing path. This is an
explicit deployment mitigation; the crate leaves GSS policy to the caller.
GSS-enabled macOS deployments need a verified native shutdown remedy.

Payload, metadata, key and queue limits apply per value. A batch iterator and a
successful handler result have no aggregate memory limit; bound them at the
application boundary. Codecs and result serialization run synchronously and
must be appropriate for the executor. Database and handler error details can
contain application data; apply the application's logging policy.

Notifications are hints; a disconnected listener or lost hint requires a
continuing polling strategy. A listener error remains observable even when its
bounded ID buffer is full. PostgreSQL's server-side notification queue is a
separate resource: exhaustion can reject a transaction's commit. Monitor it
alongside pool saturation, heartbeat age, task backlog and database maintenance.
Completed rows remain until application retention removes them; plan vacuum,
retention and snapshot refresh for the actual workload. OFFSET pages have a
total order on unchanged data, but do not provide a shared snapshot across
concurrent writes. SKIP LOCKED does not promise fairness under sustained load.

Even when no jobs are ready, polling checks worker ownership and runs a claim
transaction. Pool validation can add a connection check, and PostgreSQL row
locking can generate WAL. Account for this idle database load when choosing
polling intervals and worker counts; measure it with the deployment's pool and
database settings.

## Examples

[`examples/outbox.rs`](https://github.com/AlexeyMatskevich/apalis-diesel-postgres/blob/master/examples/outbox.rs) — runnable, demonstrates commit /
rollback / idempotency-conflict behaviour against a real database:

```sh
DATABASE_URL=postgres://127.0.0.1:5432/apalis_diesel_postgres \
    cargo run --example outbox --features tokio
```

## Runtime errors

The backend annotates common database failures with operation context so
worker logs point at the failed lifecycle step:

- Missing migrations: `database error while fetching queued jobs: …`,
  with a hint to call `apalis_diesel_postgres::setup(&pool).await`.
- Pool acquisition failures mention `DATABASE_URL`, PostgreSQL reachability,
  and pool capacity.
- Lock failures for non-lockable jobs: `task not found while locking task`,
  with the task id and queue. Usually means the job is delayed, completed,
  already locked, out of retry attempts, or in another queue.
- Acknowledgement races: `stale acknowledgement` when the stored lock no
  longer matches the worker/attempt/lock timestamp being ack'd.
- Heartbeat failures for missing worker rows: `worker not registered`,
  instead of a generic update-count mismatch.
- Codec failures: `failed to decode task payload or result with the
  configured codec` — payload was written with a different codec or is
  corrupt.
- Notification listener failures surface as stream errors. Polling still
  fetches jobs; `LISTEN`/`NOTIFY` wakeups stop until the notify stream is
  recreated.
- Idempotency conflicts: `Error::IdempotencyConflict { job_type,
  conflicting_keys, total }` when an enqueue collides with the
  `(job_type, idempotency_key)` unique constraint. `conflicting_keys` names the
  exact keys that collided — against stored rows *or between tasks in the same
  batch* — so to re-enqueue, deduplicate rather than drop: keep one task per
  conflicting key (dropping all of them would silently lose intra-batch
  duplicates that have no stored row yet) and resubmit. Match the variant (not
  the message text) to treat a duplicate as benign. One duplicate rolls back the *whole* batch, not just the colliding
  row; a surrounding transaction stays alive. The `push_*_with_conn` outbox
  methods and the `Sink` implementation return `Error` directly. The
  `TaskSink` API wraps a push failure as
  `TaskSinkError::PushError(Error::IdempotencyConflict { .. })`. After a buffered
  Sink flush fails, use a fresh sink for any reconciled retry as described above.

## Public types

```rust
use apalis_diesel_postgres::{
    CompactType, Config, JsonCodec, MIGRATIONS, PgContext, PgPool, PgTask,
    PgTaskId, PostgresStorage, SharedPostgresStorage, TaskRow, build_pool, setup,
};
```

Type aliases:

- `PgPool = Pool<ConnectionManager<PgConnection>>`
- `PgContext = SqlContext<PgPool>`
- `PgTask<Args> = Task<Args, PgContext, Ulid>`
- `PgTaskId = TaskId<Ulid>`
- `CompactType = Vec<u8>`

## Local development

```sh
nix develop                # dev shell with rust, diesel, postgres
nix run .#services         # start local PostgreSQL on 127.0.0.1:5432
```

The shell exports `DATABASE_URL=postgres://127.0.0.1:5432/apalis_diesel_postgres`
and stores data in `./.pgdata`. Editor config for Zed is generated automatically;
opt in to MCP config with `APALIS_DIESEL_POSTGRES_WRITE_MCP=1 nix develop`.

For the full pre-PR check list (`cargo fmt`, multiple `cargo check`/`cargo
test` matrices, doc warnings), see
[CONTRIBUTING.md](https://github.com/AlexeyMatskevich/apalis-diesel-postgres/blob/master/CONTRIBUTING.md). The
quick smoke path is:

```sh
env -u DATABASE_URL cargo test --features tokio --lib
DATABASE_URL=postgres://127.0.0.1:5432/apalis_diesel_postgres \
    APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE=1 \
    cargo test --features tokio -- --test-threads=1
```
