# apalis-diesel-postgres

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
`apalis-sql 1.0.0-rc.9`, `apalis-codec 0.1.0-rc.9`, `diesel 2.3`. Schema
compatible with Apalis SQL storage (`apalis.jobs`, `apalis.workers`).

## Installation

```toml
[dependencies]
apalis-diesel-postgres = { git = "https://github.com/AlexeyMatskevich/apalis-diesel-postgres", features = ["tokio"] }
diesel = { version = "2.3", features = ["postgres", "r2d2", "chrono", "serde_json"] }
serde = { version = "1", features = ["derive"] }
```

Runtime features (pick one):

- `tokio` (default) — Diesel work runs on `tokio::task::spawn_blocking`.
- `ntex` — Diesel work runs on `ntex_rt::spawn_blocking`.
- no feature — Diesel work runs on the calling thread (compile-time
  compatibility only; can stall an async executor under load).

If both `tokio` and `ntex` are enabled, `tokio` wins. Treat `--all-features`
as a compatibility check, not a runtime shape.

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
[`examples/outbox.rs`](examples/outbox.rs):

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
with its own database writes. Inject the follow-up queue's storage via
`Data<...>`, hop onto the blocking pool, and share a `&mut PgConnection`
between the business write and `push_with_conn`:

```rust,no_run
# use apalis::prelude::*;
# use apalis_diesel_postgres::{Error as PgError, PostgresStorage};
# use diesel::Connection;
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct SendEmail { to: String }
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct LogActivity { kind: String, target: String }
async fn handle_email(
    job: SendEmail,
    activity: Data<PostgresStorage<LogActivity>>,
) -> Result<(), BoxDynError> {
    let activity = (*activity).clone();
    let to = job.to.clone();

    tokio::task::spawn_blocking(move || -> Result<(), PgError> {
        let mut conn = activity.pool().get().map_err(PgError::Pool)?;
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

Wire the follow-up storage into the worker via `.data(...)`:

```rust,no_run
# use apalis::prelude::*;
# use apalis_diesel_postgres::PostgresStorage;
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct SendEmail { to: String }
# #[derive(Debug, serde::Deserialize, serde::Serialize)]
# struct LogActivity { kind: String, target: String }
# async fn handle_email(_: SendEmail, _: Data<PostgresStorage<LogActivity>>)
#     -> Result<(), BoxDynError> { Ok(()) }
# fn wire(
#     emails: PostgresStorage<SendEmail>,
#     activity: PostgresStorage<LogActivity>,
# ) {
let worker = WorkerBuilder::new("emails-worker")
    .backend(emails)
    .data(activity)
    .build(handle_email);
# let _ = worker;
# }
```

End-to-end runnable example: [`examples/worker.rs`](examples/worker.rs).

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
- The whole block runs inside `tokio::task::spawn_blocking` —
  `push_with_conn` is synchronous and would otherwise stall the runtime.
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
    .unwrap()
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
  writes or roll the whole transaction back from `Err(InvalidArgument)`.
- **No outer transaction** → Diesel auto-commits the INSERT; the call still
  works, but you lose the outbox guarantee.

## Connection pool isolation

**Do not share the apalis pool with your HTTP request handlers or other
unrelated workloads.** Apalis holds long-lived connections (fetcher
polling/`LISTEN`, lifecycle keep-alive, listener thread). If the application
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
    let _shared: SharedPostgresStorage = SharedPostgresStorage::new(pool);
}
```

`new_with_notify` and `SharedPostgresStorage` use `LISTEN
"apalis::job::insert"` to wake workers on insert. Polling stays as a
fallback. Each notify-mode storage pins one extra pooled connection while
the listener is alive — size the apalis pool accordingly.

## Examples

[`examples/outbox.rs`](examples/outbox.rs) — runnable, demonstrates commit /
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
  already locked, or in another queue.
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
test` matrices, doc warnings), see [CONTRIBUTING.md](CONTRIBUTING.md). The
quick smoke path is:

```sh
env -u DATABASE_URL cargo test --features tokio --lib
DATABASE_URL=postgres://127.0.0.1:5432/apalis_diesel_postgres \
    APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE=1 \
    cargo test --features tokio -- --test-threads=1
```
