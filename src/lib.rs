#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

use std::{fmt::Debug, future::Future, marker::PhantomData, time::Duration};

pub use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, BackendExt, TaskStream, codec::Codec, queue::Queue},
    task::{Task, task_id::TaskId},
    worker::context::WorkerContext,
};
pub use apalis_sql::{config::Config, from_row::TaskRow};
use diesel::{
    PgConnection,
    r2d2::{ConnectionManager, Pool},
};
use futures::{StreamExt, TryStreamExt};
use ulid::Ulid;

pub use crate::{
    ack::{PgAck, PgMiddleware, lock_task, lock_task_in_queue},
    error::Error,
    fetcher::{PgFetcher, PgNotify},
    lifecycle::{refresh_queue_stats_snapshot, setup, verify_schema},
    pool::{build_pool, build_pool_with},
    queries::migrations::MIGRATIONS,
    shared::{SharedFetcher, SharedPostgresError, SharedPostgresStorage},
};
// `PgSink` is an internal implementation detail of `PostgresStorage`: the
// public `Sink<PgTask>` impl lives on `PostgresStorage`, not on `PgSink`, and
// `PgSink`'s own methods are crate-private, so a re-exported `PgSink` gave
// downstream code nothing usable. Kept in scope for the `PostgresStorage.sink`
// field but no longer part of the public API.
use crate::sink::PgSink;

mod ack;
mod admin;
#[cfg(all(test, feature = "tokio"))]
mod async_specs;
mod error;
mod fetcher;
mod lease;
mod lifecycle;
#[cfg(all(test, feature = "tokio"))]
mod query_specs;

#[cfg(all(test, feature = "tokio"))]
#[path = "../tests/support/mod.rs"]
mod test_support;
#[cfg(test)]
#[path = "../tests/support/unreachable.rs"]
mod unreachable;

#[cfg(test)]
#[path = "../tests/support/unreachable_specs.rs"]
mod unreachable_specs;
#[cfg(test)]
extern crate self as apalis_diesel_postgres;
mod models;
mod notify_event;
mod pool;
mod queries;
mod runtime;
mod shared;
mod sink;

pub(crate) use notify_event::InsertEvent;
pub mod schema;

/// Diesel/r2d2 PostgreSQL pool accepted by this backend.
pub type PgPool = Pool<ConnectionManager<PgConnection>>;
/// SQL context associated with PostgreSQL tasks.
pub type PgContext = apalis_sql::context::SqlContext<PgPool>;
/// Apalis task stored in PostgreSQL.
pub type PgTask<Args> = Task<Args, PgContext, Ulid>;
/// PostgreSQL task identifier.
pub type PgTaskId = TaskId<Ulid>;
/// Compact serialized payload representation.
pub type CompactType = Vec<u8>;

/// Canonical `apalis.workers.storage_name` value for this backend. Shared by
/// the worker registration path and the admin `RegisterWorker` UPSERT so they
/// cannot drift apart.
pub(crate) const STORAGE_NAME: &str = "PostgresStorage";

/// Returns the crate name.
///
/// Sourced from `CARGO_PKG_NAME` so it cannot drift from `Cargo.toml` if the
/// crate is ever renamed.
#[must_use]
pub const fn crate_name() -> &'static str {
    env!("CARGO_PKG_NAME")
}

// apalis `WorkerBuilder::build()` requires the backend to be `Send + Sync`.
const _: fn() = || {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PostgresStorage<()>>();
    assert_send_sync::<PostgresStorage<(), JsonCodec<CompactType>, PgNotify>>();
    assert_send_sync::<PostgresStorage<(), JsonCodec<CompactType>, SharedFetcher>>();
    assert_send_sync::<SharedPostgresStorage<()>>();
};

/// What [`PostgresStorage::run_released`] observed: the worker's own result
/// and the release that followed it.
#[derive(Debug)]
pub struct ReleasedRun<T> {
    /// The value the worker future resolved to.
    pub outcome: T,
    /// The release: the number of tasks handed back, or why nothing was
    /// released (see [`PostgresStorage::release_worker`]).
    pub released: Result<usize, Error>,
}

/// PostgreSQL storage backend implemented with Diesel.
pub struct PostgresStorage<
    Args,
    Codec = JsonCodec<CompactType>,
    Fetcher = PgFetcher<CompactType, Codec>,
> {
    _marker: PhantomData<(Args, Codec)>,
    pub(crate) pool: PgPool,
    pub(crate) config: Config,
    pub(crate) fetcher: Fetcher,
    pub(crate) sink: PgSink<Args, Codec>,
    /// Registration token generated per storage instance and shared by clones.
    /// Managed operations check it to fence older registrations. This is not an
    /// authorization boundary against callers with direct table/pool access.
    pub(crate) lease_token: std::sync::Arc<str>,
    pub(crate) leases: crate::lease::LeaseRegistry,
    pub(crate) poll_factory: Option<crate::fetcher::PollStrategyFactory>,
}

// Manual Unpin requires Fetcher: Unpin so pinning guarantees from a
// `!Unpin` fetcher (e.g. one holding a self-referential future) are
// honoured by the storage wrapper. All built-in fetchers (PgFetcher,
// PgNotify, SharedFetcher) satisfy this bound.
impl<Args, Codec, Fetcher: Unpin> Unpin for PostgresStorage<Args, Codec, Fetcher> {}

impl<Args, Codec, Fetcher: Debug> Debug for PostgresStorage<Args, Codec, Fetcher> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStorage")
            .field("config", &self.config)
            .field("fetcher", &self.fetcher)
            .finish_non_exhaustive()
    }
}

impl<Args, Codec, Fetcher: Clone> Clone for PostgresStorage<Args, Codec, Fetcher> {
    fn clone(&self) -> Self {
        Self {
            _marker: PhantomData,
            pool: self.pool.clone(),
            config: self.config.clone(),
            fetcher: self.fetcher.clone(),
            sink: self.sink.clone(),
            lease_token: self.lease_token.clone(),
            leases: self.leases.clone(),
            poll_factory: self.poll_factory.clone(),
        }
    }
}

impl<Args> PostgresStorage<Args> {
    /// Create storage for the queue named after `Args`.
    ///
    /// **Do not share `pool` with HTTP request handlers or other unrelated
    /// workloads.** A notification listener holds one connection for its
    /// lifetime. Fetch, heartbeat, and ack borrow a connection per operation.
    /// Exhausting a shared pool can stall these operations and cause orphan reenqueue
    /// cascades. See the README section "Connection pool isolation" for the
    /// recommended sizing and the [`Self::push_with_conn`] outbox API for the
    /// supported way to enqueue from a backend transaction.
    #[must_use]
    pub fn new(pool: &PgPool) -> Self {
        let config = Config::new(std::any::type_name::<Args>());
        Self::new_with_config(pool, &config)
            .with_poll_strategy_factory(|| Config::default().poll_strategy().clone())
    }

    /// Create storage with an explicit Apalis SQL config.
    ///
    /// The config carries a single-use polling strategy, shared by its clones.
    /// Before creating multiple worker streams from this config or storage, set
    /// [`Self::with_poll_strategy_factory`] to build a fresh strategy per stream.
    ///
    /// **Do not share `pool` with HTTP request handlers or other unrelated
    /// workloads** — see [`Self::new`] for the rationale.
    #[must_use]
    pub fn new_with_config(pool: &PgPool, config: &Config) -> Self {
        Self {
            _marker: PhantomData,
            pool: pool.clone(),
            config: config.clone(),
            fetcher: PgFetcher::default(),
            sink: PgSink::new(pool, config),
            lease_token: queries::worker::mint_lease_token().into(),
            leases: Default::default(),
            poll_factory: None,
        }
    }

    /// Create storage that also listens for PostgreSQL notifications.
    ///
    /// The config carries a single-use polling strategy, shared by its clones.
    /// Before creating multiple worker streams from this config or storage, set
    /// [`Self::with_poll_strategy_factory`] to build a fresh strategy per stream.
    ///
    /// Notify mode uses a dedicated pooled connection for `LISTEN
    /// "apalis::job::insert"` while the polling stream is alive. **Each active
    /// worker stream spawns one listener thread and pins one pool connection.**
    /// Construction alone does not acquire that connection.
    /// If you need notify-driven dequeue across many
    /// queues, prefer [`crate::SharedPostgresStorage`] — it spawns a single
    /// listener thread shared by all queues registered with it, so the
    /// thread/connection cost stays at one regardless of queue count.
    ///
    /// **Do not share `pool` with HTTP request handlers or other unrelated
    /// workloads** — see [`Self::new`] for the rationale.
    #[must_use]
    pub fn new_with_notify(
        pool: &PgPool,
        config: &Config,
    ) -> PostgresStorage<Args, JsonCodec<CompactType>, PgNotify> {
        PostgresStorage {
            _marker: PhantomData,
            pool: pool.clone(),
            config: config.clone(),
            fetcher: PgNotify,
            sink: PgSink::new(pool, config),
            lease_token: queries::worker::mint_lease_token().into(),
            leases: Default::default(),
            poll_factory: None,
        }
    }

    /// Return the underlying Diesel/r2d2 pool.
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Return the queue configuration.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }
}

impl<Args, Codec, Fetcher> PostgresStorage<Args, Codec, Fetcher> {
    /// An acknowledger bound to this storage's registration token and local
    /// liveness, for manual acknowledgement of tasks claimed by this
    /// storage's worker streams.
    ///
    /// A failed acknowledgement retires the worker's local registration the
    /// same way the automatic middleware does, so orphan recovery can reclaim
    /// the task. [`PgAck::new`] and [`PgAck::with_lease_token`] bind no
    /// liveness and leave the heartbeat running after a failure.
    #[must_use]
    pub fn acknowledger(&self) -> PgAck {
        PgAck::with_lease_registry(
            &self.pool,
            std::sync::Arc::clone(&self.lease_token),
            self.leases.clone(),
        )
    }

    /// Release this storage's registration of `worker_id` for its queue: hand
    /// every `Running` or `Queued` task the worker still owns back to the
    /// queue, and mark the registration released so a successor can register
    /// the same name immediately instead of waiting for
    /// `reenqueue_orphaned_after`.
    ///
    /// Call this once the worker has stopped, after `Worker::run` (or
    /// `run_until`) returns, whether it returned an error or not. The local
    /// registration is retired first, so clones of this storage stop claiming
    /// and heartbeating under the name before the database is updated; a
    /// restart needs fresh storage, as after any retirement.
    ///
    /// Each recovered task consumes one attempt, like orphan recovery: the
    /// worker may have executed it partially. A task the worker acknowledged
    /// before stopping is not touched. With Apalis's graceful shutdown the
    /// worker drains its handlers first, so nothing is left to recover. The
    /// registration row itself is kept because completed tasks reference it
    /// as their last owner; [`Self::prune_workers`] removes it once nothing
    /// references it any more.
    ///
    /// Returns the number of tasks handed back.
    ///
    /// # Errors
    /// - [`Error::WorkerNotRegistered`] if the registration is absent, has
    ///   no lease token, or is owned by another storage. Nothing is released:
    ///   a successor that took the name over owns those claims now.
    /// - [`Error::Pool`], [`Error::Database`], [`Error::Blocking`] for
    ///   connection, SQL and executor failures. The database registration is
    ///   then still live; the periodic sweep recovers it once it is stale.
    pub async fn release_worker(&self, worker_id: &str) -> Result<usize, Error> {
        self.leases.for_worker(worker_id).retire();
        queries::release_worker(
            self.pool.clone(),
            self.config.clone(),
            worker_id.to_owned(),
            std::sync::Arc::clone(&self.lease_token),
        )
        .await
    }

    /// Run a worker to completion, then release its registration, whatever
    /// the run returned.
    ///
    /// `run` is typically `Worker::run_until(signal)`. The release follows
    /// on every exit path, so a redeploy under the same name can register at
    /// once; see [`Self::release_worker`] for what the release does and
    /// reports. Both results are returned: a run that failed still needs its
    /// error handled, and a release that failed leaves the registration to
    /// the periodic sweep.
    ///
    /// ```no_run
    /// # use apalis::prelude::*;
    /// # use apalis_diesel_postgres::{PgPool, PostgresStorage, ReleasedRun};
    /// # async fn handle(job: String) -> Result<(), BoxDynError> { Ok(()) }
    /// # async fn example(
    /// #     storage: PostgresStorage<String>,
    /// #     shutdown: impl std::future::Future<Output = Result<(), WorkerError>> + Send + 'static,
    /// # ) -> Result<(), BoxDynError> {
    /// let worker = WorkerBuilder::new("emails-worker")
    ///     .backend(storage.clone())
    ///     .build(handle);
    /// let ReleasedRun { outcome, released } = storage
    ///     .run_released("emails-worker", worker.run_until(shutdown))
    ///     .await;
    /// if let Err(error) = released {
    ///     eprintln!("registration stays until the stale deadline: {error}");
    /// }
    /// outcome?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn run_released<F>(&self, worker_id: &str, run: F) -> ReleasedRun<F::Output>
    where
        F: Future,
    {
        let outcome = run.await;
        let released = self.release_worker(worker_id).await;
        ReleasedRun { outcome, released }
    }

    /// Delete the terminal tasks of this storage's queue (`Done`, `Killed`,
    /// and `Failed` with no retry budget left) that completed at least
    /// `completed_before` ago, and return how many rows were deleted.
    ///
    /// Completed rows are never removed by the worker protocol; this is the
    /// retention step of the task lifecycle. Deletion runs in bounded batches,
    /// each its own transaction. A deleted task is no longer observable
    /// through `FetchById`, `WaitForCompletion` or the listings, and its
    /// `idempotency_key` becomes free for a new task, so keep
    /// `completed_before` longer than any consumer still waiting on results
    /// and than the deduplication horizon the application relies on.
    /// [`apalis_core::backend::Vacuum::vacuum`] is this method with a zero
    /// window.
    ///
    /// # Errors
    /// - [`Error::Pool`], [`Error::Database`], [`Error::Blocking`] for
    ///   connection, SQL and executor failures. Batches already deleted stay
    ///   deleted.
    pub async fn purge_terminal_tasks(&self, completed_before: Duration) -> Result<usize, Error> {
        queries::purge_terminal_tasks(
            self.pool.clone(),
            self.config.queue().to_string(),
            completed_before,
        )
        .await
    }

    /// Delete the registrations of this storage's queue that have been stale
    /// for at least `stale_for` and that no task references any more, and
    /// return how many rows were deleted.
    ///
    /// Registration rows are kept by the worker protocol so completed tasks
    /// can name their last owner; workers with unique names (one per process
    /// or pod) otherwise accumulate forever. A registration still referenced
    /// by a task, even a completed one, is kept until
    /// [`Self::purge_terminal_tasks`] removes that task. A registration in use
    /// by another transaction is skipped, and a live one is never stale, so
    /// this cannot remove a working registration. Pass a window no shorter
    /// than the longest `reenqueue_orphaned_after` any worker of the queue
    /// uses, so a merely slow heartbeat is never mistaken for an abandoned one.
    ///
    /// # Errors
    /// - [`Error::Pool`], [`Error::Database`], [`Error::Blocking`] for
    ///   connection, SQL and executor failures.
    pub async fn prune_workers(&self, stale_for: Duration) -> Result<usize, Error> {
        queries::prune_workers(
            self.pool.clone(),
            self.config.queue().to_string(),
            stale_for,
        )
        .await
    }

    /// Change the task codec while retaining pool, config, fetcher, and the
    /// sink's pipeline state (buffered tasks and any in-flight flush — the
    /// buffer holds already-encoded compact tasks, so switching the codec
    /// must not silently drop them).
    #[must_use]
    pub fn with_codec<NewCodec>(self) -> PostgresStorage<Args, NewCodec, Fetcher> {
        PostgresStorage {
            _marker: PhantomData,
            sink: self.sink.retype(),
            pool: self.pool,
            config: self.config,
            fetcher: self.fetcher,
            lease_token: self.lease_token,
            leases: self.leases,
            poll_factory: self.poll_factory,
        }
    }

    /// Build an independent polling strategy for each worker stream.
    ///
    /// Apalis SQL `Config` carries a single-use strategy even when cloned.
    /// Supply a factory when reusing a configured storage for multiple workers.
    /// The factory is called once per stream with its own polling context.
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use apalis_core::backend::poll_strategy::IntervalStrategy;
    /// # use apalis_diesel_postgres::{PostgresStorage, PgPool};
    /// # fn example(pool: &PgPool) {
    /// let storage = PostgresStorage::<String>::new(pool)
    ///     .with_poll_strategy_factory(|| IntervalStrategy::new(Duration::from_secs(1)));
    /// # }
    /// ```
    #[must_use]
    pub fn with_poll_strategy_factory<F, S>(mut self, factory: F) -> Self
    where
        F: Fn() -> S + Send + Sync + 'static,
        S: apalis_core::backend::poll_strategy::PollStrategy + 'static,
        S::Stream: Send + 'static,
    {
        self.poll_factory = Some(crate::fetcher::PollStrategyFactory::new(factory));
        self
    }

    /// Compose the keep-alive + reenqueue heartbeat stream shared by every
    /// `Backend` impl on this storage. Kept here so the per-fetcher
    /// `Backend::heartbeat` impls remain one-liners.
    pub(crate) fn heartbeat_stream(
        &self,
        worker: &WorkerContext,
    ) -> futures::stream::BoxStream<'static, Result<(), Error>> {
        // The poll stream refuses the same configuration at registration;
        // refusing it here as well keeps the first heartbeat tick from
        // reporting a misleading `WorkerNotRegistered` before that happens.
        if let Err(error) = queries::validate_liveness(&self.config) {
            return futures::stream::once(futures::future::ready(Err(error))).boxed();
        }
        let keep_alive = queries::keep_alive_stream(
            self.pool.clone(),
            self.config.clone(),
            worker.clone(),
            std::sync::Arc::clone(&self.lease_token),
        );
        let reenqueue = queries::reenqueue_orphaned_stream(self.pool.clone(), self.config.clone())
            .map_ok(|_| ());
        crate::fetcher::LeaseStream::new(
            futures::stream::select(keep_alive, reenqueue),
            self.leases.for_worker(worker.name()),
            false,
        )
        .boxed()
    }
}

/// Transactional enqueue on a caller-supplied connection — the **outbox**
/// entry point. Use these methods when you want the task INSERT to share a
/// transaction with your business-data writes.
impl<Args, EncodeCodec, Fetcher> PostgresStorage<Args, EncodeCodec, Fetcher>
where
    EncodeCodec: Codec<Args, Compact = CompactType>,
    EncodeCodec::Error: std::error::Error + Send + Sync + 'static,
{
    /// Enqueue a task using a caller-supplied `PgConnection`.
    ///
    /// For transactional outbox semantics, call this inside
    /// `conn.transaction(|c| ...)` together with your business-data writes —
    /// the INSERT into `apalis.jobs` is committed only if the outer
    /// transaction commits. Without an outer transaction, Diesel auto-commits
    /// the INSERT (same behaviour as the pool-based `Sink<Task>` path).
    ///
    /// NOTIFY is delivered when the (outer) transaction commits, so listeners
    /// only see tasks that were actually committed. No manual `pg_notify` is
    /// needed.
    ///
    /// This operation executes synchronous SQL. From an async
    /// context, invoke it on the selected runtime's blocking pool —
    /// `tokio::task::spawn_blocking` with the `tokio` feature, or
    /// `ntex_rt::spawn_blocking` with the `ntex` feature — together with your
    /// business-data writes so the entire transaction lives on one blocking
    /// task.
    ///
    /// See [`Self::push_task_with_conn`] for the full-control variant that
    /// accepts a pre-built [`PgTask`] (custom `idempotency_key`, `priority`,
    /// `run_at`, `max_attempts`, `metadata`, or `task_id`).
    ///
    /// # Errors
    /// - [`Error::Decode`] if the codec rejects `args`.
    /// - [`Error::InvalidArgument`] if the encoded payload or serialized
    ///   metadata exceeds its byte cap, or for unreachable `run_at`.
    /// - [`Error::Database`] for SQL/driver failures.
    pub fn push_with_conn(&self, conn: &mut PgConnection, args: Args) -> Result<PgTaskId, Error> {
        let encoded = EncodeCodec::encode(&args).map_err(|err| Error::Decode(Box::new(err)))?;
        let task_id = PgTaskId::new(Ulid::new());
        let mut task = PgTask::<CompactType>::new(encoded);
        task.parts.task_id = Some(task_id);
        queries::push_tasks_on_conn(conn, &self.config, vec![task])?;
        Ok(task_id)
    }

    /// Enqueue a fully-constructed [`PgTask<Args>`] using a caller-supplied
    /// connection. Use this when you need to set `idempotency_key`,
    /// `priority`, `run_at`, `max_attempts`, `metadata`, or a specific
    /// `task_id`.
    ///
    /// Semantics are identical to [`Self::push_with_conn`]; see that method's
    /// docs for the transaction/NOTIFY contract.
    ///
    /// If `task.parts.task_id` is `None`, a fresh Ulid is generated and
    /// returned. If `Some`, that id is used as-is and echoed back.
    ///
    /// # Errors
    /// - [`Error::Decode`] if the codec rejects the task's `args`.
    /// - [`Error::IdempotencyConflict`] on `idempotency_key` conflict — the
    ///   savepoint for this batch is rolled back (the whole batch, not just the
    ///   duplicate), but your outer transaction continues; decide whether to
    ///   commit or roll back based on the error.
    /// - [`Error::InvalidArgument`] if the encoded payload or serialized
    ///   metadata exceeds its byte cap.
    /// - [`Error::Database`] for SQL/driver failures.
    pub fn push_task_with_conn(
        &self,
        conn: &mut PgConnection,
        task: PgTask<Args>,
    ) -> Result<PgTaskId, Error> {
        let encoded =
            EncodeCodec::encode(&task.args).map_err(|err| Error::Decode(Box::new(err)))?;
        let task_id = task
            .parts
            .task_id
            .unwrap_or_else(|| PgTaskId::new(Ulid::new()));
        let mut compact = PgTask::<CompactType> {
            args: encoded,
            parts: task.parts,
        };
        compact.parts.task_id = Some(task_id);
        queries::push_tasks_on_conn(conn, &self.config, vec![compact])?;
        Ok(task_id)
    }

    /// Enqueue a batch of tasks from their `Args` on a caller-supplied
    /// connection, in a single INSERT.
    ///
    /// The batch form of [`Self::push_with_conn`]: identical transaction /
    /// NOTIFY contract, but the whole batch is inserted with one statement (one
    /// round-trip, with bounded NOTIFY chunks) instead of one INSERT per task. Returns the
    /// generated [`PgTaskId`]s in submission order. An empty batch is a no-op
    /// that returns an empty vector.
    ///
    /// # Errors
    /// - [`Error::Decode`] if the codec rejects any task's `args`.
    /// - [`Error::InvalidArgument`] if any encoded payload or serialized
    ///   metadata exceeds its byte cap.
    /// - [`Error::Database`] for SQL/driver failures.
    pub fn push_batch_with_conn(
        &self,
        conn: &mut PgConnection,
        args_batch: impl IntoIterator<Item = Args>,
    ) -> Result<Vec<PgTaskId>, Error> {
        let mut tasks = Vec::new();
        let mut ids = Vec::new();
        for args in args_batch {
            let encoded = EncodeCodec::encode(&args).map_err(|err| Error::Decode(Box::new(err)))?;
            let task_id = PgTaskId::new(Ulid::new());
            let mut task = PgTask::<CompactType>::new(encoded);
            task.parts.task_id = Some(task_id);
            tasks.push(task);
            ids.push(task_id);
        }
        queries::push_tasks_on_conn(conn, &self.config, tasks)?;
        Ok(ids)
    }

    /// Enqueue a batch of fully-constructed [`PgTask<Args>`] values on a
    /// caller-supplied connection, in a single INSERT.
    ///
    /// The batch form of [`Self::push_task_with_conn`]: use it when the tasks
    /// carry custom `idempotency_key`, `priority`, `run_at`, `max_attempts`,
    /// `metadata`, or `task_id`. Returns each task's [`PgTaskId`] in submission
    /// order (a fresh Ulid wherever `task_id` was `None`). An empty batch is a
    /// no-op that returns an empty vector.
    ///
    /// # Errors
    /// - [`Error::Decode`] if the codec rejects any task's `args`.
    /// - [`Error::IdempotencyConflict`] on an `idempotency_key` conflict — the
    ///   whole batch's savepoint is rolled back while the outer transaction
    ///   continues (see [`Self::push_task_with_conn`]).
    /// - [`Error::InvalidArgument`] if any encoded payload or serialized
    ///   metadata exceeds its byte cap.
    /// - [`Error::Database`] for SQL/driver failures.
    pub fn push_tasks_with_conn(
        &self,
        conn: &mut PgConnection,
        tasks: impl IntoIterator<Item = PgTask<Args>>,
    ) -> Result<Vec<PgTaskId>, Error> {
        let mut compact_tasks = Vec::new();
        let mut ids = Vec::new();
        for task in tasks {
            let encoded =
                EncodeCodec::encode(&task.args).map_err(|err| Error::Decode(Box::new(err)))?;
            let task_id = task
                .parts
                .task_id
                .unwrap_or_else(|| PgTaskId::new(Ulid::new()));
            let mut compact = PgTask::<CompactType> {
                args: encoded,
                parts: task.parts,
            };
            compact.parts.task_id = Some(task_id);
            compact_tasks.push(compact);
            ids.push(task_id);
        }
        queries::push_tasks_on_conn(conn, &self.config, compact_tasks)?;
        Ok(ids)
    }
}

/// Single generic `Backend` impl covering every `Fetcher: PgFetcherSource`.
/// Heartbeat/middleware are identical for all three modes; the per-mode
/// pipeline is delegated through `PgFetcherSource::into_compact_stream`.
impl<Args, Decode, Fetcher> Backend for PostgresStorage<Args, Decode, Fetcher>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Fetcher: crate::fetcher::PgFetcherSource,
{
    type Args = Args;
    type IdType = Ulid;
    type Context = PgContext;
    type Error = Error;
    type Stream = TaskStream<PgTask<Args>, Error>;
    type Beat = futures::stream::BoxStream<'static, Result<(), Error>>;
    type Layer = PgMiddleware;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.heartbeat_stream(worker)
    }

    fn middleware(&self) -> Self::Layer {
        PgMiddleware::with_lease_registry(
            &self.pool,
            self.config.ack(),
            std::sync::Arc::clone(&self.lease_token),
            self.leases.clone(),
        )
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        // The decode stage keeps its own pool handle (and the claiming
        // worker's id) so it can fail rows whose payload does not decode —
        // see `queries::fail_undecodable_task`.
        let pool = self.pool.clone();
        let lease_token = self.lease_token.clone();
        let lease = self.leases.for_worker(worker.name());
        let compact = self.fetcher.into_compact_stream(
            self.pool,
            self.config,
            worker.clone(),
            self.lease_token,
            self.poll_factory,
        );
        crate::fetcher::decode_task_stream::<Args, Decode>(
            crate::fetcher::LeaseStream::new(compact, lease.clone(), true).boxed(),
            pool,
            std::sync::Arc::from(worker.name().as_str()),
            Some(lease_token),
            lease,
        )
    }
}

impl<Args, Decode, Fetcher> BackendExt for PostgresStorage<Args, Decode, Fetcher>
where
    Args: Send + 'static + Unpin,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Fetcher: crate::fetcher::PgFetcherSource,
{
    type Compact = CompactType;
    type Codec = Decode;
    type CompactStream = TaskStream<PgTask<CompactType>, Self::Error>;

    fn get_queue(&self) -> Queue {
        self.config.queue().clone()
    }

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        let lease = self.leases.for_worker(worker.name());
        let compact = self.fetcher.into_compact_stream(
            self.pool,
            self.config,
            worker.clone(),
            self.lease_token,
            self.poll_factory,
        );
        crate::fetcher::LeaseStream::new(compact, lease, true).boxed()
    }
}

#[cfg(test)]
mod tests {
    use apalis_core::{
        backend::{Backend, BackendExt},
        task::status::Status,
    };
    use apalis_sql::{DateTime, DateTimeExt, from_row::FromRowError};
    use lets_expect::{AssertionError, AssertionResult, *};

    use super::*;
    use crate::unreachable::unreachable_pool;

    fn row(
        id: &str,
        status: &str,
        run_at: Option<DateTime>,
        idempotency_key: Option<&str>,
    ) -> TaskRow {
        TaskRow {
            job: b"payload".to_vec(),
            id: id.to_owned(),
            job_type: "unit-queue".to_owned(),
            status: status.to_owned(),
            attempts: 2,
            max_attempts: Some(3),
            run_at,
            last_result: None,
            lock_at: None,
            lock_by: Some("worker-a".to_owned()),
            done_at: None,
            priority: Some(7),
            metadata: Some(serde_json::json!({"kind": "unit"})),
            idempotency_key: idempotency_key.map(str::to_owned),
        }
    }

    fn compact_task_has_expected_parts(
        result: &Result<PgTask<CompactType>, FromRowError>,
    ) -> AssertionResult {
        match result {
            Ok(task)
                if task.args == b"payload"
                    && task.parts.attempt.current() == 2
                    && task.parts.status.load() == Status::Pending
                    && task.parts.ctx.priority() == 7
                    && task.parts.ctx.lock_by() == &Some("worker-a".to_owned())
                    && task.parts.idempotency_key == Some("same-key".to_owned()) =>
            {
                Ok(())
            }
            Ok(task) => Err(AssertionError::new(vec![format!(
                "unexpected task parts: {task:?}"
            )])),
            Err(error) => Err(AssertionError::new(vec![format!(
                "expected successful conversion, got {error:?}"
            )])),
        }
    }

    fn compact_task_omits_idempotency_key(
        result: &Result<PgTask<CompactType>, FromRowError>,
    ) -> AssertionResult {
        match result {
            Ok(task) if task.parts.idempotency_key.is_none() => Ok(()),
            Ok(task) => Err(AssertionError::new(vec![format!(
                "expected idempotency_key None, got {:?}",
                task.parts.idempotency_key
            )])),
            Err(error) => Err(AssertionError::new(vec![format!(
                "expected successful conversion, got {error:?}"
            )])),
        }
    }

    fn column_not_found(column: &'static str) -> impl Fn(&FromRowError) -> AssertionResult {
        move |error| match error {
            FromRowError::ColumnNotFound(found) if found == column => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected missing column {column}, got {other:?}"
            )])),
        }
    }

    fn decode_error(error: &FromRowError) -> AssertionResult {
        match error {
            FromRowError::DecodeError(_) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected decode error, got {other:?}"
            )])),
        }
    }

    fn storage_uses_queue_and_buffer<Args, Codec, Fetcher>(
        queue: &'static str,
        buffer_size: usize,
    ) -> impl Fn(&PostgresStorage<Args, Codec, Fetcher>) -> AssertionResult {
        move |storage| {
            if storage.config.queue().to_string() == queue
                && storage.config.buffer_size() == buffer_size
            {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected queue {queue:?} and buffer {buffer_size}, got queue {:?} and buffer {}",
                    storage.config.queue(),
                    storage.config.buffer_size()
                )]))
            }
        }
    }

    fn debug_mentions_public_type(result: &String) -> AssertionResult {
        if result.contains("PostgresStorage")
            && result.contains("config")
            && !result.contains("pool")
        {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "debug output did not describe storage without exposing the pool: {result}"
            )]))
        }
    }

    fn storage_for_type_name() -> PostgresStorage<String> {
        let pool = unreachable_pool();
        PostgresStorage::<String>::new(&pool)
    }

    fn storage_for_config(queue: &'static str, buffer_size: usize) -> PostgresStorage<String> {
        let pool = unreachable_pool();
        let config = Config::new(queue).set_buffer_size(buffer_size);
        PostgresStorage::<String>::new_with_config(&pool, &config)
    }

    fn notify_storage_for_config(
        queue: &'static str,
        buffer_size: usize,
    ) -> PostgresStorage<String, JsonCodec<CompactType>, PgNotify> {
        let pool = unreachable_pool();
        let config = Config::new(queue).set_buffer_size(buffer_size);
        PostgresStorage::<String>::new_with_notify(&pool, &config)
    }

    fn cloned_storage_for_config(
        queue: &'static str,
        buffer_size: usize,
    ) -> (PostgresStorage<String>, PostgresStorage<String>) {
        let original = storage_for_config(queue, buffer_size);
        let clone = original.clone();
        (original, clone)
    }

    fn shares_lease_token(
        pair: &(PostgresStorage<String>, PostgresStorage<String>),
    ) -> AssertionResult {
        let (original, clone) = pair;
        if std::sync::Arc::ptr_eq(&original.lease_token, &clone.lease_token) {
            Ok(())
        } else {
            Err(AssertionError::new(vec![
                "clone did not share the storage registration lease token".to_owned(),
            ]))
        }
    }

    fn cloned_storage(
        pair: &(PostgresStorage<String>, PostgresStorage<String>),
    ) -> AssertionResult {
        storage_uses_queue_and_buffer("clone-api", 4)(&pair.1)
    }

    fn debug_storage() -> String {
        format!("{:?}", storage_for_config("debug-api", 10))
    }

    fn storage_with_changed_codec() -> PostgresStorage<String, JsonCodec<CompactType>> {
        storage_for_config("codec-api", 6)
            .with_codec::<()>()
            .with_codec::<JsonCodec<CompactType>>()
    }

    // Pins the codec type parameter to `()` at compile time. The `Codec = ()`
    // slot is enforced by the parameter type; `Fetcher` stays inferred so the
    // assertion does not depend on the (preserved) fetcher type's spelling.
    fn pin_unit_codec<Fetcher>(_storage: &PostgresStorage<String, (), Fetcher>) {}

    fn with_codec_swaps_to_unit_codec() -> String {
        let pool = unreachable_pool();
        let storage = PostgresStorage::<String>::new(&pool).with_codec::<()>();
        // Compile-time check: this fails to compile if `with_codec::<()>` no
        // longer yields a `()` codec slot, independent of `type_name` formatting.
        pin_unit_codec(&storage);
        storage.config.queue().to_string()
    }

    fn storage_accessors() -> (String, usize) {
        let storage = storage_for_config("accessor-api", 8);
        (
            storage.config().queue().to_string(),
            storage.config().buffer_size(),
        )
    }

    fn basic_get_queue() -> String {
        storage_for_config("basic-queue-api", 4)
            .get_queue()
            .to_string()
    }

    fn notify_get_queue() -> String {
        notify_storage_for_config("notify-queue-api", 4)
            .get_queue()
            .to_string()
    }

    fn backend_trait_surfaces(notify: bool) -> (String, String, String) {
        let worker = WorkerContext::new::<()>("backend-trait-worker");
        if notify {
            let storage = notify_storage_for_config("notify-trait-api", 2);
            let middleware = std::any::type_name_of_val(&storage.middleware()).to_owned();
            let heartbeat = std::any::type_name_of_val(&storage.heartbeat(&worker)).to_owned();
            let stream = std::any::type_name_of_val(&storage.poll_compact(&worker)).to_owned();
            (middleware, heartbeat, stream)
        } else {
            let storage = storage_for_config("basic-trait-api", 2);
            let middleware = std::any::type_name_of_val(&storage.middleware()).to_owned();
            let heartbeat = std::any::type_name_of_val(&storage.heartbeat(&worker)).to_owned();
            let stream = std::any::type_name_of_val(&storage.poll_compact(&worker)).to_owned();
            (middleware, heartbeat, stream)
        }
    }

    fn exposes_accessors(result: &(String, usize)) -> AssertionResult {
        if result.0 == "accessor-api" && result.1 == 8 {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "unexpected storage accessors: {result:?}"
            )]))
        }
    }

    fn constructs_backend_traits(result: &(String, String, String)) -> AssertionResult {
        let (middleware, heartbeat, compact) = result;
        // `std::any::type_name_of_val` resolves type aliases away, so neither
        // `BoxStream` nor `PgContext` ever appears literally — `BoxStream<T>`
        // always prints as its expansion `Pin<Box<dyn Stream<Item = T> + Send>>`,
        // and `PgContext` as its expansion `SqlContext<PgPool>`. Match those
        // expansions instead of the alias names.
        //
        // The heartbeat is `Beat = BoxStream<'static, Result<(), Error>>`: a bare
        // `()` beat, so its type name carries the `Result`/`Error` item shape but
        // must NOT name a task — that would mean `heartbeat()` handed back the poll
        // stream instead.
        let heartbeat_is_beat_stream = heartbeat.contains("Pin")
            && heartbeat.contains("Box")
            && heartbeat.contains("Stream")
            && heartbeat.contains("Result")
            && heartbeat.contains("Error")
            && !heartbeat.contains("SqlContext")
            && !heartbeat.contains("Task<");
        // The compact stream is `CompactStream = TaskStream<PgTask<CompactType>,
        // Error>` = `BoxStream<'static, Result<Option<Task<Vec<u8>, PgContext,
        // Ulid>>, Error>>`: it must name the `Option`-wrapped task with its
        // `SqlContext`, distinguishing it from the bare heartbeat beat.
        let compact_is_task_stream = compact.contains("Pin")
            && compact.contains("Box")
            && compact.contains("Stream")
            && compact.contains("Option")
            && compact.contains("SqlContext")
            && compact.contains("Error");
        if middleware.contains("PgMiddleware") && heartbeat_is_beat_stream && compact_is_task_stream
        {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "unexpected backend trait surfaces: {result:?}"
            )]))
        }
    }

    #[cfg(feature = "tokio")]
    mod tokio_tests {
        use super::*;

        /// Run a worker future that resolves to `outcome` through
        /// `run_released` against an unreachable pool: the outcome must come
        /// back untouched, the release must report the pool failure, and the
        /// name must be retired locally either way.
        async fn released_run(
            outcome: Result<(), &'static str>,
        ) -> (Result<(), &'static str>, &'static str, bool) {
            let storage = storage_for_config("run-released", 1);
            let ReleasedRun { outcome, released } = storage
                .run_released("released-worker", async move { outcome })
                .await;
            let released = match released {
                Ok(_) => "released",
                Err(Error::Pool(_)) => "pool",
                Err(_) => "other",
            };
            let retired = storage.leases.for_worker("released-worker").is_retired();
            (outcome, released, retired)
        }

        lets_expect! { #tokio_test
            expect(released_run(outcome).await) as a_run_followed_by_a_release {
                let outcome: Result<(), &'static str> = Ok(());
                when the_database_cannot_be_reached {
                    to keeps_the_outcome_reports_the_release_failure_and_retires_the_name {
                        equal((Ok(()), "pool", true))
                    }
                    when the_run_itself_failed {
                        let outcome: Result<(), &'static str> = Err("worker stopped");
                        to keeps_the_failed_outcome_reports_the_release_failure_and_retires_the_name {
                            equal((Err("worker stopped"), "pool", true))
                        }
                    }
                }
            }
        }
    }

    lets_expect! {
        expect(crate_name()) {
            to returns_the_crate_name { equal("apalis-diesel-postgres") }
        }

        expect(row(id, status, run_at, idempotency_key).try_into_task_compact::<Ulid, PgPool>()) as compact_task_record {
            let id = &Ulid::new().to_string();
            let status = "Pending";
            let run_at = Some(DateTime::now());
            let idempotency_key = Some("same-key");

            when row_has_all_required_fields {
                to preserves_task_payload_and_context { compact_task_has_expected_parts }
            }

            when idempotency_key_is_absent {
                let idempotency_key = None;
                to omits_the_idempotency_key { compact_task_omits_idempotency_key }
            }

            when run_time_is_missing {
                let run_at = None;
                to rejects_the_row { be_err_and column_not_found("run_at") }
            }

            when status_is_unknown {
                let status = "Unknown";
                to rejects_the_row { be_err_and decode_error }
            }

            when id_is_not_a_ulid {
                let id = "not-a-ulid";
                to rejects_the_row { be_err_and decode_error }
            }
        }

        expect(storage) as storage_configuration {
            let storage = storage_for_type_name();

            when storage_is_built_from_the_task_type {
                to uses_the_type_name_as_queue {
                    storage_uses_queue_and_buffer(std::any::type_name::<String>(), 10)
                }
            }

            when storage_is_built_with_an_explicit_config {
                let storage = storage_for_config("public-api", 3);
                to preserves_the_supplied_config { storage_uses_queue_and_buffer("public-api", 3) }
            }

            when storage_is_built_with_notify {
                let storage = notify_storage_for_config("notify-api", 2);
                to preserves_the_supplied_config { storage_uses_queue_and_buffer("notify-api", 2) }
            }

            when storage_is_cloned {
                let storage = cloned_storage_for_config("clone-api", 4);
                to preserves_the_configuration_and_registration_identity {
                    cloned_storage,
                    shares_lease_token
                }
            }
        }

        expect(debug_storage()) as storage_description {
            to describes_the_storage_without_exposing_the_pool { debug_mentions_public_type }
        }

        expect(storage_with_changed_codec()) as configured_codec {
            to preserves_the_supplied_config { storage_uses_queue_and_buffer("codec-api", 6) }
        }

        // The codec swap to `()` is enforced at compile time by the
        // `PostgresStorage<String, ()>` annotation in the helper; this leaf
        // additionally confirms the swapped storage still constructs with the
        // task-type-derived queue.
        expect(with_codec_swaps_to_unit_codec()) as replacement_codec {
            to builds_a_unit_codec_storage_with_the_default_queue {
                equal(std::any::type_name::<String>().to_owned())
            }
        }

        expect(storage_accessors()) as storage_configuration_access {
            to exposes_the_queue_and_buffer_config { exposes_accessors }
        }

        expect(basic_get_queue()) as polling_queue {
            to returns_the_basic_queue { equal("basic-queue-api".to_owned()) }
        }

        expect(notify_get_queue()) as notification_queue {
            to returns_the_notify_queue { equal("notify-queue-api".to_owned()) }
        }

        expect(backend_trait_surfaces(notify)) as backend_components {
            let notify = false;

            when basic_polling_storage {
                to builds_heartbeat_middleware_and_compact_stream { constructs_backend_traits }
            }

            when notify_storage {
                let notify = true;
                to builds_heartbeat_middleware_and_compact_stream { constructs_backend_traits }
            }
        }
    }
}
