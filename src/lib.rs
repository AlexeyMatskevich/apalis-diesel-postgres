#![doc = include_str!("../README.md")]

use std::{fmt::Debug, marker::PhantomData};

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
    sink::PgSink,
};

mod ack;
mod admin;
mod error;
mod fetcher;
mod lifecycle;
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
#[must_use]
pub const fn crate_name() -> &'static str {
    "apalis-diesel-postgres"
}

// apalis `WorkerBuilder::build()` requires the backend to be `Send + Sync`.
const _: fn() = || {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PostgresStorage<()>>();
    assert_send_sync::<PostgresStorage<(), JsonCodec<CompactType>, PgNotify>>();
    assert_send_sync::<PostgresStorage<(), JsonCodec<CompactType>, SharedFetcher>>();
    assert_send_sync::<SharedPostgresStorage<()>>();
};

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
    /// Per-process lease token. Generated at construction (and shared by
    /// clones via `Arc`) so that the `keep_alive` heartbeat can only be
    /// refreshed by code holding this storage handle — even if a third party
    /// learns the `(worker_id, queue)` pair, they cannot extend the heartbeat
    /// without also possessing the token.
    pub(crate) lease_token: std::sync::Arc<str>,
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
        }
    }
}

impl<Args> PostgresStorage<Args> {
    /// Create storage for the queue named after `Args`.
    ///
    /// **Do not share `pool` with HTTP request handlers or other unrelated
    /// workloads.** Apalis holds long-lived connections (fetcher, lifecycle
    /// keep-alive, listener) and a backend that exhausts a shared pool will
    /// stall the worker, causing heartbeat loss and orphan reenqueue
    /// cascades. See the README section "Connection pool isolation" for the
    /// recommended sizing and the [`Self::push_with_conn`] outbox API for the
    /// supported way to enqueue from a backend transaction.
    #[must_use]
    pub fn new(pool: &PgPool) -> Self {
        let config = Config::new(std::any::type_name::<Args>());
        Self::new_with_config(pool, &config)
    }

    /// Create storage with an explicit Apalis SQL config.
    ///
    /// **Do not share `pool` with HTTP request handlers or other unrelated
    /// workloads** — see [`Self::new`] for the rationale.
    #[must_use]
    pub fn new_with_config(pool: &PgPool, config: &Config) -> Self {
        Self {
            _marker: PhantomData,
            pool: pool.clone(),
            config: config.clone(),
            fetcher: PgFetcher {
                _marker: PhantomData,
            },
            sink: PgSink::new(pool, config),
            lease_token: queries::worker::mint_lease_token().into(),
        }
    }

    /// Create storage that also listens for PostgreSQL notifications.
    ///
    /// Notify mode uses a dedicated pooled connection for `LISTEN
    /// "apalis::job::insert"` while the polling stream is alive. **Each
    /// `new_with_notify` storage spawns one listener thread and pins one
    /// pool connection.** If you need notify-driven dequeue across many
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
    /// Change the task codec while retaining pool, config, and fetcher.
    #[must_use]
    pub fn with_codec<NewCodec>(self) -> PostgresStorage<Args, NewCodec, Fetcher> {
        PostgresStorage {
            _marker: PhantomData,
            sink: PgSink::new(&self.pool, &self.config),
            pool: self.pool,
            config: self.config,
            fetcher: self.fetcher,
            lease_token: self.lease_token,
        }
    }

    /// Compose the keep-alive + reenqueue heartbeat stream shared by every
    /// `Backend` impl on this storage. Kept here so the per-fetcher
    /// `Backend::heartbeat` impls remain one-liners.
    pub(crate) fn heartbeat_stream(
        &self,
        worker: &WorkerContext,
    ) -> futures::stream::BoxStream<'static, Result<(), Error>> {
        let keep_alive = queries::keep_alive_stream(
            self.pool.clone(),
            self.config.clone(),
            worker.clone(),
            std::sync::Arc::clone(&self.lease_token),
        );
        let reenqueue = queries::reenqueue_orphaned_stream(self.pool.clone(), self.config.clone())
            .map_ok(|_| ());
        futures::stream::select(keep_alive, reenqueue).boxed()
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
    /// This is the only synchronous public method in the crate. From an async
    /// context, invoke it inside `tokio::task::spawn_blocking` together with
    /// your business-data writes so the entire transaction lives on one
    /// blocking task.
    ///
    /// See [`Self::push_task_with_conn`] for the full-control variant that
    /// accepts a pre-built [`PgTask`] (custom `idempotency_key`, `priority`,
    /// `run_at`, `max_attempts`, `metadata`, or `task_id`).
    ///
    /// # Errors
    /// - [`Error::Decode`] if the codec rejects `args`.
    /// - [`Error::InvalidArgument`] if serialized metadata exceeds the byte
    ///   cap, or for unreachable `run_at`.
    /// - [`Error::Database`] for SQL/driver failures.
    pub fn push_with_conn(
        &self,
        conn: &mut PgConnection,
        args: Args,
    ) -> Result<PgTaskId, Error> {
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
    /// - [`Error::InvalidArgument`] on `idempotency_key` conflict — the
    ///   savepoint for this batch is rolled back, but your outer transaction
    ///   continues; decide whether to commit or roll back based on the error.
    /// - [`Error::InvalidArgument`] if serialized metadata exceeds the byte
    ///   cap.
    /// - [`Error::Database`] for SQL/driver failures.
    pub fn push_task_with_conn(
        &self,
        conn: &mut PgConnection,
        task: PgTask<Args>,
    ) -> Result<PgTaskId, Error> {
        let encoded =
            EncodeCodec::encode(&task.args).map_err(|err| Error::Decode(Box::new(err)))?;
        let task_id = task.parts.task_id.unwrap_or_else(|| PgTaskId::new(Ulid::new()));
        let mut compact = PgTask::<CompactType> {
            args: encoded,
            parts: task.parts,
        };
        compact.parts.task_id = Some(task_id);
        queries::push_tasks_on_conn(conn, &self.config, vec![compact])?;
        Ok(task_id)
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
        PgMiddleware::with_lease_token(
            self.pool.clone(),
            self.config.ack(),
            std::sync::Arc::clone(&self.lease_token),
        )
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let compact = self.fetcher.into_compact_stream(
            self.pool,
            self.config,
            worker.clone(),
            self.lease_token,
        );
        crate::fetcher::decode_task_stream::<Args, Decode>(compact)
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
        self.fetcher
            .into_compact_stream(self.pool, self.config, worker.clone(), self.lease_token)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use apalis_core::{
        backend::{Backend, BackendExt},
        task::status::Status,
    };
    use apalis_sql::{DateTime, DateTimeExt, from_row::FromRowError};
    use diesel::{
        PgConnection,
        r2d2::{ConnectionManager, Pool},
    };
    use lets_expect::{AssertionError, AssertionResult, *};

    use super::*;

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

    fn unchecked_pool() -> PgPool {
        let manager = ConnectionManager::<PgConnection>::new("postgres://127.0.0.1:1/not-used");
        Pool::builder()
            .max_size(1)
            .connection_timeout(std::time::Duration::from_millis(10))
            .build_unchecked(manager)
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
        if result.contains("PostgresStorage") && result.contains("config") {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "debug output did not describe storage: {result}"
            )]))
        }
    }

    fn task_id_alias_accepts_ulid(id: PgTaskId) -> bool {
        Ulid::from_str(&id.to_string()).is_ok()
    }

    fn storage_for_type_name() -> PostgresStorage<String> {
        let pool = unchecked_pool();
        PostgresStorage::<String>::new(&pool)
    }

    fn storage_for_config(queue: &'static str, buffer_size: usize) -> PostgresStorage<String> {
        let pool = unchecked_pool();
        let config = Config::new(queue).set_buffer_size(buffer_size);
        PostgresStorage::<String>::new_with_config(&pool, &config)
    }

    fn notify_storage_for_config(
        queue: &'static str,
        buffer_size: usize,
    ) -> PostgresStorage<String, JsonCodec<CompactType>, PgNotify> {
        let pool = unchecked_pool();
        let config = Config::new(queue).set_buffer_size(buffer_size);
        PostgresStorage::<String>::new_with_notify(&pool, &config)
    }

    fn cloned_storage_for_config(
        queue: &'static str,
        buffer_size: usize,
    ) -> PostgresStorage<String> {
        storage_for_config(queue, buffer_size).clone()
    }

    fn debug_storage() -> String {
        format!("{:?}", storage_for_config("debug-api", 10))
    }

    fn storage_with_changed_codec() -> PostgresStorage<String, JsonCodec<CompactType>> {
        storage_for_config("codec-api", 6)
            .with_codec::<()>()
            .with_codec::<JsonCodec<CompactType>>()
    }

    fn type_name_after_with_codec() -> &'static str {
        let pool = unchecked_pool();
        let storage = PostgresStorage::<String>::new(&pool).with_codec::<()>();
        std::any::type_name_of_val(&storage)
    }

    fn type_name_contains_unit_codec(result: &&'static str) -> AssertionResult {
        if result.contains("PostgresStorage")
            && result.contains("alloc::string::String")
            && (result.contains(", (),") || result.contains(",()"))
        {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected with_codec::<()> to switch the codec slot, got {result}"
            )]))
        }
    }

    fn storage_accessors() -> (String, usize, u32) {
        let storage = storage_for_config("accessor-api", 8);
        (
            storage.config().queue().to_string(),
            storage.config().buffer_size(),
            storage.pool().state().connections,
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

    fn exposes_accessors(result: &(String, usize, u32)) -> AssertionResult {
        if result.0 == "accessor-api" && result.1 == 8 {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "unexpected storage accessors: {result:?}"
            )]))
        }
    }

    fn constructs_backend_traits(result: &(String, String, String)) -> AssertionResult {
        if result.0.contains("PgMiddleware")
            && result.1.contains("Stream")
            && result.2.contains("Stream")
        {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "unexpected backend trait surfaces: {result:?}"
            )]))
        }
    }

    lets_expect! {
        expect(crate_name()) {
            to returns_the_crate_name { equal("apalis-diesel-postgres") }
        }

        expect(row(id, status, run_at, idempotency_key).try_into_task_compact::<Ulid, PgPool>()) {
            let id = &Ulid::new().to_string();
            let status = "Pending";
            let run_at = Some(DateTime::now());
            let idempotency_key = Some("same-key");

            when row_has_all_required_fields {
                to preserves_task_payload_and_context { compact_task_has_expected_parts }
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

        expect(storage) {
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
                to keeps_the_queue_configuration { storage_uses_queue_and_buffer("clone-api", 4) }
            }
        }

        expect(debug_storage()) {
            to describes_the_storage_without_exposing_the_pool { debug_mentions_public_type }
        }

        expect(storage_with_changed_codec()) {
            to keeps_the_existing_pool_config_and_fetcher { storage_uses_queue_and_buffer("codec-api", 6) }
        }

        expect(type_name_after_with_codec()) {
            when with_codec_replaces_the_codec_type_parameter {
                to swaps_the_codec_slot_in_the_generic_signature { type_name_contains_unit_codec }
            }
        }

        expect(storage_accessors()) {
            to exposes_the_pool_and_config { exposes_accessors }
        }

        expect(basic_get_queue()) {
            to returns_the_basic_queue { equal("basic-queue-api".to_owned()) }
        }

        expect(notify_get_queue()) {
            to returns_the_notify_queue { equal("notify-queue-api".to_owned()) }
        }

        expect(backend_trait_surfaces(notify)) {
            let notify = false;

            when basic_polling_storage {
                to builds_heartbeat_middleware_and_compact_stream { constructs_backend_traits }
            }

            when notify_storage {
                let notify = true;
                to builds_heartbeat_middleware_and_compact_stream { constructs_backend_traits }
            }
        }

        expect(task_id_alias_accepts_ulid(task_id)) {
            let task_id = PgTaskId::from_str(&Ulid::new().to_string()).expect("generated ULID parses");

            to accepts_the_public_task_id_alias { be_true }
        }
    }
}
