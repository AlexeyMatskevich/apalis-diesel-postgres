//! Admin-facing trait implementations for `PostgresStorage`.
//!
//! These extend the storage with `BackendExpose`-style traits used by
//! dashboards and tooling: lookup, listing, metrics, worker registration, and
//! completion-waiting. SQL bodies live in `crate::queries::admin`; this file
//! holds only the apalis trait wiring.

use apalis_core::{
    backend::{
        Backend, FetchById, Filter, ListAllTasks, ListQueues, ListTasks, ListWorkers, Metrics,
        QueueInfo, RegisterWorker, RunningWorker, Statistic, TaskResult, Vacuum, WaitForCompletion,
        WireFormatBackend,
    },
    task::task_id::TaskId,
};
use futures::stream::BoxStream;
use serde::de::DeserializeOwned;

use crate::{CompactType, Error, PgTask, PostgresStorage, queries};

/// apalis 1.0 returns admin listings and lookups in the backend's compact
/// form; decode `args` with [`WireFormatBackend::codec`] when needed.
impl<Args, D, F> FetchById for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>: Backend<Task = PgTask<CompactType>, Error = Error>,
{
    fn fetch_by_id(
        &mut self,
        task_id: &TaskId,
    ) -> impl Future<Output = Result<Option<PgTask<CompactType>>, Self::Error>> + Send {
        queries::fetch_by_id(
            self.pool.clone(),
            task_id.to_string(),
            self.config.queue().to_string(),
        )
    }
}

impl<Args, D, F> ListTasks for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>: Backend<Task = PgTask<CompactType>, Error = Error>
        + WireFormatBackend<Compact = CompactType>,
{
    /// List this queue's tasks matching `filter.status`, newest-completed first
    /// (`done_at DESC, run_at DESC, id DESC`, with NULL completion times first).
    ///
    /// `filter.page`/`filter.page_size` map to `LIMIT ... OFFSET ...`. The
    /// `(job_type, status)` filter and the ordering are covered by the
    /// `jobs_list_by_queue_idx` index, allowing an ordered scan without a sort.
    /// The optimizer chooses the plan; `OFFSET` still walks and discards skipped rows, so a
    /// **deep page costs O(offset)**. This is inherent to apalis's page-based
    /// [`Filter`]; keep pages shallow (or narrow the `status`) on large tables,
    /// and prefer [`apalis_core::backend::FetchById`] /
    /// [`apalis_core::backend::WaitForCompletion`] for targeted lookups.
    fn list_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<PgTask<CompactType>>, Self::Error>> + Send {
        queries::list_tasks(self.pool.clone(), self.config.queue().to_string(), filter)
    }
}

impl<Args, D, F> ListAllTasks for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>: Backend<Task = PgTask<CompactType>, Error = Error>
        + WireFormatBackend<Compact = CompactType>,
{
    /// List tasks across **every** queue matching `filter.status`,
    /// newest-completed first (`done_at DESC, run_at DESC, id DESC`, with NULL
    /// completion times first).
    ///
    /// Same `LIMIT ... OFFSET ...` pagination as [`Self::list_tasks`], covered
    /// by the `jobs_list_all_idx` index (`status`, `done_at DESC`, `run_at
    /// DESC`, `id DESC`), allowing an ordered scan without a sort. The optimizer
    /// chooses the plan; the same **O(offset) deep-page cost**
    /// applies, and this listing scans the whole table's `status` slice rather
    /// than one queue's. Prefer shallow pages on large tables.
    fn list_all_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<PgTask<CompactType>>, Self::Error>> + Send {
        queries::list_all_tasks(self.pool.clone(), filter)
    }
}

impl<Args, D, F> ListWorkers for PostgresStorage<Args, D, F>
where
    // No `Args: Sync` bound: `list_workers` / `list_all_workers` query only
    // `apalis.workers` and never read `Args`, so requiring it would needlessly
    // exclude otherwise valid `Send + !Sync` argument types — every other admin
    // impl on this storage already omits it.
    PostgresStorage<Args, D, F>: Backend<Task = PgTask<CompactType>, Error = Error>,
{
    fn list_workers(&self) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        queries::list_workers(self.pool.clone(), Some(self.config.queue().to_string()))
    }

    fn list_all_workers(
        &self,
    ) -> impl Future<Output = Result<Vec<RunningWorker>, Self::Error>> + Send {
        queries::list_workers(self.pool.clone(), None)
    }
}

// Compile-time guard: `list_workers` / `list_all_workers` read only
// `apalis.workers` and never touch `Args`, so the impl must not require
// `Args: Sync`. `Cell<u8>` is `Send + !Sync` (and Serialize/Deserialize), so
// this fails to compile while the redundant bound is present.
#[cfg(test)]
const _: fn() = || {
    fn assert_list_workers<T: ListWorkers>() {}
    assert_list_workers::<PostgresStorage<std::cell::Cell<u8>>>();
};

impl<Args, D, F> ListQueues for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>: Backend<Task = PgTask<CompactType>, Error = Error>,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        queries::list_queues(self.pool.clone())
    }
}

impl<Args, D, F> Metrics for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>: Backend<Task = PgTask<CompactType>, Error = Error>,
{
    /// Scans `apalis.jobs` to compute global statistics. Each call evaluates
    /// 20+ FILTER aggregates over every row; cost grows linearly with the
    /// table size. Treat as a slow admin call.
    fn global(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        queries::metrics_global(self.pool.clone())
    }

    /// Same shape as [`Self::global`], scoped to the configured queue. Cost
    /// still depends on the number of jobs in that `job_type`.
    fn fetch_by_queue(&self) -> impl Future<Output = Result<Vec<Statistic>, Self::Error>> + Send {
        queries::metrics_for_queue(self.pool.clone(), self.config.queue().to_string())
    }
}

impl<Args, D, F> RegisterWorker for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>: Backend<Task = PgTask<CompactType>, Error = Error>,
{
    /// Register a worker name for this storage's queue without a lease token.
    ///
    /// The registration has no heartbeat: each call sets `last_seen`, and the
    /// orphan sweep treats the name as dead `reenqueue_orphaned_after` after
    /// the last call. Callers claiming under this name through [`lock_task`],
    /// [`lock_task_in_queue`] or `apalis.get_jobs` re-register within that
    /// window to keep their `Running` and `Queued` tasks. A worker stream
    /// registering the same name receives [`Error::AlreadyRegistered`] while
    /// this registration is fresh. A row owned by a worker stream (lease token
    /// present) is left untouched, so this path cannot keep a foreign worker
    /// fresh.
    ///
    /// [`lock_task`]: crate::lock_task
    /// [`lock_task_in_queue`]: crate::lock_task_in_queue
    fn register_worker(
        &mut self,
        worker_id: String,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        queries::register_worker(
            self.pool.clone(),
            worker_id,
            self.config.queue().to_string(),
        )
    }
}

impl<Args, D, F> Vacuum for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>: Backend<Task = PgTask<CompactType>, Error = Error>,
{
    /// Delete every terminal task of this storage's queue, whatever its age:
    /// [`PostgresStorage::purge_terminal_tasks`] with a zero window. A result
    /// that a `WaitForCompletion` consumer has not read yet is lost, and every
    /// `idempotency_key` of a deleted task becomes free again; prefer the
    /// windowed method for scheduled retention.
    fn vacuum(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send {
        queries::purge_terminal_tasks(
            self.pool.clone(),
            self.config.queue().to_string(),
            std::time::Duration::ZERO,
        )
    }

    /// [`PostgresStorage::purge_terminal_tasks`]: delete the terminal tasks of
    /// this storage's queue that completed at least `duration` ago.
    fn vacuum_before(
        &mut self,
        duration: std::time::Duration,
    ) -> impl Future<Output = Result<usize, Self::Error>> + Send {
        queries::purge_terminal_tasks(self.pool.clone(), self.config.queue().to_string(), duration)
    }
}

impl<O, Args, F, Decode> WaitForCompletion<O> for PostgresStorage<Args, Decode, F>
where
    O: 'static + Send,
    PostgresStorage<Args, Decode, F>: Backend<Task = PgTask<CompactType>, Error = Error>,
    Result<O, String>: DeserializeOwned,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<O>, Error>>;

    /// Wait for the given tasks to complete, yielding each result as it lands.
    ///
    /// The tasks must exist: an id that no row carries on a poll, and still on
    /// a poll at least one backoff interval later, yields
    /// [`Error::TaskNotFound`] for that id and is no longer waited for,
    /// whether it was never enqueued or was removed by
    /// [`PostgresStorage::purge_terminal_tasks`]. That interval of absence is
    /// tolerated so an enqueue committing right after the wait began is not
    /// mistaken for a missing task; wait for the enqueue transaction to
    /// commit before waiting on its ids. An id passed more than once is
    /// waited for once and yields one result.
    ///
    /// # Error handling
    ///
    /// A transient database error during polling does **not** abandon the
    /// batch: the poll is retried with backoff. The stream yields an `Err` and
    /// ends only once the failures *persist* across several consecutive polls.
    /// Because completed results are durable in `apalis.jobs`, a surfaced error
    /// is always safe to recover from by calling `wait_for` again with the ids
    /// that have not yet yielded a result.
    fn wait_for(&mut self, task_ids: impl IntoIterator<Item = TaskId>) -> Self::ResultStream {
        queries::admin::wait_for_completion(self.pool.clone(), task_ids)
    }

    fn check_status(
        &mut self,
        task_ids: impl IntoIterator<Item = TaskId> + Send,
    ) -> impl Future<Output = Result<Vec<TaskResult<O>>, Self::Error>> + Send {
        queries::admin::check_status(self.pool.clone(), task_ids)
    }
}
