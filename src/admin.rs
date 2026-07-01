//! Admin-facing trait implementations for `PostgresStorage`.
//!
//! These extend the storage with `BackendExpose`-style traits used by
//! dashboards and tooling: lookup, listing, metrics, worker registration, and
//! completion-waiting. SQL bodies live in `crate::queries::admin`; this file
//! holds only the apalis trait wiring.

use apalis_core::{
    backend::{
        BackendExt, FetchById, Filter, ListAllTasks, ListQueues, ListTasks, ListWorkers, Metrics,
        QueueInfo, RegisterWorker, RunningWorker, Statistic, TaskResult, WaitForCompletion,
        codec::Codec,
    },
    task::{Task, task_id::TaskId},
};
use futures::stream::BoxStream;
use serde::de::DeserializeOwned;
use ulid::Ulid;

use crate::{CompactType, Error, PgContext, PgTask, PgTaskId, PostgresStorage, queries};

impl<Args, D, F> FetchById<Args> for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>:
        BackendExt<Context = PgContext, Compact = CompactType, IdType = Ulid, Error = Error>,
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,
{
    fn fetch_by_id(
        &mut self,
        task_id: &PgTaskId,
    ) -> impl Future<Output = Result<Option<PgTask<Args>>, Self::Error>> + Send {
        queries::fetch_by_id::<Args, D>(
            self.pool.clone(),
            task_id.to_string(),
            self.config.queue().to_string(),
        )
    }
}

impl<Args, D, F> ListTasks<Args> for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>:
        BackendExt<Context = PgContext, Compact = CompactType, IdType = Ulid, Error = Error>,
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,
{
    /// List this queue's tasks matching `filter.status`, newest-completed first
    /// (`done_at DESC, run_at DESC`).
    ///
    /// `filter.page`/`filter.page_size` map to `LIMIT ... OFFSET ...`. The
    /// `(job_type, status)` filter and the ordering are covered by the
    /// `jobs_list_by_queue_idx` index, so the scan runs in index order without a
    /// sort — but `OFFSET` still walks and discards the skipped rows, so a
    /// **deep page costs O(offset)**. This is inherent to apalis's page-based
    /// [`Filter`]; keep pages shallow (or narrow the `status`) on large tables,
    /// and prefer [`apalis_core::backend::FetchById`] /
    /// [`apalis_core::backend::WaitForCompletion`] for targeted lookups.
    fn list_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<Output = Result<Vec<PgTask<Args>>, Self::Error>> + Send {
        queries::list_tasks::<Args, D>(self.pool.clone(), self.config.queue().to_string(), filter)
    }
}

impl<Args, D, F> ListAllTasks for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>:
        BackendExt<Context = PgContext, Compact = CompactType, IdType = Ulid, Error = Error>,
{
    /// List tasks across **every** queue matching `filter.status`,
    /// newest-completed first (`done_at DESC, run_at DESC`).
    ///
    /// Same `LIMIT ... OFFSET ...` pagination as [`Self::list_tasks`], covered
    /// by the `jobs_list_all_idx` index (`status`, `done_at DESC`, `run_at
    /// DESC`) so no sort is needed — but the same **O(offset) deep-page cost**
    /// applies, and this listing scans the whole table's `status` slice rather
    /// than one queue's. Prefer shallow pages on large tables.
    fn list_all_tasks(
        &self,
        filter: &Filter,
    ) -> impl Future<
        Output = Result<Vec<Task<Self::Compact, Self::Context, Self::IdType>>, Self::Error>,
    > + Send {
        queries::list_all_tasks(self.pool.clone(), filter)
    }
}

impl<Args, D, F> ListWorkers for PostgresStorage<Args, D, F>
where
    Args: Sync,
    PostgresStorage<Args, D, F>:
        BackendExt<Context = PgContext, Compact = CompactType, IdType = Ulid, Error = Error>,
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

impl<Args, D, F> ListQueues for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>:
        BackendExt<Context = PgContext, Compact = CompactType, IdType = Ulid, Error = Error>,
{
    fn list_queues(&self) -> impl Future<Output = Result<Vec<QueueInfo>, Self::Error>> + Send {
        queries::list_queues(self.pool.clone())
    }
}

impl<Args, D, F> Metrics for PostgresStorage<Args, D, F>
where
    PostgresStorage<Args, D, F>:
        BackendExt<Context = PgContext, Compact = CompactType, IdType = Ulid, Error = Error>,
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
    PostgresStorage<Args, D, F>:
        BackendExt<Context = PgContext, Compact = CompactType, IdType = Ulid, Error = Error>,
{
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

impl<O, Args, F, Decode> WaitForCompletion<O> for PostgresStorage<Args, Decode, F>
where
    O: 'static + Send,
    PostgresStorage<Args, Decode, F>:
        BackendExt<Context = PgContext, Compact = CompactType, IdType = Ulid, Error = Error>,
    Result<O, String>: DeserializeOwned,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<O, Ulid>, Error>>;

    /// Wait for the given tasks to complete, yielding each result as it lands.
    ///
    /// # Error handling
    ///
    /// A transient database error during polling does **not** abandon the
    /// batch: the poll is retried with backoff. The stream yields an `Err` and
    /// ends only once the failures *persist* across several consecutive polls.
    /// Because completed results are durable in `apalis.jobs`, a surfaced error
    /// is always safe to recover from by calling `wait_for` again with the ids
    /// that have not yet yielded a result.
    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Self::ResultStream {
        queries::admin::wait_for_completion(self.pool.clone(), task_ids)
    }

    fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>> + Send,
    ) -> impl Future<Output = Result<Vec<TaskResult<O, Ulid>>, Self::Error>> + Send {
        queries::admin::check_status(self.pool.clone(), task_ids)
    }
}
