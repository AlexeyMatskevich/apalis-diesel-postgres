//! SQL helpers backing the admin-facing trait impls (`FetchById`, `ListTasks`,
//! `ListAllTasks`, `ListWorkers`, `ListQueues`, `Metrics`, `RegisterWorker`,
//! `WaitForCompletion`). The trait glue lives in `src/admin.rs` — this module
//! owns the SQL strings and the `with_conn` orchestration so the public
//! `admin.rs` file stays focused on apalis-trait wiring.

use std::{sync::OnceLock, time::Duration};

use apalis_core::{
    backend::{Filter, QueueInfo, RunningWorker, Statistic, TaskResult, codec::Codec},
    task::{status::Status, task_id::TaskId},
};
use diesel::{
    RunQueryDsl, sql_query,
    sql_types::{Array, Integer, Text},
};
use futures::{StreamExt, stream};
use serde::de::DeserializeOwned;
use ulid::Ulid;

use crate::{
    CompactType, Error, PgPool, PgTask,
    models::{
        JobRow, QueueInfoRow, StatisticRow, TaskResultRow, TrackedTaskRow, WorkerRow,
        task_result_from_row,
    },
    queries::{filter_offset_i32, i32_from_u32, task_row, with_conn},
};

pub(crate) async fn fetch_by_id<Args, D>(
    pool: PgPool,
    task_id: String,
    queue: String,
) -> Result<Option<PgTask<Args>>, Error>
where
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,
{
    fetch_by_id_row(pool, task_id, queue)
        .await?
        .map(task_row)
        .transpose()?
        .map(|task| task.try_map(|args| D::decode(&args).map_err(|e| Error::Decode(e.into()))))
        .transpose()
}

pub(crate) fn list_tasks<Args, D>(
    pool: PgPool,
    queue: String,
    filter: &Filter,
) -> impl Future<Output = Result<Vec<PgTask<Args>>, Error>> + Send
where
    D: Codec<Args, Compact = CompactType>,
    D::Error: std::error::Error + Send + Sync + 'static,
    Args: 'static,
{
    let status = filter
        .status
        .as_ref()
        .unwrap_or(&Status::Pending)
        .to_string();
    let limit = i32_from_u32(filter.limit(), "limit");
    let offset = filter_offset_i32(filter);
    async move {
        list_tasks_rows(pool, queue, status, limit?, offset?)
            .await?
            .into_iter()
            .map(task_row)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .map(|task| task.try_map(|args| D::decode(&args).map_err(|e| Error::Decode(e.into()))))
            .collect()
    }
}

pub(crate) fn list_all_tasks(
    pool: PgPool,
    filter: &Filter,
) -> impl Future<Output = Result<Vec<PgTask<CompactType>>, Error>> + Send {
    let status = filter
        .status
        .as_ref()
        .unwrap_or(&Status::Pending)
        .to_string();
    let limit = i32_from_u32(filter.limit(), "limit");
    let offset = filter_offset_i32(filter);
    async move {
        list_all_tasks_rows(pool, status, limit?, offset?)
            .await?
            .into_iter()
            .map(task_row)
            .collect()
    }
}

pub(crate) async fn list_workers(
    pool: PgPool,
    queue: Option<String>,
) -> Result<Vec<RunningWorker>, Error> {
    if let Some(queue) = queue {
        list_workers_rows(pool, queue)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
    } else {
        list_all_workers_rows(pool)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}

pub(crate) async fn list_queues(pool: PgPool) -> Result<Vec<QueueInfo>, Error> {
    list_queues_rows(pool)
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
}

pub(crate) async fn metrics_global(pool: PgPool) -> Result<Vec<Statistic>, Error> {
    metrics_rows(pool, None)
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
}

pub(crate) async fn metrics_for_queue(
    pool: PgPool,
    queue: String,
) -> Result<Vec<Statistic>, Error> {
    metrics_rows(pool, Some(queue))
        .await
        .map(|rows| rows.into_iter().map(Into::into).collect())
}

pub(crate) fn register_worker(
    pool: PgPool,
    worker_id: String,
    worker_type: String,
) -> impl Future<Output = Result<(), Error>> + Send {
    register_worker_admin(pool, worker_id, worker_type)
}

/// Double the wait-for-completion poll backoff, capped at `max`. Extracted from
/// `wait_for_completion` so the exponential-backoff progression is unit-testable
/// without driving the whole poll stream.
fn next_backoff(backoff: Duration, max: Duration) -> Duration {
    (backoff * 2).min(max)
}

/// Whether the consecutive-DB-error streak, after counting the current failure,
/// has reached the threshold at which the error is surfaced and the wait stream
/// ends. Extracted so the threshold arithmetic is unit-testable.
fn db_errors_exhausted(error_streak: u32, max_consecutive: u32) -> bool {
    error_streak + 1 >= max_consecutive
}

/// What one successful poll decided for the ids still being waited for.
struct PollTriage<O> {
    /// Terminal results and, for ids absent on two consecutive polls, the
    /// `TaskNotFound` that ends their wait.
    items: Vec<Result<TaskResult<O, Ulid>, Error>>,
    /// Ids still waited for.
    remaining: Vec<String>,
}

/// Sort the waited ids by what the poll returned: a terminal row yields its
/// result, a present active row keeps waiting, and an absent id keeps
/// waiting once (an enqueue may commit between two polls) and ends the wait
/// with `TaskNotFound` when it is absent on the next poll as well. A row
/// that reappears clears its absence.
fn triage_poll<O>(
    remaining: Vec<String>,
    rows: Vec<TrackedTaskRow>,
    absent_since: &mut std::collections::HashMap<String, std::time::Instant>,
    now: std::time::Instant,
    grace: Duration,
) -> PollTriage<O>
where
    Result<O, String>: DeserializeOwned,
{
    let mut present: std::collections::HashMap<String, TrackedTaskRow> =
        rows.into_iter().map(|row| (row.id.clone(), row)).collect();
    let mut items = Vec::new();
    let mut next_remaining = Vec::with_capacity(remaining.len());
    for id in remaining {
        match present.remove(&id) {
            Some(row) if row.terminal => {
                absent_since.remove(&id);
                items.push(task_result_from_row(row.into()));
            }
            Some(_) => {
                absent_since.remove(&id);
                next_remaining.push(id);
            }
            None => match absent_since.get(&id).copied() {
                // Absent for a whole grace interval, however many polls ran
                // in between: the task does not exist.
                Some(first_absent) if now.saturating_duration_since(first_absent) >= grace => {
                    absent_since.remove(&id);
                    items.push(Err(Error::task_not_found(
                        "waiting for completion",
                        id,
                        None,
                        "the task does not exist: it was never enqueued, its enqueue has not committed, or it was purged",
                    )));
                }
                Some(_) => next_remaining.push(id),
                None => {
                    absent_since.insert(id.clone(), now);
                    next_remaining.push(id);
                }
            },
        }
    }
    PollTriage {
        items,
        remaining: next_remaining,
    }
}

/// The waited ids in their first order, each once: a task is waited for
/// once, however often its id was passed.
fn distinct_ids(ids: impl IntoIterator<Item = String>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    ids.into_iter()
        .filter(|id| seen.insert(id.clone()))
        .collect()
}

pub(crate) fn wait_for_completion<O>(
    pool: PgPool,
    task_ids: impl IntoIterator<Item = TaskId<Ulid>>,
) -> futures::stream::BoxStream<'static, Result<TaskResult<O, Ulid>, Error>>
where
    O: 'static + Send,
    Result<O, String>: DeserializeOwned,
{
    // `Vec<String>` keeps the per-tick clone for `tracked_task_rows` (the
    // SQL bind takes ownership); `triage_poll` prunes it in one pass.
    let remaining: Vec<String> = distinct_ids(task_ids.into_iter().map(|id| id.to_string()));
    // Exponential backoff (100ms → 2s) replaces the previous fixed 500ms
    // poll. Many concurrent `wait_for` callers no longer pin the database
    // at a steady 2 Hz; long-running waits also avoid wasteful re-polls.
    const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
    const MAX_BACKOFF: Duration = Duration::from_secs(2);
    // Tolerate transient database errors mid-wait: a failed poll is retried
    // with backoff rather than abandoning the whole batch. Only a *persistent*
    // failure (this many consecutive errors with no successful poll in between)
    // is surfaced to the caller, ending the stream. Any successful poll resets
    // the streak, so a database that merely flaps keeps making progress (jobs
    // stay durable in `apalis.jobs`, so a surfaced error is always retryable).
    const MAX_CONSECUTIVE_DB_ERRORS: u32 = 3;
    // An id absent from a poll is reported missing only once it has been
    // absent for at least one initial backoff interval. A poll that yields
    // results is followed at once by the next, so counting polls instead of
    // time would give an enqueue committing just after the wait no grace.
    let absent_since = std::collections::HashMap::new();
    stream::unfold(
        (remaining, INITIAL_BACKOFF, 0u32, absent_since),
        move |(remaining_ids, backoff, error_streak, mut absent_since)| {
            let pool = pool.clone();
            async move {
                if remaining_ids.is_empty() {
                    return None;
                }
                let rows = match tracked_task_rows(pool, remaining_ids.clone()).await {
                    Ok(rows) => rows,
                    Err(error) => {
                        // Surface the error and end the stream only once the
                        // failures persist; otherwise back off and retry the
                        // same ids, treating the blip as transient.
                        if db_errors_exhausted(error_streak, MAX_CONSECUTIVE_DB_ERRORS) {
                            return Some((
                                stream::iter(vec![Err(error)]),
                                (Vec::new(), INITIAL_BACKOFF, 0, absent_since),
                            ));
                        }
                        apalis_core::timer::sleep(backoff).await;
                        let new_backoff = next_backoff(backoff, MAX_BACKOFF);
                        return Some((
                            stream::iter(Vec::new()),
                            (remaining_ids, new_backoff, error_streak + 1, absent_since),
                        ));
                    }
                };
                let PollTriage { items, remaining } = triage_poll::<O>(
                    remaining_ids,
                    rows,
                    &mut absent_since,
                    std::time::Instant::now(),
                    INITIAL_BACKOFF,
                );
                if items.is_empty() {
                    apalis_core::timer::sleep(backoff).await;
                    let new_backoff = next_backoff(backoff, MAX_BACKOFF);
                    // A successful (if empty) poll clears the error streak.
                    return Some((
                        stream::iter(Vec::new()),
                        (remaining, new_backoff, 0, absent_since),
                    ));
                }
                // Reset backoff and the error streak after observing progress.
                Some((
                    stream::iter(items),
                    (remaining, INITIAL_BACKOFF, 0, absent_since),
                ))
            }
        },
    )
    .flatten()
    .boxed()
}

/// Every waited row that exists, with the server's terminal verdict, so a
/// wait can tell an active task from one that does not exist. Only a
/// terminal row's result is read: an active row's earlier failures can be
/// large and are polled repeatedly.
fn tracked_task_rows(
    pool: PgPool,
    ids: Vec<String>,
) -> impl Future<Output = Result<Vec<TrackedTaskRow>, Error>> + Send {
    with_conn(pool, move |conn| {
        sql_query(format!(
            "SELECT id, status,
                    CASE WHEN {terminal} THEN last_result END AS result,
                    {terminal} AS terminal
             FROM apalis.jobs
             WHERE id = ANY($1)",
            terminal = crate::queries::TERMINAL_PREDICATE
        ))
        .bind::<Array<Text>, _>(ids)
        .load::<TrackedTaskRow>(conn)
        .map_err(Error::database("fetching tracked task rows"))
    })
}

pub(crate) fn check_status<O>(
    pool: PgPool,
    task_ids: impl IntoIterator<Item = TaskId<Ulid>>,
) -> impl Future<Output = Result<Vec<TaskResult<O, Ulid>>, Error>> + Send
where
    O: 'static + Send,
    Result<O, String>: DeserializeOwned,
{
    let ids = task_ids.into_iter().map(|id| id.to_string()).collect();
    async move {
        completed_task_rows(pool, ids)
            .await?
            .into_iter()
            .map(task_result_from_row)
            .collect()
    }
}

fn fetch_by_id_row(
    pool: PgPool,
    task_id: String,
    queue: String,
) -> impl Future<Output = Result<Option<JobRow>, Error>> + Send {
    with_conn(pool, move |conn| {
        // Scope the lookup to this storage's configured queue. Task ids are
        // Ulids that could in principle be reused across queues; without
        // the `job_type` filter, a storage bound to queue A could return
        // rows owned by queue B if a caller passes a foreign id.
        sql_query("SELECT * FROM apalis.jobs WHERE id = $1 AND job_type = $2 LIMIT 1")
            .bind::<Text, _>(task_id)
            .bind::<Text, _>(queue)
            .load::<JobRow>(conn)
            .map(|rows| rows.into_iter().next())
            .map_err(Error::database("fetching task by id"))
    })
}

fn list_tasks_rows(
    pool: PgPool,
    queue: String,
    status: String,
    limit: i32,
    offset: i32,
) -> impl Future<Output = Result<Vec<JobRow>, Error>> + Send {
    with_conn(pool, move |conn| {
        sql_query(
            "SELECT * FROM apalis.jobs
             WHERE status = $1 AND job_type = $2
             ORDER BY done_at DESC, run_at DESC, id DESC
             LIMIT $3 OFFSET $4",
        )
        .bind::<Text, _>(status)
        .bind::<Text, _>(queue)
        .bind::<Integer, _>(limit)
        .bind::<Integer, _>(offset)
        .load::<JobRow>(conn)
        .map_err(Error::database("listing tasks"))
    })
}

fn list_all_tasks_rows(
    pool: PgPool,
    status: String,
    limit: i32,
    offset: i32,
) -> impl Future<Output = Result<Vec<JobRow>, Error>> + Send {
    with_conn(pool, move |conn| {
        sql_query(
            "SELECT * FROM apalis.jobs
             WHERE status = $1
             ORDER BY done_at DESC, run_at DESC, id DESC
             LIMIT $2 OFFSET $3",
        )
        .bind::<Text, _>(status)
        .bind::<Integer, _>(limit)
        .bind::<Integer, _>(offset)
        .load::<JobRow>(conn)
        .map_err(Error::database("listing all tasks"))
    })
}

fn list_workers_rows(
    pool: PgPool,
    queue: String,
) -> impl Future<Output = Result<Vec<WorkerRow>, Error>> + Send {
    with_conn(pool, move |conn| {
        // No silent LIMIT: the apalis `ListWorkers::list_workers` signature
        // takes no filter, and a hidden cap of 100 made the result
        // inconsistent on fleets with >100 workers. `apalis.workers` is
        // bounded by (workers × worker_type) and stays small in normal
        // deployments.
        sql_query(
            "SELECT * FROM apalis.workers
             WHERE worker_type = $1
             ORDER BY last_seen DESC",
        )
        .bind::<Text, _>(queue)
        .load::<WorkerRow>(conn)
        .map_err(Error::database("listing workers"))
    })
}

fn list_all_workers_rows(
    pool: PgPool,
) -> impl Future<Output = Result<Vec<WorkerRow>, Error>> + Send {
    with_conn(pool, move |conn| {
        sql_query("SELECT * FROM apalis.workers ORDER BY last_seen DESC")
            .load::<WorkerRow>(conn)
            .map_err(Error::database("listing all workers"))
    })
}

fn list_queues_rows(pool: PgPool) -> impl Future<Output = Result<Vec<QueueInfoRow>, Error>> + Send {
    with_conn(pool, move |conn| {
        sql_query(LIST_QUEUES_SQL)
            .load::<QueueInfoRow>(conn)
            .map_err(Error::database("listing queues"))
    })
}

/// SQL body for `list_queues`. An O(rows) scan over `apalis.jobs` joining
/// several CTEs; treat as a slow admin call.
const LIST_QUEUES_SQL: &str =
    "WITH job_rollup AS (
        SELECT job_type,
               COUNT(*) FILTER (WHERE status = 'Running') AS running_jobs,
               COUNT(*) FILTER (WHERE status = 'Pending') AS pending_jobs,
               COUNT(*) FILTER (WHERE status = 'Failed') AS failed_jobs,
               COUNT(*) FILTER (WHERE status IN ('Pending', 'Queued', 'Running')) AS active_jobs,
               COUNT(*) FILTER (WHERE status IN ('Queued', 'Running') AND run_at < now() - INTERVAL '1 hour') AS stale_running_jobs,
               ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'Killed') / NULLIF(COUNT(*), 0), 2) AS kill_rate,
               COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 hour') AS jobs_past_hour,
               COUNT(*) FILTER (
                   WHERE run_at >= CURRENT_DATE
                       AND run_at < CURRENT_DATE + INTERVAL '1 day'
               ) AS jobs_today,
               COUNT(*) FILTER (
                   WHERE status = 'Killed'
                       AND run_at >= CURRENT_DATE
                       AND run_at < CURRENT_DATE + INTERVAL '1 day'
               ) AS killed_jobs_today,
               ROUND(COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 hour') / 60.0, 2) AS avg_jobs_per_minute_past_hour,
               COUNT(*) AS total_jobs,
               COUNT(*) FILTER (WHERE status = 'Done') AS done_jobs,
               COUNT(*) FILTER (WHERE status = 'Killed') AS killed_jobs,
               ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'Done') / NULLIF(COUNT(*), 0), 2) AS success_rate,
               ROUND(
                   AVG(EXTRACT(EPOCH FROM (done_at - run_at)) / 60.0)
                       FILTER (WHERE status IN ('Done', 'Failed', 'Killed') AND done_at IS NOT NULL),
                   2
               ) AS avg_job_duration_mins,
               ROUND(
                   COALESCE(MAX(EXTRACT(EPOCH FROM (now() - run_at)) / 60.0)
                       FILTER (WHERE status IN ('Queued', 'Running')), 0),
                   2
               ) AS longest_running_job_mins,
               COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '7 days') AS jobs_past_7_days,
               MAX(run_at) AS most_recent_job
        FROM apalis.jobs
        GROUP BY job_type
    ),
    queue_stats AS (
        SELECT job_type,
               jsonb_agg(jsonb_build_object(
                   'title', statistic,
                   'stat_type', stat_type,
                   -- COALESCE so a NULL aggregate (e.g. AVG_JOB_DURATION_MINS on a
                   -- queue with no completed jobs) renders as text 0 instead of a
                   -- JSON null. apalis_core::Statistic.value is a non-optional
                   -- String, so a single null here fails the whole Vec<Statistic>
                   -- decode in models.rs and silently drops EVERY stat for the
                   -- queue. This mirrors the single-stat metrics value default.
                   'value', COALESCE(value, '0'),
                   'priority', priority
               ) ORDER BY priority, statistic) AS stats
        FROM job_rollup
        CROSS JOIN LATERAL (
            VALUES
                (1, 'Number', 'RUNNING_JOBS', running_jobs::TEXT),
                (1, 'Number', 'PENDING_JOBS', pending_jobs::TEXT),
                (1, 'Number', 'FAILED_JOBS', failed_jobs::TEXT),
                (2, 'Number', 'ACTIVE_JOBS', active_jobs::TEXT),
                (2, 'Number', 'STALE_RUNNING_JOBS', stale_running_jobs::TEXT),
                (2, 'Percentage', 'KILL_RATE', kill_rate::TEXT),
                (3, 'Number', 'JOBS_PAST_HOUR', jobs_past_hour::TEXT),
                (3, 'Number', 'JOBS_TODAY', jobs_today::TEXT),
                (3, 'Number', 'KILLED_JOBS_TODAY', killed_jobs_today::TEXT),
                (3, 'Decimal', 'AVG_JOBS_PER_MINUTE_PAST_HOUR', avg_jobs_per_minute_past_hour::TEXT),
                (4, 'Number', 'TOTAL_JOBS', total_jobs::TEXT),
                (4, 'Number', 'DONE_JOBS', done_jobs::TEXT),
                (4, 'Number', 'KILLED_JOBS', killed_jobs::TEXT),
                (4, 'Percentage', 'SUCCESS_RATE', success_rate::TEXT),
                (5, 'Decimal', 'AVG_JOB_DURATION_MINS', avg_job_duration_mins::TEXT),
                (5, 'Decimal', 'LONGEST_RUNNING_JOB_MINS', longest_running_job_mins::TEXT),
                (6, 'Number', 'JOBS_PAST_7_DAYS', jobs_past_7_days::TEXT),
                (8, 'Timestamp', 'MOST_RECENT_JOB', most_recent_job::TEXT)
        ) AS stats(priority, stat_type, statistic, value)
        GROUP BY job_type
    ),
    all_job_types AS (
        SELECT worker_type AS job_type FROM apalis.workers
        UNION
        SELECT DISTINCT job_type FROM apalis.jobs
    ),
    locked_workers AS (
        SELECT job_type, jsonb_agg(DISTINCT lock_by) AS workers
        FROM apalis.jobs
        WHERE lock_by IS NOT NULL
            AND status IN ('Pending', 'Queued', 'Running')
        GROUP BY job_type
    ),
    daily_activity AS (
        SELECT job_type, jsonb_agg(daily_count ORDER BY run_date) AS activity
        FROM (
            SELECT job_type, COUNT(*) AS daily_count, run_at::date AS run_date
            FROM apalis.jobs
            WHERE run_at >= now() - INTERVAL '7 days'
            GROUP BY job_type, run_at::date
        ) activity_by_day
        GROUP BY job_type
    )
    SELECT jt.job_type AS name,
           COALESCE(qs.stats, '[]'::jsonb) AS stats,
           COALESCE(lw.workers, '[]'::jsonb) AS workers,
           COALESCE(da.activity, '[]'::jsonb) AS activity
    FROM all_job_types jt
    LEFT JOIN queue_stats qs ON jt.job_type = qs.job_type
    LEFT JOIN locked_workers lw ON jt.job_type = lw.job_type
    LEFT JOIN daily_activity da ON jt.job_type = da.job_type
    ORDER BY name";

/// Cached SQL bodies for the scoped and global variants of `metrics()`. The
/// only variable parts of the query are two WHERE-fragment substitutions, so
/// each body is built once and reused.
static METRICS_SQL_BY_QUEUE: OnceLock<String> = OnceLock::new();
static METRICS_SQL_GLOBAL: OnceLock<String> = OnceLock::new();

fn metrics_sql(by_queue: bool) -> &'static str {
    let cell = if by_queue {
        &METRICS_SQL_BY_QUEUE
    } else {
        &METRICS_SQL_GLOBAL
    };
    cell.get_or_init(|| build_metrics_sql(by_queue)).as_str()
}

fn build_metrics_sql(by_queue: bool) -> String {
    let scope = if by_queue { "WHERE job_type = $1" } else { "" };
    let where_past_day = if by_queue {
        "WHERE job_type = $1 AND run_at >= now() - INTERVAL '1 day'"
    } else {
        "WHERE run_at >= now() - INTERVAL '1 day'"
    };
    format!(
            "WITH job_rollup AS (
                 SELECT COUNT(*) FILTER (WHERE status = 'Running')::TEXT AS running_jobs,
                        COUNT(*) FILTER (WHERE status = 'Pending')::TEXT AS pending_jobs,
                        COUNT(*) FILTER (WHERE status = 'Failed')::TEXT AS failed_jobs,
                        COUNT(*) FILTER (WHERE status IN ('Pending', 'Running', 'Queued'))::TEXT AS active_jobs,
                        COUNT(*) FILTER (
                            WHERE status IN ('Queued', 'Running')
                                AND run_at < now() - INTERVAL '1 hour'
                        )::TEXT AS stale_running_jobs,
                        ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'Killed') / NULLIF(COUNT(*), 0), 2)::TEXT AS kill_rate,
                        COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 hour')::TEXT AS jobs_past_hour,
                        COUNT(*) FILTER (
                            WHERE run_at >= CURRENT_DATE
                                AND run_at < CURRENT_DATE + INTERVAL '1 day'
                        )::TEXT AS jobs_today,
                        COUNT(*) FILTER (
                            WHERE status = 'Killed'
                                AND run_at >= CURRENT_DATE
                                AND run_at < CURRENT_DATE + INTERVAL '1 day'
                        )::TEXT AS killed_jobs_today,
                        ROUND(COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 hour') / 60.0, 2)::TEXT AS avg_jobs_per_minute_past_hour,
                        COUNT(*)::TEXT AS total_jobs,
                        COUNT(*) FILTER (WHERE status = 'Done')::TEXT AS done_jobs,
                        COUNT(*) FILTER (WHERE status IN ('Done', 'Failed', 'Killed'))::TEXT AS completed_jobs,
                        COUNT(*) FILTER (WHERE status = 'Killed')::TEXT AS killed_jobs,
                        ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'Done') / NULLIF(COUNT(*), 0), 2)::TEXT AS success_rate,
                        ROUND(
                            AVG(EXTRACT(EPOCH FROM (done_at - run_at)) / 60.0)
                                FILTER (WHERE status IN ('Done', 'Failed', 'Killed') AND done_at IS NOT NULL),
                            2
                        )::TEXT AS avg_job_duration_mins,
                        ROUND(
                            COALESCE(MAX(EXTRACT(EPOCH FROM (now() - run_at)) / 60.0)
                                FILTER (WHERE status IN ('Queued', 'Running')), 0),
                            2
                        )::TEXT AS longest_running_job_mins,
                        COUNT(*) FILTER (WHERE status = 'Pending' AND run_at <= now())::TEXT AS queue_backlog,
                        COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 day')::TEXT AS jobs_past_24_hours,
                        COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '7 days')::TEXT AS jobs_past_7_days,
                        COUNT(*) FILTER (
                            WHERE status = 'Killed'
                                AND run_at >= now() - INTERVAL '7 days'
                        )::TEXT AS killed_jobs_past_7_days,
                        ROUND(
                            100.0 * COUNT(*) FILTER (
                                WHERE status = 'Done'
                                    AND run_at >= now() - INTERVAL '1 day'
                            ) / NULLIF(COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 day'), 0),
                            2
                        )::TEXT AS success_rate_past_24h,
                        ROUND(COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 day') / 24.0, 2)::TEXT AS avg_jobs_per_hour_past_24h,
                        ROUND(COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '7 days') / 7.0, 2)::TEXT AS avg_jobs_per_day_past_7d,
                        trim_scale(EXTRACT(EPOCH FROM MAX(run_at)))::TEXT AS most_recent_job,
                        trim_scale(EXTRACT(EPOCH FROM (MIN(run_at) FILTER (WHERE status = 'Pending' AND run_at <= now()))))::TEXT AS oldest_pending_job
                 FROM apalis.jobs {scope}
             ),
             peak_hour AS (
                 SELECT COALESCE(MAX(hourly_count), 0)::TEXT AS value
                 FROM (
                     SELECT COUNT(*) AS hourly_count
                     FROM apalis.jobs {where_past_day}
                     GROUP BY EXTRACT(HOUR FROM run_at)
                 ) hourly
             )
             SELECT *
             FROM (
                 SELECT 1 AS priority, 'Number' AS type, 'RUNNING_JOBS' AS statistic, running_jobs AS value FROM job_rollup
                 UNION ALL SELECT 1, 'Number', 'PENDING_JOBS', pending_jobs FROM job_rollup
                 UNION ALL SELECT 2, 'Number', 'FAILED_JOBS', failed_jobs FROM job_rollup
                 UNION ALL SELECT 2, 'Number', 'ACTIVE_JOBS', active_jobs FROM job_rollup
                 UNION ALL SELECT 2, 'Number', 'STALE_RUNNING_JOBS', stale_running_jobs FROM job_rollup
                 UNION ALL SELECT 2, 'Percentage', 'KILL_RATE', kill_rate FROM job_rollup
                 UNION ALL SELECT 3, 'Number', 'JOBS_PAST_HOUR', jobs_past_hour FROM job_rollup
                 UNION ALL SELECT 3, 'Number', 'JOBS_TODAY', jobs_today FROM job_rollup
                 UNION ALL SELECT 3, 'Number', 'KILLED_JOBS_TODAY', killed_jobs_today FROM job_rollup
                 UNION ALL SELECT 3, 'Decimal', 'AVG_JOBS_PER_MINUTE_PAST_HOUR', avg_jobs_per_minute_past_hour FROM job_rollup
                 UNION ALL SELECT 4, 'Number', 'TOTAL_JOBS', total_jobs FROM job_rollup
                 UNION ALL SELECT 4, 'Number', 'DONE_JOBS', done_jobs FROM job_rollup
                 UNION ALL SELECT 4, 'Number', 'COMPLETED_JOBS', completed_jobs FROM job_rollup
                 UNION ALL SELECT 4, 'Number', 'KILLED_JOBS', killed_jobs FROM job_rollup
                 UNION ALL SELECT 4, 'Percentage', 'SUCCESS_RATE', success_rate FROM job_rollup
                 UNION ALL SELECT 5, 'Decimal', 'AVG_JOB_DURATION_MINS', avg_job_duration_mins FROM job_rollup
                 UNION ALL SELECT 5, 'Decimal', 'LONGEST_RUNNING_JOB_MINS', longest_running_job_mins FROM job_rollup
                 UNION ALL SELECT 5, 'Number', 'QUEUE_BACKLOG', queue_backlog FROM job_rollup
                 UNION ALL SELECT 6, 'Number', 'JOBS_PAST_24_HOURS', jobs_past_24_hours FROM job_rollup
                 UNION ALL SELECT 6, 'Number', 'JOBS_PAST_7_DAYS', jobs_past_7_days FROM job_rollup
                 UNION ALL SELECT 6, 'Number', 'KILLED_JOBS_PAST_7_DAYS', killed_jobs_past_7_days FROM job_rollup
                 UNION ALL SELECT 6, 'Percentage', 'SUCCESS_RATE_PAST_24H', success_rate_past_24h FROM job_rollup
                 UNION ALL SELECT 7, 'Decimal', 'AVG_JOBS_PER_HOUR_PAST_24H', avg_jobs_per_hour_past_24h FROM job_rollup
                 UNION ALL SELECT 7, 'Decimal', 'AVG_JOBS_PER_DAY_PAST_7D', avg_jobs_per_day_past_7d FROM job_rollup
                 UNION ALL SELECT 8, 'Timestamp', 'MOST_RECENT_JOB', most_recent_job FROM job_rollup
                 UNION ALL SELECT 8, 'Timestamp', 'OLDEST_PENDING_JOB', oldest_pending_job FROM job_rollup
                 UNION ALL SELECT 8, 'Number', 'PEAK_HOUR_JOBS', value FROM peak_hour
                 UNION ALL SELECT 9, 'Number', 'DB_PAGE_SIZE', current_setting('block_size')::INTEGER::TEXT
                 UNION ALL SELECT 9, 'Number', 'DB_PAGE_COUNT', (pg_total_relation_size('apalis.jobs') / current_setting('block_size')::INTEGER)::TEXT
                 UNION ALL SELECT 9, 'Number', 'DB_SIZE', pg_total_relation_size('apalis.jobs')::TEXT
             ) metrics
             ORDER BY priority, statistic"
    )
}

fn metrics_rows(
    pool: PgPool,
    queue: Option<String>,
) -> impl Future<Output = Result<Vec<StatisticRow>, Error>> + Send {
    with_conn(pool, move |conn| {
        let sql = metrics_sql(queue.is_some());
        let query = sql_query(sql);
        if let Some(queue) = queue {
            query
                .bind::<Text, _>(&queue)
                .load::<StatisticRow>(conn)
                .map_err(Error::database("fetching queue metrics"))
        } else {
            query
                .load::<StatisticRow>(conn)
                .map_err(Error::database("fetching global metrics"))
        }
    })
}

fn register_worker_admin(
    pool: PgPool,
    worker_id: String,
    worker_type: String,
) -> impl Future<Output = Result<(), Error>> + Send {
    with_conn(pool, move |conn| {
        // Match the worker-side registration path: take a per-(worker,
        // queue) advisory lock so concurrent registrations from a
        // dashboard and a live worker serialize.
        //
        // A registration without a lease token has no heartbeat: re-registering
        // is how it renews `last_seen`, and the orphan sweep and native
        // registration judge its liveness by that column. A row that carries a
        // lease token is owned by a heartbeating worker, so the conflict UPDATE
        // leaves its `last_seen`, `layers` and `storage_name` untouched: the
        // admin path can neither keep a foreign worker fresh nor poison its
        // observability.
        // `clock_timestamp()` samples the wall clock after the advisory lock
        // and the row lock are acquired: `now()` is the transaction start,
        // and a renewal that waited behind another registration for longer
        // than `reenqueue_orphaned_after` would otherwise be stale on commit.
        // Unlike the worker path (`register_worker_blocking`), this statement
        // always upserts exactly one row: the advisory lock is the *blocking*
        // variant (no `acquired` filter) and the conflict UPDATE carries no
        // WHERE gate, so the affected-row count is always 1 and cannot signal
        // an `AlreadyRegistered` condition. Dashboards re-registering an
        // existing worker is the expected idempotent case.
        sql_query(
            "WITH registration_lock AS (
                 SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))
             )
             INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at)
             SELECT $1, $2, $3, '', clock_timestamp(), clock_timestamp()
             FROM registration_lock
             ON CONFLICT (id, worker_type) DO UPDATE
             SET storage_name = CASE
                     WHEN apalis.workers.lease_token IS NULL
                         THEN EXCLUDED.storage_name
                     ELSE apalis.workers.storage_name
                 END,
                 layers = CASE
                     WHEN apalis.workers.lease_token IS NULL
                         THEN EXCLUDED.layers
                     ELSE apalis.workers.layers
                 END,
                 last_seen = CASE
                     WHEN apalis.workers.lease_token IS NULL
                         THEN clock_timestamp()
                     ELSE apalis.workers.last_seen
                 END",
        )
        .bind::<Text, _>(&worker_id)
        .bind::<Text, _>(worker_type)
        .bind::<Text, _>(crate::STORAGE_NAME)
        .execute(conn)
        .map_err(Error::database("registering worker"))?;
        Ok(())
    })
}

pub(crate) fn completed_task_rows(
    pool: PgPool,
    ids: Vec<String>,
) -> impl Future<Output = Result<Vec<TaskResultRow>, Error>> + Send {
    with_conn(pool, move |conn| {
        sql_query(format!(
            "SELECT id, status, last_result AS result
             FROM apalis.jobs
             WHERE id = ANY($1) AND {}",
            crate::queries::TERMINAL_PREDICATE
        ))
        .bind::<Array<Text>, _>(ids)
        .load::<TaskResultRow>(conn)
        .map_err(Error::database("fetching completed task results"))
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use lets_expect::*;

    use super::*;

    #[derive(Clone, Copy)]
    enum Presence {
        Terminal,
        Active,
        Absent,
    }

    fn tracked(id: &str, presence: Presence) -> Option<TrackedTaskRow> {
        match presence {
            Presence::Absent => None,
            Presence::Terminal => Some(TrackedTaskRow {
                id: id.to_owned(),
                status: "Done".to_owned(),
                result: Some(serde_json::json!({"Ok": "done"})),
                terminal: true,
            }),
            Presence::Active => Some(TrackedTaskRow {
                id: id.to_owned(),
                status: "Running".to_owned(),
                result: None,
                terminal: false,
            }),
        }
    }

    /// How long before this poll the id was first found absent.
    #[derive(Clone, Copy)]
    enum Absence {
        Never,
        LessThanTheGrace,
        TheWholeGrace,
    }

    /// One waited id through one poll, with its earlier absence. Reports what
    /// the poll yielded for it, whether it is still waited for, and whether an
    /// absence is still remembered.
    fn triaged(presence: Presence, absence: Absence) -> (&'static str, bool, bool) {
        let id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let grace = Duration::from_millis(100);
        let first_absent = std::time::Instant::now();
        let mut absent_since = std::collections::HashMap::new();
        let now = match absence {
            Absence::Never => first_absent,
            Absence::LessThanTheGrace => first_absent + grace / 2,
            Absence::TheWholeGrace => first_absent + grace,
        };
        if !matches!(absence, Absence::Never) {
            absent_since.insert(id.to_owned(), first_absent);
        }
        let PollTriage { items, remaining } = triage_poll::<String>(
            vec![id.to_owned()],
            tracked(id, presence).into_iter().collect(),
            &mut absent_since,
            now,
            grace,
        );
        let item = match items.as_slice() {
            [] => "nothing",
            [Ok(result)] if result.task_id.to_string() == id => "result",
            [Err(Error::TaskNotFound { task_id, .. })] if task_id == id => "not_found",
            _ => "unexpected",
        };
        (
            item,
            remaining == [id.to_owned()],
            absent_since.contains_key(id),
        )
    }

    lets_expect! {
        expect(triaged(presence, absence)) as a_waited_task_after_one_poll {
            let presence = Presence::Active;
            let absence = Absence::Never;
            to keeps_waiting_for_an_active_task { equal(("nothing", true, false)) }
            when the_task_is_terminal {
                let presence = Presence::Terminal;
                to yields_its_result_and_stops_waiting { equal(("result", false, false)) }
            }
            when the_task_is_absent {
                let presence = Presence::Absent;
                to keeps_waiting_once_and_remembers_the_absence { equal(("nothing", true, true)) }
                when it_was_first_absent_less_than_the_grace_ago {
                    let absence = Absence::LessThanTheGrace;
                    to keeps_waiting_whatever_number_of_polls_ran { equal(("nothing", true, true)) }
                }
                when it_was_first_absent_the_whole_grace_ago {
                    let absence = Absence::TheWholeGrace;
                    to reports_it_missing_and_stops_waiting { equal(("not_found", false, false)) }
                }
            }
            when the_task_reappears_after_an_absence {
                let absence = Absence::TheWholeGrace;
                to keeps_waiting_and_forgets_the_absence { equal(("nothing", true, false)) }
                when it_reappears_terminal {
                    let presence = Presence::Terminal;
                    to yields_its_result_and_stops_waiting { equal(("result", false, false)) }
                }
            }
        }

        expect(distinct_ids(ids.into_iter().map(str::to_owned))) as waited_ids {
            let ids = vec!["a", "b"];
            to keeps_distinct_ids_in_their_order { equal(vec!["a".to_owned(), "b".to_owned()]) }
            when an_id_repeats {
                let ids = vec!["a", "b", "a", "b", "c"];
                to keeps_each_id_once_in_its_first_order {
                    equal(vec!["a".to_owned(), "b".to_owned(), "c".to_owned()])
                }
            }
        }

        expect(next_backoff(backoff, Duration::from_secs(2))) as completion_polling_delay {
            let backoff = Duration::from_millis(100);

            to doubles_the_backoff { equal(Duration::from_millis(200)) }

            when doubling_would_exceed_the_cap {
                let backoff = Duration::from_millis(1_500);
                to clamps_to_the_maximum { equal(Duration::from_secs(2)) }
            }

            when the_backoff_already_sits_at_the_cap {
                let backoff = Duration::from_secs(2);
                to stays_at_the_maximum { equal(Duration::from_secs(2)) }
            }
        }

        expect(db_errors_exhausted(error_streak, 3)) as completion_database_error_budget {
            let error_streak = 0u32;

            to keeps_retrying_with_backoff { be_false }

            when one_more_failure_would_reach_the_threshold {
                let error_streak = 1u32;
                to keeps_retrying_with_backoff { be_false }
            }

            when the_failure_streak_reaches_the_threshold {
                let error_streak = 2u32;
                to surfaces_the_error_and_ends_the_wait { be_true }
            }
        }
    }
}
