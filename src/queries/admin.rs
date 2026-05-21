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
    models::{JobRow, QueueInfoRow, StatisticRow, TaskResultRow, WorkerRow, task_result_from_row},
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

pub(crate) fn wait_for_completion<O>(
    pool: PgPool,
    task_ids: impl IntoIterator<Item = TaskId<Ulid>>,
) -> futures::stream::BoxStream<'static, Result<TaskResult<O, Ulid>, Error>>
where
    O: 'static + Send,
    Result<O, String>: DeserializeOwned,
{
    // `Vec<String>` keeps the per-tick clone for `completed_task_rows`
    // (the SQL bind takes ownership) but uses a side `HashSet` of just-
    // completed ids so the per-tick pruning is O(n) instead of O(n·m)
    // (the previous `retain` scanned the full vec for every completed
    // row in the batch).
    let remaining: Vec<String> = task_ids.into_iter().map(|id| id.to_string()).collect();
    // Exponential backoff (100ms → 2s) replaces the previous fixed 500ms
    // poll. Many concurrent `wait_for` callers no longer pin the database
    // at a steady 2 Hz; long-running waits also avoid wasteful re-polls.
    const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
    const MAX_BACKOFF: Duration = Duration::from_secs(2);
    stream::unfold(
        (remaining, INITIAL_BACKOFF),
        move |(remaining_ids, backoff)| {
            let pool = pool.clone();
            async move {
                if remaining_ids.is_empty() {
                    return None;
                }
                let rows = match completed_task_rows(pool, remaining_ids.clone()).await {
                    Ok(rows) => rows,
                    Err(error) => {
                        return Some((
                            stream::iter(vec![Err(error)]),
                            (Vec::new(), INITIAL_BACKOFF),
                        ));
                    }
                };
                if rows.is_empty() {
                    apalis_core::timer::sleep(backoff).await;
                    let next_backoff = (backoff * 2).min(MAX_BACKOFF);
                    return Some((stream::iter(Vec::new()), (remaining_ids, next_backoff)));
                }

                let mut next_remaining = remaining_ids;
                let mut completed_ids: std::collections::HashSet<String> =
                    std::collections::HashSet::with_capacity(rows.len());
                let mut results = Vec::with_capacity(rows.len());
                for row in rows {
                    if let Some(id) = row.id.clone() {
                        completed_ids.insert(id);
                    }
                    results.push(task_result_from_row(row));
                }
                next_remaining.retain(|remaining| !completed_ids.contains(remaining));
                // Reset backoff after observing progress.
                Some((stream::iter(results), (next_remaining, INITIAL_BACKOFF)))
            }
        },
    )
    .flatten()
    .boxed()
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
             ORDER BY done_at DESC, run_at DESC
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
             ORDER BY done_at DESC, run_at DESC
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
               COUNT(*) FILTER (WHERE status = 'Running' AND run_at < now() - INTERVAL '1 hour') AS stale_running_jobs,
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
                       FILTER (WHERE status = 'Running'), 0),
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
                   'value', value,
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
                 SELECT COUNT(*) FILTER (WHERE status = 'Running')::REAL AS running_jobs,
                        COUNT(*) FILTER (WHERE status = 'Pending')::REAL AS pending_jobs,
                        COUNT(*) FILTER (WHERE status = 'Failed')::REAL AS failed_jobs,
                        COUNT(*) FILTER (WHERE status IN ('Pending', 'Running', 'Queued'))::REAL AS active_jobs,
                        COUNT(*) FILTER (
                            WHERE status = 'Running'
                                AND run_at < now() - INTERVAL '1 hour'
                        )::REAL AS stale_running_jobs,
                        ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'Killed') / NULLIF(COUNT(*), 0), 2)::REAL AS kill_rate,
                        COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 hour')::REAL AS jobs_past_hour,
                        COUNT(*) FILTER (
                            WHERE run_at >= CURRENT_DATE
                                AND run_at < CURRENT_DATE + INTERVAL '1 day'
                        )::REAL AS jobs_today,
                        COUNT(*) FILTER (
                            WHERE status = 'Killed'
                                AND run_at >= CURRENT_DATE
                                AND run_at < CURRENT_DATE + INTERVAL '1 day'
                        )::REAL AS killed_jobs_today,
                        ROUND(COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 hour') / 60.0, 2)::REAL AS avg_jobs_per_minute_past_hour,
                        COUNT(*)::REAL AS total_jobs,
                        COUNT(*) FILTER (WHERE status = 'Done')::REAL AS done_jobs,
                        COUNT(*) FILTER (WHERE status IN ('Done', 'Failed', 'Killed'))::REAL AS completed_jobs,
                        COUNT(*) FILTER (WHERE status = 'Killed')::REAL AS killed_jobs,
                        ROUND(100.0 * COUNT(*) FILTER (WHERE status = 'Done') / NULLIF(COUNT(*), 0), 2)::REAL AS success_rate,
                        ROUND(
                            AVG(EXTRACT(EPOCH FROM (done_at - run_at)) / 60.0)
                                FILTER (WHERE status IN ('Done', 'Failed', 'Killed') AND done_at IS NOT NULL),
                            2
                        )::REAL AS avg_job_duration_mins,
                        ROUND(
                            COALESCE(MAX(EXTRACT(EPOCH FROM (now() - run_at)) / 60.0)
                                FILTER (WHERE status = 'Running'), 0),
                            2
                        )::REAL AS longest_running_job_mins,
                        COUNT(*) FILTER (WHERE status = 'Pending' AND run_at <= now())::REAL AS queue_backlog,
                        COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 day')::REAL AS jobs_past_24_hours,
                        COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '7 days')::REAL AS jobs_past_7_days,
                        COUNT(*) FILTER (
                            WHERE status = 'Killed'
                                AND run_at >= now() - INTERVAL '7 days'
                        )::REAL AS killed_jobs_past_7_days,
                        ROUND(
                            100.0 * COUNT(*) FILTER (
                                WHERE status = 'Done'
                                    AND run_at >= now() - INTERVAL '1 day'
                            ) / NULLIF(COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 day'), 0),
                            2
                        )::REAL AS success_rate_past_24h,
                        ROUND(COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '1 day') / 24.0, 2)::REAL AS avg_jobs_per_hour_past_24h,
                        ROUND(COUNT(*) FILTER (WHERE run_at >= now() - INTERVAL '7 days') / 7.0, 2)::REAL AS avg_jobs_per_day_past_7d,
                        EXTRACT(EPOCH FROM MAX(run_at))::REAL AS most_recent_job,
                        EXTRACT(EPOCH FROM (MIN(run_at) FILTER (WHERE status = 'Pending' AND run_at <= now())))::REAL AS oldest_pending_job
                 FROM apalis.jobs {scope}
             ),
             peak_hour AS (
                 SELECT COALESCE(MAX(hourly_count), 0)::REAL AS value
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
                 UNION ALL SELECT 9, 'Number', 'DB_PAGE_SIZE', current_setting('block_size')::INTEGER::REAL
                 UNION ALL SELECT 9, 'Number', 'DB_PAGE_COUNT', (pg_total_relation_size('apalis.jobs') / current_setting('block_size')::INTEGER)::REAL
                 UNION ALL SELECT 9, 'Number', 'DB_SIZE', pg_total_relation_size('apalis.jobs')::REAL
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
        // The conflict UPDATE deliberately does NOT touch `last_seen`
        // (heartbeat-spoofing through admin path is closed) and uses
        // `CASE WHEN lease_token IS NULL` so live worker's `layers` /
        // `storage_name` are preserved (observability-poisoning closed).
        let count = sql_query(
            "WITH registration_lock AS (
                 SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))
             )
             INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at)
             SELECT $1, $2, $3, '', now(), now()
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
                 END",
        )
        .bind::<Text, _>(&worker_id)
        .bind::<Text, _>(worker_type)
        .bind::<Text, _>(crate::STORAGE_NAME)
        .execute(conn)
        .map_err(Error::database("registering worker"))?;
        if count == 0 {
            Err(Error::AlreadyRegistered(worker_id))
        } else {
            Ok(())
        }
    })
}

pub(crate) fn completed_task_rows(
    pool: PgPool,
    ids: Vec<String>,
) -> impl Future<Output = Result<Vec<TaskResultRow>, Error>> + Send {
    with_conn(pool, move |conn| {
        sql_query(
            "SELECT id, status, last_result AS result
             FROM apalis.jobs
             WHERE id = ANY($1)
                 AND (status = 'Done'
                      OR (status = 'Failed' AND attempts >= max_attempts)
                      OR status = 'Killed')",
        )
        .bind::<Array<Text>, _>(ids)
        .load::<TaskResultRow>(conn)
        .map_err(Error::database("fetching completed task results"))
    })
}
