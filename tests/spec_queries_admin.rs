//! Exhaustive specification for `src/queries/admin.rs`.
//!
//! Drives the public admin-facing trait surface (`ListTasks`, `ListAllTasks`,
//! `ListQueues`, `Metrics`) on `PostgresStorage`, plus the `filter_offset_i32`
//! validation that bubbles up through `list_tasks`/`list_all_tasks` when a
//! caller passes `page=0`.
//!
//! Coverage already lives elsewhere; this file does NOT re-test it:
//!   - `run_listing_metrics` (postgres_queries.rs) — broad smoke: list_tasks,
//!     list_all_tasks, list_workers, list_queues, fetch_by_queue, global.
//!   - `run_locked_workers_excludes_terminal` (postgres_specs.rs) — list_queues
//!     `locked_workers` CTE filters terminal statuses.
//!   - `run_list_workers_beyond_100` (postgres_specs.rs) — no hidden LIMIT.
//!   - `run_concurrent_admin_register` (postgres_specs.rs) — admin RegisterWorker.
//!   - `run_fetch_by_id_*`, `run_check_status_variants`, `run_wait_*`
//!     (postgres_queries.rs) — fetch_by_id, check_status, wait_for_completion.
//!   - `i32_from_u32` / `filter_offset_i32` unit tests in `queries/mod.rs`.
//!
//! What we DO pin here (each `expect` block enumerates one scenario):
//!   - `list_tasks` status filter (Done / Failed / Killed / Pending) scopes
//!     correctly per queue.
//!   - `list_tasks` `ORDER BY done_at DESC, run_at DESC` tie-break on run_at.
//!   - `list_tasks` pagination — page=1/page=2 carve disjoint slices, page=0
//!     surfaces `InvalidArgument`.
//!   - `list_all_tasks` — same filter+pagination, but cross-queue: rows from
//!     different `job_type`s coexist in the response.
//!   - `list_queues` — a queue with only `apalis.workers` rows (no jobs) still
//!     appears via the `all_job_types` UNION.
//!   - `metrics_for_queue` — basic counts on a queue with one row of each
//!     terminal status (Done/Failed/Killed) + Pending.
//!   - `metrics_global` — non-scoped variant still emits the static metrics
//!     (DB_PAGE_SIZE, DB_PAGE_COUNT, DB_SIZE).
//!
//! Tests gate on `DATABASE_URL`; without it every scenario resolves to
//! `Outcome::Skipped` and the assertions pass.

#![cfg(feature = "tokio")]

mod support;

use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use apalis_core::{
    backend::{Filter, ListAllTasks, ListQueues, ListTasks, Metrics},
    task::{status::Status, task_id::TaskId},
};
use apalis_diesel_postgres::{Config, PgPool, PostgresStorage};
use diesel::{
    PgConnection, RunQueryDsl, sql_query,
    sql_types::{Integer, Text},
};
use lets_expect::{AssertionError, AssertionResult, *};
use ulid::Ulid;

// --------------------------------------------------------------------------
// shared scaffolding (kept local — concurrent edits in postgres_specs.rs or
// spec_queries_worker.rs shouldn't conflict with this file).
// --------------------------------------------------------------------------

#[derive(Debug)]
enum Outcome<T> {
    Skipped,
    Completed(T),
}

fn observe<T, F>(
    label: &'static str,
    body: F,
) -> impl Fn(&Result<Outcome<T>, String>) -> AssertionResult
where
    F: Fn(&T) -> Result<(), String>,
{
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "{label}: scenario failed: {error}"
        )])),
        Ok(Outcome::Skipped) => Ok(()),
        Ok(Outcome::Completed(run)) => {
            body(run).map_err(|reason| AssertionError::new(vec![format!("{label}: {reason}")]))
        }
    }
}

async fn test_pool() -> Result<Option<PgPool>, String> {
    support::shared_pool().await
}

async fn with_conn<F, T>(pool: PgPool, work: F) -> Result<T, String>
where
    F: FnOnce(&mut PgConnection) -> Result<T, String> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut conn = pool.get().map_err(|e| e.to_string())?;
        work(&mut conn)
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn cleanup_queue(pool: PgPool, queue: String) -> Result<(), String> {
    with_conn(pool, move |conn| {
        sql_query("DELETE FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(&queue)
            .execute(conn)
            .map_err(|e| e.to_string())?;
        sql_query("DELETE FROM apalis.workers WHERE worker_type = $1")
            .bind::<Text, _>(&queue)
            .execute(conn)
            .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_secs()
}

fn fresh_task_id() -> String {
    TaskId::<Ulid>::from_str(&Ulid::new().to_string())
        .expect("generated ULID parses as task id")
        .to_string()
}

// --------------------------------------------------------------------------
// row insertion helpers
//
// Pending rows never carry `lock_by`, so no workers row is needed. Terminal
// rows (Done/Failed/Killed) likewise don't need a `lock_by` (the FK is only
// enforced when `lock_by IS NOT NULL`), so we keep all helpers FK-free.
// --------------------------------------------------------------------------

async fn insert_job(
    pool: PgPool,
    queue: String,
    status: &'static str,
    run_at_offset_secs: i64,
    done_at_offset_secs: Option<i64>,
    attempts: i32,
    max_attempts: i32,
) -> Result<String, String> {
    let id = Ulid::new().to_string();
    let task_id = id.clone();
    with_conn(pool, move |conn| {
        let job = serde_json::to_vec("admin-spec-payload").map_err(|e| e.to_string())?;
        match done_at_offset_secs {
            Some(done) => {
                sql_query(
                    "INSERT INTO apalis.jobs (
                        id, job_type, job, status, attempts, max_attempts, run_at, done_at
                    ) VALUES ($1, $2, $3, $4, $5, $6,
                        now() - ($7 * INTERVAL '1 second'),
                        now() - ($8 * INTERVAL '1 second'))",
                )
                .bind::<Text, _>(&id)
                .bind::<Text, _>(&queue)
                .bind::<diesel::sql_types::Binary, _>(job)
                .bind::<Text, _>(status)
                .bind::<Integer, _>(attempts)
                .bind::<Integer, _>(max_attempts)
                .bind::<Integer, _>(run_at_offset_secs as i32)
                .bind::<Integer, _>(done as i32)
                .execute(conn)
                .map_err(|e| e.to_string())?;
            }
            None => {
                sql_query(
                    "INSERT INTO apalis.jobs (
                        id, job_type, job, status, attempts, max_attempts, run_at
                    ) VALUES ($1, $2, $3, $4, $5, $6,
                        now() - ($7 * INTERVAL '1 second'))",
                )
                .bind::<Text, _>(&id)
                .bind::<Text, _>(&queue)
                .bind::<diesel::sql_types::Binary, _>(job)
                .bind::<Text, _>(status)
                .bind::<Integer, _>(attempts)
                .bind::<Integer, _>(max_attempts)
                .bind::<Integer, _>(run_at_offset_secs as i32)
                .execute(conn)
                .map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    })
    .await?;
    Ok(task_id)
}

async fn insert_worker(pool: PgPool, queue: String, worker_id: String) -> Result<(), String> {
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at)
             VALUES ($1, $2, 'PostgresStorage', '', now(), now())",
        )
        .bind::<Text, _>(&worker_id)
        .bind::<Text, _>(&queue)
        .execute(conn)
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await
}

fn filter(status: Status, page: u32, page_size: Option<u32>) -> Filter {
    Filter {
        status: Some(status),
        page,
        page_size,
    }
}

// --------------------------------------------------------------------------
// list_tasks status-filter matrix
// --------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct StatusFilterSetup {
    /// Status the caller asks `list_tasks` to filter on.
    filter_status: Status,
}

#[derive(Debug)]
struct StatusFilterRun {
    /// Ids of rows returned by list_tasks for this queue + status.
    returned_ids: Vec<String>,
    /// Expected single id for the queue+status combo.
    expected_id: String,
    /// Ids for OTHER statuses in the same queue (must be absent).
    other_status_ids: Vec<String>,
}

async fn run_status_filter(setup: StatusFilterSetup) -> Result<Outcome<StatusFilterRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-status-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Seed one row per status. Pending uses no done_at; terminals do.
    let pending = insert_job(pool.clone(), queue.clone(), "Pending", 1, None, 0, 3).await?;
    let done = insert_job(pool.clone(), queue.clone(), "Done", 5, Some(1), 1, 3).await?;
    let failed = insert_job(pool.clone(), queue.clone(), "Failed", 5, Some(1), 3, 3).await?;
    let killed = insert_job(pool.clone(), queue.clone(), "Killed", 5, Some(1), 1, 3).await?;

    let (expected_id, other_status_ids) = match setup.filter_status {
        Status::Pending => (
            pending.clone(),
            vec![done.clone(), failed.clone(), killed.clone()],
        ),
        Status::Done => (
            done.clone(),
            vec![pending.clone(), failed.clone(), killed.clone()],
        ),
        Status::Failed => (
            failed.clone(),
            vec![pending.clone(), done.clone(), killed.clone()],
        ),
        Status::Killed => (
            killed.clone(),
            vec![pending.clone(), done.clone(), failed.clone()],
        ),
        other => return Err(format!("unsupported status in this matrix: {other:?}")),
    };

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let f = filter(setup.filter_status, 1, Some(20));
    let listed = storage.list_tasks(&f).await.map_err(|e| e.to_string())?;
    let returned_ids: Vec<String> = listed
        .into_iter()
        .filter_map(|t| t.parts.task_id.map(|id| id.to_string()))
        .collect();

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(StatusFilterRun {
        returned_ids,
        expected_id,
        other_status_ids,
    }))
}

fn includes_expected_status_row()
-> impl Fn(&Result<Outcome<StatusFilterRun>, String>) -> AssertionResult {
    observe::<StatusFilterRun, _>("status filter includes target", |run| {
        if run.returned_ids.contains(&run.expected_id) {
            Ok(())
        } else {
            Err(format!(
                "expected returned ids to include {}, got {:?}",
                run.expected_id, run.returned_ids
            ))
        }
    })
}

fn excludes_other_status_rows()
-> impl Fn(&Result<Outcome<StatusFilterRun>, String>) -> AssertionResult {
    observe::<StatusFilterRun, _>("status filter excludes others", |run| {
        for id in &run.other_status_ids {
            if run.returned_ids.contains(id) {
                return Err(format!(
                    "expected {id} (different status, same queue) to be absent, got {:?}",
                    run.returned_ids
                ));
            }
        }
        Ok(())
    })
}

// --------------------------------------------------------------------------
// list_tasks default branches: an unset `status` falls back to Pending, and an
// unset `page_size` falls back to the apalis `DEFAULT_PAGE_SIZE`. Every other
// list_tasks scenario passes `Some(status)` and `Some(page_size)`, so the two
// `unwrap_or` defaults in `src/queries/admin.rs::list_tasks` (status default at
// :55-59, page_size default via `filter.limit()` at :60) had no test leaf.
// --------------------------------------------------------------------------

async fn run_list_tasks_default_status() -> Result<Outcome<StatusFilterRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-default-status-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let pending = insert_job(pool.clone(), queue.clone(), "Pending", 1, None, 0, 3).await?;
    let done = insert_job(pool.clone(), queue.clone(), "Done", 5, Some(1), 1, 3).await?;
    let failed = insert_job(pool.clone(), queue.clone(), "Failed", 5, Some(1), 3, 3).await?;
    let killed = insert_job(pool.clone(), queue.clone(), "Killed", 5, Some(1), 1, 3).await?;

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    // `status: None` must default to Pending.
    let f = Filter {
        status: None,
        page: 1,
        page_size: Some(20),
    };
    let listed = storage.list_tasks(&f).await.map_err(|e| e.to_string())?;
    let returned_ids: Vec<String> = listed
        .into_iter()
        .filter_map(|t| t.parts.task_id.map(|id| id.to_string()))
        .collect();

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(StatusFilterRun {
        returned_ids,
        expected_id: pending,
        other_status_ids: vec![done, failed, killed],
    }))
}

#[derive(Debug)]
struct DefaultLimitRun {
    returned_count: usize,
    default_limit: usize,
}

async fn run_list_tasks_default_page_size() -> Result<Outcome<DefaultLimitRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-default-size-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Read the apalis default so the assertion tracks it instead of hardcoding.
    let default_limit = Filter {
        status: Some(Status::Pending),
        page: 1,
        page_size: None,
    }
    .limit() as usize;
    // Seed comfortably more than the default so the limit is the binding cap.
    // Distinct run_at keeps the ordering total and the cut deterministic.
    for i in 0..(default_limit + 2) {
        insert_job(pool.clone(), queue.clone(), "Pending", 10 + i as i64, None, 0, 3).await?;
    }

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    // `page_size: None` must fall back to `DEFAULT_PAGE_SIZE`.
    let f = Filter {
        status: Some(Status::Pending),
        page: 1,
        page_size: None,
    };
    let returned_count = storage.list_tasks(&f).await.map_err(|e| e.to_string())?.len();

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(DefaultLimitRun {
        returned_count,
        default_limit,
    }))
}

fn returns_default_page_size_rows()
-> impl Fn(&Result<Outcome<DefaultLimitRun>, String>) -> AssertionResult {
    observe::<DefaultLimitRun, _>("default page size", |run| {
        if run.returned_count == run.default_limit {
            Ok(())
        } else {
            Err(format!(
                "expected list_tasks to cap at the default page size {}, got {} rows",
                run.default_limit, run.returned_count
            ))
        }
    })
}

// --------------------------------------------------------------------------
// list_tasks ORDER BY tie-break: same status, different run_at => later first
// --------------------------------------------------------------------------

#[derive(Debug)]
struct OrderRun {
    first_id: String,
    second_id: String,
    older_id: String,
    newer_id: String,
}

async fn run_order_by_run_at() -> Result<Outcome<OrderRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-order-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Two Pending rows, same status, different `run_at`. SQL is
    // `ORDER BY done_at DESC, run_at DESC`; with done_at NULL on both, the
    // run_at DESC arm wins ⇒ newer (offset 10s) before older (offset 600s).
    let older = insert_job(pool.clone(), queue.clone(), "Pending", 600, None, 0, 3).await?;
    let newer = insert_job(pool.clone(), queue.clone(), "Pending", 10, None, 0, 3).await?;

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let listed = storage
        .list_tasks(&filter(Status::Pending, 1, Some(20)))
        .await
        .map_err(|e| e.to_string())?;

    let ids: Vec<String> = listed
        .into_iter()
        .filter_map(|t| t.parts.task_id.map(|id| id.to_string()))
        .collect();
    if ids.len() < 2 {
        return Err(format!("expected at least 2 rows, got {ids:?}"));
    }
    let first_id = ids[0].clone();
    let second_id = ids[1].clone();

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(OrderRun {
        first_id,
        second_id,
        older_id: older,
        newer_id: newer,
    }))
}

fn newer_run_at_listed_first() -> impl Fn(&Result<Outcome<OrderRun>, String>) -> AssertionResult {
    observe::<OrderRun, _>("order by run_at DESC", |run| {
        if run.first_id == run.newer_id && run.second_id == run.older_id {
            Ok(())
        } else {
            Err(format!(
                "expected newer first then older, got first={} second={} (newer={} older={})",
                run.first_id, run.second_id, run.newer_id, run.older_id
            ))
        }
    })
}

// --------------------------------------------------------------------------
// list_tasks pagination matrix
// --------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct PaginationSetup {
    page: u32,
    page_size: u32,
}

#[derive(Debug)]
enum PaginationOutcome {
    Ok { ids: Vec<String> },
    InvalidArgument,
    OtherError(#[allow(dead_code)] String),
}

async fn run_pagination(
    setup: PaginationSetup,
) -> Result<Outcome<(PaginationOutcome, Vec<String>)>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-page-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Seed 5 Pending rows with monotonically newer run_at so the SQL order
    // (run_at DESC) is deterministic: first inserted == oldest == last in
    // the result, last inserted == newest == first.
    let mut seeded: Vec<String> = Vec::new();
    for i in 0..5 {
        let id = insert_job(
            pool.clone(),
            queue.clone(),
            "Pending",
            // offset_secs: 50 - i*10 → 50, 40, 30, 20, 10 (i=4 is newest)
            50 - (i as i64) * 10,
            None,
            0,
            3,
        )
        .await?;
        seeded.push(id);
    }
    // expected order (newest first): seeded[4], seeded[3], seeded[2], seeded[1], seeded[0]
    let expected_order: Vec<String> = seeded.iter().rev().cloned().collect();

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let result = storage
        .list_tasks(&filter(Status::Pending, setup.page, Some(setup.page_size)))
        .await;

    let outcome = match result {
        Ok(tasks) => PaginationOutcome::Ok {
            ids: tasks
                .into_iter()
                .filter_map(|t| t.parts.task_id.map(|id| id.to_string()))
                .collect(),
        },
        Err(err) => {
            // Match the variant by Display surface — `Error::InvalidArgument`
            // formats with "invalid argument" prefix.
            let s = err.to_string();
            if s.to_ascii_lowercase().contains("filter.page")
                || s.to_ascii_lowercase().contains("invalid")
            {
                PaginationOutcome::InvalidArgument
            } else {
                PaginationOutcome::OtherError(s)
            }
        }
    };

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed((outcome, expected_order)))
}

type PaginationRun = Result<Outcome<(PaginationOutcome, Vec<String>)>, String>;

fn pagination_returns_expected_slice(
    start: usize,
    len: usize,
) -> impl Fn(&PaginationRun) -> AssertionResult {
    observe::<(PaginationOutcome, Vec<String>), _>("pagination slice", move |run| match &run.0 {
        PaginationOutcome::Ok { ids } => {
            let expected = &run.1[start..(start + len).min(run.1.len())];
            if ids == expected {
                Ok(())
            } else {
                Err(format!(
                    "expected slice {expected:?} starting at {start} len {len}, got {ids:?}"
                ))
            }
        }
        other => Err(format!("expected Ok response, got {other:?}")),
    })
}

fn pagination_rejected_as_invalid_argument() -> impl Fn(&PaginationRun) -> AssertionResult {
    observe::<(PaginationOutcome, Vec<String>), _>("pagination page=0", |run| match &run.0 {
        PaginationOutcome::InvalidArgument => Ok(()),
        other => Err(format!(
            "expected InvalidArgument error for page=0, got {other:?}"
        )),
    })
}

// --------------------------------------------------------------------------
// list_all_tasks: rows from multiple queues coexist
// --------------------------------------------------------------------------

#[derive(Debug)]
struct ListAllRun {
    queue_a_id: String,
    queue_b_id: String,
    returned_ids: Vec<String>,
}

async fn run_list_all_cross_queue() -> Result<Outcome<ListAllRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue_a = format!("apalis-spec-admin-all-a-{}", Ulid::new());
    let queue_b = format!("apalis-spec-admin-all-b-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue_a.clone()).await?;
    cleanup_queue(pool.clone(), queue_b.clone()).await?;

    let a_id = insert_job(pool.clone(), queue_a.clone(), "Pending", 5, None, 0, 3).await?;
    let b_id = insert_job(pool.clone(), queue_b.clone(), "Pending", 5, None, 0, 3).await?;

    // `list_all_tasks` doesn't filter by job_type, so any storage instance
    // can read it. The configured queue is irrelevant here.
    let config = Config::new(&queue_a);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);

    // Use a large page size to make sure both rows are in the slice even if
    // the global table has many other Pending rows from concurrent tests.
    let listed = storage
        .list_all_tasks(&filter(Status::Pending, 1, Some(1000)))
        .await
        .map_err(|e| e.to_string())?;
    let returned_ids: Vec<String> = listed
        .into_iter()
        .filter_map(|t| t.parts.task_id.map(|id| id.to_string()))
        .collect();

    cleanup_queue(pool.clone(), queue_a).await?;
    cleanup_queue(pool, queue_b).await?;
    Ok(Outcome::Completed(ListAllRun {
        queue_a_id: a_id,
        queue_b_id: b_id,
        returned_ids,
    }))
}

fn list_all_returns_rows_from_both_queues()
-> impl Fn(&Result<Outcome<ListAllRun>, String>) -> AssertionResult {
    observe::<ListAllRun, _>("list_all cross-queue", |run| {
        let has_a = run.returned_ids.contains(&run.queue_a_id);
        let has_b = run.returned_ids.contains(&run.queue_b_id);
        if has_a && has_b {
            Ok(())
        } else {
            Err(format!(
                "expected both queue_a={} and queue_b={} ids in result, got has_a={has_a} has_b={has_b}",
                run.queue_a_id, run.queue_b_id
            ))
        }
    })
}

// --------------------------------------------------------------------------
// list_queues: workers-only queue still appears (UNION in all_job_types)
// --------------------------------------------------------------------------

#[derive(Debug)]
struct WorkersOnlyQueueRun {
    queue: String,
    queue_names: Vec<String>,
}

async fn run_workers_only_queue() -> Result<Outcome<WorkersOnlyQueueRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-workers-only-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    // Worker registered, zero jobs in this queue.
    insert_worker(
        pool.clone(),
        queue.clone(),
        format!("workers-only-worker-{}", Ulid::new()),
    )
    .await?;

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let queues = storage.list_queues().await.map_err(|e| e.to_string())?;
    let queue_names: Vec<String> = queues.into_iter().map(|q| q.name).collect();

    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(WorkersOnlyQueueRun {
        queue,
        queue_names,
    }))
}

fn workers_only_queue_appears_in_list()
-> impl Fn(&Result<Outcome<WorkersOnlyQueueRun>, String>) -> AssertionResult {
    observe::<WorkersOnlyQueueRun, _>("list_queues UNION", |run| {
        if run.queue_names.contains(&run.queue) {
            Ok(())
        } else {
            Err(format!(
                "expected queue {} to appear in list_queues (workers-only branch), got names containing {} entries",
                run.queue,
                run.queue_names.len()
            ))
        }
    })
}

// --------------------------------------------------------------------------
// metrics_for_queue: terminal-status counts surface correctly
// --------------------------------------------------------------------------

#[derive(Debug)]
struct MetricsRun {
    done_jobs: Option<String>,
    failed_jobs: Option<String>,
    killed_jobs: Option<String>,
    pending_jobs: Option<String>,
    total_jobs: Option<String>,
}

async fn run_metrics_terminal_mix() -> Result<Outcome<MetricsRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-metrics-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Seed: 2 Pending, 1 Done, 1 Failed (terminal), 1 Killed. The `_now`
    // offsets put run_at within the past few seconds so window-scoped
    // metrics also catch them.
    let _ = insert_job(pool.clone(), queue.clone(), "Pending", 1, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), queue.clone(), "Pending", 2, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), queue.clone(), "Done", 5, Some(1), 1, 3).await?;
    let _ = insert_job(pool.clone(), queue.clone(), "Failed", 5, Some(1), 3, 3).await?;
    let _ = insert_job(pool.clone(), queue.clone(), "Killed", 5, Some(1), 1, 3).await?;

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let stats = storage.fetch_by_queue().await.map_err(|e| e.to_string())?;

    let by_title = |title: &str| {
        stats
            .iter()
            .find(|s| s.title == title)
            .map(|s| s.value.clone())
    };

    let run = MetricsRun {
        done_jobs: by_title("DONE_JOBS"),
        failed_jobs: by_title("FAILED_JOBS"),
        killed_jobs: by_title("KILLED_JOBS"),
        pending_jobs: by_title("PENDING_JOBS"),
        total_jobs: by_title("TOTAL_JOBS"),
    };

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(run))
}

/// Metric values come back as `REAL` rendered to text, e.g. "1" or "1.0";
/// the SQL builds `value AS REAL` from a COUNT(*) so the integer rendering
/// is what Postgres chooses. Accept any representation parseable to the
/// expected count.
fn metric_count_approx_equals(
    title: &'static str,
    expected: f64,
) -> impl Fn(&MetricsRun) -> Result<(), String> {
    move |run: &MetricsRun| {
        let value_opt = match title {
            "DONE_JOBS" => &run.done_jobs,
            "FAILED_JOBS" => &run.failed_jobs,
            "KILLED_JOBS" => &run.killed_jobs,
            "PENDING_JOBS" => &run.pending_jobs,
            "TOTAL_JOBS" => &run.total_jobs,
            other => return Err(format!("unsupported metric title in matcher: {other}")),
        };
        let Some(value) = value_opt else {
            return Err(format!("expected {title} to be present in queue metrics"));
        };
        let parsed: f64 = value
            .parse()
            .map_err(|_| format!("expected {title} value to parse as a number, got {value:?}"))?;
        if (parsed - expected).abs() < 0.5 {
            Ok(())
        } else {
            Err(format!(
                "expected {title} ≈ {expected}, got {parsed} (raw {value:?})"
            ))
        }
    }
}

fn metric_value_is(
    title: &'static str,
    expected: f64,
) -> impl Fn(&Result<Outcome<MetricsRun>, String>) -> AssertionResult {
    let body = metric_count_approx_equals(title, expected);
    observe::<MetricsRun, _>(title, body)
}

// --------------------------------------------------------------------------
// metrics_global: static metrics (DB_PAGE_SIZE / DB_PAGE_COUNT / DB_SIZE)
// --------------------------------------------------------------------------

#[derive(Debug)]
struct GlobalMetricsRun {
    page_size: Option<String>,
    page_count: Option<String>,
    db_size: Option<String>,
    total_present: bool,
}

async fn run_metrics_global() -> Result<Outcome<GlobalMetricsRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-global-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    // At least one row, so TOTAL_JOBS is well-defined even on a quiet DB.
    let _ = insert_job(pool.clone(), queue.clone(), "Pending", 1, None, 0, 3).await?;

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let stats = storage.global().await.map_err(|e| e.to_string())?;

    let by_title = |t: &str| stats.iter().find(|s| s.title == t).map(|s| s.value.clone());
    let run = GlobalMetricsRun {
        page_size: by_title("DB_PAGE_SIZE"),
        page_count: by_title("DB_PAGE_COUNT"),
        db_size: by_title("DB_SIZE"),
        total_present: stats.iter().any(|s| s.title == "TOTAL_JOBS"),
    };

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(run))
}

fn db_page_size_is_positive_real()
-> impl Fn(&Result<Outcome<GlobalMetricsRun>, String>) -> AssertionResult {
    observe::<GlobalMetricsRun, _>("DB_PAGE_SIZE positive", |run| {
        let Some(value) = run.page_size.as_ref() else {
            return Err("expected DB_PAGE_SIZE to be present".into());
        };
        let parsed: f64 = value
            .parse()
            .map_err(|_| format!("expected DB_PAGE_SIZE to parse, got {value:?}"))?;
        if parsed > 0.0 {
            Ok(())
        } else {
            Err(format!("expected DB_PAGE_SIZE > 0, got {parsed}"))
        }
    })
}

fn db_page_count_and_size_present()
-> impl Fn(&Result<Outcome<GlobalMetricsRun>, String>) -> AssertionResult {
    observe::<GlobalMetricsRun, _>("DB_PAGE_COUNT/DB_SIZE", |run| {
        if run.page_count.is_some() && run.db_size.is_some() {
            Ok(())
        } else {
            Err(format!(
                "expected DB_PAGE_COUNT and DB_SIZE present, got page_count={:?} db_size={:?}",
                run.page_count, run.db_size
            ))
        }
    })
}

fn total_jobs_metric_present()
-> impl Fn(&Result<Outcome<GlobalMetricsRun>, String>) -> AssertionResult {
    observe::<GlobalMetricsRun, _>("TOTAL_JOBS in global", |run| {
        if run.total_present {
            Ok(())
        } else {
            Err("expected TOTAL_JOBS in global metrics".into())
        }
    })
}

// --------------------------------------------------------------------------
// expectations
// --------------------------------------------------------------------------

lets_expect! { #tokio_test
    // ----- list_tasks status filter matrix --------------------------------
    expect(run_status_filter(setup).await) {
        let setup = StatusFilterSetup { filter_status: Status::Pending };

        when filtering_by_pending_returns_only_pending_rows {
            to includes_the_pending_row { includes_expected_status_row() }
            to excludes_done_failed_and_killed { excludes_other_status_rows() }
        }

        when filtering_by_done_returns_only_done_rows {
            let setup = StatusFilterSetup { filter_status: Status::Done };
            to includes_the_done_row { includes_expected_status_row() }
            to excludes_pending_failed_and_killed { excludes_other_status_rows() }
        }

        when filtering_by_failed_returns_only_failed_rows {
            let setup = StatusFilterSetup { filter_status: Status::Failed };
            to includes_the_failed_row { includes_expected_status_row() }
            to excludes_pending_done_and_killed { excludes_other_status_rows() }
        }

        when filtering_by_killed_returns_only_killed_rows {
            let setup = StatusFilterSetup { filter_status: Status::Killed };
            to includes_the_killed_row { includes_expected_status_row() }
            to excludes_pending_done_and_failed { excludes_other_status_rows() }
        }
    }

    // ----- list_tasks default filter branches ------------------------------
    expect(run_list_tasks_default_status().await) {
        when the_filter_status_is_unset {
            to defaults_to_listing_pending_rows { includes_expected_status_row() }
            to excludes_non_pending_rows { excludes_other_status_rows() }
        }
    }

    expect(run_list_tasks_default_page_size().await) {
        when no_page_size_is_supplied {
            to caps_results_at_the_default_page_size { returns_default_page_size_rows() }
        }
    }

    // ----- list_tasks ORDER BY tie-break ----------------------------------
    expect(run_order_by_run_at().await) {
        when two_pending_rows_differ_only_in_run_at {
            to ranks_the_newer_run_at_row_first { newer_run_at_listed_first() }
        }
    }

    // ----- list_tasks pagination matrix -----------------------------------
    expect(run_pagination(setup).await) {
        let setup = PaginationSetup { page: 1, page_size: 2 };

        when the_first_page_with_size_two_is_requested {
            to returns_the_first_two_rows_in_descending_run_at {
                pagination_returns_expected_slice(0, 2)
            }
        }

        when the_second_page_with_size_two_is_requested {
            let setup = PaginationSetup { page: 2, page_size: 2 };
            to returns_the_next_two_rows {
                pagination_returns_expected_slice(2, 2)
            }
        }

        when the_third_page_with_size_two_holds_the_remainder {
            let setup = PaginationSetup { page: 3, page_size: 2 };
            to returns_only_the_remaining_row {
                pagination_returns_expected_slice(4, 1)
            }
        }

        when page_zero_is_invalid {
            let setup = PaginationSetup { page: 0, page_size: 2 };
            to surfaces_invalid_argument_via_filter_offset_i32 {
                pagination_rejected_as_invalid_argument()
            }
        }
    }

    // ----- list_all_tasks crosses job_type boundaries ---------------------
    expect(run_list_all_cross_queue().await) {
        when two_pending_rows_live_in_two_different_queues {
            to returns_rows_from_both_queues_because_list_all_drops_the_job_type_filter {
                list_all_returns_rows_from_both_queues()
            }
        }
    }

    // ----- list_queues UNION over workers ∪ jobs --------------------------
    expect(run_workers_only_queue().await) {
        when a_queue_only_has_workers_no_jobs {
            to still_appears_in_list_queues_via_the_all_job_types_union {
                workers_only_queue_appears_in_list()
            }
        }
    }

    // ----- metrics_for_queue counts ---------------------------------------
    expect(run_metrics_terminal_mix().await) {
        when a_queue_has_one_row_of_each_terminal_status_and_two_pending_rows {
            to reports_two_pending_jobs { metric_value_is("PENDING_JOBS", 2.0) }
            to reports_one_done_job { metric_value_is("DONE_JOBS", 1.0) }
            to reports_one_failed_job { metric_value_is("FAILED_JOBS", 1.0) }
            to reports_one_killed_job { metric_value_is("KILLED_JOBS", 1.0) }
            to reports_total_of_five_jobs { metric_value_is("TOTAL_JOBS", 5.0) }
        }
    }

    // ----- metrics_global static metrics ----------------------------------
    expect(run_metrics_global().await) {
        when the_global_metrics_query_is_executed {
            to surfaces_db_page_size_from_pg_settings { db_page_size_is_positive_real() }
            to surfaces_page_count_and_total_size_for_apalis_jobs {
                db_page_count_and_size_present()
            }
            to includes_the_total_jobs_aggregate { total_jobs_metric_present() }
        }
    }
}

// `now_unix` and `fresh_task_id` are kept to mirror the scaffolding shape of
// `spec_queries_worker.rs`; silence dead_code for the ones we don't reach
// for in this file.
#[allow(dead_code)]
fn _suppress_dead_code() {
    let _ = now_unix();
    let _ = fresh_task_id();
}
