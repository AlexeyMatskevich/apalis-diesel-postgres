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
//!   - `list_tasks` status filter (all six `Status` variants: Pending / Queued
//!     / Running / Done / Failed / Killed) scopes correctly per queue.
//!   - `list_tasks` `ORDER BY done_at DESC, run_at DESC`: primary done_at DESC
//!     across terminal rows, secondary run_at DESC tie-break.
//!   - `list_tasks` pagination — page=1/page=2 carve disjoint slices, page=0
//!     surfaces `InvalidArgument`.
//!   - `list_all_tasks` — the global listing's own axes: `page=0` surfaces
//!     `InvalidArgument` via `filter_offset_i32`, a non-zero OFFSET shifts the
//!     result past the first global row, AND the `LIMIT $2` bind caps each
//!     `page_size=1` request at exactly one row (a dropped LIMIT would
//!     over-return the whole global tail). The cross-queue "rows from many
//!     job_types coexist" property is owned by `run_listing_metrics`, not here.
//!   - `list_queues` — a queue with only `apalis.workers` rows (no jobs) still
//!     appears via the `all_job_types` UNION, a jobs-backed queue surfaces its
//!     `queue_stats` CTE counts (PENDING_JOBS / TOTAL_JOBS) in `stats`, AND the
//!     `daily_activity` CTE buckets per-day counts (ordered by `run_date`,
//!     inside the 7-day window) into `QueueInfo.activity`.
//!   - `metrics_for_queue` — counts on a queue mixing active (Pending / Running
//!     / Queued) and terminal (Done / Failed / Killed) rows, including the
//!     RUNNING_JOBS and ACTIVE_JOBS active-state filters, AND the
//!     `WHERE job_type` scope excluding a decoy queue's rows.
//!   - `metrics_global` — non-scoped variant still emits the static metrics
//!     (DB_PAGE_SIZE, DB_PAGE_COUNT, DB_SIZE) AND aggregates TOTAL_JOBS /
//!     DONE_JOBS across more than one queue.
//!
//! Tests gate on `DATABASE_URL`; without it every scenario resolves to
//! `Outcome::Skipped` and the assertions pass.

#![cfg(feature = "tokio")]

mod support;

use support::{Outcome, observe, with_conn};

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
    RunQueryDsl, sql_query,
    sql_types::{Integer, Text},
};
use lets_expect::{AssertionResult, *};
use ulid::Ulid;

async fn test_pool() -> Result<Option<PgPool>, String> {
    support::shared_pool().await
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

    // Seed one row per status. Non-terminal rows (Pending / Queued / Running)
    // carry no done_at; terminal rows (Done / Failed / Killed) do. All six
    // `Status` variants are seeded so the filter matrix can pin every reachable
    // value of the `WHERE status = $1` bind, including the active statuses
    // Queued and Running (both permitted by the jobs_status_check CHECK).
    let pending = insert_job(pool.clone(), queue.clone(), "Pending", 1, None, 0, 3).await?;
    let queued = insert_job(pool.clone(), queue.clone(), "Queued", 2, None, 0, 3).await?;
    let running = insert_job(pool.clone(), queue.clone(), "Running", 3, None, 0, 3).await?;
    let done = insert_job(pool.clone(), queue.clone(), "Done", 5, Some(1), 1, 3).await?;
    let failed = insert_job(pool.clone(), queue.clone(), "Failed", 5, Some(1), 3, 3).await?;
    let killed = insert_job(pool.clone(), queue.clone(), "Killed", 5, Some(1), 1, 3).await?;

    let (expected_id, other_status_ids) = match setup.filter_status {
        Status::Pending => (
            pending.clone(),
            vec![
                queued.clone(),
                running.clone(),
                done.clone(),
                failed.clone(),
                killed.clone(),
            ],
        ),
        Status::Queued => (
            queued.clone(),
            vec![
                pending.clone(),
                running.clone(),
                done.clone(),
                failed.clone(),
                killed.clone(),
            ],
        ),
        Status::Running => (
            running.clone(),
            vec![
                pending.clone(),
                queued.clone(),
                done.clone(),
                failed.clone(),
                killed.clone(),
            ],
        ),
        Status::Done => (
            done.clone(),
            vec![
                pending.clone(),
                queued.clone(),
                running.clone(),
                failed.clone(),
                killed.clone(),
            ],
        ),
        Status::Failed => (
            failed.clone(),
            vec![
                pending.clone(),
                queued.clone(),
                running.clone(),
                done.clone(),
                killed.clone(),
            ],
        ),
        Status::Killed => (
            killed.clone(),
            vec![
                pending.clone(),
                queued.clone(),
                running.clone(),
                done.clone(),
                failed.clone(),
            ],
        ),
        // `Status` is `#[non_exhaustive]`; every variant that currently exists
        // is enumerated above. A future variant would need its own seeded row
        // and matrix arm rather than silently falling through.
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
        insert_job(
            pool.clone(),
            queue.clone(),
            "Pending",
            10 + i as i64,
            None,
            0,
            3,
        )
        .await?;
    }

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    // `page_size: None` must fall back to `DEFAULT_PAGE_SIZE`.
    let f = Filter {
        status: Some(Status::Pending),
        page: 1,
        page_size: None,
    };
    let returned_count = storage
        .list_tasks(&f)
        .await
        .map_err(|e| e.to_string())?
        .len();

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
// list_tasks ORDER BY primary key: same status, different done_at => later
// done_at first, isolated from the run_at tie-break via crossed offsets.
// --------------------------------------------------------------------------

async fn run_order_by_done_at() -> Result<Outcome<OrderRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-order-done-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Two terminal Done rows with CROSSED offsets (offset = seconds in the
    // past, so a SMALLER offset is MORE recent). `recent_done` has a recent
    // done_at but an OLD run_at; `old_done` has an old done_at but a RECENT
    // run_at. SQL is `ORDER BY done_at DESC, run_at DESC`: the primary done_at
    // key must list `recent_done` first. If the primary key regressed to
    // run_at DESC, `old_done` (recent run_at) would lead instead — so this
    // pins the primary key, not the tie-break.
    let recent_done = insert_job(pool.clone(), queue.clone(), "Done", 600, Some(10), 0, 3).await?;
    let old_done = insert_job(pool.clone(), queue.clone(), "Done", 10, Some(600), 0, 3).await?;

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let listed = storage
        .list_tasks(&filter(Status::Done, 1, Some(20)))
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
        older_id: old_done,
        newer_id: recent_done,
    }))
}

fn later_done_at_listed_first() -> impl Fn(&Result<Outcome<OrderRun>, String>) -> AssertionResult {
    observe::<OrderRun, _>("order by done_at DESC", |run| {
        if run.first_id == run.newer_id && run.second_id == run.older_id {
            Ok(())
        } else {
            Err(format!(
                "expected later-done_at first then earlier, got first={} second={} (later={} earlier={})",
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
// list_all_tasks: this file owns the validation + pagination axes of the
// global listing. The cross-queue "rows from many job_types coexist" property
// is already owned by `run_listing_metrics` (postgres_queries.rs:
// list_all_tasks_covers_all_queues), so we do NOT re-test it here. Instead we
// pin (a) `page=0` surfacing `InvalidArgument` via `filter_offset_i32` — the
// same routing as `list_tasks`, but never exercised through `list_all_tasks`
// across the suite — and (b) a non-zero OFFSET carving the correct slice.
// --------------------------------------------------------------------------

async fn run_list_all_page_zero() -> Result<Outcome<PaginationOutcome>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-all-page0-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    // One row is enough; the page=0 validation rejects before any scan.
    let _ = insert_job(pool.clone(), queue.clone(), "Pending", 5, None, 0, 3).await?;

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let result = storage
        .list_all_tasks(&filter(Status::Pending, 0, Some(2)))
        .await;

    let outcome = match result {
        Ok(tasks) => PaginationOutcome::Ok {
            ids: tasks
                .into_iter()
                .filter_map(|t| t.parts.task_id.map(|id| id.to_string()))
                .collect(),
        },
        Err(err) => {
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
    Ok(Outcome::Completed(outcome))
}

fn list_all_page_zero_rejected_as_invalid_argument()
-> impl Fn(&Result<Outcome<PaginationOutcome>, String>) -> AssertionResult {
    observe::<PaginationOutcome, _>("list_all page=0", |run| match run {
        PaginationOutcome::InvalidArgument => Ok(()),
        other => Err(format!(
            "expected InvalidArgument error for list_all_tasks page=0, got {other:?}"
        )),
    })
}

// --------------------------------------------------------------------------
// list_all_tasks non-zero OFFSET + ORDER BY: pins the LIMIT $2 / OFFSET $3
// binds and the `ORDER BY done_at DESC, run_at DESC` on the GLOBAL listing
// (src/queries/admin.rs::list_all_tasks_rows). `run_list_all_page_zero` only
// exercises the pre-scan `filter_offset_i32` rejection, so the actual slicing
// and ordering of `list_all_tasks_rows` had no coverage — a swapped $2/$3 bind
// (limit/offset) or a lost ORDER BY would pass green.
//
// `list_all_tasks` is global (no `job_type` scope), so to stay deterministic on
// a shared DB we seed three Done rows with `done_at` at ~now() (offsets 2/1/0s
// in the past). Those are the three most-recently-completed Done rows in the
// whole table, so `ORDER BY done_at DESC` puts them at global positions 0/1/2
// regardless of what other queues hold. We then walk pages of size 1 and check
// the OFFSET carves the expected member of our own newest→oldest sequence.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct ListAllOffsetRun {
    /// Our seeded ids, newest done_at first: [newest, middle, oldest].
    seeded_newest_first: Vec<String>,
    /// First id returned for page=1,size=1 (offset 0) — expected `newest`.
    page1_first: Option<String>,
    /// First id returned for page=2,size=1 (offset 1) — expected `middle`.
    page2_first: Option<String>,
    /// First id returned for page=3,size=1 (offset 2) — expected `oldest`.
    page3_first: Option<String>,
    /// Row COUNT returned for each size=1 page. With `LIMIT $2 = 1` every page
    /// holds exactly one row; a regression that dropped `LIMIT` (keeping only
    /// `OFFSET`) would return the whole global Done tail, so pinning the length
    /// — not just the first id — is what catches a lost limit.
    page1_len: usize,
    page2_len: usize,
    page3_len: usize,
}

async fn run_list_all_offset_slice() -> Result<Outcome<ListAllOffsetRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-all-offset-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Three Done rows with done_at ~10 years in the future (offsets are
    // negative, and insert_job computes `now() - offset`), so they sort ahead
    // of every real-world Done row in this shared table regardless of what
    // other tests (or concurrent runs) have left behind — `list_all_tasks` is
    // deliberately global (no queue filter), so a mere unique queue name does
    // not isolate this listing the way it does for queue-scoped specs.
    // Newest done_at = smallest offset. run_at is crossed against done_at so a
    // regression to the secondary run_at key alone would reorder them and
    // fail the assertions below.
    const FAR_FUTURE_SECS: i64 = 315_360_000; // ~10 years
    let oldest = insert_job(
        pool.clone(),
        queue.clone(),
        "Done",
        10,
        Some(2 - FAR_FUTURE_SECS),
        1,
        3,
    )
    .await?;
    let middle = insert_job(
        pool.clone(),
        queue.clone(),
        "Done",
        20,
        Some(1 - FAR_FUTURE_SECS),
        1,
        3,
    )
    .await?;
    let newest = insert_job(
        pool.clone(),
        queue.clone(),
        "Done",
        30,
        Some(-FAR_FUTURE_SECS),
        1,
        3,
    )
    .await?;

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);

    // Collect the FULL page for each size=1 request (not just `.next()`), so we
    // can pin both the first id (OFFSET carved the right row) AND the page
    // length (LIMIT capped it to one row). A lost `LIMIT` would leave the first
    // id correct but blow the length past 1.
    let page_ids = |page: u32| {
        let storage = &storage;
        async move {
            storage
                .list_all_tasks(&filter(Status::Done, page, Some(1)))
                .await
                .map_err(|e| e.to_string())
                .map(|tasks| {
                    tasks
                        .into_iter()
                        .filter_map(|t| t.parts.task_id.map(|id| id.to_string()))
                        .collect::<Vec<String>>()
                })
        }
    };
    let page1 = page_ids(1).await?;
    let page2 = page_ids(2).await?;
    let page3 = page_ids(3).await?;

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(ListAllOffsetRun {
        seeded_newest_first: vec![newest, middle, oldest],
        page1_first: page1.first().cloned(),
        page2_first: page2.first().cloned(),
        page3_first: page3.first().cloned(),
        page1_len: page1.len(),
        page2_len: page2.len(),
        page3_len: page3.len(),
    }))
}

fn list_all_orders_newest_done_at_first()
-> impl Fn(&Result<Outcome<ListAllOffsetRun>, String>) -> AssertionResult {
    observe::<ListAllOffsetRun, _>("list_all ORDER BY done_at DESC", |run| {
        let expected = run.seeded_newest_first.first().cloned();
        if run.page1_first == expected {
            Ok(())
        } else {
            Err(format!(
                "expected the newest done_at row {expected:?} first (offset 0), got {:?}",
                run.page1_first
            ))
        }
    })
}

fn list_all_offset_one_skips_the_first_row()
-> impl Fn(&Result<Outcome<ListAllOffsetRun>, String>) -> AssertionResult {
    observe::<ListAllOffsetRun, _>("list_all OFFSET 1", |run| {
        let expected = run.seeded_newest_first.get(1).cloned();
        if run.page2_first == expected {
            Ok(())
        } else {
            Err(format!(
                "expected OFFSET 1 to skip to the second-newest row {expected:?}, got {:?}",
                run.page2_first
            ))
        }
    })
}

fn list_all_offset_two_skips_the_first_two_rows()
-> impl Fn(&Result<Outcome<ListAllOffsetRun>, String>) -> AssertionResult {
    observe::<ListAllOffsetRun, _>("list_all OFFSET 2", |run| {
        let expected = run.seeded_newest_first.get(2).cloned();
        if run.page3_first == expected {
            Ok(())
        } else {
            Err(format!(
                "expected OFFSET 2 to skip to the third-newest row {expected:?}, got {:?}",
                run.page3_first
            ))
        }
    })
}

fn list_all_limits_each_page_to_one_row()
-> impl Fn(&Result<Outcome<ListAllOffsetRun>, String>) -> AssertionResult {
    observe::<ListAllOffsetRun, _>("list_all LIMIT 1", |run| {
        // Every request asks for `page_size = 1`, so `LIMIT $2 = 1` must cap
        // each page at exactly one row. This is the arm the first-id checks
        // can't cover: if `LIMIT` were dropped while `OFFSET $3` stayed, the
        // global Done listing would return its whole tail (three seeded rows
        // here, plus any other Done rows on a shared DB) yet still lead with the
        // correct id — passing the ordering/offset assertions but failing here.
        for (page, len) in [
            (1u32, run.page1_len),
            (2, run.page2_len),
            (3, run.page3_len),
        ] {
            if len != 1 {
                return Err(format!(
                    "expected page={page} (size=1) to return exactly one row, got {len} \
                     (a dropped LIMIT would over-return here)"
                ));
            }
        }
        Ok(())
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
// list_queues: a jobs-backed queue surfaces its `queue_stats` CTE in
// `QueueInfo.stats`. Pins the `queue_stats` aggregation + the
// `COALESCE(qs.stats, '[]') AS stats` join so a drift that empties or
// miswires the stats JSON goes RED (the workers-only scenario above only
// exercises the empty-stats `[]` branch).
// --------------------------------------------------------------------------

#[derive(Debug)]
struct QueueStatsRun {
    /// Stat titles present on the seeded queue's `QueueInfo.stats`.
    stat_titles: Vec<String>,
    /// Value reported for the `PENDING_JOBS` stat, if present.
    pending_jobs_value: Option<String>,
}

async fn run_queue_stats_for_jobs_backed_queue() -> Result<Outcome<QueueStatsRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-queue-stats-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    // One Pending job, so PENDING_JOBS = 1 and TOTAL_JOBS = 1.
    let _ = insert_job(pool.clone(), queue.clone(), "Pending", 1, None, 0, 3).await?;

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let queues = storage.list_queues().await.map_err(|e| e.to_string())?;

    let info = queues
        .into_iter()
        .find(|q| q.name == queue)
        .ok_or_else(|| format!("seeded queue {queue} missing from list_queues"))?;
    let stat_titles: Vec<String> = info.stats.iter().map(|s| s.title.clone()).collect();
    let pending_jobs_value = info
        .stats
        .iter()
        .find(|s| s.title == "PENDING_JOBS")
        .map(|s| s.value.clone());

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(QueueStatsRun {
        stat_titles,
        pending_jobs_value,
    }))
}

fn queue_stats_carry_pending_and_total_titles()
-> impl Fn(&Result<Outcome<QueueStatsRun>, String>) -> AssertionResult {
    observe::<QueueStatsRun, _>("queue_stats CTE titles", |run| {
        for title in ["PENDING_JOBS", "TOTAL_JOBS"] {
            if !run.stat_titles.iter().any(|t| t == title) {
                return Err(format!(
                    "expected queue stats to include {title}, got {:?}",
                    run.stat_titles
                ));
            }
        }
        Ok(())
    })
}

fn queue_stats_report_one_pending_job()
-> impl Fn(&Result<Outcome<QueueStatsRun>, String>) -> AssertionResult {
    observe::<QueueStatsRun, _>("queue_stats PENDING_JOBS value", |run| {
        let Some(value) = run.pending_jobs_value.as_ref() else {
            return Err("expected PENDING_JOBS stat to be present".into());
        };
        if value == "1" {
            Ok(())
        } else {
            Err(format!("expected PENDING_JOBS value \"1\", got {value:?}"))
        }
    })
}

// --------------------------------------------------------------------------
// list_queues activity: the `daily_activity` CTE
// (src/queries/admin.rs::LIST_QUEUES_SQL) buckets jobs by `run_at::date`
// inside a 7-day window and emits `jsonb_agg(daily_count ORDER BY run_date)`,
// decoded into `QueueInfo.activity: Vec<usize>`. No integration scenario
// touched `activity` before, so a regression in the per-day COUNT, the
// `ORDER BY run_date`, or the 7-day window would pass green. We seed known
// per-day counts on two distinct calendar days (plus one row outside the
// window that must be dropped) and assert the resulting activity vector.
// --------------------------------------------------------------------------

const SECS_PER_DAY: i64 = 86_400;
const HALF_DAY: i64 = 43_200;

#[derive(Debug)]
struct QueueActivityRun {
    activity: Vec<usize>,
}

async fn run_queue_activity_daily_buckets() -> Result<Outcome<QueueActivityRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-activity-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Two rows on the "2 days ago" calendar date and three on "1 day ago",
    // each pinned to mid-day so the group can't straddle midnight. run_date
    // ascending is [day-2, day-1], so ORDER BY run_date yields counts [2, 3].
    let two_days_ago = 2 * SECS_PER_DAY + HALF_DAY;
    let one_day_ago = SECS_PER_DAY + HALF_DAY;
    for _ in 0..2 {
        insert_job(
            pool.clone(),
            queue.clone(),
            "Pending",
            two_days_ago,
            None,
            0,
            3,
        )
        .await?;
    }
    for _ in 0..3 {
        insert_job(
            pool.clone(),
            queue.clone(),
            "Pending",
            one_day_ago,
            None,
            0,
            3,
        )
        .await?;
    }
    // Outside the 7-day window: must be excluded from `activity` entirely.
    let eight_days_ago = 8 * SECS_PER_DAY + HALF_DAY;
    insert_job(
        pool.clone(),
        queue.clone(),
        "Pending",
        eight_days_ago,
        None,
        0,
        3,
    )
    .await?;

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let queues = storage.list_queues().await.map_err(|e| e.to_string())?;
    let info = queues
        .into_iter()
        .find(|q| q.name == queue)
        .ok_or_else(|| format!("seeded queue {queue} missing from list_queues"))?;

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(QueueActivityRun {
        activity: info.activity,
    }))
}

fn queue_activity_buckets_daily_counts_in_run_date_order()
-> impl Fn(&Result<Outcome<QueueActivityRun>, String>) -> AssertionResult {
    observe::<QueueActivityRun, _>("daily_activity buckets", |run| {
        // [2, 3] proves: per-day COUNT (2 then 3), ORDER BY run_date ascending
        // (day-2 bucket before day-1 bucket), and the 7-day window (the
        // 8-days-ago row produced no third bucket).
        if run.activity == vec![2usize, 3usize] {
            Ok(())
        } else {
            Err(format!(
                "expected activity buckets [2, 3] (day-2 then day-1, 8-days-ago row dropped), got {:?}",
                run.activity
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
    running_jobs: Option<String>,
    active_jobs: Option<String>,
    total_jobs: Option<String>,
}

async fn run_metrics_terminal_mix() -> Result<Outcome<MetricsRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-metrics-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Seed: 2 Pending, 1 Running, 1 Queued (both active, non-terminal),
    // 1 Done, 1 Failed (terminal), 1 Killed. The small run_at offsets put
    // run_at within the past few seconds so window-scoped metrics also catch
    // them. ACTIVE_JOBS = Pending + Queued + Running = 4; TOTAL_JOBS = 7.
    let _ = insert_job(pool.clone(), queue.clone(), "Pending", 1, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), queue.clone(), "Pending", 2, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), queue.clone(), "Running", 5, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), queue.clone(), "Queued", 5, None, 0, 3).await?;
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
        running_jobs: by_title("RUNNING_JOBS"),
        active_jobs: by_title("ACTIVE_JOBS"),
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
            "RUNNING_JOBS" => &run.running_jobs,
            "ACTIVE_JOBS" => &run.active_jobs,
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
// metrics_for_queue scope: the ONLY thing distinguishing `fetch_by_queue`
// from `global()` is the `WHERE job_type = $1` fragment in `build_metrics_sql`
// (src/queries/admin.rs). No scenario seeds a SECOND queue, so dropping that
// WHERE from `metrics_for_queue` would still pass `run_metrics_terminal_mix`
// on a clean DB (its expected TOTAL_JOBS=7 is unaffected by a queue the test
// never seeds). Here we seed the target queue AND a decoy queue, then assert
// `fetch_by_queue()` counts only the target — a positive/negative pair the
// existing single-queue metrics scenario can't express.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct ScopedMetricsRun {
    total_jobs: Option<String>,
    pending_jobs: Option<String>,
    done_jobs: Option<String>,
}

async fn run_metrics_scoped_excludes_other_queue() -> Result<Outcome<ScopedMetricsRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-admin-scope-mine-{}", Ulid::new());
    let other = format!("apalis-spec-admin-scope-other-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool.clone(), other.clone()).await?;

    // Target queue: 2 Pending + 1 Done => TOTAL_JOBS=3, PENDING_JOBS=2,
    // DONE_JOBS=1.
    let _ = insert_job(pool.clone(), queue.clone(), "Pending", 1, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), queue.clone(), "Pending", 2, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), queue.clone(), "Done", 5, Some(1), 1, 3).await?;
    // Decoy queue: rows that MUST NOT be counted. If the `WHERE job_type = $1`
    // scope regressed, these would inflate TOTAL_JOBS to 8 and DONE_JOBS to 3.
    let _ = insert_job(pool.clone(), other.clone(), "Pending", 1, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), other.clone(), "Pending", 2, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), other.clone(), "Done", 5, Some(1), 1, 3).await?;
    let _ = insert_job(pool.clone(), other.clone(), "Done", 6, Some(2), 1, 3).await?;
    let _ = insert_job(pool.clone(), other.clone(), "Failed", 5, Some(1), 3, 3).await?;

    let config = Config::new(&queue);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let stats = storage.fetch_by_queue().await.map_err(|e| e.to_string())?;

    let by_title = |title: &str| {
        stats
            .iter()
            .find(|s| s.title == title)
            .map(|s| s.value.clone())
    };
    let run = ScopedMetricsRun {
        total_jobs: by_title("TOTAL_JOBS"),
        pending_jobs: by_title("PENDING_JOBS"),
        done_jobs: by_title("DONE_JOBS"),
    };

    cleanup_queue(pool.clone(), queue).await?;
    cleanup_queue(pool, other).await?;
    Ok(Outcome::Completed(run))
}

fn scoped_metric_counts_only_target_queue(
    title: &'static str,
    expected: f64,
) -> impl Fn(&Result<Outcome<ScopedMetricsRun>, String>) -> AssertionResult {
    observe::<ScopedMetricsRun, _>(title, move |run| {
        let value_opt = match title {
            "TOTAL_JOBS" => &run.total_jobs,
            "PENDING_JOBS" => &run.pending_jobs,
            "DONE_JOBS" => &run.done_jobs,
            other => return Err(format!("unsupported scoped metric title: {other}")),
        };
        let Some(value) = value_opt else {
            return Err(format!("expected {title} present in scoped metrics"));
        };
        let parsed: f64 = value
            .parse()
            .map_err(|_| format!("expected {title} to parse, got {value:?}"))?;
        if (parsed - expected).abs() < 0.5 {
            Ok(())
        } else {
            Err(format!(
                "expected scoped {title} == {expected} (decoy queue excluded), got {parsed}"
            ))
        }
    })
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
// metrics_global aggregation: the defining property of `global()` vs
// `fetch_by_queue()` is that it has NO `WHERE job_type` scope, so its counts
// sum across EVERY queue (src/queries/admin.rs::build_metrics_sql, scope="").
// `run_metrics_global` only pins static/present metrics, so a regression that
// accidentally scoped the global query to a single queue would still pass it.
// Here we snapshot global TOTAL_JOBS/DONE_JOBS, seed rows across TWO distinct
// queues, and assert the deltas equal the cross-queue sum — a scoped query
// could never move the global total by both queues' rows. Deltas (not absolute
// values) keep this robust against other rows on a shared DB.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct GlobalAggregationRun {
    total_delta: f64,
    done_delta: f64,
    expected_total_delta: f64,
    expected_done_delta: f64,
}

async fn run_metrics_global_aggregates_across_queues()
-> Result<Outcome<GlobalAggregationRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue_a = format!("apalis-spec-admin-global-agg-a-{}", Ulid::new());
    let queue_b = format!("apalis-spec-admin-global-agg-b-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue_a.clone()).await?;
    cleanup_queue(pool.clone(), queue_b.clone()).await?;

    let config = Config::new(&queue_a);
    let storage = PostgresStorage::<String>::new_with_config(&pool, &config);

    let global_metric = |stats: &[apalis_core::backend::Statistic], title: &str| -> f64 {
        stats
            .iter()
            .find(|s| s.title == title)
            .and_then(|s| s.value.parse::<f64>().ok())
            .unwrap_or(0.0)
    };

    let before = storage.global().await.map_err(|e| e.to_string())?;
    let total_before = global_metric(&before, "TOTAL_JOBS");
    let done_before = global_metric(&before, "DONE_JOBS");

    // Queue A: 2 Pending + 1 Done. Queue B: 1 Pending + 2 Done.
    // Across both queues: +6 total, +3 done. A single-queue-scoped query could
    // never account for all six rows in the global total.
    let _ = insert_job(pool.clone(), queue_a.clone(), "Pending", 1, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), queue_a.clone(), "Pending", 2, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), queue_a.clone(), "Done", 5, Some(1), 1, 3).await?;
    let _ = insert_job(pool.clone(), queue_b.clone(), "Pending", 1, None, 0, 3).await?;
    let _ = insert_job(pool.clone(), queue_b.clone(), "Done", 5, Some(1), 1, 3).await?;
    let _ = insert_job(pool.clone(), queue_b.clone(), "Done", 6, Some(2), 1, 3).await?;

    let after = storage.global().await.map_err(|e| e.to_string())?;
    let total_after = global_metric(&after, "TOTAL_JOBS");
    let done_after = global_metric(&after, "DONE_JOBS");

    cleanup_queue(pool.clone(), queue_a).await?;
    cleanup_queue(pool, queue_b).await?;
    Ok(Outcome::Completed(GlobalAggregationRun {
        total_delta: total_after - total_before,
        done_delta: done_after - done_before,
        expected_total_delta: 6.0,
        expected_done_delta: 3.0,
    }))
}

fn global_total_grows_by_both_queues()
-> impl Fn(&Result<Outcome<GlobalAggregationRun>, String>) -> AssertionResult {
    observe::<GlobalAggregationRun, _>("global TOTAL_JOBS delta", |run| {
        if (run.total_delta - run.expected_total_delta).abs() < 0.5 {
            Ok(())
        } else {
            Err(format!(
                "expected global TOTAL_JOBS to grow by {} across both queues, grew by {}",
                run.expected_total_delta, run.total_delta
            ))
        }
    })
}

fn global_done_grows_by_both_queues()
-> impl Fn(&Result<Outcome<GlobalAggregationRun>, String>) -> AssertionResult {
    observe::<GlobalAggregationRun, _>("global DONE_JOBS delta", |run| {
        if (run.done_delta - run.expected_done_delta).abs() < 0.5 {
            Ok(())
        } else {
            Err(format!(
                "expected global DONE_JOBS to grow by {} across both queues, grew by {}",
                run.expected_done_delta, run.done_delta
            ))
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

        when filtering_by_queued_returns_only_queued_rows {
            let setup = StatusFilterSetup { filter_status: Status::Queued };
            to includes_the_queued_row { includes_expected_status_row() }
            to excludes_the_other_five_statuses { excludes_other_status_rows() }
        }

        when filtering_by_running_returns_only_running_rows {
            let setup = StatusFilterSetup { filter_status: Status::Running };
            to includes_the_running_row { includes_expected_status_row() }
            to excludes_the_other_five_statuses { excludes_other_status_rows() }
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

    expect(run_order_by_done_at().await) {
        when two_terminal_rows_differ_in_done_at {
            to ranks_the_later_done_at_row_first { later_done_at_listed_first() }
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

    // ----- list_all_tasks page=0 validation -------------------------------
    expect(run_list_all_page_zero().await) {
        when the_global_listing_is_requested_with_page_zero {
            to surfaces_invalid_argument_via_filter_offset_i32 {
                list_all_page_zero_rejected_as_invalid_argument()
            }
        }
    }

    // ----- list_all_tasks non-zero OFFSET + global ORDER BY ---------------
    expect(run_list_all_offset_slice().await) {
        when three_done_rows_are_the_newest_globally {
            to lists_the_newest_done_at_row_first_at_offset_zero {
                list_all_orders_newest_done_at_first()
            }
            to carves_the_second_row_at_offset_one {
                list_all_offset_one_skips_the_first_row()
            }
            to carves_the_third_row_at_offset_two {
                list_all_offset_two_skips_the_first_two_rows()
            }
            to limits_each_size_one_page_to_a_single_row {
                list_all_limits_each_page_to_one_row()
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

    // ----- list_queues stats CTE surfaces per-queue counts ----------------
    expect(run_queue_stats_for_jobs_backed_queue().await) {
        when a_queue_has_one_pending_job {
            to surfaces_pending_and_total_titles_from_the_queue_stats_cte {
                queue_stats_carry_pending_and_total_titles()
            }
            to reports_one_pending_job_in_its_stats { queue_stats_report_one_pending_job() }
        }
    }

    // ----- list_queues daily_activity CTE ---------------------------------
    expect(run_queue_activity_daily_buckets().await) {
        when jobs_land_on_two_days_inside_the_window_and_one_outside {
            to buckets_each_days_count_ordered_by_run_date {
                queue_activity_buckets_daily_counts_in_run_date_order()
            }
        }
    }

    // ----- metrics_for_queue scope excludes other queues ------------------
    expect(run_metrics_scoped_excludes_other_queue().await) {
        when another_queue_holds_rows_that_must_not_be_counted {
            to counts_only_the_target_queues_total {
                scoped_metric_counts_only_target_queue("TOTAL_JOBS", 3.0)
            }
            to counts_only_the_target_queues_pending {
                scoped_metric_counts_only_target_queue("PENDING_JOBS", 2.0)
            }
            to counts_only_the_target_queues_done {
                scoped_metric_counts_only_target_queue("DONE_JOBS", 1.0)
            }
        }
    }

    // ----- metrics_for_queue counts ---------------------------------------
    expect(run_metrics_terminal_mix().await) {
        when a_queue_mixes_active_pending_running_queued_and_terminal_rows {
            to reports_two_pending_jobs { metric_value_is("PENDING_JOBS", 2.0) }
            to reports_one_running_job { metric_value_is("RUNNING_JOBS", 1.0) }
            to reports_active_jobs_as_pending_plus_running_plus_queued {
                metric_value_is("ACTIVE_JOBS", 4.0)
            }
            to reports_one_done_job { metric_value_is("DONE_JOBS", 1.0) }
            to reports_one_failed_job { metric_value_is("FAILED_JOBS", 1.0) }
            to reports_one_killed_job { metric_value_is("KILLED_JOBS", 1.0) }
            to reports_total_of_seven_jobs { metric_value_is("TOTAL_JOBS", 7.0) }
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

    // ----- metrics_global aggregates across queues ------------------------
    expect(run_metrics_global_aggregates_across_queues().await) {
        when rows_are_seeded_across_two_distinct_queues {
            to grows_the_global_total_by_both_queues_rows {
                global_total_grows_by_both_queues()
            }
            to grows_the_global_done_count_by_both_queues_rows {
                global_done_grows_by_both_queues()
            }
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
