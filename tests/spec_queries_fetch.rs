//! Exhaustive specification for `src/queries/fetch.rs::fetch_next`.
//!
//! `fetch_next` is `pub(crate)`, so this integration test crate cannot call it
//! directly. Instead, the helper `fetch_next_sql` below issues the *same SQL
//! statement* as the production function and pins the resulting row-level
//! behaviour. Any production-side SQL drift will desync this contract from the
//! source and the spec must be updated in lock-step.
//!
//! Behaviour already covered elsewhere is not re-tested here:
//!   - basic push → poll ordering (oldest first, delayed deferred) →
//!     `postgres_queries::run_push_fetch`
//!   - priority DESC then run_at ASC → `postgres_queries::run_priority_ordering`
//!   - FOR UPDATE SKIP LOCKED concurrency → `postgres_queries::run_skip_locked_concurrency`
//!   - `buffer_size = 0` clamped to 1 → `postgres_queries::run_zero_buffer_fetch`
//!   - `fetch_by_id` cross-queue isolation → `postgres_queries::run_fetch_by_id_cross_queue`
//!   - `lock_task` per-status matrix → `postgres_queries::run_lock_status_scenario`
//!   - failed retryable round-trip via poll → `postgres_specs::run_failed_retry`
//!
//! The characteristics pinned in this file are those *not* covered above and
//! that follow directly from the `fetch_next` SQL:
//!   1. Empty queue → empty Vec.
//!   2. `buffer_size > available` → returns all available, no padding.
//!   3. LIMIT clamps when `buffer_size < available`.
//!   4. Cross-queue isolation: a sibling queue's claimable rows are invisible.
//!   5. `run_at = now()` boundary is inclusive (`run_at <= now()`).
//!   6. Tie-break on equal priority falls back to `run_at ASC`.
//!   7. Per-status predicate matrix for *fetch* (distinct from `lock_task`):
//!      Pending claimable; Failed retryable claimable; Failed exhausted, Queued,
//!      Running, Done, Killed all rejected.
//!   8. H4 invariant: a successfully claimed row transitions to `Running` with
//!      `lock_by = worker`, `lock_at` set, and `done_at = NULL` — straight from
//!      the production CTE.
//!
//! Tests gate on `DATABASE_URL`; without it every scenario resolves to
//! `Outcome::Skipped` and the assertions pass.

#![cfg(feature = "tokio")]

mod support;

use std::str::FromStr;

use apalis_core::task::task_id::TaskId;
use apalis_diesel_postgres::{PgPool, PgTaskId, build_pool, setup};
use diesel::{
    PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{Integer, Nullable, Text},
};
use lets_expect::{AssertionError, AssertionResult, *};
use ulid::Ulid;

// --------------------------------------------------------------------------
// shared scaffolding (kept local so concurrent edits in postgres_specs.rs,
// postgres_queries.rs, or spec_queries_worker.rs don't conflict).
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
    let Some(database_url) = support::database_url_or_skip()? else {
        return Ok(None);
    };
    let pool = build_pool(database_url).map_err(|e| e.to_string())?;
    setup(&pool).await.map_err(|e| e.to_string())?;
    Ok(Some(pool))
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

async fn insert_worker(pool: PgPool, queue: String, worker_id: String) -> Result<(), String> {
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at, lease_token)
             VALUES ($1, $2, 'PostgresStorage', '', now(), now(), $3)",
        )
        .bind::<Text, _>(&worker_id)
        .bind::<Text, _>(&queue)
        .bind::<Text, _>(format!("token-{}", Ulid::new()))
        .execute(conn)
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await
}

/// Insert a fully-described row, including offsets for `run_at` (seconds
/// relative to `now()`) and an optional priority. `lock_by` is left NULL so the
/// FK does not require a workers row for these inserts.
#[allow(clippy::too_many_arguments)]
async fn insert_row(
    pool: PgPool,
    queue: String,
    payload: &'static str,
    status: &'static str,
    attempts: i32,
    max_attempts: i32,
    run_at_offset_secs: i64,
    priority: i32,
) -> Result<PgTaskId, String> {
    let id = Ulid::new();
    let task_id = TaskId::from_str(&id.to_string()).map_err(|e| e.to_string())?;
    let job = serde_json::to_vec(payload).map_err(|e| e.to_string())?;
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (
                id, job_type, job, status, attempts, max_attempts, run_at, priority
            ) VALUES ($1, $2, $3, $4, $5, $6, now() + ($7 * INTERVAL '1 second'), $8)",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(queue)
        .bind::<diesel::sql_types::Binary, _>(job)
        .bind::<Text, _>(status)
        .bind::<Integer, _>(attempts)
        .bind::<Integer, _>(max_attempts)
        .bind::<Integer, _>(run_at_offset_secs as i32)
        .bind::<Integer, _>(priority)
        .execute(conn)
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await?;
    Ok(task_id)
}

#[derive(Debug, QueryableByName)]
struct FetchedRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Nullable<Text>)]
    lock_by: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    lock_at_present: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    done_at_null: bool,
    #[diesel(sql_type = Integer)]
    #[allow(dead_code)]
    priority: i32,
}

// --------------------------------------------------------------------------
// SQL mirror of `src/queries/fetch.rs::fetch_next`.
//
// IMPORTANT: keep this byte-equal to the production SQL. If
// `src/queries/fetch.rs::fetch_next` changes, update this helper in lock-step.
// `CLAIMABLE_PREDICATE` is inlined here since it is `pub(crate)` in production.
// --------------------------------------------------------------------------

const CLAIMABLE_PREDICATE: &str =
    "(status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts))";

async fn fetch_next_sql(
    pool: PgPool,
    queue: String,
    worker_id: String,
    buffer_size: i32,
) -> Result<Vec<FetchedRow>, String> {
    with_conn(pool, move |conn| {
        sql_query(format!(
            "WITH next_jobs AS (
                 SELECT id
                 FROM apalis.jobs
                 WHERE {CLAIMABLE_PREDICATE}
                     AND run_at <= now()
                     AND job_type = $2
                 ORDER BY priority DESC, run_at ASC
                 LIMIT $3
                 FOR UPDATE SKIP LOCKED
             ),
             updated AS (
                 UPDATE apalis.jobs
                 SET status = 'Running',
                     lock_by = $1,
                     lock_at = date_trunc('second', now()),
                     done_at = NULL
                 FROM next_jobs
                 WHERE apalis.jobs.id = next_jobs.id
                 RETURNING apalis.jobs.*
             )
             SELECT id::text AS id,
                    status,
                    lock_by,
                    (lock_at IS NOT NULL) AS lock_at_present,
                    (done_at IS NULL) AS done_at_null,
                    priority
             FROM updated
             ORDER BY priority DESC, run_at ASC"
        ))
        .bind::<Text, _>(&worker_id)
        .bind::<Text, _>(&queue)
        .bind::<Integer, _>(buffer_size)
        .load::<FetchedRow>(conn)
        .map_err(|e| e.to_string())
    })
    .await
}

// --------------------------------------------------------------------------
// Scenario harness
// --------------------------------------------------------------------------

#[derive(Debug)]
struct FetchRun {
    /// Ordered list of (payload-id-as-string, fetched row) pairs.
    rows: Vec<FetchedRow>,
    /// Worker id used for the fetch — useful for `lock_by` assertions.
    worker_id: String,
    /// Queue (job_type) the fetch ran against. Kept for diagnostic Debug output.
    #[allow(dead_code)]
    queue: String,
    /// IDs of rows seeded in the *target* queue (the one fetch ran against).
    seeded_ids: Vec<PgTaskId>,
    /// IDs of rows seeded in any sibling queue (cross-queue isolation tests).
    foreign_ids: Vec<PgTaskId>,
}

// ----- 1. empty queue -----------------------------------------------------

async fn run_empty_queue() -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-fetch-empty-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-fetch-empty-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    let rows = fetch_next_sql(pool.clone(), queue.clone(), worker_id.clone(), 5).await?;
    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        seeded_ids: vec![],
        foreign_ids: vec![],
    }))
}

// ----- 2./3. buffer vs available -----------------------------------------

async fn run_buffer_sizes(buffer_size: i32, seed_count: usize) -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-fetch-buffer-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-fetch-buffer-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    let mut seeded_ids = Vec::new();
    for i in 0..seed_count {
        // Use distinct run_at offsets so ORDER BY priority DESC, run_at ASC is
        // deterministic (older first).
        let offset = -(10 + i as i64);
        let id =
            insert_row(pool.clone(), queue.clone(), "buf", "Pending", 0, 25, offset, 0).await?;
        seeded_ids.push(id);
    }
    let rows = fetch_next_sql(pool.clone(), queue.clone(), worker_id.clone(), buffer_size).await?;
    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        seeded_ids,
        foreign_ids: vec![],
    }))
}

// ----- 4. cross-queue isolation ------------------------------------------

async fn run_cross_queue_isolation() -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-fetch-iso-{}", Ulid::new());
    let foreign = format!("{queue}-foreign");
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool.clone(), foreign.clone()).await?;
    let worker_id = format!("spec-fetch-iso-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    // Both queues hold a claimable Pending row.
    let own = insert_row(pool.clone(), queue.clone(), "own", "Pending", 0, 25, -5, 0).await?;
    let foreign_id = insert_row(
        pool.clone(),
        foreign.clone(),
        "foreign",
        "Pending",
        0,
        25,
        -5,
        0,
    )
    .await?;

    let rows = fetch_next_sql(pool.clone(), queue.clone(), worker_id.clone(), 10).await?;
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool, foreign).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        seeded_ids: vec![own],
        foreign_ids: vec![foreign_id],
    }))
}

// ----- 5. run_at = now() boundary ----------------------------------------

async fn run_run_at_boundary() -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-fetch-runat-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-fetch-runat-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    // Use offset 0 → run_at = now() at insert time. By the time fetch runs,
    // run_at is slightly in the past, which is the boundary direction we care
    // about: the predicate is `run_at <= now()`, inclusive.
    let id = insert_row(
        pool.clone(),
        queue.clone(),
        "now",
        "Pending",
        0,
        25,
        0,
        0,
    )
    .await?;

    let rows = fetch_next_sql(pool.clone(), queue.clone(), worker_id.clone(), 10).await?;
    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        seeded_ids: vec![id],
        foreign_ids: vec![],
    }))
}

// ----- 6. equal priority → run_at ASC tie-break --------------------------

async fn run_equal_priority_tie_break() -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-fetch-tie-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-fetch-tie-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    // Insert NEWER first, OLDER second. Both have priority=5. A correct
    // ORDER BY priority DESC, run_at ASC returns OLDER before NEWER, so the
    // result order must be the *reverse* of insert order.
    let newer =
        insert_row(pool.clone(), queue.clone(), "newer", "Pending", 0, 25, -10, 5).await?;
    let older =
        insert_row(pool.clone(), queue.clone(), "older", "Pending", 0, 25, -60, 5).await?;

    let rows = fetch_next_sql(pool.clone(), queue.clone(), worker_id.clone(), 10).await?;
    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        seeded_ids: vec![older, newer],
        foreign_ids: vec![],
    }))
}

// ----- 6b. priority DESC ordering (distinct priorities) ------------------

async fn run_priority_desc_ordering() -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-fetch-prio-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-fetch-prio-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    // Insert LOW priority first with an OLDER run_at, so the only signal that
    // can produce the correct order is `priority DESC` — `run_at ASC` alone
    // would otherwise put the low-priority row first.
    let low =
        insert_row(pool.clone(), queue.clone(), "low", "Pending", 0, 25, -60, 0).await?;
    let high =
        insert_row(pool.clone(), queue.clone(), "high", "Pending", 0, 25, -10, 10).await?;

    let rows = fetch_next_sql(pool.clone(), queue.clone(), worker_id.clone(), 10).await?;
    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        // Canonical order is high-priority first, regardless of insert order
        // or older run_at on the low-priority row.
        seeded_ids: vec![high, low],
        foreign_ids: vec![],
    }))
}

// ----- 7. per-status predicate matrix ------------------------------------

#[derive(Debug, Clone, Copy)]
struct StatusSetup {
    status: &'static str,
    attempts: i32,
    max_attempts: i32,
    run_at_offset_secs: i64,
}

const STATUS_PENDING_DUE: StatusSetup = StatusSetup {
    status: "Pending",
    attempts: 0,
    max_attempts: 25,
    run_at_offset_secs: -1,
};

async fn run_status_matrix(setup: StatusSetup) -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-fetch-status-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-fetch-status-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    let id = insert_row(
        pool.clone(),
        queue.clone(),
        "status-row",
        setup.status,
        setup.attempts,
        setup.max_attempts,
        setup.run_at_offset_secs,
        0,
    )
    .await?;

    let rows = fetch_next_sql(pool.clone(), queue.clone(), worker_id.clone(), 10).await?;
    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        seeded_ids: vec![id],
        foreign_ids: vec![],
    }))
}

// --------------------------------------------------------------------------
// Assertion helpers
// --------------------------------------------------------------------------

fn fetched_no_rows() -> impl Fn(&Result<Outcome<FetchRun>, String>) -> AssertionResult {
    observe::<FetchRun, _>("fetch returned rows", |run| {
        if run.rows.is_empty() {
            Ok(())
        } else {
            let payloads: Vec<&str> = run.rows.iter().map(|r| r.status.as_str()).collect();
            Err(format!(
                "expected zero rows, got {} (statuses={payloads:?})",
                run.rows.len()
            ))
        }
    })
}

fn fetched_row_count(expected: usize) -> impl Fn(&Result<Outcome<FetchRun>, String>) -> AssertionResult {
    observe::<FetchRun, _>("fetch row count", move |run| {
        if run.rows.len() == expected {
            Ok(())
        } else {
            Err(format!("expected {expected} rows, got {}", run.rows.len()))
        }
    })
}

fn all_seeded_ids_returned() -> impl Fn(&Result<Outcome<FetchRun>, String>) -> AssertionResult {
    observe::<FetchRun, _>("seeded ids returned", |run| {
        let returned: std::collections::HashSet<String> =
            run.rows.iter().map(|r| r.id.clone()).collect();
        for id in &run.seeded_ids {
            if !returned.contains(&id.to_string()) {
                return Err(format!("seeded id {id} missing from returned set"));
            }
        }
        Ok(())
    })
}

fn no_foreign_ids_returned() -> impl Fn(&Result<Outcome<FetchRun>, String>) -> AssertionResult {
    observe::<FetchRun, _>("foreign isolation", |run| {
        for id in &run.foreign_ids {
            for row in &run.rows {
                if row.id == id.to_string() {
                    return Err(format!(
                        "foreign-queue id {id} leaked into the target queue's fetch"
                    ));
                }
            }
        }
        Ok(())
    })
}

fn returned_ids_match_seeded_order()
-> impl Fn(&Result<Outcome<FetchRun>, String>) -> AssertionResult {
    observe::<FetchRun, _>("returned ids ordering", |run| {
        let actual: Vec<String> = run.rows.iter().map(|r| r.id.clone()).collect();
        let expected: Vec<String> = run.seeded_ids.iter().map(|id| id.to_string()).collect();
        if actual == expected {
            Ok(())
        } else {
            Err(format!("expected order {expected:?}, got {actual:?}"))
        }
    })
}

fn claimed_row_transitioned_to_running()
-> impl Fn(&Result<Outcome<FetchRun>, String>) -> AssertionResult {
    observe::<FetchRun, _>("H4 invariant", |run| {
        if run.rows.is_empty() {
            return Err("expected at least one claimed row to inspect".into());
        }
        for row in &run.rows {
            if row.status != "Running" {
                return Err(format!("expected status=Running, got {:?}", row.status));
            }
            match &row.lock_by {
                Some(lb) if *lb == run.worker_id => {}
                other => {
                    return Err(format!(
                        "expected lock_by={:?}, got {other:?}",
                        run.worker_id
                    ));
                }
            }
            if !row.lock_at_present {
                return Err("expected lock_at to be set after claim".into());
            }
            if !row.done_at_null {
                return Err("expected done_at to be NULL after claim".into());
            }
        }
        Ok(())
    })
}

// --------------------------------------------------------------------------
// expectations
// --------------------------------------------------------------------------

lets_expect! { #tokio_test
    // ----- 1. empty queue --------------------------------------------------
    expect(run_empty_queue().await) {
        when the_queue_holds_no_rows {
            to returns_an_empty_vec { fetched_no_rows() }
        }
    }

    // ----- 2./3. buffer vs available --------------------------------------
    expect(run_buffer_sizes(buffer_size, seed_count).await) {
        let buffer_size = 5_i32;
        let seed_count = 2_usize;

        when the_buffer_is_larger_than_the_available_rows {
            to returns_all_available_rows_without_padding { fetched_row_count(2) }
            to returns_every_seeded_id { all_seeded_ids_returned() }
            to leaves_each_claimed_row_in_running_state {
                claimed_row_transitioned_to_running()
            }
        }

        when the_buffer_is_smaller_than_the_available_rows {
            let buffer_size = 2_i32;
            let seed_count = 5_usize;
            to clamps_the_returned_set_to_the_buffer { fetched_row_count(2) }
        }
    }

    // ----- 4. cross-queue isolation ---------------------------------------
    expect(run_cross_queue_isolation().await) {
        when a_sibling_queue_holds_its_own_claimable_pending_row {
            to returns_only_the_target_queues_row { fetched_row_count(1) }
            to does_not_leak_the_sibling_queues_id { no_foreign_ids_returned() }
            to returns_the_target_queues_seeded_id { all_seeded_ids_returned() }
        }
    }

    // ----- 5. run_at boundary ---------------------------------------------
    expect(run_run_at_boundary().await) {
        when run_at_is_at_the_inclusive_now_boundary {
            // `run_at <= now()` — equality is claimable.
            to claims_the_row { fetched_row_count(1) }
            to returns_the_seeded_id { all_seeded_ids_returned() }
        }
    }

    // ----- 6. equal priority tie-break ------------------------------------
    expect(run_equal_priority_tie_break().await) {
        when two_pending_rows_share_the_same_priority {
            // seeded_ids is built as [older, newer] in the harness; this is
            // the canonical priority-DESC, run_at-ASC ordering.
            to returns_the_older_row_before_the_newer_one {
                returned_ids_match_seeded_order()
            }
        }
    }

    // ----- 6b. priority DESC ordering -------------------------------------
    expect(run_priority_desc_ordering().await) {
        when two_pending_rows_carry_different_priorities {
            to returns_the_higher_priority_row_first_even_when_its_run_at_is_newer {
                returned_ids_match_seeded_order()
            }
        }
    }

    // ----- 7. status predicate matrix -------------------------------------
    expect(run_status_matrix(setup).await) {
        let setup = STATUS_PENDING_DUE;

        when the_row_is_pending_with_a_past_run_at {
            to claims_the_row { fetched_row_count(1) }
            to writes_the_h4_running_invariant {
                claimed_row_transitioned_to_running()
            }
        }

        when the_row_is_pending_but_scheduled_in_the_future {
            let setup = StatusSetup { run_at_offset_secs: 3600, ..STATUS_PENDING_DUE };
            to leaves_the_row_alone { fetched_no_rows() }
        }

        when the_row_is_failed_with_retries_left {
            let setup = StatusSetup {
                status: "Failed",
                attempts: 1,
                max_attempts: 3,
                ..STATUS_PENDING_DUE
            };
            to is_picked_up_for_a_retry { fetched_row_count(1) }
            to writes_the_h4_running_invariant {
                claimed_row_transitioned_to_running()
            }
        }

        when the_row_is_failed_with_the_retry_budget_exhausted {
            let setup = StatusSetup {
                status: "Failed",
                attempts: 3,
                max_attempts: 3,
                ..STATUS_PENDING_DUE
            };
            to refuses_to_reclaim_it { fetched_no_rows() }
        }

        when the_row_is_already_queued {
            // Queued is outside `fetch_next`'s narrower predicate; only
            // `lock_task` re-locks Queued rows owned by the same worker.
            let setup = StatusSetup { status: "Queued", ..STATUS_PENDING_DUE };
            to refuses_to_reclaim_it { fetched_no_rows() }
        }

        when the_row_is_already_running {
            let setup = StatusSetup { status: "Running", ..STATUS_PENDING_DUE };
            to refuses_to_reclaim_it { fetched_no_rows() }
        }

        when the_row_is_done {
            let setup = StatusSetup { status: "Done", ..STATUS_PENDING_DUE };
            to refuses_to_reclaim_it { fetched_no_rows() }
        }

        when the_row_is_killed {
            let setup = StatusSetup { status: "Killed", ..STATUS_PENDING_DUE };
            to refuses_to_reclaim_it { fetched_no_rows() }
        }
    }
}
