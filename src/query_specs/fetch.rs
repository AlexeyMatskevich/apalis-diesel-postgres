//! Production-bound database specifications; SQL is used only for fixtures and observations.

#![cfg(feature = "tokio")]

use crate::test_support as support;

use support::{Outcome, observe, with_conn};

use std::str::FromStr;

use apalis_core::task::task_id::TaskId;
use apalis_diesel_postgres::{PgPool, PgTaskId};
use diesel::{
    QueryableByName, RunQueryDsl, sql_query,
    sql_types::{BigInt, Integer, Jsonb, Nullable, Text},
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
/// relative to `now()`) and a priority. Active rows belong to a registered
/// fixture worker; other states retain their unowned setup.
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
    let owner = if matches!(status, "Queued" | "Running") {
        let owner = format!("seeded-owner-{id}");
        insert_worker(pool.clone(), queue.clone(), owner.clone()).await?;
        Some(owner)
    } else {
        None
    };
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (
                id, job_type, job, status, attempts, max_attempts, run_at, priority, lock_by, lock_at
            ) VALUES ($1, $2, $3, $4, $5, $6, now() + ($7 * INTERVAL '1 second'), $8, $9,
                CASE WHEN $9 IS NOT NULL THEN date_trunc('second', clock_timestamp()) ELSE NULL END)",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(queue)
        .bind::<diesel::sql_types::Binary, _>(job)
        .bind::<Text, _>(status)
        .bind::<Integer, _>(attempts)
        .bind::<Integer, _>(max_attempts)
        .bind::<Integer, _>(run_at_offset_secs as i32)
        .bind::<Integer, _>(priority)
        .bind::<Nullable<Text>, _>(owner)
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
// Production query adapters: observe returned tasks without copying SQL.
async fn fetch_next_sql(
    pool: PgPool,
    queue: String,
    worker_id: String,
    buffer_size: i32,
) -> Result<Vec<FetchedRow>, String> {
    let worker = apalis_core::worker::context::WorkerContext::new::<()>(&worker_id);
    let config = crate::Config::new(&queue).set_buffer_size(buffer_size as usize);
    crate::queries::fetch_next(pool, config, worker, None)
        .await
        .map(|tasks| tasks.into_iter().map(observe_fetched).collect())
        .map_err(|error| error.to_string())
}
fn observe_fetched(task: crate::PgTask<crate::CompactType>) -> FetchedRow {
    FetchedRow {
        id: task.parts.task_id.unwrap().to_string(),
        status: task.parts.status.load().to_string(),
        lock_by: task.parts.ctx.lock_by().clone(),
        lock_at_present: task.parts.ctx.lock_at().is_some(),
        done_at_null: task.parts.ctx.done_at().is_none(),
        priority: task.parts.ctx.priority(),
    }
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

async fn run_buffer_sizes(
    buffer_size: i32,
    seed_count: usize,
) -> Result<Outcome<FetchRun>, String> {
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
        let id = insert_row(
            pool.clone(),
            queue.clone(),
            "buf",
            "Pending",
            0,
            25,
            offset,
            0,
        )
        .await?;
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
    let id = insert_row(pool.clone(), queue.clone(), "now", "Pending", 0, 25, 0, 0).await?;

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
    let newer = insert_row(
        pool.clone(),
        queue.clone(),
        "newer",
        "Pending",
        0,
        25,
        -10,
        5,
    )
    .await?;
    let older = insert_row(
        pool.clone(),
        queue.clone(),
        "older",
        "Pending",
        0,
        25,
        -60,
        5,
    )
    .await?;

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

// ----- 7. per-status predicate matrix ------------------------------------

#[derive(Debug, Clone, Copy)]
struct StatusSetup {
    status: &'static str,
    attempts: i32,
    max_attempts: i32,
    run_at_offset_secs: i64,
}

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

fn fetched_row_count(
    expected: usize,
) -> impl Fn(&Result<Outcome<FetchRun>, String>) -> AssertionResult {
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
    expect(run_empty_queue().await) as empty_queue {
        when the_queue_holds_no_rows {
            to returns_an_empty_vec { fetched_no_rows() }
        }
    }

    // ----- 2./3. buffer vs available --------------------------------------
    expect(run_buffer_sizes(buffer_size, seed_count).await) as buffer_sizes {
        let buffer_size = 5_i32;
        let seed_count = 2_usize;

        when the_buffer_is_larger_than_the_available_rows {
            to returns_all_available_rows_without_padding {
                fetched_row_count(2),
                all_seeded_ids_returned(),
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
    expect(run_cross_queue_isolation().await) as cross_queue_isolation {
        when a_sibling_queue_holds_its_own_claimable_pending_row {
            to returns_only_the_target_queues_row {
                fetched_row_count(1),
                no_foreign_ids_returned(),
                all_seeded_ids_returned()
            }
        }
    }

    // ----- 5. run_at boundary ---------------------------------------------
    expect(run_run_at_boundary().await) as run_at_boundary {
        when run_at_is_already_past_due {
            // run_at is slightly in the past by fetch time — the <= now() (past-due) direction; exact equality is not pinned (see module docstring lines 30-32).
            to claims_the_row {
                fetched_row_count(1),
                all_seeded_ids_returned()
            }
        }
    }

    // ----- 6. equal priority tie-break ------------------------------------
    expect(run_equal_priority_tie_break().await) as equal_priority_tie_break {
        when two_pending_rows_share_the_same_priority {
            // seeded_ids is built as [older, newer] in the harness; this is
            // the canonical priority-DESC, run_at-ASC ordering.
            to returns_the_older_row_before_the_newer_one {
                returned_ids_match_seeded_order()
            }
        }
    }

    // ----- 7. status predicate matrix -------------------------------------
    expect(run_status_matrix(setup).await) as polled_task_eligibility {
        let status="Pending";
        let attempts=0;
        let run_at_offset_secs=-10;
        let setup=StatusSetup{status,attempts,max_attempts:3,run_at_offset_secs};
        to records_the_running_claim { fetched_row_count(1), claimed_row_transitioned_to_running() }
        when exactly_one_attempt_remains {
            let attempts=2;
            to records_the_last_permitted_claim { fetched_row_count(1), claimed_row_transitioned_to_running() }
        }
        when the_attempt_budget_is_exhausted {
            let attempts=3;
            to refuses_a_new_execution { fetched_no_rows() }
        }
        when the_task_is_scheduled_for_the_future {
            let run_at_offset_secs=3600;
            to leaves_the_task_scheduled { fetched_no_rows() }
        }
        when the_task_failed_previously {
            let status="Failed";
            to records_the_running_claim { fetched_row_count(1), claimed_row_transitioned_to_running() }
            when exactly_one_attempt_remains {
                let attempts=2;
                to records_the_last_permitted_claim { fetched_row_count(1), claimed_row_transitioned_to_running() }
            }
            when the_attempt_budget_is_exhausted {
                let attempts=3;
                to refuses_a_new_execution { fetched_no_rows() }
            }
            when the_task_is_scheduled_for_the_future {
                let run_at_offset_secs=3600;
                to leaves_the_task_scheduled { fetched_no_rows() }
            }
        }
        when the_task_is_queued {
            let status="Queued";
            to refuses_a_new_execution { fetched_no_rows() }
        }
        when the_task_is_running {
            let status="Running";
            to refuses_a_new_execution { fetched_no_rows() }
        }
        when the_task_is_done {
            let status="Done";
            to refuses_a_new_execution { fetched_no_rows() }
        }
        when the_task_is_killed {
            let status="Killed";
            to refuses_a_new_execution { fetched_no_rows() }
        }
    }
}

// ==========================================================================
// queue_by_id — `src/queries/fetch.rs::queue_by_id` (the NOTIFY-path claim).
//
// queue_by_id reuses fetch_next's `CLAIMABLE_PREDICATE` + `run_at <= now()` +
// `job_type = $2`, but adds `AND id = ANY($3)`: a NOTIFY wakeup claims only the
// *listed* ids. The fetch_next spec above already pins the shared predicate,
// ordering, and H4 invariant; this section pins what is DISTINCT to queue_by_id
// — the id-membership filter, the queue scope at the claim layer, ordering
// across listed ids, and partial claims — plus a focused re-check that the
// eligibility predicate is wired into this *separate* SQL string, not only into
// fetch_next. queue_by_id had zero direct coverage before this block.
//
// Mirror SQL: the CTE is kept byte-equal to fetch.rs:88-110; only the final
// SELECT is reshaped into `FetchedRow`, exactly as `fetch_next_sql` does.
// --------------------------------------------------------------------------

async fn queue_by_id_sql(
    pool: PgPool,
    queue: String,
    worker_id: String,
    ids: Vec<String>,
) -> Result<Vec<FetchedRow>, String> {
    crate::queries::fetch::queue_by_id(pool, queue, ids, worker_id, None)
        .await
        .map(|tasks| tasks.into_iter().map(observe_fetched).collect())
        .map_err(|error| error.to_string())
}

async fn run_queue_by_id_eligibility(setup: StatusSetup) -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-qbi-status-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-qbi-status-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    let id = insert_row(
        pool.clone(),
        queue.clone(),
        "qbi-status-row",
        setup.status,
        setup.attempts,
        setup.max_attempts,
        setup.run_at_offset_secs,
        0,
    )
    .await?;

    let rows = queue_by_id_sql(
        pool.clone(),
        queue.clone(),
        worker_id.clone(),
        vec![id.to_string()],
    )
    .await?;
    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        seeded_ids: vec![id],
        foreign_ids: vec![],
    }))
}

async fn run_queue_by_id_skips_unlisted() -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-qbi-unlisted-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-qbi-unlisted-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    // Both rows are equally claimable; only `listed` is passed in `ids`.
    let listed = insert_row(
        pool.clone(),
        queue.clone(),
        "qbi-listed",
        "Pending",
        0,
        25,
        -2,
        0,
    )
    .await?;
    let unlisted = insert_row(
        pool.clone(),
        queue.clone(),
        "qbi-unlisted",
        "Pending",
        0,
        25,
        -1,
        0,
    )
    .await?;

    let rows = queue_by_id_sql(
        pool.clone(),
        queue.clone(),
        worker_id.clone(),
        vec![listed.to_string()],
    )
    .await?;
    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        seeded_ids: vec![listed],
        // Eligible but absent from `ids`: the id-filter must exclude it, so it
        // must NOT appear in the claimed set.
        foreign_ids: vec![unlisted],
    }))
}

async fn run_queue_by_id_cross_queue() -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-qbi-home-{}", Ulid::new());
    let other_queue = format!("apalis-spec-qbi-other-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool.clone(), other_queue.clone()).await?;
    let worker_id = format!("spec-qbi-cross-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    // The row lives in `queue`; we request its id scoped to `other_queue`. The
    // `job_type = $2` filter must keep it invisible to the foreign-queue claim.
    let id = insert_row(
        pool.clone(),
        queue.clone(),
        "qbi-foreign",
        "Pending",
        0,
        25,
        -1,
        0,
    )
    .await?;
    let rows = queue_by_id_sql(
        pool.clone(),
        other_queue.clone(),
        worker_id.clone(),
        vec![id.to_string()],
    )
    .await?;
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool, other_queue).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        seeded_ids: vec![],
        foreign_ids: vec![id],
    }))
}

async fn run_queue_by_id_ordering() -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-qbi-order-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-qbi-order-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    // `low` has the older run_at but a lower priority; priority DESC must win,
    // so `high` is returned first even though `low` would sort first by run_at.
    let high = insert_row(
        pool.clone(),
        queue.clone(),
        "qbi-high",
        "Pending",
        0,
        25,
        -1,
        9,
    )
    .await?;
    let low = insert_row(
        pool.clone(),
        queue.clone(),
        "qbi-low",
        "Pending",
        0,
        25,
        -2,
        1,
    )
    .await?;

    // Request order deliberately reversed to prove SQL ordering, not list order.
    let rows = queue_by_id_sql(
        pool.clone(),
        queue.clone(),
        worker_id.clone(),
        vec![low.to_string(), high.to_string()],
    )
    .await?;
    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        seeded_ids: vec![high, low], // expected returned order: priority DESC
        foreign_ids: vec![],
    }))
}

async fn run_queue_by_id_partial_claim() -> Result<Outcome<FetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-qbi-partial-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-qbi-partial-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_id.clone()).await?;

    // Two claimable rows flank a terminal `Done` row; all three ids are listed.
    let first = insert_row(
        pool.clone(),
        queue.clone(),
        "qbi-first",
        "Pending",
        0,
        25,
        -3,
        5,
    )
    .await?;
    let done = insert_row(
        pool.clone(),
        queue.clone(),
        "qbi-done",
        "Done",
        0,
        25,
        -2,
        5,
    )
    .await?;
    let second = insert_row(
        pool.clone(),
        queue.clone(),
        "qbi-second",
        "Pending",
        0,
        25,
        -1,
        5,
    )
    .await?;

    let rows = queue_by_id_sql(
        pool.clone(),
        queue.clone(),
        worker_id.clone(),
        vec![first.to_string(), done.to_string(), second.to_string()],
    )
    .await?;
    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(FetchRun {
        rows,
        worker_id,
        queue,
        // Both claimable, equal priority -> run_at ASC orders first before second.
        seeded_ids: vec![first, second],
        foreign_ids: vec![done], // terminal -> excluded from the partial claim
    }))
}

lets_expect! { #tokio_test
    // ----- eligibility predicate is wired into the id-path ------------------
    expect(run_queue_by_id_eligibility(setup).await) as notified_task_eligibility {
        let status="Pending";
        let attempts=0;
        let run_at_offset_secs=-10;
        let setup=StatusSetup{status,attempts,max_attempts:3,run_at_offset_secs};
        to records_the_running_claim { fetched_row_count(1), claimed_row_transitioned_to_running() }
        when exactly_one_attempt_remains {
            let attempts=2;
            to records_the_last_permitted_claim { fetched_row_count(1), claimed_row_transitioned_to_running() }
        }
        when the_attempt_budget_is_exhausted {
            let attempts=3;
            to refuses_a_new_execution { fetched_no_rows() }
        }
        when the_task_is_scheduled_for_the_future {
            let run_at_offset_secs=3600;
            to leaves_the_task_scheduled { fetched_no_rows() }
        }
        when the_task_failed_previously {
            let status="Failed";
            to records_the_running_claim { fetched_row_count(1), claimed_row_transitioned_to_running() }
            when exactly_one_attempt_remains {
                let attempts=2;
                to records_the_last_permitted_claim { fetched_row_count(1), claimed_row_transitioned_to_running() }
            }
            when the_attempt_budget_is_exhausted {
                let attempts=3;
                to refuses_a_new_execution { fetched_no_rows() }
            }
            when the_task_is_scheduled_for_the_future {
                let run_at_offset_secs=3600;
                to leaves_the_task_scheduled { fetched_no_rows() }
            }
        }
        when the_task_is_queued {
            let status="Queued";
            to refuses_a_new_execution { fetched_no_rows() }
        }
        when the_task_is_running {
            let status="Running";
            to refuses_a_new_execution { fetched_no_rows() }
        }
        when the_task_is_done {
            let status="Done";
            to refuses_a_new_execution { fetched_no_rows() }
        }
        when the_task_is_killed {
            let status="Killed";
            to refuses_a_new_execution { fetched_no_rows() }
        }
    }

    // ----- the id-membership filter ($3) ------------------------------------
    expect(run_queue_by_id_skips_unlisted().await) as queue_by_id_skips_unlisted {
        when an_eligible_row_in_the_queue_is_absent_from_the_id_list {
            to claims_only_the_listed_id {
                fetched_row_count(1),
                all_seeded_ids_returned(),
                no_foreign_ids_returned()
            }
        }
    }

    // ----- queue scope at the claim layer -----------------------------------
    expect(run_queue_by_id_cross_queue().await) as queue_by_id_cross_queue {
        when a_listed_id_belongs_to_a_different_queue {
            to refuses_to_claim_across_the_queue_boundary { fetched_no_rows() }
        }
    }

    // ----- ordering across listed ids ---------------------------------------
    expect(run_queue_by_id_ordering().await) as queue_by_id_ordering {
        when several_listed_ids_carry_different_priorities {
            to returns_them_priority_desc_then_run_at_asc { returned_ids_match_seeded_order() }
        }
    }

    // ----- partial claim ----------------------------------------------------
    expect(run_queue_by_id_partial_claim().await) as queue_by_id_partial_claim {
        when the_id_list_mixes_claimable_and_terminal_rows {
            to claims_only_the_claimable_subset {
                fetched_row_count(2),
                all_seeded_ids_returned(),
                no_foreign_ids_returned()
            }
        }
    }
}

// --------------------------------------------------------------------------
// SQL mirror of `src/queries/fetch.rs::fail_undecodable_task`.
//
// IMPORTANT: keep this byte-equal to the production SQL. If
// `src/queries/fetch.rs::fail_undecodable_task` changes, update this helper in
// lock-step. The release must pin the caller's exact claim epoch (`lock_by` +
// `lock_at` + `attempts`), mirroring `ack_task`'s defense: a delayed release
// must not fire on a row that was acked, orphan-swept, or re-claimed since.
// --------------------------------------------------------------------------

const RELEASE_ERROR_TEXT: &str = "failed to decode task payload: spec probe";

/// The claim epoch the "caller" presents. Fixed in the recent past and whole
/// seconds, matching the production claim's `date_trunc('second', now())`.
fn claim_epoch_secs() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_secs();
    now as i64 - 10
}

async fn fail_undecodable_sql(
    pool: PgPool,
    task_id: String,
    worker_id: String,
    lock_at_epoch: i64,
    attempts: i32,
    error_json: serde_json::Value,
) -> Result<usize, String> {
    crate::queries::fail_undecodable_task(
        pool,
        PgTaskId::new(task_id.parse::<ulid::Ulid>().map_err(|e| e.to_string())?),
        worker_id,
        lock_at_epoch,
        attempts,
        error_json["Err"].as_str().unwrap().to_owned(),
        None,
    )
    .await
    .map_err(|e| e.to_string())
}

/// Insert a row in an arbitrary lock state. `max_attempts` is fixed at 25:
/// the retry-budget axis of the release is pruned here because it is covered
/// through the public API (`postgres_queries` decode_release specs).
async fn insert_lock_state_row(
    pool: PgPool,
    queue: String,
    status: &'static str,
    attempts: i32,
    lock_by: Option<String>,
    lock_at_epoch: Option<i64>,
    last_result: Option<serde_json::Value>,
) -> Result<PgTaskId, String> {
    let id = Ulid::new();
    let task_id = TaskId::from_str(&id.to_string()).map_err(|e| e.to_string())?;
    let job = serde_json::to_vec("undecodable-probe").map_err(|e| e.to_string())?;
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (
                id, job_type, job, status, attempts, max_attempts, run_at,
                lock_by, lock_at, last_result
            ) VALUES (
                $1, $2, $3, $4, $5, 25, now() - INTERVAL '1 second',
                $6, to_timestamp($7::double precision), $8
            )",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(queue)
        .bind::<diesel::sql_types::Binary, _>(job)
        .bind::<Text, _>(status)
        .bind::<Integer, _>(attempts)
        .bind::<Nullable<Text>, _>(lock_by)
        .bind::<Nullable<BigInt>, _>(lock_at_epoch)
        .bind::<Nullable<Jsonb>, _>(last_result)
        .execute(conn)
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await?;
    Ok(task_id)
}

#[derive(Debug, QueryableByName)]
struct ReleasedRow {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Integer)]
    attempts: i32,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    done_at_present: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    last_result_present: bool,
    #[diesel(sql_type = Nullable<Text>)]
    last_result_err: Option<String>,
}

async fn read_released_row(pool: PgPool, task_id: String) -> Result<ReleasedRow, String> {
    with_conn(pool, move |conn| {
        sql_query(
            "SELECT status,
                    attempts,
                    (done_at IS NOT NULL) AS done_at_present,
                    (last_result IS NOT NULL) AS last_result_present,
                    last_result->>'Err' AS last_result_err
             FROM apalis.jobs
             WHERE id = $1",
        )
        .bind::<Text, _>(&task_id)
        .load::<ReleasedRow>(conn)
        .map_err(|e| e.to_string())?
        .into_iter()
        .next()
        .ok_or_else(|| "released row not found".to_owned())
    })
    .await
}

#[derive(Debug)]
struct FailUndecodableRun {
    affected: usize,
    status: String,
    attempts: i32,
    done_at_present: bool,
    last_result_present: bool,
    last_result_err: Option<String>,
}

/// The caller always presents the same claim identity (worker A, the fixed
/// epoch, attempts = 0); each context varies the *stored* row instead — the
/// realistic state the row reaches when an ack, an orphan sweep, or a
/// re-claim raced the delayed release.
async fn run_fail_undecodable(setup: &'static str) -> Result<Outcome<FailUndecodableRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-fail-undecodable-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_a = format!("fail-undecodable-a-{queue}");
    let worker_b = format!("fail-undecodable-b-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker_a.clone()).await?;
    insert_worker(pool.clone(), queue.clone(), worker_b.clone()).await?;
    let epoch = claim_epoch_secs();

    let (status, attempts, lock_by, lock_at, last_result) = match setup {
        "live_claim" => ("Running", 0, Some(worker_a.clone()), Some(epoch), None),
        "acked_done" => (
            "Done",
            1,
            Some(worker_a.clone()),
            Some(epoch),
            Some(serde_json::json!({ "Ok": null })),
        ),
        "swept_pending" => ("Pending", 1, None, None, None),
        "reclaimed_by_other" => ("Running", 0, Some(worker_b.clone()), Some(epoch), None),
        "reclaimed_later_epoch" => ("Running", 0, Some(worker_a.clone()), Some(epoch + 5), None),
        "attempts_advanced" => ("Running", 1, Some(worker_a.clone()), Some(epoch), None),
        other => return Err(format!("unknown setup: {other}")),
    };
    let task_id = insert_lock_state_row(
        pool.clone(),
        queue.clone(),
        status,
        attempts,
        lock_by,
        lock_at,
        last_result,
    )
    .await?;

    let affected = fail_undecodable_sql(
        pool.clone(),
        task_id.to_string(),
        worker_a,
        epoch,
        0,
        serde_json::json!({ "Err": RELEASE_ERROR_TEXT }),
    )
    .await?;
    let row = read_released_row(pool.clone(), task_id.to_string()).await?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(FailUndecodableRun {
        affected,
        status: row.status,
        attempts: row.attempts,
        done_at_present: row.done_at_present,
        last_result_present: row.last_result_present,
        last_result_err: row.last_result_err,
    }))
}

// --------------------------------------------------------------------------
// fail_undecodable_task: the retry-budget CASE (`attempts::bigint + 1 >=
// max_attempts` → Killed) and the overflow-safe attempts arithmetic
// (`LEAST(attempts::bigint + 1, max_attempts)`).
//
// The `run_fail_undecodable` harness above pins the *epoch* axis with a fixed
// `max_attempts = 25`, so it can only ever reach the `Failed` (budget-left)
// arm and can never overflow. These specs vary `attempts`/`max_attempts` on a
// live claim so the CASE crosses into `Killed`, the `LEAST` clamp actually
// collapses `attempts + 1` back to `max_attempts`, and the `i32::MAX` boundary
// exercises the `::bigint` promotion (a bare `attempts + 1` would raise
// "integer out of range").
// --------------------------------------------------------------------------

/// Insert a live claim (`Running`, owned by `worker`, at `lock_at_epoch`) with a
/// caller-controlled `attempts`/`max_attempts` so the release CASE and the
/// `LEAST(...)` clamp can be pinned at their boundaries.
async fn insert_live_claim_with_budget(
    pool: PgPool,
    queue: String,
    worker: String,
    attempts: i32,
    max_attempts: i32,
    lock_at_epoch: i64,
) -> Result<PgTaskId, String> {
    let id = Ulid::new();
    let task_id = TaskId::from_str(&id.to_string()).map_err(|e| e.to_string())?;
    let job = serde_json::to_vec("undecodable-budget-probe").map_err(|e| e.to_string())?;
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (
                id, job_type, job, status, attempts, max_attempts, run_at,
                lock_by, lock_at
            ) VALUES (
                $1, $2, $3, 'Running', $4, $5, now() - INTERVAL '1 second',
                $6, to_timestamp($7::double precision)
            )",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(queue)
        .bind::<diesel::sql_types::Binary, _>(job)
        .bind::<Integer, _>(attempts)
        .bind::<Integer, _>(max_attempts)
        .bind::<Text, _>(worker)
        .bind::<BigInt, _>(lock_at_epoch)
        .execute(conn)
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await?;
    Ok(task_id)
}

#[derive(Debug, Clone, Copy)]
struct BudgetSetup {
    attempts: i32,
    max_attempts: i32,
}

async fn run_fail_undecodable_budget(
    setup: BudgetSetup,
) -> Result<Outcome<FailUndecodableRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-fail-budget-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker = format!("fail-budget-{queue}");
    insert_worker(pool.clone(), queue.clone(), worker.clone()).await?;
    let epoch = claim_epoch_secs();

    let task_id = insert_live_claim_with_budget(
        pool.clone(),
        queue.clone(),
        worker.clone(),
        setup.attempts,
        setup.max_attempts,
        epoch,
    )
    .await?;

    // The caller presents its own claim epoch and the attempts it saw at claim
    // time (`setup.attempts`), exactly as the production `fail_undecodable_task`
    // caller does.
    let affected = fail_undecodable_sql(
        pool.clone(),
        task_id.to_string(),
        worker,
        epoch,
        setup.attempts,
        serde_json::json!({ "Err": RELEASE_ERROR_TEXT }),
    )
    .await?;
    let row = read_released_row(pool.clone(), task_id.to_string()).await?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(FailUndecodableRun {
        affected,
        status: row.status,
        attempts: row.attempts,
        done_at_present: row.done_at_present,
        last_result_present: row.last_result_present,
        last_result_err: row.last_result_err,
    }))
}

/// Assert the release fired (affected=1), landed the row in `status` with
/// exactly `attempts`, and persisted the decode error.
fn release_lands_in(
    status: &'static str,
    attempts: i32,
) -> impl Fn(&Result<Outcome<FailUndecodableRun>, String>) -> AssertionResult {
    observe::<FailUndecodableRun, _>("budget release", move |run| {
        if run.affected != 1 {
            return Err(format!("expected exactly one released row, got {run:?}"));
        }
        if run.status != status {
            return Err(format!("expected status={status}, got {run:?}"));
        }
        if run.attempts != attempts {
            return Err(format!("expected attempts={attempts}, got {run:?}"));
        }
        if !run.done_at_present {
            return Err(format!("expected done_at to be set, got {run:?}"));
        }
        if run.last_result_err.as_deref() != Some(RELEASE_ERROR_TEXT) {
            return Err(format!(
                "expected last_result to carry the decode error, got {run:?}"
            ));
        }
        Ok(())
    })
}

fn release_fails_the_live_claim()
-> impl Fn(&Result<Outcome<FailUndecodableRun>, String>) -> AssertionResult {
    observe::<FailUndecodableRun, _>("live-claim release", |run| {
        if run.affected != 1 {
            return Err(format!("expected exactly one released row, got {run:?}"));
        }
        if run.status != "Failed" || run.attempts != 1 || !run.done_at_present {
            return Err(format!(
                "expected the claim to become Failed/attempts=1 with done_at set, got {run:?}"
            ));
        }
        if run.last_result_err.as_deref() != Some(RELEASE_ERROR_TEXT) {
            return Err(format!(
                "expected last_result to carry the decode error, got {run:?}"
            ));
        }
        Ok(())
    })
}

fn release_skips_the_row(
    status: &'static str,
    attempts: i32,
    result_present: bool,
) -> impl Fn(&Result<Outcome<FailUndecodableRun>, String>) -> AssertionResult {
    observe::<FailUndecodableRun, _>("stale release", move |run| {
        if run.affected != 0 {
            return Err(format!(
                "expected the release to match no rows, got {run:?}"
            ));
        }
        if run.status != status
            || run.attempts != attempts
            || run.done_at_present
            || run.last_result_present != result_present
            || run.last_result_err.is_some()
        {
            return Err(format!(
                "expected the {status}/attempts={attempts} row to stay untouched, got {run:?}"
            ));
        }
        Ok(())
    })
}

lets_expect! { #tokio_test
    expect(run_fail_undecodable(setup).await) as fail_undecodable {
        let setup = "live_claim";

        when the_release_matches_the_callers_live_claim {
            to fails_the_attempt_and_persists_the_decode_error {
                release_fails_the_live_claim()
            }
        }

        when the_row_was_acked_done_before_the_release_fired {
            let setup = "acked_done";
            to leaves_the_acked_row_and_its_stored_result_untouched {
                release_skips_the_row("Done", 1, true)
            }
        }

        when the_row_was_swept_back_to_pending_before_the_release_fired {
            let setup = "swept_pending";
            to leaves_the_swept_row_untouched {
                release_skips_the_row("Pending", 1, false)
            }
        }

        when the_row_was_reclaimed_by_another_worker {
            let setup = "reclaimed_by_other";
            to leaves_the_foreign_claim_untouched {
                release_skips_the_row("Running", 0, false)
            }
        }

        when the_row_was_reclaimed_at_a_later_epoch_by_the_same_worker_name {
            let setup = "reclaimed_later_epoch";
            to leaves_the_newer_claim_untouched {
                release_skips_the_row("Running", 0, false)
            }
        }

        when the_stored_attempts_advanced_since_the_claim {
            let setup = "attempts_advanced";
            to leaves_the_advanced_row_untouched {
                release_skips_the_row("Running", 1, false)
            }
        }
    }
}

lets_expect! { #tokio_test
    // ----- retry-budget CASE + LEAST clamp + overflow safety ----------------
    expect(run_fail_undecodable_budget(setup).await) as fail_undecodable_budget {
        let setup = BudgetSetup { attempts: 0, max_attempts: 3 };

        when the_incremented_attempt_stays_below_the_budget {
            // attempts+1 = 1 < max_attempts = 3 → the CASE `ELSE 'Failed'` arm.
            to fails_the_attempt_but_keeps_it_retryable {
                release_lands_in("Failed", 1)
            }
        }

        when the_incremented_attempt_reaches_the_budget_exactly {
            // Boundary of `attempts::bigint + 1 >= max_attempts`: attempts+1 = 3
            // == max_attempts is the first value that flips the row to `Killed`.
            // A `>` off-by-one would keep it `Failed` here, so pin equality.
            let setup = BudgetSetup { attempts: 2, max_attempts: 3 };
            to kills_the_row_when_the_budget_is_hit {
                release_lands_in("Killed", 3)
            }
        }

        when the_incremented_attempt_would_exceed_the_budget {
            // attempts+1 = 3 > max_attempts = 2 → CASE still Killed, and
            // `LEAST(attempts + 1, max_attempts)` must clamp attempts back to 2
            // rather than storing 3. Without LEAST the row would read attempts=3.
            let setup = BudgetSetup { attempts: 2, max_attempts: 2 };
            to clamps_the_stored_attempts_to_the_budget {
                release_lands_in("Killed", 2)
            }
        }

        when the_stored_attempts_are_at_the_i32_maximum {
            // `apalis.jobs` enforces `attempts <= max_attempts` via
            // `jobs_attempts_lte_max_attempts_check`, so a row with
            // `attempts = i32::MAX` can only exist when `max_attempts` is also
            // `i32::MAX`. `attempts::bigint + 1` promotes to bigint so the
            // `+ 1` (i32::MAX + 1) cannot raise "integer out of range"; a bare
            // `attempts + 1` would error out and the release would return an
            // Err instead of an affected row. `LEAST(..., max_attempts)` then
            // re-bounds the bigint sum (i32::MAX + 1) back down to `i32::MAX`
            // before it is stored in the int column.
            let setup = BudgetSetup { attempts: i32::MAX, max_attempts: i32::MAX };
            to survives_the_overflow_and_clamps_to_the_budget {
                release_lands_in("Killed", i32::MAX)
            }
        }
    }
}
