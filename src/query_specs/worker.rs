//! Production-bound database specifications; SQL is used only for fixtures and observations.

#![cfg(feature = "tokio")]

use crate::test_support as support;

use support::{Outcome, observe, with_conn};

use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use apalis_core::task::task_id::TaskId;
use apalis_diesel_postgres::{PgPool, PgTaskId};
use apalis_sql::{DateTime, DateTimeExt};
use diesel::{
    Connection, PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{BigInt, Integer, Nullable, Text, Timestamptz},
};
use lets_expect::{AssertionResult, *};
use serde_json::Value;
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

async fn insert_worker_row(
    pool: PgPool,
    queue: String,
    worker_id: String,
    lease_token: Option<String>,
    last_seen_offset_secs: i64,
) -> Result<(), String> {
    with_conn(pool, move |conn| {
        // Use NOW() - interval to control freshness. last_seen_offset_secs > 0
        // pushes the heartbeat into the past (stale).
        match lease_token {
            Some(token) => {
                sql_query(
                    "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at, lease_token)
                     VALUES ($1, $2, 'PostgresStorage', '', now() - ($3 * INTERVAL '1 second'), now(), $4)",
                )
                .bind::<Text, _>(&worker_id)
                .bind::<Text, _>(&queue)
                .bind::<Integer, _>(last_seen_offset_secs as i32)
                .bind::<Text, _>(&token)
                .execute(conn)
                .map_err(|e| e.to_string())?;
            }
            None => {
                sql_query(
                    "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at)
                     VALUES ($1, $2, 'PostgresStorage', '', now() - ($3 * INTERVAL '1 second'), now())",
                )
                .bind::<Text, _>(&worker_id)
                .bind::<Text, _>(&queue)
                .bind::<Integer, _>(last_seen_offset_secs as i32)
                .execute(conn)
                .map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    })
    .await
}

async fn insert_running_row(
    pool: PgPool,
    queue: String,
    worker_id: String,
    status: &'static str,
    attempts: i32,
    max_attempts: i32,
    last_result: Option<Value>,
) -> Result<PgTaskId, String> {
    let id = Ulid::new();
    let task_id = TaskId::from_str(&id.to_string()).map_err(|e| e.to_string())?;
    let job = serde_json::to_vec("orphan-target").map_err(|e| e.to_string())?;
    let lock_at = <DateTime as DateTimeExt>::from_unix_timestamp(now_unix() as i64);
    with_conn(pool, move |conn| {
        match last_result {
            Some(value) => {
                sql_query(
                    "INSERT INTO apalis.jobs (
                        id, job_type, job, status, attempts, max_attempts, run_at, lock_by, lock_at, last_result
                    ) VALUES ($1, $2, $3, $4, $5, $6, now() - INTERVAL '1 second', $7, $8, $9)",
                )
                .bind::<Text, _>(id.to_string())
                .bind::<Text, _>(&queue)
                .bind::<diesel::sql_types::Binary, _>(job)
                .bind::<Text, _>(status)
                .bind::<Integer, _>(attempts)
                .bind::<Integer, _>(max_attempts)
                .bind::<Text, _>(&worker_id)
                .bind::<Timestamptz, _>(lock_at)
                .bind::<diesel::sql_types::Jsonb, _>(value)
                .execute(conn)
                .map_err(|e| e.to_string())?;
            }
            None => {
                sql_query(
                    "INSERT INTO apalis.jobs (
                        id, job_type, job, status, attempts, max_attempts, run_at, lock_by, lock_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, now() - INTERVAL '1 second', $7, $8)",
                )
                .bind::<Text, _>(id.to_string())
                .bind::<Text, _>(&queue)
                .bind::<diesel::sql_types::Binary, _>(job)
                .bind::<Text, _>(status)
                .bind::<Integer, _>(attempts)
                .bind::<Integer, _>(max_attempts)
                .bind::<Text, _>(&worker_id)
                .bind::<Timestamptz, _>(lock_at)
                .execute(conn)
                .map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    })
    .await?;
    Ok(task_id)
}

#[derive(Debug, QueryableByName)]
struct JobStatusRow {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Integer)]
    attempts: i32,
    #[diesel(sql_type = Nullable<diesel::sql_types::Jsonb>)]
    last_result: Option<Value>,
    #[diesel(sql_type = Nullable<Text>)]
    lock_by: Option<String>,
    /// `done_at IS NOT NULL`: the terminal (`Killed`) re-enqueue branch stamps a
    /// completion timestamp, the retry (`Pending`) branch leaves it NULL.
    #[diesel(sql_type = diesel::sql_types::Bool)]
    done_at_present: bool,
}

async fn job_status_row(pool: PgPool, id: PgTaskId) -> Result<JobStatusRow, String> {
    let id_s = id.to_string();
    with_conn(pool, move |conn| {
        sql_query(
            "SELECT status, attempts, last_result, lock_by, done_at IS NOT NULL AS done_at_present
             FROM apalis.jobs WHERE id = $1",
        )
        .bind::<Text, _>(&id_s)
        .get_result::<JobStatusRow>(conn)
        .map_err(|e| e.to_string())
    })
    .await
}

#[derive(Debug, QueryableByName)]
struct WorkerRow {
    #[diesel(sql_type = Nullable<Text>)]
    lease_token: Option<String>,
    /// Difference between now() and last_seen, in seconds (>= 0 means past).
    #[diesel(sql_type = BigInt)]
    last_seen_age_secs: i64,
}

async fn worker_row(
    pool: PgPool,
    queue: String,
    worker_id: String,
) -> Result<Option<WorkerRow>, String> {
    with_conn(pool, move |conn| {
        sql_query(
            "SELECT lease_token,
                    EXTRACT(EPOCH FROM (now() - last_seen))::BIGINT AS last_seen_age_secs
             FROM apalis.workers
             WHERE worker_type = $1 AND id = $2",
        )
        .bind::<Text, _>(&queue)
        .bind::<Text, _>(&worker_id)
        .get_result::<WorkerRow>(conn)
        .map(Some)
        .or_else(|e| match e {
            diesel::result::Error::NotFound => Ok(None),
            other => Err(other.to_string()),
        })
    })
    .await
}

// --------------------------------------------------------------------------
// Calls to the private production functions in src/queries/worker.rs.
//
// --------------------------------------------------------------------------

/// Adapter for `reenqueue_orphaned_blocking`, connection-bound so the
/// concurrency scenarios below can interleave two sweeps on two connections.
fn reenqueue_orphaned_sql_on(
    conn: &mut PgConnection,
    threshold_secs: i32,
    queue: &str,
) -> Result<usize, crate::Error> {
    let config = crate::Config::new(queue)
        .set_reenqueue_orphaned_after(std::time::Duration::from_secs(threshold_secs as u64));
    crate::queries::worker::reenqueue_orphaned_blocking(conn, &config)
}

/// Adapter for `reenqueue_orphaned_blocking`.
async fn reenqueue_orphaned_sql(
    pool: PgPool,
    threshold_secs: i32,
    queue: String,
) -> Result<usize, String> {
    with_conn(pool, move |conn| {
        reenqueue_orphaned_sql_on(conn, threshold_secs, &queue).map_err(|e| e.to_string())
    })
    .await
}

/// Adapter for `register_worker_blocking`, connection-bound so the advisory-lock
/// contention scenario below can run the register on a second connection while
/// a first connection holds the `(worker_id, worker_type)` xact-scoped lock.
fn register_worker_sql_on(
    conn: &mut PgConnection,
    worker_id: &str,
    queue: &str,
    _storage_name: &str,
    _layers: &str,
    lease_token: &str,
    stale_after_secs: i32,
) -> Result<usize, crate::Error> {
    let worker = apalis_core::worker::context::WorkerContext::new::<()>(worker_id);
    match crate::queries::worker::register_worker_blocking(
        conn,
        queue,
        &worker,
        "PostgresStorage",
        lease_token,
        std::time::Duration::from_secs(stale_after_secs as u64),
    ) {
        Ok(()) => Ok(1),
        Err(crate::Error::AlreadyRegistered { .. }) => Ok(0),
        Err(error) => Err(error),
    }
}

/// Adapter for `register_worker_blocking`. Returns affected row count (0 means
/// production would raise `Error::AlreadyRegistered`).
async fn register_worker_sql(
    pool: PgPool,
    worker_id: String,
    queue: String,
    storage_name: String,
    layers: String,
    lease_token: String,
    stale_after_secs: i32,
) -> Result<usize, String> {
    with_conn(pool, move |conn| {
        register_worker_sql_on(
            conn,
            &worker_id,
            &queue,
            &storage_name,
            &layers,
            &lease_token,
            stale_after_secs,
        )
        .map_err(|e| e.to_string())
    })
    .await
}

/// Adapter for `keep_alive`. Returns affected row count (0 means production
/// would raise `Error::WorkerNotRegistered`).
async fn keep_alive_sql(
    pool: PgPool,
    worker_id: String,
    queue: String,
    lease_token: String,
) -> Result<usize, String> {
    let worker = apalis_core::worker::context::WorkerContext::new::<()>(&worker_id);
    match crate::queries::worker::keep_alive(
        pool,
        crate::Config::new(&queue),
        worker,
        lease_token.into(),
    )
    .await
    {
        Ok(()) => Ok(1),
        Err(crate::Error::WorkerNotRegistered { .. }) => Ok(0),
        Err(error) => Err(error.to_string()),
    }
}

// --------------------------------------------------------------------------
// reenqueue_orphaned: characteristic matrix
// --------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct ReenqueueSetup {
    /// Stored row status. Production WHERE clause matches `Running` or `Queued`.
    status: &'static str,
    /// `attempts` already persisted on the row.
    attempts: i32,
    /// `max_attempts` persisted on the row.
    max_attempts: i32,
    /// If `Some`, an existing `last_result` is stored on the row.
    has_last_result: bool,
    /// Worker `last_seen` offset into the past, in seconds. Threshold is 1s.
    worker_last_seen_age_secs: i64,
    /// If `true`, the row is inserted in a DIFFERENT queue from the one the
    /// re-enqueue runs against.
    other_queue: bool,
}

#[derive(Debug)]
struct ReenqueueRun {
    affected: usize,
    status: String,
    attempts: i32,
    lock_by: Option<String>,
    last_result_value: Option<Value>,
    done_at_present: bool,
}

async fn run_reenqueue(setup: ReenqueueSetup) -> Result<Outcome<ReenqueueRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-worker-reenq-{}", Ulid::new());
    let other_queue = format!("apalis-spec-worker-reenq-other-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool.clone(), other_queue.clone()).await?;

    let worker_id = format!("spec-reenq-worker-{queue}");
    let row_queue = if setup.other_queue {
        other_queue.clone()
    } else {
        queue.clone()
    };

    // FK requires a workers row for the row_queue (matches lock_by + job_type).
    insert_worker_row(
        pool.clone(),
        row_queue.clone(),
        worker_id.clone(),
        Some(format!("token-{}", Ulid::new())),
        setup.worker_last_seen_age_secs,
    )
    .await?;

    let last_result = if setup.has_last_result {
        Some(serde_json::json!({"Ok": "preserved-result"}))
    } else {
        None
    };
    let id = insert_running_row(
        pool.clone(),
        row_queue.clone(),
        worker_id.clone(),
        setup.status,
        setup.attempts,
        setup.max_attempts,
        last_result,
    )
    .await?;

    // Re-enqueue always runs against `queue` (the targeted queue), with a
    // 1-second staleness threshold.
    let affected = reenqueue_orphaned_sql(pool.clone(), 1, queue.clone()).await?;
    let row = job_status_row(pool.clone(), id).await?;

    cleanup_queue(pool.clone(), queue).await?;
    cleanup_queue(pool, other_queue).await?;
    Ok(Outcome::Completed(ReenqueueRun {
        affected,
        status: row.status,
        attempts: row.attempts,
        lock_by: row.lock_by,
        last_result_value: row.last_result,
        done_at_present: row.done_at_present,
    }))
}

fn reenqueue_touched_one_row() -> impl Fn(&Result<Outcome<ReenqueueRun>, String>) -> AssertionResult
{
    observe::<ReenqueueRun, _>("reenqueue affected", |run| {
        if run.affected == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected exactly one row to be re-enqueued, got {}",
                run.affected
            ))
        }
    })
}

fn reenqueue_left_row_untouched()
-> impl Fn(&Result<Outcome<ReenqueueRun>, String>) -> AssertionResult {
    observe::<ReenqueueRun, _>("reenqueue not touched", |run| {
        if run.affected == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected zero rows to be affected, got {} (row now status={}, attempts={})",
                run.affected, run.status, run.attempts
            ))
        }
    })
}

fn reenqueue_row_status(
    expected: &'static str,
) -> impl Fn(&Result<Outcome<ReenqueueRun>, String>) -> AssertionResult {
    observe::<ReenqueueRun, _>("reenqueue row status", move |run| {
        if run.status == expected {
            Ok(())
        } else {
            Err(format!("expected status={expected}, got {:?}", run.status))
        }
    })
}

fn reenqueue_row_attempts(
    expected: i32,
) -> impl Fn(&Result<Outcome<ReenqueueRun>, String>) -> AssertionResult {
    observe::<ReenqueueRun, _>("reenqueue row attempts", move |run| {
        if run.attempts == expected {
            Ok(())
        } else {
            Err(format!(
                "expected attempts={expected}, got {}",
                run.attempts
            ))
        }
    })
}

fn reenqueue_clears_lock_by() -> impl Fn(&Result<Outcome<ReenqueueRun>, String>) -> AssertionResult
{
    observe::<ReenqueueRun, _>("reenqueue clears lock_by", |run| {
        if run.lock_by.is_none() {
            Ok(())
        } else {
            Err(format!("expected lock_by NULL, got {:?}", run.lock_by))
        }
    })
}

fn reenqueue_stamped_completion_timestamp()
-> impl Fn(&Result<Outcome<ReenqueueRun>, String>) -> AssertionResult {
    observe::<ReenqueueRun, _>("reenqueue stamps done_at", |run| {
        if run.done_at_present {
            Ok(())
        } else {
            Err("expected the Killed branch to stamp done_at (non-NULL), got NULL".into())
        }
    })
}

fn reenqueue_left_done_at_null()
-> impl Fn(&Result<Outcome<ReenqueueRun>, String>) -> AssertionResult {
    observe::<ReenqueueRun, _>("reenqueue leaves done_at NULL", |run| {
        if run.done_at_present {
            Err("expected the Pending (retry) branch to leave done_at NULL, got a stamped timestamp"
                .into())
        } else {
            Ok(())
        }
    })
}

fn reenqueue_preserves_lock_by()
-> impl Fn(&Result<Outcome<ReenqueueRun>, String>) -> AssertionResult {
    observe::<ReenqueueRun, _>("reenqueue preserves lock_by", |run| {
        if run.lock_by.is_some() {
            Ok(())
        } else {
            Err("expected lock_by to remain populated when row is not touched".into())
        }
    })
}

fn reenqueue_preserves_last_result()
-> impl Fn(&Result<Outcome<ReenqueueRun>, String>) -> AssertionResult {
    observe::<ReenqueueRun, _>("reenqueue preserves last_result", |run| {
        match &run.last_result_value {
            Some(value) if value.get("Ok").and_then(Value::as_str) == Some("preserved-result") => {
                Ok(())
            }
            other => Err(format!(
                "expected pre-existing last_result to remain (Ok: preserved-result), got {other:?}"
            )),
        }
    })
}

fn reenqueue_writes_heartbeat_marker()
-> impl Fn(&Result<Outcome<ReenqueueRun>, String>) -> AssertionResult {
    observe::<ReenqueueRun, _>("reenqueue writes marker", |run| {
        match &run.last_result_value {
            Some(value)
                if value
                    .get("Err")
                    .and_then(Value::as_str)
                    .map(|s| s.contains("worker heartbeat timeout"))
                    .unwrap_or(false) =>
            {
                Ok(())
            }
            other => Err(format!(
                "expected heartbeat-timeout marker in last_result, got {other:?}"
            )),
        }
    })
}

// --------------------------------------------------------------------------
// reenqueue_orphaned: concurrent sweeps apply exactly once
//
// Concurrent sweeps must consume at most one lost attempt. The UPDATE repeats the
// status predicate outside the candidate sub-select (EvalPlanQual re-check)
// and the sub-select claims rows with `FOR UPDATE OF jobs SKIP LOCKED`.
// Without both, a sweep racing another sweep on the same stale row would
// queue on its row lock and re-apply the UPDATE after the first committed —
// burning a second attempt (or prematurely killing the job).
//
// Pruned: there is no separate red for the *outer* predicate alone. The
// sub-select's FOR UPDATE already serializes competing sweeps (its locking
// re-check re-evaluates the sub-select's own predicates on the latest row
// version), so with SKIP LOCKED in place the outer predicate is unreachable
// defense-in-depth against a future edit removing the locking clause — it
// cannot be triggered deterministically from SQL level.
// --------------------------------------------------------------------------

/// Timing of the competing sweep relative to the first one. The remaining
/// reenqueue characteristics (status, attempt boundaries, staleness, queue
/// scope) are already exhausted by the single-sweep matrix above and stay at
/// their defaults here.
#[derive(Debug, Clone, Copy)]
enum CompetingSweep {
    /// Runs while the first sweep's transaction still holds the row locks it
    /// acquired — the in-flight race `SKIP LOCKED` must short-circuit.
    WhileFirstHoldsRowLocks,
    /// Runs only after the first sweep committed — the rerun must find no
    /// eligible row (`lock_by` is NULL, status is no longer Running/Queued).
    AfterFirstCommitted,
}

#[derive(Debug)]
struct ConcurrentReenqueueRun {
    first_affected: usize,
    competing_affected: usize,
    status: String,
    attempts: i32,
}

async fn run_concurrent_reenqueue(
    timing: CompetingSweep,
) -> Result<Outcome<ConcurrentReenqueueRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-worker-reenq-conc-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-reenq-conc-worker-{queue}");

    // One stale-worker Running row with attempts left: eligible for exactly
    // one re-enqueue (threshold 1s, worker last seen 10s ago).
    insert_worker_row(
        pool.clone(),
        queue.clone(),
        worker_id.clone(),
        Some(format!("token-{}", Ulid::new())),
        10,
    )
    .await?;
    let id = insert_running_row(
        pool.clone(),
        queue.clone(),
        worker_id.clone(),
        "Running",
        0,
        3,
        None,
    )
    .await?;

    let sweep_pool = pool.clone();
    let sweep_queue = queue.clone();
    let (first_affected, competing_affected) =
        tokio::task::spawn_blocking(move || -> Result<(usize, usize), String> {
            let mut first_conn = sweep_pool.get().map_err(|e| e.to_string())?;
            let mut competing_conn = sweep_pool.get().map_err(|e| e.to_string())?;
            // `SET LOCAL lock_timeout` bounds the wait so a regression (the
            // competing sweep queueing on the first sweep's row locks instead
            // of skipping them) fails fast with a lock-timeout error instead
            // of stalling the suite; LOCAL scoping resets it at COMMIT so the
            // pooled connection returns clean.
            let competing_sweep = |conn: &mut PgConnection| -> Result<usize, crate::Error> {
                conn.transaction(|tx| {
                    sql_query("SET LOCAL lock_timeout = '2s'").execute(tx)?;
                    reenqueue_orphaned_sql_on(tx, 1, &sweep_queue)
                })
            };
            match timing {
                CompetingSweep::WhileFirstHoldsRowLocks => {
                    let mut first_affected = 0;
                    let mut competing_affected = 0;
                    first_conn
                        .transaction::<_, crate::Error, _>(|tx| {
                            first_affected = reenqueue_orphaned_sql_on(tx, 1, &sweep_queue)?;
                            competing_affected = competing_sweep(&mut competing_conn)?;
                            Ok(())
                        })
                        .map_err(|e| e.to_string())?;
                    Ok((first_affected, competing_affected))
                }
                CompetingSweep::AfterFirstCommitted => {
                    let first_affected =
                        reenqueue_orphaned_sql_on(&mut first_conn, 1, &sweep_queue)
                            .map_err(|e| e.to_string())?;
                    let competing_affected =
                        competing_sweep(&mut competing_conn).map_err(|e| e.to_string())?;
                    Ok((first_affected, competing_affected))
                }
            }
        })
        .await
        .map_err(|e| e.to_string())??;

    let row = job_status_row(pool.clone(), id).await?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(ConcurrentReenqueueRun {
        first_affected,
        competing_affected,
        status: row.status,
        attempts: row.attempts,
    }))
}

fn concurrent_first_sweep_claimed_the_row()
-> impl Fn(&Result<Outcome<ConcurrentReenqueueRun>, String>) -> AssertionResult {
    observe::<ConcurrentReenqueueRun, _>("first sweep affected", |run| {
        if run.first_affected == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected the first sweep to re-enqueue the row, got affected={}",
                run.first_affected
            ))
        }
    })
}

fn concurrent_competing_sweep_touched_nothing()
-> impl Fn(&Result<Outcome<ConcurrentReenqueueRun>, String>) -> AssertionResult {
    observe::<ConcurrentReenqueueRun, _>("competing sweep affected", |run| {
        if run.competing_affected == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected the competing sweep to touch nothing, got affected={}",
                run.competing_affected
            ))
        }
    })
}

fn concurrent_attempts_incremented_exactly_once()
-> impl Fn(&Result<Outcome<ConcurrentReenqueueRun>, String>) -> AssertionResult {
    observe::<ConcurrentReenqueueRun, _>("attempts after both sweeps", |run| {
        if run.attempts == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected attempts=1 (applied exactly once), got {}",
                run.attempts
            ))
        }
    })
}

fn concurrent_row_landed_in_pending()
-> impl Fn(&Result<Outcome<ConcurrentReenqueueRun>, String>) -> AssertionResult {
    observe::<ConcurrentReenqueueRun, _>("status after both sweeps", |run| {
        if run.status == "Pending" {
            Ok(())
        } else {
            Err(format!("expected status Pending, got {:?}", run.status))
        }
    })
}

// --------------------------------------------------------------------------
// register_worker_blocking: characteristic matrix
// --------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct RegisterSetup {
    /// If `Some`, an existing workers row is pre-inserted with this lease_token
    /// (or `None` lease_token if the inner Option is `None`).
    incumbent_lease_token: Option<Option<&'static str>>,
    /// Pre-existing row's `last_seen` offset into the past (seconds).
    incumbent_age_secs: i64,
    /// Lease token the new registration call presents.
    new_lease_token: &'static str,
    /// Staleness threshold in seconds.
    stale_after_secs: i32,
}

const REGISTER_DEFAULT: RegisterSetup = RegisterSetup {
    incumbent_lease_token: None,
    incumbent_age_secs: 0,
    new_lease_token: "new-token",
    stale_after_secs: 30,
};

#[derive(Debug)]
struct RegisterRun {
    affected: usize,
    stored_lease_token: Option<String>,
}

async fn run_register(setup: RegisterSetup) -> Result<Outcome<RegisterRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-worker-reg-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-reg-worker-{queue}");

    if let Some(incumbent) = setup.incumbent_lease_token {
        insert_worker_row(
            pool.clone(),
            queue.clone(),
            worker_id.clone(),
            incumbent.map(str::to_owned),
            setup.incumbent_age_secs,
        )
        .await?;
    }

    let affected = register_worker_sql(
        pool.clone(),
        worker_id.clone(),
        queue.clone(),
        "PostgresStorage".to_owned(),
        "".to_owned(),
        setup.new_lease_token.to_owned(),
        setup.stale_after_secs,
    )
    .await?;

    let stored = worker_row(pool.clone(), queue.clone(), worker_id.clone()).await?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(RegisterRun {
        affected,
        stored_lease_token: stored.and_then(|w| w.lease_token),
    }))
}

fn register_inserted_one_row() -> impl Fn(&Result<Outcome<RegisterRun>, String>) -> AssertionResult
{
    observe::<RegisterRun, _>("register affected=1", |run| {
        if run.affected == 1 {
            Ok(())
        } else {
            Err(format!("expected affected=1, got {}", run.affected))
        }
    })
}

fn register_was_blocked() -> impl Fn(&Result<Outcome<RegisterRun>, String>) -> AssertionResult {
    observe::<RegisterRun, _>("register affected=0", |run| {
        if run.affected == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected affected=0 (would raise AlreadyRegistered), got {}",
                run.affected
            ))
        }
    })
}

fn register_stored_token_equals_none()
-> impl Fn(&Result<Outcome<RegisterRun>, String>) -> AssertionResult {
    observe::<RegisterRun, _>("register stored token", |run| {
        match &run.stored_lease_token {
            None => Ok(()),
            other => Err(format!("expected no stored lease_token, got {other:?}")),
        }
    })
}

fn register_stored_token_equals(
    expected: &'static str,
) -> impl Fn(&Result<Outcome<RegisterRun>, String>) -> AssertionResult {
    observe::<RegisterRun, _>("register stored token", move |run| {
        match &run.stored_lease_token {
            Some(t) if t == expected => Ok(()),
            other => Err(format!(
                "expected stored lease_token={expected:?}, got {other:?}"
            )),
        }
    })
}

// --------------------------------------------------------------------------
// register_worker_blocking: advisory-lock contention drives affected=0
//
// Registration first calls
// `pg_try_advisory_xact_lock(hashtext($1), hashtext($2))` inside its transaction.
// When a peer already holds that lock for the same
// `(worker_id, worker_type)` pair, the try-lock returns FALSE.
// Production returns `Error::AlreadyRegistered` before reading or inserting
// the worker row; the test adapter maps this error to affected=0.
// This differs from rejection by an incumbent token in the matrix above:
// here no incumbent row exists, so the rejection is attributable solely
// to the advisory lock.
// --------------------------------------------------------------------------

/// `pg_try_advisory_lock` keys are derived from `hashtext` per the production
/// statement; this row is the boolean outcome of the holding transaction's
/// lock acquisition.
#[derive(Debug, QueryableByName)]
struct AdvisoryLockRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    acquired: bool,
}

/// Holds the `(worker_id, queue)` advisory lock on a dedicated connection's
/// open transaction while a *second* connection runs the production register
/// path, then releases the lock. Because the lock is xact-scoped, the holding
/// transaction must stay open (BEGIN, never committed) across the register
/// call; we `ROLLBACK` at the end to release it deterministically rather than
/// relying on connection drop. Returns the register call's affected-row count.
async fn run_register_under_advisory_contention() -> Result<Outcome<RegisterRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-worker-reg-lock-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-reg-lock-worker-{queue}");

    let work_pool = pool.clone();
    let work_queue = queue.clone();
    let work_worker_id = worker_id.clone();
    let affected = tokio::task::spawn_blocking(move || -> Result<usize, String> {
        // Connection A holds the lock; connection B runs the register. Two
        // distinct pooled connections so A's open transaction does not block
        // B's statement at the connection level.
        let mut holder = work_pool.get().map_err(|e| e.to_string())?;
        let mut registrar = work_pool.get().map_err(|e| e.to_string())?;

        // A: BEGIN, grab the xact-scoped advisory lock for this (id, queue).
        sql_query("BEGIN")
            .execute(&mut holder)
            .map_err(|e| e.to_string())?;
        let lock =
            sql_query("SELECT pg_try_advisory_xact_lock(hashtext($1), hashtext($2)) AS acquired")
                .bind::<Text, _>(&work_worker_id)
                .bind::<Text, _>(&work_queue)
                .get_result::<AdvisoryLockRow>(&mut holder)
                .map_err(|e| e.to_string())?;
        if !lock.acquired {
            // Defensive: a clean DB should always grant the lock here. If it
            // does not, the contention precondition is not established and the
            // assertion below would be meaningless.
            let _ = sql_query("ROLLBACK").execute(&mut holder);
            return Err(
                "holder failed to acquire the advisory lock; cannot establish contention".into(),
            );
        }

        // B: run the production register path with a fresh lease token while A
        // still holds the lock. The first try-lock statement must return
        // FALSE; the adapter reports AlreadyRegistered as affected=0.
        let affected = register_worker_sql_on(
            &mut registrar,
            &work_worker_id,
            &work_queue,
            "PostgresStorage",
            "",
            "contended-token",
            30,
        )
        .map_err(|e| e.to_string())?;

        // A: release the xact-scoped lock (auto-releases at xact end, but be
        // explicit so the pooled connection returns clean).
        sql_query("ROLLBACK")
            .execute(&mut holder)
            .map_err(|e| e.to_string())?;

        Ok(affected)
    })
    .await
    .map_err(|e| e.to_string())??;

    let stored = worker_row(pool.clone(), queue.clone(), worker_id.clone()).await?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(RegisterRun {
        affected,
        stored_lease_token: stored.and_then(|w| w.lease_token),
    }))
}

fn register_left_no_row() -> impl Fn(&Result<Outcome<RegisterRun>, String>) -> AssertionResult {
    observe::<RegisterRun, _>("register stored no row", |run| {
        match &run.stored_lease_token {
            None => Ok(()),
            Some(t) => Err(format!(
                "expected no workers row to be inserted under lock contention, found lease_token={t:?}"
            )),
        }
    })
}

// --------------------------------------------------------------------------
// keep_alive: characteristic matrix
// --------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
struct KeepAliveSetup {
    /// Pre-existing row's lease_token (None = no token column).
    stored_lease_token: Option<&'static str>,
    /// Lease token the caller presents.
    caller_lease_token: &'static str,
    /// Use a different worker_id than the one stored.
    fabricate_unknown_worker_id: bool,
    /// Use a different queue than the one stored.
    override_queue: Option<&'static str>,
    /// If `false`, no workers row is inserted at all.
    insert_row: bool,
}

const KEEPALIVE_OK: KeepAliveSetup = KeepAliveSetup {
    stored_lease_token: Some("alive-token"),
    caller_lease_token: "alive-token",
    fabricate_unknown_worker_id: false,
    override_queue: None,
    insert_row: true,
};

#[derive(Debug)]
struct KeepAliveRun {
    affected: usize,
    last_seen_age_after: Option<i64>,
}

async fn run_keep_alive(setup: KeepAliveSetup) -> Result<Outcome<KeepAliveRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-worker-ka-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-ka-worker-{queue}");

    if setup.insert_row {
        // Insert with last_seen 30s in the past so a successful UPDATE moves
        // the age close to zero — an observable signal of the refresh.
        insert_worker_row(
            pool.clone(),
            queue.clone(),
            worker_id.clone(),
            setup.stored_lease_token.map(str::to_owned),
            30,
        )
        .await?;
    }

    let caller_id = if setup.fabricate_unknown_worker_id {
        format!("spec-ka-other-{}", Ulid::new())
    } else {
        worker_id.clone()
    };
    let caller_queue = setup
        .override_queue
        .map(str::to_owned)
        .unwrap_or_else(|| queue.clone());

    let affected = keep_alive_sql(
        pool.clone(),
        caller_id,
        caller_queue,
        setup.caller_lease_token.to_owned(),
    )
    .await?;

    let stored = if setup.insert_row {
        worker_row(pool.clone(), queue.clone(), worker_id.clone()).await?
    } else {
        None
    };
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(KeepAliveRun {
        affected,
        last_seen_age_after: stored.map(|w| w.last_seen_age_secs),
    }))
}

fn keep_alive_refreshed() -> impl Fn(&Result<Outcome<KeepAliveRun>, String>) -> AssertionResult {
    observe::<KeepAliveRun, _>("keep_alive refreshed", |run| {
        if run.affected != 1 {
            return Err(format!("expected affected=1, got {}", run.affected));
        }
        match run.last_seen_age_after {
            Some(age) if age <= 2 => Ok(()),
            Some(age) => Err(format!(
                "expected last_seen to be refreshed to now (age <= 2s), got age={age}s"
            )),
            None => Err("expected the row to remain after keep_alive".into()),
        }
    })
}

fn keep_alive_no_match() -> impl Fn(&Result<Outcome<KeepAliveRun>, String>) -> AssertionResult {
    observe::<KeepAliveRun, _>("keep_alive no match", |run| {
        if run.affected == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected affected=0 (would raise WorkerNotRegistered), got {}",
                run.affected
            ))
        }
    })
}

fn keep_alive_did_not_refresh() -> impl Fn(&Result<Outcome<KeepAliveRun>, String>) -> AssertionResult
{
    observe::<KeepAliveRun, _>("keep_alive stale", |run| match run.last_seen_age_after {
        Some(age) if age >= 20 => Ok(()),
        Some(age) => Err(format!(
            "expected stored last_seen to remain stale (age >= 20s), got age={age}s"
        )),
        None => Err("expected the row to remain even on rejection".into()),
    })
}

// --------------------------------------------------------------------------
// reenqueue_orphaned: an i32::MAX attempt count must not overflow
// --------------------------------------------------------------------------

#[derive(Debug)]
struct OverflowSweepRun {
    swept_ok: bool,
    error: Option<String>,
    status: Option<String>,
    attempts: Option<i32>,
}

async fn run_overflow_sweep() -> Result<Outcome<OverflowSweepRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-worker-overflow-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let worker_id = format!("spec-overflow-worker-{queue}");
    insert_worker_row(
        pool.clone(),
        queue.clone(),
        worker_id.clone(),
        Some(format!("token-{}", Ulid::new())),
        10,
    )
    .await?;

    // Corrupt row: `attempts` already at i32::MAX. The sweep computes
    // `attempts::bigint + 1`, which PostgreSQL rejects as "integer out of range"
    // unless the arithmetic is promoted to bigint.
    let id = insert_running_row(
        pool.clone(),
        queue.clone(),
        worker_id.clone(),
        "Running",
        i32::MAX,
        i32::MAX,
        None,
    )
    .await?;

    let sweep = reenqueue_orphaned_sql(pool.clone(), 1, queue.clone()).await;
    let (status, attempts) = if sweep.is_ok() {
        let row = job_status_row(pool.clone(), id).await?;
        (Some(row.status), Some(row.attempts))
    } else {
        (None, None)
    };
    cleanup_queue(pool.clone(), queue.clone()).await?;

    Ok(Outcome::Completed(OverflowSweepRun {
        swept_ok: sweep.is_ok(),
        error: sweep.err(),
        status,
        attempts,
    }))
}

fn overflow_sweep_does_not_overflow()
-> impl Fn(&Result<Outcome<OverflowSweepRun>, String>) -> AssertionResult {
    observe::<OverflowSweepRun, _>("overflow sweep", |run| {
        if run.swept_ok {
            Ok(())
        } else {
            Err(format!(
                "expected the sweep to survive an i32::MAX attempt count, got error: {}",
                run.error.as_deref().unwrap_or("<none>")
            ))
        }
    })
}

fn overflow_sweep_kills_the_row()
-> impl Fn(&Result<Outcome<OverflowSweepRun>, String>) -> AssertionResult {
    observe::<OverflowSweepRun, _>("overflow sweep status", |run| match run.status.as_deref() {
        Some("Killed") => Ok(()),
        other => Err(format!(
            "expected the exhausted row to be Killed, got {other:?}"
        )),
    })
}

fn overflow_sweep_clamps_attempts_to_max()
-> impl Fn(&Result<Outcome<OverflowSweepRun>, String>) -> AssertionResult {
    // `LEAST(attempts::bigint + 1, max_attempts)` re-bounds the bigint-promoted
    // arithmetic back to `max_attempts` (= i32::MAX here) so the result fits the
    // int column again. A regression that dropped the right-hand bound of
    // `LEAST`, or wrote the raw bigint back incorrectly, would land some value
    // other than i32::MAX (or blow up the UPDATE) — this reads the persisted
    // value back to pin the clamp itself, not merely the terminal status.
    observe::<OverflowSweepRun, _>("overflow sweep attempts", |run| match run.attempts {
        Some(a) if a == i32::MAX => Ok(()),
        other => Err(format!(
            "expected attempts clamped to max_attempts (i32::MAX = {}), got {other:?}",
            i32::MAX
        )),
    })
}

// --------------------------------------------------------------------------
// expectations
// --------------------------------------------------------------------------

// --------------------------------------------------------------------------
// reenqueue_orphaned: the sweep is bounded per statement
// --------------------------------------------------------------------------

/// Per-sweep row cap. MUST match `REENQUEUE_ORPHANED_BATCH_LIMIT` in
/// `src/queries/worker.rs`: this independently stated expectation checks the
/// production recovery call through the test adapter.
const SWEEP_LIMIT: i32 = 1000;

/// Bulk-insert `count` orphaned `Running` rows locked by `worker_id` in one
/// statement, so a single sweep faces more eligible rows than one batch.
async fn bulk_insert_orphans(
    pool: PgPool,
    queue: String,
    worker_id: String,
    count: i32,
) -> Result<(), String> {
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at, lock_by, lock_at)
             SELECT $1 || '-orphan-' || g::text, $1, ''::bytea, 'Running', 0, 25,
                    now() - INTERVAL '1 second', $2, now() - INTERVAL '1 second'
             FROM generate_series(1, $3) AS g",
        )
        .bind::<Text, _>(&queue)
        .bind::<Text, _>(&worker_id)
        .bind::<Integer, _>(count)
        .execute(conn)
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await
}

#[derive(Debug)]
struct BoundedSweepRun {
    total_orphans: i32,
    first_sweep: usize,
    second_sweep: usize,
    after_first: SweepRows,
    after_second: SweepRows,
}

#[derive(Debug, QueryableByName)]
struct SweepRows {
    #[diesel(sql_type = BigInt)]
    total: i64,
    #[diesel(sql_type = BigInt)]
    recovered: i64,
    #[diesel(sql_type = BigInt)]
    remaining: i64,
}

async fn observe_sweep_rows(
    pool: PgPool,
    queue: String,
    worker_id: String,
) -> Result<SweepRows, String> {
    with_conn(pool, move |conn| {
        sql_query(
            "SELECT count(*) AS total,
                count(*) FILTER(WHERE status='Pending' AND attempts=1
                    AND lock_by IS NULL AND lock_at IS NULL AND done_at IS NULL
                    AND last_result = '{\"Err\":\"Re-enqueued due to worker heartbeat timeout.\"}'::jsonb) AS recovered,
                count(*) FILTER(WHERE status='Running' AND attempts=0
                    AND lock_by=$2 AND lock_at IS NOT NULL AND done_at IS NULL
                    AND last_result IS NULL) AS remaining
             FROM apalis.jobs WHERE job_type=$1",
        )
        .bind::<Text, _>(&queue)
        .bind::<Text, _>(&worker_id)
        .get_result::<SweepRows>(conn)
        .map_err(|e| e.to_string())
    })
    .await
}

async fn bounded_sweep_with_strategy(
    pool: PgPool,
    queue: String,
    prefer_repeated_scans: bool,
) -> Result<usize, String> {
    with_conn(pool, move |conn| {
        conn.transaction::<_, crate::Error, _>(|tx| {
            if prefer_repeated_scans {
                // These are session-local planner preferences, not changes to
                // the recovery contract. They expose a legal rescan-prone plan.
                for setting in [
                    "SET LOCAL enable_hashagg=off",
                    "SET LOCAL enable_hashjoin=off",
                    "SET LOCAL enable_mergejoin=off",
                    "SET LOCAL enable_material=off",
                    "SET LOCAL enable_sort=off",
                ] {
                    sql_query(setting)
                        .execute(tx)
                        .map_err(crate::Error::database("setting sweep test plan"))?;
                }
            }
            reenqueue_orphaned_sql_on(tx, 1, &queue)
        })
        .map_err(|e| e.to_string())
    })
    .await
}

async fn run_bounded_sweep(
    prefer_repeated_scans: bool,
) -> Result<Outcome<BoundedSweepRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-worker-bounded-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let worker_id = format!("spec-bounded-worker-{queue}");
    // Stale worker (last_seen 10s ago; sweep threshold is 1s below).
    insert_worker_row(
        pool.clone(),
        queue.clone(),
        worker_id.clone(),
        Some(format!("token-{}", Ulid::new())),
        10,
    )
    .await?;

    // One full batch plus a few, so the first (bounded) sweep cannot clear them
    // all and a second sweep must finish draining the backlog.
    let total = SWEEP_LIMIT + 5;
    bulk_insert_orphans(pool.clone(), queue.clone(), worker_id.clone(), total).await?;

    let result = async {
        let first_sweep =
            bounded_sweep_with_strategy(pool.clone(), queue.clone(), prefer_repeated_scans).await?;
        // Each observation runs after the sweep's transaction has committed.
        let after_first =
            observe_sweep_rows(pool.clone(), queue.clone(), worker_id.clone()).await?;
        let second_sweep =
            bounded_sweep_with_strategy(pool.clone(), queue.clone(), prefer_repeated_scans).await?;
        let after_second =
            observe_sweep_rows(pool.clone(), queue.clone(), worker_id.clone()).await?;
        Ok(Outcome::Completed(BoundedSweepRun {
            total_orphans: total,
            first_sweep,
            second_sweep,
            after_first,
            after_second,
        }))
    }
    .await;
    cleanup_queue(pool.clone(), queue.clone()).await?;
    result
}

fn bounded_first_sweep_hits_the_cap()
-> impl Fn(&Result<Outcome<BoundedSweepRun>, String>) -> AssertionResult {
    observe::<BoundedSweepRun, _>("bounded first sweep", |run| {
        if run.first_sweep == SWEEP_LIMIT as usize {
            Ok(())
        } else {
            Err(format!(
                "expected the first sweep to reclaim exactly the {SWEEP_LIMIT}-row cap, got {} (of {} orphans)",
                run.first_sweep, run.total_orphans
            ))
        }
    })
}

fn bounded_sweep_preserves_committed_rows()
-> impl Fn(&Result<Outcome<BoundedSweepRun>, String>) -> AssertionResult {
    observe::<BoundedSweepRun, _>("bounded committed recovery", |run| {
        let total = i64::from(run.total_orphans);
        let limit = i64::from(SWEEP_LIMIT);
        if run.after_first.total == total
            && run.after_first.recovered == limit
            && run.after_first.remaining == total - limit
            && run.after_second.total == total
            && run.after_second.recovered == total
            && run.after_second.remaining == 0
        {
            Ok(())
        } else {
            Err(format!(
                "unexpected durable rows after first/second sweep: {:?} / {:?}",
                run.after_first, run.after_second
            ))
        }
    })
}

fn bounded_second_sweep_drains_the_remainder()
-> impl Fn(&Result<Outcome<BoundedSweepRun>, String>) -> AssertionResult {
    observe::<BoundedSweepRun, _>("bounded second sweep", |run| {
        let remainder = run.total_orphans as usize - SWEEP_LIMIT as usize;
        if run.second_sweep == remainder {
            Ok(())
        } else {
            Err(format!(
                "expected the second sweep to drain the remaining {remainder} orphans, got {}",
                run.second_sweep
            ))
        }
    })
}

lets_expect! { #tokio_test
    // ----- reenqueue_orphaned: i32::MAX attempts do not overflow -----------
    expect(run_overflow_sweep().await) as overflow_sweep {
        when a_corrupt_row_sits_at_the_i32_max_attempt_count {
            to sweeps_without_an_integer_overflow {
                overflow_sweep_does_not_overflow(),
                overflow_sweep_kills_the_row(),
                overflow_sweep_clamps_attempts_to_max()
            }
        }
    }

    // ----- reenqueue_orphaned matrix --------------------------------------
    expect(run_reenqueue(setup).await) as orphan_recovery {
        let status="Running";
        let attempts=0;
        let max_attempts=3;
        let has_last_result=false;
        let worker_last_seen_age_secs=10;
        let other_queue=false;
        let setup=ReenqueueSetup{status,attempts,max_attempts,has_last_result,worker_last_seen_age_secs,other_queue};
        to requeues_the_task_and_records_the_lost_attempt {
            reenqueue_touched_one_row(), reenqueue_row_status("Pending"), reenqueue_row_attempts(1),
            reenqueue_clears_lock_by(), reenqueue_left_done_at_null(), reenqueue_writes_heartbeat_marker()
        }
        when a_previous_result_exists {
            let has_last_result=true;
            to preserves_the_previous_result_while_requeuing {
                reenqueue_touched_one_row(), reenqueue_row_status("Pending"), reenqueue_row_attempts(1),
                reenqueue_clears_lock_by(), reenqueue_left_done_at_null(), reenqueue_preserves_last_result()
            }
        }
        when the_lost_attempt_exhausts_the_budget {
            let attempts=2;
            to exhausts_the_budget_and_records_the_lost_attempt {
                reenqueue_touched_one_row(), reenqueue_row_status("Killed"), reenqueue_row_attempts(3),
                reenqueue_clears_lock_by(), reenqueue_writes_heartbeat_marker(), reenqueue_stamped_completion_timestamp()
            }
            when a_previous_result_exists {
                let has_last_result=true;
                to replaces_the_previous_result_with_the_terminal_timeout {
                    reenqueue_touched_one_row(), reenqueue_row_status("Killed"), reenqueue_row_attempts(3),
                    reenqueue_clears_lock_by(), reenqueue_writes_heartbeat_marker(), reenqueue_stamped_completion_timestamp()
                }
            }
        }
        when the_attempt_budget_is_already_exhausted {
            let attempts=3;
            to exhausts_the_budget_and_records_the_lost_attempt {
                reenqueue_touched_one_row(), reenqueue_row_status("Killed"), reenqueue_row_attempts(3),
                reenqueue_clears_lock_by(), reenqueue_writes_heartbeat_marker(), reenqueue_stamped_completion_timestamp()
            }
            when a_previous_result_exists {
                let has_last_result=true;
                to replaces_the_previous_result_with_the_terminal_timeout {
                    reenqueue_touched_one_row(), reenqueue_row_status("Killed"), reenqueue_row_attempts(3),
                    reenqueue_clears_lock_by(), reenqueue_writes_heartbeat_marker(), reenqueue_stamped_completion_timestamp()
                }
            }
        }
        when the_worker_heartbeat_is_fresh {
            let worker_last_seen_age_secs=0;
            to leaves_the_live_claim_unchanged { reenqueue_left_row_untouched(), reenqueue_row_status("Running"), reenqueue_preserves_lock_by() }
        }
        when the_task_is_queued {
            let status="Queued";
            to requeues_the_task_and_records_the_lost_attempt {
                reenqueue_touched_one_row(), reenqueue_row_status("Pending"), reenqueue_row_attempts(1),
                reenqueue_clears_lock_by(), reenqueue_left_done_at_null(), reenqueue_writes_heartbeat_marker()
            }
            when a_previous_result_exists {
                let has_last_result=true;
                to preserves_the_previous_result_while_requeuing {
                    reenqueue_touched_one_row(), reenqueue_row_status("Pending"), reenqueue_row_attempts(1),
                    reenqueue_clears_lock_by(), reenqueue_left_done_at_null(), reenqueue_preserves_last_result()
                }
            }
            when the_lost_attempt_exhausts_the_budget {
                let attempts=2;
                to exhausts_the_budget_and_records_the_lost_attempt {
                    reenqueue_touched_one_row(), reenqueue_row_status("Killed"), reenqueue_row_attempts(3),
                    reenqueue_clears_lock_by(), reenqueue_writes_heartbeat_marker(), reenqueue_stamped_completion_timestamp()
                }
                when a_previous_result_exists {
                    let has_last_result=true;
                    to replaces_the_previous_result_with_the_terminal_timeout {
                        reenqueue_touched_one_row(), reenqueue_row_status("Killed"), reenqueue_row_attempts(3),
                        reenqueue_clears_lock_by(), reenqueue_writes_heartbeat_marker(), reenqueue_stamped_completion_timestamp()
                    }
                }
            }
            when the_attempt_budget_is_already_exhausted {
                let attempts=3;
                to exhausts_the_budget_and_records_the_lost_attempt {
                    reenqueue_touched_one_row(), reenqueue_row_status("Killed"), reenqueue_row_attempts(3),
                    reenqueue_clears_lock_by(), reenqueue_writes_heartbeat_marker(), reenqueue_stamped_completion_timestamp()
                }
                when a_previous_result_exists {
                    let has_last_result=true;
                    to replaces_the_previous_result_with_the_terminal_timeout {
                        reenqueue_touched_one_row(), reenqueue_row_status("Killed"), reenqueue_row_attempts(3),
                        reenqueue_clears_lock_by(), reenqueue_writes_heartbeat_marker(), reenqueue_stamped_completion_timestamp()
                    }
                }
            }
            when the_worker_heartbeat_is_fresh {
                let worker_last_seen_age_secs=0;
                to leaves_the_live_claim_unchanged { reenqueue_left_row_untouched(), reenqueue_row_status("Queued"), reenqueue_preserves_lock_by() }
            }
        }
        when the_task_is_pending {
            let status="Pending";
            to preserves_the_inactive_task { reenqueue_left_row_untouched(), reenqueue_row_status("Pending") }
        }
        when the_task_is_failed {
            let status="Failed";
            to preserves_the_inactive_task { reenqueue_left_row_untouched(), reenqueue_row_status("Failed") }
        }
        when the_task_is_done {
            let status="Done";
            to preserves_the_inactive_task { reenqueue_left_row_untouched(), reenqueue_row_status("Done") }
        }
        when the_task_is_killed {
            let status="Killed";
            to preserves_the_inactive_task { reenqueue_left_row_untouched(), reenqueue_row_status("Killed") }
        }
        when the_task_belongs_to_another_queue {
            let other_queue=true;
            to preserves_the_other_queue { reenqueue_left_row_untouched(), reenqueue_row_status("Running") }
        }
    }

    // ----- reenqueue_orphaned: bounded per-statement sweep ----------------
    expect(run_bounded_sweep(prefer_repeated_scans).await) as bounded_sweep {
        let prefer_repeated_scans = false;
        when more_orphans_exist_than_a_single_sweep_may_reclaim {
            to caps_the_first_sweep_at_the_batch_limit {
                bounded_first_sweep_hits_the_cap(),
                bounded_second_sweep_drains_the_remainder(),
                bounded_sweep_preserves_committed_rows()
            }
            when the_database_prefers_repeated_candidate_scans {
                let prefer_repeated_scans = true;
                to caps_each_sweep_and_preserves_the_remaining_attempts {
                    bounded_first_sweep_hits_the_cap(),
                    bounded_second_sweep_drains_the_remainder(),
                    bounded_sweep_preserves_committed_rows()
                }
            }
        }
    }

    // ----- reenqueue_orphaned: concurrent sweeps apply exactly once -------
    expect(run_concurrent_reenqueue(timing).await) as concurrent_reenqueue {
        let timing = CompetingSweep::WhileFirstHoldsRowLocks;

        when a_competing_sweep_runs_while_the_first_holds_row_locks {
            to claims_the_row_on_the_first_sweep {
                concurrent_first_sweep_claimed_the_row(),
                concurrent_competing_sweep_touched_nothing(),
                concurrent_attempts_incremented_exactly_once(),
                concurrent_row_landed_in_pending()
            }
        }

        when the_sweep_reruns_after_the_first_committed {
            let timing = CompetingSweep::AfterFirstCommitted;
            to claims_the_row_on_the_first_sweep {
                concurrent_first_sweep_claimed_the_row(),
                concurrent_competing_sweep_touched_nothing(),
                concurrent_attempts_incremented_exactly_once(),
                concurrent_row_landed_in_pending()
            }
        }
    }

    // ----- register_worker_blocking matrix --------------------------------
    expect(run_register(setup).await) as register {
        let setup = REGISTER_DEFAULT;

        when no_incumbent_row_exists_for_the_worker_id {
            to inserts_a_fresh_row {
                register_inserted_one_row(),
                register_stored_token_equals("new-token")
            }
        }

        when an_incumbent_row_exists_without_a_lease_token {
            // A token-free registration (admin `register_worker`, legacy
            // clients) renews `last_seen` by re-registering; while fresh it
            // holds the name like any live registration.
            let setup = RegisterSetup {
                incumbent_lease_token: Some(None),
                incumbent_age_secs: 0,
                ..REGISTER_DEFAULT
            };
            to refuses_to_replace_the_live_registration {
                register_was_blocked(),
                register_stored_token_equals_none()
            }

            when that_registration_is_stale_past_the_threshold {
                let setup = RegisterSetup {
                    incumbent_lease_token: Some(None),
                    incumbent_age_secs: 120,
                    stale_after_secs: 30,
                    ..REGISTER_DEFAULT
                };
                to takes_over_and_binds_the_lease {
                    register_inserted_one_row(),
                    register_stored_token_equals("new-token")
                }
            }
        }

        when an_incumbent_row_carries_the_same_lease_token {
            // Same-process reregistration (e.g. retry after transient error)
            // passes the same-token guard and refreshes the row.
            let setup = RegisterSetup {
                incumbent_lease_token: Some(Some("new-token")),
                ..REGISTER_DEFAULT
            };
            to refreshes_the_existing_row {
                register_inserted_one_row(),
                register_stored_token_equals("new-token")
            }
        }

        when an_incumbent_row_is_alive_with_a_different_lease_token {
            // A fresh row with another token fails the allowed guard.
            // Production returns AlreadyRegistered before the UPSERT;
            // the adapter maps this rejection to affected=0.
            let setup = RegisterSetup {
                incumbent_lease_token: Some(Some("incumbent-token")),
                incumbent_age_secs: 0,
                ..REGISTER_DEFAULT
            };
            to refuses_to_overwrite_the_incumbent {
                register_was_blocked(),
                register_stored_token_equals("incumbent-token")
            }
        }

        when an_incumbent_row_is_stale_past_the_threshold_with_a_different_token {
            // After the orphan window, the age guard allows takeover.
            // Prior owned claims are recovered before the token is replaced.
            let setup = RegisterSetup {
                incumbent_lease_token: Some(Some("dead-incumbent")),
                incumbent_age_secs: 120,
                stale_after_secs: 30,
                ..REGISTER_DEFAULT
            };
            to allows_the_takeover {
                register_inserted_one_row(),
                register_stored_token_equals("new-token")
            }
        }
    }

    // ----- register_worker_blocking: advisory-lock contention -------------
    // Advisory contention is a second path to AlreadyRegistered, which the
    // adapter maps to affected=0: a peer holding the xact-scoped
    // `(worker_id, worker_type)` advisory lock makes the initial
    // `pg_try_advisory_xact_lock` return FALSE, so registration returns
    // before reading a worker row or issuing its UPSERT.
    expect(run_register_under_advisory_contention().await) as register_under_advisory_contention {
        when a_peer_holds_the_registration_advisory_lock {
            to refuses_to_register_while_the_lock_is_held {
                register_was_blocked(),
                register_left_no_row()
            }
        }
    }

    // ----- keep_alive matrix ----------------------------------------------
    expect(run_keep_alive(setup).await) as keep_alive {
        let setup = KEEPALIVE_OK;

        when the_id_queue_and_lease_token_all_match {
            to refreshes_last_seen_to_now { keep_alive_refreshed() }
        }

        when no_workers_row_exists_at_all {
            let setup = KeepAliveSetup {
                insert_row: false,
                ..KEEPALIVE_OK
            };
            to returns_zero_rows_to_signal_worker_not_registered {
                keep_alive_no_match()
            }
        }

        when the_stored_lease_token_is_null {
            // Pre-migration row: `lease_token = $3` is NULL → unknown → false.
            let setup = KeepAliveSetup {
                stored_lease_token: None,
                ..KEEPALIVE_OK
            };
            to is_rejected_because_null_never_equals_a_supplied_token {
                keep_alive_no_match(),
                keep_alive_did_not_refresh()
            }
        }

        when the_caller_presents_a_different_lease_token {
            let setup = KeepAliveSetup {
                caller_lease_token: "wrong-token",
                ..KEEPALIVE_OK
            };
            to is_rejected_because_the_token_does_not_match {
                keep_alive_no_match(),
                keep_alive_did_not_refresh()
            }
        }

        when the_caller_targets_a_different_queue {
            let setup = KeepAliveSetup {
                override_queue: Some("apalis-spec-worker-ka-wrong-queue"),
                ..KEEPALIVE_OK
            };
            to is_rejected_because_worker_type_does_not_match {
                keep_alive_no_match(),
                keep_alive_did_not_refresh()
            }
        }

        when the_caller_targets_an_unknown_worker_id {
            let setup = KeepAliveSetup {
                fabricate_unknown_worker_id: true,
                ..KEEPALIVE_OK
            };
            to is_rejected_because_id_does_not_match {
                keep_alive_no_match(),
                keep_alive_did_not_refresh()
            }
        }
    }
}
