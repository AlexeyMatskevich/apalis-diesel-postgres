//! Exhaustive specification for `src/queries/worker.rs`.
//!
//! The functions in `queries::worker` are `pub(crate)`, so an integration
//! test crate cannot call them directly. Instead, each spec below issues the
//! *same SQL statement* as the production function and pins the resulting
//! row-level behaviour. Any production-side SQL drift will desync these
//! contracts from the source and the spec must be updated in lock-step.
//!
//! The driver SQL is centralised in the helper functions
//! `reenqueue_orphaned_sql`, `register_worker_sql`, and `keep_alive_sql` —
//! keep them byte-equal to the SQL in `src/queries/worker.rs`.
//!
//! Behaviour already covered elsewhere is not re-tested here:
//!   - `mark_worker_stale` + `register_worker` admin path → `postgres_specs::run_concurrent_admin_register`
//!   - orphan re-enqueue Pending/Killed terminal split → `postgres_queries::run_orphan_reenqueue`
//!   - `AlreadyRegistered` surfaces on the poll stream → `postgres_specs::run_registration_gate_blocks_fetcher`
//!   - lease-token rotation lifecycle (caller-token vs workers_token) → `postgres_specs::run_ack_predicate`
//!
//! Tests gate on `DATABASE_URL`; without it every scenario resolves to
//! `Outcome::Skipped` and the assertions pass.

#![cfg(feature = "tokio")]

mod support;

use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use apalis_core::task::task_id::TaskId;
use apalis_diesel_postgres::{PgPool, PgTaskId};
use apalis_sql::{DateTime, DateTimeExt};
use diesel::{
    PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{BigInt, Integer, Nullable, Text, Timestamptz},
};
use lets_expect::{AssertionError, AssertionResult, *};
use serde_json::Value;
use ulid::Ulid;

// --------------------------------------------------------------------------
// shared scaffolding (kept local so concurrent edits in postgres_specs.rs
// or postgres_queries.rs don't conflict).
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
}

async fn job_status_row(pool: PgPool, id: PgTaskId) -> Result<JobStatusRow, String> {
    let id_s = id.to_string();
    with_conn(pool, move |conn| {
        sql_query("SELECT status, attempts, last_result, lock_by FROM apalis.jobs WHERE id = $1")
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
// SQL mirrors of the private functions in src/queries/worker.rs.
//
// IMPORTANT: keep these byte-equal to the production SQL. If
// `src/queries/worker.rs` changes, update these helpers in lock-step.
// --------------------------------------------------------------------------

/// Mirror of `reenqueue_orphaned_blocking`.
async fn reenqueue_orphaned_sql(
    pool: PgPool,
    threshold_secs: i32,
    queue: String,
) -> Result<usize, String> {
    with_conn(pool, move |conn| {
        sql_query(
            "UPDATE apalis.jobs
             SET status = CASE
                     WHEN attempts + 1 >= max_attempts THEN 'Killed'
                     ELSE 'Pending'
                 END,
                 done_at = CASE
                     WHEN attempts + 1 >= max_attempts THEN now()
                     ELSE NULL
                 END,
                 lock_by = NULL,
                 lock_at = NULL,
                 attempts = LEAST(attempts + 1, max_attempts),
                 last_result = CASE
                     WHEN attempts + 1 >= max_attempts
                         THEN '{\"Err\": \"Re-enqueued due to worker heartbeat timeout.\"}'::jsonb
                     WHEN last_result IS NULL
                         THEN '{\"Err\": \"Re-enqueued due to worker heartbeat timeout.\"}'::jsonb
                     ELSE last_result
                 END
             WHERE id IN (
                 SELECT jobs.id
                 FROM apalis.jobs
                 INNER JOIN apalis.workers
                     ON jobs.lock_by = workers.id
                     AND jobs.job_type = workers.worker_type
                 WHERE (status = 'Running' OR status = 'Queued')
                     AND now() - apalis.workers.last_seen >= ($1 * INTERVAL '1 second')
                     AND jobs.job_type = $2
             )",
        )
        .bind::<Integer, _>(threshold_secs)
        .bind::<Text, _>(&queue)
        .execute(conn)
        .map_err(|e| e.to_string())
    })
    .await
}

/// Mirror of `register_worker_blocking`. Returns affected row count (0 means
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
        sql_query(
            "WITH registration_lock AS (
                 SELECT pg_try_advisory_xact_lock(hashtext($1), hashtext($2)) AS acquired
             )
             INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at, lease_token)
             SELECT $1, $2, $3, $4, now(), now(), $5
             FROM registration_lock
             WHERE acquired
             ON CONFLICT (id, worker_type) DO UPDATE
             SET storage_name = EXCLUDED.storage_name,
                 layers = EXCLUDED.layers,
                 last_seen = now(),
                 lease_token = EXCLUDED.lease_token
             WHERE apalis.workers.lease_token IS NULL
                OR apalis.workers.lease_token = EXCLUDED.lease_token
                OR now() - apalis.workers.last_seen >= ($6 * INTERVAL '1 second')",
        )
        .bind::<Text, _>(&worker_id)
        .bind::<Text, _>(&queue)
        .bind::<Text, _>(&storage_name)
        .bind::<Text, _>(&layers)
        .bind::<Text, _>(&lease_token)
        .bind::<Integer, _>(stale_after_secs)
        .execute(conn)
        .map_err(|e| e.to_string())
    })
    .await
}

/// Mirror of `keep_alive`. Returns affected row count (0 means production
/// would raise `Error::WorkerNotRegistered`).
async fn keep_alive_sql(
    pool: PgPool,
    worker_id: String,
    queue: String,
    lease_token: String,
) -> Result<usize, String> {
    with_conn(pool, move |conn| {
        sql_query(
            "UPDATE apalis.workers
             SET last_seen = now()
             WHERE id = $1 AND worker_type = $2 AND lease_token = $3",
        )
        .bind::<Text, _>(&worker_id)
        .bind::<Text, _>(&queue)
        .bind::<Text, _>(&lease_token)
        .execute(conn)
        .map_err(|e| e.to_string())
    })
    .await
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

const REENQUEUE_DEFAULT: ReenqueueSetup = ReenqueueSetup {
    status: "Running",
    attempts: 0,
    max_attempts: 3,
    has_last_result: false,
    worker_last_seen_age_secs: 10,
    other_queue: false,
};

#[derive(Debug)]
struct ReenqueueRun {
    affected: usize,
    status: String,
    attempts: i32,
    lock_by: Option<String>,
    last_result_value: Option<Value>,
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
// expectations
// --------------------------------------------------------------------------

lets_expect! { #tokio_test
    // ----- reenqueue_orphaned matrix --------------------------------------
    expect(run_reenqueue(setup).await) {
        let setup = REENQUEUE_DEFAULT;

        when a_running_row_with_a_stale_worker_has_attempts_left {
            to is_touched_by_the_orphan_sweep { reenqueue_touched_one_row() }
            to transitions_to_pending { reenqueue_row_status("Pending") }
            to increments_attempts { reenqueue_row_attempts(1) }
            to clears_the_lock_by_column { reenqueue_clears_lock_by() }
            to stamps_the_heartbeat_timeout_marker { reenqueue_writes_heartbeat_marker() }
        }

        when a_queued_row_with_a_stale_worker_has_attempts_left {
            let setup = ReenqueueSetup {
                status: "Queued",
                ..REENQUEUE_DEFAULT
            };
            to is_also_touched_because_queued_is_in_the_predicate {
                reenqueue_touched_one_row()
            }
            to transitions_to_pending { reenqueue_row_status("Pending") }
        }

        when the_pending_branch_already_carries_a_last_result {
            // Preserves a prior successful ack visible to observers while still
            // re-enqueuing the row — see comment block in
            // `reenqueue_orphaned_blocking` SQL.
            let setup = ReenqueueSetup {
                has_last_result: true,
                ..REENQUEUE_DEFAULT
            };
            to is_still_re_enqueued { reenqueue_touched_one_row() }
            to does_not_clobber_the_existing_last_result {
                reenqueue_preserves_last_result()
            }
        }

        when the_kill_branch_unconditionally_overwrites_last_result {
            // attempts+1 (=2) >= max_attempts (=2) → terminal Killed branch,
            // which stamps the marker even if last_result was non-NULL.
            let setup = ReenqueueSetup {
                attempts: 1,
                max_attempts: 2,
                has_last_result: true,
                ..REENQUEUE_DEFAULT
            };
            to transitions_to_killed { reenqueue_row_status("Killed") }
            to overwrites_last_result_with_the_marker {
                reenqueue_writes_heartbeat_marker()
            }
        }

        when the_owning_worker_heartbeat_is_still_fresh {
            // Threshold is 1s; worker is 0s old → predicate `now() - last_seen
            // >= threshold` is false, row stays put.
            let setup = ReenqueueSetup {
                worker_last_seen_age_secs: 0,
                ..REENQUEUE_DEFAULT
            };
            to leaves_the_row_alone { reenqueue_left_row_untouched() }
            to keeps_status_running { reenqueue_row_status("Running") }
            to preserves_lock_by { reenqueue_preserves_lock_by() }
        }

        when the_row_belongs_to_a_different_queue {
            // `jobs.job_type = $2` clause scopes the sweep to one queue.
            let setup = ReenqueueSetup {
                other_queue: true,
                ..REENQUEUE_DEFAULT
            };
            to leaves_the_other_queues_row_alone { reenqueue_left_row_untouched() }
            to keeps_status_running { reenqueue_row_status("Running") }
        }

        when the_row_is_already_in_a_terminal_status {
            // Done is neither Running nor Queued; the predicate filters it out.
            let setup = ReenqueueSetup {
                status: "Done",
                ..REENQUEUE_DEFAULT
            };
            to leaves_the_terminal_row_alone { reenqueue_left_row_untouched() }
        }
    }

    // ----- register_worker_blocking matrix --------------------------------
    expect(run_register(setup).await) {
        let setup = REGISTER_DEFAULT;

        when no_incumbent_row_exists_for_the_worker_id {
            to inserts_a_fresh_row { register_inserted_one_row() }
            to stores_the_new_lease_token {
                register_stored_token_equals("new-token")
            }
        }

        when an_incumbent_row_exists_with_null_lease_token {
            // Legacy / dashboard-side row; the UPSERT WHERE clause unblocks via
            // `lease_token IS NULL` and rotates the token in.
            let setup = RegisterSetup {
                incumbent_lease_token: Some(None),
                ..REGISTER_DEFAULT
            };
            to upserts_and_binds_the_lease { register_inserted_one_row() }
            to rotates_the_token_into_the_row {
                register_stored_token_equals("new-token")
            }
        }

        when an_incumbent_row_carries_the_same_lease_token {
            // Same-process reregistration (e.g. retry after transient error)
            // refreshes the row through the `lease_token = EXCLUDED` arm.
            let setup = RegisterSetup {
                incumbent_lease_token: Some(Some("new-token")),
                ..REGISTER_DEFAULT
            };
            to refreshes_the_existing_row { register_inserted_one_row() }
            to keeps_the_same_lease_token {
                register_stored_token_equals("new-token")
            }
        }

        when an_incumbent_row_is_alive_with_a_different_lease_token {
            // Live-hijack guard: row is fresh, token differs → UPDATE WHERE
            // clause filters it out and INSERT also fails ON CONFLICT, so
            // affected=0 → production raises AlreadyRegistered.
            let setup = RegisterSetup {
                incumbent_lease_token: Some(Some("incumbent-token")),
                incumbent_age_secs: 0,
                ..REGISTER_DEFAULT
            };
            to refuses_to_overwrite_the_incumbent { register_was_blocked() }
            to leaves_the_incumbent_token_in_place {
                register_stored_token_equals("incumbent-token")
            }
        }

        when an_incumbent_row_is_stale_past_the_threshold_with_a_different_token {
            // Legitimate restart after orphan window: the third WHERE arm
            // `now() - last_seen >= threshold` lets the takeover through.
            let setup = RegisterSetup {
                incumbent_lease_token: Some(Some("dead-incumbent")),
                incumbent_age_secs: 120,
                stale_after_secs: 30,
                ..REGISTER_DEFAULT
            };
            to allows_the_takeover { register_inserted_one_row() }
            to rotates_the_lease_token { register_stored_token_equals("new-token") }
        }
    }

    // ----- keep_alive matrix ----------------------------------------------
    expect(run_keep_alive(setup).await) {
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
                keep_alive_no_match()
            }
            to does_not_refresh_last_seen { keep_alive_did_not_refresh() }
        }

        when the_caller_presents_a_different_lease_token {
            let setup = KeepAliveSetup {
                caller_lease_token: "wrong-token",
                ..KEEPALIVE_OK
            };
            to is_rejected_because_the_token_does_not_match {
                keep_alive_no_match()
            }
            to does_not_refresh_last_seen { keep_alive_did_not_refresh() }
        }

        when the_caller_targets_a_different_queue {
            let setup = KeepAliveSetup {
                override_queue: Some("apalis-spec-worker-ka-wrong-queue"),
                ..KEEPALIVE_OK
            };
            to is_rejected_because_worker_type_does_not_match {
                keep_alive_no_match()
            }
            to does_not_refresh_last_seen { keep_alive_did_not_refresh() }
        }

        when the_caller_targets_an_unknown_worker_id {
            let setup = KeepAliveSetup {
                fabricate_unknown_worker_id: true,
                ..KEEPALIVE_OK
            };
            to is_rejected_because_id_does_not_match { keep_alive_no_match() }
            to does_not_refresh_last_seen { keep_alive_did_not_refresh() }
        }
    }
}
