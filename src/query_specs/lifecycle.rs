//! Database specifications for the operations that end a registration or
//! remove queue data: releasing a worker, purging terminal tasks and pruning
//! stale registrations. SQL is used only for fixtures and observations.

use crate::{
    PgPool,
    queries::{retention, worker},
    test_support as support,
};
use apalis_core::worker::context::WorkerContext;
use diesel::{
    PgConnection, QueryableByName, RunQueryDsl,
    connection::SimpleConnection,
    sql_query,
    sql_types::{BigInt, Bool, Integer, Jsonb, Nullable, Text},
};
use lets_expect::{AssertionResult, *};
use serde_json::Value;
use std::time::Duration;
use support::{Outcome, observe, with_conn};
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

fn seed_worker(
    conn: &mut PgConnection,
    queue: &str,
    worker: &str,
    lease_token: Option<&str>,
    last_seen_age_secs: i64,
) -> Result<(), String> {
    sql_query(
        "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at, lease_token)
         VALUES ($1, $2, 'PostgresStorage', '', clock_timestamp() - ($3 * INTERVAL '1 second'), clock_timestamp(), $4)",
    )
    .bind::<Text, _>(worker)
    .bind::<Text, _>(queue)
    .bind::<BigInt, _>(last_seen_age_secs)
    .bind::<Nullable<Text>, _>(lease_token)
    .execute(conn)
    .map(drop)
    .map_err(|e| e.to_string())
}

/// One job row. Active rows name their owner; a completed row keeps naming
/// its last owner, exactly as acknowledgement leaves it.
struct JobSeed<'a> {
    status: &'static str,
    attempts: i32,
    max_attempts: i32,
    owner: Option<&'a str>,
    /// `None` leaves `done_at` NULL.
    done_at_age_secs: Option<i64>,
    run_at_age_secs: i64,
    last_result: Option<Value>,
}

fn seed_job(conn: &mut PgConnection, queue: &str, seed: &JobSeed<'_>) -> Result<String, String> {
    let id = Ulid::new().to_string();
    sql_query(
        "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at, done_at, lock_by, lock_at, last_result)
         VALUES ($1, $2, '\\x00'::bytea, $3, $4, $5,
                 clock_timestamp() - ($6 * INTERVAL '1 second'),
                 CASE WHEN $7::bigint IS NULL THEN NULL ELSE clock_timestamp() - ($7 * INTERVAL '1 second') END,
                 $8,
                 CASE WHEN $8::text IS NULL THEN NULL ELSE date_trunc('second', clock_timestamp()) END,
                 $9)",
    )
    .bind::<Text, _>(&id)
    .bind::<Text, _>(queue)
    .bind::<Text, _>(seed.status)
    .bind::<Integer, _>(seed.attempts)
    .bind::<Integer, _>(seed.max_attempts)
    .bind::<BigInt, _>(seed.run_at_age_secs)
    .bind::<Nullable<BigInt>, _>(seed.done_at_age_secs)
    .bind::<Nullable<Text>, _>(seed.owner)
    .bind::<Nullable<Jsonb>, _>(seed.last_result.clone())
    .execute(conn)
    .map_err(|e| e.to_string())?;
    Ok(id)
}

#[derive(Debug, Clone, QueryableByName)]
struct JobAfter {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Integer)]
    attempts: i32,
    #[diesel(sql_type = Nullable<Text>)]
    lock_by: Option<String>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    last_result: Option<Value>,
    #[diesel(sql_type = Bool)]
    done_at_present: bool,
}

fn job_after(conn: &mut PgConnection, id: &str) -> Result<JobAfter, String> {
    sql_query(
        "SELECT status, attempts, lock_by, last_result, done_at IS NOT NULL AS done_at_present
         FROM apalis.jobs WHERE id = $1",
    )
    .bind::<Text, _>(id)
    .get_result::<JobAfter>(conn)
    .map_err(|e| e.to_string())
}

#[derive(Debug, Clone, QueryableByName)]
struct WorkerAfter {
    #[diesel(sql_type = Nullable<Text>)]
    lease_token: Option<String>,
    #[diesel(sql_type = BigInt)]
    last_seen_epoch: i64,
}

fn worker_after(
    conn: &mut PgConnection,
    queue: &str,
    worker: &str,
) -> Result<Option<WorkerAfter>, String> {
    sql_query(
        "SELECT lease_token, EXTRACT(EPOCH FROM last_seen)::bigint AS last_seen_epoch
         FROM apalis.workers WHERE worker_type = $1 AND id = $2",
    )
    .bind::<Text, _>(queue)
    .bind::<Text, _>(worker)
    .get_result::<WorkerAfter>(conn)
    .map(Some)
    .or_else(|e| match e {
        diesel::result::Error::NotFound => Ok(None),
        other => Err(other.to_string()),
    })
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

fn count_jobs(conn: &mut PgConnection, queue: &str) -> Result<i64, String> {
    sql_query("SELECT count(*)::bigint AS n FROM apalis.jobs WHERE job_type = $1")
        .bind::<Text, _>(queue)
        .get_result::<Count>(conn)
        .map(|row| row.n)
        .map_err(|e| e.to_string())
}

fn count_workers(conn: &mut PgConnection, queue: &str) -> Result<i64, String> {
    sql_query("SELECT count(*)::bigint AS n FROM apalis.workers WHERE worker_type = $1")
        .bind::<Text, _>(queue)
        .get_result::<Count>(conn)
        .map(|row| row.n)
        .map_err(|e| e.to_string())
}

// --------------------------------------------------------------------------
// release_worker_blocking
// --------------------------------------------------------------------------

/// Who owns the registration row when the release runs.
#[derive(Clone, Copy)]
enum Ownership {
    /// The row carries the releasing storage's token.
    Owned,
    /// No row exists for the name and queue.
    Absent,
    /// Another storage's fresh token owns the row.
    Foreign,
    /// A fresh admin registration without a token holds the name.
    TokenFree,
}

/// What the worker holds in the queue when it releases the registration.
#[derive(Clone, Copy)]
enum Claim {
    None,
    RunningWithBudget,
    QueuedWithBudget,
    RunningExhausted,
    RunningWithEarlierResult,
    Completed,
    ActiveInAnotherQueue,
}

#[derive(Debug)]
struct ReleaseRun {
    outcome: &'static str,
    recovered: usize,
    /// `None`: no row; `Some(token)`: the row's token after the release.
    registration: Option<WorkerAfter>,
    job: Option<JobAfter>,
    /// A fresh token registering the same name right after the release.
    successor: &'static str,
}

const HELD_TOKEN: &str = "held-token";

async fn run_release(ownership: Ownership, claim: Claim) -> Result<Outcome<ReleaseRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-release-{}", Ulid::new());
    let other_queue = format!("{queue}-other");
    let worker = format!("release-worker-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool.clone(), other_queue.clone()).await?;
    let run = {
        let (queue, other_queue, worker) = (queue.clone(), other_queue.clone(), worker.clone());
        with_conn(pool.clone(), move |conn| {
            match ownership {
                Ownership::Owned => seed_worker(conn, &queue, &worker, Some(HELD_TOKEN), 0)?,
                Ownership::Absent => {}
                Ownership::Foreign => seed_worker(conn, &queue, &worker, Some("other-token"), 0)?,
                Ownership::TokenFree => seed_worker(conn, &queue, &worker, None, 0)?,
            }
            let job_queue = match claim {
                Claim::ActiveInAnotherQueue => {
                    seed_worker(conn, &other_queue, &worker, Some(HELD_TOKEN), 0)?;
                    other_queue.as_str()
                }
                _ => queue.as_str(),
            };
            let seed = match claim {
                Claim::None => None,
                Claim::RunningWithBudget | Claim::ActiveInAnotherQueue => Some(JobSeed {
                    status: "Running",
                    attempts: 0,
                    max_attempts: 3,
                    owner: Some(&worker),
                    done_at_age_secs: None,
                    run_at_age_secs: 1,
                    last_result: None,
                }),
                Claim::QueuedWithBudget => Some(JobSeed {
                    status: "Queued",
                    attempts: 1,
                    max_attempts: 3,
                    owner: Some(&worker),
                    done_at_age_secs: None,
                    run_at_age_secs: 1,
                    last_result: None,
                }),
                Claim::RunningExhausted => Some(JobSeed {
                    status: "Running",
                    attempts: 2,
                    max_attempts: 3,
                    owner: Some(&worker),
                    done_at_age_secs: None,
                    run_at_age_secs: 1,
                    last_result: Some(serde_json::json!({"Err": "earlier failure"})),
                }),
                Claim::RunningWithEarlierResult => Some(JobSeed {
                    status: "Running",
                    attempts: 1,
                    max_attempts: 3,
                    owner: Some(&worker),
                    done_at_age_secs: None,
                    run_at_age_secs: 1,
                    last_result: Some(serde_json::json!({"Err": "earlier failure"})),
                }),
                Claim::Completed => Some(JobSeed {
                    status: "Done",
                    attempts: 1,
                    max_attempts: 3,
                    owner: Some(&worker),
                    done_at_age_secs: Some(1),
                    run_at_age_secs: 2,
                    last_result: Some(serde_json::json!({"Ok": null})),
                }),
            };
            let job_id = seed
                .map(|seed| seed_job(conn, job_queue, &seed))
                .transpose()?;
            let (outcome, recovered) =
                match worker::release_worker_blocking(conn, &queue, &worker, HELD_TOKEN) {
                    Ok(recovered) => ("released", recovered),
                    Err(crate::Error::WorkerNotRegistered { .. }) => ("not_registered", 0),
                    Err(other) => return Err(other.to_string()),
                };
            let registration = worker_after(conn, &queue, &worker)?;
            let job = job_id.map(|id| job_after(conn, &id)).transpose()?;
            let successor = match worker::register_worker_blocking(
                conn,
                &queue,
                &WorkerContext::new::<()>(&worker),
                "PostgresStorage",
                "successor-token",
                Duration::from_secs(30),
            ) {
                Ok(()) => "registered",
                Err(crate::Error::AlreadyRegistered { .. }) => "refused",
                Err(other) => return Err(other.to_string()),
            };
            Ok(ReleaseRun {
                outcome,
                recovered,
                registration,
                job,
                successor,
            })
        })
        .await
    };
    cleanup_queue(pool.clone(), queue).await?;
    cleanup_queue(pool, other_queue).await?;
    run.map(Outcome::Completed)
}

fn released(recovered: usize) -> impl Fn(&Result<Outcome<ReleaseRun>, String>) -> AssertionResult {
    observe::<ReleaseRun, _>("release outcome", move |run| {
        if run.outcome == "released" && run.recovered == recovered {
            Ok(())
        } else {
            Err(format!(
                "expected released with {recovered} recovered task(s), got {} with {}",
                run.outcome, run.recovered
            ))
        }
    })
}

fn not_registered() -> impl Fn(&Result<Outcome<ReleaseRun>, String>) -> AssertionResult {
    observe::<ReleaseRun, _>("release outcome", |run| {
        if run.outcome == "not_registered" {
            Ok(())
        } else {
            Err(format!("expected WorkerNotRegistered, got {}", run.outcome))
        }
    })
}

fn registration_released() -> impl Fn(&Result<Outcome<ReleaseRun>, String>) -> AssertionResult {
    observe::<ReleaseRun, _>("released registration row", |run| match &run.registration {
        Some(row) if row.lease_token.is_none() && row.last_seen_epoch == 0 => Ok(()),
        other => Err(format!(
            "expected the row to stay with no token and an epoch heartbeat, got {other:?}"
        )),
    })
}

fn registration_untouched(
    token: Option<&'static str>,
) -> impl Fn(&Result<Outcome<ReleaseRun>, String>) -> AssertionResult {
    observe::<ReleaseRun, _>("untouched registration row", move |run| {
        match &run.registration {
            Some(row) if row.lease_token.as_deref() == token && row.last_seen_epoch > 0 => Ok(()),
            other => Err(format!(
                "expected the row to keep token {token:?} and its heartbeat, got {other:?}"
            )),
        }
    })
}

fn registration_absent() -> impl Fn(&Result<Outcome<ReleaseRun>, String>) -> AssertionResult {
    observe::<ReleaseRun, _>("absent registration row", |run| match &run.registration {
        None => Ok(()),
        other => Err(format!("expected no row, got {other:?}")),
    })
}

fn successor(
    expected: &'static str,
) -> impl Fn(&Result<Outcome<ReleaseRun>, String>) -> AssertionResult {
    observe::<ReleaseRun, _>("successor registration", move |run| {
        if run.successor == expected {
            Ok(())
        } else {
            Err(format!(
                "expected the successor to be {expected}, got {}",
                run.successor
            ))
        }
    })
}

fn released_marker(value: &Value) -> bool {
    value.get("Err").and_then(Value::as_str)
        == Some("Re-enqueued because the worker released its registration.")
}

fn job_handed_back(
    status: &'static str,
    attempts: i32,
) -> impl Fn(&Result<Outcome<ReleaseRun>, String>) -> AssertionResult {
    observe::<ReleaseRun, _>("recovered job", move |run| match &run.job {
        Some(job)
            if job.status == status
                && job.attempts == attempts
                && job.lock_by.is_none()
                && job.done_at_present == (status == "Killed")
                && job.last_result.as_ref().is_some_and(released_marker) =>
        {
            Ok(())
        }
        other => Err(format!(
            "expected an unowned {status} row with {attempts} attempt(s) and the release marker, got {other:?}"
        )),
    })
}

/// A claim that no handler started returns to `Pending` exactly as it was
/// before the claim: same attempt count, no result, no completion timestamp.
fn job_handed_back_untouched(
    attempts: i32,
) -> impl Fn(&Result<Outcome<ReleaseRun>, String>) -> AssertionResult {
    observe::<ReleaseRun, _>("handed-back claim", move |run| match &run.job {
        Some(job)
            if job.status == "Pending"
                && job.attempts == attempts
                && job.lock_by.is_none()
                && !job.done_at_present
                && job.last_result.is_none() =>
        {
            Ok(())
        }
        other => Err(format!(
            "expected an unowned Pending row with {attempts} attempt(s) and no result, got {other:?}"
        )),
    })
}

fn job_handed_back_with_earlier_result()
-> impl Fn(&Result<Outcome<ReleaseRun>, String>) -> AssertionResult {
    observe::<ReleaseRun, _>("recovered job keeps its result", |run| match &run.job {
        Some(job)
            if job.status == "Pending"
                && job.attempts == 2
                && job.lock_by.is_none()
                && job.last_result == Some(serde_json::json!({"Err": "earlier failure"})) =>
        {
            Ok(())
        }
        other => Err(format!(
            "expected a Pending row that kept its earlier result, got {other:?}"
        )),
    })
}

fn job_untouched(
    status: &'static str,
) -> impl Fn(&Result<Outcome<ReleaseRun>, String>) -> AssertionResult {
    observe::<ReleaseRun, _>("untouched job", move |run| match &run.job {
        Some(job) if job.status == status && job.lock_by.is_some() => Ok(()),
        other => Err(format!(
            "expected the {status} row to keep its owner, got {other:?}"
        )),
    })
}

// --------------------------------------------------------------------------
// purge_terminal_tasks
// --------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Row {
    Pending,
    Queued,
    Running,
    Done,
    FailedWithBudget,
    FailedExhausted,
    Killed,
    /// A `Done` row written outside this crate, without a completion time.
    DoneWithoutCompletionTime,
}

#[derive(Clone, Copy)]
enum Age {
    /// Completed (or scheduled) just now.
    Fresh,
    /// Completed (or scheduled) an hour ago.
    Old,
}

#[derive(Clone, Copy)]
enum Window {
    Zero,
    OneMinute,
}

#[derive(Clone, Copy)]
enum Scope {
    SameQueue,
    OtherQueue,
}

#[derive(Debug)]
struct PurgeRun {
    deleted: usize,
    remaining: i64,
}

async fn run_purge(
    row: Row,
    age: Age,
    window: Window,
    scope: Scope,
) -> Result<Outcome<PurgeRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-purge-{}", Ulid::new());
    let other_queue = format!("{queue}-other");
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool.clone(), other_queue.clone()).await?;
    let run = {
        let (queue, other_queue) = (queue.clone(), other_queue.clone());
        with_conn(pool.clone(), move |conn| {
            let row_queue = match scope {
                Scope::SameQueue => queue.as_str(),
                Scope::OtherQueue => other_queue.as_str(),
            };
            let age_secs = match age {
                Age::Fresh => 0,
                Age::Old => 3_600,
            };
            let owner = format!("purge-owner-{}", Ulid::new());
            let (status, attempts, max_attempts, owned, done) = match row {
                Row::Pending => ("Pending", 0, 3, false, false),
                Row::Queued => ("Queued", 0, 3, true, false),
                Row::Running => ("Running", 0, 3, true, false),
                Row::Done => ("Done", 1, 3, true, true),
                Row::FailedWithBudget => ("Failed", 1, 3, true, true),
                Row::FailedExhausted => ("Failed", 3, 3, true, true),
                Row::Killed => ("Killed", 3, 3, true, true),
                Row::DoneWithoutCompletionTime => ("Done", 1, 3, false, false),
            };
            if owned {
                seed_worker(conn, row_queue, &owner, Some("purge-token"), 0)?;
            }
            seed_job(
                conn,
                row_queue,
                &JobSeed {
                    status,
                    attempts,
                    max_attempts,
                    owner: owned.then_some(owner.as_str()),
                    done_at_age_secs: done.then_some(age_secs),
                    run_at_age_secs: age_secs,
                    last_result: None,
                },
            )?;
            let completed_before = match window {
                Window::Zero => Duration::ZERO,
                Window::OneMinute => Duration::from_secs(60),
            };
            let cutoff =
                retention::server_cutoff(conn, completed_before).map_err(|e| e.to_string())?;
            let deleted = retention::purge_terminal_batch(conn, &queue, cutoff, 100)
                .map_err(|e| e.to_string())?;
            let remaining = count_jobs(conn, row_queue)?;
            Ok(PurgeRun { deleted, remaining })
        })
        .await
    };
    cleanup_queue(pool.clone(), queue).await?;
    cleanup_queue(pool, other_queue).await?;
    run.map(Outcome::Completed)
}

fn purged() -> impl Fn(&Result<Outcome<PurgeRun>, String>) -> AssertionResult {
    observe::<PurgeRun, _>("purge", |run| {
        if run.deleted == 1 && run.remaining == 0 {
            Ok(())
        } else {
            Err(format!("expected the row to be deleted, got {run:?}"))
        }
    })
}

fn kept() -> impl Fn(&Result<Outcome<PurgeRun>, String>) -> AssertionResult {
    observe::<PurgeRun, _>("purge", |run| {
        if run.deleted == 0 && run.remaining == 1 {
            Ok(())
        } else {
            Err(format!("expected the row to be kept, got {run:?}"))
        }
    })
}

#[derive(Debug)]
struct BatchedPurgeRun {
    first_batch: usize,
    total: usize,
    remaining: i64,
}

async fn run_batched_purge() -> Result<Outcome<BatchedPurgeRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-purge-batch-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let run = async {
        let seeded_queue = queue.clone();
        with_conn(pool.clone(), move |conn| {
            for _ in 0..5 {
                seed_job(
                    conn,
                    &seeded_queue,
                    &JobSeed {
                        status: "Done",
                        attempts: 1,
                        max_attempts: 3,
                        owner: None,
                        done_at_age_secs: Some(0),
                        run_at_age_secs: 1,
                        last_result: None,
                    },
                )?;
            }
            Ok(())
        })
        .await?;
        let first_queue = queue.clone();
        let first_batch = with_conn(pool.clone(), move |conn| {
            let cutoff =
                retention::server_cutoff(conn, Duration::ZERO).map_err(|e| e.to_string())?;
            retention::purge_terminal_batch(conn, &first_queue, cutoff, 2)
                .map_err(|e| e.to_string())
        })
        .await?;
        let total =
            retention::purge_terminal_tasks_batched(pool.clone(), queue.clone(), Duration::ZERO, 2)
                .await
                .map_err(|e| e.to_string())?;
        let count_queue = queue.clone();
        let remaining = with_conn(pool.clone(), move |conn| count_jobs(conn, &count_queue)).await?;
        Ok(BatchedPurgeRun {
            first_batch,
            total,
            remaining,
        })
    }
    .await;
    cleanup_queue(pool, queue).await?;
    run.map(Outcome::Completed)
}

/// A batch statement run under a plan that re-runs its candidate selection.
#[derive(Clone, Copy, Debug)]
enum RescannedBatch {
    Purge,
    Prune,
}

/// Leaves the planner only a nested-loop semi-join over sequential scans: the
/// plan in which a limited candidate subquery sits on the inner side and runs
/// again for every outer row.
const RESCANNING_PLAN: &str = "SET LOCAL enable_hashjoin = off; SET LOCAL enable_mergejoin = off;
    SET LOCAL enable_hashagg = off; SET LOCAL enable_sort = off; SET LOCAL enable_material = off;
    SET LOCAL enable_memoize = off; SET LOCAL enable_bitmapscan = off;
    SET LOCAL enable_indexscan = off; SET LOCAL enable_indexonlyscan = off;";

/// Five deletable rows stored in their candidate order, and one batch of two
/// under [`RESCANNING_PLAN`], rolled back so the planner settings never reach
/// another scenario. Returns how many rows the batch deleted.
async fn run_batch_under_a_rescanning_plan(
    batch: RescannedBatch,
) -> Result<Outcome<usize>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-rescan-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let run = {
        let queue = queue.clone();
        with_conn(pool.clone(), move |conn| {
            for index in 0..5 {
                match batch {
                    RescannedBatch::Purge => {
                        seed_job(
                            conn,
                            &queue,
                            &JobSeed {
                                status: "Done",
                                attempts: 1,
                                max_attempts: 3,
                                owner: None,
                                done_at_age_secs: Some(60),
                                run_at_age_secs: 120,
                                last_result: None,
                            },
                        )?;
                    }
                    RescannedBatch::Prune => {
                        seed_worker(conn, &queue, &format!("rescan-worker-{index}"), None, 3_600)?;
                    }
                }
            }
            let cutoff =
                retention::server_cutoff(conn, Duration::ZERO).map_err(|e| e.to_string())?;
            let deleted = conn
                .batch_execute(&format!("BEGIN; {RESCANNING_PLAN}"))
                .map_err(|e| e.to_string())
                .and_then(|()| {
                    match batch {
                        RescannedBatch::Purge => {
                            retention::purge_terminal_batch(conn, &queue, cutoff, 2)
                        }
                        RescannedBatch::Prune => {
                            retention::prune_workers_batch(conn, &queue, Duration::from_secs(60), 2)
                        }
                    }
                    .map_err(|e| e.to_string())
                });
            conn.batch_execute("ROLLBACK").map_err(|e| e.to_string())?;
            deleted
        })
        .await
    };
    cleanup_queue(pool, queue).await?;
    run.map(Outcome::Completed)
}

fn deletes_exactly(expected: usize) -> impl Fn(&Result<Outcome<usize>, String>) -> AssertionResult {
    observe::<usize, _>("batch under a rescanning plan", move |deleted| {
        if *deleted == expected {
            Ok(())
        } else {
            Err(format!(
                "expected one batch to delete {expected} rows, it deleted {deleted}"
            ))
        }
    })
}

fn drains_in_batches() -> impl Fn(&Result<Outcome<BatchedPurgeRun>, String>) -> AssertionResult {
    observe::<BatchedPurgeRun, _>("batched purge", |run| {
        if run.first_batch == 2 && run.total == 3 && run.remaining == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected one batch of 2, then 3 more across repeated batches, and no rows left; got {run:?}"
            ))
        }
    })
}

// --------------------------------------------------------------------------
// prune_workers_blocking
// --------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Freshness {
    Fresh,
    Stale,
}

#[derive(Clone, Copy)]
enum Reference {
    None,
    ActiveClaim,
    CompletedTask,
}

#[derive(Clone, Copy)]
enum Hold {
    Free,
    /// A peer transaction holds the row as an in-flight claim does.
    HeldByPeer,
}

#[derive(Debug)]
struct PruneRun {
    deleted: usize,
    remaining: i64,
}

async fn run_prune(
    freshness: Freshness,
    reference: Reference,
    scope: Scope,
    hold: Hold,
) -> Result<Outcome<PruneRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-prune-{}", Ulid::new());
    let other_queue = format!("{queue}-other");
    let worker = format!("prune-worker-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool.clone(), other_queue.clone()).await?;
    let run = {
        let (pool, queue, other_queue, worker) = (
            pool.clone(),
            queue.clone(),
            other_queue.clone(),
            worker.clone(),
        );
        tokio::task::spawn_blocking(move || -> Result<PruneRun, String> {
            let mut conn = pool.get().map_err(|e| e.to_string())?;
            let row_queue = match scope {
                Scope::SameQueue => queue.as_str(),
                Scope::OtherQueue => other_queue.as_str(),
            };
            let age = match freshness {
                Freshness::Fresh => 0,
                Freshness::Stale => 3_600,
            };
            seed_worker(&mut conn, row_queue, &worker, Some("prune-token"), age)?;
            match reference {
                Reference::None => {}
                Reference::ActiveClaim => {
                    seed_job(
                        &mut conn,
                        row_queue,
                        &JobSeed {
                            status: "Running",
                            attempts: 0,
                            max_attempts: 3,
                            owner: Some(&worker),
                            done_at_age_secs: None,
                            run_at_age_secs: age,
                            last_result: None,
                        },
                    )?;
                }
                Reference::CompletedTask => {
                    seed_job(
                        &mut conn,
                        row_queue,
                        &JobSeed {
                            status: "Done",
                            attempts: 1,
                            max_attempts: 3,
                            owner: Some(&worker),
                            done_at_age_secs: Some(age),
                            run_at_age_secs: age,
                            last_result: None,
                        },
                    )?;
                }
            }
            let mut peer = pool.get().map_err(|e| e.to_string())?;
            // Everything between BEGIN and ROLLBACK runs in a closure so the
            // peer's transaction is rolled back on every path: a connection
            // handed back to the shared pool inside an open transaction would
            // poison later scenarios.
            let held = |peer: &mut PgConnection, conn: &mut PgConnection| -> Result<usize, String> {
                if matches!(hold, Hold::HeldByPeer) {
                    sql_query("BEGIN")
                        .execute(peer)
                        .map_err(|e| e.to_string())?;
                    sql_query(
                        "SELECT 1 FROM apalis.workers WHERE id = $1 AND worker_type = $2 FOR KEY SHARE",
                    )
                    .bind::<Text, _>(&worker)
                    .bind::<Text, _>(row_queue)
                    .execute(peer)
                    .map_err(|e| e.to_string())?;
                }
                retention::prune_workers_batch(conn, &queue, Duration::from_secs(60), 100)
                    .map_err(|e| e.to_string())
            };
            let pruned = held(&mut peer, &mut conn);
            if matches!(hold, Hold::HeldByPeer) {
                sql_query("ROLLBACK")
                    .execute(&mut peer)
                    .map_err(|e| e.to_string())?;
            }
            let deleted = pruned?;
            let remaining = count_workers(&mut conn, row_queue)?;
            Ok(PruneRun { deleted, remaining })
        })
        .await
        .map_err(|e| e.to_string())?
    };
    cleanup_queue(pool.clone(), queue).await?;
    cleanup_queue(pool, other_queue).await?;
    run.map(Outcome::Completed)
}

fn pruned() -> impl Fn(&Result<Outcome<PruneRun>, String>) -> AssertionResult {
    observe::<PruneRun, _>("prune", |run| {
        if run.deleted == 1 && run.remaining == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected the registration to be deleted, got {run:?}"
            ))
        }
    })
}

fn retained() -> impl Fn(&Result<Outcome<PruneRun>, String>) -> AssertionResult {
    observe::<PruneRun, _>("prune", |run| {
        if run.deleted == 0 && run.remaining == 1 {
            Ok(())
        } else {
            Err(format!("expected the registration to be kept, got {run:?}"))
        }
    })
}

lets_expect! { #tokio_test
    expect(run_release(ownership, claim).await) as releasing_a_registration {
        let ownership = Ownership::Owned;
        let claim = Claim::None;
        to releases_the_name_for_an_immediate_successor {
            released(0),
            registration_released(),
            successor("registered")
        }
        when the_worker_still_runs_a_task_with_retry_budget {
            let claim = Claim::RunningWithBudget;
            to hands_the_task_back_and_consumes_one_attempt {
                released(1),
                job_handed_back("Pending", 1),
                registration_released(),
                successor("registered")
            }
        }
        when the_worker_still_holds_a_queued_task {
            let claim = Claim::QueuedWithBudget;
            to hands_the_task_back_without_consuming_an_attempt {
                released(1),
                job_handed_back_untouched(1),
                registration_released()
            }
        }
        when the_task_has_no_retry_budget_left {
            let claim = Claim::RunningExhausted;
            to kills_the_task_with_the_release_marker {
                released(1),
                job_handed_back("Killed", 3),
                registration_released()
            }
        }
        when the_task_already_recorded_an_earlier_result {
            let claim = Claim::RunningWithEarlierResult;
            to hands_the_task_back_and_keeps_that_result {
                released(1),
                job_handed_back_with_earlier_result()
            }
        }
        when the_worker_only_owns_completed_history {
            let claim = Claim::Completed;
            to leaves_the_history_untouched {
                released(0),
                job_untouched("Done"),
                registration_released(),
                successor("registered")
            }
        }
        when the_same_name_runs_a_task_in_another_queue {
            let claim = Claim::ActiveInAnotherQueue;
            to releases_only_this_queue {
                released(0),
                job_untouched("Running"),
                registration_released()
            }
        }
        when no_registration_exists {
            let ownership = Ownership::Absent;
            to reports_the_missing_registration {
                not_registered(),
                registration_absent(),
                successor("registered")
            }
        }
        when another_storage_owns_the_name {
            let ownership = Ownership::Foreign;
            to leaves_that_registration_and_its_claims_alone {
                not_registered(),
                registration_untouched(Some("other-token")),
                successor("refused")
            }
            when that_storage_runs_a_task {
                let claim = Claim::RunningWithBudget;
                to leaves_that_registration_and_its_claims_alone {
                    not_registered(),
                    job_untouched("Running"),
                    registration_untouched(Some("other-token"))
                }
            }
        }
        when a_token_free_registration_holds_the_name {
            let ownership = Ownership::TokenFree;
            to leaves_that_registration_alone {
                not_registered(),
                registration_untouched(None),
                successor("refused")
            }
        }
    }

    expect(run_purge(row, age, window, scope).await) as purging_terminal_tasks {
        let row = Row::Done;
        let age = Age::Old;
        let window = Window::OneMinute;
        let scope = Scope::SameQueue;
        to deletes_a_completed_task_older_than_the_window { purged() }
        when the_task_completed_inside_the_window {
            let age = Age::Fresh;
            to keeps_it { kept() }
            when the_window_is_zero {
                let window = Window::Zero;
                to deletes_it { purged() }
            }
        }
        when the_task_was_killed {
            let row = Row::Killed;
            to deletes_it { purged() }
        }
        when the_task_failed_with_no_retry_budget_left {
            let row = Row::FailedExhausted;
            to deletes_it { purged() }
        }
        when the_task_failed_with_retry_budget_left {
            let row = Row::FailedWithBudget;
            to keeps_the_retryable_task { kept() }
        }
        when the_task_is_pending {
            let row = Row::Pending;
            to keeps_the_active_task { kept() }
        }
        when the_task_is_queued {
            let row = Row::Queued;
            to keeps_the_active_task { kept() }
        }
        when the_task_is_running {
            let row = Row::Running;
            to keeps_the_active_task { kept() }
        }
        when the_task_has_no_completion_time {
            let row = Row::DoneWithoutCompletionTime;
            to ages_it_by_its_schedule_instead { purged() }
            when that_schedule_is_inside_the_window {
                let age = Age::Fresh;
                to keeps_it { kept() }
            }
        }
        when the_task_belongs_to_another_queue {
            let scope = Scope::OtherQueue;
            to keeps_it { kept() }
        }
    }

    expect(run_batched_purge().await) as a_purge_larger_than_one_batch {
        to drains_the_backlog_across_batches { drains_in_batches() }
    }

    expect(run_batch_under_a_rescanning_plan(batch).await) as a_batch_under_a_plan_that_rescans_its_candidates {
        let batch = RescannedBatch::Purge;
        to deletes_no_more_than_its_limit { deletes_exactly(2) }
        when the_batch_prunes_registrations {
            let batch = RescannedBatch::Prune;
            to deletes_no_more_than_its_limit { deletes_exactly(2) }
        }
    }

    expect(run_prune(freshness, reference, scope, hold).await) as pruning_stale_registrations {
        let freshness = Freshness::Stale;
        let reference = Reference::None;
        let scope = Scope::SameQueue;
        let hold = Hold::Free;
        to deletes_a_stale_unreferenced_registration { pruned() }
        when the_registration_is_fresh {
            let freshness = Freshness::Fresh;
            to keeps_it { retained() }
        }
        when a_running_task_still_names_the_registration {
            let reference = Reference::ActiveClaim;
            to keeps_it { retained() }
        }
        when a_completed_task_still_names_the_registration {
            let reference = Reference::CompletedTask;
            to keeps_it_until_that_task_is_purged { retained() }
        }
        when the_registration_belongs_to_another_queue {
            let scope = Scope::OtherQueue;
            to keeps_it { retained() }
        }
        when another_transaction_holds_the_registration {
            let hold = Hold::HeldByPeer;
            to skips_it_without_waiting { retained() }
        }
    }
}
