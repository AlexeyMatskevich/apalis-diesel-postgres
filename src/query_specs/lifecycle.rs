//! Database specifications for releasing a worker registration. SQL is used
//! only for fixtures and observations.

use crate::{PgPool, queries::worker, test_support as support};
use apalis_core::worker::context::WorkerContext;
use diesel::{
    PgConnection, QueryableByName, RunQueryDsl, sql_query,
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
            to hands_the_task_back_and_consumes_one_attempt {
                released(1),
                job_handed_back("Pending", 2),
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
}
