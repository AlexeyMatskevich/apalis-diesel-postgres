use apalis_core::worker::context::WorkerContext;
use diesel::{
    Connection, RunQueryDsl, sql_query,
    sql_types::{Array, BigInt, Integer, Jsonb, Nullable, Text},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use ulid::Ulid;

use crate::{
    CompactType, Config, Error, PgPool, PgTask, PgTaskId,
    queries::{claimed_task_row, clamp_i32, with_conn},
};

/// SQL predicate that identifies rows eligible for a fresh claim from a queue:
/// either `Pending` or `Failed`, with retry budget remaining in both states.
/// Centralised here so `fetch_next` (poll path) and `queue_by_id` (notify
/// path) agree on which rows are considered claimable — drift between the two
/// historically allowed retries to "leak" through one path but not the other.
///
/// `lock_task` deliberately uses a *superset* of this predicate to remain
/// idempotent for the same worker (already-`Queued`/`Running` rows it owns),
/// so it does not share this constant.
const CLAIMABLE_PREDICATE: &str = "(status IN ('Pending', 'Failed') AND attempts < max_attempts)";

pub(crate) fn fetch_next(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
    lease_token: Option<Arc<str>>,
) -> impl Future<Output = Result<Vec<PgTask<CompactType>>, Error>> + Send {
    with_claim_conn(
        pool,
        "claiming polled tasks",
        |tasks: &Vec<_>| !tasks.is_empty(),
        move |conn| {
            require_worker(
                conn,
                worker.name(),
                config.queue().as_ref(),
                lease_token.as_deref(),
            )?;
            // `UPDATE ... FROM cte ... RETURNING` does not preserve the CTE's
            // ordering, so we wrap the UPDATE in an outer SELECT that re-applies
            // `ORDER BY priority DESC, run_at ASC`. This pushes the sort into
            // PostgreSQL (which already has the values in a tuplestore for the
            // RETURNING) instead of doing it in Rust after fetching the rows.
            let rows: Vec<crate::models::JobRow> = sql_query(format!(
                "WITH next_jobs AS (
                 SELECT id
                 FROM apalis.jobs
                 WHERE {CLAIMABLE_PREDICATE}
                     AND run_at <= statement_timestamp()
                     AND job_type = $2
                 ORDER BY priority DESC, run_at ASC
                 LIMIT $3
                 FOR UPDATE SKIP LOCKED
             ),
             updated AS (
                 -- H4: dequeue + lock used to be two round-trips
                 -- (fetch_next → Queued, then LockTaskService → Running).
                 -- Transition straight to `Running` in this CTE so the
                 -- subsequent `LockTaskService` call becomes idempotent
                 -- (`lock_task` accepts already-Running rows owned by the
                 -- same worker) and no second round-trip is needed per job.
                 UPDATE apalis.jobs
                 SET status = 'Running',
                     lock_by = $1,
                     lock_at = date_trunc('second', statement_timestamp()),
                     done_at = NULL
                 FROM next_jobs
                 WHERE apalis.jobs.id = next_jobs.id
                 RETURNING apalis.jobs.*
             )
             SELECT * FROM updated
             ORDER BY priority DESC, run_at ASC"
            ))
            .bind::<Text, _>(worker.name())
            .bind::<Text, _>(config.queue().to_string())
            .bind::<Integer, _>(clamp_i32(config.buffer_size().max(1)))
            .load(conn)
            .map_err(Error::database("fetching queued jobs"))?;
            claimed_tasks(conn, rows)
        },
    )
}

pub(crate) fn queue_by_id(
    pool: PgPool,
    queue: String,
    ids: Vec<String>,
    worker_id: String,
    lease_token: Option<Arc<str>>,
) -> impl Future<Output = Result<Vec<PgTask<CompactType>>, Error>> + Send {
    with_claim_conn(
        pool,
        "claiming notified tasks",
        |tasks: &Vec<_>| !tasks.is_empty(),
        move |conn| {
            require_worker(conn, &worker_id, &queue, lease_token.as_deref())?;
            // Mirror `fetch_next`'s eligibility (`Pending` OR retryable `Failed`)
            // so NOTIFY wakeups also pick up retried jobs; use `FOR UPDATE SKIP
            // LOCKED` to avoid serializing on rows another consumer is claiming.
            // `UPDATE ... RETURNING` does not preserve the CTE's ordering, so we
            // wrap it in an outer SELECT that re-applies the sort in SQL — same
            // pattern as `fetch_next`.
            let rows: Vec<crate::models::JobRow> = sql_query(format!(
                "WITH candidates AS (
                 SELECT id
                 FROM apalis.jobs
                 WHERE {CLAIMABLE_PREDICATE}
                     AND run_at <= statement_timestamp()
                     AND job_type = $2
                     AND id = ANY($3)
                 ORDER BY priority DESC, run_at ASC
                 FOR UPDATE SKIP LOCKED
             ),
             updated AS (
                 UPDATE apalis.jobs
                 SET status = 'Running',
                     lock_at = date_trunc('second', statement_timestamp()),
                     lock_by = $1,
                     done_at = NULL
                 FROM candidates
                 WHERE apalis.jobs.id = candidates.id
                 RETURNING apalis.jobs.*
             )
             SELECT * FROM updated
             ORDER BY priority DESC, run_at ASC"
            ))
            .bind::<Text, _>(worker_id)
            .bind::<Text, _>(queue)
            .bind::<Array<Text>, _>(ids)
            .load(conn)
            .map_err(Error::database("claiming notified jobs"))?;
            claimed_tasks(conn, rows)
        },
    )
}

/// Release a row the dequeue SQL already claimed as `Running` but whose
/// payload failed to decode. Without this, a poisoned payload (codec drift, a
/// third-party insert) would strand the row in `Running` for as long as the
/// claiming worker keeps heartbeating: ack requires a decoded task, and
/// orphan recovery only reclaims rows of *stale* workers. Failing the attempt
/// instead routes the row through the normal retry budget — it stays
/// claimable (`Failed` with attempts left) until the budget is exhausted,
/// then turns terminal `Killed`, the same convention as `reenqueue_orphaned`.
///
/// The predicate pins the caller's exact claim epoch, mirroring `ack_task`:
/// `status`/`lock_by` alone would let a delayed release fire on a row that
/// was orphan-swept and re-claimed in the meantime (same worker name, new
/// claim), so `lock_at` and `attempts` from the claimed task must match too.
/// An ack, a sweep, or a re-claim that raced this call all leave the row
/// untouched.
pub(crate) fn fail_undecodable_task(
    pool: PgPool,
    task_id: PgTaskId,
    worker_id: String,
    lock_at: i64,
    attempts: i32,
    error: String,
    lease_token: Option<Arc<str>>,
) -> impl Future<Output = Result<usize, Error>> + Send {
    with_conn(pool, move |conn| {
        conn.transaction(|conn| {
            if let Some(token) = lease_token.as_deref() {
                let Some(queue) = task_queue(
                    conn,
                    &task_id.to_string(),
                    None,
                    "locating undecodable task",
                )?
                else {
                    return Ok(0);
                };
                if !super::worker::lock_current_worker(conn, &worker_id, &queue, Some(token))? {
                    return Ok(0);
                }
            }
            // Same externally-tagged `Result<O, String>` JSON shape as ack's
            // `last_result`, so readers observe one format for every failure path.
            let result = serde_json::json!({ "Err": crate::ack::truncate_error_payload(error) });
            let count = sql_query(
                // `attempts::bigint` promotes the arithmetic so a corrupt row at
                // i32::MAX cannot overflow the `+ 1` (which PostgreSQL rejects as
                // "integer out of range"); LEAST re-bounds the result to
                // `max_attempts`, so it fits the int column again.
                "UPDATE apalis.jobs
             SET status = CASE
                     WHEN attempts::bigint + 1 >= max_attempts THEN 'Killed'
                     ELSE 'Failed'
                 END,
                 attempts = LEAST(attempts::bigint + 1, max_attempts),
                 done_at = clock_timestamp(),
                 last_result = $3
             WHERE id = $1
                 AND status = 'Running'
                 AND lock_by = $2
                 AND lock_at = to_timestamp($4::double precision)
                 AND attempts = $5",
            )
            .bind::<Text, _>(task_id.to_string())
            .bind::<Text, _>(worker_id)
            .bind::<Jsonb, _>(result)
            .bind::<BigInt, _>(lock_at)
            .bind::<Integer, _>(attempts)
            .execute(conn)
            .map_err(Error::database("failing undecodable task"))?;
            Ok(count)
        })
    })
}

pub(crate) fn lock_task(
    pool: PgPool,
    task_id: Ulid,
    worker_id: String,
    queue: Option<String>,
) -> impl Future<Output = Result<PgTask<CompactType>, Error>> + Send {
    lock_task_with_token(pool, task_id, worker_id, queue, None)
}

pub(crate) fn lock_task_with_token(
    pool: PgPool,
    task_id: Ulid,
    worker_id: String,
    queue: Option<String>,
    lease_token: Option<Arc<str>>,
) -> impl Future<Output = Result<PgTask<CompactType>, Error>> + Send {
    with_claim_conn(
        pool,
        "locking task",
        |_| true,
        move |conn| {
            let task_id = task_id.to_string();
            if let Some(scoped) =
                task_queue(conn, &task_id, queue.as_deref(), "locating task to lock")?
            {
                require_worker(conn, &worker_id, &scoped, lease_token.as_deref())?;
            }

            let mut rows: Vec<crate::models::JobRow> = sql_query(
                "UPDATE apalis.jobs
             SET status = 'Running',
                 lock_at = CASE
                     WHEN status IN ('Queued', 'Running')
                          AND lock_by = $1 AND lock_at IS NOT NULL THEN lock_at
                     ELSE date_trunc('second', statement_timestamp())
                 END,
                 lock_by = $1,
                 done_at = NULL
             WHERE id = $2
                 AND ($3 IS NULL OR job_type = $3)
                 AND run_at <= statement_timestamp()
                 AND (
                     (status = 'Pending' AND attempts < max_attempts)
                     OR (status = 'Queued' AND lock_by = $1)
                     OR (status = 'Running' AND lock_by = $1)
                     OR (status = 'Failed' AND attempts < max_attempts)
                 )
             RETURNING *",
            )
            .bind::<Text, _>(worker_id)
            .bind::<Text, _>(&task_id)
            .bind::<Nullable<Text>, _>(queue.clone())
            .load(conn)
            .map_err(Error::database("locking task"))?;
            if rows.is_empty() {
                // Only the queue-scoped call (`$3 = job_type`) can fail because the
                // task lives in a different queue; the unscoped call (`$3 IS NULL`)
                // locks across queues, so its hint must not list that reason.
                let hint = if queue.is_some() {
                    "the task may be delayed, have exhausted its retry budget, be locked by another worker, be completed, or belong to another queue"
                } else {
                    "the task may be delayed, have exhausted its retry budget, be locked by another worker, or be completed"
                };
                Err(Error::task_not_found("locking task", task_id, queue, hint))
            } else {
                claimed_task_row(rows.remove(0))
            }
        },
    )
}

/// The flag crosses the blocking boundary because user instrumentation may panic
/// after COMMIT. A returned SQL/runtime error does not prove the claim rolled back.
async fn with_claim_conn<T, F, P>(
    pool: PgPool,
    operation: &'static str,
    has_claims: P,
    work: F,
) -> Result<T, Error>
where
    T: Send + 'static,
    F: FnOnce(&mut diesel::PgConnection) -> Result<T, Error> + Send + 'static,
    P: FnOnce(&T) -> bool + Send + 'static,
{
    let claimed = Arc::new(AtomicBool::new(false));
    let observed = claimed.clone();
    let result = with_conn(pool, move |conn| {
        conn.transaction(|conn| {
            let value = work(conn)?;
            claimed.store(has_claims(&value), Ordering::Release);
            Ok(value)
        })
    })
    .await;
    claim_outcome(result, observed.load(Ordering::Acquire), operation)
}

// Only these PostgreSQL server rejection kinds establish a rejected COMMIT.
// Unknown/transport errors and rollback composites do not prove the outcome;
// user instrumentation can also fail after a successful COMMIT.
fn known_commit_rejection(error: &Error) -> bool {
    use diesel::result::{DatabaseErrorKind as Kind, Error as DieselError};
    matches!(error,
        Error::Database { source: DieselError::DatabaseError(kind, _), .. }
        if matches!(kind, Kind::UniqueViolation | Kind::ForeignKeyViolation
            | Kind::SerializationFailure | Kind::ReadOnlyTransaction
            | Kind::NotNullViolation | Kind::CheckViolation
            | Kind::RestrictViolation | Kind::ExclusionViolation))
}

fn claim_outcome<T>(
    result: Result<T, Error>,
    claimed: bool,
    operation: &'static str,
) -> Result<T, Error> {
    match result {
        Err(source) if claimed && !known_commit_rejection(&source) => {
            Err(Error::ClaimOutcomeUnknown {
                operation,
                source: Box::new(source),
            })
        }
        other => other,
    }
}

/// The queue a task belongs to, when the task exists and, if `scope` is
/// given, belongs to that queue. The worker row for that queue is locked
/// afterwards, so this lookup precedes any worker lock; a single statement
/// cannot lock the worker row while still telling an absent task from an
/// absent registration.
fn task_queue(
    conn: &mut diesel::PgConnection,
    task_id: &str,
    scope: Option<&str>,
    operation: &'static str,
) -> Result<Option<String>, Error> {
    #[derive(diesel::QueryableByName)]
    struct TaskQueue {
        #[diesel(sql_type = Text)]
        job_type: String,
    }
    let rows = sql_query(
        "SELECT job_type FROM apalis.jobs WHERE id=$1 AND ($2::text IS NULL OR job_type=$2)",
    )
    .bind::<Text, _>(task_id)
    .bind::<Nullable<Text>, _>(scope)
    .load::<TaskQueue>(conn)
    .map_err(Error::database(operation))?;
    Ok(rows.into_iter().next().map(|row| row.job_type))
}

fn require_worker(
    conn: &mut diesel::PgConnection,
    worker: &str,
    queue: &str,
    token: Option<&str>,
) -> Result<(), Error> {
    use super::worker::Registration;
    // None is the explicitly trusted low-level fetcher compatibility surface.
    let registration = super::worker::worker_registration(conn, worker, queue, token)?;
    let hint = match (token, registration) {
        (None, _) | (Some(_), Registration::Current) => return Ok(()),
        (Some(_), Registration::Absent) => {
            "the worker is not registered for this queue; register it before claiming"
        }
        (Some(_), Registration::Replaced) => {
            "the worker registration was replaced; create a fresh storage"
        }
    };
    Err(Error::worker_not_registered(
        "claiming task",
        worker,
        queue.to_owned(),
        hint,
    ))
}

fn claimed_tasks(
    conn: &mut diesel::PgConnection,
    rows: Vec<crate::models::JobRow>,
) -> Result<Vec<PgTask<CompactType>>, Error> {
    let mut tasks = Vec::with_capacity(rows.len());
    for row in rows {
        let id = row.id.clone();
        match claimed_task_row(row) {
            Ok(task) => tasks.push(task),
            Err(error) => {
                // The transaction still owns this raw row. Structural corruption
                // cannot be repaired by retrying the same payload; isolate it
                // without discarding valid siblings from the committed batch.
                let result = serde_json::json!({"Err":crate::ack::truncate_error_payload(error.to_string())});
                sql_query("UPDATE apalis.jobs SET status='Killed', attempts=LEAST(attempts::bigint+1,max_attempts), done_at=clock_timestamp(),last_result=$2,lock_by=NULL,lock_at=NULL WHERE id=$1")
                    .bind::<Text,_>(id).bind::<Jsonb,_>(result).execute(conn)
                    .map_err(Error::database("quarantining malformed task"))?;
            }
        }
    }
    Ok(tasks)
}

#[cfg(test)]
mod commit_specs {
    use super::*;
    use lets_expect::*;
    #[derive(Clone, Copy)]
    enum Confirmation {
        Confirmed,
        ConnectionLost,
        InstrumentationPanicked,
    }
    fn observed(claimed: bool, confirmation: Confirmation) -> (&'static str, &'static str, bool) {
        let result = match confirmation {
            Confirmation::Confirmed => Ok(42),
            Confirmation::ConnectionLost => Err(Error::database("original claim commit")(
                diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::ClosedConnection,
                    Box::new("original connection loss".to_owned()),
                ),
            )),
            Confirmation::InstrumentationPanicked => Err(Error::Blocking(Box::new(
                std::io::Error::other("original instrumentation panic"),
            ))),
        };
        let result = claim_outcome(result, claimed, "test claim");
        let (kind, source, operation) = match result {
            Ok(42) => return ("confirmed", "none", true),
            Ok(_) => return ("unexpected value", "none", false),
            Err(Error::ClaimOutcomeUnknown { operation, source }) => {
                ("unknown", *source, operation == "test claim")
            }
            Err(source) => ("original", source, true),
        };
        let source = match source {
            Error::Database {
                source:
                    diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::ClosedConnection,
                        info,
                    ),
                operation,
            } if operation == "original claim commit"
                && info.message() == "original connection loss" =>
            {
                "database"
            }
            Error::Blocking(source) if source.to_string() == "original instrumentation panic" => {
                "blocking"
            }
            _ => "unexpected source",
        };
        (kind, source, operation)
    }
    #[derive(Clone, Copy)]
    enum CommitError {
        Server(diesel::result::DatabaseErrorKind),
        MissingResult,
        RollbackFailed,
    }

    fn typed_outcome(cause: CommitError) -> (bool, bool) {
        use diesel::result::{DatabaseErrorKind as Kind, Error as DriverError};
        let server =
            |kind| DriverError::DatabaseError(kind, Box::new("original server cause".to_owned()));
        let driver = match cause {
            CommitError::Server(kind) => server(kind),
            CommitError::MissingResult => DriverError::NotFound,
            CommitError::RollbackFailed => DriverError::RollbackErrorOnCommit {
                rollback_error: Box::new(server(Kind::ClosedConnection)),
                commit_error: Box::new(server(Kind::SerializationFailure)),
            },
        };
        let original = Error::database("original commit")(driver);
        let expected = format!("{original:?}");
        match claim_outcome::<()>(Err(original), true, "typed claim") {
            Err(Error::ClaimOutcomeUnknown { operation, source }) => (
                true,
                operation == "typed claim" && format!("{source:?}") == expected,
            ),
            Err(source) => (false, format!("{source:?}") == expected),
            Ok(()) => (false, false),
        }
    }

    fn preserves_known_rejection(actual: &(bool, bool)) -> AssertionResult {
        if *actual == (false, true) {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected original known rejection and full cause, observed {actual:?}"
            )]))
        }
    }
    fn preserves_unknown_outcome(actual: &(bool, bool)) -> AssertionResult {
        if *actual == (true, true) {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected unknown outcome and full cause, observed {actual:?}"
            )]))
        }
    }

    lets_expect! {
        expect(typed_outcome(cause)) as typed_claim_commit_outcome {
            let cause = CommitError::Server(diesel::result::DatabaseErrorKind::SerializationFailure);
            to preserves_the_known_serialization_rejection { preserves_known_rejection }
            when a_unique_constraint_is_violated {
                let cause = CommitError::Server(diesel::result::DatabaseErrorKind::UniqueViolation);
                to preserves_the_known_constraint_rejection { preserves_known_rejection }
            }
            when a_foreign_key_constraint_is_violated {
                let cause = CommitError::Server(diesel::result::DatabaseErrorKind::ForeignKeyViolation);
                to preserves_the_known_constraint_rejection { preserves_known_rejection }
            }
            when the_transaction_is_read_only {
                let cause = CommitError::Server(diesel::result::DatabaseErrorKind::ReadOnlyTransaction);
                to preserves_the_known_transaction_rejection { preserves_known_rejection }
            }
            when a_required_value_is_null {
                let cause = CommitError::Server(diesel::result::DatabaseErrorKind::NotNullViolation);
                to preserves_the_known_constraint_rejection { preserves_known_rejection }
            }
            when a_check_constraint_is_violated {
                let cause = CommitError::Server(diesel::result::DatabaseErrorKind::CheckViolation);
                to preserves_the_known_constraint_rejection { preserves_known_rejection }
            }
            when a_restrict_constraint_is_violated {
                let cause = CommitError::Server(diesel::result::DatabaseErrorKind::RestrictViolation);
                to preserves_the_known_constraint_rejection { preserves_known_rejection }
            }
            when an_exclusion_constraint_is_violated {
                let cause = CommitError::Server(diesel::result::DatabaseErrorKind::ExclusionViolation);
                to preserves_the_known_constraint_rejection { preserves_known_rejection }
            }
            when the_connection_is_closed {
                let cause = CommitError::Server(diesel::result::DatabaseErrorKind::ClosedConnection);
                to reports_an_unknown_outcome_and_preserves_its_cause { preserves_unknown_outcome }
            }
            when the_command_cannot_be_sent {
                let cause = CommitError::Server(diesel::result::DatabaseErrorKind::UnableToSendCommand);
                to reports_an_unknown_outcome_and_preserves_its_cause { preserves_unknown_outcome }
            }
            when the_database_error_kind_is_unknown {
                let cause = CommitError::Server(diesel::result::DatabaseErrorKind::Unknown);
                to reports_an_unknown_outcome_and_preserves_its_cause { preserves_unknown_outcome }
            }
            when the_driver_returns_no_result {
                let cause = CommitError::MissingResult;
                to reports_an_unknown_outcome_and_preserves_its_cause { preserves_unknown_outcome }
            }
            when rollback_also_fails {
                let cause = CommitError::RollbackFailed;
                to reports_an_unknown_outcome_and_preserves_both_causes { preserves_unknown_outcome }
            }
        }
    }
    lets_expect! {
        expect(observed(claimed,confirmation)) as claim_commit_confirmation {
            let claimed=true;let confirmation=Confirmation::Confirmed;
            to returns_the_confirmed_claim {equal(("confirmed","none",true))}
            when the_commit_connection_is_lost {let confirmation=Confirmation::ConnectionLost;
                to identifies_an_unknown_claim_and_preserves_the_database_cause {equal(("unknown","database",true))}
            }
            when instrumentation_panics_after_the_commit {let confirmation=Confirmation::InstrumentationPanicked;
                to identifies_an_unknown_claim_and_preserves_the_blocking_cause {equal(("unknown","blocking",true))}
            }
            when no_task_was_claimed {let claimed=false;
                to returns_the_confirmed_empty_result {equal(("confirmed","none",true))}
                when the_commit_connection_is_lost {let confirmation=Confirmation::ConnectionLost;
                    to preserves_the_database_error_without_retiring_a_claim {equal(("original","database",true))}
                }
                when instrumentation_panics_after_the_commit {let confirmation=Confirmation::InstrumentationPanicked;
                    to preserves_the_blocking_error_without_retiring_a_claim {equal(("original","blocking",true))}
                }
            }
        }
    }
}
