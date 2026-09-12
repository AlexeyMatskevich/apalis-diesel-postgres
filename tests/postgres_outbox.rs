//! Integration tests for the transactional-outbox API: `push_with_conn` and
//! `push_task_with_conn`. Each scenario verifies one branch of the contract
//! documented at `PostgresStorage::push_with_conn`:
//!
//! - the INSERT is part of the caller's transaction (commit → visible,
//!   rollback → absent),
//! - `push_task_with_conn` honours caller-supplied `PgTask<Args>` fields,
//! - an `idempotency_key` conflict rolls back via SAVEPOINT but leaves the
//!   outer transaction alive so business writes can still commit.
//!
//! Tests require `DATABASE_URL` unless explicitly run in optional database mode.
//! Every scenario initializes its own prerequisites before querying them.

#![cfg(feature = "tokio")]

mod support;

use support::{Outcome, observe, with_conn};

use std::time::{SystemTime, UNIX_EPOCH};

use apalis_diesel_postgres::{Config, Error as PgError, PgPool, PgTask, PgTaskId, PostgresStorage};
use apalis_sql::{DateTimeExt, context::SqlContext};
use diesel::{
    Connection, OptionalExtension, PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{Integer, Jsonb, Text, Timestamptz},
};
use lets_expect::{AssertionResult, *};
use ulid::Ulid;

// --------------------------------------------------------------------------
// scaffolding
// --------------------------------------------------------------------------

async fn test_pool() -> Result<Option<PgPool>, String> {
    let pool = support::shared_pool().await?;
    if let Some(pool) = pool.as_ref() {
        ensure_business_table(pool.clone()).await?;
    }
    Ok(pool)
}

async fn ensure_business_table(pool: PgPool) -> Result<(), String> {
    with_conn(pool, |conn| {
        conn.transaction::<_, diesel::result::Error, _>(|conn| {
            // CREATE TABLE IF NOT EXISTS alone races on a cold database.
            sql_query("SELECT pg_advisory_xact_lock(hashtext('apalis-outbox-test'), hashtext('business-marker'))")
                .execute(conn)?;
            sql_query("CREATE TABLE IF NOT EXISTS apalis_outbox_test_marker (key TEXT PRIMARY KEY, queue TEXT NOT NULL)")
                .execute(conn)?;
            Ok(())
        }).map_err(|e|e.to_string())
    }).await
}

async fn cleanup(pool: PgPool, queue: String) -> Result<(), String> {
    let q = queue.clone();
    with_conn(pool, move |conn| {
        sql_query("DELETE FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(&q)
            .execute(conn)
            .map_err(|e| e.to_string())?;
        sql_query("DELETE FROM apalis_outbox_test_marker WHERE queue = $1")
            .bind::<Text, _>(&q)
            .execute(conn)
            .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await
}

#[derive(QueryableByName, Debug)]
struct JobRow {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Integer)]
    priority: i32,
    #[diesel(sql_type = Integer)]
    max_attempts: i32,
    #[diesel(sql_type = Timestamptz)]
    run_at: apalis_sql::DateTime,
    #[diesel(sql_type = Jsonb)]
    metadata: serde_json::Value,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    idempotency_key: Option<String>,
}

#[derive(QueryableByName, Debug)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

#[derive(QueryableByName, Debug)]
struct PayloadRow {
    #[diesel(sql_type = Text)]
    payload: String,
}

fn fetch_job(conn: &mut PgConnection, queue: &str) -> Result<Option<JobRow>, String> {
    sql_query(
        "SELECT id, priority, max_attempts, run_at, metadata, idempotency_key
         FROM apalis.jobs WHERE job_type = $1",
    )
    .bind::<Text, _>(queue)
    .get_result::<JobRow>(conn)
    .optional()
    .map_err(|e| e.to_string())
}

fn count_jobs(conn: &mut PgConnection, queue: &str) -> Result<i64, String> {
    sql_query("SELECT COUNT(*)::bigint AS n FROM apalis.jobs WHERE job_type = $1")
        .bind::<Text, _>(queue)
        .get_result::<CountRow>(conn)
        .map(|row| row.n)
        .map_err(|e| e.to_string())
}

fn count_business(conn: &mut PgConnection, queue: &str) -> Result<i64, String> {
    sql_query("SELECT COUNT(*)::bigint AS n FROM apalis_outbox_test_marker WHERE queue = $1")
        .bind::<Text, _>(queue)
        .get_result::<CountRow>(conn)
        .map(|row| row.n)
        .map_err(|e| e.to_string())
}

// --------------------------------------------------------------------------
// Scenario 1: commit makes both the task and the business row visible.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct CommitRun {
    returned_id: String,
    db_job_id: String,
    db_jobs: i64,
    db_business: i64,
}

async fn run_commit_scenario() -> Result<Outcome<CommitRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-outbox-commit-{}", Ulid::new());
    let key = format!("marker-{queue}");
    cleanup(pool.clone(), queue.clone()).await?;

    let storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(1));
    let q = queue.clone();
    let k = key.clone();
    let pool_for_txn = pool.clone();
    let returned_id = tokio::task::spawn_blocking(move || -> Result<PgTaskId, String> {
        let mut conn = pool_for_txn.get().map_err(|e| e.to_string())?;
        conn.transaction::<_, PgError, _>(|c| {
            sql_query("INSERT INTO apalis_outbox_test_marker (key, queue) VALUES ($1, $2)")
                .bind::<Text, _>(&k)
                .bind::<Text, _>(&q)
                .execute(c)?;
            storage.push_with_conn(c, "payload".to_owned())
        })
        .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())??;

    let q2 = queue.clone();
    let observed = with_conn(pool.clone(), move |conn| {
        let job = fetch_job(conn, &q2)?
            .ok_or_else(|| "expected one job after commit, found none".to_owned())?;
        Ok::<_, String>((job.id, count_jobs(conn, &q2)?, count_business(conn, &q2)?))
    })
    .await?;

    cleanup(pool, queue).await?;
    Ok(Outcome::Completed(CommitRun {
        returned_id: returned_id.to_string(),
        db_job_id: observed.0,
        db_jobs: observed.1,
        db_business: observed.2,
    }))
}

fn commit_persists_one_job() -> impl Fn(&Result<Outcome<CommitRun>, String>) -> AssertionResult {
    observe("commit→job count", |run: &CommitRun| {
        if run.db_jobs == 1 {
            Ok(())
        } else {
            Err(format!("expected 1 job after commit, got {}", run.db_jobs))
        }
    })
}

fn commit_persists_one_business_row()
-> impl Fn(&Result<Outcome<CommitRun>, String>) -> AssertionResult {
    observe("commit→business row count", |run: &CommitRun| {
        if run.db_business == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected 1 business row after commit, got {}",
                run.db_business
            ))
        }
    })
}

fn commit_returns_id_matching_db() -> impl Fn(&Result<Outcome<CommitRun>, String>) -> AssertionResult
{
    observe("commit→returned id", |run: &CommitRun| {
        if run.returned_id == run.db_job_id {
            Ok(())
        } else {
            Err(format!(
                "returned id {:?} differs from DB id {:?}",
                run.returned_id, run.db_job_id
            ))
        }
    })
}

// --------------------------------------------------------------------------
// Scenario 1b: push_batch_with_conn inserts every task in one committed batch.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct BatchRun {
    returned_ids: usize,
    distinct_returned_ids: usize,
    db_jobs: i64,
    all_ids_present: bool,
    /// For each returned id, the `job` payload stored in `apalis.jobs` for that
    /// id. `returned[i]` must map to the row whose payload is `payload-{i}`,
    /// pinning the documented "submission order" contract.
    returned_payloads_in_order: Vec<Option<String>>,
}

const BATCH_SIZE: usize = 5;

async fn run_batch_commit_scenario() -> Result<Outcome<BatchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    // `cleanup` deletes from the business-marker table too, so ensure it exists
    // on a cold database (this scenario runs before any commit/rollback one).
    let queue = format!("apalis-outbox-batch-{}", Ulid::new());
    cleanup(pool.clone(), queue.clone()).await?;

    let storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(1));
    let payloads: Vec<String> = (0..BATCH_SIZE).map(|i| format!("payload-{i}")).collect();
    let pool_for_txn = pool.clone();
    let returned = tokio::task::spawn_blocking(move || -> Result<Vec<PgTaskId>, String> {
        let mut conn = pool_for_txn.get().map_err(|e| e.to_string())?;
        conn.transaction::<_, PgError, _>(|c| storage.push_batch_with_conn(c, payloads))
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())??;

    let q2 = queue.clone();
    let ids_for_check = returned.clone();
    let (db_jobs, all_ids_present, returned_payloads_in_order) =
        with_conn(pool.clone(), move |conn| {
            let count = count_jobs(conn, &q2)?;
            let mut present = true;
            // For each returned id, in the order it was returned, read back the
            // payload stored for that id. `job` is BYTEA holding the
            // JSON-encoded `String` (e.g. `"payload-0"`); decode it to UTF-8 so
            // the caller can compare `returned[i]` against `payload-{i}`.
            let mut payloads_in_order = Vec::with_capacity(ids_for_check.len());
            for id in &ids_for_check {
                let n = sql_query(
                    "SELECT COUNT(*)::bigint AS n FROM apalis.jobs WHERE id = $1 AND job_type = $2",
                )
                .bind::<Text, _>(id.to_string())
                .bind::<Text, _>(&q2)
                .get_result::<CountRow>(conn)
                .map(|r| r.n)
                .map_err(|e| e.to_string())?;
                if n != 1 {
                    present = false;
                }
                let payload = sql_query(
                    "SELECT convert_from(job, 'UTF8') AS payload
                     FROM apalis.jobs WHERE id = $1 AND job_type = $2",
                )
                .bind::<Text, _>(id.to_string())
                .bind::<Text, _>(&q2)
                .get_result::<PayloadRow>(conn)
                .optional()
                .map_err(|e| e.to_string())?
                .map(|row| row.payload);
                payloads_in_order.push(payload);
            }
            Ok::<_, String>((count, present, payloads_in_order))
        })
        .await?;

    cleanup(pool, queue).await?;
    let distinct: std::collections::HashSet<String> =
        returned.iter().map(ToString::to_string).collect();
    Ok(Outcome::Completed(BatchRun {
        returned_ids: returned.len(),
        distinct_returned_ids: distinct.len(),
        db_jobs,
        all_ids_present,
        returned_payloads_in_order,
    }))
}

fn batch_inserts_every_task() -> impl Fn(&Result<Outcome<BatchRun>, String>) -> AssertionResult {
    observe("batch→job count", |run: &BatchRun| {
        if run.db_jobs == BATCH_SIZE as i64 {
            Ok(())
        } else {
            Err(format!(
                "expected {BATCH_SIZE} jobs after the committed batch, got {}",
                run.db_jobs
            ))
        }
    })
}

fn batch_returns_distinct_ids_present_in_db()
-> impl Fn(&Result<Outcome<BatchRun>, String>) -> AssertionResult {
    observe("batch→returned ids", |run: &BatchRun| {
        if run.returned_ids != BATCH_SIZE || run.distinct_returned_ids != BATCH_SIZE {
            return Err(format!(
                "expected {BATCH_SIZE} distinct returned ids, got {} ({} distinct)",
                run.returned_ids, run.distinct_returned_ids
            ));
        }
        if run.all_ids_present {
            Ok(())
        } else {
            Err("a returned task id was not found in apalis.jobs".to_owned())
        }
    })
}

fn batch_returns_ids_in_submission_order()
-> impl Fn(&Result<Outcome<BatchRun>, String>) -> AssertionResult {
    observe("batch→submission order", |run: &BatchRun| {
        // The rustdoc contract is "returns the generated PgTaskId's in
        // submission order". Payloads are distinguishable (`payload-{i}`), so
        // the id returned at position `i` must be the row whose stored payload
        // is `payload-{i}`. A regression that sorts or shuffles the returned
        // ids before returning them would map `returned[i]` to the wrong row
        // here even though every id is still present in the table.
        if run.returned_payloads_in_order.len() != BATCH_SIZE {
            return Err(format!(
                "expected {BATCH_SIZE} payloads to check, got {}",
                run.returned_payloads_in_order.len()
            ));
        }
        for (i, payload) in run.returned_payloads_in_order.iter().enumerate() {
            // `job` holds the JSON-encoded String, e.g. `"payload-0"` (quotes
            // included).
            let expected = format!("\"payload-{i}\"");
            match payload {
                Some(actual) if *actual == expected => {}
                other => {
                    return Err(format!(
                        "returned id at position {i} maps to payload {other:?}, expected {expected:?}"
                    ));
                }
            }
        }
        Ok(())
    })
}

// --------------------------------------------------------------------------
// Scenario 2: rollback hides both the task and the business row.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct RollbackRun {
    push_result_was_ok: bool,
    db_jobs: i64,
    db_business: i64,
}

async fn run_rollback_scenario() -> Result<Outcome<RollbackRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-outbox-rollback-{}", Ulid::new());
    let key = format!("marker-{queue}");
    cleanup(pool.clone(), queue.clone()).await?;

    let storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(1));
    let q = queue.clone();
    let k = key.clone();
    let pool_for_txn = pool.clone();
    // The outer transaction returns an error from its closure so Diesel
    // rolls it back. We capture the inner `push_with_conn` result before
    // forcing the rollback to confirm the call itself was Ok at the time.
    let push_result_was_ok = tokio::task::spawn_blocking(move || -> Result<bool, String> {
        let mut conn = pool_for_txn.get().map_err(|e| e.to_string())?;
        let mut push_ok_observed = false;
        let txn_result: Result<(), diesel::result::Error> = conn.transaction(|c| {
            sql_query("INSERT INTO apalis_outbox_test_marker (key, queue) VALUES ($1, $2)")
                .bind::<Text, _>(&k)
                .bind::<Text, _>(&q)
                .execute(c)?;
            // The outer rollback must be the ONLY reason this transaction
            // aborts: a hidden `push_with_conn` failure would otherwise
            // produce the same `RollbackTransaction` error and silently
            // mask the broken path. Surface the push failure as a
            // distinct error variant so the assertion below can tell the
            // two cases apart.
            storage
                .push_with_conn(c, "payload".to_owned())
                .map_err(|e| {
                    diesel::result::Error::QueryBuilderError(
                        format!("push_with_conn failed during rollback test: {e}").into(),
                    )
                })?;
            push_ok_observed = true;
            // Now force the outer transaction to roll back.
            Err(diesel::result::Error::RollbackTransaction)
        });
        // The push call must have completed Ok before the forced rollback,
        // and the forced rollback must be the error we received.
        Ok(push_ok_observed
            && matches!(txn_result, Err(diesel::result::Error::RollbackTransaction)))
    })
    .await
    .map_err(|e| e.to_string())??;

    let q2 = queue.clone();
    let (db_jobs, db_business) = with_conn(pool.clone(), move |conn| {
        Ok::<_, String>((count_jobs(conn, &q2)?, count_business(conn, &q2)?))
    })
    .await?;

    cleanup(pool, queue).await?;
    Ok(Outcome::Completed(RollbackRun {
        push_result_was_ok,
        db_jobs,
        db_business,
    }))
}

fn rollback_call_succeeded_before_outer_rollback()
-> impl Fn(&Result<Outcome<RollbackRun>, String>) -> AssertionResult {
    observe("rollback→push ok", |run: &RollbackRun| {
        if run.push_result_was_ok {
            Ok(())
        } else {
            Err("the outer rollback did not take the expected RollbackTransaction path".into())
        }
    })
}

fn rollback_leaves_no_job() -> impl Fn(&Result<Outcome<RollbackRun>, String>) -> AssertionResult {
    observe("rollback→job count", |run: &RollbackRun| {
        if run.db_jobs == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected 0 jobs after rollback, got {}",
                run.db_jobs
            ))
        }
    })
}

fn rollback_leaves_no_business_row()
-> impl Fn(&Result<Outcome<RollbackRun>, String>) -> AssertionResult {
    observe("rollback→business row count", |run: &RollbackRun| {
        if run.db_business == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected 0 business rows after rollback, got {}",
                run.db_business
            ))
        }
    })
}

// --------------------------------------------------------------------------
// Scenario 3: push_task_with_conn honours caller-supplied PgTask<Args> fields.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct CustomRun {
    returned_id: String,
    db_job_id: String,
    db_priority: i32,
    db_max_attempts: i32,
    db_run_at_secs: i64,
    db_metadata: serde_json::Value,
    db_idempotency_key: Option<String>,
    expected_run_at_secs: i64,
    expected_id: String,
    expected_idempotency_key: String,
}

async fn run_custom_fields_scenario() -> Result<Outcome<CustomRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-outbox-custom-{}", Ulid::new());
    cleanup(pool.clone(), queue.clone()).await?;

    let storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(1));

    let preassigned_id = PgTaskId::new(Ulid::new());
    let expected_run_at_secs = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs()
        + 3_600) as i64;
    let mut expected_metadata = serde_json::Map::new();
    expected_metadata.insert(
        "reason".to_owned(),
        serde_json::Value::String("test".to_owned()),
    );
    expected_metadata.insert(
        "n".to_owned(),
        serde_json::Value::Number(serde_json::Number::from(7)),
    );

    let expected_idempotency_key = format!("idem-{queue}");
    let mut task = PgTask::<String>::new("payload".to_owned());
    task.parts.task_id = Some(preassigned_id);
    task.parts.run_at = expected_run_at_secs as u64;
    task.parts.idempotency_key = Some(expected_idempotency_key.clone());
    task.parts.ctx = SqlContext::new()
        .with_max_attempts(9)
        .with_priority(5)
        .with_meta(expected_metadata.clone());

    let storage_for_txn = storage.clone();
    let pool_for_txn = pool.clone();
    let returned_id = tokio::task::spawn_blocking(move || -> Result<PgTaskId, String> {
        let mut conn = pool_for_txn.get().map_err(|e| e.to_string())?;
        storage_for_txn
            .push_task_with_conn(&mut conn, task)
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())??;

    let q2 = queue.clone();
    let row = with_conn(pool.clone(), move |conn| {
        fetch_job(conn, &q2)?.ok_or_else(|| "expected one job, found none".to_owned())
    })
    .await?;

    let db_run_at_secs = row.run_at.to_unix_timestamp();

    cleanup(pool, queue).await?;
    Ok(Outcome::Completed(CustomRun {
        returned_id: returned_id.to_string(),
        db_job_id: row.id,
        db_priority: row.priority,
        db_max_attempts: row.max_attempts,
        db_run_at_secs,
        db_metadata: row.metadata,
        db_idempotency_key: row.idempotency_key,
        expected_run_at_secs,
        expected_id: preassigned_id.to_string(),
        expected_idempotency_key,
    }))
}

fn custom_returned_id_is_the_preassigned_one()
-> impl Fn(&Result<Outcome<CustomRun>, String>) -> AssertionResult {
    observe("custom→task_id", |run: &CustomRun| {
        // The API contract for `push_task_with_conn` is: when `task.parts.
        // task_id` is `Some`, that id is used verbatim and echoed back. A
        // regression that silently generates a fresh ULID would still satisfy
        // `returned_id == db_job_id` (it would just persist the wrong id),
        // so anchor the assertion on the caller's preassigned id directly.
        if run.returned_id == run.expected_id && run.db_job_id == run.expected_id {
            Ok(())
        } else {
            Err(format!(
                "expected returned id and DB id to both equal preassigned id ({:?}); got returned={:?} db={:?}",
                run.expected_id, run.returned_id, run.db_job_id
            ))
        }
    })
}

fn custom_priority_is_stored() -> impl Fn(&Result<Outcome<CustomRun>, String>) -> AssertionResult {
    observe("custom→priority", |run: &CustomRun| {
        if run.db_priority == 5 {
            Ok(())
        } else {
            Err(format!("expected priority=5, got {}", run.db_priority))
        }
    })
}

fn custom_max_attempts_is_stored() -> impl Fn(&Result<Outcome<CustomRun>, String>) -> AssertionResult
{
    observe("custom→max_attempts", |run: &CustomRun| {
        if run.db_max_attempts == 9 {
            Ok(())
        } else {
            Err(format!(
                "expected max_attempts=9, got {}",
                run.db_max_attempts
            ))
        }
    })
}

fn custom_run_at_is_stored() -> impl Fn(&Result<Outcome<CustomRun>, String>) -> AssertionResult {
    observe("custom→run_at", |run: &CustomRun| {
        if run.db_run_at_secs == run.expected_run_at_secs {
            Ok(())
        } else {
            Err(format!(
                "expected run_at={} sec, got {}",
                run.expected_run_at_secs, run.db_run_at_secs
            ))
        }
    })
}

fn custom_metadata_is_stored() -> impl Fn(&Result<Outcome<CustomRun>, String>) -> AssertionResult {
    observe("custom→metadata", |run: &CustomRun| {
        let expected = serde_json::json!({ "reason": "test", "n": 7 });
        if run.db_metadata == expected {
            Ok(())
        } else {
            Err(format!(
                "expected metadata={expected}, got {}",
                run.db_metadata
            ))
        }
    })
}

fn custom_idempotency_key_is_stored()
-> impl Fn(&Result<Outcome<CustomRun>, String>) -> AssertionResult {
    observe("custom→idempotency_key", |run: &CustomRun| {
        // Exact equality: the value is fully known at construction time, so a
        // prefix check would miss truncation or trailing corruption.
        if run.db_idempotency_key.as_deref() == Some(run.expected_idempotency_key.as_str()) {
            Ok(())
        } else {
            Err(format!(
                "expected idempotency_key {:?}, got {:?}",
                run.expected_idempotency_key, run.db_idempotency_key
            ))
        }
    })
}

// --------------------------------------------------------------------------
// Scenario 3b: push_tasks_with_conn carries each task's distinct custom fields
// through the batch. This is the ONLY outbox method that maps per-task
// idempotency_key / priority / run_at / max_attempts / metadata / task_id
// inside a single batch, so it must be pinned separately from the single-task
// `push_task_with_conn` path: a regression that reuses the first task's context
// for every row, or mis-associates the returned ids with the wrong rows, is
// only visible when the two tasks differ in every field.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct BatchCustomTaskExpectation {
    id: String,
    priority: i32,
    max_attempts: i32,
    run_at_secs: i64,
    metadata: serde_json::Value,
    idempotency_key: String,
}

#[derive(Debug)]
struct BatchCustomTaskObserved {
    priority: i32,
    max_attempts: i32,
    run_at_secs: i64,
    metadata: serde_json::Value,
    idempotency_key: Option<String>,
}

#[derive(Debug)]
struct BatchCustomRun {
    /// The ids returned by `push_tasks_with_conn`, in submission order.
    returned_ids: Vec<String>,
    /// What each task was constructed with, in submission order.
    expected: Vec<BatchCustomTaskExpectation>,
    /// The row read back keyed by the *returned* id at the same position, so a
    /// mismatch exposes both mis-carried fields and mis-associated ids.
    observed_by_returned_id: Vec<Option<BatchCustomTaskObserved>>,
}

async fn run_batch_custom_fields_scenario() -> Result<Outcome<BatchCustomRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    // `cleanup` deletes from the business-marker table too; lets_expect
    // expands each `to` into its own test, and test execution order is not
    // guaranteed, so this scenario cannot rely on another one having already
    // created the table on a cold database.
    let queue = format!("apalis-outbox-batch-custom-{}", Ulid::new());
    cleanup(pool.clone(), queue.clone()).await?;

    let storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(1));

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| e.to_string())?
        .as_secs() as i64;

    // Two fully-populated tasks that differ in EVERY custom field.
    let id_a = PgTaskId::new(Ulid::new());
    let id_b = PgTaskId::new(Ulid::new());
    let run_at_a = now_secs + 3_600;
    let run_at_b = now_secs + 7_200;
    let meta_a = serde_json::json!({ "which": "a", "n": 1 });
    let meta_b = serde_json::json!({ "which": "b", "n": 2 });
    let idem_a = format!("idem-a-{queue}");
    let idem_b = format!("idem-b-{queue}");

    let mut task_a = PgTask::<String>::new("payload-a".to_owned());
    task_a.parts.task_id = Some(id_a);
    task_a.parts.run_at = run_at_a as u64;
    task_a.parts.idempotency_key = Some(idem_a.clone());
    task_a.parts.ctx = SqlContext::new()
        .with_max_attempts(3)
        .with_priority(1)
        .with_meta(meta_a.as_object().unwrap().clone());

    let mut task_b = PgTask::<String>::new("payload-b".to_owned());
    task_b.parts.task_id = Some(id_b);
    task_b.parts.run_at = run_at_b as u64;
    task_b.parts.idempotency_key = Some(idem_b.clone());
    task_b.parts.ctx = SqlContext::new()
        .with_max_attempts(8)
        .with_priority(6)
        .with_meta(meta_b.as_object().unwrap().clone());

    let expected = vec![
        BatchCustomTaskExpectation {
            id: id_a.to_string(),
            priority: 1,
            max_attempts: 3,
            run_at_secs: run_at_a,
            metadata: meta_a.clone(),
            idempotency_key: idem_a,
        },
        BatchCustomTaskExpectation {
            id: id_b.to_string(),
            priority: 6,
            max_attempts: 8,
            run_at_secs: run_at_b,
            metadata: meta_b.clone(),
            idempotency_key: idem_b,
        },
    ];

    let storage_for_txn = storage.clone();
    let pool_for_txn = pool.clone();
    let returned = tokio::task::spawn_blocking(move || -> Result<Vec<PgTaskId>, String> {
        let mut conn = pool_for_txn.get().map_err(|e| e.to_string())?;
        conn.transaction::<_, PgError, _>(|c| {
            storage_for_txn.push_tasks_with_conn(c, vec![task_a, task_b])
        })
        .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())??;

    let returned_for_read = returned.clone();
    let q2 = queue.clone();
    let observed_by_returned_id = with_conn(pool.clone(), move |conn| {
        let mut rows = Vec::with_capacity(returned_for_read.len());
        for id in &returned_for_read {
            let row = sql_query(
                "SELECT id, priority, max_attempts, run_at, metadata, idempotency_key
                 FROM apalis.jobs WHERE id = $1 AND job_type = $2",
            )
            .bind::<Text, _>(id.to_string())
            .bind::<Text, _>(&q2)
            .get_result::<JobRow>(conn)
            .optional()
            .map_err(|e| e.to_string())?
            .map(|r| BatchCustomTaskObserved {
                priority: r.priority,
                max_attempts: r.max_attempts,
                run_at_secs: r.run_at.to_unix_timestamp(),
                metadata: r.metadata,
                idempotency_key: r.idempotency_key,
            });
            rows.push(row);
        }
        Ok::<_, String>(rows)
    })
    .await?;

    cleanup(pool, queue).await?;
    Ok(Outcome::Completed(BatchCustomRun {
        returned_ids: returned.iter().map(ToString::to_string).collect(),
        expected,
        observed_by_returned_id,
    }))
}

fn batch_custom_returns_each_task_id_in_order()
-> impl Fn(&Result<Outcome<BatchCustomRun>, String>) -> AssertionResult {
    observe("batch-custom→task ids", |run: &BatchCustomRun| {
        let expected_ids: Vec<&str> = run.expected.iter().map(|e| e.id.as_str()).collect();
        let returned_ids: Vec<&str> = run.returned_ids.iter().map(String::as_str).collect();
        if returned_ids == expected_ids {
            Ok(())
        } else {
            Err(format!(
                "expected returned ids {expected_ids:?} in submission order, got {returned_ids:?}"
            ))
        }
    })
}

fn batch_custom_carries_each_tasks_fields()
-> impl Fn(&Result<Outcome<BatchCustomRun>, String>) -> AssertionResult {
    observe("batch-custom→per-task fields", |run: &BatchCustomRun| {
        for (i, expected) in run.expected.iter().enumerate() {
            let observed = run
                .observed_by_returned_id
                .get(i)
                .ok_or_else(|| format!("no row read back for position {i}"))?
                .as_ref()
                .ok_or_else(|| {
                    format!(
                        "returned id {:?} (position {i}) had no row in apalis.jobs",
                        expected.id
                    )
                })?;
            if observed.priority != expected.priority {
                return Err(format!(
                    "task {i}: expected priority {}, got {}",
                    expected.priority, observed.priority
                ));
            }
            if observed.max_attempts != expected.max_attempts {
                return Err(format!(
                    "task {i}: expected max_attempts {}, got {}",
                    expected.max_attempts, observed.max_attempts
                ));
            }
            if observed.run_at_secs != expected.run_at_secs {
                return Err(format!(
                    "task {i}: expected run_at {} sec, got {}",
                    expected.run_at_secs, observed.run_at_secs
                ));
            }
            if observed.metadata != expected.metadata {
                return Err(format!(
                    "task {i}: expected metadata {}, got {}",
                    expected.metadata, observed.metadata
                ));
            }
            if observed.idempotency_key.as_deref() != Some(expected.idempotency_key.as_str()) {
                return Err(format!(
                    "task {i}: expected idempotency_key {:?}, got {:?}",
                    expected.idempotency_key, observed.idempotency_key
                ));
            }
        }
        Ok(())
    })
}

// --------------------------------------------------------------------------
// Scenario 3c: an empty batch is a documented no-op that returns an empty
// vector, for both `push_batch_with_conn` and `push_tasks_with_conn`. The
// implementation short-circuits in `prepare_batch` before opening any
// transaction, so this pins that neither method inserts rows nor returns a
// non-empty vector on an empty iterator.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct EmptyBatchRun {
    push_batch_returned_len: usize,
    push_tasks_returned_len: usize,
    db_jobs: i64,
}

async fn run_empty_batch_scenario() -> Result<Outcome<EmptyBatchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    // `cleanup` deletes from the business-marker table too; see the comment
    // in `run_batch_custom_fields_scenario`.
    let queue = format!("apalis-outbox-empty-{}", Ulid::new());
    cleanup(pool.clone(), queue.clone()).await?;

    let storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(1));

    let storage_for_txn = storage.clone();
    let pool_for_txn = pool.clone();
    let (push_batch_returned_len, push_tasks_returned_len) =
        tokio::task::spawn_blocking(move || -> Result<(usize, usize), String> {
            let mut conn = pool_for_txn.get().map_err(|e| e.to_string())?;
            // Empty `Args` iterator → push_batch_with_conn.
            let batch_ids: Vec<PgTaskId> = storage_for_txn
                .push_batch_with_conn(&mut conn, Vec::<String>::new())
                .map_err(|e| e.to_string())?;
            // Empty `PgTask` iterator → push_tasks_with_conn.
            let tasks_ids: Vec<PgTaskId> = storage_for_txn
                .push_tasks_with_conn(&mut conn, Vec::<PgTask<String>>::new())
                .map_err(|e| e.to_string())?;
            Ok((batch_ids.len(), tasks_ids.len()))
        })
        .await
        .map_err(|e| e.to_string())??;

    let q2 = queue.clone();
    let db_jobs = with_conn(pool.clone(), move |conn| count_jobs(conn, &q2)).await?;

    cleanup(pool, queue).await?;
    Ok(Outcome::Completed(EmptyBatchRun {
        push_batch_returned_len,
        push_tasks_returned_len,
        db_jobs,
    }))
}

fn empty_push_batch_returns_empty_vec()
-> impl Fn(&Result<Outcome<EmptyBatchRun>, String>) -> AssertionResult {
    observe("empty→push_batch len", |run: &EmptyBatchRun| {
        if run.push_batch_returned_len == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected push_batch_with_conn to return an empty vec, got {} ids",
                run.push_batch_returned_len
            ))
        }
    })
}

fn empty_push_tasks_returns_empty_vec()
-> impl Fn(&Result<Outcome<EmptyBatchRun>, String>) -> AssertionResult {
    observe("empty→push_tasks len", |run: &EmptyBatchRun| {
        if run.push_tasks_returned_len == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected push_tasks_with_conn to return an empty vec, got {} ids",
                run.push_tasks_returned_len
            ))
        }
    })
}

fn empty_batch_inserts_no_rows()
-> impl Fn(&Result<Outcome<EmptyBatchRun>, String>) -> AssertionResult {
    observe("empty→job count", |run: &EmptyBatchRun| {
        if run.db_jobs == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected no jobs inserted by empty batches, got {}",
                run.db_jobs
            ))
        }
    })
}

// --------------------------------------------------------------------------
// Scenario 3d: every outbox method documents `Error::Decode` when the codec
// rejects a task's args. With a codec whose `encode` always fails, each method
// must surface `Error::Decode` — and it must do so before touching the
// database, so no rows are inserted.
// --------------------------------------------------------------------------

/// A codec that accepts only the explicit valid prefix and otherwise rejects,
/// exercising the documented
/// `Error::Decode` branch of the outbox methods. `decode` is never reached in
/// these tests but is required by the trait.
#[derive(Debug, Clone, Default)]
struct FailingEncodeCodec;

#[derive(Debug)]
struct FailingEncodeError;

impl std::fmt::Display for FailingEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failing codec: rejected arguments")
    }
}

impl std::error::Error for FailingEncodeError {}

impl apalis_core::backend::codec::Codec<String> for FailingEncodeCodec {
    type Error = FailingEncodeError;
    type Compact = Vec<u8>;

    fn encode(val: &String) -> Result<Self::Compact, Self::Error> {
        if val == "valid" {
            Ok(b"\"valid\"".to_vec())
        } else {
            Err(FailingEncodeError)
        }
    }

    fn decode(_val: &Self::Compact) -> Result<String, Self::Error> {
        Err(FailingEncodeError)
    }
}

#[derive(Debug)]
struct EncodeFailureRun {
    push_with_conn_was_decode: bool,
    push_task_with_conn_was_decode: bool,
    push_batch_with_conn_was_decode: bool,
    push_tasks_with_conn_was_decode: bool,
    db_jobs: i64,
}

async fn run_encode_failure_scenario() -> Result<Outcome<EncodeFailureRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    // `cleanup` deletes from the business-marker table too; see the comment
    // in `run_batch_custom_fields_scenario`.
    let queue = format!("apalis-outbox-encode-fail-{}", Ulid::new());
    cleanup(pool.clone(), queue.clone()).await?;

    let storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(1))
            .with_codec::<FailingEncodeCodec>();

    let pool_for_txn = pool.clone();
    let observed =
        tokio::task::spawn_blocking(move || -> Result<(bool, bool, bool, bool), String> {
            let mut conn = pool_for_txn.get().map_err(|e| e.to_string())?;
            fn is_decode<T>(r: Result<T, PgError>) -> bool {
                matches!(r, Err(PgError::Decode(_)))
            }

            let a = is_decode(storage.push_with_conn(&mut conn, "x".to_owned()));

            let mut task = PgTask::<String>::new("x".to_owned());
            task.parts.task_id = Some(PgTaskId::new(Ulid::new()));
            let b = is_decode(storage.push_task_with_conn(&mut conn, task));

            let c = is_decode(storage.push_batch_with_conn(&mut conn, vec!["x".to_owned()]));

            let mut task2 = PgTask::<String>::new("x".to_owned());
            task2.parts.task_id = Some(PgTaskId::new(Ulid::new()));
            let d = is_decode(storage.push_tasks_with_conn(&mut conn, vec![task2]));

            Ok((a, b, c, d))
        })
        .await
        .map_err(|e| e.to_string())??;

    let q2 = queue.clone();
    let db_jobs = with_conn(pool.clone(), move |conn| count_jobs(conn, &q2)).await?;

    cleanup(pool, queue).await?;
    Ok(Outcome::Completed(EncodeFailureRun {
        push_with_conn_was_decode: observed.0,
        push_task_with_conn_was_decode: observed.1,
        push_batch_with_conn_was_decode: observed.2,
        push_tasks_with_conn_was_decode: observed.3,
        db_jobs,
    }))
}

fn encode_failure_surfaces_decode_on_every_method()
-> impl Fn(&Result<Outcome<EncodeFailureRun>, String>) -> AssertionResult {
    observe("encode-fail→Error::Decode", |run: &EncodeFailureRun| {
        let mut wrong = Vec::new();
        if !run.push_with_conn_was_decode {
            wrong.push("push_with_conn");
        }
        if !run.push_task_with_conn_was_decode {
            wrong.push("push_task_with_conn");
        }
        if !run.push_batch_with_conn_was_decode {
            wrong.push("push_batch_with_conn");
        }
        if !run.push_tasks_with_conn_was_decode {
            wrong.push("push_tasks_with_conn");
        }
        if wrong.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "expected Error::Decode from every outbox method, but these did not: {wrong:?}"
            ))
        }
    })
}

fn encode_failure_inserts_no_rows()
-> impl Fn(&Result<Outcome<EncodeFailureRun>, String>) -> AssertionResult {
    observe("encode-fail→job count", |run: &EncodeFailureRun| {
        if run.db_jobs == 0 {
            Ok(())
        } else {
            Err(format!(
                "encode failure happens before any DB write, expected 0 jobs, got {}",
                run.db_jobs
            ))
        }
    })
}

// --------------------------------------------------------------------------
// Scenario 4: idempotency_key conflict surfaces an error and the outer
// transaction can still commit its business writes (the savepoint rolls back
// only the apalis batch, not the surrounding work).
// --------------------------------------------------------------------------

#[derive(Debug)]
struct ConflictRun {
    second_push_was_conflict_error: bool,
    db_jobs_after_outer_commit: i64,
    db_business_after_outer_commit: i64,
}

async fn run_conflict_scenario() -> Result<Outcome<ConflictRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-outbox-conflict-{}", Ulid::new());
    let key = format!("marker-{queue}");
    let idem = format!("idem-{queue}");
    cleanup(pool.clone(), queue.clone()).await?;

    let storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(1));

    // Seed: a first task with the chosen idempotency_key, in its own
    // transaction so it is committed before the conflict scenario starts.
    {
        let storage = storage.clone();
        let pool_for_seed = pool.clone();
        let idem_for_seed = idem.clone();
        tokio::task::spawn_blocking(move || -> Result<(), String> {
            let mut conn = pool_for_seed.get().map_err(|e| e.to_string())?;
            let mut task = PgTask::<String>::new("payload-1".to_owned());
            task.parts.idempotency_key = Some(idem_for_seed);
            conn.transaction::<_, PgError, _>(|c| storage.push_task_with_conn(c, task).map(|_| ()))
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())??;
    }

    // Conflict scenario: open an outer transaction, insert a business row,
    // attempt a second push with the same idempotency_key (expected to
    // surface Error::IdempotencyConflict via savepoint rollback), then commit the
    // outer transaction. The business row must survive the savepoint
    // rollback.
    let q = queue.clone();
    let k = key.clone();
    let idem_for_run = idem.clone();
    let storage_for_run = storage.clone();
    let pool_for_run = pool.clone();
    let second_push_was_conflict_error =
        tokio::task::spawn_blocking(move || -> Result<bool, String> {
            let mut conn = pool_for_run.get().map_err(|e| e.to_string())?;
            let observed = std::cell::Cell::new(false);
            conn.transaction::<_, PgError, _>(|c| {
                sql_query("INSERT INTO apalis_outbox_test_marker (key, queue) VALUES ($1, $2)")
                    .bind::<Text, _>(&k)
                    .bind::<Text, _>(&q)
                    .execute(c)?;
                let mut task = PgTask::<String>::new("payload-2".to_owned());
                task.parts.idempotency_key = Some(idem_for_run.clone());
                match storage_for_run.push_task_with_conn(c, task) {
                    Ok(_) => {
                        return Err(PgError::InvalidArgument(
                            "expected idempotency conflict, got success".into(),
                        ));
                    }
                    Err(PgError::IdempotencyConflict { .. }) => {
                        observed.set(true);
                    }
                    Err(other) => {
                        return Err(PgError::InvalidArgument(format!(
                            "expected Error::IdempotencyConflict, got {other:?}"
                        )));
                    }
                }
                Ok(())
            })
            .map_err(|e| e.to_string())?;
            Ok(observed.get())
        })
        .await
        .map_err(|e| e.to_string())??;

    let q2 = queue.clone();
    let (db_jobs, db_business) = with_conn(pool.clone(), move |conn| {
        Ok::<_, String>((count_jobs(conn, &q2)?, count_business(conn, &q2)?))
    })
    .await?;

    cleanup(pool, queue).await?;
    Ok(Outcome::Completed(ConflictRun {
        second_push_was_conflict_error,
        db_jobs_after_outer_commit: db_jobs,
        db_business_after_outer_commit: db_business,
    }))
}

fn conflict_surfaces_idempotency_conflict()
-> impl Fn(&Result<Outcome<ConflictRun>, String>) -> AssertionResult {
    observe("conflict→error kind", |run: &ConflictRun| {
        if run.second_push_was_conflict_error {
            Ok(())
        } else {
            Err("second push did not surface an Error::IdempotencyConflict".into())
        }
    })
}

fn conflict_keeps_only_the_seed_job()
-> impl Fn(&Result<Outcome<ConflictRun>, String>) -> AssertionResult {
    observe(
        "conflict→job count after outer commit",
        |run: &ConflictRun| {
            if run.db_jobs_after_outer_commit == 1 {
                Ok(())
            } else {
                Err(format!(
                    "expected 1 job (seed survives, conflicting batch rolled back via savepoint), got {}",
                    run.db_jobs_after_outer_commit
                ))
            }
        },
    )
}

fn conflict_lets_outer_business_writes_commit()
-> impl Fn(&Result<Outcome<ConflictRun>, String>) -> AssertionResult {
    observe(
        "conflict→business row after outer commit",
        |run: &ConflictRun| {
            if run.db_business_after_outer_commit == 1 {
                Ok(())
            } else {
                Err(format!(
                    "expected the outer transaction's business write to survive, got {} rows",
                    run.db_business_after_outer_commit
                ))
            }
        },
    )
}

// --------------------------------------------------------------------------
// push_task_with_conn: PK conflict without idempotency keys must not poison
// the outer transaction.
//
// A primary-key conflict on a caller-supplied task id must roll back only
// the enqueue batch's SAVEPOINT, allowing the outer transaction's business
// writes to commit. `run_custom_fields_scenario` checks a unique supplied id.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct PkConflictRun {
    second_push_was_database_error: bool,
    db_jobs_after_outer_commit: i64,
    db_business_after_outer_commit: i64,
}

async fn run_pk_conflict_scenario() -> Result<Outcome<PkConflictRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-outbox-pk-conflict-{}", Ulid::new());
    let key = format!("marker-{queue}");
    cleanup(pool.clone(), queue.clone()).await?;

    let storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(1));
    let duplicate_id = PgTaskId::new(Ulid::new());

    // Seed: a first task occupying the caller-supplied id, committed in its
    // own transaction before the conflict scenario starts. No idempotency
    // keys anywhere — this drives the key-less enqueue branch.
    {
        let storage = storage.clone();
        let pool_for_seed = pool.clone();
        let seed_id = duplicate_id;
        tokio::task::spawn_blocking(move || -> Result<(), String> {
            let mut conn = pool_for_seed.get().map_err(|e| e.to_string())?;
            let mut task = PgTask::<String>::new("payload-1".to_owned());
            task.parts.task_id = Some(seed_id);
            conn.transaction::<_, PgError, _>(|c| storage.push_task_with_conn(c, task).map(|_| ()))
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())??;
    }

    // Scenario: open an outer transaction, insert a business row, attempt a
    // second push reusing the same task id (PK violation inside the batch's
    // SAVEPOINT), then commit the outer transaction. The business row must
    // survive — i.e. the outer transaction must still be usable after the
    // failed push.
    let q = queue.clone();
    let k = key.clone();
    let storage_for_run = storage.clone();
    let pool_for_run = pool.clone();
    let second_push_was_database_error =
        tokio::task::spawn_blocking(move || -> Result<bool, String> {
            let mut conn = pool_for_run.get().map_err(|e| e.to_string())?;
            let observed = std::cell::Cell::new(false);
            conn.transaction::<_, PgError, _>(|c| {
                sql_query("INSERT INTO apalis_outbox_test_marker (key, queue) VALUES ($1, $2)")
                    .bind::<Text, _>(&k)
                    .bind::<Text, _>(&q)
                    .execute(c)?;
                let mut task = PgTask::<String>::new("payload-2".to_owned());
                task.parts.task_id = Some(duplicate_id);
                match storage_for_run.push_task_with_conn(c, task) {
                    Ok(_) => {
                        return Err(PgError::InvalidArgument(
                            "expected a primary-key conflict, got success".into(),
                        ));
                    }
                    Err(PgError::Database { .. }) => {
                        observed.set(true);
                    }
                    Err(other) => {
                        return Err(PgError::InvalidArgument(format!(
                            "expected Error::Database from the PK violation, got {other:?}"
                        )));
                    }
                }
                Ok(())
            })
            .map_err(|e| e.to_string())?;
            Ok(observed.get())
        })
        .await
        .map_err(|e| e.to_string())??;

    let q2 = queue.clone();
    let (db_jobs, db_business) = with_conn(pool.clone(), move |conn| {
        Ok::<_, String>((count_jobs(conn, &q2)?, count_business(conn, &q2)?))
    })
    .await?;

    cleanup(pool, queue).await?;
    Ok(Outcome::Completed(PkConflictRun {
        second_push_was_database_error,
        db_jobs_after_outer_commit: db_jobs,
        db_business_after_outer_commit: db_business,
    }))
}

fn pk_conflict_surfaces_a_database_error()
-> impl Fn(&Result<Outcome<PkConflictRun>, String>) -> AssertionResult {
    observe("pk-conflict→error kind", |run: &PkConflictRun| {
        if run.second_push_was_database_error {
            Ok(())
        } else {
            Err("second push did not surface an Error::Database for the PK violation".into())
        }
    })
}

fn pk_conflict_keeps_only_the_seed_job()
-> impl Fn(&Result<Outcome<PkConflictRun>, String>) -> AssertionResult {
    observe(
        "pk-conflict→job count after outer commit",
        |run: &PkConflictRun| {
            if run.db_jobs_after_outer_commit == 1 {
                Ok(())
            } else {
                Err(format!(
                    "expected 1 job (seed survives, conflicting batch rolled back via savepoint), got {}",
                    run.db_jobs_after_outer_commit
                ))
            }
        },
    )
}

fn pk_conflict_lets_outer_business_writes_commit()
-> impl Fn(&Result<Outcome<PkConflictRun>, String>) -> AssertionResult {
    observe(
        "pk-conflict→business row after outer commit",
        |run: &PkConflictRun| {
            if run.db_business_after_outer_commit == 1 {
                Ok(())
            } else {
                Err(format!(
                    "expected the outer transaction's business write to survive the failed push, got {} rows",
                    run.db_business_after_outer_commit
                ))
            }
        },
    )
}

// A mixed batch must be fully validated before its first row can commit.
// The caller's transaction remains usable after either schedule rejection.
#[derive(Debug)]
struct ScheduleRun {
    accepted: bool,
    rows: i64,
    exact_schedule: Option<i64>,
    business_rows: i64,
    mixed: bool,
}
async fn run_schedule_scenario(seconds: u64, mixed: bool) -> Result<Outcome<ScheduleRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("outbox-schedule-{}", Ulid::new());
    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let q = queue.clone();
    let run = with_conn(pool.clone(),move |conn| {
        let accepted = conn.transaction::<_,PgError,_>(|conn| {
            let mut scheduled = PgTask::new("scheduled".to_owned());
            scheduled.parts.run_at = seconds;
            let result = if mixed { storage.push_tasks_with_conn(conn,vec![PgTask::new("valid".to_owned()),scheduled]).map(|_|()) }
                         else { storage.push_task_with_conn(conn,scheduled).map(|_|()) };
            let accepted = match result { Ok(()) => true, Err(PgError::InvalidArgument(message)) if message.contains("run_at") => false,
                Err(error) => return Err(error) };
            sql_query("INSERT INTO apalis_outbox_test_marker(key,queue) VALUES($1,$1)").bind::<Text,_>(&q).execute(conn)?;
            Ok(accepted)
        }).map_err(|e|e.to_string())?;
        let rows = count_jobs(conn,&q)?;
        let exact_schedule = if rows == 0 { None } else {
            Some(sql_query("SELECT max(extract(epoch from run_at))::bigint AS n FROM apalis.jobs WHERE job_type=$1")
                .bind::<Text,_>(&q).get_result::<CountRow>(conn).map_err(|e|e.to_string())?.n)
        };
        Ok(ScheduleRun { accepted,rows,exact_schedule,business_rows:count_business(conn,&q)?,mixed })
    }).await;
    cleanup(pool, queue).await?;
    run.map(Outcome::Completed)
}
fn schedule_outcome(
    expected: Option<i64>,
) -> impl Fn(&Result<Outcome<ScheduleRun>, String>) -> AssertionResult {
    observe(
        "schedule validation and caller transaction",
        move |run: &ScheduleRun| {
            let valid = run.accepted == expected.is_some()
                && run.rows == i64::from(expected.is_some()) * (1 + i64::from(run.mixed))
                && run.exact_schedule == expected
                && run.business_rows == 1;
            if valid {
                Ok(())
            } else {
                Err(format!(
                    "expected exact schedule {expected:?}, atomic batch and business commit; got {run:?}"
                ))
            }
        },
    )
}

#[derive(Debug)]
struct BatchFailureRun {
    expected_error: bool,
    jobs: i64,
    business: i64,
    expected_jobs: i64,
}
fn batch_failure_outcome() -> impl Fn(&Result<Outcome<BatchFailureRun>, String>) -> AssertionResult
{
    observe(
        "batch failure and outer transaction",
        |run: &BatchFailureRun| {
            if run.expected_error && run.jobs == run.expected_jobs && run.business == 1 {
                Ok(())
            } else {
                Err(format!(
                    "batch failure violated atomicity, error detail, or outer usability: {run:?}"
                ))
            }
        },
    )
}
async fn run_late_encoding_failure(full_tasks: bool) -> Result<Outcome<BatchFailureRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("outbox-codec-prefix-{}", Ulid::new());
    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue))
        .with_codec::<FailingEncodeCodec>();
    let q = queue.clone();
    let run = with_conn(pool.clone(), move |conn| {
        let expected_error = conn
            .transaction::<_, PgError, _>(|conn| {
                let result = if full_tasks {
                    storage.push_tasks_with_conn(
                        conn,
                        vec![
                            PgTask::new("valid".to_owned()),
                            PgTask::new("rejected".to_owned()),
                        ],
                    )
                } else {
                    storage
                        .push_batch_with_conn(conn, vec!["valid".to_owned(), "rejected".to_owned()])
                };
                let expected_error = matches!(result, Err(PgError::Decode(_)));
                sql_query("INSERT INTO apalis_outbox_test_marker(key,queue) VALUES($1,$1)")
                    .bind::<Text, _>(&q)
                    .execute(conn)?;
                Ok(expected_error)
            })
            .map_err(|e| e.to_string())?;
        Ok(BatchFailureRun {
            expected_error,
            jobs: count_jobs(conn, &q)?,
            business: count_business(conn, &q)?,
            expected_jobs: 0,
        })
    })
    .await;
    cleanup(pool, queue).await?;
    run.map(Outcome::Completed)
}
async fn run_mixed_idempotency_conflict(seeded: bool) -> Result<Outcome<BatchFailureRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("outbox-mixed-key-{}", Ulid::new());
    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let q = queue.clone();
    let run = with_conn(pool.clone(),move |conn| {
        let keyed = |key: &str| { let mut task = PgTask::new("payload".to_owned()); task.parts.idempotency_key=Some(key.to_owned());task };
        if seeded { storage.push_task_with_conn(conn,keyed("duplicate")).map_err(|e|e.to_string())?; }
        let expected_error = conn.transaction::<_,PgError,_>(|conn| {
            let tasks = vec![PgTask::new("without-key".to_owned()),keyed(if seeded {"fresh"} else {"duplicate"}),keyed("duplicate")];
            let expected_error = matches!(storage.push_tasks_with_conn(conn,tasks),Err(PgError::IdempotencyConflict { conflicting_keys,total:3,.. }) if conflicting_keys == vec!["duplicate".to_owned()]);
            sql_query("INSERT INTO apalis_outbox_test_marker(key,queue) VALUES($1,$1)").bind::<Text,_>(&q).execute(conn)?;
            Ok(expected_error)
        }).map_err(|e|e.to_string())?;
        Ok(BatchFailureRun { expected_error,jobs:count_jobs(conn,&q)?,business:count_business(conn,&q)?,expected_jobs:i64::from(seeded) })
    }).await;
    cleanup(pool, queue).await?;
    run.map(Outcome::Completed)
}

// --------------------------------------------------------------------------
// Test entry points
// --------------------------------------------------------------------------

lets_expect! { #tokio_test
    expect(run_commit_scenario().await) as commit_outcome {
        when outer_transaction_commits_with_push_with_conn {
            to persists_the_task_and_business_row_with_its_returned_id {
                commit_persists_one_job(),
                commit_persists_one_business_row(),
                commit_returns_id_matching_db()
            }
        }
    }

    expect(run_batch_commit_scenario().await) as batch_commit_outcome {
        when push_batch_with_conn_commits_a_multi_task_batch {
            to persists_every_task_in_submission_order {
                batch_inserts_every_task(),
                batch_returns_distinct_ids_present_in_db(),
                batch_returns_ids_in_submission_order()
            }
        }
    }

    expect(run_rollback_scenario().await) as rollback_outcome {
        when outer_transaction_rolls_back_with_push_with_conn {
            to rolls_back_the_task_and_business_row_after_successful_enqueue {
                rollback_call_succeeded_before_outer_rollback(),
                rollback_leaves_no_job(),
                rollback_leaves_no_business_row()
            }
        }
    }

    expect(run_custom_fields_scenario().await) as custom_fields_outcome {
        when push_task_with_conn_receives_a_fully_populated_task {
            to preserves_the_identity_and_all_task_fields {
                custom_returned_id_is_the_preassigned_one(),
                custom_priority_is_stored(),
                custom_max_attempts_is_stored(),
                custom_run_at_is_stored(),
                custom_metadata_is_stored(),
                custom_idempotency_key_is_stored()
            }
        }
    }

    expect(run_batch_custom_fields_scenario().await) as batch_custom_fields_outcome {
        when push_tasks_with_conn_receives_distinct_fully_populated_tasks {
            to preserves_every_tasks_identity_and_fields_in_submission_order {
                batch_custom_returns_each_task_id_in_order(),
                batch_custom_carries_each_tasks_fields()
            }
        }
    }

    expect(run_empty_batch_scenario().await) as empty_batch_outcome {
        when the_batch_iterators_are_empty {
            to returns_empty_results_without_inserting_rows {
                empty_push_batch_returns_empty_vec(),
                empty_push_tasks_returns_empty_vec(),
                empty_batch_inserts_no_rows()
            }
        }
    }

    expect(run_encode_failure_scenario().await) as encode_failure_outcome {
        when the_codec_rejects_the_args_on_encode {
            to reports_the_codec_error_without_inserting_rows {
                encode_failure_surfaces_decode_on_every_method(),
                encode_failure_inserts_no_rows()
            }
        }
    }

    expect(run_conflict_scenario().await) as conflict_outcome {
        when the_idempotency_key_already_exists {
            to reports_the_conflict_and_preserves_only_the_seed_and_business_write {
                conflict_surfaces_idempotency_conflict(),
                conflict_keeps_only_the_seed_job(),
                conflict_lets_outer_business_writes_commit()
            }
        }
    }

    expect(run_pk_conflict_scenario().await) as pk_conflict_outcome {
        when the_task_id_already_exists_without_idempotency_keys {
            to reports_the_database_error_and_preserves_only_the_seed_and_business_write {
                pk_conflict_surfaces_a_database_error(),
                pk_conflict_keeps_only_the_seed_job(),
                pk_conflict_lets_outer_business_writes_commit()
            }
        }
    }

}

lets_expect! { #tokio_test
    expect(run_schedule_scenario(seconds,mixed).await) as scheduled_outbox {
        when(seconds=8_210_266_876_799) as the_schedule_is_the_last_supported_second {
            when(mixed=false) as the_task_is_enqueued_alone {
                to stores_the_exact_schedule_and_commits_the_business_write { schedule_outcome(Some(8_210_266_876_799)) }
            }
            when(mixed=true) as a_valid_task_precedes_the_scheduled_task {
                to stores_both_tasks_with_the_exact_schedule_and_commits_the_business_write { schedule_outcome(Some(8_210_266_876_799)) }
            }
        }
        when(seconds=8_210_266_876_800) as the_schedule_is_one_second_outside_the_timestamp_range {
            when(mixed=false) as the_task_is_enqueued_alone {
                to rejects_the_task_and_keeps_the_business_transaction_usable { schedule_outcome(None) }
            }
            when(mixed=true) as a_valid_task_precedes_the_scheduled_task {
                to rejects_every_row_and_keeps_the_business_transaction_usable { schedule_outcome(None) }
            }
        }
        when(seconds=i64::MAX as u64) as the_schedule_exceeds_chrono_but_fits_i64 {
            when(mixed=false) as the_task_is_enqueued_alone {
                to rejects_the_task_and_keeps_the_business_transaction_usable { schedule_outcome(None) }
            }
            when(mixed=true) as a_valid_task_precedes_the_scheduled_task {
                to rejects_every_row_and_keeps_the_business_transaction_usable { schedule_outcome(None) }
            }
        }
        when(seconds=i64::MAX as u64+1) as the_schedule_exceeds_i64 {
            when(mixed=false) as the_task_is_enqueued_alone {
                to rejects_the_task_and_keeps_the_business_transaction_usable { schedule_outcome(None) }
            }
            when(mixed=true) as a_valid_task_precedes_the_scheduled_task {
                to rejects_every_row_and_keeps_the_business_transaction_usable { schedule_outcome(None) }
            }
        }
    }
}

lets_expect! { #tokio_test
    expect(run_late_encoding_failure(full_tasks).await) as partial_batch_encoding {
        when(full_tasks=false) as the_caller_submits_arguments {
            to rejects_the_batch_after_a_valid_prefix_and_preserves_the_business_write { batch_failure_outcome() }
        }
        when(full_tasks=true) as the_caller_submits_full_tasks {
            to rejects_the_batch_after_a_valid_prefix_and_preserves_the_business_write { batch_failure_outcome() }
        }
    }
    expect(run_mixed_idempotency_conflict(seeded).await) as mixed_batch_deduplication {
        when(seeded=false) as the_duplicate_occurs_within_the_batch {
            to reports_the_duplicate_and_rolls_back_keyed_and_unkeyed_rows { batch_failure_outcome() }
        }
        when(seeded=true) as the_duplicate_was_committed_by_an_earlier_sender {
            to reports_the_duplicate_and_preserves_only_the_seed_and_business_write { batch_failure_outcome() }
        }
    }
}

// Final contract gaps: outer rollback, concurrent uniqueness, and identity scope.

#[derive(QueryableByName, Debug, PartialEq, Eq)]
struct StoredOutboxTask {
    #[diesel(sql_type = Text)]
    id: String,
    #[diesel(sql_type = Text)]
    payload: String,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    idempotency_key: Option<String>,
}

fn stored_outbox_tasks(
    conn: &mut PgConnection,
    queue: &str,
) -> Result<Vec<StoredOutboxTask>, String> {
    sql_query("SELECT id, convert_from(job, 'UTF8') AS payload, idempotency_key FROM apalis.jobs WHERE job_type = $1 ORDER BY id")
        .bind::<Text, _>(queue)
        .load(conn)
        .map_err(|e| e.to_string())
}

fn outbox_marker(conn: &mut PgConnection, key: &str, queue: &str) -> diesel::QueryResult<usize> {
    sql_query("INSERT INTO apalis_outbox_test_marker(key, queue) VALUES($1, $2)")
        .bind::<Text, _>(key)
        .bind::<Text, _>(queue)
        .execute(conn)
}

#[derive(Debug)]
struct BatchRollbackContract {
    returned_ids: Vec<String>,
    visible_ids_before_rollback: Vec<String>,
    business_before_rollback: i64,
    explicit_rollback: bool,
    tasks_after_rollback: i64,
    business_after_rollback: i64,
}

async fn run_batch_rollback_contract(
    full_tasks: bool,
) -> Result<Outcome<BatchRollbackContract>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("outbox-batch-rollback-{}", Ulid::new());
    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let q = queue.clone();
    let prepared = with_conn(pool.clone(), move |conn| {
        let mut captured = None;
        let result = conn.transaction::<(), PgError, _>(|conn| {
            outbox_marker(conn, &q, &q)?;
            let payloads = vec!["first".to_owned(), "second".to_owned()];
            let ids = if full_tasks {
                storage.push_tasks_with_conn(conn, payloads.into_iter().map(PgTask::new))?
            } else {
                storage.push_batch_with_conn(conn, payloads)?
            };
            let mut ids = ids.into_iter().map(|id| id.to_string()).collect::<Vec<_>>();
            ids.sort();
            let visible_ids = stored_outbox_tasks(conn, &q)
                .map_err(PgError::InvalidArgument)?
                .into_iter()
                .map(|task| task.id)
                .collect();
            let business = count_business(conn, &q).map_err(PgError::InvalidArgument)?;
            captured = Some((ids, visible_ids, business));
            Err(diesel::result::Error::RollbackTransaction.into())
        });
        let explicit_rollback = matches!(
            result,
            Err(PgError::Database {
                source: diesel::result::Error::RollbackTransaction,
                ..
            })
        );
        let (returned_ids, visible_ids_before_rollback, business_before_rollback) = captured
            .ok_or_else(|| format!("batch enqueue did not succeed before rollback: {result:?}"))?;
        Ok((
            returned_ids,
            visible_ids_before_rollback,
            business_before_rollback,
            explicit_rollback,
        ))
    })
    .await;
    // This checkout occurs after the caller's transaction has actually ended.
    let observed = match prepared {
        Ok((
            returned_ids,
            visible_ids_before_rollback,
            business_before_rollback,
            explicit_rollback,
        )) => {
            let q = queue.clone();
            with_conn(pool.clone(), move |conn| {
                Ok(BatchRollbackContract {
                    returned_ids,
                    visible_ids_before_rollback,
                    business_before_rollback,
                    explicit_rollback,
                    tasks_after_rollback: count_jobs(conn, &q)?,
                    business_after_rollback: count_business(conn, &q)?,
                })
            })
            .await
        }
        Err(error) => Err(error),
    };
    cleanup(pool, queue).await?;
    observed.map(Outcome::Completed)
}

fn batch_rollback_contract()
-> impl Fn(&Result<Outcome<BatchRollbackContract>, String>) -> AssertionResult {
    observe("batch caller rollback", |run: &BatchRollbackContract| {
        if run.returned_ids.len() == 2
            && run.returned_ids[0] != run.returned_ids[1]
            && run.returned_ids == run.visible_ids_before_rollback
            && run.business_before_rollback == 1
            && run.explicit_rollback
            && run.tasks_after_rollback == 0
            && run.business_after_rollback == 0
        {
            Ok(())
        } else {
            Err(format!(
                "enqueue must succeed inside the caller transaction, then rollback all rows: {run:?}"
            ))
        }
    })
}

#[derive(Debug)]
struct ConcurrentOutboxContract {
    first_pid: i32,
    second_pid: i32,
    blocked_by_first: bool,
    first_completed_as_requested: bool,
    second_result_matches: bool,
    stored: Vec<StoredOutboxTask>,
    expected_task: StoredOutboxTask,
    business_rows: i64,
    expected_business_rows: i64,
}

async fn run_concurrent_outbox_contract(
    first_commits: bool,
) -> Result<Outcome<ConcurrentOutboxContract>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("outbox-concurrent-{}", Ulid::new());
    let first_id = PgTaskId::new(Ulid::new());
    let second_id = PgTaskId::new(Ulid::new());
    let first_storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let second_storage = first_storage.clone();
    let first_pool = pool.clone();
    let second_pool = pool.clone();
    let first_queue = queue.clone();
    let second_queue = queue.clone();
    let (ready_first_tx, ready_first_rx) = tokio::sync::oneshot::channel();
    let (ready_second_tx, ready_second_rx) = tokio::sync::oneshot::channel();
    let (release_first_tx, release_first_rx) = std::sync::mpsc::channel::<bool>();
    let first = tokio::task::spawn_blocking(move || -> Result<bool, String> {
        let mut conn = first_pool.get().map_err(|e| e.to_string())?;
        let result = conn.transaction::<_, PgError, _>(|conn| {
            sql_query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").execute(conn)?;
            let pid = sql_query("SELECT pg_backend_pid()::bigint AS n")
                .get_result::<CountRow>(conn)?
                .n as i32;
            let mut task = PgTask::new("first".to_owned());
            task.parts.task_id = Some(first_id);
            task.parts.idempotency_key = Some("shared-key".to_owned());
            let returned_id = first_storage.push_task_with_conn(conn, task)?;
            outbox_marker(conn, &format!("first-{first_queue}"), &first_queue)?;
            ready_first_tx
                .send(pid)
                .map_err(|_| PgError::InvalidArgument("first sender observer closed".to_owned()))?;
            // A dropped controller rolls back; a bounded wait prevents stranding a SQL transaction.
            let commit = release_first_rx
                .recv_timeout(std::time::Duration::from_secs(15))
                .map_err(|e| {
                    PgError::InvalidArgument(format!("first sender was not released: {e}"))
                })?;
            if commit {
                Ok(returned_id == first_id)
            } else {
                Err(diesel::result::Error::RollbackTransaction.into())
            }
        });
        match result {
            Ok(ids_match) if first_commits => Ok(ids_match),
            Err(PgError::Database {
                source: diesel::result::Error::RollbackTransaction,
                ..
            }) if !first_commits => Ok(true),
            other => Err(format!("unexpected first sender completion: {other:?}")),
        }
    });
    let first_pid = match ready_first_rx.await {
        Ok(pid) => pid,
        Err(error) => {
            drop(release_first_tx);
            let first_result = first.await;
            cleanup(pool, queue).await?;
            return Err(format!(
                "first sender did not reach the uncommitted INSERT: {error}; {first_result:?}"
            ));
        }
    };
    let second = tokio::task::spawn_blocking(move || -> Result<bool, String> {
        let mut conn = second_pool.get().map_err(|e| e.to_string())?;
        conn.transaction::<_, PgError, _>(|conn| {
            sql_query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").execute(conn)?;
            sql_query("SET LOCAL statement_timeout = '10s'").execute(conn)?;
            let pid = sql_query("SELECT pg_backend_pid()::bigint AS n")
                .get_result::<CountRow>(conn)?
                .n as i32;
            ready_second_tx.send(pid).map_err(|_| {
                PgError::InvalidArgument("second sender observer closed".to_owned())
            })?;
            let mut task = PgTask::new("second".to_owned());
            task.parts.task_id = Some(second_id);
            task.parts.idempotency_key = Some("shared-key".to_owned());
            let second_result_matches = match second_storage.push_task_with_conn(conn, task) {
                Ok(id) => !first_commits && id == second_id,
                Err(PgError::IdempotencyConflict {
                    job_type,
                    conflicting_keys,
                    total,
                }) => {
                    first_commits
                        && job_type == second_queue
                        && conflicting_keys == ["shared-key"]
                        && total == 1
                }
                Err(error) => return Err(error),
            };
            // A caught duplicate must leave the caller's business transaction usable.
            outbox_marker(conn, &format!("second-{second_queue}"), &second_queue)?;
            Ok(second_result_matches)
        })
        .map_err(|e| e.to_string())
    });
    let second_pid = ready_second_rx.await.map_err(|e| e.to_string());
    let blocked = match second_pid {
        Ok(second_pid) => with_conn(pool.clone(), move |conn| {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let blockers = sql_query("SELECT count(*)::bigint AS n FROM unnest(pg_blocking_pids($1)) AS blocker(pid) WHERE pid = $2")
                    .bind::<Integer, _>(second_pid).bind::<Integer, _>(first_pid)
                    .get_result::<CountRow>(conn).map_err(|e| e.to_string())?.n;
                if blockers > 0 { return Ok((second_pid, true)); }
                if std::time::Instant::now() >= deadline { return Err(format!("second backend {second_pid} never waited for first backend {first_pid}")); }
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }).await,
        Err(error) => Err(error),
    };
    // Release and join BOTH senders even if observation failed, before cleaning task-owned rows.
    let released = release_first_tx
        .send(first_commits)
        .map_err(|e| e.to_string());
    let (first_result, second_result) = tokio::join!(first, second);
    let run = async {
        released?;
        let (second_pid, blocked_by_first) = blocked?;
        let first_completed_as_requested = first_result.map_err(|e| e.to_string())??;
        let second_result_matches = second_result.map_err(|e| e.to_string())??;
        eprintln!("outbox arbitration: first_pid={first_pid}, second_pid={second_pid}, blocked_by_first={blocked_by_first}, first_commits={first_commits}");
        let q = queue.clone();
        with_conn(pool.clone(), move |conn| Ok(ConcurrentOutboxContract {
            first_pid, second_pid, blocked_by_first, first_completed_as_requested, second_result_matches,
            stored: stored_outbox_tasks(conn, &q)?,
            expected_task: StoredOutboxTask {
                id: if first_commits { first_id } else { second_id }.to_string(),
                payload: if first_commits { "\"first\"" } else { "\"second\"" }.to_owned(),
                idempotency_key: Some("shared-key".to_owned()),
            },
            business_rows: count_business(conn, &q)?, expected_business_rows: if first_commits { 2 } else { 1 },
        })).await
    }.await;
    cleanup(pool, queue).await?;
    run.map(Outcome::Completed)
}

fn concurrent_outbox_contract()
-> impl Fn(&Result<Outcome<ConcurrentOutboxContract>, String>) -> AssertionResult {
    observe(
        "concurrent outbox unique arbitration",
        |run: &ConcurrentOutboxContract| {
            if run.first_pid != run.second_pid
                && run.blocked_by_first
                && run.first_completed_as_requested
                && run.second_result_matches
                && run.stored.as_slice() == std::slice::from_ref(&run.expected_task)
                && run.business_rows == run.expected_business_rows
            {
                Ok(())
            } else {
                Err(format!(
                    "two physical senders must arbitrate the key and preserve exactly the winning task and committed business writes: {run:?}"
                ))
            }
        },
    )
}

#[derive(Debug)]
struct CrossQueueIdentityContract {
    first_result_matches: bool,
    second_result_matches: bool,
    first_tasks: Vec<StoredOutboxTask>,
    second_tasks: Vec<StoredOutboxTask>,
    expected_first: StoredOutboxTask,
    expected_second: Vec<StoredOutboxTask>,
    first_business: i64,
    second_business: i64,
}

async fn run_cross_queue_identity_contract(
    same_task_id: bool,
) -> Result<Outcome<CrossQueueIdentityContract>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let first_queue = format!("outbox-identity-first-{}", Ulid::new());
    let second_queue = format!("outbox-identity-second-{}", Ulid::new());
    let first_storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&first_queue));
    let second_storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&second_queue));
    let first_id = PgTaskId::new(Ulid::new());
    let second_id = if same_task_id {
        first_id
    } else {
        PgTaskId::new(Ulid::new())
    };
    let key = (!same_task_id).then(|| "shared-key".to_owned());
    let q1 = first_queue.clone();
    let q2 = second_queue.clone();
    let run = with_conn(pool.clone(), move |conn| {
        let mut first_task = PgTask::new("first".to_owned());
        first_task.parts.task_id = Some(first_id);
        first_task.parts.idempotency_key = key.clone();
        let first_result_matches = conn
            .transaction::<_, PgError, _>(|conn| {
                let returned = first_storage.push_task_with_conn(conn, first_task)?;
                outbox_marker(conn, &q1, &q1)?;
                Ok(returned == first_id)
            })
            .map_err(|e| e.to_string())?;
        let mut second_task = PgTask::new("second".to_owned());
        second_task.parts.task_id = Some(second_id);
        second_task.parts.idempotency_key = key.clone();
        let second_result_matches = conn
            .transaction::<_, PgError, _>(|conn| {
                let outcome = match second_storage.push_task_with_conn(conn, second_task) {
                    Ok(id) => !same_task_id && id == second_id,
                    Err(PgError::Database {
                        source:
                            diesel::result::Error::DatabaseError(
                                diesel::result::DatabaseErrorKind::UniqueViolation,
                                _,
                            ),
                        ..
                    }) => same_task_id,
                    Err(error) => return Err(error),
                };
                outbox_marker(conn, &q2, &q2)?;
                Ok(outcome)
            })
            .map_err(|e| e.to_string())?;
        Ok(CrossQueueIdentityContract {
            first_result_matches,
            second_result_matches,
            first_tasks: stored_outbox_tasks(conn, &q1)?,
            second_tasks: stored_outbox_tasks(conn, &q2)?,
            expected_first: StoredOutboxTask {
                id: first_id.to_string(),
                payload: "\"first\"".to_owned(),
                idempotency_key: key.clone(),
            },
            expected_second: if same_task_id {
                vec![]
            } else {
                vec![StoredOutboxTask {
                    id: second_id.to_string(),
                    payload: "\"second\"".to_owned(),
                    idempotency_key: key,
                }]
            },
            first_business: count_business(conn, &q1)?,
            second_business: count_business(conn, &q2)?,
        })
    })
    .await;
    let first_cleanup = cleanup(pool.clone(), first_queue).await;
    let second_cleanup = cleanup(pool, second_queue).await;
    first_cleanup?;
    second_cleanup?;
    run.map(Outcome::Completed)
}

fn cross_queue_identity_contract()
-> impl Fn(&Result<Outcome<CrossQueueIdentityContract>, String>) -> AssertionResult {
    observe(
        "cross-queue identity scope",
        |run: &CrossQueueIdentityContract| {
            if run.first_result_matches
                && run.second_result_matches
                && run.first_tasks.as_slice() == std::slice::from_ref(&run.expected_first)
                && run.second_tasks == run.expected_second
                && run.first_business == 1
                && run.second_business == 1
            {
                Ok(())
            } else {
                Err(format!(
                    "keys must be queue-scoped, task IDs global, and a caught PK failure must preserve the second business transaction: {run:?}"
                ))
            }
        },
    )
}

lets_expect! { #tokio_test
    expect(run_batch_rollback_contract(full_tasks).await) as outbox_contract_batch_rollback {
        when(full_tasks=false) as the_caller_submits_arguments {
            to rolls_back_both_accepted_tasks_and_the_business_write { batch_rollback_contract() }
        }
        when(full_tasks=true) as the_caller_submits_full_tasks {
            to rolls_back_both_accepted_tasks_and_the_business_write { batch_rollback_contract() }
        }
    }
    expect(run_concurrent_outbox_contract(first_commits).await) as outbox_contract_concurrent_sender {
        when(first_commits=true) as the_earlier_sender_commits {
            to reports_the_duplicate_after_waiting_and_commits_the_second_business_write { concurrent_outbox_contract() }
        }
        when(first_commits=false) as the_earlier_sender_rolls_back {
            to accepts_the_waiting_sender_and_preserves_only_its_task_and_business_write { concurrent_outbox_contract() }
        }
    }
    expect(run_cross_queue_identity_contract(same_task_id).await) as outbox_contract_cross_queue_identity {
        when(same_task_id=false) as different_queues_receive_the_same_key_with_distinct_task_ids {
            to accepts_both_tasks_and_business_writes { cross_queue_identity_contract() }
        }
        when(same_task_id=true) as different_queues_receive_the_same_task_id_without_keys {
            to reports_the_primary_key_conflict_and_commits_the_second_business_write { cross_queue_identity_contract() }
        }
    }
}
