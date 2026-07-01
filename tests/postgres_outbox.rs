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
//! Tests gate on `DATABASE_URL`; without it every scenario resolves to
//! `Outcome::Skipped` and the assertions pass.

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
    support::shared_pool().await
}

async fn ensure_business_table(pool: PgPool) -> Result<(), String> {
    with_conn(pool, |conn| {
        sql_query(
            "CREATE TABLE IF NOT EXISTS apalis_outbox_test_marker (
                key TEXT PRIMARY KEY,
                queue TEXT NOT NULL
            )",
        )
        .execute(conn)
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await
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
    ensure_business_table(pool.clone()).await?;
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
    ensure_business_table(pool.clone()).await?;
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
    ensure_business_table(pool.clone()).await?;
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

/// A codec that always fails to encode, exercising the documented
/// `Error::Decode` branch of the outbox methods. `decode` is never reached in
/// these tests but is required by the trait.
#[derive(Debug, Clone, Default)]
struct FailingEncodeCodec;

#[derive(Debug)]
struct FailingEncodeError;

impl std::fmt::Display for FailingEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("failing codec: encode always fails")
    }
}

impl std::error::Error for FailingEncodeError {}

impl apalis_core::backend::codec::Codec<String> for FailingEncodeCodec {
    type Error = FailingEncodeError;
    type Compact = Vec<u8>;

    fn encode(_val: &String) -> Result<Self::Compact, Self::Error> {
        Err(FailingEncodeError)
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
    ensure_business_table(pool.clone()).await?;
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
// Regression for the round-11 audit follow-up: the key-less enqueue path
// briefly ran as a bare INSERT on the caller's connection, so a PK violation
// on a caller-supplied task id aborted the caller's *outer* transaction (its
// business writes were silently rolled back on COMMIT). The outbox path now
// wraps key-less batches in `conn.transaction(...)` again, so the failure is
// contained in the batch's SAVEPOINT. The happy path (unique caller-supplied
// id) is already pinned by `run_custom_fields_scenario`.
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
    ensure_business_table(pool.clone()).await?;
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

// --------------------------------------------------------------------------
// Test entry points
// --------------------------------------------------------------------------

lets_expect! { #tokio_test
    expect(run_commit_scenario().await) {
        when outer_transaction_commits_with_push_with_conn {
            to persists_exactly_one_task { commit_persists_one_job() }
            to persists_exactly_one_business_row { commit_persists_one_business_row() }
            to returns_a_task_id_that_matches_the_stored_row { commit_returns_id_matching_db() }
        }
    }

    expect(run_batch_commit_scenario().await) {
        when push_batch_with_conn_commits_a_multi_task_batch {
            to inserts_every_task_in_the_batch { batch_inserts_every_task() }
            to returns_distinct_ids_that_all_landed { batch_returns_distinct_ids_present_in_db() }
            to returns_ids_in_submission_order { batch_returns_ids_in_submission_order() }
        }
    }

    expect(run_rollback_scenario().await) {
        when outer_transaction_rolls_back_with_push_with_conn {
            to confirms_the_inner_push_call_was_observed_before_rollback {
                rollback_call_succeeded_before_outer_rollback()
            }
            to leaves_the_apalis_table_empty { rollback_leaves_no_job() }
            to leaves_the_business_table_empty { rollback_leaves_no_business_row() }
        }
    }

    expect(run_custom_fields_scenario().await) {
        when push_task_with_conn_receives_a_fully_populated_task {
            to honours_the_preassigned_task_id { custom_returned_id_is_the_preassigned_one() }
            to stores_the_priority { custom_priority_is_stored() }
            to stores_the_max_attempts { custom_max_attempts_is_stored() }
            to stores_the_scheduled_run_at { custom_run_at_is_stored() }
            to stores_the_metadata { custom_metadata_is_stored() }
            to stores_the_idempotency_key { custom_idempotency_key_is_stored() }
        }
    }

    expect(run_batch_custom_fields_scenario().await) {
        when push_tasks_with_conn_receives_a_batch_of_distinct_fully_populated_tasks {
            to returns_each_task_id_in_submission_order { batch_custom_returns_each_task_id_in_order() }
            to carries_each_tasks_own_custom_fields { batch_custom_carries_each_tasks_fields() }
        }
    }

    expect(run_empty_batch_scenario().await) {
        when the_batch_iterators_are_empty {
            to push_batch_with_conn_returns_an_empty_vector { empty_push_batch_returns_empty_vec() }
            to push_tasks_with_conn_returns_an_empty_vector { empty_push_tasks_returns_empty_vec() }
            to inserts_no_rows { empty_batch_inserts_no_rows() }
        }
    }

    expect(run_encode_failure_scenario().await) {
        when the_codec_rejects_the_args_on_encode {
            to every_outbox_method_surfaces_error_decode {
                encode_failure_surfaces_decode_on_every_method()
            }
            to no_rows_are_inserted { encode_failure_inserts_no_rows() }
        }
    }

    expect(run_conflict_scenario().await) {
        when push_task_with_conn_collides_on_idempotency_key {
            to surfaces_an_idempotency_conflict {
                conflict_surfaces_idempotency_conflict()
            }
            to rolls_back_only_the_apalis_batch_via_savepoint {
                conflict_keeps_only_the_seed_job()
            }
            to leaves_the_outer_business_writes_intact {
                conflict_lets_outer_business_writes_commit()
            }
        }
    }

    expect(run_pk_conflict_scenario().await) {
        when push_task_with_conn_collides_on_a_caller_supplied_id_without_idempotency_keys {
            to surfaces_a_database_error {
                pk_conflict_surfaces_a_database_error()
            }
            to rolls_back_only_the_apalis_batch_via_savepoint {
                pk_conflict_keeps_only_the_seed_job()
            }
            to leaves_the_outer_business_writes_intact {
                pk_conflict_lets_outer_business_writes_commit()
            }
        }
    }
}
