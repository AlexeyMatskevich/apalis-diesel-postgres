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

use std::time::{SystemTime, UNIX_EPOCH};

use apalis_diesel_postgres::{
    Config, Error as PgError, PgPool, PgTask, PgTaskId, PostgresStorage,
};
use apalis_sql::{DateTimeExt, context::SqlContext};
use diesel::{
    Connection, OptionalExtension, PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{Integer, Jsonb, Text, Timestamptz},
};
use lets_expect::{AssertionError, AssertionResult, *};
use ulid::Ulid;

// --------------------------------------------------------------------------
// scaffolding
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
    // surface InvalidArgument via savepoint rollback), then commit the
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
                    Err(PgError::InvalidArgument(msg)) if msg.contains("idempotency_key") => {
                        observed.set(true);
                    }
                    Err(other) => {
                        return Err(PgError::InvalidArgument(format!(
                            "expected InvalidArgument(idempotency), got {other:?}"
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

fn conflict_surfaces_invalid_argument()
-> impl Fn(&Result<Outcome<ConflictRun>, String>) -> AssertionResult {
    observe("conflict→error kind", |run: &ConflictRun| {
        if run.second_push_was_conflict_error {
            Ok(())
        } else {
            Err("second push did not surface an idempotency_key InvalidArgument".into())
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

    expect(run_conflict_scenario().await) {
        when push_task_with_conn_collides_on_idempotency_key {
            to surfaces_invalid_argument_with_the_idempotency_hint {
                conflict_surfaces_invalid_argument()
            }
            to rolls_back_only_the_apalis_batch_via_savepoint {
                conflict_keeps_only_the_seed_job()
            }
            to leaves_the_outer_business_writes_intact {
                conflict_lets_outer_business_writes_commit()
            }
        }
    }
}
