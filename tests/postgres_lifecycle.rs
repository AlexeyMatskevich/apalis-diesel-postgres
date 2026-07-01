#![cfg(feature = "tokio")]

mod support;

use std::time::Duration;

use apalis_core::{
    backend::{BackendExt, FetchById, TaskSink},
    error::BoxDynError,
    task::{attempt::Attempt, status::Status},
    worker::{context::WorkerContext, ext::ack::Acknowledge},
};
use apalis_diesel_postgres::{
    Config, PgAck, PgPool, PgTaskId, PostgresStorage, build_pool, lock_task, setup,
};
use diesel::{
    QueryableByName, RunQueryDsl, sql_query,
    sql_types::{Jsonb, Nullable, Text},
};
use futures::StreamExt;
use lets_expect::{AssertionError, AssertionResult, *};
use ulid::Ulid;

/// Observations from running the push → poll → lock → ack → fetch pipeline.
/// Each field is asserted by a dedicated `to` block; `lets_expect` re-runs the
/// subject once per block, so the (idempotent, freshly-queued, self-cleaning)
/// pipeline executes once per asserted observable.
#[derive(Debug)]
struct LifecycleRun {
    polled_payload: String,
    lock_outcome: Result<(), String>,
    ack_outcome: Result<(), String>,
    fetched_args: Option<String>,
    fetched_status: Option<Status>,
    /// Whether `fetch_by_id` returns `None` when handed a fresh, never-inserted
    /// task id. Unlike round-tripping the *matching* id (which the `WHERE id =
    /// $1` filter makes tautological), a miss exercises the filter's ability to
    /// reject a non-matching id and is not fixed by the setup.
    fetched_by_absent_id: Option<String>,
    /// The `last_result` JSONB persisted for the acked row, read straight from
    /// `apalis.jobs`. Asserts ack wrote the serialized `Ok("processed")` payload
    /// rather than only that the ack call returned `Ok(())`.
    acked_last_result: Option<serde_json::Value>,
}

#[derive(Debug)]
enum LifecycleOutcome {
    Skipped,
    Completed(LifecycleRun),
}

async fn cleanup_queue(pool: PgPool, queue: String) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        let mut conn = pool.get().map_err(|error| error.to_string())?;
        sql_query("DELETE FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(queue.clone())
            .execute(&mut conn)
            .map_err(|error| error.to_string())?;
        sql_query("DELETE FROM apalis.workers WHERE worker_type = $1")
            .bind::<Text, _>(queue)
            .execute(&mut conn)
            .map_err(|error| error.to_string())?;
        Ok(())
    })
    .await
    .map_err(|error| error.to_string())?
}

#[derive(QueryableByName)]
struct LastResultRow {
    #[diesel(sql_type = Nullable<Jsonb>)]
    last_result: Option<serde_json::Value>,
}

/// Read the `last_result` JSONB persisted for a given task id/queue directly
/// from `apalis.jobs`, so the ack leaf can assert the stored payload rather than
/// only that the ack call returned `Ok(())`.
async fn last_result_for_id(
    pool: PgPool,
    task_id: String,
    queue: String,
) -> Result<Option<serde_json::Value>, String> {
    tokio::task::spawn_blocking(move || {
        let mut conn = pool.get().map_err(|error| error.to_string())?;
        let rows = sql_query(
            "SELECT last_result FROM apalis.jobs WHERE id = $1 AND job_type = $2 LIMIT 1",
        )
        .bind::<Text, _>(task_id)
        .bind::<Text, _>(queue)
        .load::<LastResultRow>(&mut conn)
        .map_err(|error| error.to_string())?;
        Ok(rows.into_iter().next().and_then(|row| row.last_result))
    })
    .await
    .map_err(|error| error.to_string())?
}

async fn next_task(
    stream: &mut (
             impl futures::Stream<
        Item = Result<
            Option<apalis_diesel_postgres::PgTask<apalis_diesel_postgres::CompactType>>,
            apalis_diesel_postgres::Error,
        >,
    > + Unpin
         ),
) -> Result<apalis_diesel_postgres::PgTask<apalis_diesel_postgres::CompactType>, String> {
    let deadline = Duration::from_secs(5);
    loop {
        let item = tokio::time::timeout(deadline, stream.next())
            .await
            .map_err(|_| "timed out waiting for a queued task".to_owned())?
            .ok_or_else(|| "task stream ended".to_owned())?
            .map_err(|error| error.to_string())?;
        if let Some(task) = item {
            return Ok(task);
        }
    }
}

async fn run_lifecycle() -> Result<LifecycleOutcome, String> {
    let Some(database_url) = support::database_url_or_skip()? else {
        return Ok(LifecycleOutcome::Skipped);
    };

    let pool = build_pool(database_url).map_err(|error| error.to_string())?;
    setup(&pool).await.map_err(|error| error.to_string())?;

    let queue = format!("apalis-diesel-postgres-test-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let config = Config::new(&queue).set_buffer_size(1);
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    storage
        .push("payload".to_owned())
        .await
        .map_err(|error| error.to_string())?;

    let worker = WorkerContext::new::<()>("integration-worker");
    let mut compact_stream = storage.clone().poll_compact(&worker);
    let mut task = next_task(&mut compact_stream).await?;
    let polled_payload = String::from_utf8(task.args.clone()).map_err(|e| e.to_string())?;
    let task_id = task
        .parts
        .task_id
        .ok_or_else(|| "polled task had no task id".to_owned())?;
    let polled_task_id = task_id.to_string();

    let lock_outcome = lock_task(&pool, &task_id, worker.name())
        .await
        .map(|_| ())
        .map_err(|error| error.to_string());

    let mut ack = PgAck::new(&pool);
    task.parts.attempt = Attempt::new_with_value(1);
    let result: Result<String, BoxDynError> = Ok("processed".to_owned());
    let ack_outcome = ack
        .ack(&result, &task.parts)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string());

    let mut decoded = PostgresStorage::<String>::new_with_config(&pool, &config);
    let fetched = decoded
        .fetch_by_id(&task_id)
        .await
        .map_err(|error| error.to_string())?;
    let fetched_args = fetched.as_ref().map(|task| task.args.clone());
    let fetched_status = fetched.as_ref().map(|task| task.parts.status.load());

    // `fetch_by_id` for a fresh id that was never inserted must return `None`:
    // this exercises the `WHERE id = $1` filter rejecting a non-match, which the
    // matching-id lookup above cannot (any returned row necessarily has that id).
    let absent_id = PgTaskId::new(Ulid::new());
    let fetched_by_absent_id = decoded
        .fetch_by_id(&absent_id)
        .await
        .map_err(|error| error.to_string())?
        .map(|task| task.args.clone());

    // Read the persisted `last_result` directly so we can assert ack wrote the
    // serialized result, not merely that the ack call returned `Ok(())`.
    let acked_last_result = last_result_for_id(pool.clone(), polled_task_id, queue.clone()).await?;

    cleanup_queue(pool, queue).await?;

    Ok(LifecycleOutcome::Completed(LifecycleRun {
        polled_payload,
        lock_outcome,
        ack_outcome,
        fetched_args,
        fetched_status,
        fetched_by_absent_id,
        acked_last_result,
    }))
}

async fn lifecycle_outcome() -> Result<LifecycleOutcome, String> {
    run_lifecycle().await
}

fn observe<F>(
    name: &'static str,
    check: F,
) -> impl Fn(&Result<LifecycleOutcome, String>) -> AssertionResult
where
    F: Fn(&LifecycleRun) -> Result<(), String>,
{
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "{name}: lifecycle scenario failed: {error}"
        )])),
        Ok(LifecycleOutcome::Skipped) => Ok(()),
        Ok(LifecycleOutcome::Completed(run)) => {
            check(run).map_err(|reason| AssertionError::new(vec![format!("{name}: {reason}")]))
        }
    }
}

fn polled_payload_matches_pushed() -> impl Fn(&Result<LifecycleOutcome, String>) -> AssertionResult
{
    observe("polled payload", |run| {
        let serialized = serde_json::to_string(&"payload".to_string()).unwrap();
        if run.polled_payload == serialized {
            Ok(())
        } else {
            Err(format!(
                "expected payload {serialized:?}, got {:?}",
                run.polled_payload
            ))
        }
    })
}

fn fetch_by_id_misses_an_absent_id() -> impl Fn(&Result<LifecycleOutcome, String>) -> AssertionResult
{
    observe("fetch_by_id miss", |run| match &run.fetched_by_absent_id {
        None => Ok(()),
        Some(args) => Err(format!(
            "expected fetch_by_id for a never-inserted id to return None, got a row with args {args:?}"
        )),
    })
}

fn lock_task_acquires_the_row() -> impl Fn(&Result<LifecycleOutcome, String>) -> AssertionResult {
    observe("lock_task", |run| {
        run.lock_outcome
            .as_ref()
            .map(|_| ())
            .map_err(|error| format!("expected lock to succeed, got error: {error}"))
    })
}

fn ack_succeeds_and_persists_the_result()
-> impl Fn(&Result<LifecycleOutcome, String>) -> AssertionResult {
    observe("ack", |run| {
        run.ack_outcome
            .as_ref()
            .map_err(|error| format!("expected ack to succeed, got error: {error}"))?;
        // ack could return Ok yet write the wrong (or no) `last_result`; assert
        // the row carries the externally-tagged serialized `Ok("processed")`.
        let expected = serde_json::json!({ "Ok": "processed" });
        match &run.acked_last_result {
            Some(value) if *value == expected => Ok(()),
            Some(other) => Err(format!("expected last_result {expected}, got {other}")),
            None => Err("expected ack to persist last_result, got SQL NULL".into()),
        }
    })
}

fn fetch_by_id_returns_the_task() -> impl Fn(&Result<LifecycleOutcome, String>) -> AssertionResult {
    observe("fetch_by_id payload", |run| match &run.fetched_args {
        Some(args) if args == "payload" => Ok(()),
        Some(other) => Err(format!(
            "expected fetched args == \"payload\", got {other:?}"
        )),
        None => Err("fetch_by_id returned None for an acked task".into()),
    })
}

fn fetched_task_has_done_status() -> impl Fn(&Result<LifecycleOutcome, String>) -> AssertionResult {
    observe("fetched status", |run| match &run.fetched_status {
        Some(Status::Done) => Ok(()),
        Some(other) => Err(format!("expected Status::Done, got {other:?}")),
        None => Err("fetch_by_id returned None — cannot inspect status".into()),
    })
}

lets_expect! { #tokio_test
    expect(lifecycle_outcome().await) {
        when database_is_available_and_a_task_completes_one_full_pass {
            to polls_the_pushed_payload {
                polled_payload_matches_pushed()
            }
            to returns_none_when_fetch_by_id_is_given_an_absent_id {
                fetch_by_id_misses_an_absent_id()
            }
            to acquires_a_row_lock_for_the_worker {
                lock_task_acquires_the_row()
            }
            to acknowledges_and_persists_the_completed_result {
                ack_succeeds_and_persists_the_result()
            }
            to fetches_the_acked_task_back_by_id {
                fetch_by_id_returns_the_task()
            }
            to records_the_terminal_done_status {
                fetched_task_has_done_status()
            }
        }
    }
}
