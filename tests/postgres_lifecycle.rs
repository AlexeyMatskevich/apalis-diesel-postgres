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
    Config, PgAck, PgPool, PostgresStorage, build_pool, lock_task, setup,
};
use diesel::{RunQueryDsl, sql_query, sql_types::Text};
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
    /// The task id carried by the polled row (the pipeline cannot proceed
    /// without it; its presence is a hard precondition guarded upstream).
    polled_task_id: String,
    lock_outcome: Result<(), String>,
    ack_outcome: Result<(), String>,
    fetched_args: Option<String>,
    fetched_status: Option<Status>,
    /// The task id carried by the row fetched back by id, used to assert the
    /// id round-trips through `fetch_by_id` rather than asserting a literal.
    fetched_task_id: Option<String>,
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
    let ulid = *task_id.inner();

    let lock_outcome = lock_task(&pool, &ulid, worker.name())
        .await
        .map(|_| ())
        .map_err(|error| error.to_string());

    let mut ack = PgAck::new(pool.clone());
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
    let fetched_task_id = fetched
        .as_ref()
        .and_then(|task| task.parts.task_id.as_ref().map(|id| id.to_string()));

    cleanup_queue(pool, queue).await?;

    Ok(LifecycleOutcome::Completed(LifecycleRun {
        polled_payload,
        polled_task_id,
        lock_outcome,
        ack_outcome,
        fetched_args,
        fetched_status,
        fetched_task_id,
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

fn fetch_by_id_round_trips_the_task_id()
-> impl Fn(&Result<LifecycleOutcome, String>) -> AssertionResult {
    observe("task id round-trip", |run| match &run.fetched_task_id {
        Some(id) if *id == run.polled_task_id => Ok(()),
        Some(other) => Err(format!(
            "expected fetch_by_id to return task id {}, got {other}",
            run.polled_task_id
        )),
        None => Err("fetch_by_id returned a task without a task id".into()),
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

fn ack_marks_the_row_done() -> impl Fn(&Result<LifecycleOutcome, String>) -> AssertionResult {
    observe("ack", |run| {
        run.ack_outcome
            .as_ref()
            .map(|_| ())
            .map_err(|error| format!("expected ack to succeed, got error: {error}"))
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
            to round_trips_the_polled_task_id_through_fetch_by_id {
                fetch_by_id_round_trips_the_task_id()
            }
            to acquires_a_row_lock_for_the_worker {
                lock_task_acquires_the_row()
            }
            to acknowledges_the_completed_task {
                ack_marks_the_row_done()
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
