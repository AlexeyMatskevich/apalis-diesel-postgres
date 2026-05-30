#![cfg(feature = "tokio")]

mod support;

use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use apalis_core::{
    backend::{
        Backend, FetchById, Filter, ListAllTasks, ListQueues, ListTasks, ListWorkers, Metrics,
        RegisterWorker, TaskSink, WaitForCompletion,
    },
    error::BoxDynError,
    task::{Task, attempt::Attempt, builder::TaskBuilder, status::Status, task_id::TaskId},
    worker::{context::WorkerContext, ext::ack::Acknowledge},
};
use apalis_diesel_postgres::{
    Config, PgAck, PgContext, PgPool, PgTask, PgTaskId, PostgresStorage, lock_task,
};
use apalis_sql::{DateTime, DateTimeExt, context::SqlContext};
use diesel::{
    PgConnection, QueryableByName, RunQueryDsl,
    r2d2::{ConnectionManager, Pool},
    sql_query,
    sql_types::{BigInt, Integer, Jsonb, Nullable, Text, Timestamptz},
};
use futures::StreamExt;
use lets_expect::{AssertionError, AssertionResult, *};
use serde_json::Value;
use ulid::Ulid;

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

#[derive(Debug, QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

#[derive(Debug, QueryableByName)]
struct StatusRow {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Integer)]
    attempts: i32,
    #[diesel(sql_type = Nullable<Jsonb>)]
    last_result: Option<Value>,
}

async fn test_pool() -> Result<Option<PgPool>, String> {
    support::shared_pool().await
}

fn invalid_pool() -> PgPool {
    let manager = ConnectionManager::<PgConnection>::new("postgres://127.0.0.1:1/not-used");
    Pool::builder()
        .max_size(1)
        .connection_timeout(Duration::from_millis(10))
        .build_unchecked(manager)
}

async fn with_conn<F, T>(pool: PgPool, work: F) -> Result<T, String>
where
    F: FnOnce(&mut diesel::PgConnection) -> Result<T, String> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut conn = pool.get().map_err(|error| error.to_string())?;
        work(&mut conn)
    })
    .await
    .map_err(|error| error.to_string())?
}

async fn cleanup_queue(pool: PgPool, queue: String) -> Result<(), String> {
    with_conn(pool, move |conn| {
        sql_query("DELETE FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(&queue)
            .execute(conn)
            .map_err(|error| error.to_string())?;
        sql_query("DELETE FROM apalis.workers WHERE worker_type = $1")
            .bind::<Text, _>(&queue)
            .execute(conn)
            .map_err(|error| error.to_string())?;
        Ok(())
    })
    .await
}

async fn count_jobs(pool: PgPool, queue: String) -> Result<i64, String> {
    with_conn(pool, move |conn| {
        sql_query("SELECT COUNT(*) AS count FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(queue)
            .load::<CountRow>(conn)
            .map_err(|error| error.to_string())?
            .into_iter()
            .next()
            .map(|row| row.count)
            .ok_or_else(|| "count query returned no rows".to_owned())
    })
    .await
}

async fn job_status(pool: PgPool, id: PgTaskId) -> Result<StatusRow, String> {
    with_conn(pool, move |conn| {
        sql_query("SELECT status, attempts, last_result FROM apalis.jobs WHERE id = $1")
            .bind::<Text, _>(id.to_string())
            .load::<StatusRow>(conn)
            .map_err(|error| error.to_string())?
            .into_iter()
            .next()
            .ok_or_else(|| "status query returned no rows".to_owned())
    })
    .await
}

async fn insert_completed_task(
    pool: PgPool,
    queue: String,
    payload: &'static str,
    status: &'static str,
    attempts: i32,
    max_attempts: i32,
    result: Value,
) -> Result<PgTaskId, String> {
    let id = Ulid::new();
    let task_id = TaskId::from_str(&id.to_string()).map_err(|error| error.to_string())?;
    let job = serde_json::to_vec(payload).map_err(|error| error.to_string())?;
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (
                id, job_type, job, status, attempts, max_attempts, run_at, last_result
            ) VALUES ($1, $2, $3, $4, $5, $6, now(), $7)",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(queue)
        .bind::<diesel::sql_types::Binary, _>(job)
        .bind::<Text, _>(status)
        .bind::<Integer, _>(attempts)
        .bind::<Integer, _>(max_attempts)
        .bind::<Jsonb, _>(result)
        .execute(conn)
        .map_err(|error| error.to_string())?;
        Ok(())
    })
    .await?;
    Ok(task_id)
}

async fn insert_pending_task(
    pool: PgPool,
    queue: String,
    job: Vec<u8>,
) -> Result<PgTaskId, String> {
    let id = Ulid::new();
    let task_id = TaskId::from_str(&id.to_string()).map_err(|error| error.to_string())?;
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (
                id, job_type, job, status, attempts, max_attempts, run_at
            ) VALUES ($1, $2, $3, 'Pending', 0, 25, now() - INTERVAL '1 second')",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(queue)
        .bind::<diesel::sql_types::Binary, _>(job)
        .execute(conn)
        .map_err(|error| error.to_string())?;
        Ok(())
    })
    .await?;
    Ok(task_id)
}

async fn insert_locked_task(
    pool: PgPool,
    queue: String,
    worker_id: String,
    status: &'static str,
    attempts: i32,
    max_attempts: i32,
    lock_at: DateTime,
) -> Result<PgTaskId, String> {
    let id = Ulid::new();
    let task_id = TaskId::from_str(&id.to_string()).map_err(|error| error.to_string())?;
    let job = serde_json::to_vec("will-fail").map_err(|error| error.to_string())?;
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (
                id, job_type, job, status, attempts, max_attempts, run_at, lock_by, lock_at
            ) VALUES ($1, $2, $3, $4, $5, $6, now() - INTERVAL '1 second', $7, $8)",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(queue)
        .bind::<diesel::sql_types::Binary, _>(job)
        .bind::<Text, _>(status)
        .bind::<Integer, _>(attempts)
        .bind::<Integer, _>(max_attempts)
        .bind::<Text, _>(worker_id)
        .bind::<Timestamptz, _>(lock_at)
        .execute(conn)
        .map_err(|error| error.to_string())?;
        Ok(())
    })
    .await?;
    Ok(task_id)
}

async fn insert_running_task(
    pool: PgPool,
    queue: String,
    worker_id: String,
    attempts: i32,
    max_attempts: i32,
    lock_at: DateTime,
) -> Result<PgTaskId, String> {
    insert_locked_task(
        pool,
        queue,
        worker_id,
        "Running",
        attempts,
        max_attempts,
        lock_at,
    )
    .await
}

async fn mark_worker_stale(pool: PgPool, queue: String, worker_id: String) -> Result<(), String> {
    with_conn(pool, move |conn| {
        sql_query(
            "UPDATE apalis.workers
             SET last_seen = now() - INTERVAL '10 seconds'
             WHERE worker_type = $1 AND id = $2",
        )
        .bind::<Text, _>(queue)
        .bind::<Text, _>(worker_id)
        .execute(conn)
        .map_err(|error| error.to_string())?;
        Ok(())
    })
    .await
}

async fn next_task<Args>(
    stream: &mut (
             impl futures::Stream<Item = Result<Option<PgTask<Args>>, apalis_diesel_postgres::Error>>
             + Unpin
         ),
) -> Result<PgTask<Args>, String> {
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

async fn next_stream_error<Args>(
    stream: &mut (
             impl futures::Stream<Item = Result<Option<PgTask<Args>>, apalis_diesel_postgres::Error>>
             + Unpin
         ),
) -> Result<String, String> {
    let deadline = Duration::from_secs(5);
    loop {
        let item = tokio::time::timeout(deadline, stream.next())
            .await
            .map_err(|_| "timed out waiting for a stream error".to_owned())?
            .ok_or_else(|| "task stream ended".to_owned())?;
        match item {
            Ok(Some(_)) => return Err("unexpected task arrived".to_owned()),
            Ok(None) => {}
            Err(error) => return Ok(error.to_string()),
        }
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_secs()
}

fn task_id() -> PgTaskId {
    TaskId::from_str(&Ulid::new().to_string()).expect("generated ULID parses as task id")
}

fn task(
    payload: &'static str,
    run_at: u64,
    attempts: usize,
    max_attempts: i32,
    idempotency_key: Option<&'static str>,
) -> Task<String, PgContext, Ulid> {
    let mut builder = TaskBuilder::new(payload.to_owned())
        .with_task_id(task_id())
        .run_at_timestamp(run_at)
        .with_attempt(Attempt::new_with_value(attempts))
        .with_ctx(SqlContext::new().with_max_attempts(max_attempts));
    if let Some(key) = idempotency_key {
        builder = builder.with_idempotency_key(key);
    }
    builder.build()
}

// ------------------------------ push / fetch ------------------------------

#[derive(Debug)]
struct PushFetchRun {
    first_args: String,
    second_args: String,
    delayed_args: Option<String>,
}

async fn run_push_fetch() -> Result<Outcome<PushFetchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-push-fetch-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let config = Config::new(&queue).set_buffer_size(5);
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let now = now_unix();
    let oldest = task("oldest", now - 60, 0, 25, None);
    let newer = task("newer", now - 30, 0, 25, None);
    let later = task("later", now + 3600, 0, 25, None);
    let later_id = later
        .parts
        .task_id
        .ok_or_else(|| "later task had no id".to_owned())?;

    storage.push_task(oldest).await.map_err(|e| e.to_string())?;
    storage.push_task(newer).await.map_err(|e| e.to_string())?;
    storage.push_task(later).await.map_err(|e| e.to_string())?;

    let worker = WorkerContext::new::<()>("query-push-fetch-worker");
    let mut stream = storage.clone().poll(&worker);
    let first = next_task(&mut stream).await?;
    let second = next_task(&mut stream).await?;
    let mut by_id = PostgresStorage::<String>::new_with_config(&pool, &config);
    let fetched_later = by_id
        .fetch_by_id(&later_id)
        .await
        .map_err(|e| e.to_string())?;

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(PushFetchRun {
        first_args: first.args,
        second_args: second.args,
        delayed_args: fetched_later.map(|t| t.args),
    }))
}

fn fetched_oldest_task_first() -> impl Fn(&Result<Outcome<PushFetchRun>, String>) -> AssertionResult
{
    observe::<PushFetchRun, _>("oldest first", |run| {
        if run.first_args == "oldest" {
            Ok(())
        } else {
            Err(format!(
                "expected the oldest task first, got {:?}",
                run.first_args
            ))
        }
    })
}

fn fetched_newer_task_second() -> impl Fn(&Result<Outcome<PushFetchRun>, String>) -> AssertionResult
{
    observe::<PushFetchRun, _>("newer second", |run| {
        if run.second_args == "newer" {
            Ok(())
        } else {
            Err(format!(
                "expected the newer task second, got {:?}",
                run.second_args
            ))
        }
    })
}

fn delayed_task_fetchable_by_id()
-> impl Fn(&Result<Outcome<PushFetchRun>, String>) -> AssertionResult {
    observe::<PushFetchRun, _>("delayed fetch_by_id", |run| match &run.delayed_args {
        Some(value) if value == "later" => Ok(()),
        Some(other) => Err(format!("expected delayed args == \"later\", got {other:?}")),
        None => Err("delayed task could not be fetched by id".into()),
    })
}

fn delayed_task_not_polled() -> impl Fn(&Result<Outcome<PushFetchRun>, String>) -> AssertionResult {
    observe::<PushFetchRun, _>("delayed task polling", |run| {
        let polled = [run.first_args.as_str(), run.second_args.as_str()];
        if polled.contains(&"later") {
            Err("future-dated task was polled before its run_at".into())
        } else {
            Ok(())
        }
    })
}

// ------------------------- fetch_by_id cross-queue ------------------------

#[derive(Debug)]
struct CrossQueueRun {
    fetched_args: Option<String>,
}

async fn run_fetch_by_id_cross_queue() -> Result<Outcome<CrossQueueRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let source_queue = format!("apalis-query-fetch-source-{}", Ulid::new());
    let observer_queue = format!("{source_queue}-observer");
    cleanup_queue(pool.clone(), source_queue.clone()).await?;
    cleanup_queue(pool.clone(), observer_queue.clone()).await?;

    let mut source = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&source_queue));
    let task = task("cross-queue", now_unix() - 1, 0, 25, None);
    let task_id = task
        .parts
        .task_id
        .ok_or_else(|| "cross-queue task had no id".to_owned())?;
    source.push_task(task).await.map_err(|e| e.to_string())?;

    let mut observer =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&observer_queue));
    let fetched = observer
        .fetch_by_id(&task_id)
        .await
        .map_err(|e| e.to_string())?;

    cleanup_queue(pool.clone(), source_queue).await?;
    cleanup_queue(pool, observer_queue).await?;
    Ok(Outcome::Completed(CrossQueueRun {
        fetched_args: fetched.map(|t| t.args),
    }))
}

fn cross_queue_fetch_returns_none()
-> impl Fn(&Result<Outcome<CrossQueueRun>, String>) -> AssertionResult {
    // fetch_by_id is scoped to the storage's configured queue (admin.rs:54);
    // an observer storage configured for a different queue must NOT see the
    // source queue's row even when the id matches.
    observe::<CrossQueueRun, _>("cross-queue fetch_by_id isolation", |run| {
        match &run.fetched_args {
            None => Ok(()),
            Some(other) => Err(format!(
                "expected fetch_by_id from a foreign queue to return None, got Some({other:?})"
            )),
        }
    })
}

// ------------------------------ idempotency -------------------------------

#[derive(Debug)]
struct IdempotencyRun {
    duplicate_rejected: bool,
    primary_count: i64,
    other_count: i64,
}

async fn run_idempotency(same_queue: bool) -> Result<Outcome<IdempotencyRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let primary_queue = format!("apalis-query-idempotency-{}", Ulid::new());
    let other_queue = format!("{primary_queue}-other");
    cleanup_queue(pool.clone(), primary_queue.clone()).await?;
    cleanup_queue(pool.clone(), other_queue.clone()).await?;

    let mut primary =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&primary_queue));
    let mut secondary = PostgresStorage::<String>::new_with_config(
        &pool,
        &Config::new(if same_queue {
            &primary_queue
        } else {
            &other_queue
        }),
    );

    primary
        .push_task(task("first", now_unix() - 1, 0, 25, Some("same-key")))
        .await
        .map_err(|e| e.to_string())?;
    let duplicate_result = secondary
        .push_task(task("duplicate", now_unix() - 1, 0, 25, Some("same-key")))
        .await;

    let primary_count = count_jobs(pool.clone(), primary_queue.clone()).await?;
    let other_count = count_jobs(pool.clone(), other_queue.clone()).await?;
    cleanup_queue(pool.clone(), primary_queue).await?;
    cleanup_queue(pool, other_queue).await?;

    Ok(Outcome::Completed(IdempotencyRun {
        duplicate_rejected: duplicate_result.is_err(),
        primary_count,
        other_count,
    }))
}

fn duplicate_push_rejected() -> impl Fn(&Result<Outcome<IdempotencyRun>, String>) -> AssertionResult
{
    observe::<IdempotencyRun, _>("duplicate push", |run| {
        if run.duplicate_rejected {
            Ok(())
        } else {
            Err("expected the second push to be rejected".into())
        }
    })
}

fn duplicate_push_accepted() -> impl Fn(&Result<Outcome<IdempotencyRun>, String>) -> AssertionResult
{
    observe::<IdempotencyRun, _>("duplicate push (different queue)", |run| {
        if run.duplicate_rejected {
            Err("expected the second push to be accepted in a different queue".into())
        } else {
            Ok(())
        }
    })
}

fn keeps_one_job_in_primary_queue()
-> impl Fn(&Result<Outcome<IdempotencyRun>, String>) -> AssertionResult {
    observe::<IdempotencyRun, _>("primary queue row count", |run| {
        if run.primary_count == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected exactly one row in the primary queue, got {}",
                run.primary_count
            ))
        }
    })
}

fn keeps_one_row_in_primary_queue_for_cross_queue_duplicate()
-> impl Fn(&Result<Outcome<IdempotencyRun>, String>) -> AssertionResult {
    observe::<IdempotencyRun, _>("primary queue row count (cross-queue)", |run| {
        if run.primary_count == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected one row in the primary queue, got {}",
                run.primary_count
            ))
        }
    })
}

fn keeps_one_row_in_secondary_queue_for_cross_queue_duplicate()
-> impl Fn(&Result<Outcome<IdempotencyRun>, String>) -> AssertionResult {
    observe::<IdempotencyRun, _>("secondary queue row count (cross-queue)", |run| {
        if run.other_count == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected one row in the secondary queue, got {}",
                run.other_count
            ))
        }
    })
}

fn other_queue_remains_empty_for_same_queue_duplicate()
-> impl Fn(&Result<Outcome<IdempotencyRun>, String>) -> AssertionResult {
    observe::<IdempotencyRun, _>("same-queue duplicate isolation", |run| {
        if run.other_count == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected the unrelated queue to stay empty, got {}",
                run.other_count
            ))
        }
    })
}

// ------------------------------ ack boundary ------------------------------

#[derive(Debug)]
struct AckBoundaryRun {
    status: String,
    attempts: i32,
    last_result_present: bool,
}

async fn run_ack_boundary(terminal: bool) -> Result<Outcome<AckBoundaryRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-ack-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let attempts: usize = if terminal { 2 } else { 1 };
    let worker_name = format!("query-ack-worker-{queue}");
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    storage
        .register_worker(worker_name.clone())
        .await
        .map_err(|e| e.to_string())?;
    let lock_at = <DateTime as DateTimeExt>::from_unix_timestamp(now_unix() as i64);
    let id = insert_running_task(
        pool.clone(),
        queue.clone(),
        worker_name.clone(),
        attempts.saturating_sub(1) as i32,
        2,
        lock_at,
    )
    .await?;
    let parts = TaskBuilder::new(())
        .with_task_id(id)
        .with_attempt(Attempt::new_with_value(attempts))
        .with_ctx(
            PgContext::new()
                .with_max_attempts(2)
                .with_queue(queue.clone())
                .with_lock_at(Some(lock_at.to_unix_timestamp()))
                .with_lock_by(Some(worker_name)),
        )
        .build()
        .parts;

    let mut ack = PgAck::new(pool.clone());
    let result: Result<String, BoxDynError> = Err(std::io::Error::other("failed").into());
    ack.ack(&result, &parts).await.map_err(|e| e.to_string())?;

    let status = job_status(pool.clone(), id).await?;
    cleanup_queue(pool, queue).await?;

    Ok(Outcome::Completed(AckBoundaryRun {
        status: status.status,
        attempts: status.attempts,
        last_result_present: status.last_result.is_some(),
    }))
}

fn ack_recorded_status(
    expected: &'static str,
) -> impl Fn(&Result<Outcome<AckBoundaryRun>, String>) -> AssertionResult {
    observe::<AckBoundaryRun, _>("ack status", move |run| {
        if run.status == expected {
            Ok(())
        } else {
            Err(format!(
                "expected status {expected:?}, got {:?}",
                run.status
            ))
        }
    })
}

fn ack_recorded_attempts(
    expected: i32,
) -> impl Fn(&Result<Outcome<AckBoundaryRun>, String>) -> AssertionResult {
    observe::<AckBoundaryRun, _>("ack attempts", move |run| {
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

fn ack_persisted_last_result()
-> impl Fn(&Result<Outcome<AckBoundaryRun>, String>) -> AssertionResult {
    observe::<AckBoundaryRun, _>("ack last_result", |run| {
        if run.last_result_present {
            Ok(())
        } else {
            Err("expected last_result to be persisted after ack".into())
        }
    })
}

// ------------------------------- ack stale --------------------------------

#[derive(Debug)]
struct AckStaleRun {
    ack_error: Option<String>,
    task_id: String,
    status: String,
    attempts: i32,
    last_result_present: bool,
}

async fn run_ack_stale() -> Result<Outcome<AckStaleRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-stale-ack-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let worker_name = format!("query-stale-ack-worker-{queue}");
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    storage
        .register_worker(worker_name.clone())
        .await
        .map_err(|e| e.to_string())?;
    let lock_at = <DateTime as DateTimeExt>::from_unix_timestamp(now_unix() as i64);
    let id = insert_running_task(
        pool.clone(),
        queue.clone(),
        worker_name.clone(),
        0,
        2,
        lock_at,
    )
    .await?;
    let parts = TaskBuilder::new(())
        .with_task_id(id)
        .with_attempt(Attempt::new_with_value(1))
        .with_ctx(
            PgContext::new()
                .with_max_attempts(2)
                .with_queue(queue.clone())
                .with_lock_at(Some(lock_at.to_unix_timestamp()))
                .with_lock_by(Some(format!("{worker_name}-stale"))),
        )
        .build()
        .parts;

    let mut ack = PgAck::new(pool.clone());
    let result: Result<String, BoxDynError> = Ok("processed".to_owned());
    let ack_result = ack.ack(&result, &parts).await;
    let status = job_status(pool.clone(), id).await?;
    cleanup_queue(pool, queue).await?;

    Ok(Outcome::Completed(AckStaleRun {
        ack_error: ack_result.err().map(|e| e.to_string()),
        task_id: id.to_string(),
        status: status.status,
        attempts: status.attempts,
        last_result_present: status.last_result.is_some(),
    }))
}

fn stale_ack_returns_error() -> impl Fn(&Result<Outcome<AckStaleRun>, String>) -> AssertionResult {
    observe::<AckStaleRun, _>("stale ack error", |run| match run.ack_error.as_deref() {
        Some(message)
            if message.starts_with(&format!("stale acknowledgement for task {}", run.task_id)) =>
        {
            Ok(())
        }
        other => Err(format!(
            "expected Error::StaleAcknowledgement embedding task {}, got {other:?}",
            run.task_id
        )),
    })
}

fn stale_ack_keeps_status_running()
-> impl Fn(&Result<Outcome<AckStaleRun>, String>) -> AssertionResult {
    observe::<AckStaleRun, _>("stale ack status", |run| {
        if run.status == "Running" {
            Ok(())
        } else {
            Err(format!(
                "expected row to remain Running, got status={:?}",
                run.status
            ))
        }
    })
}

fn stale_ack_does_not_increment_attempts()
-> impl Fn(&Result<Outcome<AckStaleRun>, String>) -> AssertionResult {
    observe::<AckStaleRun, _>("stale ack attempts", |run| {
        if run.attempts == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected attempts to stay 0 after a stale ack, got {}",
                run.attempts
            ))
        }
    })
}

fn stale_ack_does_not_write_last_result()
-> impl Fn(&Result<Outcome<AckStaleRun>, String>) -> AssertionResult {
    observe::<AckStaleRun, _>("stale ack last_result", |run| {
        if !run.last_result_present {
            Ok(())
        } else {
            Err("expected last_result to remain NULL after a stale ack".into())
        }
    })
}

// ------------------------------ lock boundary -----------------------------

#[derive(Debug)]
struct LockBoundaryRun {
    lock_succeeded: bool,
}

async fn run_lock_boundary(lockable: bool) -> Result<Outcome<LockBoundaryRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-lock-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let config = Config::new(&queue);
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let run_at = if lockable {
        now_unix() - 1
    } else {
        now_unix() + 3600
    };
    let task = task("lock-boundary", run_at, 0, 25, None);
    let id = task
        .parts
        .task_id
        .ok_or_else(|| "constructed task had no id".to_owned())?;
    storage.push_task(task).await.map_err(|e| e.to_string())?;

    let mut worker_storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let worker_name = format!("query-lock-worker-{queue}");
    worker_storage
        .register_worker(worker_name.clone())
        .await
        .map_err(|e| e.to_string())?;
    let lock_result = lock_task(&pool, id.inner(), &worker_name).await;
    cleanup_queue(pool, queue).await?;

    Ok(Outcome::Completed(LockBoundaryRun {
        lock_succeeded: lock_result.is_ok(),
    }))
}

fn lock_succeeded() -> impl Fn(&Result<Outcome<LockBoundaryRun>, String>) -> AssertionResult {
    observe::<LockBoundaryRun, _>("lock_task on due task", |run| {
        if run.lock_succeeded {
            Ok(())
        } else {
            Err("expected lock_task to acquire the row".into())
        }
    })
}

fn lock_refused() -> impl Fn(&Result<Outcome<LockBoundaryRun>, String>) -> AssertionResult {
    observe::<LockBoundaryRun, _>("lock_task on delayed task", |run| {
        if run.lock_succeeded {
            Err("expected lock_task to refuse a future-dated task".into())
        } else {
            Ok(())
        }
    })
}

// --------------------------- fetch ordering / concurrency ----------------

#[derive(Debug)]
struct PriorityOrderRun {
    polled_payloads: Vec<String>,
}

async fn run_priority_ordering() -> Result<Outcome<PriorityOrderRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-priority-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let config = Config::new(&queue).set_buffer_size(10);
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &config);

    // Insert in mixed order: low priority FIRST (older run_at), then high
    // priority (newer run_at), then mid priority. A naive ORDER BY run_at
    // would yield low → high → mid; the real ORDER BY priority DESC, run_at
    // ASC must yield high → mid → low irrespective of insertion order.
    let now = now_unix();
    let low = task("low", now - 90, 0, 25, None);
    let mut high_builder = TaskBuilder::new("high".to_owned())
        .with_task_id(task_id())
        .run_at_timestamp(now - 30)
        .with_attempt(Attempt::new_with_value(0))
        .with_ctx(SqlContext::new().with_max_attempts(25).with_priority(9));
    let _ = &mut high_builder;
    let high = high_builder.build();
    let mid = TaskBuilder::new("mid".to_owned())
        .with_task_id(task_id())
        .run_at_timestamp(now - 60)
        .with_attempt(Attempt::new_with_value(0))
        .with_ctx(SqlContext::new().with_max_attempts(25).with_priority(5))
        .build();

    storage.push_task(low).await.map_err(|e| e.to_string())?;
    storage.push_task(high).await.map_err(|e| e.to_string())?;
    storage.push_task(mid).await.map_err(|e| e.to_string())?;

    let worker = WorkerContext::new::<()>(&format!("priority-worker-{queue}"));
    let mut stream = storage.clone().poll(&worker);
    let first = next_task(&mut stream).await?;
    let second = next_task(&mut stream).await?;
    let third = next_task(&mut stream).await?;

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(PriorityOrderRun {
        polled_payloads: vec![first.args, second.args, third.args],
    }))
}

fn priority_ordering_high_then_mid_then_low()
-> impl Fn(&Result<Outcome<PriorityOrderRun>, String>) -> AssertionResult {
    observe::<PriorityOrderRun, _>("priority ordering", |run| {
        let expected = vec!["high".to_owned(), "mid".to_owned(), "low".to_owned()];
        if run.polled_payloads == expected {
            Ok(())
        } else {
            Err(format!(
                "expected polling order {expected:?}, got {:?}",
                run.polled_payloads
            ))
        }
    })
}

#[derive(Debug)]
struct SkipLockedRun {
    payloads: (String, String),
    distinct: bool,
}

async fn run_skip_locked_concurrency() -> Result<Outcome<SkipLockedRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-skip-locked-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let config = Config::new(&queue).set_buffer_size(1);
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let now = now_unix();
    storage
        .push_task(task("payload-a", now - 5, 0, 25, None))
        .await
        .map_err(|e| e.to_string())?;
    storage
        .push_task(task("payload-b", now - 4, 0, 25, None))
        .await
        .map_err(|e| e.to_string())?;

    let storage_a = PostgresStorage::<String>::new_with_config(&pool, &config);
    let storage_b = PostgresStorage::<String>::new_with_config(&pool, &config);
    let worker_a = WorkerContext::new::<()>(&format!("skip-locked-a-{queue}"));
    let worker_b = WorkerContext::new::<()>(&format!("skip-locked-b-{queue}"));

    let mut stream_a = storage_a.poll(&worker_a);
    let mut stream_b = storage_b.poll(&worker_b);
    let (task_a, task_b) = tokio::join!(next_task(&mut stream_a), next_task(&mut stream_b));
    let task_a = task_a?;
    let task_b = task_b?;

    cleanup_queue(pool, queue).await?;
    let distinct = task_a.args != task_b.args;
    Ok(Outcome::Completed(SkipLockedRun {
        payloads: (task_a.args, task_b.args),
        distinct,
    }))
}

fn skip_locked_distributes_distinct_rows()
-> impl Fn(&Result<Outcome<SkipLockedRun>, String>) -> AssertionResult {
    observe::<SkipLockedRun, _>("skip locked distribution", |run| {
        if run.distinct {
            Ok(())
        } else {
            Err(format!(
                "expected two workers to receive distinct payloads, got both {:?}",
                run.payloads
            ))
        }
    })
}

fn skip_locked_covers_the_pushed_set()
-> impl Fn(&Result<Outcome<SkipLockedRun>, String>) -> AssertionResult {
    observe::<SkipLockedRun, _>("skip locked coverage", |run| {
        let mut polled = vec![run.payloads.0.clone(), run.payloads.1.clone()];
        polled.sort();
        let expected = vec!["payload-a".to_owned(), "payload-b".to_owned()];
        if polled == expected {
            Ok(())
        } else {
            Err(format!(
                "expected payloads {expected:?} to be polled across both workers, got {polled:?}"
            ))
        }
    })
}

// --------------------------- lock_task status matrix ---------------------

#[derive(Debug, QueryableByName)]
struct StatusOnly {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Nullable<Text>)]
    lock_by: Option<String>,
}

#[derive(Debug)]
struct LockScenarioRun {
    lock_succeeded: bool,
    final_status: String,
    final_lock_by: Option<String>,
}

#[allow(clippy::too_many_arguments)]
async fn insert_status_row(
    pool: PgPool,
    queue: String,
    status: &'static str,
    attempts: i32,
    max_attempts: i32,
    run_at_offset_seconds: i64,
    lock_by: Option<String>,
    lock_at: Option<DateTime>,
) -> Result<PgTaskId, String> {
    let id = Ulid::new();
    let task_id = TaskId::from_str(&id.to_string()).map_err(|error| error.to_string())?;
    let job = serde_json::to_vec("lock-matrix").map_err(|error| error.to_string())?;
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (
                id, job_type, job, status, attempts, max_attempts, run_at, lock_by, lock_at
            ) VALUES (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                now() + ($7 || ' seconds')::interval,
                $8,
                $9
            )",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(queue)
        .bind::<diesel::sql_types::Binary, _>(job)
        .bind::<Text, _>(status)
        .bind::<Integer, _>(attempts)
        .bind::<Integer, _>(max_attempts)
        .bind::<Text, _>(run_at_offset_seconds.to_string())
        .bind::<Nullable<Text>, _>(lock_by)
        .bind::<Nullable<Timestamptz>, _>(lock_at)
        .execute(conn)
        .map_err(|error| error.to_string())?;
        Ok(())
    })
    .await?;
    Ok(task_id)
}

async fn fetch_status_only(pool: PgPool, id: PgTaskId) -> Result<StatusOnly, String> {
    with_conn(pool, move |conn| {
        sql_query("SELECT status, lock_by FROM apalis.jobs WHERE id = $1")
            .bind::<Text, _>(id.to_string())
            .load::<StatusOnly>(conn)
            .map_err(|error| error.to_string())?
            .into_iter()
            .next()
            .ok_or_else(|| "status row missing".to_owned())
    })
    .await
}

async fn run_lock_status_scenario(
    scenario: &'static str,
) -> Result<Outcome<LockScenarioRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-lock-matrix-{scenario}-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let primary_worker = format!("matrix-primary-{queue}");
    let other_worker = format!("matrix-other-{queue}");
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    storage
        .register_worker(primary_worker.clone())
        .await
        .map_err(|e| e.to_string())?;
    storage
        .register_worker(other_worker.clone())
        .await
        .map_err(|e| e.to_string())?;

    let now = <DateTime as DateTimeExt>::from_unix_timestamp(now_unix() as i64);

    // (status, attempts, max_attempts, run_at_offset_seconds, lock_by, lock_at)
    let (status, attempts, max_attempts, run_offset, lock_by, lock_at) = match scenario {
        "pending_due" => ("Pending", 0, 25, -1, None, None),
        "pending_future" => ("Pending", 0, 25, 3600, None, None),
        "queued_by_self" => ("Queued", 0, 25, -1, Some(primary_worker.clone()), Some(now)),
        "queued_by_other" => ("Queued", 0, 25, -1, Some(other_worker.clone()), Some(now)),
        "running_by_other" => ("Running", 0, 25, -1, Some(other_worker.clone()), Some(now)),
        "failed_retryable" => ("Failed", 1, 3, -1, None, None),
        "failed_exhausted" => ("Failed", 3, 3, -1, None, None),
        "done" => ("Done", 1, 3, -1, None, None),
        "killed" => ("Killed", 3, 3, -1, None, None),
        other => return Err(format!("unknown lock scenario: {other}")),
    };

    let id = insert_status_row(
        pool.clone(),
        queue.clone(),
        status,
        attempts,
        max_attempts,
        run_offset,
        lock_by,
        lock_at,
    )
    .await?;

    let lock_result = lock_task(&pool, id.inner(), &primary_worker).await;
    let row = fetch_status_only(pool.clone(), id).await?;
    cleanup_queue(pool, queue).await?;

    Ok(Outcome::Completed(LockScenarioRun {
        lock_succeeded: lock_result.is_ok(),
        final_status: row.status,
        final_lock_by: row.lock_by,
    }))
}

fn lock_matrix_succeeds() -> impl Fn(&Result<Outcome<LockScenarioRun>, String>) -> AssertionResult {
    observe::<LockScenarioRun, _>("lock_task matrix", |run| {
        if run.lock_succeeded {
            Ok(())
        } else {
            Err(format!(
                "expected lock to succeed for scenario, final_status={:?}, lock_by={:?}",
                run.final_status, run.final_lock_by
            ))
        }
    })
}

fn lock_matrix_refuses() -> impl Fn(&Result<Outcome<LockScenarioRun>, String>) -> AssertionResult {
    observe::<LockScenarioRun, _>("lock_task matrix", |run| {
        if run.lock_succeeded {
            Err(format!(
                "expected lock to be refused for scenario, but the row was locked: status={:?}, lock_by={:?}",
                run.final_status, run.final_lock_by
            ))
        } else {
            Ok(())
        }
    })
}

fn lock_matrix_status_equals(
    expected: &'static str,
) -> impl Fn(&Result<Outcome<LockScenarioRun>, String>) -> AssertionResult {
    observe::<LockScenarioRun, _>("lock_task matrix status", move |run| {
        if run.final_status == expected {
            Ok(())
        } else {
            Err(format!(
                "expected status {expected:?} after lock attempt, got {:?}",
                run.final_status
            ))
        }
    })
}

fn lock_matrix_owned_by(
    expected_substring: &'static str,
) -> impl Fn(&Result<Outcome<LockScenarioRun>, String>) -> AssertionResult {
    observe::<LockScenarioRun, _>("lock_task matrix lock_by", move |run| {
        match run.final_lock_by.as_deref() {
            Some(name) if name.contains(expected_substring) => Ok(()),
            other => Err(format!(
                "expected lock_by containing {expected_substring:?}, got {other:?}"
            )),
        }
    })
}

// ------------------------------ listing/metrics ---------------------------

#[derive(Debug)]
struct ListingMetricsRun {
    listed_tasks_current_queue_only: bool,
    all_tasks_includes_both_queues: bool,
    current_queue_worker_visible: bool,
    other_queue_worker_hidden_from_scoped_list: bool,
    other_queue_worker_visible_in_all_list: bool,
    queue_info_present: bool,
    pending_metric_present: bool,
    total_metric_present: bool,
}

async fn run_listing_metrics() -> Result<Outcome<ListingMetricsRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-listing-{}", Ulid::new());
    let other_queue = format!("{queue}-other");
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool.clone(), other_queue.clone()).await?;

    let config = Config::new(&queue);
    let other_config = Config::new(&other_queue);
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let mut other = PostgresStorage::<String>::new_with_config(&pool, &other_config);
    storage
        .push_task(task("pending", now_unix() - 1, 0, 25, None))
        .await
        .map_err(|e| e.to_string())?;
    other
        .push_task(task("other-pending", now_unix() - 1, 0, 25, None))
        .await
        .map_err(|e| e.to_string())?;
    storage
        .register_worker("listing-worker".to_owned())
        .await
        .map_err(|e| e.to_string())?;
    other
        .register_worker("other-listing-worker".to_owned())
        .await
        .map_err(|e| e.to_string())?;

    let filter = Filter {
        status: Some(Status::Pending),
        page: 1,
        page_size: Some(20),
    };
    let listed = storage
        .list_tasks(&filter)
        .await
        .map_err(|e| e.to_string())?;
    let all = storage
        .list_all_tasks(&filter)
        .await
        .map_err(|e| e.to_string())?;
    let workers = storage.list_workers().await.map_err(|e| e.to_string())?;
    let all_workers = storage
        .list_all_workers()
        .await
        .map_err(|e| e.to_string())?;
    let queues = storage.list_queues().await.map_err(|e| e.to_string())?;
    let queue_metrics = storage.fetch_by_queue().await.map_err(|e| e.to_string())?;
    let global_metrics = storage.global().await.map_err(|e| e.to_string())?;

    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool, other_queue.clone()).await?;

    let run = ListingMetricsRun {
        listed_tasks_current_queue_only: listed.len() == 1 && listed[0].args == "pending",
        all_tasks_includes_both_queues: all
            .iter()
            .filter(|t| {
                t.parts.ctx.queue().as_deref() == Some(queue.as_str())
                    || t.parts.ctx.queue().as_deref() == Some(other_queue.as_str())
            })
            .count()
            >= 2,
        current_queue_worker_visible: workers.iter().any(|w| w.id == "listing-worker"),
        other_queue_worker_hidden_from_scoped_list: !workers
            .iter()
            .any(|w| w.id == "other-listing-worker"),
        other_queue_worker_visible_in_all_list: all_workers
            .iter()
            .any(|w| w.id == "other-listing-worker"),
        queue_info_present: queues.iter().any(|info| info.name == queue),
        pending_metric_present: queue_metrics.iter().any(|s| s.title == "PENDING_JOBS"),
        total_metric_present: global_metrics.iter().any(|s| s.title == "TOTAL_JOBS"),
    };
    Ok(Outcome::Completed(run))
}

fn list_tasks_scoped_to_current_queue()
-> impl Fn(&Result<Outcome<ListingMetricsRun>, String>) -> AssertionResult {
    observe::<ListingMetricsRun, _>("list_tasks scope", |run| {
        if run.listed_tasks_current_queue_only {
            Ok(())
        } else {
            Err("list_tasks should return only this queue's pending tasks".into())
        }
    })
}

fn list_all_tasks_covers_all_queues()
-> impl Fn(&Result<Outcome<ListingMetricsRun>, String>) -> AssertionResult {
    observe::<ListingMetricsRun, _>("list_all_tasks scope", |run| {
        if run.all_tasks_includes_both_queues {
            Ok(())
        } else {
            Err("list_all_tasks should return rows from every queue".into())
        }
    })
}

fn list_workers_includes_current_queue_worker()
-> impl Fn(&Result<Outcome<ListingMetricsRun>, String>) -> AssertionResult {
    observe::<ListingMetricsRun, _>("list_workers includes current", |run| {
        if run.current_queue_worker_visible {
            Ok(())
        } else {
            Err("list_workers should include this queue's worker".into())
        }
    })
}

fn list_workers_hides_other_queue_workers()
-> impl Fn(&Result<Outcome<ListingMetricsRun>, String>) -> AssertionResult {
    observe::<ListingMetricsRun, _>("list_workers hides others", |run| {
        if run.other_queue_worker_hidden_from_scoped_list {
            Ok(())
        } else {
            Err("list_workers should not surface workers from a different queue".into())
        }
    })
}

fn list_all_workers_surfaces_other_queue_workers()
-> impl Fn(&Result<Outcome<ListingMetricsRun>, String>) -> AssertionResult {
    observe::<ListingMetricsRun, _>("list_all_workers spans queues", |run| {
        if run.other_queue_worker_visible_in_all_list {
            Ok(())
        } else {
            Err("list_all_workers should include workers from other queues".into())
        }
    })
}

fn list_queues_includes_active_queue()
-> impl Fn(&Result<Outcome<ListingMetricsRun>, String>) -> AssertionResult {
    observe::<ListingMetricsRun, _>("list_queues", |run| {
        if run.queue_info_present {
            Ok(())
        } else {
            Err("list_queues should include the active queue".into())
        }
    })
}

fn queue_metrics_include_pending_jobs()
-> impl Fn(&Result<Outcome<ListingMetricsRun>, String>) -> AssertionResult {
    observe::<ListingMetricsRun, _>("queue metrics", |run| {
        if run.pending_metric_present {
            Ok(())
        } else {
            Err("expected PENDING_JOBS in queue metrics".into())
        }
    })
}

fn global_metrics_include_total_jobs()
-> impl Fn(&Result<Outcome<ListingMetricsRun>, String>) -> AssertionResult {
    observe::<ListingMetricsRun, _>("global metrics", |run| {
        if run.total_metric_present {
            Ok(())
        } else {
            Err("expected TOTAL_JOBS in global metrics".into())
        }
    })
}

// ------------------------------ completion --------------------------------

#[derive(Debug)]
struct CompletionRun {
    status: Status,
    payload: Result<String, String>,
}

async fn run_completion_check_status() -> Result<Outcome<CompletionRun>, String> {
    completion_case(false).await
}

async fn run_completion_wait_for() -> Result<Outcome<CompletionRun>, String> {
    completion_case(true).await
}

async fn completion_case(waiting: bool) -> Result<Outcome<CompletionRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-completion-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let id = insert_completed_task(
        pool.clone(),
        queue.clone(),
        "completed",
        "Done",
        1,
        2,
        serde_json::json!({"Ok": "processed"}),
    )
    .await?;

    let result = if waiting {
        let mut stream =
            <PostgresStorage<String> as WaitForCompletion<String>>::wait_for(&storage, [id]);
        tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timed out waiting for completion".to_owned())?
            .ok_or_else(|| "completion stream ended".to_owned())?
            .map_err(|error| error.to_string())?
    } else {
        let results =
            <PostgresStorage<String> as WaitForCompletion<String>>::check_status(&storage, [id])
                .await
                .map_err(|e| e.to_string())?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| "check_status returned no rows".to_owned())?
    };
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(CompletionRun {
        status: result.status,
        payload: result.result.map_err(|e| e.to_string()),
    }))
}

async fn run_completion_cross_check() -> Result<Outcome<CompletionRun>, String> {
    completion_cross_queue(false).await
}

async fn run_completion_cross_wait() -> Result<Outcome<CompletionRun>, String> {
    completion_cross_queue(true).await
}

async fn completion_cross_queue(waiting: bool) -> Result<Outcome<CompletionRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let source_queue = format!("apalis-query-completion-source-{}", Ulid::new());
    let observer_queue = format!("{source_queue}-observer");
    cleanup_queue(pool.clone(), source_queue.clone()).await?;
    cleanup_queue(pool.clone(), observer_queue.clone()).await?;

    let observer = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&observer_queue));
    let id = insert_completed_task(
        pool.clone(),
        source_queue.clone(),
        "completed",
        "Done",
        1,
        2,
        serde_json::json!({"Ok": "processed"}),
    )
    .await?;

    let result = if waiting {
        let mut stream =
            <PostgresStorage<String> as WaitForCompletion<String>>::wait_for(&observer, [id]);
        tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timed out waiting for cross-queue completion".to_owned())?
            .ok_or_else(|| "cross-queue completion stream ended".to_owned())?
            .map_err(|e| e.to_string())?
    } else {
        let results =
            <PostgresStorage<String> as WaitForCompletion<String>>::check_status(&observer, [id])
                .await
                .map_err(|e| e.to_string())?;
        results
            .into_iter()
            .next()
            .ok_or_else(|| "cross-queue check_status returned no rows".to_owned())?
    };
    cleanup_queue(pool.clone(), source_queue).await?;
    cleanup_queue(pool, observer_queue).await?;
    Ok(Outcome::Completed(CompletionRun {
        status: result.status,
        payload: result.result.map_err(|e| e.to_string()),
    }))
}

fn completion_reports_done_status()
-> impl Fn(&Result<Outcome<CompletionRun>, String>) -> AssertionResult {
    observe::<CompletionRun, _>("completion status", |run| {
        if run.status == Status::Done {
            Ok(())
        } else {
            Err(format!("expected Status::Done, got {:?}", run.status))
        }
    })
}

fn completion_carries_decoded_payload()
-> impl Fn(&Result<Outcome<CompletionRun>, String>) -> AssertionResult {
    observe::<CompletionRun, _>("completion payload", |run| match &run.payload {
        Ok(value) if value == "processed" => Ok(()),
        Ok(other) => Err(format!("expected \"processed\", got {other:?}")),
        Err(error) => Err(format!("expected Ok payload, got error: {error}")),
    })
}

// ----------------------------- wait edge cases ----------------------------

#[derive(Debug)]
struct WaitEmptyRun {
    stream_ended_immediately: bool,
}

async fn run_wait_empty() -> Result<Outcome<WaitEmptyRun>, String> {
    let storage = PostgresStorage::<String>::new_with_config(
        &invalid_pool(),
        &Config::new("apalis-query-wait-empty"),
    );
    let mut stream = <PostgresStorage<String> as WaitForCompletion<String>>::wait_for(&storage, []);
    let ended = stream.next().await.is_none();
    Ok(Outcome::Completed(WaitEmptyRun {
        stream_ended_immediately: ended,
    }))
}

fn wait_empty_terminates_without_db()
-> impl Fn(&Result<Outcome<WaitEmptyRun>, String>) -> AssertionResult {
    observe::<WaitEmptyRun, _>("wait_for empty", |run| {
        if run.stream_ended_immediately {
            Ok(())
        } else {
            Err("expected wait_for(no ids) to end immediately".into())
        }
    })
}

#[derive(Debug)]
struct WaitErrorRun {
    received_error: bool,
}

async fn run_wait_error() -> Result<Outcome<WaitErrorRun>, String> {
    let storage = PostgresStorage::<String>::new_with_config(
        &invalid_pool(),
        &Config::new("apalis-query-wait-error"),
    );
    let mut stream =
        <PostgresStorage<String> as WaitForCompletion<String>>::wait_for(&storage, [task_id()]);
    let item = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .map_err(|_| "timed out waiting for wait_for error".to_owned())?
        .ok_or_else(|| "wait_for stream ended before returning an error".to_owned())?;
    Ok(Outcome::Completed(WaitErrorRun {
        received_error: item.is_err(),
    }))
}

fn wait_error_surfaces_db_error()
-> impl Fn(&Result<Outcome<WaitErrorRun>, String>) -> AssertionResult {
    observe::<WaitErrorRun, _>("wait_for db error", |run| {
        if run.received_error {
            Ok(())
        } else {
            Err("expected wait_for to surface a database error".into())
        }
    })
}

#[derive(Debug)]
struct WaitPendingRun {
    timed_out_without_result: bool,
}

async fn run_wait_pending() -> Result<Outcome<WaitPendingRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-wait-pending-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let id = insert_pending_task(
        pool.clone(),
        queue.clone(),
        serde_json::to_vec("pending").map_err(|e| e.to_string())?,
    )
    .await?;

    let mut stream =
        <PostgresStorage<String> as WaitForCompletion<String>>::wait_for(&storage, [id]);
    let timed_out = tokio::time::timeout(Duration::from_millis(100), stream.next())
        .await
        .is_err();
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(WaitPendingRun {
        timed_out_without_result: timed_out,
    }))
}

fn wait_pending_does_not_complete_early()
-> impl Fn(&Result<Outcome<WaitPendingRun>, String>) -> AssertionResult {
    observe::<WaitPendingRun, _>("wait_for pending", |run| {
        if run.timed_out_without_result {
            Ok(())
        } else {
            Err("wait_for produced a terminal result for a non-terminal task".into())
        }
    })
}

#[derive(Debug)]
struct WaitMalformedRun {
    first_item_was_error: bool,
    stream_ended_after_one_item: bool,
}

async fn run_wait_malformed_terminal() -> Result<Outcome<WaitMalformedRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-wait-malformed-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let id = insert_completed_task(
        pool.clone(),
        queue.clone(),
        "completed",
        "Done",
        1,
        2,
        serde_json::json!({"unexpected": true}),
    )
    .await?;

    let mut stream =
        <PostgresStorage<String> as WaitForCompletion<String>>::wait_for(&storage, [id]);
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .map_err(|_| "timed out waiting for malformed completion".to_owned())?
        .ok_or_else(|| "completion stream ended before returning malformed result".to_owned())?;
    let ended = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .map_err(|_| "wait_for kept retrying malformed terminal result".to_owned())?
        .is_none();
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(WaitMalformedRun {
        first_item_was_error: first.is_err(),
        stream_ended_after_one_item: ended,
    }))
}

fn malformed_wait_first_yields_decode_error()
-> impl Fn(&Result<Outcome<WaitMalformedRun>, String>) -> AssertionResult {
    observe::<WaitMalformedRun, _>("malformed wait first item", |run| {
        if run.first_item_was_error {
            Ok(())
        } else {
            Err("expected wait_for to surface a decode error for a malformed terminal".into())
        }
    })
}

fn malformed_wait_finishes_after_one()
-> impl Fn(&Result<Outcome<WaitMalformedRun>, String>) -> AssertionResult {
    observe::<WaitMalformedRun, _>("malformed wait termination", |run| {
        if run.stream_ended_after_one_item {
            Ok(())
        } else {
            Err("wait_for should stop after yielding the decode error".into())
        }
    })
}

// ------------------------------ zero buffer -------------------------------

#[derive(Debug)]
struct ZeroBufferRun {
    polled_payload: String,
}

async fn run_zero_buffer_fetch() -> Result<Outcome<ZeroBufferRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-zero-buffer-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    insert_pending_task(
        pool.clone(),
        queue.clone(),
        serde_json::to_vec("zero-buffer").map_err(|e| e.to_string())?,
    )
    .await?;

    let storage =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(0));
    let worker = WorkerContext::new::<()>(&format!("query-zero-buffer-worker-{queue}"));
    let mut stream = storage.poll(&worker);
    let task = next_task(&mut stream).await?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(ZeroBufferRun {
        polled_payload: task.args,
    }))
}

fn zero_buffer_still_fetches_one()
-> impl Fn(&Result<Outcome<ZeroBufferRun>, String>) -> AssertionResult {
    observe::<ZeroBufferRun, _>("zero-buffer polling", |run| {
        if run.polled_payload == "zero-buffer" {
            Ok(())
        } else {
            Err(format!(
                "expected payload \"zero-buffer\", got {:?}",
                run.polled_payload
            ))
        }
    })
}

// ----------------------- orphan reenqueue ---------------------------------

#[derive(Debug)]
struct OrphanRun {
    status: String,
    attempts: i32,
    last_result_present: bool,
}

async fn run_orphan_reenqueue(queued: bool, terminal: bool) -> Result<Outcome<OrphanRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-orphan-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let config = Config::new(&queue).set_reenqueue_orphaned_after(Duration::from_secs(1));
    let stale_worker = format!("query-orphan-stale-worker-{queue}");
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    storage
        .register_worker(stale_worker.clone())
        .await
        .map_err(|e| e.to_string())?;
    let attempts = if terminal { 1 } else { 0 };
    let max_attempts = if terminal { 2 } else { 3 };
    let lock_at = <DateTime as DateTimeExt>::from_unix_timestamp(now_unix() as i64);
    let id = insert_locked_task(
        pool.clone(),
        queue.clone(),
        stale_worker.clone(),
        if queued { "Queued" } else { "Running" },
        attempts,
        max_attempts,
        lock_at,
    )
    .await?;
    mark_worker_stale(pool.clone(), queue.clone(), stale_worker).await?;

    let new_worker = WorkerContext::new::<()>(&format!("query-orphan-new-worker-{queue}"));
    let mut stream = storage.clone().poll(&new_worker);
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .map_err(|_| "timed out waiting for initial heartbeat".to_owned())?
        .ok_or_else(|| "poll stream ended during initial heartbeat".to_owned())?
        .map_err(|e| e.to_string())?;
    let status = job_status(pool.clone(), id).await?;
    cleanup_queue(pool, queue).await?;

    Ok(Outcome::Completed(OrphanRun {
        status: status.status,
        attempts: status.attempts,
        last_result_present: status.last_result.is_some(),
    }))
}

fn orphan_status_equals(
    expected: &'static str,
) -> impl Fn(&Result<Outcome<OrphanRun>, String>) -> AssertionResult {
    observe::<OrphanRun, _>("orphan status", move |run| {
        if run.status == expected {
            Ok(())
        } else {
            Err(format!(
                "expected status {expected:?}, got {:?}",
                run.status
            ))
        }
    })
}

fn orphan_attempts_equals(
    expected: i32,
) -> impl Fn(&Result<Outcome<OrphanRun>, String>) -> AssertionResult {
    observe::<OrphanRun, _>("orphan attempts", move |run| {
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

fn orphan_recorded_last_result() -> impl Fn(&Result<Outcome<OrphanRun>, String>) -> AssertionResult
{
    observe::<OrphanRun, _>("orphan last_result", |run| {
        if run.last_result_present {
            Ok(())
        } else {
            Err("expected last_result to be recorded after re-enqueue/kill".into())
        }
    })
}

// ----------------------------- poll decode --------------------------------

#[derive(Debug)]
struct PollDecodeRun {
    error: String,
}

async fn run_poll_decode_basic() -> Result<Outcome<PollDecodeRun>, String> {
    poll_decode(false).await
}

async fn run_poll_decode_notify() -> Result<Outcome<PollDecodeRun>, String> {
    poll_decode(true).await
}

async fn poll_decode(notify: bool) -> Result<Outcome<PollDecodeRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-decode-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    insert_pending_task(pool.clone(), queue.clone(), b"not-json".to_vec()).await?;

    let worker = WorkerContext::new::<()>(&format!("query-decode-worker-{queue}"));
    let error = if notify {
        let storage = PostgresStorage::<String>::new_with_notify(&pool, &Config::new(&queue));
        let mut stream = storage.poll(&worker);
        next_stream_error(&mut stream).await?
    } else {
        let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
        let mut stream = storage.poll(&worker);
        next_stream_error(&mut stream).await?
    };
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(PollDecodeRun { error }))
}

fn poll_decode_error_mentions_payload()
-> impl Fn(&Result<Outcome<PollDecodeRun>, String>) -> AssertionResult {
    observe::<PollDecodeRun, _>("poll decode error", |run| {
        if run.error.contains("failed to decode task payload") {
            Ok(())
        } else {
            Err(format!(
                "expected decode error to mention payload decoding, got {:?}",
                run.error
            ))
        }
    })
}

// -------------------- Additional boundary scenarios -----------------------

#[derive(Debug)]
struct IdempotencyNoKeysRun {
    second_push_accepted: bool,
    row_count: i64,
}

async fn run_idempotency_without_keys() -> Result<Outcome<IdempotencyNoKeysRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-nokey-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    storage
        .push_task(task("first", now_unix() - 1, 0, 25, None))
        .await
        .map_err(|e| e.to_string())?;
    let second = storage
        .push_task(task("second", now_unix() - 1, 0, 25, None))
        .await;
    let row_count = count_jobs(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(IdempotencyNoKeysRun {
        second_push_accepted: second.is_ok(),
        row_count,
    }))
}

fn no_key_second_push_is_accepted()
-> impl Fn(&Result<Outcome<IdempotencyNoKeysRun>, String>) -> AssertionResult {
    observe::<IdempotencyNoKeysRun, _>("second push without idempotency key", |run| {
        if run.second_push_accepted {
            Ok(())
        } else {
            Err("two pushes with idempotency_key=None should both be accepted".into())
        }
    })
}

fn no_key_pushes_create_two_rows()
-> impl Fn(&Result<Outcome<IdempotencyNoKeysRun>, String>) -> AssertionResult {
    observe::<IdempotencyNoKeysRun, _>("no-key row count", |run| {
        if run.row_count == 2 {
            Ok(())
        } else {
            Err(format!("expected two rows, got {}", run.row_count))
        }
    })
}

#[derive(Debug)]
struct LockAlreadyHeldRun {
    second_lock_failed: bool,
    status: String,
    lock_by: Option<String>,
}

#[derive(Debug, QueryableByName)]
struct LockByRow {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Nullable<Text>)]
    lock_by: Option<String>,
}

async fn run_lock_already_held() -> Result<Outcome<LockAlreadyHeldRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-lock-held-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let first_worker = format!("query-lock-held-first-{queue}");
    let second_worker = format!("query-lock-held-second-{queue}");
    storage
        .register_worker(first_worker.clone())
        .await
        .map_err(|e| e.to_string())?;
    storage
        .register_worker(second_worker.clone())
        .await
        .map_err(|e| e.to_string())?;
    let task = task("already-locked", now_unix() - 1, 0, 25, None);
    let id = task.parts.task_id.expect("task has id");
    storage.push_task(task).await.map_err(|e| e.to_string())?;

    lock_task(&pool, id.inner(), &first_worker)
        .await
        .map_err(|e| format!("first lock failed: {e}"))?;
    let second = lock_task(&pool, id.inner(), &second_worker).await;

    let id_text = id.to_string();
    let row = with_conn(pool.clone(), move |conn| {
        sql_query("SELECT status, lock_by FROM apalis.jobs WHERE id = $1")
            .bind::<Text, _>(id_text)
            .load::<LockByRow>(conn)
            .map_err(|e| e.to_string())?
            .into_iter()
            .next()
            .ok_or_else(|| "row missing".to_owned())
    })
    .await?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(LockAlreadyHeldRun {
        second_lock_failed: second.is_err(),
        status: row.status,
        lock_by: row.lock_by,
    }))
}

fn second_lock_is_refused()
-> impl Fn(&Result<Outcome<LockAlreadyHeldRun>, String>) -> AssertionResult {
    observe::<LockAlreadyHeldRun, _>("second lock_task refusal", |run| {
        if run.second_lock_failed {
            Ok(())
        } else {
            Err("expected lock_task on an already-locked row to fail".into())
        }
    })
}

fn original_lock_holder_is_preserved()
-> impl Fn(&Result<Outcome<LockAlreadyHeldRun>, String>) -> AssertionResult {
    observe::<LockAlreadyHeldRun, _>("lock_by stability", |run| match run.lock_by.as_deref() {
        Some(name) if name.contains("first") => Ok(()),
        other => Err(format!(
            "expected lock_by to remain the first worker, got {other:?}"
        )),
    })
}

fn locked_row_status_remains_running()
-> impl Fn(&Result<Outcome<LockAlreadyHeldRun>, String>) -> AssertionResult {
    observe::<LockAlreadyHeldRun, _>("locked row status", |run| {
        if run.status == "Running" {
            Ok(())
        } else {
            Err(format!(
                "expected status to stay Running for the locked row, got {:?}",
                run.status
            ))
        }
    })
}

#[derive(Debug)]
struct AckOnPendingRun {
    ack_error: Option<String>,
    task_id: String,
    status: String,
    attempts: i32,
    last_result_present: bool,
}

async fn run_ack_on_pending_row() -> Result<Outcome<AckOnPendingRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-ack-pending-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_name = format!("query-ack-pending-worker-{queue}");
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    storage
        .register_worker(worker_name.clone())
        .await
        .map_err(|e| e.to_string())?;
    let id = insert_pending_task(
        pool.clone(),
        queue.clone(),
        serde_json::to_vec("pending").map_err(|e| e.to_string())?,
    )
    .await?;

    let parts = TaskBuilder::new(())
        .with_task_id(id)
        .with_attempt(Attempt::new_with_value(1))
        .with_ctx(
            PgContext::new()
                .with_max_attempts(25)
                .with_queue(queue.clone())
                .with_lock_at(Some(now_unix() as i64))
                .with_lock_by(Some(worker_name)),
        )
        .build()
        .parts;

    let mut ack = PgAck::new(pool.clone());
    let result: Result<String, BoxDynError> = Ok("processed".to_owned());
    let ack_result = ack.ack(&result, &parts).await;
    let status = job_status(pool.clone(), id).await?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(AckOnPendingRun {
        ack_error: ack_result.err().map(|e| e.to_string()),
        task_id: id.to_string(),
        status: status.status,
        attempts: status.attempts,
        last_result_present: status.last_result.is_some(),
    }))
}

fn ack_on_pending_row_is_rejected()
-> impl Fn(&Result<Outcome<AckOnPendingRun>, String>) -> AssertionResult {
    observe::<AckOnPendingRun, _>("ack on pending error", |run| {
        match run.ack_error.as_deref() {
            Some(message)
                if message
                    .starts_with(&format!("stale acknowledgement for task {}", run.task_id)) =>
            {
                Ok(())
            }
            other => Err(format!(
                "expected Error::StaleAcknowledgement embedding task {}, got {other:?}",
                run.task_id
            )),
        }
    })
}

fn pending_row_status_unchanged_after_rejected_ack()
-> impl Fn(&Result<Outcome<AckOnPendingRun>, String>) -> AssertionResult {
    observe::<AckOnPendingRun, _>("ack on pending status", |run| {
        if run.status == "Pending" {
            Ok(())
        } else {
            Err(format!(
                "expected status to stay Pending, got {:?}",
                run.status
            ))
        }
    })
}

fn pending_row_attempts_unchanged_after_rejected_ack()
-> impl Fn(&Result<Outcome<AckOnPendingRun>, String>) -> AssertionResult {
    observe::<AckOnPendingRun, _>("ack on pending attempts", |run| {
        if run.attempts == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected attempts to stay 0 on Pending row, got {}",
                run.attempts
            ))
        }
    })
}

fn pending_row_last_result_unchanged_after_rejected_ack()
-> impl Fn(&Result<Outcome<AckOnPendingRun>, String>) -> AssertionResult {
    observe::<AckOnPendingRun, _>("ack on pending last_result", |run| {
        if !run.last_result_present {
            Ok(())
        } else {
            Err("ack to a Pending row should not write last_result".into())
        }
    })
}

#[derive(Debug)]
struct WaitMixedRun {
    first_item_status: Option<Status>,
    pending_item_resolved_quickly: bool,
}

async fn run_wait_for_mixed() -> Result<Outcome<WaitMixedRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-wait-mixed-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let pending_id = insert_pending_task(
        pool.clone(),
        queue.clone(),
        serde_json::to_vec("pending").map_err(|e| e.to_string())?,
    )
    .await?;
    let terminal_id = insert_completed_task(
        pool.clone(),
        queue.clone(),
        "completed",
        "Done",
        1,
        2,
        serde_json::json!({"Ok": "processed"}),
    )
    .await?;

    let mut stream = <PostgresStorage<String> as WaitForCompletion<String>>::wait_for(
        &storage,
        [pending_id, terminal_id],
    );
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .map_err(|_| "timed out waiting for first mixed completion".to_owned())?
        .ok_or_else(|| "completion stream ended without items".to_owned())?
        .map_err(|e| e.to_string())?;
    let pending_quick = tokio::time::timeout(Duration::from_millis(150), stream.next())
        .await
        .is_err();
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(WaitMixedRun {
        first_item_status: Some(first.status),
        pending_item_resolved_quickly: !pending_quick,
    }))
}

fn mixed_wait_yields_terminal_first()
-> impl Fn(&Result<Outcome<WaitMixedRun>, String>) -> AssertionResult {
    observe::<WaitMixedRun, _>("mixed wait first item", |run| {
        match &run.first_item_status {
            Some(Status::Done) => Ok(()),
            other => Err(format!(
                "expected first wait_for item to be the terminal task (Done), got {other:?}"
            )),
        }
    })
}

fn mixed_wait_keeps_pending_open()
-> impl Fn(&Result<Outcome<WaitMixedRun>, String>) -> AssertionResult {
    observe::<WaitMixedRun, _>("mixed wait pending stays open", |run| {
        if run.pending_item_resolved_quickly {
            Err("wait_for should not resolve the pending id until it terminates".into())
        } else {
            Ok(())
        }
    })
}

// ---------------------- lock_task missing-row branch ----------------------

#[derive(Debug)]
struct LockMissingRowRun {
    lock_error: Option<String>,
    phantom_id: String,
}

async fn run_lock_missing_row() -> Result<Outcome<LockMissingRowRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-lock-missing-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let worker_name = format!("query-lock-missing-worker-{queue}");
    storage
        .register_worker(worker_name.clone())
        .await
        .map_err(|e| e.to_string())?;

    let phantom_id = Ulid::new();
    let result = lock_task(&pool, &phantom_id, &worker_name).await;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(LockMissingRowRun {
        lock_error: result.err().map(|e| e.to_string()),
        phantom_id: phantom_id.to_string(),
    }))
}

fn lock_missing_row_rejected()
-> impl Fn(&Result<Outcome<LockMissingRowRun>, String>) -> AssertionResult {
    // The Display for `Error::TaskNotFound` starts with this exact prefix
    // when the operation label is "locking task" (queries.rs:279). Matching
    // the prefix pins the test to the specific variant produced by lock_task
    // rather than any error.
    observe::<LockMissingRowRun, _>("lock_task on missing row", |run| {
        match run.lock_error.as_deref() {
            Some(message) if message.starts_with("task not found while locking task") => Ok(()),
            other => Err(format!(
                "expected Error::TaskNotFound with operation=`locking task`, got {other:?}"
            )),
        }
    })
}

fn lock_missing_row_error_mentions_task()
-> impl Fn(&Result<Outcome<LockMissingRowRun>, String>) -> AssertionResult {
    observe::<LockMissingRowRun, _>("lock_task missing row error", |run| {
        match run.lock_error.as_deref() {
            Some(message) if message.contains(&format!("task_id: {}", run.phantom_id)) => Ok(()),
            other => Err(format!(
                "expected error message to embed the phantom task_id {}, got {other:?}",
                run.phantom_id
            )),
        }
    })
}

// ----------------------- ack on already-terminal row ----------------------

#[derive(Debug)]
struct AckOnTerminalRun {
    ack_error: Option<String>,
    task_id: String,
    status: String,
    attempts: i32,
    last_result_payload: Option<Value>,
}

async fn run_ack_on_terminal_row() -> Result<Outcome<AckOnTerminalRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-ack-terminal-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_name = format!("query-ack-terminal-worker-{queue}");
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    storage
        .register_worker(worker_name.clone())
        .await
        .map_err(|e| e.to_string())?;
    let lock_at_secs = now_unix() as i64;
    let id = insert_completed_task(
        pool.clone(),
        queue.clone(),
        "completed",
        "Done",
        1,
        2,
        serde_json::json!({"Ok": "already-processed"}),
    )
    .await?;

    let parts = TaskBuilder::new(())
        .with_task_id(id)
        .with_attempt(Attempt::new_with_value(1))
        .with_ctx(
            PgContext::new()
                .with_max_attempts(2)
                .with_queue(queue.clone())
                .with_lock_at(Some(lock_at_secs))
                .with_lock_by(Some(worker_name)),
        )
        .build()
        .parts;

    let mut ack = PgAck::new(pool.clone());
    let result: Result<String, BoxDynError> = Ok("second-ack".to_owned());
    let ack_result = ack.ack(&result, &parts).await;
    let status = job_status(pool.clone(), id).await?;
    cleanup_queue(pool, queue).await?;

    Ok(Outcome::Completed(AckOnTerminalRun {
        ack_error: ack_result.err().map(|e| e.to_string()),
        task_id: id.to_string(),
        status: status.status,
        attempts: status.attempts,
        last_result_payload: status.last_result,
    }))
}

fn terminal_ack_rejected() -> impl Fn(&Result<Outcome<AckOnTerminalRun>, String>) -> AssertionResult
{
    // `Error::StaleAcknowledgement` Display starts with "stale
    // acknowledgement for task {task_id}" (error.rs:71-73). The variant is
    // the only one returned by ack_task when zero rows match
    // (queries.rs:331). Pin both the prefix and the embedded task_id so a
    // future ack error of a different shape — e.g. a Database variant — is
    // rejected.
    observe::<AckOnTerminalRun, _>("ack on terminal row", |run| {
        match run.ack_error.as_deref() {
            Some(message)
                if message
                    .starts_with(&format!("stale acknowledgement for task {}", run.task_id)) =>
            {
                Ok(())
            }
            other => Err(format!(
                "expected Error::StaleAcknowledgement embedding task {}, got {other:?}",
                run.task_id
            )),
        }
    })
}

fn terminal_ack_status_unchanged()
-> impl Fn(&Result<Outcome<AckOnTerminalRun>, String>) -> AssertionResult {
    observe::<AckOnTerminalRun, _>("ack on terminal status", |run| {
        if run.status == "Done" {
            Ok(())
        } else {
            Err(format!(
                "expected row to remain in Done, got status={:?}",
                run.status
            ))
        }
    })
}

fn terminal_ack_attempts_unchanged()
-> impl Fn(&Result<Outcome<AckOnTerminalRun>, String>) -> AssertionResult {
    observe::<AckOnTerminalRun, _>("ack on terminal attempts", |run| {
        if run.attempts == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected attempts to stay at 1, got {}",
                run.attempts
            ))
        }
    })
}

fn terminal_ack_last_result_unchanged()
-> impl Fn(&Result<Outcome<AckOnTerminalRun>, String>) -> AssertionResult {
    observe::<AckOnTerminalRun, _>("ack on terminal last_result", |run| {
        match &run.last_result_payload {
            Some(value)
                if value.get("Ok").and_then(|v| v.as_str()) == Some("already-processed") =>
            {
                Ok(())
            }
            other => Err(format!(
                "expected last_result to remain the original payload, got {other:?}"
            )),
        }
    })
}

// ---------------------------- fetch_by_id missing -------------------------

#[derive(Debug)]
struct FetchByIdMissingRun {
    fetched_is_none: bool,
}

async fn run_fetch_by_id_missing() -> Result<Outcome<FetchByIdMissingRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-fetch-missing-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let phantom = task_id();
    let fetched = storage
        .fetch_by_id(&phantom)
        .await
        .map_err(|e| e.to_string())?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(FetchByIdMissingRun {
        fetched_is_none: fetched.is_none(),
    }))
}

fn fetch_by_id_missing_returns_none()
-> impl Fn(&Result<Outcome<FetchByIdMissingRun>, String>) -> AssertionResult {
    observe::<FetchByIdMissingRun, _>("fetch_by_id missing", |run| {
        if run.fetched_is_none {
            Ok(())
        } else {
            Err("expected fetch_by_id to return None for an unknown id".into())
        }
    })
}

// ------------------------- check_status variants --------------------------

#[derive(Debug)]
struct CheckStatusVariantRun {
    results_len: usize,
    first_status: Option<Status>,
}

async fn run_check_status_variants(
    scenario: &'static str,
) -> Result<Outcome<CheckStatusVariantRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-check-{scenario}-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let target_id = match scenario {
        "pending" => {
            insert_pending_task(
                pool.clone(),
                queue.clone(),
                serde_json::to_vec("pending").map_err(|e| e.to_string())?,
            )
            .await?
        }
        "missing" => task_id(),
        other => return Err(format!("unknown check_status scenario: {other}")),
    };

    let results =
        <PostgresStorage<String> as WaitForCompletion<String>>::check_status(&storage, [target_id])
            .await
            .map_err(|e| e.to_string())?;
    cleanup_queue(pool, queue).await?;
    let first_status = results.first().map(|r| r.status.clone());
    Ok(Outcome::Completed(CheckStatusVariantRun {
        results_len: results.len(),
        first_status,
    }))
}

fn check_status_returns_no_rows()
-> impl Fn(&Result<Outcome<CheckStatusVariantRun>, String>) -> AssertionResult {
    observe::<CheckStatusVariantRun, _>("check_status missing", |run| {
        if run.results_len == 0 {
            Ok(())
        } else {
            Err(format!(
                "expected check_status to return no results for a missing id, got {} ({:?})",
                run.results_len, run.first_status
            ))
        }
    })
}

// ---------------------- idempotency empty-string key ----------------------

async fn run_idempotency_empty_key() -> Result<Outcome<IdempotencyRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-query-empty-key-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    storage
        .push_task(task("first", now_unix() - 1, 0, 25, Some("")))
        .await
        .map_err(|e| e.to_string())?;
    let duplicate = storage
        .push_task(task("second", now_unix() - 1, 0, 25, Some("")))
        .await;
    let primary_count = count_jobs(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(IdempotencyRun {
        duplicate_rejected: duplicate.is_err(),
        primary_count,
        other_count: 0,
    }))
}

fn empty_key_duplicate_rejected()
-> impl Fn(&Result<Outcome<IdempotencyRun>, String>) -> AssertionResult {
    observe::<IdempotencyRun, _>("empty-key duplicate", |run| {
        if run.duplicate_rejected {
            Ok(())
        } else {
            Err(
                "an empty-string idempotency key should still enforce uniqueness like a regular key"
                    .into(),
            )
        }
    })
}

fn empty_key_row_count_is_one()
-> impl Fn(&Result<Outcome<IdempotencyRun>, String>) -> AssertionResult {
    observe::<IdempotencyRun, _>("empty-key row count", |run| {
        if run.primary_count == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected exactly one row after duplicate empty-key pushes, got {}",
                run.primary_count
            ))
        }
    })
}

lets_expect! { #tokio_test
    expect(run_push_fetch().await) {
        when due_and_delayed_tasks_are_pushed_together {
            to polls_the_oldest_due_task_first {
                fetched_oldest_task_first()
            }
            to polls_the_newer_due_task_second {
                fetched_newer_task_second()
            }
            to does_not_poll_the_delayed_task_until_its_run_at {
                delayed_task_not_polled()
            }
            to keeps_the_delayed_task_fetchable_by_id {
                delayed_task_fetchable_by_id()
            }
        }
    }

    expect(run_fetch_by_id_cross_queue().await) {
        when a_task_is_fetched_by_id_from_an_unrelated_queue_storage {
            to does_not_leak_the_row_across_queues {
                cross_queue_fetch_returns_none()
            }
        }
    }

    expect(run_idempotency(same_queue).await) {
        when an_idempotency_key_collides_in_the_same_queue {
            let same_queue = true;
            to rejects_the_second_push {
                duplicate_push_rejected()
            }
            to keeps_exactly_one_row_in_the_primary_queue {
                keeps_one_job_in_primary_queue()
            }
            to leaves_unrelated_queues_empty {
                other_queue_remains_empty_for_same_queue_duplicate()
            }
        }

        when the_same_key_is_pushed_into_a_different_queue {
            let same_queue = false;
            to accepts_both_pushes {
                duplicate_push_accepted()
            }
            to keeps_one_row_in_the_primary_queue {
                keeps_one_row_in_primary_queue_for_cross_queue_duplicate()
            }
            to keeps_one_row_in_the_secondary_queue {
                keeps_one_row_in_secondary_queue_for_cross_queue_duplicate()
            }
        }
    }

    expect(run_ack_boundary(terminal).await) {
        when a_failed_attempt_still_has_retries_left {
            let terminal = false;
            to records_status_failed {
                ack_recorded_status("Failed")
            }
            to records_the_executed_attempt_count {
                ack_recorded_attempts(1)
            }
            to persists_the_failure_payload {
                ack_persisted_last_result()
            }
        }

        when a_failed_attempt_has_exhausted_the_retry_budget {
            let terminal = true;
            to records_status_killed {
                ack_recorded_status("Killed")
            }
            to records_the_final_attempt_count {
                ack_recorded_attempts(2)
            }
            to persists_the_failure_payload {
                ack_persisted_last_result()
            }
        }
    }

    expect(run_ack_stale().await) {
        when ack_arrives_from_a_worker_that_no_longer_holds_the_lock {
            to surfaces_a_database_error {
                stale_ack_returns_error()
            }
            to keeps_the_row_in_running_status {
                stale_ack_keeps_status_running()
            }
            to does_not_advance_the_attempt_counter {
                stale_ack_does_not_increment_attempts()
            }
            to does_not_write_a_last_result_payload {
                stale_ack_does_not_write_last_result()
            }
        }
    }

    expect(run_lock_boundary(lockable).await) {
        when the_task_is_due_for_execution {
            let lockable = true;
            to succeeds_in_acquiring_the_row {
                lock_succeeded()
            }
        }

        when the_task_is_scheduled_for_a_future_run_at {
            let lockable = false;
            to refuses_to_acquire_the_row {
                lock_refused()
            }
        }
    }

    expect(run_priority_ordering().await) {
        when due_tasks_have_different_priorities {
            to polls_high_priority_before_mid_then_low {
                priority_ordering_high_then_mid_then_low()
            }
        }
    }

    expect(run_skip_locked_concurrency().await) {
        when two_workers_poll_the_same_queue_concurrently {
            to delivers_distinct_payloads_via_skip_locked {
                skip_locked_distributes_distinct_rows()
            }
            to covers_every_pushed_payload_between_the_workers {
                skip_locked_covers_the_pushed_set()
            }
        }
    }

    expect(run_lock_status_scenario(scenario).await) {
        let scenario = "pending_due";

        when the_task_is_pending_with_a_past_run_at {
            to acquires_the_row_for_the_primary_worker { lock_matrix_succeeds() }
            to leaves_the_row_in_running_state { lock_matrix_status_equals("Running") }
            to records_the_primary_worker_as_lock_holder { lock_matrix_owned_by("primary") }
        }

        when the_task_is_pending_with_a_future_run_at {
            let scenario = "pending_future";
            to refuses_to_acquire_the_row { lock_matrix_refuses() }
            to keeps_the_row_in_pending_state { lock_matrix_status_equals("Pending") }
        }

        when the_task_is_queued_by_the_same_worker {
            let scenario = "queued_by_self";
            to re_locks_the_row_for_the_same_worker { lock_matrix_succeeds() }
            to leaves_the_row_in_running_state { lock_matrix_status_equals("Running") }
            to keeps_the_lock_holder_as_the_primary_worker { lock_matrix_owned_by("primary") }
        }

        when the_task_is_queued_by_a_different_worker {
            let scenario = "queued_by_other";
            to refuses_to_acquire_the_row { lock_matrix_refuses() }
            to keeps_the_row_in_queued_state { lock_matrix_status_equals("Queued") }
            to preserves_the_other_worker_as_lock_holder { lock_matrix_owned_by("other") }
        }

        when the_task_is_running_by_a_different_worker {
            let scenario = "running_by_other";
            to refuses_to_acquire_the_row { lock_matrix_refuses() }
            to keeps_the_row_in_running_state { lock_matrix_status_equals("Running") }
            to preserves_the_running_worker_as_lock_holder { lock_matrix_owned_by("other") }
        }

        when the_task_failed_but_still_has_retries {
            let scenario = "failed_retryable";
            to acquires_the_row_for_a_retry { lock_matrix_succeeds() }
            to transitions_the_row_into_running_state { lock_matrix_status_equals("Running") }
            to records_the_primary_worker_as_lock_holder { lock_matrix_owned_by("primary") }
        }

        when the_task_failed_and_exhausted_the_retry_budget {
            let scenario = "failed_exhausted";
            to refuses_to_acquire_the_row { lock_matrix_refuses() }
            to keeps_the_row_in_failed_state { lock_matrix_status_equals("Failed") }
        }

        when the_task_is_already_done {
            let scenario = "done";
            to refuses_to_acquire_a_completed_row { lock_matrix_refuses() }
            to keeps_the_row_in_done_state { lock_matrix_status_equals("Done") }
        }

        when the_task_is_killed {
            let scenario = "killed";
            to refuses_to_acquire_a_killed_row { lock_matrix_refuses() }
            to keeps_the_row_in_killed_state { lock_matrix_status_equals("Killed") }
        }
    }

    expect(run_listing_metrics().await) {
        when storage_exposes_listing_and_metric_apis_on_a_populated_database {
            to list_tasks_is_scoped_to_the_current_queue {
                list_tasks_scoped_to_current_queue()
            }
            to list_all_tasks_spans_every_queue {
                list_all_tasks_covers_all_queues()
            }
            to includes_the_current_queue_worker_in_list_workers {
                list_workers_includes_current_queue_worker()
            }
            to hides_other_queue_workers_from_list_workers {
                list_workers_hides_other_queue_workers()
            }
            to surfaces_other_queue_workers_through_list_all_workers {
                list_all_workers_surfaces_other_queue_workers()
            }
            to list_queues_includes_the_active_queue {
                list_queues_includes_active_queue()
            }
            to queue_metrics_expose_pending_jobs_counter {
                queue_metrics_include_pending_jobs()
            }
            to global_metrics_expose_total_jobs_counter {
                global_metrics_include_total_jobs()
            }
        }
    }

    expect(run_completion_check_status().await) {
        when check_status_inspects_a_terminal_task {
            to reports_done_status {
                completion_reports_done_status()
            }
            to surfaces_the_decoded_payload {
                completion_carries_decoded_payload()
            }
        }
    }

    expect(run_completion_wait_for().await) {
        when wait_for_streams_a_terminal_task {
            to reports_done_status {
                completion_reports_done_status()
            }
            to surfaces_the_decoded_payload {
                completion_carries_decoded_payload()
            }
        }
    }

    expect(run_completion_cross_check().await) {
        when check_status_is_used_from_an_unrelated_queue_storage {
            to reports_done_status {
                completion_reports_done_status()
            }
            to surfaces_the_decoded_payload {
                completion_carries_decoded_payload()
            }
        }
    }

    expect(run_completion_cross_wait().await) {
        when wait_for_is_used_from_an_unrelated_queue_storage {
            to reports_done_status {
                completion_reports_done_status()
            }
            to surfaces_the_decoded_payload {
                completion_carries_decoded_payload()
            }
        }
    }

    expect(run_wait_empty().await) {
        when wait_for_is_called_with_no_ids {
            to ends_the_stream_without_touching_the_database {
                wait_empty_terminates_without_db()
            }
        }
    }

    expect(run_wait_error().await) {
        when wait_for_cannot_reach_the_database {
            to surfaces_the_database_error_through_the_stream {
                wait_error_surfaces_db_error()
            }
        }
    }

    expect(run_wait_pending().await) {
        when wait_for_targets_a_non_terminal_task {
            to keeps_waiting_until_the_task_finishes {
                wait_pending_does_not_complete_early()
            }
        }
    }

    expect(run_wait_malformed_terminal().await) {
        when wait_for_observes_a_terminal_row_with_an_unparseable_result {
            to yields_a_decode_error_first {
                malformed_wait_first_yields_decode_error()
            }
            to does_not_keep_retrying_after_the_decode_error {
                malformed_wait_finishes_after_one()
            }
        }
    }

    expect(run_zero_buffer_fetch().await) {
        when buffer_size_is_zero_in_the_config {
            to still_polls_one_due_task {
                zero_buffer_still_fetches_one()
            }
        }
    }

    expect(run_orphan_reenqueue(false, false).await) {
        when a_stale_worker_left_a_running_task_with_retries_available {
            to requeues_the_task_back_to_pending {
                orphan_status_equals("Pending")
            }
            to records_the_retry_attempt {
                orphan_attempts_equals(1)
            }
            to writes_the_timeout_failure_to_last_result {
                orphan_recorded_last_result()
            }
        }
    }

    expect(run_orphan_reenqueue(true, true).await) {
        when a_stale_worker_left_a_queued_task_with_no_retries_remaining {
            to kills_the_task {
                orphan_status_equals("Killed")
            }
            to records_the_final_attempt {
                orphan_attempts_equals(2)
            }
            to writes_the_timeout_failure_to_last_result {
                orphan_recorded_last_result()
            }
        }
    }

    // Off-diagonal of (queued, terminal): the reenqueue path branches on the
    // retry budget, not on the snapshot status. Pin both crossings explicitly
    // so a future regression that conflates "Queued" with "no retries" or
    // "Running" with "has retries" is caught.

    expect(run_orphan_reenqueue(true, false).await) {
        when a_stale_worker_left_a_queued_task_with_retries_available {
            to requeues_the_task_back_to_pending {
                orphan_status_equals("Pending")
            }
            to records_the_retry_attempt {
                orphan_attempts_equals(1)
            }
            to writes_the_timeout_failure_to_last_result {
                orphan_recorded_last_result()
            }
        }
    }

    expect(run_orphan_reenqueue(false, true).await) {
        when a_stale_worker_left_a_running_task_with_no_retries_remaining {
            to kills_the_task {
                orphan_status_equals("Killed")
            }
            to records_the_final_attempt {
                orphan_attempts_equals(2)
            }
            to writes_the_timeout_failure_to_last_result {
                orphan_recorded_last_result()
            }
        }
    }

    expect(run_poll_decode_basic().await) {
        when basic_polling_storage_decodes_a_malformed_payload {
            to surfaces_a_decode_error_through_the_stream {
                poll_decode_error_mentions_payload()
            }
        }
    }

    expect(run_poll_decode_notify().await) {
        when notify_polling_storage_decodes_a_malformed_payload {
            to surfaces_a_decode_error_through_the_stream {
                poll_decode_error_mentions_payload()
            }
        }
    }

    expect(run_idempotency_without_keys().await) {
        when two_pushes_into_the_same_queue_omit_the_idempotency_key {
            to accepts_the_second_push {
                no_key_second_push_is_accepted()
            }
            to stores_one_row_per_push {
                no_key_pushes_create_two_rows()
            }
        }
    }

    expect(run_lock_already_held().await) {
        when lock_task_is_called_on_a_row_already_locked_by_another_worker {
            to refuses_to_re_lock_the_row {
                second_lock_is_refused()
            }
            to preserves_the_original_lock_holder {
                original_lock_holder_is_preserved()
            }
            to keeps_the_row_in_running_status {
                locked_row_status_remains_running()
            }
        }
    }

    expect(run_ack_on_pending_row().await) {
        when ack_targets_a_row_that_was_never_locked {
            to returns_a_database_error {
                ack_on_pending_row_is_rejected()
            }
            to does_not_change_the_row_status {
                pending_row_status_unchanged_after_rejected_ack()
            }
            to does_not_advance_the_attempt_counter {
                pending_row_attempts_unchanged_after_rejected_ack()
            }
            to does_not_write_a_last_result_payload {
                pending_row_last_result_unchanged_after_rejected_ack()
            }
        }
    }

    expect(run_lock_missing_row().await) {
        when lock_task_targets_an_id_that_was_never_inserted {
            to returns_a_task_not_found_error {
                lock_missing_row_rejected()
            }
            to surfaces_the_missing_task_id_in_the_error {
                lock_missing_row_error_mentions_task()
            }
        }
    }

    expect(run_ack_on_terminal_row().await) {
        when ack_targets_a_row_that_is_already_marked_done {
            to returns_a_database_error {
                terminal_ack_rejected()
            }
            to leaves_the_row_in_done_state {
                terminal_ack_status_unchanged()
            }
            to does_not_advance_the_attempt_counter {
                terminal_ack_attempts_unchanged()
            }
            to preserves_the_existing_last_result_payload {
                terminal_ack_last_result_unchanged()
            }
        }
    }

    expect(run_fetch_by_id_missing().await) {
        when fetch_by_id_is_called_with_an_id_that_does_not_exist {
            to returns_none {
                fetch_by_id_missing_returns_none()
            }
        }
    }

    expect(run_check_status_variants(scenario).await) {
        let scenario = "pending";

        when check_status_inspects_a_non_terminal_row {
            to omits_the_id_from_the_result_set {
                check_status_returns_no_rows()
            }
        }

        when check_status_is_called_with_an_id_that_does_not_exist {
            let scenario = "missing";
            to omits_the_id_from_the_result_set {
                check_status_returns_no_rows()
            }
        }
    }

    expect(run_idempotency_empty_key().await) {
        when an_empty_string_idempotency_key_is_used_for_two_pushes_in_the_same_queue {
            to rejects_the_second_push {
                empty_key_duplicate_rejected()
            }
            to keeps_a_single_row_in_the_queue {
                empty_key_row_count_is_one()
            }
        }
    }

    expect(run_wait_for_mixed().await) {
        when wait_for_is_called_with_one_pending_and_one_terminal_id {
            to yields_the_terminal_task_first {
                mixed_wait_yields_terminal_first()
            }
            to keeps_waiting_for_the_pending_task_after_the_first_item {
                mixed_wait_keeps_pending_open()
            }
        }
    }
}
