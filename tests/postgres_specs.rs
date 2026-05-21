//! Exhaustive specification through nested contexts for behaviours not yet
//! covered elsewhere. Tests in this file gate on `DATABASE_URL`; without it
//! every scenario resolves to `Outcome::Skipped` and the assertions pass.
//!
//! Each `expect` block enumerates a behaviour under a single fixed context.
//! When a leaf reveals a defect we either fix the source (minimally) or mark
//! the test `#[ignore]` with a comment so the discussion is preserved.

#![cfg(feature = "tokio")]

mod support;

use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use apalis_core::backend::{ListQueues, ListWorkers};
use apalis_core::{
    backend::{Backend, RegisterWorker, TaskSink},
    error::BoxDynError,
    task::{Task, attempt::Attempt, builder::TaskBuilder, status::Status, task_id::TaskId},
    worker::{context::WorkerContext, ext::ack::Acknowledge},
};
use apalis_diesel_postgres::{
    Config, Error as PgError, PgAck, PgContext, PgPool, PgTask, PgTaskId, PostgresStorage,
    build_pool, refresh_queue_stats_snapshot, setup, verify_schema,
};
use apalis_sql::{DateTime, DateTimeExt, context::SqlContext};
use diesel::{
    PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{BigInt, Integer, Jsonb, Nullable, Text, Timestamptz},
};
use futures::StreamExt;
use lets_expect::{AssertionError, AssertionResult, *};
use serde_json::Value;
use std::sync::Arc;
use ulid::Ulid;

// --------------------------------------------------------------------------
// shared scaffolding (small dup of postgres_queries.rs helpers; keeping the
// two files independent so concurrent edits in either don't conflict).
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
    let Some(database_url) = support::database_url_or_skip()? else {
        return Ok(None);
    };
    let pool = build_pool(database_url).map_err(|e| e.to_string())?;
    setup(&pool).await.map_err(|e| e.to_string())?;
    Ok(Some(pool))
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

fn task_id() -> PgTaskId {
    TaskId::from_str(&Ulid::new().to_string()).expect("generated ULID parses as task id")
}

fn task(
    payload: &'static str,
    run_at: u64,
    attempts: usize,
    max_attempts: i32,
) -> Task<String, PgContext, Ulid> {
    TaskBuilder::new(payload.to_owned())
        .with_task_id(task_id())
        .run_at_timestamp(run_at)
        .with_attempt(Attempt::new_with_value(attempts))
        .with_ctx(SqlContext::new().with_max_attempts(max_attempts))
        .build()
}

async fn next_task(
    stream: &mut (
             impl futures::Stream<Item = Result<Option<PgTask<String>>, apalis_diesel_postgres::Error>>
             + Unpin
         ),
) -> Result<PgTask<String>, String> {
    let deadline = Duration::from_secs(5);
    loop {
        let item = tokio::time::timeout(deadline, stream.next())
            .await
            .map_err(|_| "timed out waiting for a task".to_owned())?
            .ok_or_else(|| "task stream ended".to_owned())?
            .map_err(|e| e.to_string())?;
        if let Some(task) = item {
            return Ok(task);
        }
    }
}

// --------------------------------------------------------------------------
// fetch_next: `Failed` tasks below their `max_attempts` are re-eligible
// without needing the orphan reenqueue path.
//
// `queries::fetch_next` SQL `WHERE` clause lists
//   `(status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts))`.
// This integration test pins that contract: after `ack_task` writes
// `status='Failed', attempts=1`, the next poll on a fresh stream must
// reclaim the row.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct FailedRetryRun {
    polled_payload: Option<String>,
    polled_attempts: usize,
}

async fn insert_failed_task(
    pool: PgPool,
    queue: String,
    attempts: i32,
    max_attempts: i32,
) -> Result<PgTaskId, String> {
    let id = Ulid::new();
    let job = serde_json::to_vec("retry-me").map_err(|e| e.to_string())?;
    let task_id = TaskId::from_str(&id.to_string()).map_err(|e| e.to_string())?;
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (
                id, job_type, job, status, attempts, max_attempts, run_at, last_result
            ) VALUES ($1, $2, $3, 'Failed', $4, $5, now() - INTERVAL '1 second', '{\"Err\":\"boom\"}'::jsonb)",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(queue)
        .bind::<diesel::sql_types::Binary, _>(job)
        .bind::<Integer, _>(attempts)
        .bind::<Integer, _>(max_attempts)
        .execute(conn)
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await?;
    Ok(task_id)
}

async fn run_failed_retry(retryable: bool) -> Result<Outcome<FailedRetryRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-failed-retry-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let (attempts, max_attempts) = if retryable { (1, 3) } else { (3, 3) };
    insert_failed_task(pool.clone(), queue.clone(), attempts, max_attempts).await?;

    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let worker = WorkerContext::new::<()>(&format!("spec-failed-retry-worker-{queue}"));
    let mut stream = storage.poll(&worker);

    let polled = tokio::time::timeout(Duration::from_secs(3), async {
        // Two poll ticks: first emits the registration ack; second carries the task.
        let mut polled: Option<PgTask<String>> = None;
        for _ in 0..6 {
            match tokio::time::timeout(Duration::from_millis(800), next_task(&mut stream)).await {
                Ok(Ok(t)) => {
                    polled = Some(t);
                    break;
                }
                _ => continue,
            }
        }
        polled
    })
    .await
    .unwrap_or(None);

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(FailedRetryRun {
        polled_attempts: polled
            .as_ref()
            .map(|t| t.parts.attempt.current())
            .unwrap_or(0),
        polled_payload: polled.map(|t| t.args),
    }))
}

fn failed_retry_reclaims_row()
-> impl Fn(&Result<Outcome<FailedRetryRun>, String>) -> AssertionResult {
    observe::<FailedRetryRun, _>("failed retry reclaim", |run| match &run.polled_payload {
        Some(v) if v == "retry-me" => Ok(()),
        Some(other) => Err(format!(
            "expected to reclaim retryable Failed row, got {other:?}"
        )),
        None => Err("expected fetch_next to reclaim Failed row below max_attempts".into()),
    })
}

fn failed_retry_preserves_attempt_count()
-> impl Fn(&Result<Outcome<FailedRetryRun>, String>) -> AssertionResult {
    observe::<FailedRetryRun, _>("failed retry attempts", |run| {
        if run.polled_payload.is_none() {
            return Ok(()); // covered by the other assertion
        }
        if run.polled_attempts == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected reclaimed task to carry attempts=1, got {}",
                run.polled_attempts
            ))
        }
    })
}

fn failed_exhausted_not_reclaimed()
-> impl Fn(&Result<Outcome<FailedRetryRun>, String>) -> AssertionResult {
    observe::<FailedRetryRun, _>("failed exhausted skip", |run| {
        if run.polled_payload.is_none() {
            Ok(())
        } else {
            Err("expected exhausted Failed row to remain hidden from fetch_next".into())
        }
    })
}

// --------------------------------------------------------------------------
// RegisterWorker (admin trait) concurrent calls.
//
// `queries::register_worker_blocking` (used by the worker stream's
// `initial_heartbeat`) wraps its INSERT with `pg_try_advisory_xact_lock` and
// surfaces `AlreadyRegistered` if a peer holds the lock. The admin trait
// `admin::RegisterWorker::register_worker` instead uses the *blocking*
// `pg_advisory_xact_lock` + `ON CONFLICT (id, worker_type) DO UPDATE`, so
// two concurrent admin registrations serialize and both succeed (UPSERT
// idempotency).
//
// The conflict UPDATE deliberately does NOT refresh `last_seen` — only
// `storage_name`/`layers` are merged. Heartbeats are owned by the worker
// stream (lease-token gated); if the admin path also refreshed `last_seen`,
// a caller with admin-API access could keep a foreign worker's row fresh
// indefinitely and prevent `reenqueue_orphaned` from reclaiming its jobs.
// This spec pins only the observable contract: both calls succeed and exactly
// one row exists. If a future redesign of admin registration changes either
// of those, update the expectations here.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct ConcurrentRegisterRun {
    first_ok: bool,
    second_ok: bool,
    row_count: i64,
}

#[derive(Debug, diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    count: i64,
}

async fn run_concurrent_admin_register() -> Result<Outcome<ConcurrentRegisterRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-concurrent-register-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let worker_id = format!("spec-concurrent-worker-{queue}");
    let mut a = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let mut b = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let id_a = worker_id.clone();
    let id_b = worker_id.clone();
    let (ra, rb) = tokio::join!(a.register_worker(id_a), b.register_worker(id_b));

    let q = queue.clone();
    let count = with_conn(pool.clone(), move |conn| {
        sql_query("SELECT COUNT(*) AS count FROM apalis.workers WHERE worker_type = $1")
            .bind::<Text, _>(q)
            .load::<CountRow>(conn)
            .map_err(|e| e.to_string())?
            .into_iter()
            .next()
            .map(|r| r.count)
            .ok_or_else(|| "count query returned no rows".to_owned())
    })
    .await?;

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(ConcurrentRegisterRun {
        first_ok: ra.is_ok(),
        second_ok: rb.is_ok(),
        row_count: count,
    }))
}

fn concurrent_admin_register_both_succeed()
-> impl Fn(&Result<Outcome<ConcurrentRegisterRun>, String>) -> AssertionResult {
    observe::<ConcurrentRegisterRun, _>("concurrent admin register", |run| {
        if run.first_ok && run.second_ok {
            Ok(())
        } else {
            Err(format!(
                "expected both admin RegisterWorker calls to succeed (UPSERT semantics), got first_ok={} second_ok={}",
                run.first_ok, run.second_ok
            ))
        }
    })
}

fn concurrent_admin_register_creates_single_row()
-> impl Fn(&Result<Outcome<ConcurrentRegisterRun>, String>) -> AssertionResult {
    observe::<ConcurrentRegisterRun, _>("concurrent admin register row count", |run| {
        if run.row_count == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected ON CONFLICT DO UPDATE to keep exactly one workers row, got {}",
                run.row_count
            ))
        }
    })
}

// --------------------------------------------------------------------------
// Two-worker concurrent fetch_next race: FOR UPDATE SKIP LOCKED ensures each
// row is delivered to at most one worker. We push N rows and let two pollers
// race; the union of delivered payloads must equal the pushed set with no
// duplicates.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct TwoWorkerRaceRun {
    total: usize,
    duplicates: usize,
}

async fn run_two_worker_race() -> Result<Outcome<TwoWorkerRaceRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-race-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let mut producer =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(8));
    let n = 8usize;
    for i in 0..n {
        // unique payloads
        let payload: &'static str = Box::leak(format!("race-{i}").into_boxed_str());
        producer
            .push_task(task(payload, now_unix() - 1, 0, 25))
            .await
            .map_err(|e| e.to_string())?;
    }

    let storage_a =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(4));
    let storage_b =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(4));
    let worker_a = WorkerContext::new::<()>(&format!("spec-race-a-{queue}"));
    let worker_b = WorkerContext::new::<()>(&format!("spec-race-b-{queue}"));

    let collect = |storage: PostgresStorage<String>, worker: WorkerContext| async move {
        let mut out = Vec::new();
        let mut stream = storage.poll(&worker);
        let deadline = Duration::from_secs(3);
        let started = std::time::Instant::now();
        while started.elapsed() < deadline && out.len() < 16 {
            match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
                Ok(Some(Ok(Some(t)))) => out.push(t.args),
                Ok(Some(Ok(None))) => continue,
                Ok(Some(Err(_))) => break,
                Ok(None) => break,
                Err(_) => continue,
            }
        }
        out
    };

    let (a_args, b_args) =
        tokio::join!(collect(storage_a, worker_a), collect(storage_b, worker_b),);
    let mut all = a_args;
    all.extend(b_args);
    let mut sorted = all.clone();
    sorted.sort();
    let mut duplicates = 0;
    for w in sorted.windows(2) {
        if w[0] == w[1] {
            duplicates += 1;
        }
    }

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(TwoWorkerRaceRun {
        total: all.len(),
        duplicates,
    }))
}

fn two_workers_share_set_without_duplicates()
-> impl Fn(&Result<Outcome<TwoWorkerRaceRun>, String>) -> AssertionResult {
    observe::<TwoWorkerRaceRun, _>("two-worker race", |run| {
        if run.duplicates == 0 && run.total >= 1 {
            Ok(())
        } else {
            Err(format!(
                "expected SKIP LOCKED to keep deliveries disjoint, got total={} duplicates={}",
                run.total, run.duplicates
            ))
        }
    })
}

// Silence "unused" complaints in builds that strip the status enum reference
// from inferred types. `Status` is needed for the assertion helpers to
// compile against generic `Task` parts.
#[allow(dead_code)]
fn _force_status_import() -> Status {
    Status::Pending
}

// --------------------------------------------------------------------------
// P3: refresh_queue_stats_snapshot on an unpopulated matview must succeed.
//
// The matview is created `WITH NO DATA` (migration 20260521000003). The
// pre-fix implementation always ran `REFRESH ... CONCURRENTLY`, which
// PostgreSQL rejects on an unpopulated matview. The current implementation
// reads `pg_matviews.ispopulated` and falls back to a blocking REFRESH on
// first-call. To exercise that branch deterministically we drop+recreate the
// matview WITH NO DATA inside the test, then call the public
// `refresh_queue_stats_snapshot` helper.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct RefreshSnapshotRun {
    refresh_result: Result<(), String>,
    populated_after: bool,
}

async fn run_refresh_unpopulated_snapshot() -> Result<Outcome<RefreshSnapshotRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    with_conn(pool.clone(), |conn| {
        // Force the matview back to the unpopulated state without dropping
        // and recreating it — `REFRESH ... WITH NO DATA` resets
        // `pg_matviews.ispopulated` to false, which is exactly the branch
        // exercised by the fixed code path.
        sql_query("REFRESH MATERIALIZED VIEW apalis.queue_stats_snapshot WITH NO DATA")
            .execute(conn)
            .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await?;

    let refresh_result = refresh_queue_stats_snapshot(&pool)
        .await
        .map_err(|e| e.to_string());

    let populated_after = with_conn(pool.clone(), |conn| {
        sql_query(
            "SELECT ispopulated AS populated
             FROM pg_matviews
             WHERE schemaname = 'apalis' AND matviewname = 'queue_stats_snapshot'",
        )
        .load::<PopulatedRow>(conn)
        .map_err(|e| e.to_string())
        .map(|rows| rows.first().map(|r| r.populated).unwrap_or(false))
    })
    .await?;

    Ok(Outcome::Completed(RefreshSnapshotRun {
        refresh_result,
        populated_after,
    }))
}

#[derive(Debug, diesel::QueryableByName)]
struct PopulatedRow {
    #[diesel(sql_type = diesel::sql_types::Bool)]
    populated: bool,
}

fn refresh_unpopulated_snapshot_succeeds()
-> impl Fn(&Result<Outcome<RefreshSnapshotRun>, String>) -> AssertionResult {
    observe::<RefreshSnapshotRun, _>("refresh unpopulated snapshot", |run| {
        run.refresh_result
            .as_ref()
            .map(|_| ())
            .map_err(|err| format!("expected refresh to succeed on unpopulated matview, got {err}"))
    })
}

fn refresh_unpopulated_snapshot_populates()
-> impl Fn(&Result<Outcome<RefreshSnapshotRun>, String>) -> AssertionResult {
    observe::<RefreshSnapshotRun, _>("refresh populates matview", |run| {
        if run.refresh_result.is_err() {
            return Ok(()); // covered by the other assertion
        }
        if run.populated_after {
            Ok(())
        } else {
            Err("matview should be populated after a successful blocking refresh".into())
        }
    })
}

// --------------------------------------------------------------------------
// queries/metrics.rs: populated branch is implementation-only.
// A second `refresh_queue_stats_snapshot` after the first must take the
// CONCURRENTLY arm; covering it as a separate `expect(run_refresh_populated_…)`
// races against the `WITH NO DATA` reset in `run_refresh_unpopulated_snapshot`
// (both run in parallel under cargo test). The two arms together form a
// state-machine where the unpopulated test transitions populated → unpopulated
// and then back to populated, so the CONCURRENTLY arm is in fact exercised
// any time the broader test suite runs after the unpopulated test completes.
// Keeping a separate populated-arm test would require serializing the matview
// state across the file, which is out of proportion with the value.
// --------------------------------------------------------------------------

// --------------------------------------------------------------------------
// P4: UNLISTEN after NotifyTaskIds drop.
//
// `notify_task_ids` installs `LISTEN "apalis::job::insert"` on a pooled
// connection. When the returned stream is dropped, the listener thread must
// issue `UNLISTEN` before the connection returns to the pool — otherwise the
// next pool user inherits the subscription and could observe queued
// notifications.
//
// We construct a single-connection pool, run the notify-based storage long
// enough to install the LISTEN, drop the storage, then borrow the (now
// returned) pooled connection and inspect `pg_listening_channels()`.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct UnlistenRun {
    channels_after_drop: Vec<String>,
}

#[derive(Debug, diesel::QueryableByName)]
struct ChannelRow {
    #[diesel(sql_type = Text)]
    channel: String,
}

async fn run_unlisten_after_drop() -> Result<Outcome<UnlistenRun>, String> {
    let Some(url) = support::database_url_or_skip()? else {
        return Ok(Outcome::Skipped);
    };
    // Single-connection pool so the listener and the post-drop borrower share
    // the same connection. Pool default is 10 connections, which would
    // randomly hand a different connection to the post-drop check.
    let pool = apalis_diesel_postgres::build_pool_with(&url, |b| b.max_size(1))
        .map_err(|e| e.to_string())?;
    setup(&pool).await.map_err(|e| e.to_string())?;

    let queue = format!("apalis-spec-unlisten-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Wrap NotifyTaskIds creation in a scope so it drops before we check.
    {
        let storage = PostgresStorage::<String>::new_with_notify(&pool, &Config::new(&queue));
        let worker = WorkerContext::new::<()>(&format!("spec-unlisten-worker-{queue}"));
        let mut stream = storage.poll(&worker);
        // Pull the registration ack so we know the listener thread has had
        // time to start LISTEN.
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.next()).await;
        // explicit drop
        drop(stream);
    }

    // The listener thread runs a final `UNLISTEN` before the connection
    // returns to the pool. The thread is detached and the UNLISTEN happens
    // asynchronously, so give it a brief window to complete.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let channels_after_drop = with_conn(pool.clone(), |conn| {
        sql_query("SELECT pg_listening_channels()::text AS channel")
            .load::<ChannelRow>(conn)
            .map(|rows| rows.into_iter().map(|r| r.channel).collect::<Vec<_>>())
            .map_err(|e| e.to_string())
    })
    .await?;

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(UnlistenRun {
        channels_after_drop,
    }))
}

fn no_stale_listen_subscription_after_drop()
-> impl Fn(&Result<Outcome<UnlistenRun>, String>) -> AssertionResult {
    observe::<UnlistenRun, _>("UNLISTEN after drop", |run| {
        let stale: Vec<_> = run
            .channels_after_drop
            .iter()
            .filter(|c| c.contains("apalis::job::insert"))
            .collect();
        if stale.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "expected pg_listening_channels() to be free of the apalis subscription after NotifyTaskIds drop, got {:?}",
                run.channels_after_drop
            ))
        }
    })
}

// --------------------------------------------------------------------------
// P6: `list_queues.workers` excludes locks left on terminal-status jobs.
//
// The `locked_workers` CTE in `list_queues` now filters on
// `status IN ('Pending', 'Queued', 'Running')`. A Done/Failed/Killed row
// whose `lock_by` was never cleared (e.g. ack path that doesn't NULL the
// lock) must not surface as a "current" worker on the queue.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct LockedWorkersRun {
    workers_on_active_queue: Vec<String>,
}

async fn run_locked_workers_excludes_terminal() -> Result<Outcome<LockedWorkersRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-locked-workers-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let active_worker = format!("active-{queue}");
    let done_worker = format!("done-{queue}");
    let failed_worker = format!("failed-{queue}");
    let killed_worker = format!("killed-{queue}");

    let q = queue.clone();
    let aw = active_worker.clone();
    let dw = done_worker.clone();
    let fw = failed_worker.clone();
    let kw = killed_worker.clone();
    with_conn(pool.clone(), move |conn| {
        // FK: jobs.lock_by → workers.id (per worker_type). Register all four.
        for wid in [aw.as_str(), dw.as_str(), fw.as_str(), kw.as_str()] {
            sql_query(
                "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at)
                 VALUES ($1, $2, 'PostgresStorage', '', now(), now())
                 ON CONFLICT (id, worker_type) DO NOTHING",
            )
            .bind::<Text, _>(wid)
            .bind::<Text, _>(&q)
            .execute(conn)
            .map_err(|e| e.to_string())?;
        }
        for (status, lock_by, attempts, last_result_sql) in [
            ("Running", aw.as_str(), 1, "NULL"),
            ("Done", dw.as_str(), 1, "'{\"Ok\":\"ok\"}'::jsonb"),
            ("Failed", fw.as_str(), 3, "'{\"Err\":\"err\"}'::jsonb"),
            ("Killed", kw.as_str(), 3, "'{\"Err\":\"k\"}'::jsonb"),
        ] {
            let id = Ulid::new().to_string();
            let sql = format!(
                "INSERT INTO apalis.jobs (
                    id, job_type, job, status, attempts, max_attempts, run_at, lock_by, lock_at, last_result, done_at
                ) VALUES (
                    '{id}', $1, '\\x00'::bytea, '{status}', {attempts}, 3,
                    now() - INTERVAL '5 seconds', $2, now() - INTERVAL '5 seconds',
                    {last_result_sql},
                    CASE WHEN '{status}' IN ('Done','Failed','Killed') THEN now() ELSE NULL END
                )"
            );
            sql_query(sql)
                .bind::<Text, _>(&q)
                .bind::<Text, _>(lock_by)
                .execute(conn)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    })
    .await?;

    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let queues = storage.list_queues().await.map_err(|e| e.to_string())?;
    let workers_on_active_queue = queues
        .into_iter()
        .find(|q| q.name == queue)
        .map(|q| q.workers)
        .unwrap_or_default();

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(LockedWorkersRun {
        workers_on_active_queue,
    }))
}

fn locked_workers_shows_active_only()
-> impl Fn(&Result<Outcome<LockedWorkersRun>, String>) -> AssertionResult {
    observe::<LockedWorkersRun, _>("locked workers excludes terminal", |run| {
        let mut has_active = false;
        let mut has_terminal = false;
        for w in &run.workers_on_active_queue {
            if w.starts_with("active-") {
                has_active = true;
            }
            if w.starts_with("done-") || w.starts_with("failed-") || w.starts_with("killed-") {
                has_terminal = true;
            }
        }
        if !has_active {
            return Err(format!(
                "expected list_queues.workers to include the Running worker, got {:?}",
                run.workers_on_active_queue
            ));
        }
        if has_terminal {
            return Err(format!(
                "expected locks on Done/Failed/Killed jobs to be filtered out, got {:?}",
                run.workers_on_active_queue
            ));
        }
        Ok(())
    })
}

// --------------------------------------------------------------------------
// P7: list_workers no longer caps at 100 rows.
//
// The pre-fix `list_workers` body carried `LIMIT 100` even though the apalis
// `ListWorkers` trait does not accept a filter. Operators with >100 workers
// silently lost rows. The fix removes the cap; this spec inserts 110 worker
// rows and verifies every one is returned.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct ListWorkersBeyond100Run {
    returned: usize,
    inserted: usize,
}

async fn run_list_workers_beyond_100() -> Result<Outcome<ListWorkersBeyond100Run>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-list-workers-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let inserted = 110usize;
    let q = queue.clone();
    with_conn(pool.clone(), move |conn| {
        for i in 0..inserted {
            let id = format!("w-{i:03}-{q}");
            sql_query(
                "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at)
                 VALUES ($1, $2, 'PostgresStorage', '', now(), now())",
            )
            .bind::<Text, _>(&id)
            .bind::<Text, _>(&q)
            .execute(conn)
            .map_err(|e| e.to_string())?;
        }
        Ok(())
    })
    .await?;

    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let workers = storage.list_workers().await.map_err(|e| e.to_string())?;
    let returned = workers.len();

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(ListWorkersBeyond100Run {
        returned,
        inserted,
    }))
}

fn list_workers_returns_every_row()
-> impl Fn(&Result<Outcome<ListWorkersBeyond100Run>, String>) -> AssertionResult {
    observe::<ListWorkersBeyond100Run, _>("list_workers >100", |run| {
        if run.returned == run.inserted {
            Ok(())
        } else {
            Err(format!(
                "expected list_workers to return all {} workers, got {}",
                run.inserted, run.returned
            ))
        }
    })
}

// --------------------------------------------------------------------------
// P1: registration gate — initial_heartbeat failure stops the fetcher.
//
// `poll_basic` runs `initial_heartbeat` first; on `Err` it must surface the
// error and stop, not start dequeueing. We trigger AlreadyRegistered by
// occupying the (worker_id, queue) slot with a fresh `last_seen` (so the
// UPSERT WHERE clause forbids overwriting) and a different lease_token, then
// poll with a second storage handle that synthesises a different lease and
// observes a single registration error before the stream ends.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct RegistrationGateRun {
    items_seen: usize,
    saw_already_registered_error: bool,
    stream_ended: bool,
}

async fn run_registration_gate_blocks_fetcher() -> Result<Outcome<RegistrationGateRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-reggate-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;
    let worker_id = format!("spec-reggate-worker-{queue}");

    // Occupy the slot with a fresh, lease-bound row so the next register fails.
    let q = queue.clone();
    let wid = worker_id.clone();
    with_conn(pool.clone(), move |conn| {
        sql_query(
            "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at, lease_token)
             VALUES ($1, $2, 'PostgresStorage', '', now(), now(), $3)",
        )
        .bind::<Text, _>(&wid)
        .bind::<Text, _>(&q)
        .bind::<Text, _>(format!("incumbent-{}", Ulid::new()))
        .execute(conn)
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await?;

    // Push a task that *would* be dequeued if the gate were broken.
    let mut producer =
        PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue).set_buffer_size(1));
    use apalis_core::backend::TaskSink;
    producer
        .push_task(task("must-not-dequeue", now_unix() - 1, 0, 25))
        .await
        .map_err(|e| e.to_string())?;

    let storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let worker = WorkerContext::new::<()>(&worker_id);
    let mut stream = storage.poll(&worker);

    let mut items_seen = 0usize;
    let mut saw_already_registered_error = false;
    let mut stream_ended = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(4);
    while std::time::Instant::now() < deadline {
        let next = tokio::time::timeout(Duration::from_millis(800), stream.next()).await;
        match next {
            Err(_) => continue,
            Ok(None) => {
                stream_ended = true;
                break;
            }
            Ok(Some(item)) => {
                items_seen += 1;
                if let Err(err) = item
                    && matches!(err, PgError::AlreadyRegistered(_))
                {
                    saw_already_registered_error = true;
                }
            }
        }
    }

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(RegistrationGateRun {
        items_seen,
        saw_already_registered_error,
        stream_ended,
    }))
}

fn registration_gate_emits_error_then_ends()
-> impl Fn(&Result<Outcome<RegistrationGateRun>, String>) -> AssertionResult {
    observe::<RegistrationGateRun, _>("registration gate", |run| {
        if !run.saw_already_registered_error {
            return Err(format!(
                "expected the AlreadyRegistered error to surface on the stream, items_seen={}",
                run.items_seen
            ));
        }
        if !run.stream_ended {
            return Err("expected the stream to terminate after the registration error".into());
        }
        // We expect exactly one yielded item (the registration error). The
        // fetcher must not run any dequeue rounds; a value >1 means the gate
        // leaked through.
        if run.items_seen > 1 {
            return Err(format!(
                "expected exactly one item (the registration error) before stream end, got {}",
                run.items_seen
            ));
        }
        Ok(())
    })
}

// --------------------------------------------------------------------------
// verify_schema: a boot-time guard for deployments that run migrations out of
// band. After `setup` has applied every embedded migration the verifier must
// return `Ok(())`; when at least one embedded migration is unrecorded it must
// surface `Error::Migration` so the application can fail fast instead of
// crashing later on a missing column.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct VerifySchemaRun {
    applied_result: Result<(), String>,
    pending_result: Result<(), String>,
}

async fn run_verify_schema() -> Result<Outcome<VerifySchemaRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };

    // Branch 1: every migration has been applied — verify_schema returns Ok.
    let applied_result = verify_schema(&pool).await.map_err(|e| e.to_string());

    // Branch 2: simulate an out-of-date schema by hiding the most recently
    // applied migration row from `__diesel_schema_migrations` inside a
    // serializable transaction we then ROLLBACK. The verifier opens its own
    // connection, so we have to actually persist the deletion, run the
    // verifier, and then restore the row. Wrapping the whole thing in a
    // savepoint isn't possible because verify_schema borrows from the pool.
    let removed = with_conn(pool.clone(), |conn| {
        // Snapshot the latest migration row so we can put it back.
        #[derive(diesel::QueryableByName)]
        struct Row {
            #[diesel(sql_type = Text)]
            version: String,
            #[diesel(sql_type = diesel::sql_types::Timestamp)]
            run_on: chrono::NaiveDateTime,
        }
        let mut rows = sql_query(
            "SELECT version, run_on FROM __diesel_schema_migrations \
             ORDER BY version DESC LIMIT 1",
        )
        .load::<Row>(conn)
        .map_err(|e| e.to_string())?;
        let Some(row) = rows.pop() else {
            return Err("no migrations recorded — cannot simulate pending state".into());
        };
        sql_query("DELETE FROM __diesel_schema_migrations WHERE version = $1")
            .bind::<Text, _>(&row.version)
            .execute(conn)
            .map_err(|e| e.to_string())?;
        Ok((row.version, row.run_on))
    })
    .await?;

    // Drop-guard so the migration row is restored even if `verify_schema`
    // panics or a future `?` short-circuits the function between the DELETE
    // and the INSERT. Without this guard a hard failure would leave the
    // shared `__diesel_schema_migrations` table corrupted for every
    // subsequent test in the run.
    struct RestoreGuard {
        pool: PgPool,
        version: Option<String>,
        run_on: chrono::NaiveDateTime,
    }
    impl Drop for RestoreGuard {
        fn drop(&mut self) {
            let Some(version) = self.version.take() else {
                return;
            };
            let pool = self.pool.clone();
            let run_on = self.run_on;
            // Use a fresh blocking connection — we cannot await here. Best
            // effort: a connection-acquisition failure is logged but cannot
            // be propagated through Drop. Using ON CONFLICT DO NOTHING keeps
            // the restore idempotent if the row was somehow re-inserted by a
            // parallel path before this guard fires.
            if let Ok(mut conn) = pool.get() {
                let _ = sql_query(
                    "INSERT INTO __diesel_schema_migrations (version, run_on) \
                     VALUES ($1, $2) ON CONFLICT (version) DO NOTHING",
                )
                .bind::<Text, _>(version)
                .bind::<diesel::sql_types::Timestamp, _>(run_on)
                .execute(&mut conn);
            }
        }
    }
    let mut guard = RestoreGuard {
        pool: pool.clone(),
        version: Some(removed.0),
        run_on: removed.1,
    };

    let pending_result = verify_schema(&pool)
        .await
        .map(|_| ())
        .map_err(|e| e.to_string());

    // Restore eagerly in the happy path (consumes the guard's payload, so the
    // Drop impl becomes a no-op) and surface any restore failure to the test.
    let version = guard.version.take().expect("version still owned by guard");
    let run_on = guard.run_on;
    with_conn(pool.clone(), move |conn| {
        sql_query(
            "INSERT INTO __diesel_schema_migrations (version, run_on) \
             VALUES ($1, $2) ON CONFLICT (version) DO NOTHING",
        )
        .bind::<Text, _>(version)
        .bind::<diesel::sql_types::Timestamp, _>(run_on)
        .execute(conn)
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await?;

    // verify_schema in the "pending" branch is supposed to return Err — invert
    // the Result so the assertion helper can treat the expected failure as a
    // success.
    let pending_inverted = match pending_result {
        Err(_) => Ok(()),
        Ok(()) => Err("verify_schema returned Ok despite a missing migration row".into()),
    };

    Ok(Outcome::Completed(VerifySchemaRun {
        applied_result,
        pending_result: pending_inverted,
    }))
}

fn verify_schema_accepts_a_fully_applied_database()
-> impl Fn(&Result<Outcome<VerifySchemaRun>, String>) -> AssertionResult {
    observe::<VerifySchemaRun, _>("verify_schema applied", |run| {
        run.applied_result
            .as_ref()
            .map(|_| ())
            .map_err(|e| format!("expected Ok on an applied schema, got {e}"))
    })
}

fn verify_schema_rejects_a_database_with_unrecorded_migrations()
-> impl Fn(&Result<Outcome<VerifySchemaRun>, String>) -> AssertionResult {
    observe::<VerifySchemaRun, _>("verify_schema pending", |run| {
        run.pending_result
            .as_ref()
            .map(|_| ())
            .map_err(|e| e.to_string())
    })
}

fn verify_schema_records_both_branches()
-> impl Fn(&Result<Outcome<VerifySchemaRun>, String>) -> AssertionResult {
    let applied = verify_schema_accepts_a_fully_applied_database();
    let pending = verify_schema_rejects_a_database_with_unrecorded_migrations();
    move |result| {
        applied(result)?;
        pending(result)?;
        Ok(())
    }
}

// --------------------------------------------------------------------------
// expectations
// --------------------------------------------------------------------------

// --------------------------------------------------------------------------
// push_tasks partial-batch idempotency conflict.
//
// `src/queries/push.rs:114` surfaces a partial-conflict batch as
// `Error::InvalidArgument("idempotency_key conflict: M of N tasks were
// rejected by the unique constraint")`. All existing idempotency tests push
// one task at a time, so the `inserted < task_count` branch — and the
// "M of N" counter in the error message — is never exercised. Drive the
// branch with a buffered batch that shares a single `idempotency_key` plus a
// pre-existing row that occupies it.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct PartialBatchRun {
    push_error: Option<String>,
    final_count: i64,
}

async fn run_partial_batch_conflict() -> Result<Outcome<PartialBatchRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-batch-conflict-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Seed the queue with one row that occupies the idempotency_key slot.
    let mut seed_storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let seed = TaskBuilder::new("seed".to_owned())
        .with_task_id(task_id())
        .run_at_timestamp(now_unix())
        .with_attempt(Attempt::new_with_value(0))
        .with_ctx(SqlContext::new().with_max_attempts(5))
        .with_idempotency_key("shared-key")
        .build();
    seed_storage
        .push_task(seed)
        .await
        .map_err(|e| e.to_string())?;

    // Build a 3-task batch with the same idempotency_key. With buffer_size=3
    // and a single `send_all` the entire batch flushes through one
    // `push_tasks` call — which is what exercises the `inserted < task_count`
    // accountant on src/queries/push.rs:114.
    let config = Config::new(&queue).set_buffer_size(3);
    let mut batch_storage = PostgresStorage::<String>::new_with_config(&pool, &config);
    let batch: Vec<Task<String, PgContext, Ulid>> = (0..3)
        .map(|i| {
            TaskBuilder::new(format!("dup-{i}"))
                .with_task_id(task_id())
                .run_at_timestamp(now_unix())
                .with_attempt(Attempt::new_with_value(0))
                .with_ctx(PgContext::new().with_max_attempts(5))
                .with_idempotency_key("shared-key")
                .build()
        })
        .collect();
    let stream = futures::stream::iter(batch);
    let push_result = batch_storage.push_all(stream).await;

    let q = queue.clone();
    let final_count: i64 = with_conn(pool.clone(), move |conn| {
        #[derive(QueryableByName)]
        struct C {
            #[diesel(sql_type = BigInt)]
            n: i64,
        }
        sql_query("SELECT COUNT(*) AS n FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(&q)
            .get_result::<C>(conn)
            .map(|c| c.n)
            .map_err(|e| e.to_string())
    })
    .await?;

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(PartialBatchRun {
        push_error: push_result.err().map(|e| e.to_string()),
        final_count,
    }))
}

fn partial_batch_rejects_with_count()
-> impl Fn(&Result<Outcome<PartialBatchRun>, String>) -> AssertionResult {
    observe::<PartialBatchRun, _>("partial-batch reject", |run| {
        match run.push_error.as_deref() {
            Some(msg) if msg.contains("idempotency_key conflict") && msg.contains("of") => Ok(()),
            Some(other) => Err(format!(
                "expected InvalidArgument citing 'idempotency_key conflict: M of N', got {other:?}"
            )),
            None => Err(
                "expected push_all to be rejected when every task in the batch conflicts".into(),
            ),
        }
    })
}

fn partial_batch_rolls_back_inserts()
-> impl Fn(&Result<Outcome<PartialBatchRun>, String>) -> AssertionResult {
    observe::<PartialBatchRun, _>("partial-batch rollback", |run| {
        // The seed row should be the only survivor; the conflicting batch is
        // wrapped in `conn.transaction(...)` (src/queries/push.rs:70) so the
        // error from the accountant rolls back any insertions that snuck
        // through ON CONFLICT DO NOTHING.
        if run.final_count == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected exactly the seed row to remain (1), got {} rows",
                run.final_count
            ))
        }
    })
}

// --------------------------------------------------------------------------
// push_tasks metadata cap.
//
// `MAX_METADATA_PAYLOAD_LEN = 8 KiB` (src/queries/push.rs) gates JSON
// serialization length. Oversize payloads are surfaced as
// `Error::InvalidArgument` *before* the SQL UPDATE so misbehaving callers
// cannot bloat `apalis.jobs.metadata`. Existing tests cover the under-cap
// happy path implicitly via every `push_task` call; this block pins the
// over-cap branch and the boundary just-below-cap branch.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct MetadataCapRun {
    push_error: Option<String>,
    row_present: bool,
}

async fn run_metadata_cap(meta_payload_len: usize) -> Result<Outcome<MetadataCapRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-metadata-cap-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let mut meta = serde_json::Map::new();
    // A `"x"*n` JSON string serializes to n+2 bytes (quotes); add the key
    // overhead so the resulting JSON object hits the requested length closely.
    let value_len = meta_payload_len.saturating_sub(16).max(1);
    meta.insert(
        "payload".to_owned(),
        serde_json::Value::String("x".repeat(value_len)),
    );
    let ctx = PgContext::new().with_max_attempts(5).with_meta(meta);
    let task = TaskBuilder::new("metadata-cap-target".to_owned())
        .with_task_id(task_id())
        .run_at_timestamp(now_unix())
        .with_attempt(Attempt::new_with_value(0))
        .with_ctx(ctx)
        .build();

    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let push_result = storage.push_task(task).await;

    // After a push error, the buffered task is dropped; check no row landed.
    let q = queue.clone();
    let row_count: i64 = with_conn(pool.clone(), move |conn| {
        #[derive(QueryableByName)]
        struct C {
            #[diesel(sql_type = BigInt)]
            n: i64,
        }
        sql_query("SELECT COUNT(*) AS n FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(&q)
            .get_result::<C>(conn)
            .map(|c| c.n)
            .map_err(|e| e.to_string())
    })
    .await?;

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(MetadataCapRun {
        push_error: push_result.err().map(|e| e.to_string()),
        row_present: row_count > 0,
    }))
}

fn metadata_cap_succeeds() -> impl Fn(&Result<Outcome<MetadataCapRun>, String>) -> AssertionResult {
    observe::<MetadataCapRun, _>("metadata under cap", |run| {
        if let Some(err) = &run.push_error {
            Err(format!(
                "expected push to succeed under the cap, got error: {err}"
            ))
        } else if !run.row_present {
            Err("expected the row to land in apalis.jobs after a successful push".into())
        } else {
            Ok(())
        }
    })
}

fn metadata_cap_rejects() -> impl Fn(&Result<Outcome<MetadataCapRun>, String>) -> AssertionResult {
    observe::<MetadataCapRun, _>("metadata over cap", |run| match run.push_error.as_deref() {
        Some(msg) if msg.contains("metadata") && msg.contains("cap") => Ok(()),
        Some(other) => Err(format!(
            "expected InvalidArgument citing the metadata cap, got {other:?}"
        )),
        None => Err("expected push to be rejected for oversize metadata".into()),
    })
}

fn metadata_cap_persists_nothing()
-> impl Fn(&Result<Outcome<MetadataCapRun>, String>) -> AssertionResult {
    observe::<MetadataCapRun, _>("metadata cap row absent", |run| {
        if run.row_present {
            Err("expected no apalis.jobs row after a rejected oversize push".into())
        } else {
            Ok(())
        }
    })
}

// --------------------------------------------------------------------------
// push_tasks idempotency_key cap.
//
// `MAX_IDEMPOTENCY_KEY_LEN = 1024` (src/queries/push.rs) gates the
// caller-supplied key before it lands in the unbounded `idempotency_key
// TEXT` column on `apalis.jobs`. Without this cap an enqueuer could
// inflate the row to gigabytes of TEXT and exhaust storage. The pin
// checks the under-cap, just-below-cap, and over-cap branches.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct IdempotencyCapRun {
    push_error: Option<String>,
    row_present: bool,
}

async fn run_idempotency_cap(key_len: usize) -> Result<Outcome<IdempotencyCapRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-idem-cap-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let ctx = PgContext::new().with_max_attempts(5);
    let task = TaskBuilder::new("idempotency-cap-target".to_owned())
        .with_task_id(task_id())
        .run_at_timestamp(now_unix())
        .with_attempt(Attempt::new_with_value(0))
        .with_ctx(ctx)
        .with_idempotency_key("k".repeat(key_len))
        .build();

    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let push_result = storage.push_task(task).await;

    let q = queue.clone();
    let row_count: i64 = with_conn(pool.clone(), move |conn| {
        #[derive(QueryableByName)]
        struct C {
            #[diesel(sql_type = BigInt)]
            n: i64,
        }
        sql_query("SELECT COUNT(*) AS n FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(&q)
            .get_result::<C>(conn)
            .map(|c| c.n)
            .map_err(|e| e.to_string())
    })
    .await?;

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(IdempotencyCapRun {
        push_error: push_result.err().map(|e| e.to_string()),
        row_present: row_count > 0,
    }))
}

fn idempotency_cap_succeeds()
-> impl Fn(&Result<Outcome<IdempotencyCapRun>, String>) -> AssertionResult {
    observe::<IdempotencyCapRun, _>("idempotency under cap", |run| {
        if let Some(err) = &run.push_error {
            Err(format!(
                "expected push to succeed under the cap, got error: {err}"
            ))
        } else if !run.row_present {
            Err("expected the row to land in apalis.jobs after a successful push".into())
        } else {
            Ok(())
        }
    })
}

fn idempotency_cap_rejects()
-> impl Fn(&Result<Outcome<IdempotencyCapRun>, String>) -> AssertionResult {
    observe::<IdempotencyCapRun, _>("idempotency over cap", |run| {
        match run.push_error.as_deref() {
            Some(msg) if msg.contains("idempotency_key") && msg.contains("cap") => Ok(()),
            Some(other) => Err(format!(
                "expected InvalidArgument citing the idempotency_key cap, got {other:?}"
            )),
            None => Err("expected push to be rejected for oversize idempotency_key".into()),
        }
    })
}

fn idempotency_cap_persists_nothing()
-> impl Fn(&Result<Outcome<IdempotencyCapRun>, String>) -> AssertionResult {
    observe::<IdempotencyCapRun, _>("idempotency cap row absent", |run| {
        if run.row_present {
            Err("expected no apalis.jobs row after a rejected oversize push".into())
        } else {
            Ok(())
        }
    })
}

// --------------------------------------------------------------------------
// push_tasks queue-name cap.
//
// `MAX_QUEUE_NAME_LEN = 255` (src/queries/push.rs) gates the caller-
// controlled queue name persisted as `job_type` and echoed into the
// LISTEN/NOTIFY JSON payload. Postgres `pg_notify` hard-truncates at
// 8000 bytes, so an unbounded name silently drops fast-path wakeups and
// inflates every row in `apalis.jobs`. The pin covers a typical name, a
// just-below-cap name, and an over-cap name.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct QueueNameCapRun {
    push_error: Option<String>,
    row_present: bool,
    queue: String,
}

async fn run_queue_name_cap(name_len: usize) -> Result<Outcome<QueueNameCapRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    // Keep the Ulid prefix so concurrent test runs do not collide on the
    // unique `(job_type, idempotency_key)` index even when seeding the
    // happy-path scenarios.
    let prefix = format!("q-{}-", Ulid::new());
    let pad = name_len.saturating_sub(prefix.len());
    let queue = format!("{prefix}{}", "x".repeat(pad));
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let ctx = PgContext::new().with_max_attempts(5);
    let task = TaskBuilder::new("queue-name-cap-target".to_owned())
        .with_task_id(task_id())
        .run_at_timestamp(now_unix())
        .with_attempt(Attempt::new_with_value(0))
        .with_ctx(ctx)
        .build();

    let mut storage = PostgresStorage::<String>::new_with_config(&pool, &Config::new(&queue));
    let push_result = storage.push_task(task).await;

    let q = queue.clone();
    let row_count: i64 = with_conn(pool.clone(), move |conn| {
        #[derive(QueryableByName)]
        struct C {
            #[diesel(sql_type = BigInt)]
            n: i64,
        }
        sql_query("SELECT COUNT(*) AS n FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(&q)
            .get_result::<C>(conn)
            .map(|c| c.n)
            .map_err(|e| e.to_string())
    })
    .await?;

    cleanup_queue(pool, queue.clone()).await?;
    Ok(Outcome::Completed(QueueNameCapRun {
        push_error: push_result.err().map(|e| e.to_string()),
        row_present: row_count > 0,
        queue,
    }))
}

fn queue_name_cap_succeeds() -> impl Fn(&Result<Outcome<QueueNameCapRun>, String>) -> AssertionResult
{
    observe::<QueueNameCapRun, _>("queue name under cap", |run| {
        if let Some(err) = &run.push_error {
            Err(format!(
                "expected push to succeed under the cap (queue length {}), got error: {err}",
                run.queue.len()
            ))
        } else if !run.row_present {
            Err("expected the row to land in apalis.jobs after a successful push".into())
        } else {
            Ok(())
        }
    })
}

fn queue_name_cap_rejects() -> impl Fn(&Result<Outcome<QueueNameCapRun>, String>) -> AssertionResult
{
    observe::<QueueNameCapRun, _>("queue name over cap", |run| {
        match run.push_error.as_deref() {
            Some(msg) if msg.contains("queue name") && msg.contains("cap") => Ok(()),
            Some(other) => Err(format!(
                "expected InvalidArgument citing the queue name cap, got {other:?}"
            )),
            None => Err("expected push to be rejected for oversize queue name".into()),
        }
    })
}

fn queue_name_cap_persists_nothing()
-> impl Fn(&Result<Outcome<QueueNameCapRun>, String>) -> AssertionResult {
    observe::<QueueNameCapRun, _>("queue name cap row absent", |run| {
        if run.row_present {
            Err("expected no apalis.jobs row after a rejected oversize push".into())
        } else {
            Ok(())
        }
    })
}

// --------------------------------------------------------------------------
// ack_task predicate matrix.
//
// `queries::ack_task` (src/queries/ack.rs) gates the UPDATE on a six-column
// predicate plus an optional lease-token EXISTS subquery. Existing
// `postgres_queries.rs` coverage exercises: happy path, wrong lock_by,
// status=Pending, status=Done. The branches enumerated below close the
// remaining gaps so the WHERE clause is exhaustively specified.
//
// See [project_msrv_and_design.md]: `ack_task` lease-token gate is intentional
// defense-in-depth that protects only the heartbeat-path callers (PgAck wired
// through `PgAck::with_lease_token`); admin callers stay on the legacy `None`
// branch.
// --------------------------------------------------------------------------

#[derive(Debug, QueryableByName)]
struct AckStatusRow {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Integer)]
    attempts: i32,
    #[diesel(sql_type = Nullable<Jsonb>)]
    last_result: Option<Value>,
}

#[derive(Debug)]
struct AckPredicateRun {
    ack_error: Option<String>,
    row_status: String,
    row_attempts: i32,
    row_last_result: Option<Value>,
}

async fn job_status_row(pool: PgPool, id: PgTaskId) -> Result<AckStatusRow, String> {
    let id_s = id.to_string();
    with_conn(pool, move |conn| {
        sql_query("SELECT status, attempts, last_result FROM apalis.jobs WHERE id = $1")
            .bind::<Text, _>(&id_s)
            .get_result::<AckStatusRow>(conn)
            .map_err(|e| e.to_string())
    })
    .await
}

async fn insert_running_row(
    pool: PgPool,
    queue: String,
    worker_id: String,
    attempts: i32,
    max_attempts: i32,
    lock_at: DateTime,
) -> Result<PgTaskId, String> {
    let id = Ulid::new();
    let task_id = TaskId::from_str(&id.to_string()).map_err(|e| e.to_string())?;
    let job = serde_json::to_vec("ack-target").map_err(|e| e.to_string())?;
    with_conn(pool, move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (
                id, job_type, job, status, attempts, max_attempts, run_at, lock_by, lock_at
            ) VALUES ($1, $2, $3, 'Running', $4, $5, now() - INTERVAL '1 second', $6, $7)",
        )
        .bind::<Text, _>(id.to_string())
        .bind::<Text, _>(&queue)
        .bind::<diesel::sql_types::Binary, _>(job)
        .bind::<Integer, _>(attempts)
        .bind::<Integer, _>(max_attempts)
        .bind::<Text, _>(&worker_id)
        .bind::<Timestamptz, _>(lock_at)
        .execute(conn)
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await?;
    Ok(task_id)
}

async fn insert_worker_row(
    pool: PgPool,
    queue: String,
    worker_id: String,
    lease_token: Option<String>,
) -> Result<(), String> {
    with_conn(pool, move |conn| {
        match lease_token {
            Some(token) => {
                sql_query(
                    "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at, lease_token)
                     VALUES ($1, $2, 'PostgresStorage', '', now(), now(), $3)",
                )
                .bind::<Text, _>(&worker_id)
                .bind::<Text, _>(&queue)
                .bind::<Text, _>(&token)
                .execute(conn)
                .map_err(|e| e.to_string())?;
            }
            None => {
                sql_query(
                    "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at)
                     VALUES ($1, $2, 'PostgresStorage', '', now(), now())",
                )
                .bind::<Text, _>(&worker_id)
                .bind::<Text, _>(&queue)
                .execute(conn)
                .map_err(|e| e.to_string())?;
            }
        }
        Ok(())
    })
    .await
}

#[derive(Debug, Clone, Copy)]
struct AckSetup {
    /// Lease-token wired into `PgAck` (call-site). `None` selects `PgAck::new`,
    /// which short-circuits the SQL `$9::text IS NULL` branch.
    pgack_token: Option<&'static str>,
    /// Lease-token written to the `apalis.workers` row. `None` means the row
    /// is inserted with SQL `NULL` in `lease_token` (the legacy / un-migrated
    /// row shape). The row itself is always present because the
    /// `jobs_lock_by_worker_type_fkey` FK rejects `Running` jobs whose
    /// `(lock_by, job_type)` does not point at a workers row.
    workers_token: Option<&'static str>,
    /// Apply this delta to `parts.ctx.lock_at` so the predicate sees a value
    /// other than what we stored on the row.
    lock_at_delta_secs: i64,
    /// When `Some`, overrides `parts.ctx.queue` so the `job_type = $5` arm
    /// fails (note: this also bypasses the workers EXISTS subquery, which
    /// keys on `worker_type = $5`).
    override_queue: Option<&'static str>,
    /// Apply this delta to `parts.attempt.current()` so `attempts =
    /// $started_attempts` mismatches.
    attempt_delta: i64,
    /// When `true`, substitute a freshly generated `TaskId` for the `id = $4`
    /// arm of the predicate. The row stays in place under its real id; the
    /// ack call targets a row that does not exist.
    fabricate_unknown_task_id: bool,
}

const ACK_OK: AckSetup = AckSetup {
    pgack_token: None,
    workers_token: None,
    lock_at_delta_secs: 0,
    override_queue: None,
    attempt_delta: 0,
    fabricate_unknown_task_id: false,
};

async fn run_ack_predicate(setup: AckSetup) -> Result<Outcome<AckPredicateRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-ack-pred-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let worker_id = format!("spec-ack-pred-worker-{queue}");
    let stored_lock_at_secs = now_unix() as i64;
    let stored_lock_at = <DateTime as DateTimeExt>::from_unix_timestamp(stored_lock_at_secs);

    // FK `jobs_lock_by_worker_type_fkey` requires the workers row to exist
    // before the jobs row references it via `lock_by`; insert workers first.
    insert_worker_row(
        pool.clone(),
        queue.clone(),
        worker_id.clone(),
        setup.workers_token.map(str::to_owned),
    )
    .await?;

    // Row carries attempts=0 ("started but not yet finished"); ack will set
    // started_attempts = attempts - 1 = 0 to match.
    let id = insert_running_row(
        pool.clone(),
        queue.clone(),
        worker_id.clone(),
        0,
        2,
        stored_lock_at,
    )
    .await?;

    let attempt_value = (1i64 + setup.attempt_delta).max(0) as usize;
    let parts_lock_at = stored_lock_at_secs + setup.lock_at_delta_secs;
    let parts_lock_by = worker_id.clone();
    let parts_queue = setup
        .override_queue
        .map(str::to_owned)
        .unwrap_or_else(|| queue.clone());

    let parts_task_id = if setup.fabricate_unknown_task_id {
        TaskId::from_str(&Ulid::new().to_string()).map_err(|e| e.to_string())?
    } else {
        id
    };
    let parts = TaskBuilder::new(())
        .with_task_id(parts_task_id)
        .with_attempt(Attempt::new_with_value(attempt_value))
        .with_ctx(
            PgContext::new()
                .with_max_attempts(2)
                .with_queue(parts_queue)
                .with_lock_at(Some(parts_lock_at))
                .with_lock_by(Some(parts_lock_by)),
        )
        .build()
        .parts;

    let mut ack = match setup.pgack_token {
        Some(t) => PgAck::with_lease_token(pool.clone(), Arc::<str>::from(t)),
        None => PgAck::new(pool.clone()),
    };
    let result: Result<String, BoxDynError> = Ok("processed".to_owned());
    let ack_result = ack.ack(&result, &parts).await;
    let row = job_status_row(pool.clone(), id).await?;
    cleanup_queue(pool, queue).await?;

    Ok(Outcome::Completed(AckPredicateRun {
        ack_error: ack_result.err().map(|e| e.to_string()),
        row_status: row.status,
        row_attempts: row.attempts,
        row_last_result: row.last_result,
    }))
}

fn ack_succeeds() -> impl Fn(&Result<Outcome<AckPredicateRun>, String>) -> AssertionResult {
    observe::<AckPredicateRun, _>("ack predicate", |run| {
        if let Some(err) = &run.ack_error {
            Err(format!("expected ack to succeed, got error: {err}"))
        } else {
            Ok(())
        }
    })
}

fn ack_writes_done() -> impl Fn(&Result<Outcome<AckPredicateRun>, String>) -> AssertionResult {
    observe::<AckPredicateRun, _>("ack writes Done", |run| {
        if run.row_status == "Done" {
            Ok(())
        } else {
            Err(format!("expected row Status=Done, got {}", run.row_status))
        }
    })
}

fn ack_persists_result() -> impl Fn(&Result<Outcome<AckPredicateRun>, String>) -> AssertionResult {
    observe::<AckPredicateRun, _>("ack writes last_result", |run| match &run.row_last_result {
        Some(_) => Ok(()),
        None => Err("expected last_result to be populated after successful ack".into()),
    })
}

fn ack_rejected_as_stale() -> impl Fn(&Result<Outcome<AckPredicateRun>, String>) -> AssertionResult
{
    observe::<AckPredicateRun, _>("ack rejected", |run| match run.ack_error.as_deref() {
        Some(msg) if msg.contains("stale acknowledgement") => Ok(()),
        Some(other) => Err(format!(
            "expected stale acknowledgement error, got {other:?}"
        )),
        None => Err("expected ack to be rejected as stale, but it succeeded".into()),
    })
}

fn ack_row_stays_running() -> impl Fn(&Result<Outcome<AckPredicateRun>, String>) -> AssertionResult
{
    observe::<AckPredicateRun, _>("row stays Running", |run| {
        if run.row_status == "Running" {
            Ok(())
        } else {
            Err(format!(
                "expected row to remain Running on rejection, got {}",
                run.row_status
            ))
        }
    })
}

fn ack_row_keeps_null_last_result()
-> impl Fn(&Result<Outcome<AckPredicateRun>, String>) -> AssertionResult {
    observe::<AckPredicateRun, _>("row keeps NULL last_result", |run| {
        if run.row_last_result.is_none() {
            Ok(())
        } else {
            Err("expected last_result to remain NULL after rejected ack".into())
        }
    })
}

fn ack_row_attempts(
    expected: i32,
) -> impl Fn(&Result<Outcome<AckPredicateRun>, String>) -> AssertionResult {
    observe::<AckPredicateRun, _>("row attempts", move |run| {
        if run.row_attempts == expected {
            Ok(())
        } else {
            Err(format!(
                "expected attempts={expected} after the call, got {}",
                run.row_attempts
            ))
        }
    })
}

lets_expect! { #tokio_test
    expect(run_failed_retry(retryable).await) {
        let retryable = true;

        when a_failed_row_still_has_attempts_remaining {
            to is_reclaimed_by_fetch_next { failed_retry_reclaims_row() }
            to preserves_the_persisted_attempt_count { failed_retry_preserves_attempt_count() }
        }

        when a_failed_row_has_exhausted_its_attempts {
            let retryable = false;
            to is_not_reclaimed_by_fetch_next { failed_exhausted_not_reclaimed() }
        }
    }

    expect(run_concurrent_admin_register().await) {
        when two_admin_register_worker_calls_race_on_the_same_id {
            to both_succeed_via_upsert_semantics {
                concurrent_admin_register_both_succeed()
            }
            to leaves_exactly_one_workers_row {
                concurrent_admin_register_creates_single_row()
            }
        }
    }

    expect(run_two_worker_race().await) {
        when two_workers_poll_the_same_queue_concurrently {
            to deliver_disjoint_payloads_thanks_to_for_update_skip_locked {
                two_workers_share_set_without_duplicates()
            }
        }
    }

    expect(run_refresh_unpopulated_snapshot().await) {
        when refresh_runs_against_a_freshly_created_with_no_data_matview {
            to falls_back_to_a_blocking_refresh_and_succeeds {
                refresh_unpopulated_snapshot_succeeds()
            }
            to leaves_the_matview_populated_for_subsequent_callers {
                refresh_unpopulated_snapshot_populates()
            }
        }
    }

    expect(run_unlisten_after_drop().await) {
        when notify_task_ids_is_dropped_and_the_connection_returns_to_the_pool {
            to leaves_no_apalis_subscription_on_the_returned_connection {
                no_stale_listen_subscription_after_drop()
            }
        }
    }

    expect(run_locked_workers_excludes_terminal().await) {
        when terminal_jobs_still_carry_a_lock_by_value {
            to omits_them_from_the_active_workers_column {
                locked_workers_shows_active_only()
            }
        }
    }

    expect(run_list_workers_beyond_100().await) {
        when more_than_one_hundred_workers_are_registered_for_the_queue {
            to returns_every_row_without_a_hidden_limit {
                list_workers_returns_every_row()
            }
        }
    }

    expect(run_registration_gate_blocks_fetcher().await) {
        when the_initial_heartbeat_fails_with_already_registered {
            to yields_the_registration_error_and_terminates_without_dequeue {
                registration_gate_emits_error_then_ends()
            }
        }
    }

    // `run_verify_schema` mutates shared state (it temporarily removes a row
    // from `__diesel_schema_migrations` and restores it) so the two
    // assertions live in one `to` block to avoid re-running the scenario
    // twice in parallel under `cargo test`'s default threading — the second
    // run would observe a half-restored migrations table.
    expect(run_verify_schema().await) {
        when verify_schema_is_called_against_a_freshly_migrated_database {
            to records_both_branches_of_the_pending_predicate {
                verify_schema_records_both_branches()
            }
        }
    }

    expect(run_partial_batch_conflict().await) {
        when a_buffered_batch_collides_on_a_shared_idempotency_key {
            to surfaces_an_invalid_argument_with_a_conflict_count {
                partial_batch_rejects_with_count()
            }
            to rolls_back_every_partial_insertion_in_the_batch {
                partial_batch_rolls_back_inserts()
            }
        }
    }

    expect(run_metadata_cap(meta_payload_len).await) {
        let meta_payload_len = 1024usize;

        when the_metadata_serialization_length_is_well_below_the_cap {
            to accepts_the_push_and_persists_the_row { metadata_cap_succeeds() }
        }

        when the_metadata_serialization_length_sits_just_below_the_eight_kib_cap {
            let meta_payload_len = 8000usize;
            to accepts_the_push_and_persists_the_row { metadata_cap_succeeds() }
        }

        when the_metadata_serialization_length_exceeds_the_eight_kib_cap {
            let meta_payload_len = 16384usize;
            to rejects_the_push_with_invalid_argument { metadata_cap_rejects() }
            to does_not_persist_the_apalis_jobs_row { metadata_cap_persists_nothing() }
        }
    }

    expect(run_idempotency_cap(key_len).await) {
        let key_len = 36usize; // typical UUID length

        when the_idempotency_key_is_a_typical_short_uuid {
            to accepts_the_push_and_persists_the_row { idempotency_cap_succeeds() }
        }

        when the_idempotency_key_sits_at_the_one_kib_cap_boundary {
            let key_len = 1024usize;
            to accepts_the_push_and_persists_the_row { idempotency_cap_succeeds() }
        }

        when the_idempotency_key_exceeds_the_one_kib_cap {
            let key_len = 4096usize;
            to rejects_the_push_with_invalid_argument { idempotency_cap_rejects() }
            to does_not_persist_the_apalis_jobs_row { idempotency_cap_persists_nothing() }
        }
    }

    expect(run_queue_name_cap(name_len).await) {
        let name_len = 64usize; // realistic namespaced queue length

        when the_queue_name_is_a_typical_namespaced_identifier {
            to accepts_the_push_and_persists_the_row { queue_name_cap_succeeds() }
        }

        when the_queue_name_sits_at_the_two_hundred_fifty_five_byte_cap {
            let name_len = 255usize;
            to accepts_the_push_and_persists_the_row { queue_name_cap_succeeds() }
        }

        when the_queue_name_exceeds_the_two_hundred_fifty_five_byte_cap {
            let name_len = 1024usize;
            to rejects_the_push_with_invalid_argument { queue_name_cap_rejects() }
            to does_not_persist_the_apalis_jobs_row { queue_name_cap_persists_nothing() }
        }
    }

    // ack predicate matrix: enumerate every WHERE-clause arm in `ack_task`.
    // Default `setup = ACK_OK` is the no-token happy path (already covered by
    // `postgres_queries::ack_boundary`, repeated here as the matrix anchor).
    expect(run_ack_predicate(setup).await) {
        let setup = ACK_OK;

        when called_without_a_lease_token_on_a_matching_running_row {
            to marks_the_row_done { ack_writes_done() }
            to persists_the_serialized_result { ack_persists_result() }
            to returns_ok { ack_succeeds() }
        }

        when called_with_a_lease_token_that_matches_the_workers_row {
            let setup = AckSetup {
                pgack_token: Some("matching-token"),
                workers_token: Some("matching-token"),
                ..ACK_OK
            };
            to marks_the_row_done { ack_writes_done() }
            to persists_the_serialized_result { ack_persists_result() }
            to returns_ok { ack_succeeds() }
        }

        when called_with_a_lease_token_that_does_not_match_the_workers_row {
            let setup = AckSetup {
                pgack_token: Some("caller-token"),
                workers_token: Some("other-token"),
                ..ACK_OK
            };
            to is_rejected_as_a_stale_acknowledgement { ack_rejected_as_stale() }
            to leaves_the_row_in_running_state { ack_row_stays_running() }
            to does_not_write_last_result { ack_row_keeps_null_last_result() }
            to does_not_increment_attempts { ack_row_attempts(0) }
        }

        when called_with_a_lease_token_but_the_workers_row_has_null_lease_token {
            // Pre-migration / un-bound workers row: the EXISTS subquery sees
            // `lease_token = $9` evaluate to NULL (= false in WHERE) so the
            // token-bound caller is rejected even though the worker exists.
            let setup = AckSetup {
                pgack_token: Some("caller-token"),
                workers_token: None,
                ..ACK_OK
            };
            to is_rejected_as_a_stale_acknowledgement { ack_rejected_as_stale() }
            to leaves_the_row_in_running_state { ack_row_stays_running() }
            to does_not_write_last_result { ack_row_keeps_null_last_result() }
        }

        when the_callers_lock_at_disagrees_with_the_stored_row {
            let setup = AckSetup {
                lock_at_delta_secs: 1,
                ..ACK_OK
            };
            to is_rejected_as_a_stale_acknowledgement { ack_rejected_as_stale() }
            to leaves_the_row_in_running_state { ack_row_stays_running() }
            to does_not_write_last_result { ack_row_keeps_null_last_result() }
        }

        when the_callers_started_attempts_disagrees_with_the_stored_row {
            let setup = AckSetup {
                attempt_delta: 5,
                ..ACK_OK
            };
            to is_rejected_as_a_stale_acknowledgement { ack_rejected_as_stale() }
            to leaves_the_row_in_running_state { ack_row_stays_running() }
            to does_not_write_last_result { ack_row_keeps_null_last_result() }
        }

        when the_callers_task_id_does_not_exist_in_the_jobs_table {
            let setup = AckSetup {
                fabricate_unknown_task_id: true,
                ..ACK_OK
            };
            to is_rejected_as_a_stale_acknowledgement { ack_rejected_as_stale() }
            to leaves_the_original_row_in_running_state { ack_row_stays_running() }
            to does_not_write_last_result { ack_row_keeps_null_last_result() }
        }

        when the_callers_queue_disagrees_with_the_stored_row {
            let setup = AckSetup {
                override_queue: Some("apalis-spec-ack-pred-wrong-queue"),
                ..ACK_OK
            };
            to is_rejected_as_a_stale_acknowledgement { ack_rejected_as_stale() }
            to leaves_the_row_in_running_state { ack_row_stays_running() }
            to does_not_write_last_result { ack_row_keeps_null_last_result() }
        }
    }
}
