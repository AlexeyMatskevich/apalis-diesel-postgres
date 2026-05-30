//! End-to-end coverage of `PostgresStorage` driven by the real apalis
//! `Worker` runtime. Guards `Backend: Send + Sync` and the in-handler outbox
//! semantics. Skipped silently when `DATABASE_URL` is unset.

#![cfg(feature = "tokio")]

mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use apalis::prelude::*;
use apalis_core::{backend::FetchById, task::status::Status};
use apalis_diesel_postgres::{
    Config, Error as PgError, PgPool, PgTask, PostgresStorage, build_pool, setup,
};
use apalis_sql::context::SqlContext;
use diesel::{Connection, RunQueryDsl, sql_query, sql_types::Text};
use lets_expect::{AssertionError, AssertionResult, *};
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use ulid::Ulid;

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SendEmail {
    to: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct LogActivity {
    kind: String,
    target: String,
}

#[derive(Debug, Clone, Copy)]
enum HandlerOutcome {
    Ok,
    Err,
}

/// Aggregated observations from one `WorkerBuilder::run()` pass.
#[derive(Debug)]
struct WorkerRun {
    handler_invocations: u64,
    in_handler_push_invocations: u64,
    email_status: Option<Status>,
    activity_count: i64,
}

#[derive(Debug)]
enum WorkerOutcome {
    Skipped,
    Completed(WorkerRun),
}

async fn cleanup_queues(pool: PgPool, queues: Vec<String>) -> Result<(), String> {
    tokio::task::spawn_blocking(move || -> Result<(), String> {
        let mut conn = pool.get().map_err(|e| e.to_string())?;
        for queue in &queues {
            sql_query("DELETE FROM apalis.jobs WHERE job_type = $1")
                .bind::<Text, _>(queue)
                .execute(&mut conn)
                .map_err(|e| e.to_string())?;
            sql_query("DELETE FROM apalis.workers WHERE worker_type = $1")
                .bind::<Text, _>(queue)
                .execute(&mut conn)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    })
    .await
    .map_err(|e| e.to_string())??;
    Ok(())
}

#[derive(diesel::QueryableByName)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn count_jobs(pool: PgPool, queue: String) -> Result<i64, String> {
    tokio::task::spawn_blocking(move || -> Result<i64, String> {
        let mut conn = pool.get().map_err(|e| e.to_string())?;
        let row: CountRow =
            sql_query("SELECT COUNT(*)::bigint AS n FROM apalis.jobs WHERE job_type = $1")
                .bind::<Text, _>(&queue)
                .get_result(&mut conn)
                .map_err(|e| e.to_string())?;
        Ok(row.n)
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn run_worker_integration(handler_outcome: HandlerOutcome) -> Result<WorkerOutcome, String> {
    let Some(database_url) = support::database_url_or_skip()? else {
        return Ok(WorkerOutcome::Skipped);
    };

    let pool = build_pool(database_url).map_err(|e| e.to_string())?;
    setup(&pool).await.map_err(|e| e.to_string())?;

    let suffix = Ulid::new();
    let emails_queue = format!("worker-int-emails-{suffix}");
    let activity_queue = format!("worker-int-activity-{suffix}");
    cleanup_queues(
        pool.clone(),
        vec![emails_queue.clone(), activity_queue.clone()],
    )
    .await?;

    let mut emails: PostgresStorage<SendEmail> =
        PostgresStorage::new_with_config(&pool, &Config::new(&emails_queue));
    let activity: PostgresStorage<LogActivity> =
        PostgresStorage::new_with_config(&pool, &Config::new(&activity_queue));

    let email_task_id = {
        let storage = emails.clone();
        tokio::task::spawn_blocking(move || -> Result<_, PgError> {
            let mut conn = storage.pool().get().map_err(PgError::Pool)?;
            conn.transaction(|c| {
                // `max_attempts = 1` makes a single handler `Err` terminal
                // (`Killed`), so the err-branch row is never re-fetchable. This
                // keeps `handler_invocations == 1` deterministic instead of
                // relying on `worker_handle.abort()` winning a race against the
                // fetcher's next poll (a `Failed` row with attempts left would
                // be immediately re-claimed).
                let mut task = PgTask::<SendEmail>::new(SendEmail {
                    to: "ada@example.com".to_owned(),
                });
                task.parts.ctx = SqlContext::new().with_max_attempts(1);
                storage.push_task_with_conn(c, task)
            })
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?
    };

    let invocations = Arc::new(AtomicU64::new(0));
    let push_ok = Arc::new(AtomicU64::new(0));
    let done = Arc::new(Notify::new());

    let activity_for_handler = activity.clone();
    let invocations_for_handler = invocations.clone();
    let push_ok_for_handler = push_ok.clone();
    let done_for_handler = done.clone();

    let handler = move |job: SendEmail| {
        let activity = activity_for_handler.clone();
        let invocations = invocations_for_handler.clone();
        let push_ok = push_ok_for_handler.clone();
        let done = done_for_handler.clone();
        async move {
            invocations.fetch_add(1, Ordering::Relaxed);

            let activity_for_blocking = activity.clone();
            let to = job.to.clone();
            let pushed = tokio::task::spawn_blocking(move || -> Result<(), PgError> {
                let mut conn = activity_for_blocking.pool().get().map_err(PgError::Pool)?;
                conn.transaction(|c| {
                    activity_for_blocking.push_with_conn(
                        c,
                        LogActivity {
                            kind: "email_sent".to_owned(),
                            target: to,
                        },
                    )?;
                    Ok::<_, PgError>(())
                })
            })
            .await;
            if matches!(pushed, Ok(Ok(()))) {
                push_ok.fetch_add(1, Ordering::Relaxed);
            }

            done.notify_one();
            match handler_outcome {
                HandlerOutcome::Ok => Ok(()),
                HandlerOutcome::Err => {
                    Err::<(), BoxDynError>("handler intentionally failed".into())
                }
            }
        }
    };

    let worker = WorkerBuilder::new("worker-int-emails")
        .backend(emails.clone())
        .build(handler);
    let worker_handle = tokio::spawn(async move {
        let _ = worker.run().await;
    });

    // Barrier: wait until the handler has run (or 10s) before polling for the
    // terminal row, so the poll loop observes the post-execution state. The
    // invocation count is asserted separately, so the wait's boolean result
    // carries no extra signal and is intentionally not captured.
    let _ = tokio::time::timeout(Duration::from_secs(10), done.notified()).await;

    // Poll until apalis writes the terminal row — Done for the ok branch,
    // anything non-Running for the err branch. Deterministic on behaviour, not
    // wall time.
    let target_terminal = matches!(handler_outcome, HandlerOutcome::Ok);
    let email_status = {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let status = emails
                .fetch_by_id(&email_task_id)
                .await
                .map_err(|e| e.to_string())?
                .map(|task| task.parts.status.load());
            let reached = match status {
                Some(Status::Done) => target_terminal,
                Some(Status::Failed | Status::Killed) => !target_terminal,
                _ => false,
            };
            if reached || std::time::Instant::now() >= deadline {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    };

    worker_handle.abort();
    if let Err(join_err) = worker_handle.await
        && join_err.is_panic()
    {
        std::panic::resume_unwind(join_err.into_panic());
    }

    let handler_invocations = invocations.load(Ordering::Relaxed);
    let in_handler_push_invocations = push_ok.load(Ordering::Relaxed);
    let activity_count = count_jobs(pool.clone(), activity_queue.clone()).await?;
    cleanup_queues(pool, vec![emails_queue, activity_queue]).await?;

    Ok(WorkerOutcome::Completed(WorkerRun {
        handler_invocations,
        in_handler_push_invocations,
        email_status,
        activity_count,
    }))
}

async fn worker_outcome(handler_outcome: HandlerOutcome) -> Result<WorkerOutcome, String> {
    run_worker_integration(handler_outcome).await
}

fn observe<F>(
    name: &'static str,
    check: F,
) -> impl Fn(&Result<WorkerOutcome, String>) -> AssertionResult
where
    F: Fn(&WorkerRun) -> Result<(), String>,
{
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "{name}: worker integration failed: {error}"
        )])),
        Ok(WorkerOutcome::Skipped) => Ok(()),
        Ok(WorkerOutcome::Completed(run)) => {
            check(run).map_err(|reason| AssertionError::new(vec![format!("{name}: {reason}")]))
        }
    }
}

fn handler_was_invoked_exactly_once() -> impl Fn(&Result<WorkerOutcome, String>) -> AssertionResult
{
    observe("handler invocation count", |run| {
        match run.handler_invocations {
            1 => Ok(()),
            n => Err(format!("expected exactly 1 handler invocation, got {n}")),
        }
    })
}

fn in_handler_push_succeeded_exactly_once()
-> impl Fn(&Result<WorkerOutcome, String>) -> AssertionResult {
    observe("in-handler push_with_conn count", |run| {
        match run.in_handler_push_invocations {
            1 => Ok(()),
            n => Err(format!(
                "expected exactly 1 successful in-handler push, got {n}"
            )),
        }
    })
}

fn email_terminal_status_is_done() -> impl Fn(&Result<WorkerOutcome, String>) -> AssertionResult {
    observe("email terminal status", |run| match &run.email_status {
        Some(Status::Done) => Ok(()),
        Some(other) => Err(format!("expected Status::Done, got {other:?}")),
        None => Err("email row vanished after ack".into()),
    })
}

fn email_terminal_status_is_killed() -> impl Fn(&Result<WorkerOutcome, String>) -> AssertionResult {
    observe("email terminal status (err branch)", |run| {
        // `max_attempts = 1` means a single handler `Err` exhausts the retry
        // budget, so the row must reach the terminal `Killed` state and never be
        // re-fetchable. Asserting exactly `Killed` (not merely "not Done") is
        // what proves the de-flake invariant: a looser check would also pass on
        // a still-`Running` row, i.e. on the very race this fix removes.
        match &run.email_status {
            Some(Status::Killed) => Ok(()),
            Some(other) => Err(format!(
                "expected terminal Status::Killed for the exhausted err task, got {other:?}"
            )),
            None => Err("email row vanished after the failed ack".into()),
        }
    })
}

fn activity_queue_holds_exactly_one_row()
-> impl Fn(&Result<WorkerOutcome, String>) -> AssertionResult {
    observe("activity fan-out count", |run| match run.activity_count {
        1 => Ok(()),
        n => Err(format!("expected exactly 1 activity row, got {n}")),
    })
}

lets_expect! { #tokio_test
    expect(worker_outcome(handler_outcome).await) {
        let handler_outcome = HandlerOutcome::Ok;

        when the_handler_returns_ok {
            to invokes_the_handler_exactly_once {
                handler_was_invoked_exactly_once()
            }
            to commits_the_in_handler_push_with_conn_once {
                in_handler_push_succeeded_exactly_once()
            }
            to marks_the_email_task_as_done {
                email_terminal_status_is_done()
            }
            to leaves_the_follow_up_log_row_visible {
                activity_queue_holds_exactly_one_row()
            }
        }

        when the_handler_returns_err {
            let handler_outcome = HandlerOutcome::Err;

            to still_invokes_the_handler_exactly_once {
                handler_was_invoked_exactly_once()
            }
            to still_commits_the_in_handler_outbox_row {
                // `push_with_conn` runs in its own Diesel transaction that
                // commits *before* the handler returns, so the follow-up row
                // is durable independently of the handler's final result.
                activity_queue_holds_exactly_one_row()
            }
            to kills_the_email_task_after_the_exhausted_attempt {
                email_terminal_status_is_killed()
            }
        }
    }
}
