//! End-to-end coverage of `PostgresStorage` driven by the real apalis
//! `Worker` runtime. Guards `Backend: Send + Sync` and the in-handler outbox
//! semantics on each enabled runtime, including ntex with both features.

#![cfg(any(feature = "tokio", feature = "ntex"))]

mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use apalis::prelude::*;
use apalis_core::{
    backend::FetchById,
    layers::{Layer, Service},
    task::{attempt::Attempt, status::Status},
};
use apalis_diesel_postgres::{
    Config, Error as PgError, PgPool, PgTask, PostgresStorage, build_pool, setup,
};
use apalis_sql::context::SqlContext;
use diesel::{Connection, RunQueryDsl, sql_query, sql_types::Text};
use lets_expect::{AssertionError, AssertionResult, *};
use serde::{Deserialize, Serialize};

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

/// Exercise the backend middleware's fallback through the actual Worker stack.
/// Readiness and the call use the same inner service instance.
#[derive(Clone)]
struct RefreshClaimLayer(bool);

#[derive(Clone)]
struct RefreshClaimService<S> {
    inner: S,
    refresh: bool,
}

impl<S> Layer<S> for RefreshClaimLayer {
    type Service = RefreshClaimService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RefreshClaimService {
            inner,
            refresh: self.0,
        }
    }
}

impl<S, J> Service<PgTask<J>> for RefreshClaimService<S>
where
    S: Service<PgTask<J>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut task: PgTask<J>) -> Self::Future {
        if self.refresh {
            task.parts.ctx = task.parts.ctx.with_lock_by(None).with_lock_at(None);
        }
        self.inner.call(task)
    }
}

/// Aggregated observations from one `WorkerBuilder::run()` pass.
#[derive(Debug)]
struct WorkerRun {
    handler_invocations: u64,
    in_handler_push_invocations: u64,
    email_status: Option<Status>,
    email_attempts: Option<usize>,
    handler_attempt: usize,
    activity_count: i64,
    activity_payload: Option<LogActivity>,
    shutdown_completed: bool,
}

#[derive(Debug)]
enum WorkerOutcome {
    Skipped,
    Completed(WorkerRun),
}

async fn cleanup_queues(pool: PgPool, queues: Vec<String>) -> Result<(), String> {
    blocking(move || -> Result<(), String> {
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
    blocking(move || -> Result<i64, String> {
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

#[derive(diesel::QueryableByName)]
struct JobPayloadRow {
    #[diesel(sql_type = diesel::sql_types::Binary)]
    job: Vec<u8>,
}

/// Reads back the single follow-up row's stored `job` payload and decodes it
/// with the same JSON codec the storage encoded it with, so the test can assert
/// the *content* the handler wrote (not merely that a row exists).
async fn fetch_activity_payload(
    pool: PgPool,
    queue: String,
) -> Result<Option<LogActivity>, String> {
    blocking(move || -> Result<Option<LogActivity>, String> {
        let mut conn = pool.get().map_err(|e| e.to_string())?;
        let rows: Vec<JobPayloadRow> = sql_query("SELECT job FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(&queue)
            .load(&mut conn)
            .map_err(|e| e.to_string())?;
        match rows.into_iter().next() {
            None => Ok(None),
            Some(row) => {
                let decoded: LogActivity =
                    serde_json::from_slice(&row.job).map_err(|e| e.to_string())?;
                Ok(Some(decoded))
            }
        }
    })
    .await
    .map_err(|e| e.to_string())?
}

async fn run_worker_integration(
    handler_outcome: HandlerOutcome,
    refresh_claim: bool,
) -> Result<WorkerOutcome, String> {
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

    let emails: PostgresStorage<SendEmail> =
        PostgresStorage::new_with_config(&pool, &Config::new(&emails_queue));
    let activity: PostgresStorage<LogActivity> =
        PostgresStorage::new_with_config(&pool, &Config::new(&activity_queue));

    let email_task_id = {
        let storage = emails.clone();
        blocking(move || -> Result<_, PgError> {
            let mut conn = storage.pool().get().map_err(PgError::Pool)?;
            conn.transaction(|c| {
                // `max_attempts = 1` makes a single handler `Err` terminal
                // (`Killed`), so the err-branch row is never re-fetchable. This
                // keeps `handler_invocations == 1` deterministic instead of
                // relying on worker shutdown winning a race against the
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
    let handler_attempt = Arc::new(AtomicUsize::new(usize::MAX));

    let activity_for_handler = activity.clone();
    let invocations_for_handler = invocations.clone();
    let push_ok_for_handler = push_ok.clone();
    let attempt_for_handler = handler_attempt.clone();

    let handler = move |job: SendEmail, attempt: Attempt| {
        let activity = activity_for_handler.clone();
        let invocations = invocations_for_handler.clone();
        let push_ok = push_ok_for_handler.clone();
        let handler_attempt = attempt_for_handler.clone();
        async move {
            invocations.fetch_add(1, Ordering::Relaxed);
            handler_attempt.store(attempt.current(), Ordering::Relaxed);

            let activity_for_blocking = activity.clone();
            let to = job.to.clone();
            let pushed = blocking(move || -> Result<(), PgError> {
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

            match handler_outcome {
                HandlerOutcome::Ok => Ok(()),
                HandlerOutcome::Err => {
                    Err::<(), BoxDynError>("handler intentionally failed".into())
                }
            }
        }
    };

    let worker = WorkerBuilder::new(format!("worker-int-{suffix}"))
        .backend(emails.clone())
        .layer(RefreshClaimLayer(refresh_claim))
        .build(handler);
    let mut observer = emails.clone();
    let terminal_observed = Arc::new(std::sync::Mutex::new(None));
    let signal_status = terminal_observed.clone();
    let signal = async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let status = observer
                .fetch_by_id(&email_task_id)
                .await
                .map_err(|error| apalis_core::error::WorkerError::StreamError(Box::new(error)))?
                .map(|task| (task.parts.status.load(), task.parts.attempt.current()));
            if matches!(status, Some((Status::Done | Status::Killed, _))) {
                *signal_status.lock().expect("status mutex") = status;
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(apalis_core::error::WorkerError::PanicError(
                    "terminal state timeout".into(),
                ));
            }
            apalis_core::timer::sleep(Duration::from_millis(10)).await;
        }
    };
    worker
        .run_until(signal)
        .await
        .map_err(|error| error.to_string())?;
    let (email_status, email_attempts) = terminal_observed
        .lock()
        .expect("status mutex")
        .clone()
        .map_or((None, None), |(status, attempts)| {
            (Some(status), Some(attempts))
        });

    let handler_invocations = invocations.load(Ordering::Relaxed);
    let in_handler_push_invocations = push_ok.load(Ordering::Relaxed);
    let activity_count = count_jobs(pool.clone(), activity_queue.clone()).await?;
    let activity_payload = fetch_activity_payload(pool.clone(), activity_queue.clone()).await?;
    cleanup_queues(pool, vec![emails_queue, activity_queue]).await?;

    Ok(WorkerOutcome::Completed(WorkerRun {
        handler_invocations,
        in_handler_push_invocations,
        email_status,
        email_attempts,
        handler_attempt: handler_attempt.load(Ordering::Relaxed),
        activity_count,
        activity_payload,
        shutdown_completed: true,
    }))
}

async fn blocking<F, T>(work: F) -> Result<T, String>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    #[cfg(feature = "tokio")]
    if tokio::runtime::Handle::try_current().is_ok() {
        return tokio::task::spawn_blocking(work)
            .await
            .map_err(|error| error.to_string());
    }
    #[cfg(feature = "ntex")]
    return ntex_rt::spawn_blocking(work)
        .await
        .map_err(|error| error.to_string());
    #[cfg(not(feature = "ntex"))]
    unreachable!("test runtime is entered by its runner")
}

#[cfg(feature = "tokio")]
fn run_on_tokio(outcome: HandlerOutcome, refresh_claim: bool) -> Result<WorkerOutcome, String> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?
        .block_on(run_worker_integration(outcome, refresh_claim))
}

#[cfg(feature = "ntex")]
fn run_on_ntex(outcome: HandlerOutcome, refresh_claim: bool) -> Result<WorkerOutcome, String> {
    ntex::rt::System::build()
        .build(ntex::rt::DefaultRuntime)
        .block_on(async move {
            assert!(ntex_rt::System::try_current().is_some());
            assert!(
                tokio::runtime::Handle::try_current().is_err(),
                "ntex coverage must not enter Tokio"
            );
            run_worker_integration(outcome, refresh_claim).await
        })
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

fn handler_observes_the_first_attempt() -> impl Fn(&Result<WorkerOutcome, String>) -> AssertionResult
{
    observe("handler attempt", |run| match run.handler_attempt {
        1 => Ok(()),
        n => Err(format!("expected handler Attempt 1, got {n}")),
    })
}

fn acknowledgement_records_one_attempt()
-> impl Fn(&Result<WorkerOutcome, String>) -> AssertionResult {
    observe("persisted attempt", |run| match run.email_attempts {
        Some(1) => Ok(()),
        other => Err(format!(
            "expected persisted attempts Some(1), got {other:?}"
        )),
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

fn activity_row_carries_the_business_payload()
-> impl Fn(&Result<WorkerOutcome, String>) -> AssertionResult {
    // The whole point of the in-handler outbox is to durably record business
    // data *derived from the task*: the handler maps `SendEmail { to }` into
    // `LogActivity { kind: "email_sent", target: to }`. Reading the stored `job`
    // payload back and asserting both fields is what proves the payload was
    // encoded correctly end-to-end — a count-only check would stay green even if
    // `push_with_conn` wrote an empty/wrong payload or dropped the `target`.
    observe("activity payload content", |run| {
        match &run.activity_payload {
            None => Err("expected a decoded activity payload, found no follow-up row".into()),
            Some(payload) => {
                if payload.kind != "email_sent" {
                    return Err(format!(
                        "expected activity kind \"email_sent\", got {:?}",
                        payload.kind
                    ));
                }
                if payload.target != "ada@example.com" {
                    return Err(format!(
                        "expected activity target \"ada@example.com\" (derived from the task's `to`), got {:?}",
                        payload.target
                    ));
                }
                Ok(())
            }
        }
    })
}

fn shuts_down_normally() -> impl Fn(&Result<WorkerOutcome, String>) -> AssertionResult {
    observe("worker shutdown", |run| {
        if run.shutdown_completed {
            Ok(())
        } else {
            Err("shutdown did not finish".into())
        }
    })
}

#[cfg(feature = "tokio")]
mod tokio_worker {
    use super::*;
    lets_expect! {
        expect(run_on_tokio(handler_outcome, refresh_claim)) as worker_completion {
            let handler_outcome = HandlerOutcome::Ok;
            let refresh_claim = false;
            to commits_the_follow_up_and_acknowledges_before_shutdown {
                handler_was_invoked_exactly_once(),
                handler_observes_the_first_attempt(),
                acknowledgement_records_one_attempt(),
                in_handler_push_succeeded_exactly_once(),
                email_terminal_status_is_done(),
                activity_queue_holds_exactly_one_row(),
                activity_row_carries_the_business_payload(),
                shuts_down_normally()
            }
            when the_handler_returns_an_error {
                let handler_outcome = HandlerOutcome::Err;
                to preserves_the_committed_follow_up_and_kills_before_shutdown {
                    handler_was_invoked_exactly_once(),
                    handler_observes_the_first_attempt(),
                    acknowledgement_records_one_attempt(),
                    in_handler_push_succeeded_exactly_once(),
                    email_terminal_status_is_killed(),
                    activity_queue_holds_exactly_one_row(),
                    activity_row_carries_the_business_payload(),
                    shuts_down_normally()
                }
            }
            when the_claim_metadata_needs_refresh {
                let refresh_claim = true;
                to commits_the_follow_up_and_acknowledges_before_shutdown {
                    handler_was_invoked_exactly_once(),
                    handler_observes_the_first_attempt(),
                    acknowledgement_records_one_attempt(),
                    in_handler_push_succeeded_exactly_once(),
                    email_terminal_status_is_done(),
                    activity_queue_holds_exactly_one_row(),
                    activity_row_carries_the_business_payload(),
                    shuts_down_normally()
                }
                when the_handler_returns_an_error {
                    let handler_outcome = HandlerOutcome::Err;
                    to preserves_the_committed_follow_up_and_kills_before_shutdown {
                        handler_was_invoked_exactly_once(),
                        handler_observes_the_first_attempt(),
                        acknowledgement_records_one_attempt(),
                        in_handler_push_succeeded_exactly_once(),
                        email_terminal_status_is_killed(),
                        activity_queue_holds_exactly_one_row(),
                        activity_row_carries_the_business_payload(),
                        shuts_down_normally()
                    }
                }
            }
        }
    }
}
#[cfg(feature = "ntex")]
mod ntex_worker {
    use super::*;
    lets_expect! {
        expect(run_on_ntex(handler_outcome, refresh_claim)) as worker_completion {
            let handler_outcome = HandlerOutcome::Ok;
            let refresh_claim = false;
            to commits_the_follow_up_and_acknowledges_before_shutdown {
                handler_was_invoked_exactly_once(),
                handler_observes_the_first_attempt(),
                acknowledgement_records_one_attempt(),
                in_handler_push_succeeded_exactly_once(),
                email_terminal_status_is_done(),
                activity_queue_holds_exactly_one_row(),
                activity_row_carries_the_business_payload(),
                shuts_down_normally()
            }
            when the_handler_returns_an_error {
                let handler_outcome = HandlerOutcome::Err;
                to preserves_the_committed_follow_up_and_kills_before_shutdown {
                    handler_was_invoked_exactly_once(),
                    handler_observes_the_first_attempt(),
                    acknowledgement_records_one_attempt(),
                    in_handler_push_succeeded_exactly_once(),
                    email_terminal_status_is_killed(),
                    activity_queue_holds_exactly_one_row(),
                    activity_row_carries_the_business_payload(),
                    shuts_down_normally()
                }
            }
            when the_claim_metadata_needs_refresh {
                let refresh_claim = true;
                to commits_the_follow_up_and_acknowledges_before_shutdown {
                    handler_was_invoked_exactly_once(),
                    handler_observes_the_first_attempt(),
                    acknowledgement_records_one_attempt(),
                    in_handler_push_succeeded_exactly_once(),
                    email_terminal_status_is_done(),
                    activity_queue_holds_exactly_one_row(),
                    activity_row_carries_the_business_payload(),
                    shuts_down_normally()
                }
                when the_handler_returns_an_error {
                    let handler_outcome = HandlerOutcome::Err;
                    to preserves_the_committed_follow_up_and_kills_before_shutdown {
                        handler_was_invoked_exactly_once(),
                        handler_observes_the_first_attempt(),
                        acknowledgement_records_one_attempt(),
                        in_handler_push_succeeded_exactly_once(),
                        email_terminal_status_is_killed(),
                        activity_queue_holds_exactly_one_row(),
                        activity_row_carries_the_business_payload(),
                        shuts_down_normally()
                    }
                }
            }
        }
    }
}
