//! The end of a worker's life through the public API: releasing a
//! registration after the worker stops, restarting the same name at once,
//! refusing a heartbeat schedule that cannot keep a registration fresh, and
//! removing completed history.

#![cfg(feature = "tokio")]

mod support;

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use apalis::prelude::*;
use apalis_core::{
    backend::{Backend, BackendExt, FetchById, TaskSink, Vacuum},
    task::status::Status,
    worker::context::WorkerContext,
};
use apalis_diesel_postgres::{
    CompactType, Config, Error, PgPool, PgTask, PgTaskId, PostgresStorage, ReleasedRun, build_pool,
    setup,
};
use diesel::{QueryableByName, RunQueryDsl, sql_query, sql_types::Text};
use futures::StreamExt;
use lets_expect::{AssertionResult, *};
use support::{Outcome, observe};

async fn pool() -> Result<Option<PgPool>, String> {
    let Some(url) = support::database_url_or_skip()? else {
        return Ok(None);
    };
    let pool = build_pool(url).map_err(|e| e.to_string())?;
    setup(&pool).await.map_err(|e| e.to_string())?;
    Ok(Some(pool))
}

async fn cleanup(pool: PgPool, queue: String) -> Result<(), String> {
    support::with_conn(pool, move |conn| {
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

fn config(queue: &str) -> Config {
    Config::new(queue)
        .set_keep_alive(Duration::from_millis(50))
        .set_reenqueue_orphaned_after(Duration::from_secs(60))
}

fn item(next: Option<Result<Option<PgTask<CompactType>>, Error>>) -> &'static str {
    match next {
        Some(Ok(None)) => "registered",
        Some(Ok(Some(_))) => "task",
        Some(Err(Error::AlreadyRegistered { .. })) => "already_registered",
        Some(Err(Error::WorkerRetired { .. })) => "retired",
        Some(Err(Error::InvalidArgument(_))) => "invalid_argument",
        Some(Err(_)) => "other_error",
        None => "ended",
    }
}

async fn next_item<S>(stream: &mut S) -> &'static str
where
    S: futures::Stream<Item = Result<Option<PgTask<CompactType>>, Error>> + Unpin,
{
    item(
        tokio::time::timeout(Duration::from_secs(5), stream.next())
            .await
            .expect("a stream item within five seconds"),
    )
}

#[derive(QueryableByName)]
struct JobRow {
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    attempts: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<Text>)]
    lock_by: Option<String>,
}

/// Enqueue `args` under a caller-chosen id so the scenario can observe the row.
async fn push_with_id(
    producer: &mut PostgresStorage<String>,
    args: &str,
    run_at: Option<u64>,
) -> Result<PgTaskId, String> {
    let id = PgTaskId::new(ulid::Ulid::new());
    let mut task = PgTask::<String>::new(args.to_owned());
    task.parts.task_id = Some(id);
    if let Some(run_at) = run_at {
        task.parts.run_at = run_at;
    }
    producer.push_task(task).await.map_err(|e| e.to_string())?;
    Ok(id)
}

async fn job_row(pool: PgPool, id: PgTaskId) -> Result<(String, i32, Option<String>), String> {
    support::with_conn(pool, move |conn| {
        sql_query("SELECT status, attempts, lock_by FROM apalis.jobs WHERE id = $1")
            .bind::<Text, _>(id.to_string())
            .get_result::<JobRow>(conn)
            .map(|row| (row.status, row.attempts, row.lock_by))
            .map_err(|e| e.to_string())
    })
    .await
}

// --------------------------------------------------------------------------
// A registration released with an unfinished claim
// --------------------------------------------------------------------------

#[derive(Debug)]
struct ReleaseObservation {
    claimed: &'static str,
    released: Result<usize, &'static str>,
    row_after_release: (String, i32, Option<String>),
    clone_after_release: &'static str,
    successor: &'static str,
    successor_task: &'static str,
    released_again: Result<usize, &'static str>,
}

fn release_error(error: Error) -> &'static str {
    match error {
        Error::WorkerNotRegistered { .. } => "not_registered",
        _ => "other_error",
    }
}

async fn release_with_unfinished_claim() -> Result<Outcome<ReleaseObservation>, String> {
    let Some(pool) = pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("worker-release-claim-{}", ulid::Ulid::new());
    cleanup(pool.clone(), queue.clone()).await?;
    let observation = async {
        let worker = WorkerContext::new::<()>("rolling-worker");
        let storage = PostgresStorage::<String>::new_with_config(&pool, &config(&queue));
        let mut producer = storage.clone();
        let task_id = push_with_id(&mut producer, "unfinished", None).await?;
        let mut stream = storage.clone().poll_compact(&worker);
        assert_eq!(next_item(&mut stream).await, "registered");
        let claimed = next_item(&mut stream).await;
        // The process stops without acknowledging: the stream goes away with
        // the task still owned by the name.
        drop(stream);
        let released = storage
            .release_worker(worker.name())
            .await
            .map_err(release_error);
        let row_after_release = job_row(pool.clone(), task_id).await?;
        // The storage that released is retired locally, for every clone.
        let mut clone = storage.clone().poll_compact(&worker);
        let clone_after_release = next_item(&mut clone).await;
        drop(clone);
        // A fresh storage takes the name over at once and receives the task.
        let fresh = PostgresStorage::<String>::new_with_config(&pool, &config(&queue));
        let mut successor_stream = fresh.clone().poll_compact(&worker);
        let successor = next_item(&mut successor_stream).await;
        let successor_task = next_item(&mut successor_stream).await;
        drop(successor_stream);
        let released_again = storage
            .release_worker(worker.name())
            .await
            .map_err(release_error);
        Ok::<_, String>(ReleaseObservation {
            claimed,
            released,
            row_after_release,
            clone_after_release,
            successor,
            successor_task,
            released_again,
        })
    }
    .await;
    cleanup(pool, queue).await?;
    observation.map(Outcome::Completed)
}

fn hands_the_claim_to_an_immediate_successor()
-> impl Fn(&Result<Outcome<ReleaseObservation>, String>) -> AssertionResult {
    observe::<ReleaseObservation, _>("release with an unfinished claim", |o| {
        let expected_row = ("Pending".to_owned(), 1, None);
        if o.claimed == "task"
            && o.released == Ok(1)
            && o.row_after_release == expected_row
            && o.clone_after_release == "retired"
            && o.successor == "registered"
            && o.successor_task == "task"
            && o.released_again == Err("not_registered")
        {
            Ok(())
        } else {
            Err(format!(
                "expected the claim handed back as Pending with one attempt consumed, the releasing storage retired, an immediate successor that receives the task, and a second release refused; got {o:?}"
            ))
        }
    })
}

// --------------------------------------------------------------------------
// Restarting a real worker under the same name
// --------------------------------------------------------------------------

#[derive(Debug)]
struct RestartObservation {
    first_run: Result<(), String>,
    released: Result<usize, &'static str>,
    second_run: Result<(), String>,
    handled: usize,
    statuses: Vec<Status>,
}

/// Run one `WorkerBuilder` worker until the given task is terminal.
async fn run_until_done(
    storage: PostgresStorage<String>,
    name: &str,
    task_id: PgTaskId,
    handled: Arc<AtomicUsize>,
) -> Result<(), String> {
    let handler = move |_job: String| {
        let handled = handled.clone();
        async move {
            handled.fetch_add(1, Ordering::Relaxed);
            Ok::<(), BoxDynError>(())
        }
    };
    let worker = WorkerBuilder::new(name)
        .backend(storage.clone())
        .build(handler);
    let mut observer = storage;
    let signal = async move {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let status = observer
                .fetch_by_id(&task_id)
                .await
                .map_err(|error| WorkerError::StreamError(Box::new(error)))?
                .map(|task| task.parts.status.load());
            if matches!(status, Some(Status::Done | Status::Killed)) {
                return Ok(());
            }
            if std::time::Instant::now() >= deadline {
                return Err(WorkerError::PanicError("terminal state timeout".into()));
            }
            apalis_core::timer::sleep(Duration::from_millis(10)).await;
        }
    };
    worker.run_until(signal).await.map_err(|e| e.to_string())
}

#[derive(Clone, Copy)]
enum Handover {
    /// The first deployment released explicitly after its run returned.
    Released,
    /// The first deployment ran through `run_released`.
    RunReleased,
    /// The first deployment stopped without releasing.
    Kept,
}

async fn restart_under_the_same_name(
    handover: Handover,
) -> Result<Outcome<RestartObservation>, String> {
    let Some(pool) = pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("worker-release-restart-{}", ulid::Ulid::new());
    cleanup(pool.clone(), queue.clone()).await?;
    let observation = async {
        let name = "deployed-worker";
        let handled = Arc::new(AtomicUsize::new(0));
        let first = PostgresStorage::<String>::new_with_config(&pool, &config(&queue));
        let mut producer = first.clone();
        let first_task = push_with_id(&mut producer, "first deploy", None).await?;
        let (first_run, released) = match handover {
            Handover::Released => {
                let run = run_until_done(first.clone(), name, first_task, handled.clone()).await;
                let released = first.release_worker(name).await.map_err(release_error);
                (run, released)
            }
            Handover::RunReleased => {
                let ReleasedRun { outcome, released } = first
                    .run_released(
                        name,
                        run_until_done(first.clone(), name, first_task, handled.clone()),
                    )
                    .await;
                (outcome, released.map_err(release_error))
            }
            Handover::Kept => (
                run_until_done(first.clone(), name, first_task, handled.clone()).await,
                Ok(0),
            ),
        };
        let second = PostgresStorage::<String>::new_with_config(&pool, &config(&queue));
        let second_task = push_with_id(&mut producer, "second deploy", None).await?;
        let second_run = run_until_done(second, name, second_task, handled.clone()).await;
        let mut statuses = Vec::new();
        for id in [first_task, second_task] {
            statuses.push(
                producer
                    .fetch_by_id(&id)
                    .await
                    .map_err(|e| e.to_string())?
                    .map(|task| task.parts.status.load())
                    .ok_or("the task row exists")?,
            );
        }
        Ok::<_, String>(RestartObservation {
            first_run,
            released,
            second_run,
            handled: handled.load(Ordering::Relaxed),
            statuses,
        })
    }
    .await;
    cleanup(pool, queue).await?;
    observation.map(Outcome::Completed)
}

fn restarts_immediately() -> impl Fn(&Result<Outcome<RestartObservation>, String>) -> AssertionResult
{
    observe::<RestartObservation, _>("restart after release", |o| {
        if o.first_run.is_ok()
            && o.released == Ok(0)
            && o.second_run.is_ok()
            && o.handled == 2
            && o.statuses == [Status::Done, Status::Done]
        {
            Ok(())
        } else {
            Err(format!(
                "expected both deployments to run their task under the same name with nothing left to recover; got {o:?}"
            ))
        }
    })
}

fn is_refused_until_stale()
-> impl Fn(&Result<Outcome<RestartObservation>, String>) -> AssertionResult {
    observe::<RestartObservation, _>("restart without release", |o| {
        let refused = o
            .second_run
            .as_ref()
            .is_err_and(|error| error.contains("already registered"));
        if o.first_run.is_ok()
            && refused
            && o.handled == 1
            && o.statuses == [Status::Done, Status::Pending]
        {
            Ok(())
        } else {
            Err(format!(
                "expected the second deployment to be refused while the first registration is fresh; got {o:?}"
            ))
        }
    })
}

// --------------------------------------------------------------------------
// A heartbeat schedule that cannot keep the registration fresh
// --------------------------------------------------------------------------

#[derive(Debug)]
struct LivenessObservation {
    poll: &'static str,
    heartbeat: &'static str,
    registered_rows: i64,
}

#[derive(QueryableByName)]
struct Count {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

async fn misconfigured_liveness() -> Result<Outcome<LivenessObservation>, String> {
    let Some(pool) = pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("worker-release-liveness-{}", ulid::Ulid::new());
    cleanup(pool.clone(), queue.clone()).await?;
    let observation = async {
        let config = Config::new(&queue)
            .set_keep_alive(Duration::from_secs(2))
            .set_reenqueue_orphaned_after(Duration::from_secs(1));
        let worker = WorkerContext::new::<()>("misconfigured-worker");
        let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
        let mut poll = storage.clone().poll_compact(&worker);
        let poll = next_item(&mut poll).await;
        let mut heartbeat = storage.heartbeat(&worker);
        let heartbeat = match tokio::time::timeout(Duration::from_secs(5), heartbeat.next())
            .await
            .expect("a heartbeat item within five seconds")
        {
            Some(Err(Error::InvalidArgument(_))) => "invalid_argument",
            Some(Err(_)) => "other_error",
            Some(Ok(())) => "beat",
            None => "ended",
        };
        let count_queue = queue.clone();
        let registered_rows = support::with_conn(pool.clone(), move |conn| {
            sql_query("SELECT count(*)::bigint AS n FROM apalis.workers WHERE worker_type = $1")
                .bind::<Text, _>(&count_queue)
                .get_result::<Count>(conn)
                .map(|row| row.n)
                .map_err(|e| e.to_string())
        })
        .await?;
        Ok::<_, String>(LivenessObservation {
            poll,
            heartbeat,
            registered_rows,
        })
    }
    .await;
    cleanup(pool, queue).await?;
    observation.map(Outcome::Completed)
}

fn is_refused_before_registering()
-> impl Fn(&Result<Outcome<LivenessObservation>, String>) -> AssertionResult {
    observe::<LivenessObservation, _>("misconfigured liveness", |o| {
        if o.poll == "invalid_argument"
            && o.heartbeat == "invalid_argument"
            && o.registered_rows == 0
        {
            Ok(())
        } else {
            Err(format!(
                "expected both streams to refuse the schedule before any registration; got {o:?}"
            ))
        }
    })
}

// --------------------------------------------------------------------------
// A heartbeat that starts before the registration
// --------------------------------------------------------------------------

#[derive(Debug)]
struct HeartbeatOrderObservation {
    before_registration: &'static str,
    registration: &'static str,
    after_registration: &'static str,
    after_retirement: &'static str,
}

async fn heartbeat_item<S>(heartbeat: &mut S, wait: Duration) -> &'static str
where
    S: futures::Stream<Item = Result<(), Error>> + Unpin,
{
    match tokio::time::timeout(wait, heartbeat.next()).await {
        Err(_) => "pending",
        Ok(Some(Ok(()))) => "beat",
        Ok(Some(Err(Error::WorkerNotRegistered { .. }))) => "not_registered",
        Ok(Some(Err(Error::WorkerRetired { .. }))) => "retired",
        Ok(Some(Err(_))) => "other_error",
        Ok(None) => "ended",
    }
}

/// The heartbeat stream is created and polled before the task stream has
/// registered the name, with a heartbeat interval far shorter than any
/// registration round trip. It must wait for the registration instead of
/// renewing a row that does not exist yet and ending the worker.
async fn heartbeat_before_registration() -> Result<Outcome<HeartbeatOrderObservation>, String> {
    let Some(pool) = pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("worker-release-heartbeat-order-{}", ulid::Ulid::new());
    cleanup(pool.clone(), queue.clone()).await?;
    let observation = async {
        let config = Config::new(&queue)
            .set_keep_alive(Duration::from_millis(10))
            .set_reenqueue_orphaned_after(Duration::from_secs(60));
        let worker = WorkerContext::new::<()>("early-heartbeat-worker");
        let storage = PostgresStorage::<String>::new_with_config(&pool, &config);
        let mut heartbeat = storage.heartbeat(&worker);
        // Many heartbeat intervals pass while nothing is registered.
        let before_registration = heartbeat_item(&mut heartbeat, Duration::from_millis(300)).await;
        let mut tasks = storage.clone().poll_compact(&worker);
        let registration = next_item(&mut tasks).await;
        let after_registration = heartbeat_item(&mut heartbeat, Duration::from_secs(5)).await;
        // Dropping the registered task stream retires the name.
        drop(tasks);
        let after_retirement = heartbeat_item(&mut heartbeat, Duration::from_secs(5)).await;
        Ok::<_, String>(HeartbeatOrderObservation {
            before_registration,
            registration,
            after_registration,
            after_retirement,
        })
    }
    .await;
    cleanup(pool, queue).await?;
    observation.map(Outcome::Completed)
}

fn waits_for_the_registration()
-> impl Fn(&Result<Outcome<HeartbeatOrderObservation>, String>) -> AssertionResult {
    observe::<HeartbeatOrderObservation, _>("heartbeat before registration", |o| {
        if o.before_registration == "pending"
            && o.registration == "registered"
            && o.after_registration == "beat"
            && o.after_retirement == "retired"
        {
            Ok(())
        } else {
            Err(format!(
                "expected the heartbeat to wait for the registration, renew after it, and stop after retirement; got {o:?}"
            ))
        }
    })
}

// --------------------------------------------------------------------------
// Removing completed history
// --------------------------------------------------------------------------

#[derive(Debug)]
struct RetentionObservation {
    kept_by_window: usize,
    vacuumed: usize,
    remaining: Vec<Option<Status>>,
    pruned_workers: usize,
    workers_left: i64,
}

async fn retention_after_a_run() -> Result<Outcome<RetentionObservation>, String> {
    let Some(pool) = pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("worker-release-retention-{}", ulid::Ulid::new());
    cleanup(pool.clone(), queue.clone()).await?;
    let observation = async {
        let handled = Arc::new(AtomicUsize::new(0));
        let storage = PostgresStorage::<String>::new_with_config(&pool, &config(&queue));
        let mut producer = storage.clone();
        let done = push_with_id(&mut producer, "to complete", None).await?;
        run_until_done(storage.clone(), "retention-worker", done, handled.clone()).await?;
        storage
            .release_worker("retention-worker")
            .await
            .map_err(|e| e.to_string())?;
        // A task scheduled far ahead stays active and must survive both steps.
        let pending = push_with_id(&mut producer, "still pending", Some(4_000_000_000)).await?;
        let kept_by_window = storage
            .purge_terminal_tasks(Duration::from_secs(3_600))
            .await
            .map_err(|e| e.to_string())?;
        let vacuumed = producer.vacuum().await.map_err(|e| e.to_string())?;
        let mut remaining = Vec::new();
        for id in [done, pending] {
            remaining.push(
                producer
                    .fetch_by_id(&id)
                    .await
                    .map_err(|e| e.to_string())?
                    .map(|task| task.parts.status.load()),
            );
        }
        // The released registration is stale and, once its task is gone,
        // unreferenced.
        let pruned_workers = storage
            .prune_workers(Duration::from_secs(60))
            .await
            .map_err(|e| e.to_string())?;
        let count_queue = queue.clone();
        let workers_left = support::with_conn(pool.clone(), move |conn| {
            sql_query("SELECT count(*)::bigint AS n FROM apalis.workers WHERE worker_type = $1")
                .bind::<Text, _>(&count_queue)
                .get_result::<Count>(conn)
                .map(|row| row.n)
                .map_err(|e| e.to_string())
        })
        .await?;
        Ok::<_, String>(RetentionObservation {
            kept_by_window,
            vacuumed,
            remaining,
            pruned_workers,
            workers_left,
        })
    }
    .await;
    cleanup(pool, queue).await?;
    observation.map(Outcome::Completed)
}

fn removes_only_completed_history()
-> impl Fn(&Result<Outcome<RetentionObservation>, String>) -> AssertionResult {
    observe::<RetentionObservation, _>("retention", |o| {
        if o.kept_by_window == 0
            && o.vacuumed == 1
            && o.remaining == [None, Some(Status::Pending)]
            && o.pruned_workers == 1
            && o.workers_left == 0
        {
            Ok(())
        } else {
            Err(format!(
                "expected the window to keep the fresh result, vacuum to remove exactly it, the scheduled task to survive, and the released registration to be pruned afterwards; got {o:?}"
            ))
        }
    })
}

lets_expect! { #tokio_test
    expect(release_with_unfinished_claim().await) as a_released_registration {
        when the_worker_stopped_with_an_unfinished_claim {
            to hands_the_claim_to_an_immediate_successor { hands_the_claim_to_an_immediate_successor() }
        }
    }

    expect(restart_under_the_same_name(handover).await) as a_worker_redeployed_under_its_name {
        let handover = Handover::Released;
        when the_previous_deployment_released_its_registration {
            to restarts_immediately { restarts_immediately() }
        }
        when the_previous_deployment_ran_through_run_released {
            let handover = Handover::RunReleased;
            to restarts_immediately { restarts_immediately() }
        }
        when the_previous_deployment_did_not_release_its_registration {
            let handover = Handover::Kept;
            to is_refused_until_the_registration_is_stale { is_refused_until_stale() }
        }
    }

    expect(heartbeat_before_registration().await) as a_heartbeat_started_before_the_registration {
        to waits_for_the_registration { waits_for_the_registration() }
    }

    expect(misconfigured_liveness().await) as a_heartbeat_slower_than_the_stale_deadline {
        to is_refused_before_registering { is_refused_before_registering() }
    }

    expect(retention_after_a_run().await) as completed_history {
        to is_removed_only_by_explicit_retention { removes_only_completed_history() }
    }
}
