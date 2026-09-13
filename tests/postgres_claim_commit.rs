//! Lost COMMIT responses must not leave claims under a live worker heartbeat.
#![cfg(feature = "tokio")]
#[path = "support/commit_proxy.rs"]
mod commit_proxy;
mod support;
use apalis_core::{
    backend::{
        Backend, BackendExt, FetchById, TaskSink, TaskStream,
        poll_strategy::{IntervalStrategy, StreamStrategy},
        shared::MakeShared,
    },
    layers::{Layer, Service},
    task::{attempt::Attempt, builder::TaskBuilder},
    worker::context::WorkerContext,
};
use apalis_diesel_postgres::{
    CompactType, Config, Error, JsonCodec, PgContext, PgMiddleware, PgPool, PgTask, PgTaskId,
    PostgresStorage, SharedPostgresStorage, build_pool_with, setup,
};
use commit_proxy::CommitProxy;
use diesel::{
    Connection, RunQueryDsl, sql_query,
    sql_types::{Integer, Jsonb, Text},
};
use futures::{
    StreamExt,
    stream::{self, BoxStream},
};
use lets_expect::*;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
#[derive(Clone, Copy)]
enum Path {
    Polling,
    Notify,
    Shared,
    Fallback,
}
#[derive(Clone, Copy)]
enum Fault {
    LostResponse,
    InstrumentationPanic,
    EmptyClaim,
    RejectedQuery,
}
#[derive(Clone)]
struct Handler(Arc<AtomicUsize>);
impl Service<PgTask<String>> for Handler {
    type Response = ();
    type Error = std::convert::Infallible;
    type Future = std::future::Ready<Result<(), Self::Error>>;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, _: PgTask<String>) -> Self::Future {
        self.0.fetch_add(1, Ordering::SeqCst);
        std::future::ready(Ok(()))
    }
}
#[derive(Debug)]
struct PanicCommit(Arc<AtomicBool>);
impl diesel::r2d2::CustomizeConnection<diesel::PgConnection, diesel::r2d2::Error> for PanicCommit {
    fn on_acquire(&self, conn: &mut diesel::PgConnection) -> Result<(), diesel::r2d2::Error> {
        let armed = self.0.clone();
        conn.set_instrumentation(move |event: diesel::connection::InstrumentationEvent<'_>| {
            if let diesel::connection::InstrumentationEvent::FinishQuery {
                query, error: None, ..
            } = event
                && format!("{query}") == "COMMIT"
                && armed.swap(false, Ordering::SeqCst)
            {
                panic!("controlled instrumentation panic after COMMIT");
            }
        });
        Ok(())
    }
}
struct Streams {
    tasks: TaskStream<PgTask<CompactType>, Error>,
    heartbeat: BoxStream<'static, Result<(), Error>>,
    middleware: PgMiddleware,
}
fn streams(pool: &PgPool, config: &Config, worker: &WorkerContext, path: Path) -> Streams {
    macro_rules! prepare {
        ($storage:expr) => {{
            let storage = $storage;
            let middleware = storage.middleware();
            let heartbeat = storage.heartbeat(worker);
            let tasks = storage.poll_compact(worker);
            Streams {
                tasks,
                heartbeat,
                middleware,
            }
        }};
    }
    match path {
        Path::Polling => prepare!(
            PostgresStorage::<String>::new_with_config(pool, config)
                .with_poll_strategy_factory(|| IntervalStrategy::new(Duration::from_millis(5)))
        ),
        Path::Fallback => prepare!(
            PostgresStorage::<String>::new_with_config(pool, config)
                .with_poll_strategy_factory(|| StreamStrategy::new(stream::pending::<()>()))
        ),
        Path::Notify => prepare!(
            PostgresStorage::<String>::new_with_notify(pool, config)
                .with_poll_strategy_factory(|| StreamStrategy::new(stream::pending::<()>()))
        ),
        Path::Shared => {
            let mut shared: SharedPostgresStorage<JsonCodec<CompactType>> =
                SharedPostgresStorage::new(pool);
            let storage=<SharedPostgresStorage<JsonCodec<CompactType>> as MakeShared<String>>::make_shared_with_config(&mut shared,config.clone()).unwrap();
            prepare!(
                storage.with_poll_strategy_factory(|| StreamStrategy::new(stream::pending::<()>()))
            )
        }
    }
}
#[derive(Debug, Default)]
struct Observation {
    unknown: bool,
    source_preserved: bool,
    durable: bool,
    heartbeat_stopped: bool,
    claim_stopped: bool,
    recovered: bool,
    effects: usize,
    ordinary_error: bool,
    worker_continues: bool,
}
#[derive(diesel::QueryableByName)]
struct Stored {
    #[diesel(sql_type=Text)]
    status: String,
    #[diesel(sql_type=Integer)]
    attempts: i32,
}
fn unknown(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut error = Some(error);
    while let Some(current) = error {
        if matches!(
            current.downcast_ref::<Error>(),
            Some(Error::ClaimOutcomeUnknown { .. })
        ) {
            return true;
        }
        error = current.source();
    }
    false
}
fn has_database_cause(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut error = Some(error);
    while let Some(current) = error {
        if matches!(
            current.downcast_ref::<Error>(),
            Some(Error::Database { .. })
        ) {
            return true;
        }
        error = current.source();
    }
    false
}
fn preserved_source(error: &(dyn std::error::Error + 'static), panic_fault: bool) -> bool {
    let mut error = Some(error);
    while let Some(current) = error {
        if let Some(Error::ClaimOutcomeUnknown { source, .. }) = current.downcast_ref::<Error>() {
            return if panic_fault {
                matches!(source.as_ref(), Error::Blocking(_))
            } else {
                matches!(
                    source.as_ref(),
                    Error::Database {
                        source: diesel::result::Error::DatabaseError(
                            diesel::result::DatabaseErrorKind::ClosedConnection,
                            _
                        ),
                        ..
                    }
                )
            };
        }
        error = current.source();
    }
    false
}
async fn scenario(path: Path, fault: Fault, automatic: bool, url: String) -> Observation {
    let direct = build_pool_with(&url, |b| b.max_size(2).min_idle(Some(0))).unwrap();
    setup(&direct).await.unwrap();
    let proxy = CommitProxy::new(&url).unwrap();
    let panic_arm = Arc::new(AtomicBool::new(false));
    let pool = if matches!(fault, Fault::InstrumentationPanic) {
        build_pool_with(proxy.url(), |b| {
            b.max_size(3)
                .min_idle(Some(0))
                .connection_customizer(Box::new(PanicCommit(panic_arm.clone())))
        })
        .unwrap()
    } else {
        build_pool_with(proxy.url(), |b| b.max_size(3).min_idle(Some(0))).unwrap()
    };
    let queue = format!("claim_commit_{}", ulid::Ulid::new());
    let trigger = format!(
        "claim_reject_{}",
        ulid::Ulid::new().to_string().to_lowercase()
    );
    let config = Config::new(&queue)
        .set_ack(automatic)
        .set_keep_alive(Duration::from_millis(10))
        .set_reenqueue_orphaned_after(Duration::from_secs(30));
    let worker = WorkerContext::new::<()>("commit-worker");
    let Streams {
        mut tasks,
        mut heartbeat,
        middleware,
    } = streams(&pool, &config, &worker, path);
    assert!(tasks.next().await.unwrap().is_ok());
    if matches!(path, Path::Notify | Path::Shared) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while !proxy.listening() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("listener did not reach CommandComplete LISTEN");
    }
    let mut producer = PostgresStorage::<String>::new_with_config(&direct, &config);
    let id = PgTaskId::new(ulid::Ulid::new());
    let mut task = TaskBuilder::new("work".to_owned())
        .with_task_id(id)
        .with_attempt(Attempt::new_with_value(1))
        .with_ctx(PgContext::new().with_queue(queue.clone()))
        .build();
    producer.push_task(task.clone()).await.unwrap();
    let mut conn = direct.get().unwrap();
    #[derive(diesel::QueryableByName)]
    struct Snapshot {
        #[diesel(sql_type = Jsonb)]
        value: serde_json::Value,
    }
    let snapshot = |conn: &mut diesel::PgConnection| {
        sql_query("SELECT to_jsonb(jobs) AS value FROM apalis.jobs WHERE id=$1")
            .bind::<Text, _>(id.to_string())
            .get_result::<Snapshot>(conn)
            .unwrap()
            .value
    };
    let scheduled_before = if matches!(fault, Fault::EmptyClaim) {
        sql_query(
            "UPDATE apalis.jobs SET run_at=statement_timestamp()+INTERVAL '1 day' WHERE id=$1",
        )
        .bind::<Text, _>(id.to_string())
        .execute(&mut conn)
        .unwrap();
        Some(snapshot(&mut conn))
    } else {
        None
    };
    if matches!(fault, Fault::RejectedQuery) {
        sql_query(format!("CREATE FUNCTION apalis.{trigger}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.job_type='{queue}' AND NEW.status IN ('Queued','Running') THEN RAISE EXCEPTION 'controlled claim rollback'; END IF; RETURN NEW; END $$")).execute(&mut conn).unwrap();
        sql_query(format!("CREATE TRIGGER {trigger} BEFORE UPDATE ON apalis.jobs FOR EACH ROW EXECUTE FUNCTION apalis.{trigger}()" )).execute(&mut conn).unwrap();
    }
    let effects = Arc::new(AtomicUsize::new(0));
    match fault {
        Fault::LostResponse | Fault::EmptyClaim => proxy.arm(),
        Fault::InstrumentationPanic => panic_arm.store(true, Ordering::SeqCst),
        Fault::RejectedQuery => {}
    }
    let error: Box<dyn std::error::Error + Send + Sync> = if matches!(path, Path::Fallback) {
        task.parts.data.insert(worker.clone());
        let mut service = middleware.clone().layer(Handler(effects.clone()));
        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        service.call(task.clone()).await.unwrap_err()
    } else {
        Box::new(
            tokio::time::timeout(Duration::from_secs(3), tasks.next())
                .await
                .unwrap()
                .unwrap()
                .unwrap_err(),
        )
    };
    let mut observation = Observation {
        unknown: unknown(error.as_ref()),
        source_preserved: preserved_source(
            error.as_ref(),
            matches!(fault, Fault::InstrumentationPanic),
        ),
        ..Observation::default()
    };
    if matches!(fault, Fault::LostResponse | Fault::EmptyClaim) {
        assert!(
            proxy.committed(),
            "proxy never observed ReadyForQuery Idle after the intercepted COMMIT"
        );
    }
    if matches!(fault, Fault::EmptyClaim | Fault::RejectedQuery) {
        observation.ordinary_error = !observation.unknown && has_database_cause(error.as_ref());
        if matches!(fault, Fault::RejectedQuery) {
            let row: Stored = sql_query("SELECT status,attempts FROM apalis.jobs WHERE id=$1")
                .bind::<Text, _>(id.to_string())
                .get_result(&mut conn)
                .unwrap();
            observation.durable = row.status == "Pending" && row.attempts == 0;
            sql_query(format!("DROP TRIGGER {trigger} ON apalis.jobs"))
                .execute(&mut conn)
                .unwrap();
            sql_query(format!("DROP FUNCTION apalis.{trigger}()"))
                .execute(&mut conn)
                .unwrap();
        } else {
            observation.durable = Some(snapshot(&mut conn)) == scheduled_before;
            sql_query("UPDATE apalis.jobs SET run_at=statement_timestamp()-INTERVAL '1 second' WHERE id=$1")
                .bind::<Text, _>(id.to_string()).execute(&mut conn).unwrap();
        }
        let mut alive = true;
        for _ in 0..3 {
            alive &= matches!(heartbeat.next().await, Some(Ok(())));
        }
        if matches!(path, Path::Fallback) {
            task.parts.data.insert(worker.clone());
            let mut service = middleware.layer(Handler(effects.clone()));
            futures::future::poll_fn(|cx| service.poll_ready(cx))
                .await
                .unwrap();
            observation.worker_continues =
                alive && service.call(task).await.is_ok() && effects.load(Ordering::SeqCst) == 1;
        } else {
            let delivered = tokio::time::timeout(Duration::from_secs(3), tasks.next())
                .await
                .unwrap();
            observation.worker_continues =
                alive && matches!(delivered,Some(Ok(Some(task))) if task.parts.task_id==Some(id));
        }
    } else {
        let row: Stored = sql_query("SELECT status,attempts FROM apalis.jobs WHERE id=$1")
            .bind::<Text, _>(id.to_string())
            .get_result(&mut conn)
            .unwrap();
        // A fetcher claims `Queued`; the middleware's `lock_task` claims and
        // starts in one statement.
        let claimed_status = if matches!(path, Path::Fallback) {
            "Running"
        } else {
            "Queued"
        };
        observation.durable = row.status == claimed_status && row.attempts == 0;
        observation.heartbeat_stopped = matches!(
            heartbeat.next().await,
            Some(Err(Error::WorkerRetired { .. }))
        );
        observation.claim_stopped = matches!(
            tokio::time::timeout(Duration::from_secs(2), tasks.next()).await,
            Ok(Some(Err(Error::WorkerRetired { .. })))
        );
        // Establish the stale-worker precondition directly; no timing-dependent sleep.
        sql_query("UPDATE apalis.workers SET last_seen=TIMESTAMPTZ 'epoch' WHERE worker_type=$1")
            .bind::<Text, _>(&queue)
            .execute(&mut conn)
            .unwrap();
        let mut replacement =
            PostgresStorage::<String>::new_with_config(&direct, &Config::new(&queue))
                .with_poll_strategy_factory(|| IntervalStrategy::new(Duration::from_millis(5)));
        let mut next = replacement.clone().poll(&worker);
        assert!(next.next().await.unwrap().is_ok());
        let mut recovered = tokio::time::timeout(Duration::from_secs(3), next.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .unwrap();
        // Recovery charges an attempt only for a claim that was started.
        let charged = usize::from(matches!(path, Path::Fallback));
        observation.recovered =
            recovered.parts.task_id == Some(id) && recovered.parts.attempt.current() == charged;
        recovered.parts.attempt = Attempt::new_with_value(2);
        recovered.parts.data.insert(worker.clone());
        let mut service = replacement.middleware().layer(Handler(effects.clone()));
        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        service.call(recovered).await.unwrap();
        let done = replacement.fetch_by_id(&id).await.unwrap().unwrap();
        observation.recovered &= done.parts.status.load().to_string() == "Done"
            && done.parts.attempt.current() == charged + 1;
        drop(next);
        observation.effects = effects.load(Ordering::SeqCst);
    }
    drop(tasks);
    drop(heartbeat);
    drop(pool);
    sql_query("DELETE FROM apalis.jobs WHERE job_type=$1")
        .bind::<Text, _>(&queue)
        .execute(&mut conn)
        .unwrap();
    sql_query("DELETE FROM apalis.workers WHERE worker_type=$1")
        .bind::<Text, _>(&queue)
        .execute(&mut conn)
        .unwrap();
    observation
}
async fn run(
    path: Path,
    fault: Fault,
    automatic: bool,
) -> Result<support::Outcome<Observation>, String> {
    let Some(url) = support::database_url_or_skip()? else {
        return Ok(support::Outcome::Skipped);
    };
    Ok(support::Outcome::Completed(
        scenario(path, fault, automatic, url).await,
    ))
}
fn recovers_unknown_claim()
-> impl Fn(&Result<support::Outcome<Observation>, String>) -> AssertionResult {
    support::observe("unconfirmed claim completion", |r: &Observation| {
        if r.unknown
            && r.source_preserved
            && r.durable
            && r.heartbeat_stopped
            && r.claim_stopped
            && r.recovered
            && r.effects == 1
        {
            Ok(())
        } else {
            Err(format!("unexpected claim outcome: {r:?}"))
        }
    })
}
fn remains_available() -> impl Fn(&Result<support::Outcome<Observation>, String>) -> AssertionResult
{
    support::observe("no lost claim", |r: &Observation| {
        if r.ordinary_error && r.durable && r.worker_continues {
            Ok(())
        } else {
            Err(format!("unexpected no-claim outcome: {r:?}"))
        }
    })
}
lets_expect! {#tokio_test
    expect(run(path,fault,automatic).await) as unconfirmed_claim_completion {
        let path=Path::Polling;let fault=Fault::LostResponse;let automatic=true;
        to retires_the_worker_and_recovers_the_committed_task {recovers_unknown_claim()}
        when instrumentation_panics_after_commit {let fault=Fault::InstrumentationPanic;
            to preserves_the_panic_cause_and_recovers_the_committed_task {recovers_unknown_claim()}
        }
        when notifications_deliver_the_claim {let path=Path::Notify;
            to retires_the_worker_and_recovers_the_committed_task {recovers_unknown_claim()}
            when instrumentation_panics_after_commit {let fault=Fault::InstrumentationPanic;
                to preserves_the_panic_cause_and_recovers_the_committed_task {recovers_unknown_claim()}
            }
        }
        when a_shared_listener_delivers_the_claim {let path=Path::Shared;
            to retires_the_worker_and_recovers_the_committed_task {recovers_unknown_claim()}
            when instrumentation_panics_after_commit {let fault=Fault::InstrumentationPanic;
                to preserves_the_panic_cause_and_recovers_the_committed_task {recovers_unknown_claim()}
            }
        }
        when middleware_acquires_the_claim {let path=Path::Fallback;
            to retires_the_worker_and_recovers_the_committed_task {recovers_unknown_claim()}
            when instrumentation_panics_after_commit {let fault=Fault::InstrumentationPanic;
                to preserves_the_panic_cause_and_recovers_the_committed_task {recovers_unknown_claim()}
            }
            when sql_rejects_the_claim_before_commit {let fault=Fault::RejectedQuery;
                to rolls_back_the_claim_and_keeps_the_worker_available {remains_available()}
            }
            when acknowledgement_is_manual {let automatic=false;
                to retires_the_worker_and_recovers_the_committed_task {recovers_unknown_claim()}
                when instrumentation_panics_after_commit {let fault=Fault::InstrumentationPanic;
                    to preserves_the_panic_cause_and_recovers_the_committed_task {recovers_unknown_claim()}
                }
                when sql_rejects_the_claim_before_commit {let fault=Fault::RejectedQuery;
                    to rolls_back_the_claim_and_keeps_the_worker_available {remains_available()}
                }
            }
        }
        when the_committed_fetch_contains_no_tasks {let fault=Fault::EmptyClaim;
            to preserves_the_error_and_keeps_the_worker_available {remains_available()}
        }
        when sql_rejects_the_claim_before_commit {let fault=Fault::RejectedQuery;
            to rolls_back_the_claim_and_keeps_the_worker_available {remains_available()}
        }
    }
}
