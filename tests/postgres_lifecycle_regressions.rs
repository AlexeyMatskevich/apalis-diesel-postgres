//! Lifecycle ownership, retry budgets, and recovery after interrupted work.
#![cfg(feature = "tokio")]
mod support;
#[path = "support/unreachable.rs"]
mod unreachable;
async fn database_case<T, F: Future<Output = T>>(
    run: impl FnOnce() -> F,
) -> Result<support::Outcome<T>, String> {
    if support::database_url_or_skip()?.is_none() {
        return Ok(support::Outcome::Skipped);
    }
    Ok(support::Outcome::Completed(run().await))
}
fn satisfies<T: std::fmt::Debug>(
    condition: impl Fn(&T) -> bool,
) -> impl Fn(&Result<support::Outcome<T>, String>) -> lets_expect::AssertionResult {
    support::observe("lifecycle contract", move |value| {
        if condition(value) {
            Ok(())
        } else {
            Err(format!("unexpected lifecycle observation: {value:?}"))
        }
    })
}

fn no_remaining_claims()
-> impl Fn(&Result<support::Outcome<(i64, i64)>, String>) -> lets_expect::AssertionResult {
    satisfies(|r: &(i64, i64)| r.1 == 0)
}
fn operation_refused()
-> impl Fn(&Result<support::Outcome<bool>, String>) -> lets_expect::AssertionResult {
    satisfies(|r: &bool| !*r)
}
fn poison_isolated()
-> impl Fn(&Result<support::Outcome<(bool, i64)>, String>) -> lets_expect::AssertionResult {
    satisfies(|r: &(bool, i64)| r.0 && r.1 == 1)
}
#[derive(Debug)]
struct MiddlewareRun {
    succeeded: bool,
    effects: usize,
    status: String,
}
#[derive(Debug)]
struct CompletionRun {
    failed: bool,
    stopped_heartbeat: bool,
    stopped_claim: bool,
    status: String,
    effects: usize,
}
fn middleware_completed()
-> impl Fn(&Result<support::Outcome<MiddlewareRun>, String>) -> lets_expect::AssertionResult {
    satisfies(|r: &MiddlewareRun| r.succeeded && r.effects == 1 && r.status == "Done")
}
fn completion_recovered()
-> impl Fn(&Result<support::Outcome<CompletionRun>, String>) -> lets_expect::AssertionResult {
    satisfies(|r: &CompletionRun| {
        r.failed && r.stopped_heartbeat && r.stopped_claim && r.status == "Done" && r.effects == 2
    })
}

mod claims {
    use apalis_core::{
        backend::{Backend, BackendExt},
        worker::context::WorkerContext,
    };
    use apalis_diesel_postgres::{Config, PgPool, PostgresStorage, build_pool, setup};
    use diesel::{
        QueryableByName, RunQueryDsl, sql_query,
        sql_types::{Array, BigInt, Text},
    };
    use futures::StreamExt;
    use lets_expect::*;
    use std::time::Duration;

    async fn pool() -> PgPool {
        let pool = build_pool(std::env::var("DATABASE_URL").unwrap()).unwrap();
        setup(&pool).await.unwrap();
        pool
    }
    async fn sql(pool: &PgPool, query: String) {
        let pool = pool.clone();
        tokio::task::spawn_blocking(move || {
            sql_query(query).execute(&mut pool.get().unwrap()).unwrap()
        })
        .await
        .unwrap();
    }
    #[derive(QueryableByName)]
    struct Count {
        #[diesel(sql_type=BigInt)]
        n: i64,
    }
    async fn count(pool: &PgPool, query: String) -> i64 {
        let pool = pool.clone();
        tokio::task::spawn_blocking(move || {
            sql_query(query)
                .get_result::<Count>(&mut pool.get().unwrap())
                .unwrap()
                .n
        })
        .await
        .unwrap()
    }
    async fn orphan_takeover() -> (i64, i64) {
        let p = pool().await;
        let q = format!("lifecycle_restart_{}", ulid::Ulid::new());
        let config = Config::new(&q)
            .set_reenqueue_orphaned_after(Duration::from_secs(60))
            .set_keep_alive(Duration::from_millis(10));
        let worker = WorkerContext::new::<()>("same-worker");
        let old = PostgresStorage::<String>::new_with_config(&p, &config);
        let mut old_stream = old.poll_compact(&worker);
        assert!(old_stream.next().await.unwrap().is_ok());
        sql(&p,format!("INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at,lock_by,lock_at) SELECT 'orphan-'||g,'{q}',''::bytea,'Running',0,25,now(),'same-worker',date_trunc('second',now()) FROM generate_series(1,1005)g")).await;
        sql(&p,format!("UPDATE apalis.workers SET last_seen=now()-interval '2 minutes' WHERE worker_type='{q}'")).await;
        let new = PostgresStorage::<String>::new_with_config(&p, &config);
        let mut beat = new.heartbeat(&worker);
        let mut new_stream = new.poll_compact(&worker);
        assert!(new_stream.next().await.unwrap().is_ok());
        let after = count(
            &p,
            format!("SELECT count(*)n FROM apalis.jobs WHERE job_type='{q}' AND status='Running'"),
        )
        .await;
        for _ in 0..4 {
            assert!(beat.next().await.unwrap().is_ok());
        }
        let later = count(
            &p,
            format!("SELECT count(*)n FROM apalis.jobs WHERE job_type='{q}' AND status='Running'"),
        )
        .await;
        sql(&p, format!("DELETE FROM apalis.jobs WHERE job_type='{q}'")).await;
        sql(
            &p,
            format!("DELETE FROM apalis.workers WHERE worker_type='{q}'"),
        )
        .await;
        eprintln!("remaining old Running: after takeover={after}, after heartbeat/sweep={later}");
        (after, later)
    }
    async fn stale_stream_claim() -> bool {
        let p = pool().await;
        let q = format!("lifecycle_stale_{}", ulid::Ulid::new());
        let config = Config::new(&q).set_reenqueue_orphaned_after(Duration::from_secs(60));
        let worker = WorkerContext::new::<()>("same-worker");
        let old = PostgresStorage::<String>::new_with_config(&p, &config);
        let mut old_stream = old.poll_compact(&worker);
        assert!(old_stream.next().await.unwrap().is_ok());
        sql(&p,format!("UPDATE apalis.workers SET last_seen=now()-interval '2 minutes' WHERE worker_type='{q}'")).await;
        let new = PostgresStorage::<String>::new_with_config(&p, &config);
        let mut new_stream = new.poll_compact(&worker);
        assert!(new_stream.next().await.unwrap().is_ok());
        let id = ulid::Ulid::new();
        sql(&p,format!("INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at) VALUES('{id}','{q}',''::bytea,'Pending',0,25,now())")).await;
        let got = matches!(
            tokio::time::timeout(Duration::from_secs(3), old_stream.next()).await,
            Ok(Some(Ok(Some(_))))
        );
        sql(&p, format!("DELETE FROM apalis.jobs WHERE job_type='{q}'")).await;
        sql(
            &p,
            format!("DELETE FROM apalis.workers WHERE worker_type='{q}'"),
        )
        .await;
        eprintln!("old stream claimed after token takeover={got}");
        got
    }
    async fn malformed_batch() -> (bool, i64) {
        let p = pool().await;
        let q = format!("lifecycle_malformed_id_{}", ulid::Ulid::new());
        let config = Config::new(&q).set_buffer_size(2);
        let worker = WorkerContext::new::<()>("batch-worker");
        let storage = PostgresStorage::<String>::new_with_config(&p, &config);
        let mut stream = storage.poll_compact(&worker);
        assert!(stream.next().await.unwrap().is_ok());
        let good = ulid::Ulid::new().to_string();
        let ids = vec!["not-a-ulid".to_owned(), good.clone()];
        let pp = p.clone();
        let qq = q.clone();
        tokio::task::spawn_blocking(move || {
        sql_query("INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at) SELECT unnest($1::text[]),$2,''::bytea,'Pending',0,25,now()")
        .bind::<Array<Text>,_>(ids).bind::<Text,_>(qq).execute(&mut pp.get().unwrap()).unwrap();
    }).await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(3), stream.next())
            .await
            .unwrap()
            .unwrap();
        eprintln!("mixed batch stream result={result:?}");
        let delivered = matches!(result, Ok(Some(ref task)) if task.parts.task_id.map(|id|id.to_string())==Some(good.clone()));
        let bad_terminal=count(&p,format!("SELECT count(*)n FROM apalis.jobs WHERE job_type='{q}' AND id='not-a-ulid' AND status='Killed' AND lock_by IS NULL")).await;
        let running = count(
            &p,
            format!("SELECT count(*)n FROM apalis.jobs WHERE job_type='{q}' AND status='Running'"),
        )
        .await;
        sql(&p, format!("DELETE FROM apalis.jobs WHERE job_type='{q}'")).await;
        sql(
            &p,
            format!("DELETE FROM apalis.workers WHERE worker_type='{q}'"),
        )
        .await;
        eprintln!("after return both claimed rows Running={running}");
        (delivered, bad_terminal)
    }
    async fn exhausted_pending() -> bool {
        let p = pool().await;
        let q = format!("lifecycle_exhausted_{}", ulid::Ulid::new());
        let config = Config::new(&q);
        let worker = WorkerContext::new::<()>("budget-worker");
        let storage = PostgresStorage::<String>::new_with_config(&p, &config);
        let mut stream = storage.poll_compact(&worker);
        assert!(stream.next().await.unwrap().is_ok());
        let id = ulid::Ulid::new();
        sql(&p,format!("INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at) VALUES('{id}','{q}',''::bytea,'Pending',1,1,now())")).await;
        let got = matches!(
            tokio::time::timeout(Duration::from_secs(3), stream.next()).await,
            Ok(Some(Ok(Some(_))))
        );
        sql(&p, format!("DELETE FROM apalis.jobs WHERE job_type='{q}'")).await;
        sql(
            &p,
            format!("DELETE FROM apalis.workers WHERE worker_type='{q}'"),
        )
        .await;
        eprintln!("exhausted Pending was claimed={got}");
        got
    }
    lets_expect! { #tokio_test
        expect(crate::database_case(orphan_takeover).await) as restarting_worker {
            when old_claims_exceed_one_sweep {
                to eventually_recovers_every_old_claim { crate::no_remaining_claims() }
            }
        }
        expect(crate::database_case(stale_stream_claim).await) as old_registration {
            when a_new_process_takes_over {
                to cannot_claim_new_work { crate::operation_refused() }
            }
        }
        expect(crate::database_case(malformed_batch).await) as mixed_batch {
            when an_id_is_invalid {
                to delivers_the_valid_sibling_and_quarantines_the_bad_row { crate::poison_isolated() }
            }
        }
        expect(crate::database_case(exhausted_pending).await) as exhausted_task {
            when no_attempts_remain {
                to is_not_claimed_again { crate::operation_refused() }
            }
        }
    }
}

mod middleware {
    use apalis_core::{
        backend::{FetchById, RegisterWorker, TaskSink},
        layers::{Layer, Service},
        task::builder::TaskBuilder,
        worker::context::WorkerContext,
    };
    use apalis_diesel_postgres::{
        Config, PgContext, PgMiddleware, PgTask, PostgresStorage, build_pool, setup,
    };
    use lets_expect::*;
    use std::{
        future::{Ready, ready},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };
    #[derive(Clone)]
    struct Handler(Arc<AtomicUsize>);
    impl Service<PgTask<String>> for Handler {
        type Response = ();
        type Error = std::io::Error;
        type Future = Ready<Result<(), Self::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: PgTask<String>) -> Self::Future {
            self.0.fetch_add(1, Ordering::SeqCst);
            ready(Ok(()))
        }
    }
    async fn direct_middleware() -> crate::MiddlewareRun {
        let p = build_pool(std::env::var("DATABASE_URL").unwrap()).unwrap();
        setup(&p).await.unwrap();
        let q = format!("lifecycle_middleware_{}", ulid::Ulid::new());
        let config = Config::new(&q);
        let mut storage = PostgresStorage::<String>::new_with_config(&p, &config);
        storage
            .register_worker("middleware-worker".to_owned())
            .await
            .unwrap();
        let id = apalis_diesel_postgres::PgTaskId::new(ulid::Ulid::new());
        let mut task = TaskBuilder::new("work".to_owned())
            .with_task_id(id)
            .with_ctx(PgContext::new().with_queue(q.clone()))
            .build();
        storage.push_task(task.clone()).await.unwrap();
        task.parts
            .data
            .insert(WorkerContext::new::<()>("middleware-worker"));
        let effects = Arc::new(AtomicUsize::new(0));
        let mut service = PgMiddleware::new(&p, true).layer(Handler(effects.clone()));
        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        let result = service.call(task).await;
        let status = storage
            .fetch_by_id(&id)
            .await
            .unwrap()
            .unwrap()
            .parts
            .status
            .load()
            .to_string();
        eprintln!(
            "middleware result={result:?}; effects={}; persisted status={status}",
            effects.load(Ordering::SeqCst)
        );
        crate::MiddlewareRun {
            succeeded: result.is_ok(),
            effects: effects.load(Ordering::SeqCst),
            status,
        }
    }
    lets_expect! {#tokio_test
     expect(crate::database_case(direct_middleware).await) as public_middleware {
      when the_pending_task_needs_a_lock {
       to locks_executes_and_acknowledges { crate::middleware_completed() }
      }
     }
    }
}

mod races {
    use apalis_core::{
        backend::BackendExt,
        error::BoxDynError,
        task::attempt::Attempt,
        worker::{context::WorkerContext, ext::ack::Acknowledge},
    };
    use apalis_diesel_postgres::{Config, PgAck, PgPool, PostgresStorage, build_pool, setup};
    use diesel::{
        QueryableByName, RunQueryDsl, sql_query,
        sql_types::{BigInt, Text},
    };
    use futures::StreamExt;
    use lets_expect::*;
    use std::{sync::Arc, time::Duration};
    async fn pool() -> PgPool {
        let p = build_pool(std::env::var("DATABASE_URL").unwrap()).unwrap();
        setup(&p).await.unwrap();
        p
    }
    async fn sql(p: &PgPool, s: String) {
        let p = p.clone();
        tokio::task::spawn_blocking(move || sql_query(s).execute(&mut p.get().unwrap()).unwrap())
            .await
            .unwrap();
    }
    #[derive(QueryableByName)]
    struct N {
        #[diesel(sql_type=BigInt)]
        n: i64,
    }
    async fn n(p: &PgPool, s: String) -> i64 {
        let p = p.clone();
        tokio::task::spawn_blocking(move || {
            sql_query(s)
                .get_result::<N>(&mut p.get().unwrap())
                .unwrap()
                .n
        })
        .await
        .unwrap()
    }
    #[derive(QueryableByName)]
    struct Token {
        #[diesel(sql_type=Text)]
        lease_token: String,
    }
    async fn immediate_takeover() -> bool {
        let p = pool().await;
        let q = format!("lifecycle_fractional_timeout_{}", ulid::Ulid::new());
        let c = Config::new(&q).set_reenqueue_orphaned_after(Duration::from_millis(500));
        let w = WorkerContext::new::<()>("fraction-worker");
        let mut a = PostgresStorage::<String>::new_with_config(&p, &c).poll_compact(&w);
        assert!(a.next().await.unwrap().is_ok());
        let mut b = PostgresStorage::<String>::new_with_config(&p, &c).poll_compact(&w);
        let result = b.next().await.unwrap();
        eprintln!("immediate takeover with 500ms timeout = {result:?}");
        sql(
            &p,
            format!("DELETE FROM apalis.workers WHERE worker_type='{q}'"),
        )
        .await;
        result.is_ok()
    }
    async fn controlled_deadlock() -> bool {
        let p = pool().await;
        let q = format!("lifecycle_deadlock_{}", ulid::Ulid::new());
        let c = Config::new(&q).set_reenqueue_orphaned_after(Duration::from_secs(60));
        let w = WorkerContext::new::<()>("deadlock-worker");
        let mut old = PostgresStorage::<String>::new_with_config(&p, &c).poll_compact(&w);
        assert!(old.next().await.unwrap().is_ok());
        let id = ulid::Ulid::new();
        sql(&p,format!("INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at) VALUES('{id}','{q}',''::bytea,'Pending',0,25,now())")).await;
        let mut task = old.next().await.unwrap().unwrap().unwrap();
        task.parts.attempt = Attempt::new_with_value(1);
        let pp = p.clone();
        let qq = q.clone();
        let token = tokio::task::spawn_blocking(move || {
            sql_query("SELECT lease_token FROM apalis.workers WHERE worker_type=$1")
                .bind::<Text, _>(qq)
                .get_result::<Token>(&mut pp.get().unwrap())
                .unwrap()
                .lease_token
        })
        .await
        .unwrap();
        sql(&p,format!("UPDATE apalis.workers SET last_seen=now()-interval '2 minutes' WHERE worker_type='{q}'")).await;
        sql(&p,"CREATE OR REPLACE FUNCTION apalis.test_block_recovery() RETURNS trigger AS $$ BEGIN IF NEW.status='Pending' AND OLD.status='Running' THEN PERFORM pg_advisory_xact_lock(8765412); END IF; RETURN NEW; END $$ LANGUAGE plpgsql".to_owned()).await;
        sql(&p,format!("CREATE TRIGGER test_block_recovery BEFORE UPDATE ON apalis.jobs FOR EACH ROW WHEN (OLD.job_type='{q}') EXECUTE FUNCTION apalis.test_block_recovery()")).await;
        let mut holder = p.get().unwrap();
        sql_query("BEGIN").execute(&mut holder).unwrap();
        sql_query("SELECT pg_advisory_xact_lock(8765412)")
            .execute(&mut holder)
            .unwrap();
        let mut fresh = PostgresStorage::<String>::new_with_config(&p, &c).poll_compact(&w);
        let takeover = tokio::spawn(async move { fresh.next().await });
        for _ in 0..200 {
            if n(&p,"SELECT count(*)n FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory'".to_owned()).await>0{break;}
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(n(&p,"SELECT count(*)n FROM pg_stat_activity WHERE datname=current_database() AND wait_event='advisory'".to_owned()).await>0);
        let mut ack = PgAck::with_lease_token(&p, Arc::from(token));
        let acknowledger = tokio::spawn(async move {
            let result: Result<(), BoxDynError> = Ok(());
            ack.ack(&result, &task.parts).await
        });
        for _ in 0..200 {
            if n(&p,"SELECT count(*)n FROM pg_stat_activity WHERE datname=current_database() AND wait_event='transactionid'".to_owned()).await>0{break;}
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let waiting=n(&p,"SELECT count(*)n FROM pg_stat_activity WHERE datname=current_database() AND wait_event='transactionid'".to_owned()).await;
        assert!(waiting > 0);
        sql_query("COMMIT").execute(&mut holder).unwrap();
        drop(holder);
        let a = tokio::time::timeout(Duration::from_secs(5), takeover)
            .await
            .unwrap()
            .unwrap();
        let b = tokio::time::timeout(Duration::from_secs(5), acknowledger)
            .await
            .unwrap()
            .unwrap();
        eprintln!("controlled recovery={a:?}; ack={b:?}");
        let deadlock = !(matches!(a, Some(Ok(None)))
            && matches!(
                b,
                Err(apalis_diesel_postgres::Error::StaleAcknowledgement { .. })
            ));
        sql(
            &p,
            "DROP TRIGGER test_block_recovery ON apalis.jobs".to_owned(),
        )
        .await;
        sql(&p, "DROP FUNCTION apalis.test_block_recovery()".to_owned()).await;
        sql(&p, format!("DELETE FROM apalis.jobs WHERE job_type='{q}'")).await;
        sql(
            &p,
            format!("DELETE FROM apalis.workers WHERE worker_type='{q}'"),
        )
        .await;
        deadlock
    }
    lets_expect! {#tokio_test
     expect(crate::database_case(immediate_takeover).await)as fractional_timeout{when the_incumbent_is_fresh{to rejects_takeover_before_the_timeout{crate::operation_refused()}}}
     expect(crate::database_case(controlled_deadlock).await)as ownership_operations{when recovery_holds_the_job_before_ack_finishes{to do_not_deadlock{crate::operation_refused()}}}
    }
}

mod retirement {
    use apalis_core::{
        backend::{Backend, FetchById, TaskSink},
        layers::{Layer, Service},
        task::{attempt::Attempt, builder::TaskBuilder},
        worker::context::WorkerContext,
    };
    use apalis_diesel_postgres::{
        Config, Error, PgPool, PgTask, PgTaskId, PostgresStorage, build_pool_with, setup,
    };
    use diesel::{
        PgConnection,
        r2d2::{ConnectionManager, PooledConnection},
    };
    use futures::StreamExt;
    use lets_expect::*;
    use std::{
        future::{Ready, ready},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    };
    type Held = Arc<Mutex<Option<PooledConnection<ConnectionManager<PgConnection>>>>>;
    #[derive(Debug)]
    struct Response(bool);
    impl serde::Serialize for Response {
        fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            if self.0 {
                Err(serde::ser::Error::custom("result cannot be encoded"))
            } else {
                s.serialize_unit()
            }
        }
    }
    #[derive(Clone)]
    struct Handler {
        effects: Arc<AtomicUsize>,
        pool: PgPool,
        held: Held,
        serialization_fails: bool,
    }
    impl Service<PgTask<String>> for Handler {
        type Response = Response;
        type Error = std::io::Error;
        type Future = Ready<Result<Response, Self::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: PgTask<String>) -> Self::Future {
            if self.effects.fetch_add(1, Ordering::SeqCst) == 0 && !self.serialization_fails {
                *self.held.lock().unwrap() = Some(self.pool.get().unwrap());
            }
            ready(Ok(Response(self.serialization_fails)))
        }
    }
    async fn recovered_completion(serialization_fails: bool) -> crate::CompletionRun {
        let pool = build_pool_with(std::env::var("DATABASE_URL").unwrap(), |b| {
            b.max_size(1)
                .min_idle(Some(0))
                .connection_timeout(Duration::from_millis(40))
        })
        .unwrap();
        // A lazy pool has no connection yet. Prepare it independently from the
        // short checkout timeout used to exercise the later acknowledgement.
        let startup_pool = pool.clone();
        tokio::task::spawn_blocking(move || {
            drop(startup_pool.get_timeout(Duration::from_secs(30)).unwrap());
        })
        .await
        .unwrap();
        setup(&pool).await.unwrap();
        let queue = format!("lifecycle_completion_{}", ulid::Ulid::new());
        let config = Config::new(&queue)
            .set_buffer_size(1)
            .set_keep_alive(Duration::from_millis(10))
            .set_reenqueue_orphaned_after(Duration::from_secs(1));
        let mut storage = PostgresStorage::<String>::new_with_config(&pool, &config);
        let worker = WorkerContext::new::<()>("completion-worker");
        let id = PgTaskId::new(ulid::Ulid::new());
        storage
            .push_task(
                TaskBuilder::new("effect".to_owned())
                    .with_task_id(id)
                    .build(),
            )
            .await
            .unwrap();
        let mut tasks = storage.clone().poll(&worker);
        assert!(tasks.next().await.unwrap().is_ok());
        let mut task = tasks.next().await.unwrap().unwrap().unwrap();
        task.parts.attempt = Attempt::new_with_value(1);
        task.parts.data.insert(worker.clone());
        let effects = Arc::new(AtomicUsize::new(0));
        let held: Held = Arc::new(Mutex::new(None));
        let handler = Handler {
            effects: effects.clone(),
            pool: pool.clone(),
            held: held.clone(),
            serialization_fails,
        };
        let mut service = storage.middleware().layer(handler);
        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        let result = service.call(task).await;
        // Keep the connection occupied until ack has actually failed, even if
        // the executor delays the handler or the acknowledgement.
        held.lock().unwrap().take();
        let failed = matches!(
            (
                serialization_fails,
                result
                    .as_ref()
                    .err()
                    .and_then(|e| e.downcast_ref::<Error>())
            ),
            (false, Some(Error::Pool(_))) | (true, Some(Error::Json(_)))
        );
        assert!(failed, "unexpected acknowledgement result: {result:?}");
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        let mut heartbeat = storage.heartbeat(&worker);
        let stopped_heartbeat = matches!(
            heartbeat.next().await,
            Some(Err(Error::WorkerRetired { .. }))
        );
        let stopped_claim = matches!(tasks.next().await, Some(Err(Error::WorkerRetired { .. })));
        assert!(stopped_heartbeat && stopped_claim);
        // Model a stale registration after proving that its owner has stopped.
        // Recovery itself must still reclaim the original job and retry it.
        let aging_pool = pool.clone();
        let aging_queue = queue.clone();
        let aging_worker = worker.name().clone();
        let aged = tokio::task::spawn_blocking(move || {
            use diesel::{RunQueryDsl, sql_query, sql_types::Text};
            sql_query(
                "UPDATE apalis.workers SET last_seen=clock_timestamp()-interval '2 minutes' \
                 WHERE worker_type=$1 AND id=$2",
            )
            .bind::<Text, _>(aging_queue)
            .bind::<Text, _>(aging_worker)
            .execute(&mut aging_pool.get().unwrap())
            .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(aged, 1);
        let replacement_config = Config::new(&queue)
            .set_buffer_size(1)
            .set_keep_alive(Duration::from_millis(10))
            .set_reenqueue_orphaned_after(Duration::from_secs(1));
        let mut replacement =
            PostgresStorage::<String>::new_with_config(&pool, &replacement_config);
        let mut next = replacement.clone().poll(&worker);
        assert!(next.next().await.unwrap().is_ok());
        let mut recovered = next.next().await.unwrap().unwrap().unwrap();
        assert_eq!(recovered.parts.task_id, Some(id));
        assert_eq!(recovered.parts.attempt.current(), 1);
        recovered.parts.attempt = Attempt::new_with_value(2);
        recovered.parts.data.insert(worker);
        let mut finishing = replacement.middleware().layer(Handler {
            effects: effects.clone(),
            pool: pool.clone(),
            held,
            serialization_fails: false,
        });
        futures::future::poll_fn(|cx| finishing.poll_ready(cx))
            .await
            .unwrap();
        finishing.call(recovered).await.unwrap();
        let status = replacement
            .fetch_by_id(&id)
            .await
            .unwrap()
            .unwrap()
            .parts
            .status
            .load()
            .to_string();
        let cleanup = pool.clone();
        tokio::task::spawn_blocking(move || {
            use diesel::{RunQueryDsl, sql_query, sql_types::Text};
            let mut c = cleanup.get().unwrap();
            sql_query("DELETE FROM apalis.jobs WHERE job_type=$1")
                .bind::<Text, _>(&queue)
                .execute(&mut c)
                .unwrap();
            sql_query("DELETE FROM apalis.workers WHERE worker_type=$1")
                .bind::<Text, _>(&queue)
                .execute(&mut c)
                .unwrap();
        })
        .await
        .unwrap();
        crate::CompletionRun {
            failed,
            stopped_heartbeat,
            stopped_claim,
            status,
            effects: effects.load(Ordering::SeqCst),
        }
    }
    async fn required_recovered_completion(
        serialization_fails: bool,
    ) -> Result<crate::support::Outcome<crate::CompletionRun>, String> {
        crate::database_case(|| recovered_completion(serialization_fails)).await
    }
    lets_expect! {#tokio_test
     expect(required_recovered_completion(serialization_fails).await) as lost_completion_obligation {
     let serialization_fails=false;
     to retires_and_recovers_after_a_pool_error { crate::completion_recovered() }
     when the_handler_result_cannot_be_serialized {
     let serialization_fails=true;
     to retires_and_recovers_after_an_encoding_error {crate::completion_recovered()}
     }
    }}
}

mod registered_middleware {
    use apalis_core::{
        backend::{Backend, BackendExt, FetchById, TaskSink},
        layers::{Layer, Service},
        task::builder::TaskBuilder,
        worker::context::WorkerContext,
    };
    use apalis_diesel_postgres::{
        Config, Error, PgContext, PgTask, PgTaskId, PostgresStorage, build_pool, setup,
    };
    use diesel::{RunQueryDsl, sql_query, sql_types::Text};
    use futures::StreamExt;
    use lets_expect::*;
    use std::{
        future::{Ready, ready},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    };
    #[derive(Debug)]
    struct FallbackRun {
        error: &'static str,
        effects: usize,
        context_complete: bool,
        status: String,
    }
    #[derive(Clone, Copy)]
    enum Registration {
        Current,
        Replaced,
        Retired,
    }
    #[derive(Clone)]
    struct Handler {
        effects: Arc<AtomicUsize>,
        context: Arc<AtomicBool>,
    }
    impl Service<PgTask<String>> for Handler {
        type Response = ();
        type Error = std::io::Error;
        type Future = Ready<Result<(), Self::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, task: PgTask<String>) -> Self::Future {
            self.effects.fetch_add(1, Ordering::SeqCst);
            self.context.store(
                task.parts.ctx.lock_by().as_deref() == Some("fallback-worker")
                    && task.parts.ctx.lock_at().is_some()
                    && task.args == "work",
                Ordering::SeqCst,
            );
            ready(Ok(()))
        }
    }
    fn error_kind(result: &Result<(), apalis_core::error::BoxDynError>) -> &'static str {
        let Err(error) = result else { return "success" };
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(error.as_ref());
        while let Some(error) = current {
            if let Some(error) = error.downcast_ref::<Error>() {
                return match error {
                    Error::WorkerNotRegistered { .. } => "replaced",
                    Error::WorkerRetired { .. } => "retired",
                    _ => "other",
                };
            }
            current = error.source();
        }
        "other"
    }
    async fn fallback(registration: Registration, auto_ack: bool) -> FallbackRun {
        let pool = build_pool(std::env::var("DATABASE_URL").unwrap()).unwrap();
        setup(&pool).await.unwrap();
        let queue = format!("lifecycle_fallback_{}", ulid::Ulid::new());
        let config = Config::new(&queue)
            .set_ack(auto_ack)
            .set_reenqueue_orphaned_after(Duration::from_secs(60));
        let mut storage = PostgresStorage::<String>::new_with_config(&pool, &config);
        let worker = WorkerContext::new::<()>("fallback-worker");
        let mut old_stream = Some(storage.clone().poll_compact(&worker));
        assert!(old_stream.as_mut().unwrap().next().await.unwrap().is_ok());
        if matches!(registration, Registration::Replaced) {
            let p = pool.clone();
            let q = queue.clone();
            tokio::task::spawn_blocking(move||sql_query("UPDATE apalis.workers SET last_seen=now()-interval '2 minutes' WHERE worker_type=$1").bind::<Text,_>(q).execute(&mut p.get().unwrap()).unwrap()).await.unwrap();
            let replacement = PostgresStorage::<String>::new_with_config(
                &pool,
                &Config::new(&queue).set_reenqueue_orphaned_after(Duration::from_secs(60)),
            );
            let mut replacement_stream = replacement.poll_compact(&worker);
            assert!(replacement_stream.next().await.unwrap().is_ok());
        }
        if matches!(registration, Registration::Retired) {
            drop(old_stream.take());
        }
        let id = PgTaskId::new(ulid::Ulid::new());
        let mut task = TaskBuilder::new("work".to_owned())
            .with_task_id(id)
            .with_ctx(PgContext::new().with_queue(queue.clone()))
            .build();
        storage.push_task(task.clone()).await.unwrap();
        task.parts
            .data
            .insert(WorkerContext::new::<()>("fallback-worker"));
        let effects = Arc::new(AtomicUsize::new(0));
        let context = Arc::new(AtomicBool::new(false));
        let mut service = storage.middleware().layer(Handler {
            effects: effects.clone(),
            context: context.clone(),
        });
        futures::future::poll_fn(|cx| service.poll_ready(cx))
            .await
            .unwrap();
        let result = service.call(task).await;
        let observed = FallbackRun {
            error: error_kind(&result),
            effects: effects.load(Ordering::SeqCst),
            context_complete: context.load(Ordering::SeqCst),
            status: storage
                .fetch_by_id(&id)
                .await
                .unwrap()
                .unwrap()
                .parts
                .status
                .load()
                .to_string(),
        };
        eprintln!("fallback auto_ack={auto_ack}: {observed:?}; result={result:?}");
        tokio::task::spawn_blocking(move || {
            let mut c = pool.get().unwrap();
            sql_query("DELETE FROM apalis.jobs WHERE job_type=$1")
                .bind::<Text, _>(&queue)
                .execute(&mut c)
                .unwrap();
            sql_query("DELETE FROM apalis.workers WHERE worker_type=$1")
                .bind::<Text, _>(&queue)
                .execute(&mut c)
                .unwrap();
        })
        .await
        .unwrap();
        observed
    }
    fn outcome(
        kind: &'static str,
        effects: usize,
        context: bool,
        status: &'static str,
    ) -> impl Fn(&Result<crate::support::Outcome<FallbackRun>, String>) -> AssertionResult {
        crate::satisfies(move |r: &FallbackRun| {
            r.error == kind
                && r.effects == effects
                && r.context_complete == context
                && r.status == status
        })
    }
    async fn run(
        registration: Registration,
        auto_ack: bool,
    ) -> Result<crate::support::Outcome<FallbackRun>, String> {
        crate::database_case(|| fallback(registration, auto_ack)).await
    }
    lets_expect! {#tokio_test
      expect(run(registration,auto_ack).await) as registered_fallback {
        let registration=Registration::Current; let auto_ack=true;
        to locks_passes_the_claim_context_and_acknowledges {outcome("success",1,true,"Done")}
        when the_registration_has_been_replaced {let registration=Registration::Replaced;
          to refuses_the_claim_before_running_the_handler {outcome("replaced",0,false,"Pending")}
        }
        when the_registration_has_retired {let registration=Registration::Retired;
          to refuses_the_claim_before_running_the_handler {outcome("retired",0,false,"Pending")}
        }
        when acknowledgement_is_manual {let auto_ack=false;
          to passes_the_claim_context_to_the_handler {outcome("success",1,true,"Running")}
          when the_registration_has_been_replaced {let registration=Registration::Replaced;
            to refuses_the_claim_before_running_the_handler {outcome("replaced",0,false,"Pending")}
          }
          when the_registration_has_retired {let registration=Registration::Retired;
            to refuses_the_claim_before_running_the_handler {outcome("retired",0,false,"Pending")}
          }
        }
      }
    }
}

mod unpolled_stream {
    use apalis_core::{
        backend::{
            Backend, BackendExt, TaskSink, TaskStream, poll_strategy::IntervalStrategy,
            shared::MakeShared,
        },
        task::Task,
        worker::context::WorkerContext,
    };
    use apalis_diesel_postgres::{
        CompactType, Config, Error, JsonCodec, PgPool, PgTask, PgTaskId, PostgresStorage,
        SharedPostgresStorage, build_pool, setup,
    };
    use diesel::{RunQueryDsl, sql_query, sql_types::Text};
    use futures::{StreamExt, stream::BoxStream};
    use lets_expect::*;
    use std::time::Duration;
    #[derive(Clone, Copy)]
    enum Mode {
        Polling,
        Notify,
        Shared,
    }
    #[derive(Debug)]
    struct Observation {
        heartbeat_succeeded: bool,
        exact_task_delivered: bool,
    }
    struct Streams {
        active: TaskStream<PgTask<String>, Error>,
        heartbeat: BoxStream<'static, Result<(), Error>>,
        discard: Box<dyn FnOnce() + Send>,
    }
    fn streams(
        pool: &PgPool,
        config: &Config,
        worker: &WorkerContext,
        mode: Mode,
        decoded: bool,
    ) -> Streams {
        macro_rules! prepared {
            ($storage:expr,$disposable:expr) => {{
                let storage = $storage;
                let disposable = $disposable;
                let heartbeat = storage.heartbeat(worker);
                let active = storage.poll(worker);
                let worker = worker.clone();
                Streams {
                    active,
                    heartbeat,
                    discard: Box::new(move || {
                        if decoded {
                            drop(disposable.poll(&worker));
                        } else {
                            drop(disposable.poll_compact(&worker));
                        }
                    }),
                }
            }};
        }
        match mode {
            Mode::Polling => {
                let storage = PostgresStorage::<String>::new_with_config(pool, config)
                    .with_poll_strategy_factory(|| IntervalStrategy::new(Duration::from_millis(5)));
                let disposable = storage.clone();
                prepared!(storage, disposable)
            }
            Mode::Notify => {
                let storage = PostgresStorage::<String>::new_with_notify(pool, config)
                    .with_poll_strategy_factory(|| IntervalStrategy::new(Duration::from_millis(5)));
                let disposable = storage.clone();
                prepared!(storage, disposable)
            }
            Mode::Shared => {
                let mut factory: SharedPostgresStorage<JsonCodec<CompactType>> =
                    SharedPostgresStorage::new(pool);
                let storage = <SharedPostgresStorage<JsonCodec<CompactType>> as MakeShared<
                    String,
                >>::make_shared_with_config(
                    &mut factory, config.clone()
                )
                .unwrap()
                .with_poll_strategy_factory(|| IntervalStrategy::new(Duration::from_millis(5)));
                // SharedFetcher is not Clone: a second subscription owns a distinct registration.
                let disposable = <SharedPostgresStorage<JsonCodec<CompactType>> as MakeShared<
                    String,
                >>::make_shared_with_config(
                    &mut factory, config.clone()
                )
                .unwrap()
                .with_poll_strategy_factory(|| IntervalStrategy::new(Duration::from_millis(5)));
                prepared!(storage, disposable)
            }
        }
    }
    async fn discard_unpolled_sibling(mode: Mode, decoded: bool) -> Observation {
        let pool = build_pool(std::env::var("DATABASE_URL").unwrap()).unwrap();
        setup(&pool).await.unwrap();
        let queue = format!("lifecycle_unpolled_{}", ulid::Ulid::new());
        let config = Config::new(&queue).set_keep_alive(Duration::from_millis(5));
        let worker = WorkerContext::new::<()>("active-worker");
        let Streams {
            mut active,
            mut heartbeat,
            discard,
        } = streams(&pool, &config, &worker, mode, decoded);
        assert!(active.next().await.unwrap().is_ok());
        // Drop without first poll: no registration/claim SQL was dispatched by this stream.
        discard();
        let mut producer = PostgresStorage::<String>::new_with_config(&pool, &config);
        let id = PgTaskId::new(ulid::Ulid::new());
        let mut task = Task::new("work".to_owned());
        task.parts.task_id = Some(id);
        producer.push_task(task).await.unwrap();
        let heartbeat_result = heartbeat.next().await;
        let delivered = tokio::time::timeout(Duration::from_secs(2), active.next()).await;
        eprintln!(
            "unpolled sibling decoded={decoded}: heartbeat={heartbeat_result:?}; next={delivered:?}"
        );
        let observation = Observation {
            heartbeat_succeeded: matches!(heartbeat_result, Some(Ok(()))),
            exact_task_delivered: matches!(delivered,Ok(Some(Ok(Some(task)))) if task.parts.task_id==Some(id)),
        };
        drop(active);
        drop(heartbeat);
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().unwrap();
            sql_query("DELETE FROM apalis.jobs WHERE job_type=$1")
                .bind::<Text, _>(&queue)
                .execute(&mut conn)
                .unwrap();
            sql_query("DELETE FROM apalis.workers WHERE worker_type=$1")
                .bind::<Text, _>(&queue)
                .execute(&mut conn)
                .unwrap();
        })
        .await
        .unwrap();
        observation
    }
    fn preserves_active_worker()
    -> impl Fn(&Result<crate::support::Outcome<Observation>, String>) -> AssertionResult {
        crate::satisfies(|r: &Observation| r.heartbeat_succeeded && r.exact_task_delivered)
    }
    async fn run(
        mode: Mode,
        decoded: bool,
    ) -> Result<crate::support::Outcome<Observation>, String> {
        crate::database_case(|| discard_unpolled_sibling(mode, decoded)).await
    }
    lets_expect! {#tokio_test
      expect(run(mode,decoded).await) as an_unpolled_worker_stream {
        let mode=Mode::Polling;let decoded=false;
        to preserves_the_active_sibling {preserves_active_worker()}
        when decoding_is_enabled {let decoded=true;to preserves_the_active_sibling {preserves_active_worker()}}
        when notifications_are_enabled {let mode=Mode::Notify;
          to preserves_the_active_sibling {preserves_active_worker()}
          when decoding_is_enabled {let decoded=true;to preserves_the_active_sibling {preserves_active_worker()}}
        }
        when the_shared_subscriptions_are_independent {let mode=Mode::Shared;
          to preserves_the_active_registration {preserves_active_worker()}
          when decoding_is_enabled {let decoded=true;to preserves_the_active_registration {preserves_active_worker()}}
        }
      }
    }
}

// Only a stream that held a registration can lose completion obligations.
mod dropped_stream {
    use apalis_core::{backend::BackendExt, worker::context::WorkerContext};
    use apalis_diesel_postgres::{
        CompactType, Config, Error, PgPool, PgTask, PostgresStorage, build_pool, setup,
    };
    use diesel::{RunQueryDsl, sql_query, sql_types::Text};
    use futures::{Stream, StreamExt};
    use lets_expect::*;
    use std::time::Duration;
    #[derive(Clone, Copy)]
    enum Registration {
        Succeeded,
        Refused,
        Unreachable,
    }
    #[derive(Debug)]
    struct Observation {
        first_item: &'static str,
        sibling_item: &'static str,
        sibling_after_holder_stale: &'static str,
    }
    fn kind(item: Option<Result<Option<PgTask<CompactType>>, Error>>) -> &'static str {
        match item {
            Some(Ok(None)) => "registered",
            Some(Ok(Some(_))) => "task",
            Some(Err(Error::AlreadyRegistered { .. })) => "already_registered",
            Some(Err(Error::WorkerRetired { .. })) => "retired",
            Some(Err(Error::Pool(_))) => "pool",
            Some(Err(_)) => "other_error",
            None => "ended",
        }
    }
    async fn next_kind<S>(stream: &mut S) -> &'static str
    where
        S: Stream<Item = Result<Option<PgTask<CompactType>>, Error>> + Unpin,
    {
        kind(
            tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("a stream item within five seconds"),
        )
    }
    async fn age_registration(pool: &PgPool, queue: &str, worker: &str) {
        let pool = pool.clone();
        let queue = queue.to_owned();
        let worker = worker.to_owned();
        tokio::task::spawn_blocking(move || {
            sql_query(
                "UPDATE apalis.workers SET last_seen=clock_timestamp()-interval '2 minutes' \
                 WHERE worker_type=$1 AND id=$2",
            )
            .bind::<Text, _>(queue)
            .bind::<Text, _>(worker)
            .execute(&mut pool.get().unwrap())
            .unwrap();
        })
        .await
        .unwrap();
    }
    async fn dispose_after_first_item(registration: Registration) -> Observation {
        let pool = build_pool(std::env::var("DATABASE_URL").unwrap()).unwrap();
        setup(&pool).await.unwrap();
        let queue = format!("lifecycle_dropped_{}", ulid::Ulid::new());
        let config = Config::new(&queue)
            .set_keep_alive(Duration::from_millis(5))
            .set_reenqueue_orphaned_after(Duration::from_secs(60));
        let worker = WorkerContext::new::<()>("rolling-worker");
        // Another process keeps the name registered while the subject starts.
        let mut holder = None;
        if matches!(registration, Registration::Refused) {
            let mut stream =
                PostgresStorage::<String>::new_with_config(&pool, &config).poll_compact(&worker);
            assert_eq!(next_kind(&mut stream).await, "registered");
            holder = Some(stream);
        }
        let subject_pool = match registration {
            Registration::Unreachable => crate::unreachable::unreachable_pool(),
            Registration::Succeeded | Registration::Refused => pool.clone(),
        };
        let storage = PostgresStorage::<String>::new_with_config(&subject_pool, &config);
        let mut first = storage.clone().poll_compact(&worker);
        let first_item = next_kind(&mut first).await;
        drop(first);
        let mut sibling = storage.clone().poll_compact(&worker);
        let sibling_item = next_kind(&mut sibling).await;
        drop(sibling);
        // The other process exits and its registration passes the stale deadline.
        drop(holder);
        age_registration(&pool, &queue, worker.name()).await;
        let mut later = storage.clone().poll_compact(&worker);
        let sibling_after_holder_stale = next_kind(&mut later).await;
        drop(later);
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().unwrap();
            sql_query("DELETE FROM apalis.jobs WHERE job_type=$1")
                .bind::<Text, _>(&queue)
                .execute(&mut conn)
                .unwrap();
            sql_query("DELETE FROM apalis.workers WHERE worker_type=$1")
                .bind::<Text, _>(&queue)
                .execute(&mut conn)
                .unwrap();
        })
        .await
        .unwrap();
        Observation {
            first_item,
            sibling_item,
            sibling_after_holder_stale,
        }
    }
    fn observes(
        first_item: &'static str,
        sibling_item: &'static str,
        sibling_after_holder_stale: &'static str,
    ) -> impl Fn(&Result<crate::support::Outcome<Observation>, String>) -> AssertionResult {
        crate::satisfies(move |o: &Observation| {
            o.first_item == first_item
                && o.sibling_item == sibling_item
                && o.sibling_after_holder_stale == sibling_after_holder_stale
        })
    }
    async fn run(
        registration: Registration,
    ) -> Result<crate::support::Outcome<Observation>, String> {
        crate::database_case(|| dispose_after_first_item(registration)).await
    }
    lets_expect! {#tokio_test
      expect(run(registration).await) as a_worker_stream_dropped_after_its_first_item {
        let registration=Registration::Succeeded;
        to retires_the_registration_for_every_clone {observes("registered","retired","retired")}
        when the_name_is_held_by_a_live_registration {let registration=Registration::Refused;
          to leaves_the_clones_free_to_register_once_the_holder_is_stale {
            observes("already_registered","already_registered","registered")
          }
        }
        when the_pool_is_unreachable {let registration=Registration::Unreachable;
          to leaves_the_clones_free_to_retry {observes("pool","pool","pool")}
        }
      }
    }
}

// Claim identity and runtime execution counts have separate responsibilities.
mod attempt_accounting {
    use crate::support;
    use apalis_core::{
        backend::{BackendExt, RegisterWorker},
        layers::{Layer, Service},
        task::{attempt::Attempt, builder::TaskBuilder},
        worker::{AttemptOnPollFuture, context::WorkerContext, ext::ack::Acknowledge},
    };
    use apalis_diesel_postgres::{
        Config, Error, PgAck, PgContext, PgMiddleware, PgTask, PgTaskId, PostgresStorage,
    };
    use diesel::{
        QueryableByName, RunQueryDsl, sql_query,
        sql_types::{Integer, Jsonb},
    };
    use futures::{
        FutureExt, StreamExt,
        future::{BoxFuture, Either},
    };
    use lets_expect::*;
    use serde_json::{Value, json};
    use std::{
        sync::{Arc, Mutex},
        task::{Context, Poll},
    };

    #[derive(Clone, Copy, Debug)]
    enum Entry {
        Direct,
        Tracked,
        PreclaimedDirect,
    }
    #[derive(Clone, Copy, Debug)]
    enum Input {
        Default,
        Coherent,
        StaleHigh,
    }

    #[derive(Clone)]
    struct Handler {
        fails: bool,
        observed: Arc<Mutex<Vec<usize>>>,
    }
    impl Service<PgTask<String>> for Handler {
        type Response = String;
        type Error = std::io::Error;
        type Future = BoxFuture<'static, Result<String, Self::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, task: PgTask<String>) -> Self::Future {
            let observed = self.observed.clone();
            let fails = self.fails;
            async move {
                observed
                    .lock()
                    .map_err(|_| std::io::Error::other("handler observation lock poisoned"))?
                    .push(task.parts.attempt.current());
                if fails {
                    Err(std::io::Error::other("handler failure"))
                } else {
                    Ok("accepted".to_owned())
                }
            }
            .boxed()
        }
    }
    // Match the real Worker boundary: PgMiddleware -> ack -> Tracker -> handler.
    #[derive(Clone)]
    struct MaybeTracked {
        inner: Handler,
        tracked: bool,
    }
    impl Service<PgTask<String>> for MaybeTracked {
        type Response = String;
        type Error = std::io::Error;
        type Future = Either<
            <Handler as Service<PgTask<String>>>::Future,
            AttemptOnPollFuture<<Handler as Service<PgTask<String>>>::Future>,
        >;
        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }
        fn call(&mut self, task: PgTask<String>) -> Self::Future {
            let attempt = task.parts.attempt.clone();
            let future = self.inner.call(task);
            if self.tracked {
                Either::Right(AttemptOnPollFuture::new(attempt, future))
            } else {
                Either::Left(future)
            }
        }
    }
    #[derive(QueryableByName)]
    struct JsonRow {
        #[diesel(sql_type = Jsonb)]
        value: Value,
    }
    async fn row(pool: apalis_diesel_postgres::PgPool) -> Result<Value, String> {
        support::with_conn(pool, |conn| {
        sql_query("SELECT jsonb_build_object('status',status,'attempts',attempts,'max_attempts',max_attempts,
          'last_result',last_result,'owner',lock_by,'locked',lock_at IS NOT NULL,'done',done_at IS NOT NULL) AS value FROM apalis.jobs")
            .get_result::<JsonRow>(conn).map(|r| r.value).map_err(|e| e.to_string())
    }).await
    }

    #[derive(Debug)]
    struct Run {
        steps: Vec<Value>,
        expected: Vec<Value>,
        handler_attempts: Vec<usize>,
        input_attempts: Vec<usize>,
        expected_handler_attempts: Vec<usize>,
    }

    async fn fixture(
        url: String,
        previous: i32,
        budget: i32,
    ) -> Result<(apalis_diesel_postgres::PgPool, PostgresStorage<String>), String> {
        let pool =
            apalis_diesel_postgres::build_pool_with(url, |b| b.max_size(2).min_idle(Some(1)))
                .map_err(|e| e.to_string())?;
        apalis_diesel_postgres::setup(&pool)
            .await
            .map_err(|e| e.to_string())?;
        let mut storage =
            PostgresStorage::<String>::new_with_config(&pool, &Config::new("attempt-queue"));
        storage
            .register_worker("attempt-worker".to_owned())
            .await
            .map_err(|e| e.to_string())?;
        support::with_conn(pool.clone(), move |conn| {
            sql_query("INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at,last_result)
              VALUES('01ARZ3NDEKTSV4RRFFQ69G5FAV','attempt-queue',convert_to('\"payload\"','UTF8'),
                CASE WHEN $1=0 THEN 'Pending' ELSE 'Failed' END,$1,$2,clock_timestamp()-interval '1second',
                CASE WHEN $1=0 THEN NULL ELSE '{\"Err\":\"previous failure\"}'::jsonb END)")
                .bind::<Integer,_>(previous).bind::<Integer,_>(budget).execute(conn).map(|_| ()).map_err(|e| e.to_string())
        }).await?;
        Ok((pool, storage))
    }

    async fn next_claim(
        stream: &mut apalis_core::backend::TaskStream<PgTask<Vec<u8>>, Error>,
    ) -> Result<PgTask<Vec<u8>>, String> {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match stream.next().await {
                    Some(Ok(Some(task))) => return Ok(task),
                    Some(Ok(None)) => {}
                    Some(Err(error)) => return Err(error.to_string()),
                    None => return Err("claim stream ended".to_owned()),
                }
            }
        })
        .await
        .map_err(|e| e.to_string())?
    }

    async fn scenario(
        entry: Entry,
        previous: i32,
        fails: bool,
        input: Input,
        budget: i32,
    ) -> Result<support::Outcome<Run>, String> {
        support::with_isolated_database(move |url| async move {
        let (pool, storage) = fixture(url, previous, budget).await?;
        let observed = Arc::new(Mutex::new(Vec::new()));
        let worker=WorkerContext::new::<()>("attempt-worker");
        let mut stream=matches!(entry,Entry::PreclaimedDirect).then(||storage.clone().poll_compact(&worker));
        let mut service = PgMiddleware::new(&pool, true).layer(MaybeTracked { inner: Handler{ fails, observed: observed.clone() }, tracked:matches!(entry,Entry::Tracked) });
        let count = if fails { budget-previous } else { 1 };
        let mut steps = Vec::new(); let mut expected = Vec::new(); let mut input_attempts = Vec::new();
        for n in 1..=count {
            let completed = previous+n;
            expected.push(json!({"result":if fails {"handler_error"} else {"ok"}, "row":{
                "status":if fails {if completed==budget {"Killed"} else {"Failed"}} else {"Done"},
                "attempts":completed,"max_attempts":budget,"last_result":if fails {json!({"Err":"handler failure"})} else {json!({"Ok":"accepted"})},
                "owner":"attempt-worker","locked":true,"done":true}}));
        }
        for _ in 0..count {
            let stored = row(pool.clone()).await?;
            let stored_attempts = stored["attempts"].as_u64().ok_or("missing stored attempts")? as usize;
            let builder = TaskBuilder::new("payload".to_owned())
                .with_task_id(PgTaskId::new(ulid::Ulid::from_string("01ARZ3NDEKTSV4RRFFQ69G5FAV").map_err(|e|e.to_string())?))
                .with_ctx(PgContext::new().with_queue("attempt-queue".to_owned()))
                .data(WorkerContext::new::<()>("attempt-worker"));
            let mut task = match input {
                Input::Default => builder.build(),
                Input::Coherent => builder.with_attempt(Attempt::new_with_value(stored_attempts)).build(),
                Input::StaleHigh => builder.with_attempt(Attempt::new_with_value(99)).build(),
            };
            if let Some(stream)=&mut stream {
                let compact=next_claim(stream).await?;
                task=compact.map(|_|"payload".to_owned());
                task.parts.data.insert(worker.clone());
            }
            input_attempts.push(task.parts.attempt.current());
            futures::future::poll_fn(|cx| service.poll_ready(cx)).await.map_err(|e|e.to_string())?;
            let result = service.call(task).await;
            let classification = match &result {
                Ok(_) => "ok",
                Err(error) if matches!(error.downcast_ref::<Error>(), Some(Error::StaleAcknowledgement{..})) => "stale_ack",
                Err(error) if error.downcast_ref::<std::io::Error>().is_some() && error.to_string()=="handler failure" => "handler_error",
                Err(_) => "other_error",
            };
            let after = row(pool.clone()).await?;
            steps.push(json!({"result":classification,"row":after}));
            if classification=="stale_ack" || classification=="other_error" { break; }
        }
        let handler_attempts = observed.lock().map_err(|e|e.to_string())?.clone();
        let expected_handler_attempts = (previous..previous+count)
            .map(|n| n as usize + usize::from(matches!(entry, Entry::Tracked))).collect();
        Ok(Run { steps, expected, handler_attempts, input_attempts, expected_handler_attempts })
    }).await
    }

    fn correct() -> impl Fn(&Result<support::Outcome<Run>, String>) -> AssertionResult {
        support::observe("public middleware attempt accounting", |r: &Run| {
            if r.steps == r.expected
                && r.handler_attempts == r.expected_handler_attempts
                && r.input_attempts.len() == r.expected.len()
            {
                Ok(())
            } else {
                Err(format!(
                    "expected steps {}; observed {}; handler attempts {:?} (expected {:?}); input attempts {:?}",
                    json!(r.expected),
                    json!(r.steps),
                    r.handler_attempts,
                    r.expected_handler_attempts,
                    r.input_attempts
                ))
            }
        })
    }
    async fn acknowledge_cloned_claim() -> Result<support::Outcome<Value>, String> {
        support::with_isolated_database(|url| async move {
            let (pool, storage) = fixture(url, 0, 3).await?;
            let worker = WorkerContext::new::<()>("attempt-worker");
            let mut stream = storage.poll_compact(&worker);
            let task = next_claim(&mut stream).await?;
            let parts = task.parts.clone();
            drop(task);
            let input_attempt = parts.attempt.current();
            let mut acknowledger = PgAck::new(&pool);
            let result: Result<String, apalis_core::error::BoxDynError> = Ok("accepted".to_owned());
            let first = acknowledger.ack(&result, &parts).await;
            let first_row = row(pool.clone()).await?;
            let second = acknowledger.ack(&result, &parts).await;
            let second_row = row(pool).await?;
            Ok(
                json!({"input_attempt":input_attempt,"first_succeeded":first.is_ok(),
                "repeated_ack_is_stale":matches!(second, Err(Error::StaleAcknowledgement{..})),
                "first_row":first_row,"second_row":second_row}),
            )
        })
        .await
    }

    fn completed_only_once() -> impl Fn(&Result<support::Outcome<Value>, String>) -> AssertionResult
    {
        support::observe("manual acknowledgement of cloned claim", |actual| {
            let row = json!({"status":"Done","attempts":1,"max_attempts":3,
                "last_result":{"Ok":"accepted"},"owner":"attempt-worker","locked":true,"done":true});
            let expected = json!({"input_attempt":0,"first_succeeded":true,
                "repeated_ack_is_stale":true,"first_row":row,"second_row":row});
            if actual == &expected {
                Ok(())
            } else {
                Err(format!("expected {expected}, observed {actual}"))
            }
        })
    }

    lets_expect! {
        #tokio_test
        expect(acknowledge_cloned_claim().await) as manual_acknowledgement {
            to completes_a_cloned_claim_once_without_external_increment { completed_only_once() }
        }
        expect(scenario(Entry::Direct, previous, fails, Input::Default, budget).await) as direct_middleware {
            let previous=0;
            let fails=false;
            let budget=3;
            to records_the_successful_execution_once { correct() }
            when the_handler_keeps_failing {
                let fails=true;
                to consumes_the_budget_and_becomes_terminal { correct() }
                when only_one_attempt_is_allowed {
                    let budget=1;
                    to becomes_terminal_after_the_first_failure { correct() }
                }
            }
            when a_previous_execution_failed {
                let previous=1;
                to acknowledges_the_next_success { correct() }
                when the_handler_keeps_failing {
                    let fails=true;
                    to consumes_the_remaining_budget { correct() }
                }
            }
            when the_next_attempt_reaches_the_largest_stored_count {
                let budget=i32::MAX;
                let previous=budget-1;
                to records_the_success_at_the_integer_boundary { correct() }
                when the_handler_fails {
                    let fails=true;
                    to becomes_terminal_at_the_integer_boundary { correct() }
                }
            }
        }
        expect(scenario(Entry::Tracked, previous, fails, Input::Coherent, budget).await) as tracked_middleware {
            let previous=0;
            let fails=false;
            let budget=3;
            to records_the_successful_execution_once { correct() }
            when the_handler_keeps_failing {
                let fails=true;
                to consumes_the_budget_and_becomes_terminal { correct() }
                when only_one_attempt_is_allowed {
                    let budget=1;
                    to becomes_terminal_after_the_first_failure { correct() }
                }
            }
            when a_previous_execution_failed {
                let previous=1;
                to acknowledges_the_next_success { correct() }
                when the_handler_keeps_failing {
                    let fails=true;
                    to consumes_the_remaining_budget { correct() }
                }
            }
        }
        expect(scenario(Entry::PreclaimedDirect, previous, fails, Input::Default, budget).await) as preclaimed_middleware {
            let previous=0;
            let fails=false;
            let budget=3;
            to records_the_successful_execution_once { correct() }
            when the_handler_keeps_failing {
                let fails=true;
                to consumes_the_budget_and_becomes_terminal { correct() }
                when only_one_attempt_is_allowed {
                    let budget=1;
                    to becomes_terminal_after_the_first_failure { correct() }
                }
            }
            when a_previous_execution_failed {
                let previous=1;
                to acknowledges_the_next_success { correct() }
                when the_handler_keeps_failing {
                    let fails=true;
                    to consumes_the_remaining_budget { correct() }
                }
            }
        }
        expect(scenario(entry, 1, false, input, 3).await) as fallback_counter_rehydration {
            let entry=Entry::Direct;
            let input=Input::Coherent;
            to acknowledges_a_coherent_input_counter { correct() }
            when the_input_counter_is_too_large {
                let input=Input::StaleHigh;
                to uses_the_database_attempt_for_acknowledgement { correct() }
            }
            when apalis_counts_the_execution {
                let entry=Entry::Tracked;
                when the_input_counter_is_default {
                    let input=Input::Default;
                    to rehydrates_a_stale_default_counter_before_acknowledging { correct() }
                }
                when the_input_counter_is_too_large {
                    let input=Input::StaleHigh;
                    to rehydrates_the_counter_before_acknowledging { correct() }
                }
            }
        }
    }
}
