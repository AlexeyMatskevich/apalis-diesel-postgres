//! Completion and failure of the buffered public Sink, on either runtime.
mod support;
// This shared wire fixture also exposes LISTEN observations for other suites.
#[allow(dead_code)]
#[path = "support/commit_proxy.rs"]
mod commit_proxy;

use apalis_core::{backend::FetchById, task::builder::TaskBuilder};
use apalis_diesel_postgres::{
    Config, Error, PgContext, PgPool, PgTaskId, PostgresStorage, build_pool_with,
};
use diesel::{
    RunQueryDsl, sql_query,
    sql_types::{Bool, Integer, Text},
};
use futures::{Sink, SinkExt};
use lets_expect::*;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

type Storage = PostgresStorage<serde_json::Value>;

fn on_runtime<F: Future + 'static>(future: F) -> F::Output
where
    F::Output: 'static,
{
    let bounded = async move {
        match futures::future::select(
            Box::pin(future),
            Box::pin(apalis_core::timer::sleep(Duration::from_secs(15))),
        )
        .await
        {
            futures::future::Either::Left((result, _)) => result,
            futures::future::Either::Right(_) => {
                panic!("Sink scenario did not finish before its failure deadline")
            }
        }
    };
    #[cfg(feature = "tokio")]
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime starts")
            .block_on(bounded)
    }
    #[cfg(all(feature = "ntex", not(feature = "tokio")))]
    {
        ntex::rt::System::build()
            .build(ntex::rt::DefaultRuntime)
            .block_on(bounded)
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Flush,
    Close,
    Ready,
    Retype,
    Clone,
}

#[derive(Debug)]
struct FailureObservation {
    first_conflict: bool,
    stored_none: bool,
    next_expected: bool,
}

async fn failed_batch(
    operation: Operation,
) -> Result<support::Outcome<FailureObservation>, String> {
    let Some(pool) = support::shared_pool().await? else {
        return Ok(support::Outcome::Skipped);
    };
    let queue = format!("sink-failure-{}", ulid::Ulid::new());
    let config = Config::new(&queue).set_buffer_size(3);
    let mut storage = Storage::new_with_config(&pool, &config);
    let first_id = PgTaskId::new(ulid::Ulid::new());
    let second_id = PgTaskId::new(ulid::Ulid::new());
    let task = |id| {
        TaskBuilder::new(b"{}".to_vec())
            .with_task_id(id)
            .with_idempotency_key("same-key")
            .with_ctx(PgContext::new().with_queue(queue.clone()))
            .build()
    };
    storage
        .feed(task(first_id))
        .await
        .map_err(|e| e.to_string())?;
    storage
        .feed(task(second_id))
        .await
        .map_err(|e| e.to_string())?;
    let first_conflict = matches!(
        storage.flush().await,
        Err(Error::IdempotencyConflict { total: 2, .. })
    );
    let stored_none = storage
        .fetch_by_id(&first_id)
        .await
        .map_err(|e| e.to_string())?
        .is_none()
        && storage
            .fetch_by_id(&second_id)
            .await
            .map_err(|e| e.to_string())?
            .is_none();
    let subsequent = match operation {
        Operation::Flush => storage.flush().await,
        Operation::Close => storage.close().await,
        Operation::Ready => {
            futures::future::poll_fn(|cx| Pin::new(&mut storage).poll_ready(cx)).await
        }
        Operation::Retype => storage.with_codec::<()>().flush().await,
        Operation::Clone => storage.clone().flush().await,
    };
    let next_expected = if matches!(operation, Operation::Clone) {
        subsequent.is_ok()
    } else {
        matches!(subsequent, Err(Error::SinkFailed))
    };
    Ok(support::Outcome::Completed(FailureObservation {
        first_conflict,
        stored_none,
        next_expected,
    }))
}

fn retains_failure()
-> impl Fn(&Result<support::Outcome<FailureObservation>, String>) -> AssertionResult {
    support::observe("failed batch ownership", |r: &FailureObservation| {
        if r.first_conflict && r.stored_none && r.next_expected {
            Ok(())
        } else {
            Err(format!("batch rollback and subsequent disposition: {r:?}"))
        }
    })
}

#[derive(Debug)]
struct BoundaryObservation {
    first_observed: bool,
    next_expected: bool,
    durable: bool,
}

async fn completion_boundary(
    cancel_wait: bool,
) -> Result<support::Outcome<BoundaryObservation>, String> {
    let Some(direct) = support::shared_pool().await? else {
        return Ok(support::Outcome::Skipped);
    };
    let url = support::database_url_or_skip()?.ok_or("database was required")?;
    let proxy = commit_proxy::CommitProxy::new(&url)?;
    let pool = build_pool_with(proxy.url(), |builder| builder.max_size(1).min_idle(Some(0)))
        .map_err(|e| e.to_string())?;
    let queue = format!("sink-boundary-{}", ulid::Ulid::new());
    let config = Config::new(&queue);
    let mut storage = Storage::new_with_config(&pool, &config);
    let id = PgTaskId::new(ulid::Ulid::new());
    // A keyed batch takes an explicit transaction, whose COMMIT the proxy observes.
    let task = TaskBuilder::new(b"{}".to_vec())
        .with_task_id(id)
        .with_idempotency_key("boundary")
        .with_ctx(PgContext::new().with_queue(queue))
        .build();
    storage.feed(task).await.map_err(|e| e.to_string())?;
    let first_observed;
    let next_expected;
    if cancel_wait {
        let holder = pool.get().map_err(|e| e.to_string())?;
        let mut waiting = Box::pin(storage.flush());
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        first_observed = waiting.as_mut().poll(&mut cx).is_pending();
        drop(waiting);
        drop(holder);
        next_expected = storage.flush().await.is_ok();
    } else {
        proxy.arm();
        first_observed = storage.flush().await.is_err() && proxy.committed();
        next_expected = matches!(storage.flush().await, Err(Error::SinkFailed));
    }
    let mut reader = Storage::new_with_config(&direct, &config);
    let durable = reader
        .fetch_by_id(&id)
        .await
        .map_err(|e| e.to_string())?
        .is_some_and(|task| {
            task.parts.attempt.current() == 0 && task.parts.status.load().to_string() == "Pending"
        });
    sql_query("DELETE FROM apalis.jobs WHERE id=$1")
        .bind::<Text, _>(id.to_string())
        .execute(&mut direct.get().map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    drop(storage);
    drop(pool);
    drop(proxy);
    Ok(support::Outcome::Completed(BoundaryObservation {
        first_observed,
        next_expected,
        durable,
    }))
}

fn retains_completion()
-> impl Fn(&Result<support::Outcome<BoundaryObservation>, String>) -> AssertionResult {
    support::observe("sink completion boundary", |r: &BoundaryObservation| {
        if r.first_observed && r.next_expected && r.durable {
            Ok(())
        } else {
            Err(format!("completion and durability: {r:?}"))
        }
    })
}

#[derive(Debug, PartialEq, Eq, diesel::QueryableByName)]
struct StoredRow {
    #[diesel(sql_type=Text)]
    id: String,
    #[diesel(sql_type=Text)]
    status: String,
    #[diesel(sql_type=Integer)]
    attempts: i32,
    #[diesel(sql_type=Bool)]
    unowned: bool,
}

async fn rows(pool: PgPool, queue: String) -> Result<Vec<StoredRow>, String> {
    support::with_conn(pool,move|conn|sql_query(
        "SELECT id,status,attempts,lock_by IS NULL AND lock_at IS NULL AS unowned FROM apalis.jobs WHERE job_type=$1 ORDER BY id"
    ).bind::<Text,_>(queue).load(conn).map_err(|e|e.to_string())).await
}

#[derive(Debug)]
struct InterleavingObservation {
    initial_pending: bool,
    first: Vec<StoredRow>,
    second: Vec<StoredRow>,
    expected: Vec<String>,
}

async fn interleaved_submission() -> Result<support::Outcome<InterleavingObservation>, String> {
    let Some(reader) = support::shared_pool().await? else {
        return Ok(support::Outcome::Skipped);
    };
    let url = support::database_url_or_skip()?.ok_or("database was required")?;
    let pool =
        build_pool_with(url, |b| b.max_size(1).min_idle(Some(0))).map_err(|e| e.to_string())?;
    let queue = format!("sink-interleaving-{}", ulid::Ulid::new());
    let config = Config::new(&queue).set_buffer_size(2);
    let a = PgTaskId::new(ulid::Ulid::new());
    let b = PgTaskId::new(ulid::Ulid::new());
    let task = |id| {
        TaskBuilder::new(b"{}".to_vec())
            .with_task_id(id)
            .with_ctx(PgContext::new().with_queue(queue.clone()))
            .build()
    };
    let mut storage = Storage::new_with_config(&pool, &config);
    let holder = pool.get().map_err(|e| e.to_string())?;
    storage.feed(task(a)).await.map_err(|e| e.to_string())?;
    // Reserve capacity for B before beginning a flush of A. Every start_send
    // is preceded by a successful poll_ready, as the public Sink requires.
    futures::future::poll_fn(|cx| Pin::new(&mut storage).poll_ready(cx))
        .await
        .map_err(|e| e.to_string())?;
    let initial_pending =
        futures::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut storage).poll_flush(cx)))
            .await
            .is_pending();
    Pin::new(&mut storage)
        .start_send(task(b))
        .map_err(|e| e.to_string())?;
    drop(holder);
    storage.flush().await.map_err(|e| e.to_string())?;
    let first = rows(reader.clone(), queue.clone()).await?;
    // A second flush must leave the committed tasks unchanged. The first
    // observation already requires both accepted tasks to be durable.
    storage.flush().await.map_err(|e| e.to_string())?;
    let second = rows(reader.clone(), queue.clone()).await?;
    let mut expected = vec![a.to_string(), b.to_string()];
    expected.sort();
    support::with_conn(reader, move |conn| {
        sql_query("DELETE FROM apalis.jobs WHERE job_type=$1")
            .bind::<Text, _>(queue)
            .execute(conn)
            .map(|_| ())
            .map_err(|e| e.to_string())
    })
    .await?;
    Ok(support::Outcome::Completed(InterleavingObservation {
        initial_pending,
        first,
        second,
        expected,
    }))
}

fn completes_every_accepted_task()
-> impl Fn(&Result<support::Outcome<InterleavingObservation>, String>) -> AssertionResult {
    support::observe(
        "all accepted tasks are flushed",
        |r: &InterleavingObservation| {
            if r.initial_pending
                && r.first.iter().map(|row| row.id.clone()).collect::<Vec<_>>() == r.expected
                && r.first
                    .iter()
                    .all(|row| row.status == "Pending" && row.attempts == 0 && row.unowned)
                && r.second == r.first
            {
                Ok(())
            } else {
                Err(format!("completion before all accepted tasks: {r:?}"))
            }
        },
    )
}

lets_expect! {
    expect(on_runtime(failed_batch(operation))) as failed_batch_completion {
        let operation = Operation::Flush;
        to preserves_the_failure { retains_failure() }
        when the_sink_is_closed { let operation = Operation::Close;
            to preserves_the_failure { retains_failure() }
        }
        when readiness_is_requested { let operation = Operation::Ready;
            to preserves_the_failure { retains_failure() }
        }
        when the_codec_is_changed { let operation = Operation::Retype;
            to preserves_the_failure { retains_failure() }
        }
        when a_fresh_sink_is_cloned { let operation = Operation::Clone;
            to starts_an_empty_pipeline { retains_failure() }
        }
    }
    expect(on_runtime(completion_boundary(cancel_wait))) as sink_completion_boundary {
        let cancel_wait = true;
        to completes_the_batch_after_waiting_is_cancelled { retains_completion() }
        when the_commit_response_is_lost { let cancel_wait = false;
            to preserves_the_failure_after_the_durable_commit { retains_completion() }
        }
    }
    expect(on_runtime(interleaved_submission())) as accepted_tasks_during_an_ongoing_flush {
        to persists_every_accepted_task_before_reporting_completion { completes_every_accepted_task() }
    }
}
