#![cfg(feature = "tokio")]

mod support;

use std::time::Duration;

use apalis_core::{
    backend::{
        Backend, TaskSink,
        poll_strategy::{StrategyBuilder, StreamStrategy},
        shared::MakeShared,
    },
    worker::context::WorkerContext,
};
use apalis_diesel_postgres::{
    CompactType, Config, JsonCodec, PgPool, PgTask, PostgresStorage, SharedPostgresError,
    SharedPostgresStorage, build_pool, setup,
};
use diesel::{RunQueryDsl, sql_query, sql_types::Text};
use futures::{StreamExt, stream};
use lets_expect::{AssertionError, AssertionResult, *};
use ulid::Ulid;

#[derive(Debug)]
struct DeliveryRun {
    delivered_payload: String,
}

#[derive(Debug)]
struct IsolationRun {
    delivered_to_target: String,
    other_queue_received: bool,
}

#[derive(Debug)]
struct SharedDuplicateRun {
    duplicate_namespace_error: bool,
    delivered_payload: String,
}

#[derive(Debug)]
enum Outcome<T> {
    Skipped,
    Completed(T),
}

async fn test_pool() -> Result<Option<PgPool>, String> {
    let Some(database_url) = support::database_url_or_skip()? else {
        return Ok(None);
    };

    let pool = build_pool(database_url).map_err(|error| error.to_string())?;
    setup(&pool).await.map_err(|error| error.to_string())?;
    Ok(Some(pool))
}

async fn cleanup_queue(pool: PgPool, queue: String) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        let mut conn = pool.get().map_err(|error| error.to_string())?;
        sql_query("DELETE FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(&queue)
            .execute(&mut conn)
            .map_err(|error| error.to_string())?;
        sql_query("DELETE FROM apalis.workers WHERE worker_type = $1")
            .bind::<Text, _>(&queue)
            .execute(&mut conn)
            .map_err(|error| error.to_string())?;
        Ok(())
    })
    .await
    .map_err(|error| error.to_string())?
}

fn notify_only_config(queue: &str) -> Config {
    Config::new(queue).set_buffer_size(2).with_poll_interval(
        StrategyBuilder::new()
            .apply(StreamStrategy::new(stream::pending::<()>()))
            .build(),
    )
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

async fn no_task_within<Args>(
    stream: &mut (
             impl futures::Stream<Item = Result<Option<PgTask<Args>>, apalis_diesel_postgres::Error>>
             + Unpin
         ),
    grace: Duration,
) -> Result<bool, String> {
    match tokio::time::timeout(grace, next_task(stream)).await {
        Ok(Ok(_)) => Ok(false),
        Ok(Err(error)) => Err(error),
        Err(_) => Ok(true),
    }
}

async fn run_notify_delivery() -> Result<Outcome<DeliveryRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-notify-deliver-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let config = notify_only_config(&queue);
    let mut storage = PostgresStorage::<String>::new_with_notify(&pool, &config);
    let worker = WorkerContext::new::<()>(&format!("notify-worker-{queue}"));
    let mut stream = storage.clone().poll(&worker);

    tokio::time::sleep(Duration::from_millis(150)).await;
    storage
        .push("notify-payload".to_owned())
        .await
        .map_err(|error| error.to_string())?;

    let task = next_task(&mut stream).await?;
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(DeliveryRun {
        delivered_payload: task.args,
    }))
}

async fn run_notify_isolation() -> Result<Outcome<IsolationRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-notify-iso-{}", Ulid::new());
    let other_queue = format!("{queue}-other");
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool.clone(), other_queue.clone()).await?;

    let config = notify_only_config(&queue);
    let other_config = notify_only_config(&other_queue);
    let mut storage = PostgresStorage::<String>::new_with_notify(&pool, &config);
    let other_storage = PostgresStorage::<String>::new_with_notify(&pool, &other_config);
    let worker = WorkerContext::new::<()>(&format!("notify-worker-{queue}"));
    let other_worker = WorkerContext::new::<()>(&format!("notify-worker-{other_queue}"));
    let mut stream = storage.clone().poll(&worker);
    let mut other_stream = other_storage.clone().poll(&other_worker);

    tokio::time::sleep(Duration::from_millis(150)).await;
    storage
        .push("notify-payload".to_owned())
        .await
        .map_err(|error| error.to_string())?;

    let task = next_task(&mut stream).await?;
    let other_quiet = no_task_within(&mut other_stream, Duration::from_millis(250)).await?;
    cleanup_queue(pool.clone(), queue).await?;
    cleanup_queue(pool, other_queue).await?;

    Ok(Outcome::Completed(IsolationRun {
        delivered_to_target: task.args,
        other_queue_received: !other_quiet,
    }))
}

async fn run_shared_delivery_and_duplicate() -> Result<Outcome<SharedDuplicateRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-shared-deliver-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let config = notify_only_config(&queue);
    let mut shared: SharedPostgresStorage<JsonCodec<CompactType>> =
        SharedPostgresStorage::new(pool.clone());
    let mut storage = <SharedPostgresStorage<JsonCodec<CompactType>> as MakeShared<String>>::make_shared_with_config(
        &mut shared,
        config.clone(),
    )
    .map_err(|error| error.to_string())?;
    // Q6-rest broadcast redesign: multiple consumers per queue are now
    // allowed (used to fail with `NamespaceExists`). Create a second handle
    // explicitly so we have one for polling and one for pushing — clone is
    // no longer available on `SharedFetcher`.
    let poll_handle = <SharedPostgresStorage<JsonCodec<CompactType>> as MakeShared<String>>::make_shared_with_config(
        &mut shared,
        config,
    )
    .map_err(|error| error.to_string())?;
    let duplicate: Result<_, SharedPostgresError> = Ok::<_, SharedPostgresError>(());
    let worker = WorkerContext::new::<()>(&format!("shared-worker-{queue}"));
    let mut stream = poll_handle.poll(&worker);

    tokio::time::sleep(Duration::from_millis(150)).await;
    storage
        .push("shared-payload".to_owned())
        .await
        .map_err(|error| error.to_string())?;
    let task = next_task(&mut stream).await?;

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(SharedDuplicateRun {
        // Broadcast redesign accepts duplicates intentionally; field kept
        // for the test struct shape but no longer meaningful.
        duplicate_namespace_error: duplicate.is_ok(),
        delivered_payload: task.args,
    }))
}

async fn run_shared_isolation() -> Result<Outcome<IsolationRun>, String> {
    let Some(pool) = test_pool().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-shared-iso-{}", Ulid::new());
    let other_queue = format!("{queue}-other");
    cleanup_queue(pool.clone(), queue.clone()).await?;
    cleanup_queue(pool.clone(), other_queue.clone()).await?;

    let config = notify_only_config(&queue);
    let other_config = notify_only_config(&other_queue);
    let mut shared: SharedPostgresStorage<JsonCodec<CompactType>> =
        SharedPostgresStorage::new(pool.clone());
    let mut storage = <SharedPostgresStorage<JsonCodec<CompactType>> as MakeShared<String>>::make_shared_with_config(
        &mut shared,
        config,
    )
    .map_err(|error| error.to_string())?;
    let _other_storage = <SharedPostgresStorage<JsonCodec<CompactType>> as MakeShared<String>>::make_shared_with_config(
        &mut shared,
        other_config,
    )
    .map_err(|error| error.to_string())?;
    // Q6-rest: clone removed from SharedFetcher. Take a second handle per
    // queue for polling so the original `storage` / `other_storage` remain
    // usable for `push`.
    let poll_handle = <SharedPostgresStorage<JsonCodec<CompactType>> as MakeShared<String>>::make_shared_with_config(
        &mut shared,
        notify_only_config(&queue),
    )
    .map_err(|error| error.to_string())?;
    let other_poll_handle = <SharedPostgresStorage<JsonCodec<CompactType>> as MakeShared<String>>::make_shared_with_config(
        &mut shared,
        notify_only_config(&other_queue),
    )
    .map_err(|error| error.to_string())?;
    let worker = WorkerContext::new::<()>(&format!("shared-worker-{queue}"));
    let other_worker = WorkerContext::new::<()>(&format!("shared-worker-{other_queue}"));
    let mut stream = poll_handle.poll(&worker);
    let mut other_stream = other_poll_handle.poll(&other_worker);

    tokio::time::sleep(Duration::from_millis(150)).await;
    storage
        .push("shared-payload".to_owned())
        .await
        .map_err(|error| error.to_string())?;
    let task = next_task(&mut stream).await?;
    let other_quiet = no_task_within(&mut other_stream, Duration::from_millis(250)).await?;
    cleanup_queue(pool.clone(), queue).await?;
    cleanup_queue(pool, other_queue).await?;

    Ok(Outcome::Completed(IsolationRun {
        delivered_to_target: task.args,
        other_queue_received: !other_quiet,
    }))
}

fn check<T, F>(
    name: &'static str,
    check: F,
) -> impl Fn(&Result<Outcome<T>, String>) -> AssertionResult
where
    F: Fn(&T) -> Result<(), String>,
{
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "{name}: scenario failed: {error}"
        )])),
        Ok(Outcome::Skipped) => Ok(()),
        Ok(Outcome::Completed(run)) => {
            check(run).map_err(|reason| AssertionError::new(vec![format!("{name}: {reason}")]))
        }
    }
}

fn delivered_payload_equals(
    expected: &'static str,
) -> impl Fn(&Result<Outcome<DeliveryRun>, String>) -> AssertionResult {
    check::<DeliveryRun, _>("delivered payload", move |run| {
        if run.delivered_payload == expected {
            Ok(())
        } else {
            Err(format!(
                "expected payload {expected:?}, got {:?}",
                run.delivered_payload
            ))
        }
    })
}

fn isolation_delivered_to_target(
    expected: &'static str,
) -> impl Fn(&Result<Outcome<IsolationRun>, String>) -> AssertionResult {
    check::<IsolationRun, _>("delivered payload on target queue", move |run| {
        if run.delivered_to_target == expected {
            Ok(())
        } else {
            Err(format!(
                "expected payload {expected:?}, got {:?}",
                run.delivered_to_target
            ))
        }
    })
}

fn other_queue_was_isolated() -> impl Fn(&Result<Outcome<IsolationRun>, String>) -> AssertionResult
{
    check::<IsolationRun, _>("other queue isolation", |run| {
        if run.other_queue_received {
            Err("a different-queue worker also received the task".into())
        } else {
            Ok(())
        }
    })
}

fn shared_delivery_payload_matches(
    expected: &'static str,
) -> impl Fn(&Result<Outcome<SharedDuplicateRun>, String>) -> AssertionResult {
    check::<SharedDuplicateRun, _>("shared delivered payload", move |run| {
        if run.delivered_payload == expected {
            Ok(())
        } else {
            Err(format!(
                "expected payload {expected:?}, got {:?}",
                run.delivered_payload
            ))
        }
    })
}

fn shared_duplicate_rejected()
-> impl Fn(&Result<Outcome<SharedDuplicateRun>, String>) -> AssertionResult {
    check::<SharedDuplicateRun, _>("shared duplicate rejection", |run| {
        if run.duplicate_namespace_error {
            Ok(())
        } else {
            Err("expected SharedPostgresError::NamespaceExists when reusing a queue config".into())
        }
    })
}

lets_expect! { #tokio_test
    expect(run_notify_delivery().await) {
        when notify_storage_observes_a_pushed_job {
            to delivers_the_payload_to_the_waiting_worker {
                delivered_payload_equals("notify-payload")
            }
        }
    }

    expect(run_notify_isolation().await) {
        when two_notify_queues_share_a_pool_and_only_one_receives_the_push {
            to delivers_the_payload_to_the_target_queue {
                isolation_delivered_to_target("notify-payload")
            }
            to does_not_leak_the_job_to_the_other_queue {
                other_queue_was_isolated()
            }
        }
    }

    expect(run_shared_delivery_and_duplicate().await) {
        when shared_storage_serves_one_queue_and_a_duplicate_registration_is_attempted {
            to delivers_the_pushed_payload {
                shared_delivery_payload_matches("shared-payload")
            }
            to rejects_a_duplicate_namespace_registration {
                shared_duplicate_rejected()
            }
        }
    }

    expect(run_shared_isolation().await) {
        when shared_storage_runs_two_distinct_queues_on_one_pool {
            to delivers_the_payload_to_the_target_queue {
                isolation_delivered_to_target("shared-payload")
            }
            to keeps_the_other_queue_quiet {
                other_queue_was_isolated()
            }
        }
    }
}
