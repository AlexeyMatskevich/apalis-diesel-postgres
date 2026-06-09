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
    CompactType, Config, JsonCodec, PgPool, PgTask, PostgresStorage, SharedPostgresStorage,
    build_pool_with,
};
use diesel::{QueryableByName, RunQueryDsl, sql_query, sql_types::Text};
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
    second_registration_accepted: bool,
    delivered_payload: String,
}

#[derive(Debug)]
enum Outcome<T> {
    Skipped,
    Completed(T),
}

async fn test_pool() -> Result<Option<PgPool>, String> {
    support::shared_pool().await
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
    // Broadcast redesign: a second registration on the SAME queue config is now
    // accepted (it used to be rejected). Capture the real result so the
    // assertion can genuinely fail if that ever regresses, then use the handle
    // as a second consumer that must also receive the broadcast. `SharedFetcher`
    // is not `Clone`, so the second consumer is created via a fresh
    // `make_shared_with_config` rather than by cloning the first handle.
    let second = <SharedPostgresStorage<JsonCodec<CompactType>> as MakeShared<String>>::make_shared_with_config(
        &mut shared,
        config,
    );
    let second_registration_accepted = second.is_ok();
    let Ok(poll_handle) = second else {
        cleanup_queue(pool, queue).await?;
        return Ok(Outcome::Completed(SharedDuplicateRun {
            second_registration_accepted,
            delivered_payload: String::new(),
        }));
    };
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
        second_registration_accepted,
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

fn shared_second_registration_accepted()
-> impl Fn(&Result<Outcome<SharedDuplicateRun>, String>) -> AssertionResult {
    check::<SharedDuplicateRun, _>("shared second registration", |run| {
        if run.second_registration_accepted {
            Ok(())
        } else {
            Err(
                "expected the broadcast redesign to accept a second registration for the same queue config, but make_shared_with_config rejected it"
                    .into(),
            )
        }
    })
}

// --------------------------------------------------------------------------
// shared listener returns its pooled connection without a LISTEN subscription
//
// Regression for the round-11 audit fix: every exit path of the shared
// listener thread now funnels through a single boundary that executes
// `UNLISTEN` before the pooled connection drops back into r2d2. Without it,
// the next pool user inherits the subscription and notifications accumulate
// in libpq's receive buffer with no consumer. Only the normal (empty
// registry) exit is driven here: the error exits (connection failure,
// poisoned registry) cannot be triggered deterministically from an
// integration test, and all paths share the same single cleanup boundary.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct UnlistenRun {
    /// Channels the recycled connection still listens on. Must be empty.
    leaked_channels: Vec<String>,
}

#[derive(QueryableByName)]
struct ChannelRow {
    #[diesel(sql_type = Text)]
    channel: String,
}

async fn wait_until(
    what: &'static str,
    deadline: Duration,
    predicate: impl Fn() -> bool,
) -> Result<(), String> {
    let started = std::time::Instant::now();
    while !predicate() {
        if started.elapsed() > deadline {
            return Err(format!("timed out waiting for {what}"));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(())
}

async fn run_listener_recycles_a_clean_connection() -> Result<Outcome<UnlistenRun>, String> {
    // Consult the shared pool first so migrations have run and the
    // DATABASE_URL-skip path stays uniform with the other scenarios…
    if test_pool().await?.is_none() {
        return Ok(Outcome::Skipped);
    }
    // …but observe the listener through a dedicated single-connection pool:
    // with `max_size = 1` the connection handed back by `pool.get()` below is
    // the very connection the listener thread used for LISTEN.
    let url = support::database_url_or_skip()?
        .ok_or_else(|| "DATABASE_URL vanished between checks".to_owned())?;
    let pool = build_pool_with(url, |builder| builder.max_size(1).min_idle(Some(0)))
        .map_err(|e| e.to_string())?;

    let queue = format!("apalis-shared-unlisten-{}", Ulid::new());
    let mut shared: SharedPostgresStorage<JsonCodec<CompactType>> =
        SharedPostgresStorage::new(pool.clone());
    let storage = <SharedPostgresStorage<JsonCodec<CompactType>> as MakeShared<String>>::make_shared_with_config(
        &mut shared,
        notify_only_config(&queue),
    )
    .map_err(|e| e.to_string())?;

    // The listener thread claims the pool's only connection and LISTENs.
    wait_until(
        "the shared listener to claim the pool's only connection",
        Duration::from_secs(5),
        || pool.state().connections == 1,
    )
    .await?;

    // Dropping the only registration empties the registry; the listener
    // notices within its poll interval, exits, and recycles the connection.
    drop(storage);
    wait_until(
        "the shared listener to exit and return its connection to the pool",
        Duration::from_secs(5),
        || pool.state().idle_connections == 1,
    )
    .await?;

    let check_pool = pool.clone();
    let leaked_channels = tokio::task::spawn_blocking(move || {
        let mut conn = check_pool.get().map_err(|e| e.to_string())?;
        sql_query("SELECT channel FROM pg_listening_channels() AS t(channel)")
            .load::<ChannelRow>(&mut conn)
            .map(|rows| rows.into_iter().map(|row| row.channel).collect::<Vec<_>>())
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())??;

    Ok(Outcome::Completed(UnlistenRun { leaked_channels }))
}

fn recycled_connection_has_no_subscriptions()
-> impl Fn(&Result<Outcome<UnlistenRun>, String>) -> AssertionResult {
    check::<UnlistenRun, _>("recycled connection subscriptions", |run| {
        if run.leaked_channels.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "the connection returned to the pool still LISTENs on {:?}",
                run.leaked_channels
            ))
        }
    })
}

lets_expect! { #tokio_test
    expect(run_listener_recycles_a_clean_connection().await) {
        when the_last_registration_drops_and_the_listener_exits {
            to returns_the_connection_without_a_listen_subscription {
                recycled_connection_has_no_subscriptions()
            }
        }
    }

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
        when shared_storage_serves_one_queue_and_a_second_consumer_registers_on_it {
            to delivers_the_pushed_payload_to_the_second_consumer {
                shared_delivery_payload_matches("shared-payload")
            }
            to accepts_the_second_registration_for_the_same_queue {
                shared_second_registration_accepted()
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
