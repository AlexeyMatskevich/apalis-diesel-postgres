//! Exhaustive specification for `src/queries/notify.rs` and the companion
//! `src/notify_event.rs` decoder.
//!
//! The functions under test are `pub(crate)`, so an integration test crate
//! cannot call them directly. Two complementary observation strategies are
//! used:
//!
//!   1. A raw `LISTEN "apalis::job::insert"` on a pinned pooled connection
//!      observes the NOTIFY payloads emitted by the statement-level trigger
//!      (`migrations/…notify_new_jobs_fast_path`). This pins payload shape
//!      and the trigger's row-filtering contract that `InsertEvent` parses.
//!
//!   2. An end-to-end fetch via `PostgresStorage::new_with_notify`'s stream
//!      observes that hand-crafted payloads (legacy `{job_type, id}` shape,
//!      cross-queue payloads) flow through the listener and surface as
//!      `PgTask`s — exercising the `InsertEvent` decoder + queue-filter
//!      branches in `notify_task_ids`.
//!
//! Behaviour already covered elsewhere is not re-tested here:
//!   - notify-driven delivery, queue isolation → `postgres_notify_shared`
//!   - UNLISTEN after `NotifyTaskIds` drop → `postgres_specs::run_unlisten_after_drop`
//!   - malformed payload skipped on the polling stream
//!     → `postgres_queries::run_poll_decode_notify`
//!
//! Tests gate on `DATABASE_URL`; without it every scenario resolves to
//! `Outcome::Skipped` and the assertions pass.

#![cfg(feature = "tokio")]

mod support;

use std::time::Duration;

use apalis_core::{backend::Backend, worker::context::WorkerContext};
use apalis_diesel_postgres::{Config, PgPool, PgTask, PostgresStorage, build_pool_with, setup};
use diesel::{PgConnection, RunQueryDsl, sql_query, sql_types::Text};
use futures::StreamExt;
use lets_expect::{AssertionError, AssertionResult, *};
use serde_json::Value;
use ulid::Ulid;

// --------------------------------------------------------------------------
// shared scaffolding
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

/// Build a pool with at least two connections so the LISTEN observer and the
/// INSERT/`pg_notify` writer can run on independent connections.
async fn test_pool_multi() -> Result<Option<PgPool>, String> {
    let Some(database_url) = support::database_url_or_skip()? else {
        return Ok(None);
    };
    // Need at least 2 simultaneous connections: one pinned to the LISTEN
    // observer thread, one for the writer. `min_idle = 0` keeps idle
    // connections from piling up across many parallel test scenarios.
    let pool = build_pool_with(database_url, |b| b.max_size(3).min_idle(Some(0)))
        .map_err(|e| e.to_string())?;
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

// --------------------------------------------------------------------------
// trigger payload observation: capture NOTIFY messages on a pinned listener
// --------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ObservedNotify {
    payload: String,
}

/// Run `setup` on a pinned listener connection, INSERT job rows on a
/// separate connection, then drain `notifications_iter` for up to `deadline`
/// and return the captured payloads (limited to `max_payloads`).
async fn capture_trigger_notifies<F>(
    pool: PgPool,
    setup_writer: F,
    max_payloads: usize,
    deadline: Duration,
) -> Result<Vec<ObservedNotify>, String>
where
    F: FnOnce(&mut PgConnection) -> Result<(), String> + Send + 'static,
{
    // Take the listener connection synchronously and hold it for the
    // duration of the test on a blocking thread. The writer runs on a
    // separate pool checkout so the listener never serializes against the
    // INSERT.
    let listener_pool = pool.clone();
    let writer_pool = pool.clone();

    tokio::task::spawn_blocking(move || -> Result<Vec<ObservedNotify>, String> {
        let mut listener = listener_pool.get().map_err(|e| e.to_string())?;
        sql_query("LISTEN \"apalis::job::insert\"")
            .execute(&mut *listener)
            .map_err(|e| e.to_string())?;

        // Drain any leftover notifications from prior tests that may have
        // arrived between checkout and LISTEN.
        for _ in listener.notifications_iter().flatten() {}

        // Run writer on a *different* pooled connection.
        {
            let mut writer = writer_pool.get().map_err(|e| e.to_string())?;
            setup_writer(&mut writer)?;
        }

        // Poll for notifications until we either reach `max_payloads` or the
        // deadline elapses with no new payload in the last poll interval.
        let started = std::time::Instant::now();
        let mut collected: Vec<ObservedNotify> = Vec::new();
        while started.elapsed() < deadline && collected.len() < max_payloads {
            let mut any = false;
            for notif in listener.notifications_iter() {
                let notif = notif.map_err(|e| e.to_string())?;
                if notif.channel == "apalis::job::insert" {
                    collected.push(ObservedNotify {
                        payload: notif.payload,
                    });
                    any = true;
                    if collected.len() >= max_payloads {
                        break;
                    }
                }
            }
            if !any {
                std::thread::sleep(Duration::from_millis(25));
            }
        }

        // Best-effort UNLISTEN before the connection returns to the pool.
        let _ = sql_query("UNLISTEN \"apalis::job::insert\"").execute(&mut *listener);
        Ok(collected)
    })
    .await
    .map_err(|e| e.to_string())?
}

// --------------------------------------------------------------------------
// scenarios driving the trigger
// --------------------------------------------------------------------------

#[derive(Debug)]
struct TriggerRun {
    /// All payloads observed during the deadline window, filtered to those
    /// whose `job_type` field equals the test queue.
    payloads: Vec<Value>,
    /// Number of NOTIFY messages observed for *other* queues (cross-talk
    /// sanity check; informational — not currently asserted).
    #[allow(dead_code)]
    other_queue_payloads: usize,
}

fn parse_payloads(observed: Vec<ObservedNotify>, queue: &str) -> TriggerRun {
    let mut payloads = Vec::new();
    let mut other = 0;
    for ObservedNotify { payload } in observed {
        match serde_json::from_str::<Value>(&payload) {
            Ok(v) => {
                if v.get("job_type").and_then(Value::as_str) == Some(queue) {
                    payloads.push(v);
                } else {
                    other += 1;
                }
            }
            Err(_) => {
                // Unparseable payload (e.g. the empty drop-wakeup NOTIFY).
                // Ignore — it cannot belong to our queue.
            }
        }
    }
    TriggerRun {
        payloads,
        other_queue_payloads: other,
    }
}

async fn run_single_row_insert() -> Result<Outcome<TriggerRun>, String> {
    let Some(pool) = test_pool_multi().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-notify-single-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let queue_for_writer = queue.clone();
    let observed = capture_trigger_notifies(
        pool.clone(),
        move |conn| {
            sql_query(
                "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at)
                 VALUES ($1, $2, $3, 'Pending', 0, 3, now() - INTERVAL '1 second')",
            )
            .bind::<Text, _>(Ulid::new().to_string())
            .bind::<Text, _>(&queue_for_writer)
            .bind::<diesel::sql_types::Binary, _>(serde_json::to_vec("x").unwrap())
            .execute(conn)
            .map_err(|e| e.to_string())?;
            Ok(())
        },
        4,
        Duration::from_millis(750),
    )
    .await?;

    let run = parse_payloads(observed, &queue);
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(run))
}

async fn run_future_dated_insert() -> Result<Outcome<TriggerRun>, String> {
    let Some(pool) = test_pool_multi().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-notify-future-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let queue_for_writer = queue.clone();
    let observed = capture_trigger_notifies(
        pool.clone(),
        move |conn| {
            sql_query(
                "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at)
                 VALUES ($1, $2, $3, 'Pending', 0, 3, now() + INTERVAL '1 hour')",
            )
            .bind::<Text, _>(Ulid::new().to_string())
            .bind::<Text, _>(&queue_for_writer)
            .bind::<diesel::sql_types::Binary, _>(serde_json::to_vec("x").unwrap())
            .execute(conn)
            .map_err(|e| e.to_string())?;
            Ok(())
        },
        4,
        // Shorter window because we expect *no* payloads — only need to wait
        // long enough that any NOTIFY would have been delivered.
        Duration::from_millis(400),
    )
    .await?;

    let run = parse_payloads(observed, &queue);
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(run))
}

async fn run_batch_insert_chunks() -> Result<Outcome<TriggerRun>, String> {
    let Some(pool) = test_pool_multi().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-notify-batch-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    let queue_for_writer = queue.clone();
    // 150 rows in a single INSERT → trigger fast-path branches to the
    // chunked aggregate (chunk size 100), yielding two NOTIFY payloads.
    let observed = capture_trigger_notifies(
        pool.clone(),
        move |conn| {
            // Build a multi-row INSERT via generate_series so all rows land
            // in one statement and thus share one `new_jobs` transition table.
            sql_query(
                "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at)
                 SELECT
                     gen_random_uuid()::text || '-' || g::text,
                     $1,
                     '\"x\"'::bytea,
                     'Pending',
                     0,
                     3,
                     now() - INTERVAL '1 second'
                 FROM generate_series(1, 150) g",
            )
            .bind::<Text, _>(&queue_for_writer)
            .execute(conn)
            .map_err(|e| e.to_string())?;
            Ok(())
        },
        8,
        Duration::from_millis(1500),
    )
    .await?;

    let run = parse_payloads(observed, &queue);
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(run))
}

async fn run_on_conflict_no_notify() -> Result<Outcome<TriggerRun>, String> {
    let Some(pool) = test_pool_multi().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-notify-conflict-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Pre-seed a row, drain its NOTIFY, then issue an INSERT ... ON CONFLICT
    // DO NOTHING with the *same id*. The transition table for the conflict
    // case is empty, so the trigger's `COUNT(*) = 0` early-return path
    // fires and no NOTIFY is emitted.
    let existing_id = Ulid::new().to_string();
    let queue_seed = queue.clone();
    let seed_id = existing_id.clone();
    with_conn(pool.clone(), move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at)
             VALUES ($1, $2, $3, 'Pending', 0, 3, now() - INTERVAL '1 second')",
        )
        .bind::<Text, _>(&seed_id)
        .bind::<Text, _>(&queue_seed)
        .bind::<diesel::sql_types::Binary, _>(serde_json::to_vec("x").unwrap())
        .execute(conn)
        .map_err(|e| e.to_string())
        .map(|_| ())
    })
    .await?;
    // Give the seed's NOTIFY time to flush out of any session buffer.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let queue_for_writer = queue.clone();
    let conflict_id = existing_id;
    let observed = capture_trigger_notifies(
        pool.clone(),
        move |conn| {
            sql_query(
                "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at)
                 VALUES ($1, $2, $3, 'Pending', 0, 3, now() - INTERVAL '1 second')
                 ON CONFLICT (id) DO NOTHING",
            )
            .bind::<Text, _>(&conflict_id)
            .bind::<Text, _>(&queue_for_writer)
            .bind::<diesel::sql_types::Binary, _>(serde_json::to_vec("x").unwrap())
            .execute(conn)
            .map_err(|e| e.to_string())?;
            Ok(())
        },
        4,
        Duration::from_millis(400),
    )
    .await?;

    let run = parse_payloads(observed, &queue);
    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(run))
}

// --------------------------------------------------------------------------
// end-to-end legacy payload acceptance via the storage stream
//
// Drives the `InsertEvent` decoder's `{job_type, id}` back-compat branch by
// inserting a row with `run_at` in the future (so the trigger does NOT fire)
// and then hand-crafting a legacy NOTIFY payload that points at that id.
// --------------------------------------------------------------------------

#[derive(Debug)]
struct LegacyShapeRun {
    received_id: Option<String>,
    expected_id: String,
}

async fn run_legacy_payload_decoded() -> Result<Outcome<LegacyShapeRun>, String> {
    let Some(pool) = test_pool_multi().await? else {
        return Ok(Outcome::Skipped);
    };
    let queue = format!("apalis-spec-notify-legacy-{}", Ulid::new());
    cleanup_queue(pool.clone(), queue.clone()).await?;

    // Insert a row that exists in apalis.jobs but is NOT yet eligible for
    // dequeue (so the natural trigger does not race our hand-crafted NOTIFY).
    // We then update run_at into the past *without* triggering NOTIFY (UPDATE
    // is not covered by the `notify_new_jobs` AFTER INSERT trigger), and
    // finally publish a legacy `{job_type, id}` payload by hand.
    let row_id = Ulid::new().to_string();
    let queue_for_seed = queue.clone();
    let row_id_for_seed = row_id.clone();
    with_conn(pool.clone(), move |conn| {
        sql_query(
            "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at)
             VALUES ($1, $2, $3, 'Pending', 0, 3, now() + INTERVAL '1 hour')",
        )
        .bind::<Text, _>(&row_id_for_seed)
        .bind::<Text, _>(&queue_for_seed)
        .bind::<diesel::sql_types::Binary, _>(serde_json::to_vec("legacy-payload").unwrap())
        .execute(conn)
        .map_err(|e| e.to_string())
        .map(|_| ())
    })
    .await?;

    // Spin up the storage stream (it LISTENs).
    let config = Config::new(&queue).set_buffer_size(8);
    let storage = PostgresStorage::<String>::new_with_notify(&pool, &config);
    let worker = WorkerContext::new::<()>(&format!("notify-legacy-worker-{queue}"));
    let mut stream = storage.clone().poll(&worker);

    // Let the listener thread install LISTEN before we fire the NOTIFY.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // Flip run_at into the past so the row is fetchable when the listener
    // sees the id, then publish a legacy-shaped NOTIFY payload.
    let queue_for_publish = queue.clone();
    let row_id_for_publish = row_id.clone();
    with_conn(pool.clone(), move |conn| {
        sql_query("UPDATE apalis.jobs SET run_at = now() - INTERVAL '1 second' WHERE id = $1")
            .bind::<Text, _>(&row_id_for_publish)
            .execute(conn)
            .map_err(|e| e.to_string())?;
        let payload = serde_json::json!({
            "job_type": queue_for_publish,
            "id": row_id_for_publish,
        })
        .to_string();
        sql_query("SELECT pg_notify('apalis::job::insert', $1)")
            .bind::<Text, _>(&payload)
            .execute(conn)
            .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await?;

    // Wait for the storage stream to surface the task.
    let received = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            match stream.next().await {
                Some(Ok(Some(task))) => return Ok::<_, String>(Some(task)),
                Some(Ok(None)) => continue,
                Some(Err(e)) => return Err(e.to_string()),
                None => return Ok(None),
            }
        }
    })
    .await
    .map_err(|_| "timed out waiting for legacy-shape NOTIFY to surface".to_owned())??;

    drop(stream);
    let received_id =
        received.and_then(|t: PgTask<String>| t.parts.task_id.map(|id| id.to_string()));

    cleanup_queue(pool, queue).await?;
    Ok(Outcome::Completed(LegacyShapeRun {
        received_id,
        expected_id: row_id,
    }))
}

// --------------------------------------------------------------------------
// assertions
// --------------------------------------------------------------------------

fn produced_exactly_one_payload() -> impl Fn(&Result<Outcome<TriggerRun>, String>) -> AssertionResult
{
    observe::<TriggerRun, _>("payload count", |run| {
        if run.payloads.len() == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected exactly one NOTIFY payload, got {} (payloads={:?})",
                run.payloads.len(),
                run.payloads
            ))
        }
    })
}

fn produced_no_payload() -> impl Fn(&Result<Outcome<TriggerRun>, String>) -> AssertionResult {
    observe::<TriggerRun, _>("payload count zero", |run| {
        if run.payloads.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "expected zero NOTIFY payloads, got {} (payloads={:?})",
                run.payloads.len(),
                run.payloads
            ))
        }
    })
}

fn payload_uses_ids_array_shape() -> impl Fn(&Result<Outcome<TriggerRun>, String>) -> AssertionResult
{
    observe::<TriggerRun, _>("payload shape", |run| {
        let Some(payload) = run.payloads.first() else {
            return Err("no payloads captured to inspect shape".into());
        };
        let ids = payload
            .get("ids")
            .ok_or_else(|| format!("payload missing `ids` field: {payload:?}"))?;
        if !ids.is_array() {
            return Err(format!("`ids` is not an array: {payload:?}"));
        }
        if payload.get("id").and_then(|v| v.as_str()).is_some() {
            return Err(format!(
                "post-migration trigger should not emit legacy `id` field: {payload:?}"
            ));
        }
        Ok(())
    })
}

fn payload_carries_a_single_id() -> impl Fn(&Result<Outcome<TriggerRun>, String>) -> AssertionResult
{
    observe::<TriggerRun, _>("payload ids length", |run| {
        let Some(payload) = run.payloads.first() else {
            return Err("no payloads captured".into());
        };
        let ids = payload
            .get("ids")
            .and_then(Value::as_array)
            .ok_or_else(|| format!("payload missing `ids` array: {payload:?}"))?;
        if ids.len() == 1 {
            Ok(())
        } else {
            Err(format!("expected `ids` length=1, got {}", ids.len()))
        }
    })
}

fn payloads_are_chunked_at_one_hundred(
    expected_total: usize,
) -> impl Fn(&Result<Outcome<TriggerRun>, String>) -> AssertionResult {
    observe::<TriggerRun, _>("payload chunking", move |run| {
        if run.payloads.is_empty() {
            return Err("no payloads captured".into());
        }
        let mut total = 0usize;
        for (idx, payload) in run.payloads.iter().enumerate() {
            let ids = payload
                .get("ids")
                .and_then(Value::as_array)
                .ok_or_else(|| format!("payload[{idx}] missing `ids` array: {payload:?}"))?;
            if ids.len() > 100 {
                return Err(format!(
                    "payload[{idx}] carries {} ids, exceeds 100-id chunk cap",
                    ids.len()
                ));
            }
            total += ids.len();
        }
        if total != expected_total {
            return Err(format!(
                "expected total of {expected_total} ids across payloads, got {total}"
            ));
        }
        Ok(())
    })
}

fn produced_at_least_two_payloads()
-> impl Fn(&Result<Outcome<TriggerRun>, String>) -> AssertionResult {
    observe::<TriggerRun, _>("payload count >=2", |run| {
        if run.payloads.len() >= 2 {
            Ok(())
        } else {
            Err(format!(
                "expected at least two NOTIFY payloads from a 150-row batch (chunk cap 100), got {}",
                run.payloads.len()
            ))
        }
    })
}

fn legacy_payload_delivered_target_row()
-> impl Fn(&Result<Outcome<LegacyShapeRun>, String>) -> AssertionResult {
    observe::<LegacyShapeRun, _>("legacy delivery", |run| match &run.received_id {
        Some(id) if *id == run.expected_id => Ok(()),
        Some(other) => Err(format!(
            "expected legacy-payload task id {}, got {other}",
            run.expected_id
        )),
        None => Err("legacy-payload task never surfaced".into()),
    })
}

// --------------------------------------------------------------------------
// expectations
// --------------------------------------------------------------------------

lets_expect! { #tokio_test
    expect(run_single_row_insert().await) {
        when a_single_eligible_row_is_inserted_via_the_statement_level_trigger {
            to emits_exactly_one_notify_payload { produced_exactly_one_payload() }
            to uses_the_post_migration_ids_array_shape { payload_uses_ids_array_shape() }
            to carries_the_single_inserted_id { payload_carries_a_single_id() }
        }
    }

    expect(run_future_dated_insert().await) {
        when the_inserted_row_is_scheduled_for_a_future_run_at {
            // Trigger filters `WHERE run_at <= cutoff`, so a future-dated
            // row is intentionally invisible until the polling fetcher or a
            // subsequent UPDATE picks it up.
            to does_not_emit_a_notify_payload { produced_no_payload() }
        }
    }

    expect(run_batch_insert_chunks().await) {
        when a_single_statement_inserts_more_rows_than_the_chunk_cap {
            // 150 rows → chunk size 100 → two NOTIFY payloads (100 + 50).
            to chunks_the_payloads_at_one_hundred_ids_each {
                payloads_are_chunked_at_one_hundred(150)
            }
            to emits_more_than_one_payload_for_the_batch {
                produced_at_least_two_payloads()
            }
        }
    }

    expect(run_on_conflict_no_notify().await) {
        when an_insert_collides_on_the_primary_key_and_does_nothing {
            // Transition table excludes skipped rows, COUNT(*) = 0 fires the
            // trigger's early-return — no NOTIFY for a no-op INSERT.
            to does_not_emit_a_notify_payload { produced_no_payload() }
        }
    }

    expect(run_legacy_payload_decoded().await) {
        when a_hand_crafted_legacy_job_type_id_payload_is_published {
            // Exercises the back-compat branch of `InsertEvent::into_ids`:
            // `ids` is empty, `id` is Some(_), so the listener forwards the
            // single legacy id. The row was made fetchable by an UPDATE that
            // bypasses the AFTER INSERT trigger so no real NOTIFY races us.
            to is_decoded_and_surfaces_the_referenced_task_id {
                legacy_payload_delivered_target_row()
            }
        }
    }
}
