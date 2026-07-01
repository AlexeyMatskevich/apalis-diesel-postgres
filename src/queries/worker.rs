use std::sync::Arc;

use apalis_core::worker::context::WorkerContext;
use diesel::{
    Connection, PgConnection, RunQueryDsl, sql_query,
    sql_types::{Integer, Text},
};
use futures::stream;
use ulid::Ulid;

use crate::{
    Config, Error, PgPool,
    queries::{clamp_i32, with_conn},
};

/// Mint a per-registration lease token. Tokens are random Ulids: only the
/// process that issued one can refresh the corresponding `apalis.workers` row,
/// closing the heartbeat-spoofing window described on the `lease_token`
/// migration.
pub(crate) fn mint_lease_token() -> String {
    Ulid::new().to_string()
}

/// Per-statement cap on the orphan-recovery sweep. Without a bound, a mass
/// worker death could leave every in-flight row eligible at once and turn the
/// sweep into a single unbounded UPDATE that locks and rewrites the entire
/// backlog in one statement. Capping the subselect keeps each sweep's lock
/// footprint and statement duration bounded; a backlog larger than one batch
/// drains across subsequent `keep_alive`-interval sweeps (and is further
/// parallelised across a queue's workers by `FOR UPDATE ... SKIP LOCKED`).
pub(crate) const REENQUEUE_ORPHANED_BATCH_LIMIT: i32 = 1000;

pub(crate) fn reenqueue_orphaned_blocking(
    conn: &mut PgConnection,
    config: &Config,
) -> Result<usize, Error> {
    sql_query(
        "UPDATE apalis.jobs
         SET status = CASE
                 WHEN attempts::bigint + 1 >= max_attempts THEN 'Killed'
                 ELSE 'Pending'
             END,
             done_at = CASE
                 WHEN attempts::bigint + 1 >= max_attempts THEN now()
                 ELSE NULL
             END,
             lock_by = NULL,
             lock_at = NULL,
             -- attempts::bigint promotes the arithmetic so a corrupt row
             -- sitting at i32::MAX cannot overflow the + 1 (which PostgreSQL
             -- rejects as an integer-out-of-range error); LEAST re-bounds the
             -- result to max_attempts, so it fits the int column again. The
             -- >= comparisons above are likewise promoted to avoid the overflow.
             attempts = LEAST(attempts::bigint + 1, max_attempts),
             -- On terminal `Killed` transitions stamp the timeout marker
             -- unconditionally (no further ack can succeed because
             -- `ack_task` requires status='Running'). On `Pending`
             -- transitions stamp the marker only when `last_result` is NULL
             -- so observers can tell a heartbeat-timeout reenqueue from a
             -- normal retry — but never clobber an existing result, because
             -- a worker racing this UPDATE (heartbeat not yet refreshed)
             -- might have acked the task moments earlier and its outcome
             -- must remain visible.
             last_result = CASE
                 WHEN attempts::bigint + 1 >= max_attempts
                     THEN '{\"Err\": \"Re-enqueued due to worker heartbeat timeout.\"}'::jsonb
                 WHEN last_result IS NULL
                     THEN '{\"Err\": \"Re-enqueued due to worker heartbeat timeout.\"}'::jsonb
                 ELSE last_result
             END
         -- The status predicate is repeated outside the sub-select on purpose.
         -- Under READ COMMITTED, a row that a concurrent transaction already
         -- re-enqueued (or acked) between our sub-select snapshot and our row
         -- lock is re-checked here against its NEW version (EvalPlanQual);
         -- without the outer predicate this sweep would apply twice — burning
         -- an extra attempt, prematurely killing the job, or flipping an
         -- already-acked 'Done' row back to 'Pending'. `FOR UPDATE SKIP
         -- LOCKED` below additionally keeps concurrent sweeps from queueing
         -- behind each other's row locks.
         WHERE (status = 'Running' OR status = 'Queued')
           AND id IN (
             SELECT jobs.id
             FROM apalis.jobs
             INNER JOIN apalis.workers
                 ON jobs.lock_by = workers.id
                 AND jobs.job_type = workers.worker_type
             WHERE (status = 'Running' OR status = 'Queued')
                 AND now() - apalis.workers.last_seen >= ($1 * INTERVAL '1 second')
                 AND jobs.job_type = $2
             -- Bound the sweep so a mass worker death cannot turn this into one
             -- unbounded UPDATE over the whole backlog; the remainder drains on
             -- the next keep_alive-interval sweep.
             LIMIT $3
             FOR UPDATE OF jobs SKIP LOCKED
         )",
    )
    .bind::<Integer, _>(clamp_i32(config.reenqueue_orphaned_after().as_secs()))
    .bind::<Text, _>(config.queue().to_string())
    .bind::<Integer, _>(REENQUEUE_ORPHANED_BATCH_LIMIT)
    .execute(conn)
    .map_err(Error::database("re-enqueueing orphaned jobs"))
}

pub(crate) fn reenqueue_orphaned(
    pool: PgPool,
    config: Config,
) -> impl Future<Output = Result<usize, Error>> + Send {
    with_conn(pool, move |conn| reenqueue_orphaned_blocking(conn, &config))
}

pub(crate) fn reenqueue_orphaned_stream(
    pool: PgPool,
    config: Config,
) -> impl futures::Stream<Item = Result<usize, Error>> + Send {
    stream::unfold((), move |_| {
        let pool = pool.clone();
        let config = config.clone();
        async move {
            apalis_core::timer::sleep(*config.keep_alive()).await;
            Some((reenqueue_orphaned(pool, config).await, ()))
        }
    })
}

pub(crate) fn register_worker_blocking(
    conn: &mut PgConnection,
    worker_type: &str,
    worker: &WorkerContext,
    storage_name: &'static str,
    lease_token: &str,
    stale_after_secs: i32,
) -> Result<(), Error> {
    // ON CONFLICT update is gated on the existing row being either
    // unbound (NULL lease_token, e.g. left over from a dashboard-side
    // registration) or stale (last_seen older than the orphan-recovery
    // threshold). This closes the live-hijack window: a process that
    // re-registers with an already-claimed `(worker_id, worker_type)`
    // can no longer silently rotate the lease_token out from under a
    // healthy heartbeater. Legitimate restart still works once the
    // previous registration ages past the orphan threshold (the same
    // moment `reenqueue_orphaned` would start stealing its jobs back).
    let count = sql_query(
        "WITH registration_lock AS (
             SELECT pg_try_advisory_xact_lock(hashtext($1), hashtext($2)) AS acquired
         )
         INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at, lease_token)
         SELECT $1, $2, $3, $4, now(), now(), $5
         FROM registration_lock
         WHERE acquired
         ON CONFLICT (id, worker_type) DO UPDATE
         SET storage_name = EXCLUDED.storage_name,
             layers = EXCLUDED.layers,
             last_seen = now(),
             lease_token = EXCLUDED.lease_token
         WHERE apalis.workers.lease_token IS NULL
            OR apalis.workers.lease_token = EXCLUDED.lease_token
            OR now() - apalis.workers.last_seen >= ($6 * INTERVAL '1 second')",
    )
    .bind::<Text, _>(worker.name())
    .bind::<Text, _>(worker_type)
    .bind::<Text, _>(storage_name)
    .bind::<Text, _>(worker.get_service())
    .bind::<Text, _>(lease_token)
    .bind::<Integer, _>(stale_after_secs)
    .execute(conn)
    .map_err(Error::database("registering worker"))?;
    if count == 0 {
        Err(Error::already_registered(worker.name(), worker_type))
    } else {
        Ok(())
    }
}

pub(crate) fn initial_heartbeat(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
    storage_name: &'static str,
    lease_token: Arc<str>,
) -> impl Future<Output = Result<(), Error>> + Send {
    with_conn(pool, move |conn| {
        let stale_after_secs = clamp_i32(config.reenqueue_orphaned_after().as_secs());
        conn.transaction(|tx| {
            reenqueue_orphaned_blocking(tx, &config)?;
            register_worker_blocking(
                tx,
                config.queue().as_ref(),
                &worker,
                storage_name,
                &lease_token,
                stale_after_secs,
            )
        })
    })
}

pub(crate) fn keep_alive(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
    lease_token: Arc<str>,
) -> impl Future<Output = Result<(), Error>> + Send {
    with_conn(pool, move |conn| {
        let count = sql_query(
            "UPDATE apalis.workers
             SET last_seen = now()
             WHERE id = $1 AND worker_type = $2 AND lease_token = $3",
        )
        .bind::<Text, _>(worker.name())
        .bind::<Text, _>(config.queue().to_string())
        .bind::<Text, _>(&*lease_token)
        .execute(conn)
        .map_err(Error::database("updating worker heartbeat"))?;
        // Either no row exists for this (worker_id, queue) OR the stored
        // lease_token does not match — both mean *this* process is no longer
        // the authoritative heartbeater (e.g. another registration took over).
        // Recreating the worker stream rotates the token.
        // Pass the queue by reference: the successful (non-zero) path never
        // needs it, so the `String` is allocated only when building the error.
        heartbeat_outcome(count, &worker, config.queue().as_ref())
    })
}

/// Map the heartbeat UPDATE's affected-row count to a result: zero rows means
/// this process is no longer the authoritative heartbeater (unregistered or a
/// rotated lease token), any positive count is a successful heartbeat. Extracted
/// so the zero/non-zero decision is unit-testable without a database.
fn heartbeat_outcome(
    updated_rows: usize,
    worker: &WorkerContext,
    queue: &str,
) -> Result<(), Error> {
    if updated_rows == 0 {
        Err(Error::worker_not_registered(
            "updating worker heartbeat",
            worker.name(),
            // Allocated here (the cold path) rather than on every heartbeat.
            queue.to_owned(),
            "the worker may not be registered for this queue, or another process has re-registered with a different lease token; recreate the worker stream",
        ))
    } else {
        Ok(())
    }
}

pub(crate) fn keep_alive_stream(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
    lease_token: Arc<str>,
) -> impl futures::Stream<Item = Result<(), Error>> + Send {
    stream::unfold((), move |_| {
        let pool = pool.clone();
        let config = config.clone();
        let worker = worker.clone();
        let lease_token = Arc::clone(&lease_token);
        async move {
            apalis_core::timer::sleep(*config.keep_alive()).await;
            Some((keep_alive(pool, config, worker, lease_token).await, ()))
        }
    })
}

#[cfg(test)]
mod tests {
    use lets_expect::{AssertionError, AssertionResult, *};
    use ulid::Ulid;

    use super::*;

    fn minted_lease_token_parses_as_ulid() -> bool {
        Ulid::from_string(&mint_lease_token()).is_ok()
    }

    fn two_minted_lease_tokens_differ() -> bool {
        mint_lease_token() != mint_lease_token()
    }

    fn worker_not_registered(error: &Error) -> AssertionResult {
        match error {
            // Also pins the queue on the error: `heartbeat_outcome` now defers
            // the `String` allocation to this cold path, so the surfaced queue
            // must still be the one the caller passed by reference.
            Error::WorkerNotRegistered { queue, .. } if queue == "heartbeat-queue" => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected WorkerNotRegistered for queue \"heartbeat-queue\", got {other:?}"
            )])),
        }
    }

    fn heartbeat(updated_rows: usize) -> Result<(), Error> {
        let worker = WorkerContext::new::<()>("heartbeat-worker");
        heartbeat_outcome(updated_rows, &worker, "heartbeat-queue")
    }

    lets_expect! {
        expect(minted_lease_token_parses_as_ulid()) {
            when a_lease_token_is_minted {
                to is_a_well_formed_ulid { be_true }
            }
        }

        expect(two_minted_lease_tokens_differ()) {
            when two_lease_tokens_are_minted {
                to each_call_mints_a_distinct_token { be_true }
            }
        }

        expect(heartbeat(rows)) {
            let rows = 1;

            to reports_a_successful_heartbeat { be_ok }

            when the_update_affected_no_rows {
                let rows = 0;
                to reports_the_worker_is_no_longer_registered { be_err_and worker_not_registered }
            }
        }
    }
}
