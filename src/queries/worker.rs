use crate::{Config, Error, PgPool, queries::with_conn};
use apalis_core::worker::context::WorkerContext;
use diesel::{
    Connection, PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{Array, BigInt, Bool, Double, Jsonb, Nullable, Text},
};
use futures::stream;
use std::{sync::Arc, time::Duration};
use ulid::Ulid;

pub(crate) fn mint_lease_token() -> String {
    Ulid::new().to_string()
}
/// Per-statement cap on the periodic orphan-recovery sweep. Without a bound,
/// a mass worker death would rewrite every orphaned row in one statement
/// while holding their locks; the sweep repeats every `keep_alive` interval,
/// so a larger backlog drains across sweeps. Registration takeover is
/// deliberately unbounded: it must recover every claim of the identity it
/// replaces before the new token renews the heartbeat, or those rows would
/// stay hidden behind a live registration.
pub(crate) const REENQUEUE_ORPHANED_BATCH_LIMIT: i32 = 1000;

// Compare elapsed seconds as a numeric value instead of constructing an interval:
// even Duration::MAX is representable, and subsecond deadlines are preserved.
fn timeout_seconds(duration: Duration) -> f64 {
    duration.as_secs_f64()
}
#[derive(QueryableByName)]
struct WorkerIdentity {
    #[diesel(sql_type = Text)]
    id: String,
}
#[derive(QueryableByName)]
struct Decision {
    #[diesel(sql_type = Bool)]
    allowed: bool,
}

#[derive(QueryableByName)]
struct RegistrationDecision {
    #[diesel(sql_type = Bool)]
    allowed: bool,
    #[diesel(sql_type = Bool)]
    lost: bool,
}

/// What the worker row says about a name for a queue, judged against the
/// caller's lease token.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Registration {
    /// No registration exists for this name and queue.
    Absent,
    /// A registration exists, but another token owns it.
    Replaced,
    /// The caller's token owns the registration, or the caller presented none.
    Current,
}

/// All native ownership operations take the worker row before any job row.
/// Takeover/recovery explicitly use FOR UPDATE, so KEY SHARE fences identity
/// changes while allowing the independent last_seen heartbeat UPDATE to proceed.
pub(crate) fn worker_registration(
    conn: &mut PgConnection,
    worker: &str,
    queue: &str,
    token: Option<&str>,
) -> Result<Registration, Error> {
    #[derive(QueryableByName)]
    struct Held {
        #[diesel(sql_type = Bool)]
        current: bool,
    }
    let rows = sql_query(
        "SELECT ($3::text IS NULL OR lease_token IS NOT DISTINCT FROM $3) AS current
        FROM apalis.workers WHERE id=$1 AND worker_type=$2 FOR KEY SHARE",
    )
    .bind::<Text, _>(worker)
    .bind::<Text, _>(queue)
    .bind::<Nullable<Text>, _>(token)
    .load::<Held>(conn)
    .map_err(Error::database("checking worker registration"))?;
    Ok(match rows.first() {
        None => Registration::Absent,
        Some(held) if held.current => Registration::Current,
        Some(_) => Registration::Replaced,
    })
}

/// Whether the caller's token currently owns the registration; see
/// [`worker_registration`] for the distinction between absent and replaced.
pub(crate) fn lock_current_worker(
    conn: &mut PgConnection,
    worker: &str,
    queue: &str,
    token: Option<&str>,
) -> Result<bool, Error> {
    Ok(worker_registration(conn, worker, queue, token)? == Registration::Current)
}

/// Why a worker's active claims are handed back to the queue. The reason is
/// recorded in `last_result` of every recovered row that had no result yet,
/// and of every row that the recovery kills.
#[derive(Clone, Copy)]
enum Recovery {
    /// The periodic sweep found the worker's heartbeat stale; the batch is
    /// bounded and skips rows another sweep already holds.
    StaleSweep,
    /// A fresh registration takes the name over from a stale incumbent; it
    /// must wait for, and recover, every claim of that incumbent.
    Takeover,
    /// The worker released its own registration; like a takeover it waits
    /// for every claim that is still being committed.
    Release,
}

impl Recovery {
    fn result(self) -> serde_json::Value {
        let message = match self {
            Self::StaleSweep | Self::Takeover => "Re-enqueued due to worker heartbeat timeout.",
            Self::Release => "Re-enqueued because the worker released its registration.",
        };
        serde_json::json!({ "Err": message })
    }
}

fn recover_owned(
    conn: &mut PgConnection,
    queue: &str,
    workers: Vec<String>,
    recovery: Recovery,
) -> Result<usize, Error> {
    if workers.is_empty() {
        return Ok(0);
    }
    // Takeover and release must wait for every incumbent claim. SKIP LOCKED is
    // appropriate only for a periodic sweep, whose stale worker identity
    // remains unchanged.
    let bounded = matches!(recovery, Recovery::StaleSweep);
    let skip = if bounded { "SKIP LOCKED" } else { "" };
    // Keep one candidate set even when the planner chooses a nested-loop
    // join. Re-evaluating a locking LIMIT subquery after each UPDATE can select
    // more rows as earlier candidates leave the active states.
    sql_query(format!(
        "WITH candidates AS MATERIALIZED (
        SELECT id FROM apalis.jobs WHERE job_type=$1 AND lock_by=ANY($2)
            AND status IN ('Running','Queued') ORDER BY id LIMIT $3 FOR UPDATE {skip}
        )
        UPDATE apalis.jobs SET
        status=CASE WHEN attempts::bigint+1>=max_attempts THEN 'Killed' ELSE 'Pending' END,
        done_at=CASE WHEN attempts::bigint+1>=max_attempts THEN clock_timestamp() ELSE NULL END,
        lock_by=NULL, lock_at=NULL,
        attempts=LEAST(attempts::bigint+1,max_attempts),
        last_result=CASE WHEN attempts::bigint+1>=max_attempts OR last_result IS NULL
            THEN $4 ELSE last_result END
        FROM candidates
        WHERE apalis.jobs.status IN ('Running','Queued') AND apalis.jobs.id=candidates.id"
    ))
    .bind::<Text, _>(queue)
    .bind::<Array<Text>, _>(workers)
    .bind::<Nullable<BigInt>, _>(bounded.then_some(i64::from(REENQUEUE_ORPHANED_BATCH_LIMIT)))
    .bind::<Jsonb, _>(recovery.result())
    .execute(conn)
    .map_err(Error::database("re-enqueueing orphaned jobs"))
}

/// Hand a registration back: recover every claim the token still owns and
/// mark the row released, so a successor registers immediately instead of
/// waiting for the stale deadline. The row is kept because completed jobs
/// keep referencing it as their last owner.
///
/// Locking the row `FOR UPDATE` orders this after every in-flight claim
/// (which holds `FOR KEY SHARE` on it) and fences takeover, the sweep and
/// the heartbeat until the release commits.
pub(crate) fn release_worker_blocking(
    conn: &mut PgConnection,
    queue: &str,
    worker: &str,
    lease_token: &str,
) -> Result<usize, Error> {
    conn.transaction(|tx| {
        let owned = sql_query(
            "SELECT id FROM apalis.workers WHERE id=$1 AND worker_type=$2 AND lease_token=$3 FOR UPDATE",
        )
        .bind::<Text, _>(worker)
        .bind::<Text, _>(queue)
        .bind::<Text, _>(lease_token)
        .load::<WorkerIdentity>(tx)
        .map_err(Error::database("locking the worker registration for release"))?;
        if owned.is_empty() {
            return Err(Error::worker_not_registered(
                "releasing worker registration",
                worker,
                queue.to_owned(),
                "the registration is absent, has no lease token, or is owned by another storage; nothing was released",
            ));
        }
        let recovered = recover_owned(tx, queue, vec![worker.to_owned()], Recovery::Release)?;
        // The epoch is older than any stale deadline, so every sweeper and
        // successor treats the row as released regardless of its own window,
        // and dashboards read a zero heartbeat.
        sql_query(
            "UPDATE apalis.workers SET lease_token=NULL, last_seen=to_timestamp(0)
             WHERE id=$1 AND worker_type=$2 AND lease_token=$3",
        )
        .bind::<Text, _>(worker)
        .bind::<Text, _>(queue)
        .bind::<Text, _>(lease_token)
        .execute(tx)
        .map_err(Error::database("releasing worker registration"))?;
        Ok(recovered)
    })
}

pub(crate) fn release_worker(
    pool: PgPool,
    config: Config,
    worker: String,
    lease_token: Arc<str>,
) -> impl Future<Output = Result<usize, Error>> + Send {
    with_conn(pool, move |conn| {
        release_worker_blocking(conn, config.queue().as_ref(), &worker, &lease_token)
    })
}

pub(crate) fn reenqueue_orphaned_blocking(
    conn: &mut PgConnection,
    config: &Config,
) -> Result<usize, Error> {
    conn.transaction(|tx| {
        let workers=sql_query("SELECT id FROM apalis.workers w WHERE worker_type=$1
            AND EXTRACT(EPOCH FROM (clock_timestamp()-last_seen)) >= $2
            AND EXISTS(SELECT 1 FROM apalis.jobs j WHERE j.job_type=w.worker_type AND j.lock_by=w.id AND j.status IN ('Running','Queued'))
            ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED")
            .bind::<Text,_>(config.queue().as_ref())
            .bind::<Double,_>(timeout_seconds(config.reenqueue_orphaned_after()))
            .bind::<BigInt,_>(i64::from(REENQUEUE_ORPHANED_BATCH_LIMIT))
            .load::<WorkerIdentity>(tx).map_err(Error::database("locking orphaned workers"))?;
        recover_owned(tx, config.queue().as_ref(), workers.into_iter().map(|w|w.id).collect(), Recovery::StaleSweep)
    })
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
    stale_after: Duration,
) -> Result<(), Error> {
    conn.transaction(|tx| {
        let acquired=sql_query("SELECT pg_try_advisory_xact_lock(hashtext($1),hashtext($2)) AS allowed")
            .bind::<Text,_>(worker.name()).bind::<Text,_>(worker_type).get_result::<Decision>(tx)
            .map_err(Error::database("locking worker registration"))?.allowed;
        if !acquired { return Err(Error::already_registered(worker.name(),worker_type)); }
        // Materialize the locked row before sampling the server clock. A caller
        // waiting behind a heartbeat must use its updated row, and a caller
        // waiting across the stale deadline must decide after that wait.
        // Liveness is judged by `last_seen` alone. A registration without a
        // lease token renews it through the admin `register_worker` path, so
        // while it is fresh its claims belong to a live consumer and this
        // registration must wait for the stale deadline like any other.
        let existing=sql_query("WITH locked AS MATERIALIZED (
                SELECT lease_token,last_seen FROM apalis.workers
                WHERE id=$1 AND worker_type=$2 FOR UPDATE
            ), sampled AS MATERIALIZED (
                SELECT lease_token,last_seen,clock_timestamp() AS observed_at FROM locked
            )
            SELECT lease_token IS NOT DISTINCT FROM $3
                    OR EXTRACT(EPOCH FROM(observed_at-last_seen)) >= $4 AS allowed,
                lease_token IS DISTINCT FROM $3
                    OR EXTRACT(EPOCH FROM(observed_at-last_seen)) >= $4 AS lost
            FROM sampled")
            .bind::<Text,_>(worker.name()).bind::<Text,_>(worker_type).bind::<Text,_>(lease_token)
            .bind::<Double,_>(timeout_seconds(stale_after)).load::<RegistrationDecision>(tx)
            .map_err(Error::database("checking worker registration"))?;
        if let Some(decision)=existing.into_iter().next() {
            if !decision.allowed { return Err(Error::already_registered(worker.name(),worker_type)); }
            if decision.lost { recover_owned(tx,worker_type,vec![worker.name().to_owned()],Recovery::Takeover)?; }
        }
        sql_query("INSERT INTO apalis.workers(id,worker_type,storage_name,layers,last_seen,started_at,lease_token)
            VALUES($1,$2,$3,$4,clock_timestamp(),clock_timestamp(),$5)
            ON CONFLICT(id,worker_type) DO UPDATE SET storage_name=EXCLUDED.storage_name,layers=EXCLUDED.layers,last_seen=clock_timestamp(),lease_token=EXCLUDED.lease_token")
            .bind::<Text,_>(worker.name()).bind::<Text,_>(worker_type).bind::<Text,_>(storage_name)
            .bind::<Text,_>(worker.get_service()).bind::<Text,_>(lease_token).execute(tx)
            .map_err(Error::database("registering worker"))?;
        Ok(())
    })
}
pub(crate) fn initial_heartbeat(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
    storage_name: &'static str,
    lease_token: Arc<str>,
) -> impl Future<Output = Result<(), Error>> + Send {
    with_conn(pool, move |conn| {
        // A failed startup sweep must not publish a new registration first.
        // Keep these transactions separate: registration locks its own worker,
        // whereas the global sweep locks unrelated workers in sorted order.
        reenqueue_orphaned_blocking(conn, &config)?;
        register_worker_blocking(
            conn,
            config.queue().as_ref(),
            &worker,
            storage_name,
            &lease_token,
            config.reenqueue_orphaned_after(),
        )?;
        Ok(())
    })
}

pub(crate) fn keep_alive(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
    lease_token: Arc<str>,
) -> impl Future<Output = Result<(), Error>> + Send {
    with_conn(pool, move |conn| {
        keep_alive_blocking(conn, &config, &worker, &lease_token)
    })
}

pub(crate) fn keep_alive_blocking(
    conn: &mut PgConnection,
    config: &Config,
    worker: &WorkerContext,
    lease_token: &str,
) -> Result<(), Error> {
    let count = sql_query(
        "UPDATE apalis.workers
         SET last_seen = clock_timestamp()
         WHERE id = $1 AND worker_type = $2 AND lease_token = $3",
    )
    .bind::<Text, _>(worker.name())
    .bind::<Text, _>(config.queue().as_ref())
    .bind::<Text, _>(lease_token)
    .execute(conn)
    .map_err(Error::database("updating worker heartbeat"))?;
    // Zero rows means the worker has been removed or its token was replaced.
    // The successful path does not allocate a queue name for an error.
    heartbeat_outcome(count, worker, config.queue().as_ref())
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
        expect(minted_lease_token_parses_as_ulid()) as a_new_lease_token {
            when a_lease_token_is_minted {
                to is_a_well_formed_ulid { be_true }
            }
        }

        expect(two_minted_lease_tokens_differ()) as independent_registrations {
            when two_lease_tokens_are_minted {
                to mints_a_distinct_token { be_true }
            }
        }

        expect(heartbeat(rows)) as renewing_a_registration {
            let rows = 1;

            to reports_a_successful_heartbeat { be_ok }

            when the_update_affected_no_rows {
                let rows = 0;
                to reports_the_worker_is_no_longer_registered { be_err_and worker_not_registered }
            }
        }
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;
    use lets_expect::*;
    lets_expect! {
        expect(timeout_seconds(duration)) as orphan_timeout_seconds {
            let duration=Duration::from_secs(1);
            to preserves_a_whole_second { equal(1.0) }
            when the_timeout_has_a_fractional_second {
                let duration=Duration::from_millis(500);
                to preserves_the_requested_half_second { equal(0.5) }
            }
            when the_timeout_is_zero {
                let duration=Duration::ZERO;
                to permits_immediate_recovery { equal(0.0) }
            }
            when the_timeout_is_the_largest_duration {
                let duration=Duration::MAX;
                to remains_representable_without_interval_overflow { equal(Duration::MAX.as_secs_f64()) }
            }
        }
    }
}
