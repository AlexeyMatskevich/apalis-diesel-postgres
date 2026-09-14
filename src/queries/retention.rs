//! End of life for queue data. Completed history and released registrations
//! are never removed by the worker protocol; these are the two explicit
//! retention operations, both scoped to one queue and both batched, so no
//! statement holds more than a bounded number of rows and every batch is a
//! cancellation point that returns its connection to the pool.

use std::time::Duration;

use diesel::{
    PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{BigInt, Double, Text},
};

use crate::{
    Config, Error, PgPool,
    queries::{TERMINAL_PREDICATE, with_conn, worker::timeout_seconds},
};

/// Rows deleted per statement by [`purge_terminal_tasks`]. Each statement is
/// its own transaction, so the locks and WAL of one purge never cover more
/// than this many rows at once; a larger backlog drains across statements.
pub(crate) const PURGE_BATCH_LIMIT: i64 = 10_000;

/// Registrations deleted per statement by [`prune_workers`]. Every deleted
/// row also runs the foreign-key probe over the queue's history, so the
/// batch stays as small as the orphan sweep's.
pub(crate) const PRUNE_BATCH_LIMIT: i64 = 1_000;

#[derive(QueryableByName)]
struct Epoch {
    #[diesel(sql_type = Double)]
    value: f64,
}

/// The completion cutoff as server epoch seconds, sampled once per purge so
/// a queue that keeps completing tasks cannot extend the purge indefinitely.
pub(crate) fn server_cutoff(
    conn: &mut PgConnection,
    completed_before: Duration,
) -> Result<f64, Error> {
    sql_query("SELECT EXTRACT(EPOCH FROM clock_timestamp())::double precision - $1 AS value")
        .bind::<Double, _>(timeout_seconds(completed_before))
        .get_result::<Epoch>(conn)
        .map(|row| row.value)
        .map_err(Error::database("sampling the retention cutoff"))
}

/// Delete one batch of terminal rows of `queue` whose completion is at or
/// before `cutoff` (server epoch seconds). A terminal row without a
/// completion time (written outside this crate) is aged by its schedule
/// instead. No ordering: `SKIP LOCKED` already lets concurrent purges share
/// the backlog, and an order would sort every eligible row per batch.
pub(crate) fn purge_terminal_batch(
    conn: &mut PgConnection,
    queue: &str,
    cutoff: f64,
    limit: i64,
) -> Result<usize, Error> {
    // Select the batch once. A locking LIMIT subquery inside `IN (...)` can be
    // planned on the inner side of a nested-loop semi-join and run again for
    // every outer row; each run skips the rows this statement already deleted,
    // so one batch could delete the whole backlog.
    sql_query(format!(
        "WITH candidates AS MATERIALIZED (
            SELECT id FROM apalis.jobs
            WHERE job_type = $1
                AND {TERMINAL_PREDICATE}
                AND EXTRACT(EPOCH FROM COALESCE(done_at, run_at)) <= $2
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM apalis.jobs USING candidates
        WHERE apalis.jobs.id = candidates.id"
    ))
    .bind::<Text, _>(queue)
    .bind::<Double, _>(cutoff)
    .bind::<BigInt, _>(limit)
    .execute(conn)
    .map_err(Error::database("purging terminal tasks"))
}

/// Delete every terminal row of `queue` completed at least `completed_before`
/// ago, in bounded batches, and return how many rows were deleted.
pub(crate) fn purge_terminal_tasks(
    pool: PgPool,
    queue: String,
    completed_before: Duration,
) -> impl Future<Output = Result<usize, Error>> + Send {
    purge_terminal_tasks_batched(pool, queue, completed_before, PURGE_BATCH_LIMIT)
}

/// [`purge_terminal_tasks`] with an explicit batch size: statements repeat,
/// each on its own pooled connection, until one deletes fewer rows than
/// `limit`. A limit below one would never terminate and is raised to one.
pub(crate) async fn purge_terminal_tasks_batched(
    pool: PgPool,
    queue: String,
    completed_before: Duration,
    limit: i64,
) -> Result<usize, Error> {
    let limit = limit.max(1);
    let cutoff = with_conn(pool.clone(), move |conn| {
        server_cutoff(conn, completed_before)
    })
    .await?;
    let mut total = 0;
    loop {
        let batch_queue = queue.clone();
        let deleted = with_conn(pool.clone(), move |conn| {
            purge_terminal_batch(conn, &batch_queue, cutoff, limit)
        })
        .await?;
        total += deleted;
        if i64::try_from(deleted).is_ok_and(|deleted| deleted < limit) {
            return Ok(total);
        }
    }
}

/// A prune window shorter than the queue's stale deadline would delete a
/// registration that the protocol still treats as live: a worker whose
/// heartbeat merely lags is stale only after `reenqueue_orphaned_after`, and
/// deleting its row before that turns its next renewal into
/// `WorkerNotRegistered`. Released rows carry an epoch heartbeat and pass
/// any window.
pub(crate) fn validate_prune_window(config: &Config, stale_for: Duration) -> Result<(), Error> {
    let stale_after = config.reenqueue_orphaned_after();
    if stale_for < stale_after {
        return Err(Error::InvalidArgument(format!(
            "stale_for ({stale_for:?}) must not be shorter than reenqueue_orphaned_after ({stale_after:?}): a registration is stale only after that deadline, and pruning it earlier would remove a live worker"
        )));
    }
    Ok(())
}

/// Delete one batch of registrations of `queue` that have been stale for at
/// least `stale_for` and that no job references any more. A row another
/// transaction holds (a claim, a takeover, a release) is skipped rather than
/// waited for: it is in use by definition. The foreign key from
/// `apalis.jobs.lock_by` remains the last line of defense, so a row that
/// still owns history cannot disappear.
pub(crate) fn prune_workers_batch(
    conn: &mut PgConnection,
    queue: &str,
    stale_for: Duration,
    limit: i64,
) -> Result<usize, Error> {
    // Select the batch once, for the reason given in `purge_terminal_batch`.
    sql_query(
        "WITH candidates AS MATERIALIZED (
            SELECT w.id, w.worker_type FROM apalis.workers w
            WHERE w.worker_type = $1
                AND EXTRACT(EPOCH FROM (clock_timestamp() - w.last_seen)) >= $2
                AND NOT EXISTS (
                    SELECT 1 FROM apalis.jobs j
                    WHERE j.job_type = w.worker_type AND j.lock_by = w.id
                )
            ORDER BY w.id
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM apalis.workers USING candidates
        WHERE apalis.workers.id = candidates.id
            AND apalis.workers.worker_type = candidates.worker_type",
    )
    .bind::<Text, _>(queue)
    .bind::<Double, _>(timeout_seconds(stale_for))
    .bind::<BigInt, _>(limit)
    .execute(conn)
    .map_err(Error::database("pruning stale workers"))
}

/// Delete every prunable registration of the configured queue, in bounded
/// batches, and return how many rows were deleted.
pub(crate) async fn prune_workers(
    pool: PgPool,
    config: Config,
    stale_for: Duration,
) -> Result<usize, Error> {
    validate_prune_window(&config, stale_for)?;
    let queue = config.queue().to_string();
    let mut total = 0;
    loop {
        let batch_queue = queue.clone();
        let deleted = with_conn(pool.clone(), move |conn| {
            prune_workers_batch(conn, &batch_queue, stale_for, PRUNE_BATCH_LIMIT)
        })
        .await?;
        total += deleted;
        if i64::try_from(deleted).is_ok_and(|deleted| deleted < PRUNE_BATCH_LIMIT) {
            return Ok(total);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lets_expect::{AssertionError, AssertionResult, *};

    fn prune_window(stale_for_secs: u64, deadline_secs: u64) -> Result<(), Error> {
        validate_prune_window(
            &Config::new("prune").set_reenqueue_orphaned_after(Duration::from_secs(deadline_secs)),
            Duration::from_secs(stale_for_secs),
        )
    }

    fn refused_as_too_short(error: &Error) -> AssertionResult {
        match error {
            Error::InvalidArgument(message) if message.contains("must not be shorter") => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected the window to be refused as too short, got {other:?}"
            )])),
        }
    }

    lets_expect! {
        expect(prune_window(stale_for_secs, deadline_secs)) as a_prune_window {
            let stale_for_secs = 600;
            let deadline_secs = 300;
            to is_accepted_when_longer_than_the_stale_deadline { be_ok }
            when the_window_equals_the_deadline {
                let stale_for_secs = 300;
                to is_accepted { be_ok }
            }
            when the_window_is_shorter_than_the_deadline {
                let stale_for_secs = 299;
                to is_refused_before_touching_the_database { be_err_and refused_as_too_short }
            }
            when the_window_is_zero {
                let stale_for_secs = 0;
                to is_refused_before_touching_the_database { be_err_and refused_as_too_short }
            }
        }
    }
}
