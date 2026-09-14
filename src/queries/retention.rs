//! End of life for queue data. Completed history and released registrations
//! are never removed by the worker protocol; these are the two explicit
//! retention operations, both scoped to one queue and both batched, so no
//! statement holds more than a bounded number of rows and every batch is a
//! cancellation point that returns its connection to the pool.

use std::time::Duration;

use diesel::{
    Connection, PgConnection, QueryableByName, RunQueryDsl, sql_query,
    sql_types::{Array, BigInt, Double, Text},
};

use crate::{
    Config, Error, PgPool,
    queries::{TERMINAL_PREDICATE, with_conn, worker::timeout_seconds},
};

/// Rows deleted per statement by [`purge_terminal_tasks`]. Each statement is
/// its own transaction, so the locks and WAL of one purge never cover more
/// than this many rows at once; a larger backlog drains across statements.
pub(crate) const PURGE_BATCH_LIMIT: i64 = 10_000;

/// Registrations locked, and then deleted, per batch by [`prune_workers`].
/// Every deleted row also runs the foreign-key probe over the queue's
/// history, so the batch stays as small as the orphan sweep's.
pub(crate) const PRUNE_BATCH_LIMIT: i64 = 1_000;

/// Epoch seconds of the earliest timestamp PostgreSQL represents, 24 November
/// 4714 BC; `to_timestamp` refuses anything earlier.
const TIMESTAMP_FLOOR_EPOCH: &str = "-210866803200";

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
    // so one batch could delete the whole backlog. The age filter compares the
    // columns themselves, so the `(job_type, done_at)` and `run_at` indexes
    // serve it and a queue with nothing to purge is not scanned; the cutoff is
    // clamped to the earliest timestamp PostgreSQL represents, so a window
    // longer than that range selects nothing instead of failing. The batch is
    // deleted by primary key.
    sql_query(format!(
        "WITH candidates AS MATERIALIZED (
            SELECT id FROM apalis.jobs
            WHERE job_type = $1
                AND {TERMINAL_PREDICATE}
                AND (done_at <= to_timestamp(GREATEST($2, {TIMESTAMP_FLOOR_EPOCH}))
                    OR (done_at IS NULL
                        AND run_at <= to_timestamp(GREATEST($2, {TIMESTAMP_FLOOR_EPOCH}))))
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        )
        DELETE FROM apalis.jobs WHERE id = ANY(ARRAY(SELECT id FROM candidates))"
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
            "stale_for ({stale_for:?}) must not be shorter than reenqueue_orphaned_after ({stale_after:?}): a registration is stale only after that deadline, and pruning it earlier would remove a live worker; when workers of the queue run with different deadlines, use the longest"
        )));
    }
    Ok(())
}

/// Delete one batch of registrations of `queue` that have been stale for at
/// least `stale_for` and that no job references any more, and return how
/// many were deleted. A row another transaction holds (a claim, a takeover, a
/// release) is skipped rather than waited for: it is in use by definition. A
/// registration claimed while the batch selects its candidates is kept.
#[cfg(all(test, feature = "tokio"))]
pub(crate) fn prune_workers_batch(
    conn: &mut PgConnection,
    queue: &str,
    stale_for: Duration,
    limit: i64,
) -> Result<usize, Error> {
    prune_workers_batch_counted(conn, queue, stale_for, limit).map(|batch| batch.deleted)
}

/// How many registrations one prune batch locked and how many it deleted.
struct PruneBatch {
    locked: usize,
    deleted: usize,
}

fn prune_workers_batch_counted(
    conn: &mut PgConnection,
    queue: &str,
    stale_for: Duration,
    limit: i64,
) -> Result<PruneBatch, Error> {
    #[derive(QueryableByName)]
    struct Candidate {
        #[diesel(sql_type = Text)]
        id: String,
    }
    conn.transaction(|tx| {
        // Lock the batch first. The reference check here reads this
        // statement's snapshot, so a claim that commits before a candidate is
        // locked can still reference it; deleting in the same statement would
        // then fail the whole batch on the foreign key. The delete checks
        // again under a later snapshot, taken after the locks, which no new
        // claim can pass because a claim needs a share lock on the
        // registration. A plain locking SELECT runs once, so the batch keeps
        // its limit. The reference checks here and in the delete are
        // correlated scalar subqueries, so each candidate probes
        // `jobs_job_type_lock_by_idx`; `NOT EXISTS` is planned as an
        // anti-join over the queue's whole history.
        let candidates = sql_query(
            "SELECT w.id FROM apalis.workers w
             WHERE w.worker_type = $1
                 AND EXTRACT(EPOCH FROM (clock_timestamp() - w.last_seen)) >= $2
                 AND NOT (SELECT EXISTS (
                     SELECT 1 FROM apalis.jobs j
                     WHERE j.job_type = w.worker_type AND j.lock_by = w.id
                 ))
             ORDER BY w.id
             LIMIT $3
             FOR UPDATE SKIP LOCKED",
        )
        .bind::<Text, _>(queue)
        .bind::<Double, _>(timeout_seconds(stale_for))
        .bind::<BigInt, _>(limit)
        .load::<Candidate>(tx)
        .map_err(Error::database("locking stale workers to prune"))?;
        let locked = candidates.len();
        let deleted = if locked == 0 {
            0
        } else {
            delete_unreferenced_registrations(
                tx,
                queue,
                candidates
                    .into_iter()
                    .map(|candidate| candidate.id)
                    .collect(),
            )?
        };
        Ok(PruneBatch { locked, deleted })
    })
}

/// Delete the registrations `ids` of `queue` that no job references under
/// this statement's snapshot. Prune calls it with those registrations locked,
/// so no claim can reference one between the check and the delete.
pub(crate) fn delete_unreferenced_registrations(
    conn: &mut PgConnection,
    queue: &str,
    ids: Vec<String>,
) -> Result<usize, Error> {
    sql_query(
        "DELETE FROM apalis.workers w
         WHERE w.worker_type = $1
             AND w.id = ANY($2)
             AND NOT (SELECT EXISTS (
                 SELECT 1 FROM apalis.jobs j
                 WHERE j.job_type = w.worker_type AND j.lock_by = w.id
             ))",
    )
    .bind::<Text, _>(queue)
    .bind::<Array<Text>, _>(ids)
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
        let batch = with_conn(pool.clone(), move |conn| {
            prune_workers_batch_counted(conn, &batch_queue, stale_for, PRUNE_BATCH_LIMIT)
        })
        .await?;
        total += batch.deleted;
        // A batch that locked fewer rows than its limit found every prunable
        // row. One whose candidates were claimed meanwhile deletes fewer than
        // it locked, and the next batch goes on.
        if i64::try_from(batch.locked).is_ok_and(|locked| locked < PRUNE_BATCH_LIMIT) {
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
