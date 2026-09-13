//! End of life for queue data. Completed history and released registrations
//! are never removed by the worker protocol; these are the two explicit
//! retention operations, both scoped to one queue.

use std::time::Duration;

use diesel::{
    PgConnection, RunQueryDsl, sql_query,
    sql_types::{BigInt, Double, Text},
};

use crate::{Error, PgPool, queries::with_conn};

/// Rows deleted per statement by [`purge_terminal_tasks`]. Each statement is
/// its own transaction, so the locks and WAL of one purge never cover more
/// than this many rows at once; a larger backlog drains across statements.
pub(crate) const PURGE_BATCH_LIMIT: i64 = 10_000;

fn seconds(duration: Duration) -> f64 {
    duration.as_secs_f64()
}

/// Delete one batch of terminal rows of `queue` whose completion is at least
/// `completed_before` old. A terminal row without a completion time (written
/// outside this crate) is aged by its schedule instead.
pub(crate) fn purge_terminal_batch(
    conn: &mut PgConnection,
    queue: &str,
    completed_before: Duration,
    limit: i64,
) -> Result<usize, Error> {
    sql_query(
        "DELETE FROM apalis.jobs WHERE id IN (
            SELECT id FROM apalis.jobs
            WHERE job_type = $1
                AND (status IN ('Done', 'Killed')
                     OR (status = 'Failed' AND attempts >= max_attempts))
                AND EXTRACT(EPOCH FROM (clock_timestamp() - COALESCE(done_at, run_at))) >= $2
            ORDER BY id
            LIMIT $3
            FOR UPDATE SKIP LOCKED
        )",
    )
    .bind::<Text, _>(queue)
    .bind::<Double, _>(seconds(completed_before))
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

/// [`purge_terminal_tasks`] with an explicit batch size: statements repeat
/// until one deletes fewer rows than `limit`.
pub(crate) fn purge_terminal_tasks_batched(
    pool: PgPool,
    queue: String,
    completed_before: Duration,
    limit: i64,
) -> impl Future<Output = Result<usize, Error>> + Send {
    with_conn(pool, move |conn| {
        let mut total = 0;
        loop {
            let deleted = purge_terminal_batch(conn, &queue, completed_before, limit)?;
            total += deleted;
            if i64::try_from(deleted).is_ok_and(|deleted| deleted < limit) {
                return Ok(total);
            }
        }
    })
}

/// Delete the registrations of `queue` that have been stale for at least
/// `stale_for` and that no job references any more. A row another
/// transaction holds (a claim, a takeover, a release) is skipped rather than
/// waited for: it is in use by definition. The foreign key from
/// `apalis.jobs.lock_by` remains the last line of defense, so a row that
/// still owns history cannot disappear.
pub(crate) fn prune_workers_blocking(
    conn: &mut PgConnection,
    queue: &str,
    stale_for: Duration,
) -> Result<usize, Error> {
    sql_query(
        "DELETE FROM apalis.workers WHERE (id, worker_type) IN (
            SELECT w.id, w.worker_type FROM apalis.workers w
            WHERE w.worker_type = $1
                AND EXTRACT(EPOCH FROM (clock_timestamp() - w.last_seen)) >= $2
                AND NOT EXISTS (
                    SELECT 1 FROM apalis.jobs j
                    WHERE j.job_type = w.worker_type AND j.lock_by = w.id
                )
            FOR UPDATE SKIP LOCKED
        )",
    )
    .bind::<Text, _>(queue)
    .bind::<Double, _>(seconds(stale_for))
    .execute(conn)
    .map_err(Error::database("pruning stale workers"))
}

pub(crate) fn prune_workers(
    pool: PgPool,
    queue: String,
    stale_for: Duration,
) -> impl Future<Output = Result<usize, Error>> + Send {
    with_conn(pool, move |conn| {
        prune_workers_blocking(conn, &queue, stale_for)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use lets_expect::*;

    lets_expect! {
        expect(seconds(duration)) as a_retention_window {
            let duration = Duration::from_secs(3_600);
            to is_compared_in_seconds { equal(3_600.0) }
            when the_window_is_zero {
                let duration = Duration::ZERO;
                to matches_every_completed_row { equal(0.0) }
            }
            when the_window_has_a_fractional_second {
                let duration = Duration::from_millis(1_500);
                to keeps_the_fraction { equal(1.5) }
            }
            when the_window_is_the_largest_duration {
                let duration = Duration::MAX;
                to stays_representable_without_interval_overflow { equal(Duration::MAX.as_secs_f64()) }
            }
        }
    }
}
