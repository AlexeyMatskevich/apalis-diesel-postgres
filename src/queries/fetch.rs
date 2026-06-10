use apalis_core::worker::context::WorkerContext;
use diesel::{
    RunQueryDsl, sql_query,
    sql_types::{Array, BigInt, Integer, Jsonb, Nullable, Text},
};
use ulid::Ulid;

use crate::{
    CompactType, Config, Error, PgPool, PgTask, PgTaskId,
    queries::{clamp_i32, task_rows, with_conn},
};

/// SQL predicate that identifies rows eligible for a fresh claim from a queue:
/// either `Pending`, or a `Failed` row whose retry budget is not yet exhausted.
/// Centralised here so `fetch_next` (poll path) and `queue_by_id` (notify
/// path) agree on which rows are considered claimable — drift between the two
/// historically allowed retries to "leak" through one path but not the other.
///
/// `lock_task` deliberately uses a *superset* of this predicate to remain
/// idempotent for the same worker (already-`Queued`/`Running` rows it owns),
/// so it does not share this constant.
const CLAIMABLE_PREDICATE: &str =
    "(status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts))";

pub(crate) fn fetch_next(
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
) -> impl Future<Output = Result<Vec<PgTask<CompactType>>, Error>> + Send {
    with_conn(pool, move |conn| {
        // `UPDATE ... FROM cte ... RETURNING` does not preserve the CTE's
        // ordering, so we wrap the UPDATE in an outer SELECT that re-applies
        // `ORDER BY priority DESC, run_at ASC`. This pushes the sort into
        // PostgreSQL (which already has the values in a tuplestore for the
        // RETURNING) instead of doing it in Rust after fetching the rows.
        let rows: Vec<crate::models::JobRow> = sql_query(format!(
            "WITH next_jobs AS (
                 SELECT id
                 FROM apalis.jobs
                 WHERE {CLAIMABLE_PREDICATE}
                     AND run_at <= now()
                     AND job_type = $2
                 ORDER BY priority DESC, run_at ASC
                 LIMIT $3
                 FOR UPDATE SKIP LOCKED
             ),
             updated AS (
                 -- H4: dequeue + lock used to be two round-trips
                 -- (fetch_next → Queued, then LockTaskService → Running).
                 -- Transition straight to `Running` in this CTE so the
                 -- subsequent `LockTaskService` call becomes idempotent
                 -- (`lock_task` accepts already-Running rows owned by the
                 -- same worker) and no second round-trip is needed per job.
                 UPDATE apalis.jobs
                 SET status = 'Running',
                     lock_by = $1,
                     lock_at = date_trunc('second', now()),
                     done_at = NULL
                 FROM next_jobs
                 WHERE apalis.jobs.id = next_jobs.id
                 RETURNING apalis.jobs.*
             )
             SELECT * FROM updated
             ORDER BY priority DESC, run_at ASC"
        ))
        .bind::<Text, _>(worker.name())
        .bind::<Text, _>(config.queue().to_string())
        .bind::<Integer, _>(clamp_i32(config.buffer_size().max(1)))
        .load(conn)
        .map_err(Error::database("fetching queued jobs"))?;
        task_rows(rows)
    })
}

pub(crate) fn queue_by_id(
    pool: PgPool,
    queue: String,
    ids: Vec<String>,
    worker_id: String,
) -> impl Future<Output = Result<Vec<PgTask<CompactType>>, Error>> + Send {
    with_conn(pool, move |conn| {
        // Mirror `fetch_next`'s eligibility (`Pending` OR retryable `Failed`)
        // so NOTIFY wakeups also pick up retried jobs; use `FOR UPDATE SKIP
        // LOCKED` to avoid serializing on rows another consumer is claiming.
        // `UPDATE ... RETURNING` does not preserve the CTE's ordering, so we
        // wrap it in an outer SELECT that re-applies the sort in SQL — same
        // pattern as `fetch_next`.
        let rows: Vec<crate::models::JobRow> = sql_query(format!(
            "WITH candidates AS (
                 SELECT id
                 FROM apalis.jobs
                 WHERE {CLAIMABLE_PREDICATE}
                     AND run_at <= now()
                     AND job_type = $2
                     AND id = ANY($3)
                 ORDER BY priority DESC, run_at ASC
                 FOR UPDATE SKIP LOCKED
             ),
             updated AS (
                 UPDATE apalis.jobs
                 SET status = 'Running',
                     lock_at = date_trunc('second', now()),
                     lock_by = $1,
                     done_at = NULL
                 FROM candidates
                 WHERE apalis.jobs.id = candidates.id
                 RETURNING apalis.jobs.*
             )
             SELECT * FROM updated
             ORDER BY priority DESC, run_at ASC"
        ))
        .bind::<Text, _>(worker_id)
        .bind::<Text, _>(queue)
        .bind::<Array<Text>, _>(ids)
        .load(conn)
        .map_err(Error::database("claiming notified jobs"))?;
        task_rows(rows)
    })
}

/// Release a row the dequeue SQL already claimed as `Running` but whose
/// payload failed to decode. Without this, a poisoned payload (codec drift, a
/// third-party insert) would strand the row in `Running` for as long as the
/// claiming worker keeps heartbeating: ack requires a decoded task, and
/// orphan recovery only reclaims rows of *stale* workers. Failing the attempt
/// instead routes the row through the normal retry budget — it stays
/// claimable (`Failed` with attempts left) until the budget is exhausted,
/// then turns terminal `Killed`, the same convention as `reenqueue_orphaned`.
///
/// The predicate pins the caller's exact claim epoch, mirroring `ack_task`:
/// `status`/`lock_by` alone would let a delayed release fire on a row that
/// was orphan-swept and re-claimed in the meantime (same worker name, new
/// claim), so `lock_at` and `attempts` from the claimed task must match too.
/// An ack, a sweep, or a re-claim that raced this call all leave the row
/// untouched.
pub(crate) fn fail_undecodable_task(
    pool: PgPool,
    task_id: PgTaskId,
    worker_id: String,
    lock_at: i64,
    attempts: i32,
    error: String,
) -> impl Future<Output = Result<(), Error>> + Send {
    with_conn(pool, move |conn| {
        // Same externally-tagged `Result<O, String>` JSON shape as ack's
        // `last_result`, so readers observe one format for every failure path.
        let result = serde_json::json!({ "Err": crate::ack::truncate_error_payload(error) });
        sql_query(
            "UPDATE apalis.jobs
             SET status = CASE
                     WHEN attempts + 1 >= max_attempts THEN 'Killed'
                     ELSE 'Failed'
                 END,
                 attempts = LEAST(attempts + 1, max_attempts),
                 done_at = now(),
                 last_result = $3
             WHERE id = $1
                 AND status = 'Running'
                 AND lock_by = $2
                 AND lock_at = to_timestamp($4::double precision)
                 AND attempts = $5",
        )
        .bind::<Text, _>(task_id.to_string())
        .bind::<Text, _>(worker_id)
        .bind::<Jsonb, _>(result)
        .bind::<BigInt, _>(lock_at)
        .bind::<Integer, _>(attempts)
        .execute(conn)
        .map_err(Error::database("failing undecodable task"))?;
        Ok(())
    })
}

pub(crate) fn lock_task(
    pool: PgPool,
    task_id: Ulid,
    worker_id: String,
    queue: Option<String>,
) -> impl Future<Output = Result<(), Error>> + Send {
    with_conn(pool, move |conn| {
        let task_id = task_id.to_string();
        let rows: Vec<crate::models::JobRow> = sql_query(
            "UPDATE apalis.jobs
             SET status = 'Running',
                 lock_at = CASE
                     WHEN status IN ('Queued', 'Running')
                          AND lock_by = $1 AND lock_at IS NOT NULL THEN lock_at
                     ELSE date_trunc('second', now())
                 END,
                 lock_by = $1,
                 done_at = NULL
             WHERE id = $2
                 AND ($3 IS NULL OR job_type = $3)
                 AND run_at <= now()
                 AND (
                     status = 'Pending'
                     OR (status = 'Queued' AND lock_by = $1)
                     OR (status = 'Running' AND lock_by = $1)
                     OR (status = 'Failed' AND attempts < max_attempts)
                 )
             RETURNING *",
        )
        .bind::<Text, _>(worker_id)
        .bind::<Text, _>(&task_id)
        .bind::<Nullable<Text>, _>(queue.clone())
        .load(conn)
        .map_err(Error::database("locking task"))?;
        if rows.is_empty() {
            Err(Error::task_not_found(
                "locking task",
                task_id,
                queue,
                "the task may be delayed, already locked by another worker, completed, or in another queue",
            ))
        } else {
            Ok(())
        }
    })
}
