use apalis_core::task::status::Status;
use diesel::{
    RunQueryDsl, sql_query,
    sql_types::{BigInt, Integer, Jsonb, Nullable, Text},
};

use crate::{Error, PgPool, PgTaskId, queries::with_conn};

pub(crate) struct AckTaskUpdate {
    pub(crate) task_id: PgTaskId,
    pub(crate) attempts: i32,
    pub(crate) started_attempts: i32,
    /// `None` writes SQL `NULL` to `last_result`.
    pub(crate) result: Option<serde_json::Value>,
    pub(crate) status: Status,
    pub(crate) worker_id: String,
    pub(crate) queue: String,
    pub(crate) lock_at: i64,
    /// When `Some`, the ack additionally requires the worker row to carry
    /// this exact `lease_token`. Matches the heartbeat-path defense and
    /// prevents ack spoofing by a caller who only knows `(task_id, queue,
    /// worker_id, lock_at, attempts)`. When `None`, ack falls back to the
    /// pre-lease-token predicate (callers without a token, e.g. admin).
    pub(crate) lease_token: Option<String>,
}

pub(crate) fn ack_task(
    pool: PgPool,
    update: AckTaskUpdate,
) -> impl Future<Output = Result<(), Error>> + Send {
    with_conn(pool, move |conn| {
        let task_id = update.task_id.to_string();
        let queue = update.queue;
        let worker_id = update.worker_id;
        // `$9::text IS NULL` short-circuits the EXISTS check for callers that
        // didn't supply a token; passing a token therefore adds defense in
        // depth without breaking pre-token call sites (tests, admin tooling).
        let count = sql_query(
            "UPDATE apalis.jobs
             SET status = $1, attempts = $2, last_result = $3, done_at = now()
             WHERE id = $4
                 AND job_type = $5
                 AND lock_by = $6
                 AND lock_at = to_timestamp($7::double precision)
                 AND attempts = $8
                 AND status = 'Running'
                 AND (
                     $9::text IS NULL
                     OR EXISTS (
                         SELECT 1 FROM apalis.workers
                         WHERE id = $6 AND worker_type = $5
                             AND lease_token = $9
                         FOR SHARE
                     )
                 )",
        )
        .bind::<Text, _>(update.status.to_string())
        .bind::<Integer, _>(update.attempts)
        .bind::<Nullable<Jsonb>, _>(update.result)
        .bind::<Text, _>(&task_id)
        .bind::<Text, _>(&queue)
        .bind::<Text, _>(&worker_id)
        .bind::<BigInt, _>(update.lock_at)
        .bind::<Integer, _>(update.started_attempts)
        .bind::<Nullable<Text>, _>(update.lease_token)
        .execute(conn)
        .map_err(Error::database("acknowledging task"))?;
        if count == 0 {
            Err(Error::stale_acknowledgement(task_id, queue, worker_id))
        } else {
            Ok(())
        }
    })
}
