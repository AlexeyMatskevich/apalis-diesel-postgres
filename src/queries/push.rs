use apalis_sql::{DateTime, DateTimeExt};
use diesel::{
    Connection, PgConnection, RunQueryDsl, sql_query,
    sql_types::{Array, Binary, Integer, Nullable, Text, Timestamptz},
};
use ulid::Ulid;

use crate::{CompactType, Config, Error, PgPool, PgTask, queries::with_conn};

/// Cap on serialized task `metadata` JSON. Matches the `last_result` cap
/// (`MAX_ERROR_PAYLOAD_LEN` in `src/ack.rs`): unbounded JSONB on `apalis.jobs`
/// is a storage-exhaustion vector for any caller able to enqueue tasks.
pub(crate) const MAX_METADATA_PAYLOAD_LEN: usize = 8 * 1024;

/// Cap caller-supplied queue names before persisting them as `job_type` and
/// echoing them into NOTIFY JSON payloads. 255 bytes keeps queue names in the
/// same practical envelope as database identifiers while leaving ample room for
/// Rust type names and namespaced application queues.
pub(crate) const MAX_QUEUE_NAME_LEN: usize = 255;

/// Cap caller-supplied `idempotency_key` values before persisting them to the
/// unbounded `TEXT` column. Common idempotency keys are UUIDs (36 bytes), ULIDs
/// (26 bytes), SHA-256 hex digests (64 bytes), or 128-byte content hashes; 1024
/// bytes leaves room for prefixed/composite keys without allowing unbounded
/// per-row storage growth.
pub(crate) const MAX_IDEMPOTENCY_KEY_LEN: usize = 1024;

pub(crate) fn push_tasks(
    pool: PgPool,
    config: Config,
    tasks: Vec<PgTask<CompactType>>,
) -> impl Future<Output = Result<(), Error>> + Send {
    with_conn(pool, move |conn| push_tasks_on_conn(conn, &config, tasks))
}

/// Synchronous, connection-bound batch enqueue. Holds the `INSERT ... ON
/// CONFLICT DO NOTHING` and the post-check `inserted < task_count` together
/// inside `conn.transaction(...)` so that an `idempotency_key` conflict
/// rolls the partial INSERT back even when the caller already runs inside
/// its own outer transaction (Diesel uses a SAVEPOINT in that case). Without
/// this inner wrapper, a caller that catches the conflict error and commits
/// the outer transaction would silently keep the partially-inserted batch.
pub(crate) fn push_tasks_on_conn(
    conn: &mut PgConnection,
    config: &Config,
    tasks: Vec<PgTask<CompactType>>,
) -> Result<(), Error> {
    if tasks.is_empty() {
        return Ok(());
    }

    let job_type = config.queue().to_string();
    if job_type.len() > MAX_QUEUE_NAME_LEN {
        return Err(Error::InvalidArgument(format!(
            "queue name is {} bytes, exceeds the {MAX_QUEUE_NAME_LEN}-byte cap",
            job_type.len(),
        )));
    }

    let mut ids = Vec::with_capacity(tasks.len());
    let mut jobs = Vec::with_capacity(tasks.len());
    let mut max_attempts = Vec::with_capacity(tasks.len());
    let mut run_ats = Vec::with_capacity(tasks.len());
    let mut priorities = Vec::with_capacity(tasks.len());
    let mut metadata = Vec::with_capacity(tasks.len());
    let mut idempotency_keys = Vec::with_capacity(tasks.len());

    for task in tasks {
        ids.push(
            task.parts
                .task_id
                .map(|task_id| task_id.to_string())
                .unwrap_or_else(|| Ulid::new().to_string()),
        );
        jobs.push(task.args);
        max_attempts.push(task.parts.ctx.max_attempts());
        let run_at_secs = i64::try_from(task.parts.run_at).map_err(|_| {
            Error::InvalidArgument(format!(
                "run_at {} exceeds i64::MAX seconds and cannot be stored",
                task.parts.run_at
            ))
        })?;
        run_ats.push(<DateTime as DateTimeExt>::from_unix_timestamp(run_at_secs));
        priorities.push(task.parts.ctx.priority());
        // Serialize metadata once into the text representation we hand to
        // Postgres (cast to jsonb in the SELECT below). Previously this
        // path serialized twice — once for the byte-length check and again
        // when diesel encoded `Value` as `Jsonb` at bind time.
        let meta_json = serde_json::to_string(task.parts.ctx.meta())
            .map_err(|err| Error::InvalidArgument(format!("serializing task metadata: {err}")))?;
        if meta_json.len() > MAX_METADATA_PAYLOAD_LEN {
            return Err(Error::InvalidArgument(format!(
                "task metadata is {} bytes, exceeds the {MAX_METADATA_PAYLOAD_LEN}-byte cap",
                meta_json.len(),
            )));
        }
        metadata.push(meta_json);
        let idempotency_key = task.parts.idempotency_key;
        if let Some(key) = idempotency_key.as_deref()
            && key.len() > MAX_IDEMPOTENCY_KEY_LEN
        {
            return Err(Error::InvalidArgument(format!(
                "idempotency_key is {} bytes, exceeds the {MAX_IDEMPOTENCY_KEY_LEN}-byte cap",
                key.len(),
            )));
        }
        idempotency_keys.push(idempotency_key);
    }

    let task_count = ids.len();
    let any_idempotency_key = idempotency_keys.iter().any(Option::is_some);
    conn.transaction(|conn| {
        let inserted = sql_query(
            "INSERT INTO apalis.jobs (
                id,
                job_type,
                job,
                status,
                attempts,
                max_attempts,
                run_at,
                priority,
                metadata,
                idempotency_key
            )
            SELECT
                unnest($1::text[]) AS id,
                $2::text AS job_type,
                unnest($3::bytea[]) AS job,
                'Pending' AS status,
                0 AS attempts,
                unnest($4::integer[]) AS max_attempts,
                unnest($5::timestamptz[]) AS run_at,
                unnest($6::integer[]) AS priority,
                unnest($7::text[])::jsonb AS metadata,
                unnest($8::text[]) AS idempotency_key
            ON CONFLICT (job_type, idempotency_key)
                WHERE idempotency_key IS NOT NULL
                DO NOTHING",
        )
        .bind::<Array<Text>, _>(ids)
        .bind::<Text, _>(job_type)
        .bind::<Array<Binary>, _>(jobs)
        .bind::<Array<Integer>, _>(max_attempts)
        .bind::<Array<Timestamptz>, _>(run_ats)
        .bind::<Array<Integer>, _>(priorities)
        .bind::<Array<Text>, _>(metadata)
        .bind::<Array<Nullable<Text>>, _>(idempotency_keys)
        .execute(conn)
        .map_err(Error::database("inserting jobs"))?;
        // Surface ON CONFLICT DO NOTHING as an error to the caller when
        // the batch carried any `idempotency_key`: silent dedup makes the
        // caller unable to distinguish a fresh enqueue from a rejected
        // duplicate. Without an `idempotency_key`, no conflict path is
        // possible, so the inserted count must equal the batch.
        if inserted < task_count && any_idempotency_key {
            return Err(Error::InvalidArgument(format!(
                "idempotency_key conflict: {} of {} tasks were rejected by the unique constraint",
                task_count - inserted,
                task_count,
            )));
        }
        Ok(())
    })
}
