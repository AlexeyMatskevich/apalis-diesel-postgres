use std::collections::{HashMap, HashSet};

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

/// Cap the encoded task payload (`task.args`, stored in the unbounded `job`
/// BYTEA column) before persisting it. Like `metadata`, the queue name, and
/// `idempotency_key`, an unbounded payload is a storage-exhaustion vector for
/// any caller able to enqueue tasks — and the payload is the field most likely
/// to be large, so it is the most important one to bound. 1 MiB is generous
/// enough for structured job arguments while keeping per-row growth in check;
/// jobs that must carry more should store the blob externally (object storage,
/// a dedicated table) and enqueue a reference instead.
pub(crate) const MAX_JOB_PAYLOAD_LEN: usize = 1024 * 1024;

/// One `RETURNING idempotency_key` row from the batch INSERT: the keys that
/// actually landed (ON CONFLICT DO NOTHING skips duplicates). Used to recover
/// which submitted keys collided.
#[derive(diesel::QueryableByName)]
struct ReturnedIdempotencyKey {
    #[diesel(sql_type = Nullable<Text>)]
    idempotency_key: Option<String>,
}

/// Batch INSERT shared by both enqueue paths. The conflict-recovery path
/// appends `RETURNING idempotency_key` to this literal; keeping one copy of
/// the statement (and one bind site, [`JobBatchBinds::into_query`]) prevents
/// the two paths from drifting apart.
const INSERT_JOBS_SQL: &str = "INSERT INTO apalis.jobs (
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
        DO NOTHING";

/// Column-major bind arrays for [`INSERT_JOBS_SQL`], collected by the prep
/// loop in [`push_tasks_on_conn`]. Bundled into a struct so the `$1..$8`
/// bind order lives in exactly one place regardless of which SQL tail
/// (with or without `RETURNING`) executes.
struct JobBatchBinds {
    ids: Vec<String>,
    job_type: String,
    jobs: Vec<CompactType>,
    max_attempts: Vec<i32>,
    run_ats: Vec<DateTime>,
    priorities: Vec<i32>,
    metadata: Vec<String>,
    idempotency_keys: Vec<Option<String>>,
}

impl JobBatchBinds {
    fn into_query(
        self,
        sql: String,
    ) -> diesel::query_builder::BoxedSqlQuery<
        'static,
        diesel::pg::Pg,
        diesel::query_builder::SqlQuery,
    > {
        sql_query(sql)
            .into_boxed()
            .bind::<Array<Text>, _>(self.ids)
            .bind::<Text, _>(self.job_type)
            .bind::<Array<Binary>, _>(self.jobs)
            .bind::<Array<Integer>, _>(self.max_attempts)
            .bind::<Array<Timestamptz>, _>(self.run_ats)
            .bind::<Array<Integer>, _>(self.priorities)
            .bind::<Array<Text>, _>(self.metadata)
            .bind::<Array<Nullable<Text>>, _>(self.idempotency_keys)
    }
}

pub(crate) fn push_tasks(
    pool: PgPool,
    config: Config,
    tasks: Vec<PgTask<CompactType>>,
) -> impl Future<Output = Result<(), Error>> + Send {
    with_conn(pool, move |conn| push_tasks_pooled(conn, &config, tasks))
}

/// Pool-path batch enqueue — the sink's flush hot path.
///
/// The connection is freshly checked out from the pool and is never inside a
/// caller transaction, so a batch without idempotency keys runs as one bare,
/// statement-atomic INSERT with no BEGIN/COMMIT round-trips, `RETURNING`
/// materialization, or key copies. Keyed batches share the conflict-recovery
/// transaction with the outbox path.
fn push_tasks_pooled(
    conn: &mut PgConnection,
    config: &Config,
    tasks: Vec<PgTask<CompactType>>,
) -> Result<(), Error> {
    let Some(batch) = prepare_batch(config, tasks)? else {
        return Ok(());
    };
    if batch.any_idempotency_key {
        conn.transaction(|conn| insert_reporting_conflicts(conn, batch))
    } else {
        insert_batch(conn, batch)
    }
}

/// Synchronous, connection-bound batch enqueue for the outbox API
/// ([`crate::PostgresStorage::push_with_conn`] /
/// [`crate::PostgresStorage::push_task_with_conn`]).
///
/// The caller may run inside its own transaction, so every batch — keyed or
/// not — executes inside `conn.transaction(...)` (a SAVEPOINT in that case):
/// any insert error, be it an idempotency conflict or a PK violation on a
/// caller-supplied task id, rolls back only this batch and leaves the outer
/// transaction usable. Without the wrapper a failing INSERT would abort the
/// caller's transaction outright, and a caught idempotency conflict would
/// silently keep the partially-inserted batch.
pub(crate) fn push_tasks_on_conn(
    conn: &mut PgConnection,
    config: &Config,
    tasks: Vec<PgTask<CompactType>>,
) -> Result<(), Error> {
    let Some(batch) = prepare_batch(config, tasks)? else {
        return Ok(());
    };
    if batch.any_idempotency_key {
        conn.transaction(|conn| insert_reporting_conflicts(conn, batch))
    } else {
        // No key ⇒ no conflict bookkeeping needed, but the SAVEPOINT
        // protection for the caller's outer transaction still applies.
        conn.transaction(|conn| insert_batch(conn, batch))
    }
}

/// A validated, column-major batch ready to bind. Produced by
/// [`prepare_batch`], consumed by [`insert_batch`] /
/// [`insert_reporting_conflicts`].
struct PreparedBatch {
    binds: JobBatchBinds,
    task_count: usize,
    any_idempotency_key: bool,
}

/// Validate caps and collect the batch into column-major bind arrays.
/// Returns `None` for an empty batch.
fn prepare_batch(
    config: &Config,
    tasks: Vec<PgTask<CompactType>>,
) -> Result<Option<PreparedBatch>, Error> {
    if tasks.is_empty() {
        return Ok(None);
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
        if task.args.len() > MAX_JOB_PAYLOAD_LEN {
            return Err(Error::InvalidArgument(format!(
                "task payload is {} bytes, exceeds the {MAX_JOB_PAYLOAD_LEN}-byte cap",
                task.args.len(),
            )));
        }
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
    Ok(Some(PreparedBatch {
        binds: JobBatchBinds {
            ids,
            job_type,
            jobs,
            max_attempts,
            run_ats,
            priorities,
            metadata,
            idempotency_keys,
        },
        task_count,
        any_idempotency_key,
    }))
}

/// Bare batch INSERT for key-less batches: the partial unique index
/// `(job_type, idempotency_key) WHERE idempotency_key IS NOT NULL` cannot
/// conflict when every key is NULL, so no `RETURNING` bookkeeping is needed.
fn insert_batch(conn: &mut PgConnection, batch: PreparedBatch) -> Result<(), Error> {
    batch
        .binds
        .into_query(INSERT_JOBS_SQL.to_owned())
        .execute(conn)
        .map_err(Error::database("inserting jobs"))?;
    Ok(())
}

/// Keyed-batch INSERT with the `inserted < task_count` conflict accountant.
///
/// Must run inside `conn.transaction(...)`: returning `Err` relies on the
/// surrounding rollback to undo the rows `ON CONFLICT DO NOTHING` already
/// inserted, so a single duplicate undoes the *entire* batch while the
/// caller's outer transaction stays alive. The typed `IdempotencyConflict`
/// lets callers branch on the variant instead of parsing the message text —
/// silent dedup would make a fresh enqueue indistinguishable from a rejected
/// duplicate.
fn insert_reporting_conflicts(conn: &mut PgConnection, batch: PreparedBatch) -> Result<(), Error> {
    // `job_type` and `idempotency_keys` are moved into the INSERT bind below;
    // keep copies so the conflict branch can name the queue and report exactly
    // which keys collided in `Error::IdempotencyConflict`.
    let conflict_job_type = batch.binds.job_type.clone();
    let submitted_keys: Vec<String> = batch
        .binds
        .idempotency_keys
        .iter()
        .flatten()
        .cloned()
        .collect();
    let inserted_rows = batch
        .binds
        .into_query(format!(
            "{INSERT_JOBS_SQL}\n            RETURNING idempotency_key"
        ))
        .load::<ReturnedIdempotencyKey>(conn)
        .map_err(Error::database("inserting jobs"))?;
    let inserted = inserted_rows.len();
    if inserted < batch.task_count {
        // Recover the distinct keys that collided. Walk the submitted keys
        // and consume one matching inserted row per key; any submission left
        // without a matching inserted row is a collision (intra-batch or
        // against an already-stored row).
        let mut inserted_remaining: HashMap<&str, usize> = HashMap::new();
        for row in &inserted_rows {
            if let Some(key) = row.idempotency_key.as_deref() {
                *inserted_remaining.entry(key).or_insert(0) += 1;
            }
        }
        let mut seen: HashSet<&str> = HashSet::new();
        let mut conflicting_keys: Vec<String> = Vec::new();
        for key in &submitted_keys {
            let inserted_here = inserted_remaining
                .get_mut(key.as_str())
                .is_some_and(|count| {
                    if *count > 0 {
                        *count -= 1;
                        true
                    } else {
                        false
                    }
                });
            if !inserted_here && seen.insert(key.as_str()) {
                conflicting_keys.push(key.clone());
            }
        }
        return Err(Error::idempotency_conflict(
            conflict_job_type,
            conflicting_keys,
            batch.task_count,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use lets_expect::{AssertionError, AssertionResult, *};

    use super::*;

    fn prepare_batch_for_payload(len: usize) -> Result<Option<PreparedBatch>, Error> {
        let config = Config::new("payload-cap");
        let task = PgTask::<CompactType>::new(vec![0_u8; len]);
        prepare_batch(&config, vec![task])
    }

    fn accepts_one_task(result: &Result<Option<PreparedBatch>, Error>) -> AssertionResult {
        match result {
            Ok(Some(batch)) if batch.task_count == 1 => Ok(()),
            Ok(Some(batch)) => Err(AssertionError::new(vec![format!(
                "expected a prepared batch of one task, got {}",
                batch.task_count
            )])),
            Ok(None) => Err(AssertionError::new(vec![
                "expected a prepared batch, got an empty batch".to_owned(),
            ])),
            Err(error) => Err(AssertionError::new(vec![format!(
                "expected a prepared batch, got error: {error:?}"
            )])),
        }
    }

    fn rejects_with_payload_cap(result: &Result<Option<PreparedBatch>, Error>) -> AssertionResult {
        match result {
            Err(Error::InvalidArgument(message))
                if message.contains("task payload") && message.contains("cap") =>
            {
                Ok(())
            }
            Err(error) => Err(AssertionError::new(vec![format!(
                "expected an InvalidArgument citing the task payload cap, got a different error: {error:?}"
            )])),
            Ok(_) => Err(AssertionError::new(vec![
                "expected the payload cap error, but the oversized payload was accepted".to_owned(),
            ])),
        }
    }

    lets_expect! {
        expect(prepare_batch_for_payload(len)) {
            let len = 128;

            when the_payload_is_well_below_the_cap {
                to accepts_the_task { accepts_one_task }
            }

            when the_payload_is_exactly_at_the_cap {
                let len = MAX_JOB_PAYLOAD_LEN;
                to accepts_the_boundary_task { accepts_one_task }
            }

            when the_payload_is_one_byte_over_the_cap {
                let len = MAX_JOB_PAYLOAD_LEN + 1;
                to rejects_the_oversized_payload { rejects_with_payload_cap }
            }
        }
    }
}
