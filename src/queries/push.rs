use std::collections::{HashMap, HashSet};

use apalis_sql::DateTime;
use diesel::{
    Connection, PgConnection, RunQueryDsl, sql_query,
    sql_types::{Array, Binary, Integer, Nullable, Text, Timestamptz},
};
use ulid::Ulid;

use crate::{CompactType, Config, Error, PgPool, PgTask};

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

/// Why a buffered flush did not write its batch, by the phase it reached.
pub(crate) enum FlushFailure {
    /// No statement was issued: the batch is handed back for a later flush.
    NotStarted {
        /// The batch, unchanged.
        tasks: Vec<PgTask<CompactType>>,
        /// The connection or executor failure.
        error: Error,
    },
    /// Validation refused the batch before any statement; nothing was written.
    Rejected(Error),
    /// A statement was issued; the batch may or may not have been written.
    Uncertain(Error),
}

enum FlushPhase {
    NotStarted(Error),
    Rejected(Error),
    Uncertain(Error),
}

impl FlushFailure {
    /// The underlying error, for specs that do not act on the phase.
    #[cfg(all(test, feature = "tokio"))]
    pub(crate) fn into_error(self) -> Error {
        match self {
            Self::NotStarted { error, .. } | Self::Rejected(error) | Self::Uncertain(error) => {
                error
            }
        }
    }
}

/// Pool-path batch enqueue, the sink's flush, with its failure classified by
/// phase so the sink fails permanently only after a statement was issued.
///
/// The connection is freshly checked out from the pool and is never inside a
/// caller transaction, so a batch without idempotency keys runs as one bare,
/// statement-atomic INSERT with no BEGIN/COMMIT round-trips, `RETURNING`
/// materialization, or key copies. Keyed batches share the conflict-recovery
/// transaction with the outbox path.
pub(crate) fn flush_tasks(
    pool: PgPool,
    config: Config,
    tasks: Vec<PgTask<CompactType>>,
) -> impl Future<Output = Result<(), FlushFailure>> + Send {
    // The batch stays in this slot until the blocking closure runs, so a
    // closure that never ran hands the batch back untouched.
    let pending = std::sync::Arc::new(std::sync::Mutex::new(Some(tasks)));
    let slot = pending.clone();
    let run = crate::runtime::run_blocking(move || {
        let mut conn = match pool.get() {
            Ok(conn) => conn,
            Err(error) => return Ok(Err(FlushPhase::NotStarted(error.into()))),
        };
        let tasks = slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .expect("a flush batch is taken exactly once");
        let batch = match prepare_batch(&config, tasks) {
            Ok(Some(batch)) => batch,
            Ok(None) => return Ok(Ok(())),
            Err(error) => return Ok(Err(FlushPhase::Rejected(error))),
        };
        let written = if batch.any_idempotency_key {
            conn.transaction(|conn| insert_reporting_conflicts(conn, batch))
        } else {
            insert_batch(&mut conn, batch)
        };
        Ok(written.map_err(FlushPhase::Uncertain))
    });
    async move {
        let take_back = || {
            pending
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
        };
        match run.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(FlushPhase::NotStarted(error))) => Err(FlushFailure::NotStarted {
                tasks: take_back().unwrap_or_default(),
                error,
            }),
            Ok(Err(FlushPhase::Rejected(error))) => Err(FlushFailure::Rejected(error)),
            Ok(Err(FlushPhase::Uncertain(error))) => Err(FlushFailure::Uncertain(error)),
            // The executor failed. A batch still in the slot proves the
            // closure never ran; otherwise its statement may have been issued.
            Err(error) => match take_back() {
                Some(tasks) => Err(FlushFailure::NotStarted { tasks, error }),
                None => Err(FlushFailure::Uncertain(error)),
            },
        }
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

/// The derived column values of one task that passed every cap.
struct ValidatedTask {
    run_at: DateTime,
    meta_json: String,
}

/// Check the queue name cap once per batch.
fn validate_queue(config: &Config) -> Result<String, Error> {
    let job_type = config.queue().to_string();
    if job_type.len() > MAX_QUEUE_NAME_LEN {
        return Err(Error::InvalidArgument(format!(
            "queue name is {} bytes, exceeds the {MAX_QUEUE_NAME_LEN}-byte cap",
            job_type.len(),
        )));
    }
    Ok(job_type)
}

/// Check every per-task cap and derive the column values that need it.
/// The single place where a task can be rejected before any statement.
fn validate_columns(task: &PgTask<CompactType>) -> Result<ValidatedTask, Error> {
    if task.args.len() > MAX_JOB_PAYLOAD_LEN {
        return Err(Error::InvalidArgument(format!(
            "task payload is {} bytes, exceeds the {MAX_JOB_PAYLOAD_LEN}-byte cap",
            task.args.len(),
        )));
    }
    let run_at_secs = i64::try_from(task.parts.run_at).map_err(|_| {
        Error::InvalidArgument(format!(
            "run_at {} exceeds i64::MAX seconds and cannot be stored",
            task.parts.run_at
        ))
    })?;
    let run_at = DateTime::from_timestamp(run_at_secs, 0).ok_or_else(|| {
        Error::InvalidArgument(format!(
            "run_at {} exceeds the supported timestamp range",
            task.parts.run_at
        ))
    })?;
    // Serialize metadata once into the text representation handed to
    // Postgres (cast to jsonb in the SELECT below), so the byte-length check
    // and the bind share one pass.
    let meta_json = serde_json::to_string(task.parts.ctx.meta())
        .map_err(|err| Error::InvalidArgument(format!("serializing task metadata: {err}")))?;
    if meta_json.len() > MAX_METADATA_PAYLOAD_LEN {
        return Err(Error::InvalidArgument(format!(
            "task metadata is {} bytes, exceeds the {MAX_METADATA_PAYLOAD_LEN}-byte cap",
            meta_json.len(),
        )));
    }
    if let Some(key) = task.parts.idempotency_key.as_deref()
        && key.len() > MAX_IDEMPOTENCY_KEY_LEN
    {
        return Err(Error::InvalidArgument(format!(
            "idempotency_key is {} bytes, exceeds the {MAX_IDEMPOTENCY_KEY_LEN}-byte cap",
            key.len(),
        )));
    }
    Ok(ValidatedTask { run_at, meta_json })
}

/// Reject a task that no batch could ever write, before it is buffered.
///
/// Every cap that `prepare_batch` enforces is checked here against the same
/// rules, so a buffered batch can only fail for database reasons.
pub(crate) fn validate_task(config: &Config, task: &PgTask<CompactType>) -> Result<(), Error> {
    validate_queue(config)?;
    validate_columns(task).map(drop)
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

    let job_type = validate_queue(config)?;

    let mut ids = Vec::with_capacity(tasks.len());
    let mut jobs = Vec::with_capacity(tasks.len());
    let mut max_attempts = Vec::with_capacity(tasks.len());
    let mut run_ats = Vec::with_capacity(tasks.len());
    let mut priorities = Vec::with_capacity(tasks.len());
    let mut metadata = Vec::with_capacity(tasks.len());
    let mut idempotency_keys = Vec::with_capacity(tasks.len());

    for task in tasks {
        let ValidatedTask { run_at, meta_json } = validate_columns(&task)?;
        ids.push(
            task.parts
                .task_id
                .map(|task_id| task_id.to_string())
                .unwrap_or_else(|| Ulid::new().to_string()),
        );
        jobs.push(task.args);
        max_attempts.push(task.parts.ctx.max_attempts());
        run_ats.push(run_at);
        priorities.push(task.parts.ctx.priority());
        metadata.push(meta_json);
        idempotency_keys.push(task.parts.idempotency_key);
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

    fn prepare_empty_batch() -> Result<Option<PreparedBatch>, Error> {
        let config = Config::new("payload-cap");
        prepare_batch(&config, Vec::<PgTask<CompactType>>::new())
    }

    fn skips_the_empty_batch(result: &Result<Option<PreparedBatch>, Error>) -> AssertionResult {
        match result {
            Ok(None) => Ok(()),
            Ok(Some(batch)) => Err(AssertionError::new(vec![format!(
                "expected an empty batch to produce Ok(None), got a batch of {} task(s)",
                batch.task_count
            )])),
            Err(error) => Err(AssertionError::new(vec![format!(
                "expected an empty batch to produce Ok(None), got error: {error:?}"
            )])),
        }
    }

    fn accepts_one_task(
        expected_len: usize,
    ) -> impl Fn(&Result<Option<PreparedBatch>, Error>) -> AssertionResult {
        move |result| match result {
            Ok(Some(batch))
                if batch.task_count == 1
                    && batch.binds.jobs.len() == 1
                    && batch.binds.jobs[0].len() == expected_len
                    && batch.binds.jobs[0].iter().all(|byte| *byte == 0)
                    && batch.binds.ids.len() == 1
                    && Ulid::from_string(&batch.binds.ids[0]).is_ok()
                    && batch.binds.max_attempts.len() == 1
                    && batch.binds.run_ats.len() == 1
                    && batch.binds.priorities.len() == 1
                    && batch.binds.metadata.len() == 1
                    && batch.binds.idempotency_keys == [None]
                    && !batch.any_idempotency_key =>
            {
                Ok(())
            }
            Ok(_) => Err(AssertionError::new(vec![format!(
                "expected one aligned task with a generated id and exact {expected_len}-byte zero payload"
            )])),
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
        expect(prepare_batch_for_payload(len)) as payload_validation {
            let len = 128;

            when the_payload_is_empty {
                let len = 0;
                to accepts_the_zero_length_payload { accepts_one_task(len) }
            }

            when the_payload_is_well_below_the_cap {
                to accepts_the_task { accepts_one_task(len) }
            }

            when the_payload_is_exactly_at_the_cap {
                let len = MAX_JOB_PAYLOAD_LEN;
                to accepts_the_boundary_task { accepts_one_task(len) }
            }

            when the_payload_is_one_byte_over_the_cap {
                let len = MAX_JOB_PAYLOAD_LEN + 1;
                to rejects_the_oversized_payload { rejects_with_payload_cap }
            }
        }
    }

    lets_expect! {
        expect(prepare_empty_batch()) as empty_batch {
            when the_batch_is_empty {
                to produces_no_batch { skips_the_empty_batch }
            }
        }
    }
    fn prepared_schedule(run_at: u64) -> Result<Vec<i64>, Error> {
        let mut task = PgTask::<CompactType>::new(Vec::new());
        task.parts.run_at = run_at;
        prepare_batch(&Config::new("schedule"), vec![task]).map(|batch| {
            batch
                .into_iter()
                .flat_map(|batch| batch.binds.run_ats)
                .map(|run_at| run_at.timestamp())
                .collect()
        })
    }

    fn preserves_schedule(expected: i64) -> impl Fn(&Result<Vec<i64>, Error>) -> AssertionResult {
        move |result| match result {
            Ok(values) if values == &[expected] => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected exact timestamp {expected}, got {other:?}"
            )])),
        }
    }

    fn rejects_schedule(result: &Result<Vec<i64>, Error>) -> AssertionResult {
        match result {
            Err(Error::InvalidArgument(message)) if message.contains("run_at") => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected run_at InvalidArgument, got {other:?}"
            )])),
        }
    }

    lets_expect! {
        expect(prepared_schedule(seconds)) as scheduled_enqueue_validation {
            when(seconds = 0) as the_schedule_is_the_unix_epoch {
                to preserves_the_exact_timestamp { preserves_schedule(0) }
            }
            when(seconds = 1_800_000_000) as the_schedule_is_within_the_supported_range {
                to preserves_the_exact_timestamp { preserves_schedule(1_800_000_000) }
            }
            when(seconds = 8_210_266_876_799) as the_schedule_is_the_last_representable_second {
                to preserves_the_exact_timestamp { preserves_schedule(8_210_266_876_799) }
            }
            when(seconds = 8_210_266_876_800) as the_schedule_is_one_second_above_the_timestamp_limit {
                to rejects_the_unrepresentable_schedule { rejects_schedule }
            }
            when(seconds = i64::MAX as u64) as the_schedule_fits_i64_but_exceeds_the_timestamp_range {
                to rejects_the_unrepresentable_schedule { rejects_schedule }
            }
            when(seconds = i64::MAX as u64 + 1) as the_schedule_exceeds_i64 {
                to rejects_the_unrepresentable_schedule { rejects_schedule }
            }
            when(seconds = u64::MAX) as the_schedule_is_the_largest_input_value {
                to rejects_the_unrepresentable_schedule { rejects_schedule }
            }
        }
    }
}
