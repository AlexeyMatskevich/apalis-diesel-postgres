use std::str::FromStr;

use apalis_core::{
    backend::{QueueInfo, RunningWorker, StatType, Statistic, TaskResult},
    task::{builder::TaskBuilder, metadata::MetadataStore, status::Status, task_id::TaskId},
};
use diesel::deserialize::QueryableByName;
use diesel::sql_types::{Binary, Int4, Jsonb, Nullable, Text, Timestamptz};
use serde::de::DeserializeOwned;
use serde_json::Value;
use ulid::Ulid;

use crate::{
    CompactType, Error, PgTask,
    config::{DateTime, DateTimeExt},
};

#[derive(Debug, QueryableByName)]
pub(crate) struct JobRow {
    #[diesel(sql_type = Binary)]
    pub(crate) job: Vec<u8>,
    #[diesel(sql_type = Text)]
    pub(crate) id: String,
    #[diesel(sql_type = Text)]
    pub(crate) job_type: String,
    #[diesel(sql_type = Text)]
    pub(crate) status: String,
    #[diesel(sql_type = Int4)]
    pub(crate) attempts: i32,
    #[diesel(sql_type = Int4)]
    pub(crate) max_attempts: i32,
    #[diesel(sql_type = Timestamptz)]
    pub(crate) run_at: DateTime,
    /// Read with the row but not carried: an apalis 1.0 task has no place
    /// for it. `WaitForCompletion` reads results through its own queries.
    #[allow(dead_code)]
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub(crate) last_result: Option<Value>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub(crate) lock_at: Option<DateTime>,
    #[diesel(sql_type = Nullable<Text>)]
    pub(crate) lock_by: Option<String>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub(crate) done_at: Option<DateTime>,
    #[diesel(sql_type = Nullable<Int4>)]
    pub(crate) priority: Option<i32>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub(crate) metadata: Option<Value>,
    #[diesel(sql_type = Nullable<Text>)]
    pub(crate) idempotency_key: Option<String>,
}

impl JobRow {
    /// Build the compact task this row stores.
    ///
    /// The task id must be a ULID, as every id this backend writes is.
    /// `last_result` has no place on an apalis 1.0 task and is not carried.
    pub(crate) fn into_task(self) -> Result<PgTask<CompactType>, Error> {
        let task_id = ulid::Ulid::from_string(&self.id)
            .map(TaskId::from_ulid)
            .map_err(|error| Error::Row(Box::new(error)))?;
        let status = Status::from_str(&self.status).map_err(|error| Error::Row(Box::new(error)))?;
        let run_at = u64::try_from(self.run_at.to_unix_timestamp()).unwrap_or(0);
        let unix = |time: DateTime| u64::try_from(time.to_unix_timestamp()).ok();
        let mut task = TaskBuilder::new(self.job)
            .task_id(task_id)
            .queue(self.job_type.into())
            .status(status)
            .attempt(self.attempts.max(0) as usize)
            .max_attempts(self.max_attempts.max(0) as usize)
            .run_at_timestamp(run_at)
            .lock_at(self.lock_at.and_then(unix))
            .lock_by(self.lock_by)
            .done_at(self.done_at.and_then(unix))
            .with_metadata(metadata_from_json(self.metadata));
        if let Some(priority) = self.priority {
            task = task.priority(priority.max(0) as usize);
        }
        if let Some(key) = self.idempotency_key {
            task = task.idempotency_key(key);
        }
        Ok(task.build())
    }
}

/// Read the `metadata` column into apalis's string store.
///
/// A string value is taken as-is; any other value is kept as its JSON text.
/// Anything but a JSON object — a third-party writer can store a bare number,
/// string, array, bool, or null straight into `apalis.jobs`, bypassing this
/// crate's push API — reads as empty metadata instead of failing the row and,
/// on the polling path, stranding every co-claimed row.
pub(crate) fn metadata_from_json(value: Option<Value>) -> MetadataStore {
    let Some(Value::Object(map)) = value else {
        return MetadataStore::new();
    };
    MetadataStore::from_map(
        map.into_iter()
            .map(|(key, value)| match value {
                Value::String(text) => (key, text),
                other => (key, other.to_string()),
            })
            .collect(),
    )
}

/// Write apalis's string store as the `metadata` JSON object.
pub(crate) fn metadata_to_json(store: &MetadataStore) -> Value {
    Value::Object(
        store
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect(),
    )
}

#[derive(Debug, QueryableByName)]
pub(crate) struct WorkerRow {
    #[diesel(sql_type = Text)]
    pub(crate) id: String,
    #[diesel(sql_type = Text)]
    pub(crate) worker_type: String,
    #[diesel(sql_type = Text)]
    pub(crate) storage_name: String,
    #[diesel(sql_type = Nullable<Text>)]
    pub(crate) layers: Option<String>,
    #[diesel(sql_type = Timestamptz)]
    pub(crate) last_seen: DateTime,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    pub(crate) started_at: Option<DateTime>,
}

impl From<WorkerRow> for RunningWorker {
    fn from(row: WorkerRow) -> Self {
        Self {
            id: row.id,
            queue: row.worker_type,
            backend: row.storage_name,
            started_at: row
                .started_at
                .and_then(|time| u64::try_from(time.to_unix_timestamp()).ok())
                .unwrap_or(0),
            last_heartbeat: u64::try_from(row.last_seen.to_unix_timestamp()).unwrap_or(0),
            layers: row.layers.unwrap_or_default(),
        }
    }
}

#[derive(Debug, QueryableByName)]
pub(crate) struct StatisticRow {
    #[diesel(sql_type = Nullable<Int4>)]
    pub(crate) priority: Option<i32>,
    #[diesel(sql_type = Nullable<Text>)]
    pub(crate) r#type: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub(crate) statistic: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub(crate) value: Option<String>,
}

impl From<StatisticRow> for Statistic {
    fn from(row: StatisticRow) -> Self {
        Self {
            title: row.statistic.unwrap_or_default(),
            stat_type: StatType::from_str(&row.r#type.unwrap_or_default())
                .unwrap_or(StatType::Number),
            value: row.value.unwrap_or_else(|| "0".to_owned()),
            priority: Some(row.priority.unwrap_or_default().max(0) as u64),
        }
    }
}

#[derive(Debug, QueryableByName)]
pub(crate) struct QueueInfoRow {
    #[diesel(sql_type = Nullable<Text>)]
    pub(crate) name: Option<String>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub(crate) stats: Option<Value>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub(crate) workers: Option<Value>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub(crate) activity: Option<Value>,
}

impl From<QueueInfoRow> for QueueInfo {
    fn from(row: QueueInfoRow) -> Self {
        Self {
            name: row.name.unwrap_or_default(),
            stats: row
                .stats
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default(),
            workers: row
                .workers
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default(),
            activity: row
                .activity
                .and_then(|value| serde_json::from_value(value).ok())
                .unwrap_or_default(),
        }
    }
}

/// One row a completion wait tracks: present in `apalis.jobs`, with the
/// server's own verdict on whether it will ever run again.
#[derive(Debug, QueryableByName)]
pub(crate) struct TrackedTaskRow {
    #[diesel(sql_type = Text)]
    pub(crate) id: String,
    #[diesel(sql_type = Text)]
    pub(crate) status: String,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub(crate) result: Option<Value>,
    #[diesel(sql_type = Int4)]
    pub(crate) attempts: i32,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    pub(crate) terminal: bool,
}

impl From<TrackedTaskRow> for TaskResultRow {
    fn from(row: TrackedTaskRow) -> Self {
        Self {
            id: Some(row.id),
            status: Some(row.status),
            result: row.result,
            attempts: Some(row.attempts),
        }
    }
}

#[derive(Debug, QueryableByName)]
pub(crate) struct TaskResultRow {
    #[diesel(sql_type = Nullable<Text>)]
    pub(crate) id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub(crate) status: Option<String>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub(crate) result: Option<Value>,
    #[diesel(sql_type = Nullable<Int4>)]
    pub(crate) attempts: Option<i32>,
}

pub(crate) fn task_result_from_row<O>(row: TaskResultRow) -> Result<TaskResult<O>, Error>
where
    Result<O, String>: DeserializeOwned,
{
    let id = row.id.ok_or(Error::MissingField("id"))?;
    let status = row.status.ok_or(Error::MissingField("status"))?;
    let result = row.result.ok_or(Error::MissingField("last_result"))?;
    Ok(TaskResult {
        task_id: Ulid::from_string(&id)
            .map(TaskId::from_ulid)
            .map_err(|error| Error::Row(Box::new(error)))?,
        attempt: row.attempts.map_or(0, |attempts| attempts.max(0) as usize),
        status: Status::from_str(&status).map_err(|error| Error::Row(Box::new(error)))?,
        result: serde_json::from_value(result)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use apalis_core::backend::{StatType, queue::Queue};
    use lets_expect::{AssertionError, AssertionResult, *};
    use serde_json::json;

    fn job_row(
        attempts: i32,
        max_attempts: i32,
        priority: Option<i32>,
        metadata: Option<Value>,
        idempotency_key: Option<String>,
        last_result: Option<Value>,
    ) -> JobRow {
        JobRow {
            job: vec![1, 2, 3],
            id: "task-1".to_string(),
            job_type: "email".to_string(),
            status: "pending".to_string(),
            attempts,
            max_attempts,
            run_at: DateTime::now(),
            last_result,
            lock_at: Some(DateTime::now()),
            lock_by: Some("worker-1".to_string()),
            done_at: Some(DateTime::now()),
            priority,
            metadata,
            idempotency_key,
        }
    }

    fn worker_row(
        last_seen: DateTime,
        started_at: Option<DateTime>,
        layers: Option<String>,
    ) -> WorkerRow {
        WorkerRow {
            id: "worker-1".to_string(),
            worker_type: "email".to_string(),
            storage_name: "postgres".to_string(),
            layers,
            last_seen,
            started_at,
        }
    }

    fn statistic_row(
        statistic: Option<String>,
        r#type: Option<String>,
        value: Option<String>,
        priority: Option<i32>,
    ) -> StatisticRow {
        StatisticRow {
            priority,
            r#type,
            statistic,
            value,
        }
    }

    fn queue_info_row(
        name: Option<String>,
        stats: Option<Value>,
        workers: Option<Value>,
        activity: Option<Value>,
    ) -> QueueInfoRow {
        QueueInfoRow {
            name,
            stats,
            workers,
            activity,
        }
    }

    fn expected_metadata() -> Option<Value> {
        Some(json!({"trace": "abc", "nested": {"count": 1}}))
    }

    fn expected_idempotency_key() -> Option<String> {
        Some("same-request".to_string())
    }

    fn decimal_statistic() -> Value {
        json!([{
            "title": "processed",
            "stat_type": "Decimal",
            "value": "7.5",
            "priority": 2
        }])
    }

    fn workers() -> Value {
        json!(["worker-1", "worker-2"])
    }

    fn activity() -> Value {
        json!([1, 2, 3, 4])
    }

    fn has_stat_type(expected: StatType) -> impl Fn(&Statistic) -> AssertionResult {
        move |statistic| {
            let matches = matches!(
                (&statistic.stat_type, &expected),
                (StatType::Timestamp, StatType::Timestamp)
                    | (StatType::Number, StatType::Number)
                    | (StatType::Decimal, StatType::Decimal)
                    | (StatType::Percentage, StatType::Percentage)
            );

            if matches {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected stat type {expected:?}, got {:?}",
                    statistic.stat_type
                )]))
            }
        }
    }

    fn contains_decimal_statistic(queue_info: &QueueInfo) -> AssertionResult {
        match queue_info.stats.as_slice() {
            [statistic] => {
                if statistic.title != "processed" {
                    return Err(AssertionError::new(vec![format!(
                        "expected statistic title \"processed\", got {:?}",
                        statistic.title
                    )]));
                }
                has_stat_type(StatType::Decimal)(statistic)?;
                if statistic.value != "7.5" {
                    return Err(AssertionError::new(vec![format!(
                        "expected statistic value \"7.5\", got {:?}",
                        statistic.value
                    )]));
                }
                if statistic.priority == Some(2) {
                    Ok(())
                } else {
                    Err(AssertionError::new(vec![format!(
                        "expected statistic priority Some(2), got {:?}",
                        statistic.priority
                    )]))
                }
            }
            other => Err(AssertionError::new(vec![format!(
                "expected one statistic, got {}",
                other.len()
            )])),
        }
    }

    /// Convert a `JobRow` carrying the given `metadata` JSON into a compact task
    /// and return its metadata re-serialized. apalis-sql's
    /// `try_into_task_compact` did `metadata.as_object().cloned().unwrap()`
    /// (`from_row.rs`), so any non-object JSONB — which a third-party writer can
    /// insert straight into `apalis.jobs`, bypassing this crate's push API —
    /// used to panic during row conversion. In the polling path that panic
    /// fires mid-batch, stranding every co-claimed row as `Queued`. Coercing
    /// non-object metadata to an empty object in `JobRow::into_task` keeps the
    /// conversion total, matching apalis-sql's own defensive `try_into_task`.
    fn compact_metadata_json(metadata: Value) -> Result<String, String> {
        let mut row = job_row(
            1,
            3,
            Some(0),
            Some(metadata),
            None,
            Some(json!({"ok": true})),
        );
        row.id = Ulid::generate().to_string();
        row.status = "Pending".to_owned();
        let task = row.into_task().map_err(|error| error.to_string())?;
        serde_json::to_string(&metadata_to_json(task.metadata())).map_err(|error| error.to_string())
    }

    /// The task a row converts to. `into_task` parses `id` and `status`; the
    /// shared `job_row` helper stores placeholders, so give it a real ULID and
    /// a parseable status.
    fn task_from(mut row: JobRow) -> PgTask<CompactType> {
        row.id = Ulid::from(1u128).to_string();
        row.status = "Pending".to_owned();
        row.into_task().expect("the row converts")
    }

    fn forwarded_task_record(has_completion: bool) -> PgTask<CompactType> {
        let mut row = job_row(
            1,
            3,
            Some(9),
            expected_metadata(),
            expected_idempotency_key(),
            None,
        );
        row.run_at = <DateTime as DateTimeExt>::from_unix_timestamp(123);
        row.lock_at = has_completion.then(|| <DateTime as DateTimeExt>::from_unix_timestamp(456));
        row.lock_by = has_completion.then(|| "worker-1".to_owned());
        row.done_at = has_completion.then(|| <DateTime as DateTimeExt>::from_unix_timestamp(789));
        task_from(row)
    }

    lets_expect! {
        expect(forwarded_task_record(has_completion)) as forwarded_task_record {
            let has_completion = true;
            when ownership_and_completion_are_present {
                to preserves_the_record_fields {
                    have(args.clone()) equal(vec![1, 2, 3]),
                    have(task_id()) equal(Some(&TaskId::from_ulid(Ulid::from(1u128)))),
                    have(queue()) equal(Some(&Queue::from("email"))),
                    have(status()) equal(Status::Pending),
                    have(run_at()) equal(Some(123)),
                    have(lock_at()) equal(Some(456)),
                    have(lock_by()) equal(Some("worker-1")),
                    have(done_at()) equal(Some(789))
                }
            }
            when ownership_and_completion_are_absent {
                let has_completion = false;
                to preserves_the_unowned_record {
                    have(args.clone()) equal(vec![1, 2, 3]),
                    have(task_id()) equal(Some(&TaskId::from_ulid(Ulid::from(1u128)))),
                    have(queue()) equal(Some(&Queue::from("email"))),
                    have(status()) equal(Status::Pending),
                    have(run_at()) equal(Some(123)),
                    have(lock_at()) equal(None),
                    have(lock_by()) equal(None),
                    have(done_at()) equal(None)
                }
            }
        }

        expect(compact_metadata_json(metadata)) as compact_task_metadata {
            let metadata: Value = json!({"trace": "abc"});

            when metadata_is_a_json_object {
                to preserves_the_object_verbatim {
                    be_ok_and equal(r#"{"trace":"abc"}"#.to_owned())
                }
            }

            when metadata_is_a_bare_number {
                let metadata: Value = json!(42);
                to coerces_the_non_object_to_an_empty_object { be_ok_and equal("{}".to_owned()) }
            }

            when metadata_is_a_bare_string {
                let metadata: Value = json!("nope");
                to coerces_the_non_object_to_an_empty_object { be_ok_and equal("{}".to_owned()) }
            }

            when metadata_is_a_json_array {
                let metadata: Value = json!([1, 2, 3]);
                to coerces_the_non_object_to_an_empty_object { be_ok_and equal("{}".to_owned()) }
            }

            when metadata_is_a_boolean {
                let metadata: Value = json!(true);
                to coerces_the_non_object_to_an_empty_object { be_ok_and equal("{}".to_owned()) }
            }

            when metadata_is_json_null {
                let metadata: Value = json!(null);
                to coerces_the_non_object_to_an_empty_object { be_ok_and equal("{}".to_owned()) }
            }
        }

        expect(task_from(job_row(attempts, max_attempts, priority, metadata.clone(), idempotency_key.clone(), last_result.clone()))) as task_attempt_count {
            let attempts = 1;
            let max_attempts = 3;
            let priority = Some(9);
            let metadata = expected_metadata();
            let idempotency_key = expected_idempotency_key();
            let last_result: Option<Value> = Some(json!({"ok": true}));

            to preserves_the_positive_attempt_count { have(attempt()) equal(1) }

            when attempts_is_zero {
                let attempts = 0;
                to preserves_zero_attempts { have(attempt()) equal(0) }
            }

            when attempts_is_negative {
                let attempts = -1;
                to clamps_attempts_to_zero { have(attempt()) equal(0) }
            }
        }

        expect(task_from(job_row(attempts, max_attempts, priority, metadata.clone(), idempotency_key.clone(), last_result.clone()))) as task_attempt_budget {
            let attempts = 1;
            let max_attempts = 3;
            let priority = Some(9);
            let metadata = expected_metadata();
            let idempotency_key = expected_idempotency_key();
            let last_result: Option<Value> = Some(json!({"ok": true}));

            to preserves_the_positive_attempt_limit { have(max_attempts()) equal(Some(3)) }

            when max_attempts_is_zero {
                let max_attempts = 0;
                to preserves_zero_as_the_attempt_limit { have(max_attempts()) equal(Some(0)) }
            }

            when max_attempts_is_negative {
                let max_attempts = -1;
                to clamps_the_attempt_limit_to_zero { have(max_attempts()) equal(Some(0)) }
            }
        }

        expect(task_from(job_row(attempts, max_attempts, priority, metadata.clone(), idempotency_key.clone(), last_result.clone()))) as task_priority {
            let attempts = 1;
            let max_attempts = 3;
            let priority = Some(9);
            let metadata = expected_metadata();
            let idempotency_key = expected_idempotency_key();
            let last_result: Option<Value> = Some(json!({"ok": true}));

            when priority_is_absent {
                let priority = None;
                to leaves_priority_absent { have(priority()) equal(None) }
            }

            to preserves_the_positive_priority { have(priority()) equal(Some(9)) }

            when priority_is_zero {
                let priority = Some(0);
                to preserves_zero_priority { have(priority()) equal(Some(0)) }
            }

            when priority_is_negative {
                let priority = Some(-1);
                to clamps_priority_to_zero { have(priority()) equal(Some(0)) }
            }
        }

        expect(task_from(job_row(attempts, max_attempts, priority, metadata.clone(), idempotency_key.clone(), last_result.clone()))) as task_metadata {
            let attempts = 1;
            let max_attempts = 3;
            let priority = Some(9);
            let metadata = expected_metadata();
            let idempotency_key = expected_idempotency_key();
            let last_result: Option<Value> = Some(json!({"ok": true}));

            to preserves_present_metadata { have(metadata()) equal(&metadata_from_json(expected_metadata())) }
            when metadata_is_absent {
                let metadata = None;
                to leaves_metadata_absent { have(metadata()) equal(&MetadataStore::new()) }
            }
        }

        expect(task_from(job_row(attempts, max_attempts, priority, metadata.clone(), idempotency_key.clone(), last_result.clone()))) as task_deduplication_key {
            let attempts = 1;
            let max_attempts = 3;
            let priority = Some(9);
            let metadata = expected_metadata();
            let idempotency_key = expected_idempotency_key();
            let last_result: Option<Value> = Some(json!({"ok": true}));

            to preserves_the_key { have(idempotency_key()) equal(Some("same-request")) }
            when the_key_is_absent {
                let idempotency_key = None;
                to leaves_the_key_absent { have(idempotency_key()) equal(None) }
            }
        }

        expect(RunningWorker::from(worker_row(last_seen, started_at, layers))) as worker_heartbeat_time {
            let last_seen = <DateTime as DateTimeExt>::from_unix_timestamp(123);
            let started_at = Some(<DateTime as DateTimeExt>::from_unix_timestamp(456));
            let layers = Some("layer-a,layer-b".to_string());

            to converts_last_seen_to_a_unix_timestamp { have(last_heartbeat) equal(123) }

            when last_seen_is_the_epoch {
                // Boundary at the `>= 0` edge of `u64::try_from(..).unwrap_or(0)`
                // (src/models.rs:104): the epoch 1970-01-01 is a valid timestamp
                // that must convert to `0`, not be rejected as if it were the
                // negative branch. Pins the successful-conversion side of the
                // clamp so an off-by-one that treated `0` as invalid would still
                // be caught by the paired non-zero/`123` case above.
                let last_seen = <DateTime as DateTimeExt>::from_unix_timestamp(0);
                to converts_the_epoch_to_zero { have(last_heartbeat) equal(0) }
            }

            when last_seen_is_a_negative_unix_timestamp {
                // Mirror of started_at below: `u64::try_from(negative).unwrap_or(0)`
                // (src/models.rs:95) collapses a pre-epoch heartbeat to zero
                // rather than surfacing an error. The public unsigned timestamp
                // cannot preserve pre-epoch values inserted directly into the DB.
                let last_seen = <DateTime as DateTimeExt>::from_unix_timestamp(-1);
                to silently_clamps_pre_epoch_heartbeat_to_zero { have(last_heartbeat) equal(0) }
            }
        }

        expect(RunningWorker::from(worker_row(last_seen, started_at, layers))) as worker_start_time {
            let last_seen = <DateTime as DateTimeExt>::from_unix_timestamp(123);
            let started_at = Some(<DateTime as DateTimeExt>::from_unix_timestamp(456));
            let layers = Some("layer-a,layer-b".to_string());

            to converts_started_at_to_a_unix_timestamp { have(started_at) equal(456) }

            when started_at_is_absent {
                let started_at = None;
                to defaults_started_at_to_zero { have(started_at) equal(0) }
            }

            when started_at_is_a_negative_unix_timestamp {
                // Documents the silent fallback: `u64::try_from(negative).unwrap_or(0)`
                // collapses pre-epoch timestamps to zero rather than surfacing an
                // error. Dashboards consuming `RunningWorker.started_at` therefore
                // cannot distinguish "started before 1970" from "unknown start time"
                // The public unsigned representation preserves this compatibility
                // fallback even for timestamps inserted directly into the DB.
                let started_at = Some(<DateTime as DateTimeExt>::from_unix_timestamp(-1));
                to silently_clamps_pre_epoch_to_zero { have(started_at) equal(0) }
            }
            when the_worker_started_at_the_epoch {
                let started_at = Some(<DateTime as DateTimeExt>::from_unix_timestamp(0));
                to preserves_the_epoch { have(started_at) equal(0) }
            }
        }

        expect(RunningWorker::from(worker_row(last_seen, started_at, layers))) as worker_layers {
            let last_seen = <DateTime as DateTimeExt>::from_unix_timestamp(123);
            let started_at = Some(<DateTime as DateTimeExt>::from_unix_timestamp(456));
            let layers = Some("layer-a,layer-b".to_string());

            to preserves_layers { have(layers) equal("layer-a,layer-b".to_string()) }

            when layers_are_absent {
                let layers = None;
                to defaults_layers_to_an_empty_string { have(layers) equal(String::new()) }
            }
        }

        expect(Statistic::from(statistic_row(statistic, stat_type, value, priority))) as statistic_title {
            let statistic = Some("processed".to_string());
            let stat_type = Some("Decimal".to_string());
            let value = Some("7.5".to_owned());
            let priority = Some(4);

            to preserves_the_title { have(title) equal("processed".to_string()) }

            when title_is_absent {
                let statistic = None;
                to defaults_the_title_to_an_empty_string { have(title) equal(String::new()) }
            }
        }

        expect(Statistic::from(statistic_row(statistic, stat_type, value, priority))) as statistic_type {
            let statistic = Some("processed".to_string());
            let stat_type = Some("Decimal".to_string());
            let value = Some("7.5".to_owned());
            let priority = Some(4);

            to maps_the_type_to_decimal { has_stat_type(StatType::Decimal) }

            when type_is_percentage {
                let stat_type = Some("Percentage".to_string());
                to maps_the_type_to_percentage { has_stat_type(StatType::Percentage) }
            }

            when type_is_timestamp {
                let stat_type = Some("Timestamp".to_string());
                to maps_the_type_to_timestamp { has_stat_type(StatType::Timestamp) }
            }

            when type_is_unknown {
                let stat_type = Some("Unexpected".to_string());
                to defaults_the_type_to_number { has_stat_type(StatType::Number) }
            }

            when type_is_absent {
                let stat_type = None;
                to defaults_the_type_to_number { has_stat_type(StatType::Number) }
            }
        }

        expect(Statistic::from(statistic_row(statistic, stat_type, value, priority))) as statistic_value {
            let statistic = Some("processed".to_string());
            let stat_type = Some("Decimal".to_string());
            let value = Some("7.5".to_owned());
            let priority = Some(4);

            to preserves_the_decimal_text { have(value) equal("7.5".to_string()) }

            when the_value_exceeds_single_precision_integer_accuracy {
                let value = Some("16777217".to_owned());
                to preserves_every_integer_digit { have(value) equal("16777217".to_owned()) }
            }

            when the_value_exceeds_double_precision_integer_accuracy {
                let value = Some("9007199254740993".to_owned());
                to preserves_every_integer_digit { have(value) equal("9007199254740993".to_owned()) }
            }

            when value_is_absent {
                let value = None;
                to defaults_the_value_to_zero { have(value) equal("0".to_string()) }
            }
        }

        expect(Statistic::from(statistic_row(statistic, stat_type, value, priority))) as statistic_priority {
            let statistic = Some("processed".to_string());
            let stat_type = Some("Decimal".to_string());
            let value = Some("7.5".to_owned());
            let priority = Some(4);

            when priority_is_absent {
                let priority = None;
                to defaults_priority_to_zero { have(priority) equal(Some(0)) }
            }

            to preserves_the_positive_priority { have(priority) equal(Some(4)) }

            when priority_is_zero {
                let priority = Some(0);
                to preserves_zero_priority { have(priority) equal(Some(0)) }
            }

            when priority_is_negative {
                let priority = Some(-1);
                to clamps_priority_to_zero { have(priority) equal(Some(0)) }
            }
        }

        expect(QueueInfo::from(queue_info_row(name, stats, workers, activity))) as queue_name {
            let name = Some("email".to_string());
            let stats = Some(decimal_statistic());
            let workers = Some(workers());
            let activity = Some(activity());

            to preserves_the_name { have(name) equal("email".to_string()) }

            when name_is_absent {
                let name = None;
                to defaults_the_name_to_an_empty_string { have(name) equal(String::new()) }
            }
        }

        expect(QueueInfo::from(queue_info_row(name, stats, workers, activity))) as queue_statistics {
            let name = Some("email".to_string());
            let stats = Some(decimal_statistic());
            let workers = Some(workers());
            let activity = Some(activity());

            to decodes_the_stats { contains_decimal_statistic }

            when stats_are_invalid {
                let stats = Some(json!({"not": "a statistic list"}));
                to defaults_stats_to_an_empty_list { have(stats.len()) equal(0) }
            }

            when stats_are_absent {
                let stats = None;
                to defaults_stats_to_an_empty_list { have(stats.len()) equal(0) }
            }
        }

        expect(QueueInfo::from(queue_info_row(name, stats, workers, activity))) as queue_workers {
            let name = Some("email".to_string());
            let stats = Some(decimal_statistic());
            let workers = Some(workers());
            let activity = Some(activity());

            to decodes_the_workers { have(workers) equal(vec!["worker-1".to_string(), "worker-2".to_string()]) }

            when workers_are_invalid {
                let workers = Some(json!({"not": "a worker list"}));
                to defaults_workers_to_an_empty_list { have(workers) equal(Vec::<String>::new()) }
            }

            when workers_are_absent {
                let workers = None;
                to defaults_workers_to_an_empty_list { have(workers) equal(Vec::<String>::new()) }
            }
        }

        expect(QueueInfo::from(queue_info_row(name, stats, workers, activity))) as queue_activity {
            let name = Some("email".to_string());
            let stats = Some(decimal_statistic());
            let workers = Some(workers());
            let activity = Some(activity());

            to decodes_the_activity { have(activity) equal(vec![1, 2, 3, 4]) }

            when activity_is_invalid {
                let activity = Some(json!({"not": "an activity list"}));
                to defaults_activity_to_an_empty_list { have(activity) equal(Vec::<usize>::new()) }
            }

            when activity_is_absent {
                let activity = None;
                to defaults_activity_to_an_empty_list { have(activity) equal(Vec::<usize>::new()) }
            }
        }

    }
}
