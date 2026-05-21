use std::str::FromStr;

use apalis_core::{
    backend::{QueueInfo, RunningWorker, Statistic, TaskResult},
    task::{status::Status, task_id::TaskId},
};
use apalis_sql::{DateTime, DateTimeExt, TaskRow};
use diesel::deserialize::QueryableByName;
use diesel::sql_types::{Binary, Float4, Int4, Jsonb, Nullable, Text, Timestamptz};
use serde::de::DeserializeOwned;
use serde_json::Value;
use ulid::Ulid;

use crate::Error;

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

impl From<JobRow> for TaskRow {
    fn from(row: JobRow) -> Self {
        Self {
            job: row.job,
            id: row.id,
            job_type: row.job_type,
            status: row.status,
            attempts: row.attempts.max(0) as usize,
            max_attempts: Some(row.max_attempts.max(0) as usize),
            run_at: Some(row.run_at),
            last_result: row.last_result,
            lock_at: row.lock_at,
            lock_by: row.lock_by,
            done_at: row.done_at,
            priority: row.priority.map(|priority| priority.max(0) as usize),
            metadata: row.metadata,
            idempotency_key: row.idempotency_key,
        }
    }
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
    #[diesel(sql_type = Nullable<Float4>)]
    pub(crate) value: Option<f32>,
}

impl From<StatisticRow> for Statistic {
    fn from(row: StatisticRow) -> Self {
        Self {
            title: row.statistic.unwrap_or_default(),
            stat_type: apalis_sql::stat_type_from_string(&row.r#type.unwrap_or_default()),
            value: row.value.unwrap_or_default().to_string(),
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

#[derive(Debug, QueryableByName)]
pub(crate) struct TaskResultRow {
    #[diesel(sql_type = Nullable<Text>)]
    pub(crate) id: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    pub(crate) status: Option<String>,
    #[diesel(sql_type = Nullable<Jsonb>)]
    pub(crate) result: Option<Value>,
}

pub(crate) fn task_result_from_row<O>(row: TaskResultRow) -> Result<TaskResult<O, Ulid>, Error>
where
    Result<O, String>: DeserializeOwned,
{
    let id = row.id.ok_or(Error::MissingField("id"))?;
    let status = row.status.ok_or(Error::MissingField("status"))?;
    let result = row.result.ok_or(Error::MissingField("last_result"))?;
    Ok(TaskResult::new(
        TaskId::from_str(&id).map_err(|error| Error::Row(Box::new(error)))?,
        Status::from_str(&status).map_err(|error| Error::Row(Box::new(error)))?,
        serde_json::from_value(result)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use apalis_core::backend::StatType;
    use lets_expect::{AssertionError, AssertionResult, *};
    use serde_json::json;

    fn job_row(
        attempts: i32,
        max_attempts: i32,
        priority: Option<i32>,
        metadata: Option<Value>,
        idempotency_key: Option<String>,
    ) -> JobRow {
        JobRow {
            job: vec![1, 2, 3],
            id: "task-1".to_string(),
            job_type: "email".to_string(),
            status: "pending".to_string(),
            attempts,
            max_attempts,
            run_at: DateTime::now(),
            last_result: Some(json!({"ok": true})),
            lock_at: Some(DateTime::now()),
            lock_by: Some("worker-1".to_string()),
            done_at: Some(DateTime::now()),
            priority,
            metadata,
            idempotency_key,
        }
    }

    fn worker_row(started_at: Option<DateTime>, layers: Option<String>) -> WorkerRow {
        WorkerRow {
            id: "worker-1".to_string(),
            worker_type: "email".to_string(),
            storage_name: "postgres".to_string(),
            layers,
            last_seen: DateTime::now(),
            started_at,
        }
    }

    fn statistic_row(
        statistic: Option<String>,
        r#type: Option<String>,
        value: Option<f32>,
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

    lets_expect! {
        expect(TaskRow::from(job_row(attempts, max_attempts, priority, metadata.clone(), idempotency_key.clone()))) {
            let attempts = 1;
            let max_attempts = 3;
            let priority = Some(9);
            let metadata = expected_metadata();
            let idempotency_key = expected_idempotency_key();

            when attempts_is_positive {
                to preserves_the_positive_attempt_count { have(attempts) equal(1) }
            }

            when attempts_is_zero {
                let attempts = 0;
                to preserves_zero_attempts { have(attempts) equal(0) }
            }

            when attempts_is_negative {
                let attempts = -1;
                to clamps_attempts_to_zero { have(attempts) equal(0) }
            }

            when max_attempts_is_positive {
                to preserves_the_positive_attempt_limit { have(max_attempts) equal(Some(3)) }
            }

            when max_attempts_is_zero {
                let max_attempts = 0;
                to preserves_zero_as_the_attempt_limit { have(max_attempts) equal(Some(0)) }
            }

            when max_attempts_is_negative {
                let max_attempts = -1;
                to clamps_the_attempt_limit_to_zero { have(max_attempts) equal(Some(0)) }
            }

            when priority_is_absent {
                let priority = None;
                to leaves_priority_absent { have(priority) equal(None) }
            }

            when priority_is_positive {
                to preserves_the_positive_priority { have(priority) equal(Some(9)) }
            }

            when priority_is_zero {
                let priority = Some(0);
                to preserves_zero_priority { have(priority) equal(Some(0)) }
            }

            when priority_is_negative {
                let priority = Some(-1);
                to clamps_priority_to_zero { have(priority) equal(Some(0)) }
            }

            when metadata_and_idempotency_are_present {
                to preserves_metadata_and_idempotency {
                    have(metadata) equal(expected_metadata()),
                    have(idempotency_key) equal(idempotency_key.clone())
                }
            }

            when metadata_and_idempotency_are_absent {
                let metadata = None;
                let idempotency_key = None;
                to leaves_metadata_and_idempotency_absent {
                    have(metadata) equal(None),
                    have(idempotency_key) equal(None)
                }
            }
        }

        expect(RunningWorker::from(worker_row(started_at, layers))) {
            let started_at = Some(DateTime::now());
            let layers = Some("layer-a,layer-b".to_string());

            when started_at_is_present {
                to converts_started_at_to_a_unix_timestamp { have(started_at) be_greater_than(0) }
            }

            when started_at_is_absent {
                let started_at = None;
                to defaults_started_at_to_zero { have(started_at) equal(0) }
            }

            when started_at_is_a_negative_unix_timestamp {
                // Documents the silent fallback: `u64::try_from(negative).unwrap_or(0)`
                // collapses pre-epoch timestamps to zero rather than surfacing an
                // error. Dashboards consuming `RunningWorker.started_at` therefore
                // cannot distinguish "started before 1970" from "unknown start time"
                // — acceptable because the DB schema never produces such values,
                // but the behaviour is asserted here so any future change is
                // intentional rather than accidental.
                let started_at = Some(<DateTime as DateTimeExt>::from_unix_timestamp(-1));
                to silently_clamps_pre_epoch_to_zero { have(started_at) equal(0) }
            }

            when layers_are_present {
                to preserves_layers { have(layers) equal("layer-a,layer-b".to_string()) }
            }

            when layers_are_absent {
                let layers = None;
                to defaults_layers_to_an_empty_string { have(layers) equal(String::new()) }
            }
        }

        expect(Statistic::from(statistic_row(statistic, stat_type, value, priority))) {
            let statistic = Some("processed".to_string());
            let stat_type = Some("Decimal".to_string());
            let value = Some(7.5);
            let priority = Some(4);

            when title_is_present {
                to preserves_the_title { have(title) equal("processed".to_string()) }
            }

            when title_is_absent {
                let statistic = None;
                to defaults_the_title_to_an_empty_string { have(title) equal(String::new()) }
            }

            when type_is_decimal {
                to maps_the_type_to_decimal { has_stat_type(StatType::Decimal) }
            }

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

            when value_is_present {
                to stringifies_the_value { have(value) equal("7.5".to_string()) }
            }

            when value_is_absent {
                let value = None;
                to defaults_the_value_to_zero { have(value) equal("0".to_string()) }
            }

            when priority_is_absent {
                let priority = None;
                to defaults_priority_to_zero { have(priority) equal(Some(0)) }
            }

            when priority_is_positive {
                to preserves_the_positive_priority { have(priority) equal(Some(4)) }
            }

            when priority_is_zero {
                let priority = Some(0);
                to preserves_zero_priority { have(priority) equal(Some(0)) }
            }

            when priority_is_negative {
                let priority = Some(-1);
                to clamps_priority_to_zero { have(priority) equal(Some(0)) }
            }
        }

        expect(QueueInfo::from(queue_info_row(name, stats, workers, activity))) {
            let name = Some("email".to_string());
            let stats = Some(decimal_statistic());
            let workers = Some(workers());
            let activity = Some(activity());

            when name_is_present {
                to preserves_the_name { have(name) equal("email".to_string()) }
            }

            when name_is_absent {
                let name = None;
                to defaults_the_name_to_an_empty_string { have(name) equal(String::new()) }
            }

            when stats_are_valid {
                to decodes_the_stats { contains_decimal_statistic }
            }

            when stats_are_invalid {
                let stats = Some(json!({"not": "a statistic list"}));
                to defaults_stats_to_an_empty_list { have(stats.len()) equal(0) }
            }

            when stats_are_absent {
                let stats = None;
                to defaults_stats_to_an_empty_list { have(stats.len()) equal(0) }
            }

            when workers_are_valid {
                to decodes_the_workers { have(workers) equal(vec!["worker-1".to_string(), "worker-2".to_string()]) }
            }

            when workers_are_invalid {
                let workers = Some(json!({"not": "a worker list"}));
                to defaults_workers_to_an_empty_list { have(workers) equal(Vec::<String>::new()) }
            }

            when workers_are_absent {
                let workers = None;
                to defaults_workers_to_an_empty_list { have(workers) equal(Vec::<String>::new()) }
            }

            when activity_is_valid {
                to decodes_the_activity { have(activity) equal(vec![1, 2, 3, 4]) }
            }

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
