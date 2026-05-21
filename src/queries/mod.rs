use apalis_core::backend::Filter;
use apalis_sql::TaskRow;
use diesel::PgConnection;

use crate::{CompactType, Error, PgPool, PgTask, models::JobRow, runtime};

mod ack;
pub(crate) mod admin;
mod fetch;
mod metrics;
mod notify;
mod push;
pub(crate) mod worker;

pub(crate) use metrics::refresh_queue_stats_snapshot;

pub(crate) use ack::{AckTaskUpdate, ack_task};
pub(crate) use admin::{
    fetch_by_id, list_all_tasks, list_queues, list_tasks, list_workers, metrics_for_queue,
    metrics_global, register_worker,
};
pub(crate) use fetch::{fetch_next, lock_task};
pub(crate) use notify::{
    NOTIFY_CHANNEL_CAPACITY_MAX, NOTIFY_LISTENER_POLL_INTERVAL, notify_task_ids,
};
pub(crate) use push::{push_tasks, push_tasks_on_conn};
pub(crate) use worker::{initial_heartbeat, keep_alive_stream, reenqueue_orphaned_stream};

pub(super) fn with_conn<F, T>(
    pool: PgPool,
    work: F,
) -> impl Future<Output = Result<T, Error>> + Send
where
    F: FnOnce(&mut PgConnection) -> Result<T, Error> + Send + 'static,
    T: Send + 'static,
{
    runtime::run_blocking(move || {
        let mut conn = pool.get()?;
        work(&mut conn)
    })
}

pub(super) fn clamp_i32<T>(value: T) -> i32
where
    T: TryInto<i32>,
{
    value.try_into().unwrap_or(i32::MAX)
}

pub(super) fn i32_from_u32(value: u32, field: &'static str) -> Result<i32, Error> {
    i32::try_from(value)
        .map_err(|_| Error::InvalidArgument(format!("{field} {value} exceeds i32::MAX")))
}

pub(super) fn filter_offset_i32(filter: &Filter) -> Result<i32, Error> {
    let page = filter.page;
    if page == 0 {
        return Err(Error::InvalidArgument(
            "filter.page must be >= 1 (1-based pagination)".to_owned(),
        ));
    }
    let limit = filter.limit();
    let offset = page
        .checked_sub(1)
        .and_then(|p| p.checked_mul(limit))
        .ok_or_else(|| {
            Error::InvalidArgument(format!(
                "filter offset overflows u32 (page={page}, limit={limit})"
            ))
        })?;
    i32_from_u32(offset, "offset")
}

pub(super) fn task_row(row: JobRow) -> Result<PgTask<CompactType>, Error> {
    let row: TaskRow = row.into();
    row.try_into_task_compact()
        .map_err(|error| Error::Row(Box::new(error)))
}

pub(super) fn task_rows(rows: Vec<JobRow>) -> Result<Vec<PgTask<CompactType>>, Error> {
    rows.into_iter().map(task_row).collect()
}

/// Chunk an id stream into batches and resolve each batch into tasks via
/// `queue_by_id`. Shared between the notify and shared-listener fetchers,
/// which previously inlined the same `ready_chunks(...).then(queue_by_id)`
/// shape — keeping it in one place ensures any change to batching/back-
/// pressure semantics applies to both backends.
pub(crate) fn batch_ids_into_tasks<S>(
    pool: PgPool,
    queue: String,
    worker_id: String,
    chunk_size: usize,
    ids: S,
) -> impl futures::Stream<Item = Result<Option<PgTask<CompactType>>, Error>> + Send + 'static
where
    S: futures::Stream<Item = Result<crate::PgTaskId, Error>> + Send + 'static,
{
    use futures::{StreamExt, future::ready, stream};

    let chunk = chunk_size.max(1);
    ids.ready_chunks(chunk)
        .then(move |events| {
            let pool = pool.clone();
            let queue = queue.clone();
            let worker_id = worker_id.clone();
            async move {
                let ids = events
                    .into_iter()
                    .map(|event| event.map(|task_id| task_id.to_string()))
                    .collect::<Result<Vec<_>, Error>>()?;
                fetch::queue_by_id(pool, queue, ids, worker_id)
                    .await
                    .map(|tasks| tasks.into_iter().map(Some).collect::<Vec<_>>())
            }
        })
        .flat_map(|tasks| match tasks {
            Ok(tasks) => stream::iter(tasks.into_iter().map(Ok)).boxed(),
            Err(error) => stream::once(ready(Err(error))).boxed(),
        })
}
#[cfg(test)]
mod tests {
    use apalis_core::{backend::Filter, task::status::Status};
    use lets_expect::{AssertionError, AssertionResult, *};
    use serde_json::json;
    use ulid::Ulid;

    use super::*;
    use crate::models::{TaskResultRow, task_result_from_row};

    fn clamp_usize(value: usize) -> i32 {
        clamp_i32(value)
    }

    fn clamp_u64(value: u64) -> i32 {
        clamp_i32(value)
    }

    fn convert_u32(value: u32) -> Result<i32, Error> {
        i32_from_u32(value, "limit")
    }

    fn filter(page: u32, page_size: Option<u32>) -> Filter {
        Filter {
            status: None,
            page,
            page_size,
        }
    }

    fn offset_for(page: u32, page_size: Option<u32>) -> Result<i32, Error> {
        filter_offset_i32(&filter(page, page_size))
    }

    fn invalid_argument_message(error: &Error) -> AssertionResult {
        match error {
            Error::InvalidArgument(_) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected InvalidArgument, got {other:?}"
            )])),
        }
    }

    fn invalid_argument_with(expected: &'static str) -> impl Fn(&Error) -> AssertionResult {
        move |error| match error {
            Error::InvalidArgument(message) if message.contains(expected) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected InvalidArgument containing {expected:?}, got {other:?}"
            )])),
        }
    }

    fn task_result_row_with(
        id: Option<&'static str>,
        status: Option<&'static str>,
        result: Option<serde_json::Value>,
    ) -> TaskResultRow {
        TaskResultRow {
            id: id.map(str::to_owned),
            status: status.map(str::to_owned),
            result,
        }
    }

    fn task_result_for(
        id: Option<&'static str>,
        status: Option<&'static str>,
        result: Option<serde_json::Value>,
    ) -> Result<apalis_core::backend::TaskResult<String, Ulid>, Error> {
        task_result_from_row::<String>(task_result_row_with(id, status, result))
    }

    fn missing_field(field: &'static str) -> impl Fn(&Error) -> AssertionResult {
        move |error| match error {
            Error::MissingField(found) if *found == field => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected MissingField({field:?}), got {other:?}"
            )])),
        }
    }

    fn row_error(error: &Error) -> AssertionResult {
        match error {
            Error::Row(_) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected Error::Row, got {other:?}"
            )])),
        }
    }

    fn json_error(error: &Error) -> AssertionResult {
        match error {
            Error::Json(_) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected Error::Json, got {other:?}"
            )])),
        }
    }

    fn task_result_done_with_payload(
        result: &apalis_core::backend::TaskResult<String, Ulid>,
    ) -> AssertionResult {
        if result.status != Status::Done {
            return Err(AssertionError::new(vec![format!(
                "expected Status::Done, got {:?}",
                result.status
            )]));
        }
        match &result.result {
            Ok(value) if value == "processed" => Ok(()),
            Ok(other) => Err(AssertionError::new(vec![format!(
                "expected payload \"processed\", got {other:?}"
            )])),
            Err(message) => Err(AssertionError::new(vec![format!(
                "expected Ok payload, got Err({message:?})"
            )])),
        }
    }

    fn task_result_failed_with_message(
        result: &apalis_core::backend::TaskResult<String, Ulid>,
    ) -> AssertionResult {
        if result.status != Status::Failed {
            return Err(AssertionError::new(vec![format!(
                "expected Status::Failed, got {:?}",
                result.status
            )]));
        }
        match &result.result {
            Err(message) if message == "boom" => Ok(()),
            Err(other) => Err(AssertionError::new(vec![format!(
                "expected Err message \"boom\", got {other:?}"
            )])),
            Ok(value) => Err(AssertionError::new(vec![format!(
                "expected Err payload, got Ok({value:?})"
            )])),
        }
    }

    fn ulid_string() -> String {
        Ulid::new().to_string()
    }

    lets_expect! {
        expect(clamp_usize(value)) {
            let value = 5_usize;

            when value_is_inside_i32_range {
                to returns_the_value_as_i32 { equal(5_i32) }
            }

            when value_is_zero {
                let value = 0_usize;
                to returns_zero { equal(0_i32) }
            }

            when value_equals_i32_max {
                let value = i32::MAX as usize;
                to returns_i32_max { equal(i32::MAX) }
            }

            when value_overflows_i32 {
                let value = i32::MAX as usize + 1;
                to clamps_to_i32_max { equal(i32::MAX) }
            }
        }

        expect(clamp_u64(value)) {
            let value = 5_u64;

            when value_is_inside_i32_range {
                to returns_the_value_as_i32 { equal(5_i32) }
            }

            when value_equals_i32_max {
                let value = i32::MAX as u64;
                to returns_i32_max { equal(i32::MAX) }
            }

            when value_overflows_i32 {
                let value = i32::MAX as u64 + 1;
                to clamps_to_i32_max { equal(i32::MAX) }
            }
        }

        expect(convert_u32(value)) {
            let value = 5_u32;

            when value_fits_in_i32 {
                to returns_ok_with_the_value { be_ok_and equal(5_i32) }
            }

            when value_equals_i32_max {
                let value = i32::MAX as u32;
                to returns_ok_with_i32_max { be_ok_and equal(i32::MAX) }
            }

            when value_overflows_i32 {
                let value = i32::MAX as u32 + 1;
                to returns_invalid_argument {
                    be_err_and invalid_argument_with("limit")
                }
            }
        }

        expect(offset_for(page, page_size)) {
            let page = 1_u32;
            let page_size: Option<u32> = Some(20);

            when page_is_one_with_explicit_size {
                to returns_zero_offset { be_ok_and equal(0_i32) }
            }

            when page_is_one_with_no_size {
                let page_size = None;
                to returns_zero_offset_using_the_default_limit {
                    be_ok_and equal(0_i32)
                }
            }

            when page_is_higher_than_one {
                let page = 3_u32;
                let page_size = Some(10);
                to returns_the_zero_indexed_offset {
                    be_ok_and equal(20_i32)
                }
            }

            when page_is_zero_which_is_invalid {
                let page = 0_u32;
                to returns_invalid_argument_for_zero_page {
                    be_err_and invalid_argument_with("filter.page")
                }
            }

            when page_multiplied_by_limit_overflows_u32 {
                let page = u32::MAX;
                let page_size = Some(u32::MAX);
                to returns_invalid_argument_for_overflow {
                    be_err_and invalid_argument_message
                }
            }
        }

        expect(task_result_for(id, status, result)) {
            let id: Option<&'static str> = Some("01HABCDEFGHJKMNPQRSTVWXYZ0");
            let status: Option<&'static str> = Some("Done");
            let result: Option<serde_json::Value> = Some(json!({"Ok": "processed"}));

            when row_has_id_status_and_ok_payload {
                to returns_the_decoded_task_result { be_ok_and task_result_done_with_payload }
            }

            when row_has_id_status_and_err_payload {
                let result: Option<serde_json::Value> = Some(json!({"Err": "boom"}));
                let status: Option<&'static str> = Some("Failed");
                to returns_the_decoded_failure_result {
                    be_ok_and task_result_failed_with_message
                }
            }

            when id_is_missing {
                let id: Option<&'static str> = None;
                to rejects_with_missing_id { be_err_and missing_field("id") }
            }

            when status_is_missing {
                let status: Option<&'static str> = None;
                to rejects_with_missing_status { be_err_and missing_field("status") }
            }

            when last_result_is_missing {
                let result: Option<serde_json::Value> = None;
                to rejects_with_missing_last_result { be_err_and missing_field("last_result") }
            }

            when id_is_not_a_valid_ulid {
                let id: Option<&'static str> = Some("not-a-ulid");
                to rejects_with_a_row_error { be_err_and row_error }
            }

            when status_is_not_a_known_status {
                let status: Option<&'static str> = Some("Unknown");
                to rejects_with_a_row_error { be_err_and row_error }
            }

            when payload_cannot_be_deserialised_into_the_result_type {
                let result: Option<serde_json::Value> = Some(json!({"unexpected": true}));
                to rejects_with_a_json_error { be_err_and json_error }
            }
        }

        expect(ulid_string().len()) {
            to has_canonical_ulid_length { equal(26) }
        }
    }
}

pub(crate) mod migrations;
