use apalis_core::backend::Filter;
use apalis_sql::TaskRow;
use diesel::PgConnection;

use crate::{CompactType, Error, PgPool, PgTask, models::JobRow, runtime};

mod ack;
pub(crate) mod admin;
pub(crate) mod fetch;
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
pub(crate) use fetch::{fail_undecodable_task, fetch_next, lock_task};
pub(crate) use notify::{NOTIFY_LISTENER_POLL_INTERVAL, clamp_notify_capacity, notify_task_ids};
pub(crate) use push::{FlushFailure, flush_tasks, push_tasks_on_conn, validate_task};
#[cfg(test)]
pub(crate) use push::{
    MAX_IDEMPOTENCY_KEY_LEN, MAX_JOB_PAYLOAD_LEN, MAX_METADATA_PAYLOAD_LEN, MAX_QUEUE_NAME_LEN,
};
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

/// Record acknowledgement evidence only for rows returned by a successful claim.
pub(super) fn claimed_task_row(row: JobRow) -> Result<PgTask<CompactType>, Error> {
    let mut task = task_row(row)?;
    crate::ack::record_claim(&mut task.parts);
    Ok(task)
}

/// Chunk an id stream into batches and resolve each batch into tasks via
/// `queue_by_id`. Shared between the notify and shared-listener fetchers,
/// preserving both a listener error and any IDs received before it. An error
/// ends a batch, but does not discard its successful prefix or later input.
pub(crate) fn batch_ids_into_tasks<S>(
    pool: PgPool,
    queue: String,
    worker_id: String,
    chunk_size: usize,
    ids: S,
    lease_token: Option<std::sync::Arc<str>>,
) -> impl futures::Stream<Item = Result<Option<PgTask<CompactType>>, Error>> + Send + 'static
where
    S: futures::Stream<Item = Result<crate::PgTaskId, Error>> + Send + 'static,
{
    use futures::{StreamExt, TryStreamExt, stream};

    ids.try_ready_chunks(chunk_size.max(1))
        .flat_map(move |chunk| {
            let (ids, failure) = match chunk {
                Ok(ids) => (ids, None),
                Err(futures::stream::TryReadyChunksError(ids, error)) => (ids, Some(error)),
            };
            let pool = pool.clone();
            let queue = queue.clone();
            let worker_id = worker_id.clone();
            let lease_token = lease_token.clone();
            let failures = stream::iter(failure.into_iter().map(Err));
            let tasks = stream::once(async move {
                if ids.is_empty() {
                    return Ok(Vec::new());
                }
                fetch::queue_by_id(
                    pool,
                    queue,
                    ids.into_iter().map(|id| id.to_string()).collect(),
                    worker_id,
                    lease_token,
                )
                .await
            })
            .flat_map(|tasks| match tasks {
                Ok(tasks) => stream::iter(tasks.into_iter().map(|task| Ok(Some(task)))).boxed(),
                Err(error) => stream::iter([Err(error)]).boxed(),
            });
            failures.chain(tasks)
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

    fn id_row_error(error: &Error) -> AssertionResult {
        match error {
            Error::Row(cause)
                if cause
                    .downcast_ref::<apalis_core::task::task_id::TaskIdError<ulid::DecodeError>>()
                    .is_some() =>
            {
                Ok(())
            }
            other => Err(AssertionError::new(vec![format!(
                "expected ULID parse cause, got {other:?}"
            )])),
        }
    }
    fn status_row_error(error: &Error) -> AssertionResult {
        match error {
            Error::Row(cause)
                if cause
                    .downcast_ref::<apalis_core::task::status::StatusError>()
                    .is_some() =>
            {
                Ok(())
            }
            other => Err(AssertionError::new(vec![format!(
                "expected status parse cause, got {other:?}"
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

    fn exact_task_result(
        expected_status: Status,
        success: bool,
    ) -> impl Fn(&apalis_core::backend::TaskResult<String, Ulid>) -> AssertionResult {
        move |result| {
            let expected_result = if success {
                Ok("processed".to_owned())
            } else {
                Err("boom".to_owned())
            };
            if result.task_id.to_string() == "01HABCDEFGHJKMNPQRSTVWXYZ0"
                && result.status == expected_status
                && result.result == expected_result
            {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected exact ID, {expected_status:?}, {expected_result:?}; got {result:?}"
                )]))
            }
        }
    }

    lets_expect! {
        expect(clamp_usize(value)) as bounded_batch_size {
            let value = 5_usize;

            to returns_the_value_as_i32 { equal(5_i32) }

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

        expect(clamp_u64(value)) as bounded_numeric_value {
            let value = 5_u64;

            to returns_the_value_as_i32 { equal(5_i32) }

            when value_is_zero {
                let value = 0_u64;
                to returns_zero { equal(0_i32) }
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

        expect(convert_u32(value)) as database_integer_argument {
            let value = 5_u32;

            to returns_ok_with_the_value { be_ok_and equal(5_i32) }

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

        expect(offset_for(page, page_size)) as task_page_offset {
            let page = 1_u32;
            let page_size: Option<u32> = Some(20);

            to returns_zero_offset_for_page_one { be_ok_and equal(0_i32) }

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
                    // Pin the u32-multiply branch specifically (not merely any
                    // InvalidArgument): the adjacent i32-cast case below matches
                    // "exceeds i32::MAX", so this must match its own distinct
                    // "overflows u32" message to tell the two branches apart.
                    be_err_and invalid_argument_with("overflows u32")
                }
            }

            when the_computed_offset_fits_u32_but_exceeds_i32_max {
                // page-1 times limit stays inside u32 (2_147_483_648) but the
                // final `i32_from_u32` cast rejects it: this is the second,
                // distinct error branch from the u32-multiply overflow above.
                let page = (i32::MAX as u32) + 2;
                let page_size = Some(1_u32);
                to returns_invalid_argument_for_i32_overflow {
                    be_err_and invalid_argument_with("exceeds i32::MAX")
                }
            }
        }

        expect(task_result_for(id, status, result)) as required_result_fields {
            let id: Option<&'static str> = Some("invalid");
            let status: Option<&'static str> = Some("Unknown");
            let result: Option<serde_json::Value> = Some(json!({"unexpected": true}));
            when id_is_present {
                when status_is_present {
                    when result_is_present {
                        to reports_the_first_validation_error { be_err_and id_row_error }
                    }
                    when result_is_absent {
                        let result: Option<serde_json::Value> = None;
                        to reports_the_first_validation_error { be_err_and missing_field("last_result") }
                    }
                }
                when status_is_absent {
                    let status: Option<&'static str> = None;
                    when result_is_present {
                        to reports_the_first_validation_error { be_err_and missing_field("status") }
                    }
                    when result_is_absent {
                        let result: Option<serde_json::Value> = None;
                        to reports_the_first_validation_error { be_err_and missing_field("status") }
                    }
                }
            }
            when id_is_absent {
                let id: Option<&'static str> = None;
                when status_is_present {
                    when result_is_present {
                        to reports_the_first_validation_error { be_err_and missing_field("id") }
                    }
                    when result_is_absent {
                        let result: Option<serde_json::Value> = None;
                        to reports_the_first_validation_error { be_err_and missing_field("id") }
                    }
                }
                when status_is_absent {
                    let status: Option<&'static str> = None;
                    when result_is_present {
                        to reports_the_first_validation_error { be_err_and missing_field("id") }
                    }
                    when result_is_absent {
                        let result: Option<serde_json::Value> = None;
                        to reports_the_first_validation_error { be_err_and missing_field("id") }
                    }
                }
            }
        }
        expect(task_result_for(id, status, result)) as complete_task_result {
            let id: Option<&'static str> = Some("01HABCDEFGHJKMNPQRSTVWXYZ0");
            let status: Option<&'static str> = Some("Unknown");
            let result: Option<serde_json::Value> = Some(json!({"unexpected": true}));
            when the_identifier_is_malformed {
                let id: Option<&'static str> = Some("not-a-ulid");
                to rejects_the_identifier_before_other_invalid_values { be_err_and id_row_error }
            }
            when the_identifier_is_valid {
                when the_status_is_unknown {
                    to rejects_the_status_before_the_payload { be_err_and status_row_error }
                }
                when the_task_is_pending {
                    let status: Option<&'static str> = Some("Pending");
                    when the_result_is_successful {
                        let result = Some(json!({"Ok": "processed"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Pending, true) }
                    }
                    when the_result_is_a_failure {
                        let result = Some(json!({"Err": "boom"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Pending, false) }
                    }
                    when the_payload_is_malformed {
                        to rejects_the_payload { be_err_and json_error }
                    }
                }
                when the_task_is_queued {
                    let status: Option<&'static str> = Some("Queued");
                    when the_result_is_successful {
                        let result = Some(json!({"Ok": "processed"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Queued, true) }
                    }
                    when the_result_is_a_failure {
                        let result = Some(json!({"Err": "boom"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Queued, false) }
                    }
                    when the_payload_is_malformed {
                        to rejects_the_payload { be_err_and json_error }
                    }
                }
                when the_task_is_running {
                    let status: Option<&'static str> = Some("Running");
                    when the_result_is_successful {
                        let result = Some(json!({"Ok": "processed"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Running, true) }
                    }
                    when the_result_is_a_failure {
                        let result = Some(json!({"Err": "boom"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Running, false) }
                    }
                    when the_payload_is_malformed {
                        to rejects_the_payload { be_err_and json_error }
                    }
                }
                when the_task_is_done {
                    let status: Option<&'static str> = Some("Done");
                    when the_result_is_successful {
                        let result = Some(json!({"Ok": "processed"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Done, true) }
                    }
                    when the_result_is_a_failure {
                        let result = Some(json!({"Err": "boom"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Done, false) }
                    }
                    when the_payload_is_malformed {
                        to rejects_the_payload { be_err_and json_error }
                    }
                }
                when the_task_is_failed {
                    let status: Option<&'static str> = Some("Failed");
                    when the_result_is_successful {
                        let result = Some(json!({"Ok": "processed"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Failed, true) }
                    }
                    when the_result_is_a_failure {
                        let result = Some(json!({"Err": "boom"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Failed, false) }
                    }
                    when the_payload_is_malformed {
                        to rejects_the_payload { be_err_and json_error }
                    }
                }
                when the_task_is_killed {
                    let status: Option<&'static str> = Some("Killed");
                    when the_result_is_successful {
                        let result = Some(json!({"Ok": "processed"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Killed, true) }
                    }
                    when the_result_is_a_failure {
                        let result = Some(json!({"Err": "boom"}));
                        to preserves_the_complete_result { be_ok_and exact_task_result(Status::Killed, false) }
                    }
                    when the_payload_is_malformed {
                        to rejects_the_payload { be_err_and json_error }
                    }
                }
            }
        }
    }

    #[derive(Clone, Copy)]
    enum Hints {
        Empty,
        Mixed,
        Errors,
    }

    type HintResult = Result<Option<PgTask<CompactType>>, Error>;

    fn rejected_hint_chunks(
        shape: Hints,
        capacity: usize,
        error_position: usize,
    ) -> Vec<HintResult> {
        use futures::{StreamExt, executor::block_on, stream};
        let hints = (0..if matches!(shape, Hints::Empty) { 0 } else { 3 })
            .map(|index| {
                if matches!(shape, Hints::Errors) || index == error_position {
                    Err(Error::InvalidArgument(format!("notification {index}")))
                } else {
                    Ok(crate::PgTaskId::new(Ulid::new()))
                }
            })
            .collect::<Vec<_>>();
        let mut output = batch_ids_into_tasks(
            crate::unreachable::unreachable_pool(),
            "queue".to_owned(),
            "worker".to_owned(),
            capacity,
            stream::iter(hints),
            None,
        )
        .boxed();
        if matches!(shape, Hints::Mixed) {
            // The retained hints need SQL; inspect only the preceding listener
            // error here. Database regressions assert the complete task stream.
            block_on(output.next()).into_iter().collect()
        } else {
            block_on(output.collect())
        }
    }

    fn exact_hint_errors(positions: Vec<usize>) -> impl Fn(&Vec<HintResult>) -> AssertionResult {
        move |results| {
            let expected = positions
                .iter()
                .map(|i| format!("notification {i}"))
                .collect::<Vec<_>>();
            let actual = results
                .iter()
                .map(|result| match result {
                    Err(Error::InvalidArgument(message)) => Some(message.clone()),
                    _ => None,
                })
                .collect::<Option<Vec<_>>>();
            if actual.as_ref() == Some(&expected) {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected {expected:?}, got {results:?}"
                )]))
            }
        }
    }

    lets_expect! {
        expect(rejected_hint_chunks(Hints::Empty, capacity, 0)) as empty_notification_stream {
            let capacity = 0;
            to ends_without_a_batch { exact_hint_errors(vec![]) }
            when capacity_is_one {
                let capacity = 1;
                to ends_without_a_batch { exact_hint_errors(vec![]) }
            }
            when capacity_is_larger {
                let capacity = 8;
                to ends_without_a_batch { exact_hint_errors(vec![]) }
            }
        }
        expect(rejected_hint_chunks(Hints::Mixed, capacity, error_position)) as rejected_notification_batch {
            let capacity = 3;
            let error_position = 0;
            to reports_the_error_before_claiming_retained_hints { exact_hint_errors(vec![0]) }
            when the_middle_hint_is_invalid {
                let error_position = 1;
                to reports_the_error_before_claiming_retained_hints { exact_hint_errors(vec![1]) }
            }
            when the_last_hint_is_invalid {
                let error_position = 2;
                to reports_the_error_before_claiming_retained_hints { exact_hint_errors(vec![2]) }
            }
            when capacity_exceeds_the_batch {
                let capacity = 8;
                to reports_the_error_before_claiming_retained_hints { exact_hint_errors(vec![0]) }
                when the_middle_hint_is_invalid {
                    let error_position = 1;
                    to reports_the_error_before_claiming_retained_hints { exact_hint_errors(vec![1]) }
                }
                when the_last_hint_is_invalid {
                    let error_position = 2;
                    to reports_the_error_before_claiming_retained_hints { exact_hint_errors(vec![2]) }
                }
            }
        }
        expect(rejected_hint_chunks(Hints::Errors, capacity, 0)) as invalid_notification_stream {
            let capacity = 0;
            to returns_each_single_hint_error { exact_hint_errors(vec![0,1,2]) }
            when capacity_is_one {
                let capacity = 1;
                to returns_each_single_hint_error { exact_hint_errors(vec![0,1,2]) }
            }
            when capacity_exceeds_the_stream {
                let capacity = 8;
                to returns_every_hint_error { exact_hint_errors(vec![0,1,2]) }
            }
        }
    }
}

pub(crate) mod migrations;

#[cfg(all(test, feature = "tokio"))]
#[path = "../query_specs/notification_regressions.rs"]
mod notification_regressions;
