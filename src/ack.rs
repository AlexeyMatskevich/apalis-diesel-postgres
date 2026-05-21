use apalis_core::{
    error::{AbortError, BoxDynError},
    layers::{Layer, Service},
    task::{Parts, status::Status},
    worker::{
        context::WorkerContext,
        ext::ack::{Acknowledge, AcknowledgeLayer},
    },
};
use futures::{
    FutureExt,
    future::{BoxFuture, Either},
};
use serde::Serialize;
use ulid::Ulid;

use std::sync::Arc;

use crate::{Error, PgContext, PgPool, PgTask, queries};

/// Acknowledges task completion by updating `apalis.jobs`.
///
/// When constructed via [`PgAck::with_lease_token`], the acknowledge SQL is
/// additionally bound to the worker's `lease_token` so the per-process secret
/// that already protects heartbeat refreshes (migration `20260521000002`) also
/// guards ack writes. Callers that hold only `(task_id, queue, worker_id,
/// lock_at, attempts)` — values that appear in dashboards and admin payloads —
/// cannot forge an ack without also possessing the token.
#[derive(Debug, Clone)]
pub struct PgAck {
    pool: PgPool,
    lease_token: Option<Arc<str>>,
}

#[cfg(test)]
mod tests {
    use std::{
        future::{Ready, ready},
        task::{Context, Poll},
    };

    use apalis_core::{
        error::BoxDynError,
        layers::Service,
        task::{Parts, attempt::Attempt, builder::TaskBuilder, status::Status, task_id::TaskId},
        worker::ext::ack::Acknowledge,
    };
    use diesel::{
        PgConnection,
        r2d2::{ConnectionManager, Pool},
    };
    use futures::{executor::block_on, task::noop_waker_ref};
    use lets_expect::{AssertionError, AssertionResult, *};

    use super::*;

    #[derive(Debug, Clone)]
    enum ReadyState {
        Ready,
        Error,
        Pending,
    }

    #[derive(Debug, Clone)]
    struct ReadyService {
        state: ReadyState,
    }

    impl Service<PgTask<()>> for ReadyService {
        type Response = ();
        type Error = std::io::Error;
        type Future = Ready<Result<(), Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            match self.state {
                ReadyState::Ready => Poll::Ready(Ok(())),
                ReadyState::Error => Poll::Ready(Err(std::io::Error::other("inner failed"))),
                ReadyState::Pending => Poll::Pending,
            }
        }

        fn call(&mut self, _req: PgTask<()>) -> Self::Future {
            ready(Ok(()))
        }
    }

    fn unchecked_pool() -> PgPool {
        let manager = ConnectionManager::<PgConnection>::new("postgres://127.0.0.1:1/not-used");
        Pool::builder()
            .max_size(1)
            .connection_timeout(std::time::Duration::from_millis(10))
            .build_unchecked(manager)
    }

    fn task_id() -> TaskId<Ulid> {
        TaskId::new(Ulid::new())
    }

    fn parts_for_ack(attempts: usize, max_attempts: i32) -> Parts<PgContext, Ulid> {
        TaskBuilder::new(())
            .with_task_id(task_id())
            .with_attempt(Attempt::new_with_value(attempts))
            .with_ctx(PgContext::new().with_max_attempts(max_attempts))
            .build()
            .parts
    }

    fn box_error(message: &'static str) -> BoxDynError {
        std::io::Error::other(message).into()
    }

    fn ack_missing_field(
        has_task_id: bool,
        has_lock_by: bool,
        has_queue: bool,
        has_lock_at: bool,
    ) -> Result<(), crate::Error> {
        block_on(async move {
            let mut parts = parts_for_ack(1, 3);
            if !has_task_id {
                parts.task_id = None;
            }
            let mut ctx = parts.ctx.clone();
            if has_lock_by {
                ctx = ctx.with_lock_by(Some("ack-worker".to_owned()));
            }
            if has_queue {
                ctx = ctx.with_queue("ack-queue".to_owned());
            }
            if has_lock_at {
                ctx = ctx.with_lock_at(Some(1_700_000_000));
            }
            parts.ctx = ctx;

            let mut ack = PgAck::new(unchecked_pool());
            let result: Result<(), BoxDynError> = Ok(());
            ack.ack(&result, &parts).await
        })
    }

    fn truncated_payload_length(input_len: usize) -> usize {
        truncate_error_payload("x".repeat(input_len)).len()
    }

    fn truncated_payload_marker_present(input_len: usize) -> bool {
        truncate_error_payload("x".repeat(input_len)).ends_with("…[truncated]")
    }

    fn poll_lock_ready(state: ReadyState) -> Poll<Result<(), BoxDynError>> {
        let mut service = LockTaskService {
            inner: ReadyService { state },
            pool: unchecked_pool(),
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        service.poll_ready(&mut cx)
    }

    fn layered_service_debug() -> String {
        let layer = LockTaskLayer::new(unchecked_pool());
        let service = layer.layer(ReadyService {
            state: ReadyState::Ready,
        });
        format!("{service:?}")
    }

    fn middleware_auto_ack_enabled(auto_ack: bool) -> bool {
        PgMiddleware::new(unchecked_pool(), auto_ack).auto_ack()
    }

    async fn lock_service_call_async(
        has_worker: bool,
        has_task_id: bool,
    ) -> Result<(), BoxDynError> {
        let mut task = TaskBuilder::new(())
            .with_ctx(PgContext::new().with_queue("lock-service-unit".to_owned()))
            .build();
        if has_worker {
            task.parts
                .data
                .insert(WorkerContext::new::<()>("lock-service-worker"));
        }
        if has_task_id {
            task.parts.task_id = Some(task_id());
        }

        let mut service = LockTaskService {
            inner: ReadyService {
                state: ReadyState::Ready,
            },
            pool: unchecked_pool(),
        };
        service.call(task).await
    }

    fn lock_service_call_missing_field(
        has_worker: bool,
        has_task_id: bool,
    ) -> Result<(), BoxDynError> {
        block_on(lock_service_call_async(has_worker, has_task_id))
    }

    fn missing_field(field: &'static str) -> impl Fn(&crate::Error) -> AssertionResult {
        move |error| match error {
            crate::Error::MissingField(found) if *found == field => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected missing field {field}, got {other:?}"
            )])),
        }
    }

    fn poll_ready_ok(result: &Poll<Result<(), BoxDynError>>) -> AssertionResult {
        match result {
            Poll::Ready(Ok(())) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected ready ok, got {other:?}"
            )])),
        }
    }

    fn poll_ready_err(result: &Poll<Result<(), BoxDynError>>) -> AssertionResult {
        match result {
            Poll::Ready(Err(_)) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected ready error, got {other:?}"
            )])),
        }
    }

    fn poll_pending(result: &Poll<Result<(), BoxDynError>>) -> AssertionResult {
        match result {
            Poll::Pending => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected pending, got {other:?}"
            )])),
        }
    }

    fn debug_mentions_lock_service(result: &String) -> AssertionResult {
        if result.contains("LockTaskService") && result.contains("pool") {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected lock service debug output, got {result}"
            )]))
        }
    }

    fn abort_contains(expected: &'static str) -> impl Fn(&BoxDynError) -> AssertionResult {
        move |error| {
            let message = error.to_string();
            if message.contains(expected) {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected abort containing {expected:?}, got {message:?}"
                )]))
            }
        }
    }

    lets_expect! {
        expect(calculate_status(&parts, &result)) {
            let parts = parts_for_ack(attempts, max_attempts);
            let result: Result<(), BoxDynError> = Ok(());
            let attempts = 1;
            let max_attempts = 3;

            when task_succeeds {
                to marks_the_task_done { equal(Status::Done) }
            }

            when task_fails_below_the_attempt_limit {
                let result: Result<(), BoxDynError> = Err(box_error("retry"));
                to marks_the_task_failed { equal(Status::Failed) }
            }

            when task_fails_at_the_attempt_limit {
                let attempts = 3;
                let result: Result<(), BoxDynError> = Err(box_error("exact limit"));
                to kills_the_task { equal(Status::Killed) }
            }

            when task_fails_above_the_attempt_limit {
                let attempts = 4;
                let result: Result<(), BoxDynError> = Err(box_error("above limit"));
                to kills_the_task { equal(Status::Killed) }
            }

            when task_fails_with_a_negative_max_attempts_from_a_corrupt_row {
                // Documents the doc-comment contract on `calculate_status`:
                // negative `max_attempts` (which the schema rejects but a
                // hand-crafted row could carry) is treated as terminal so a
                // corrupt row cannot drive an infinite retry loop.
                // `usize::try_from(-1)` returns Err, falling into the `_ =>
                // Killed` arm.
                let max_attempts = -1;
                let attempts = 0;
                let result: Result<(), BoxDynError> = Err(box_error("corrupt row"));
                to kills_the_task_to_avoid_an_infinite_retry { equal(Status::Killed) }
            }

            when task_fails_with_zero_max_attempts_on_the_first_attempt {
                // Boundary case: max=0 means no retries are allowed. The
                // first failure (attempts >= max=0) must terminate.
                let max_attempts = 0;
                let attempts = 0;
                let result: Result<(), BoxDynError> = Err(box_error("no retries"));
                to kills_the_task { equal(Status::Killed) }
            }
        }

        expect(poll_lock_ready(state)) {
            let state = ReadyState::Ready;

            when inner_service_is_ready {
                to returns_ready { poll_ready_ok }
            }

            when inner_service_returns_an_error {
                let state = ReadyState::Error;
                to returns_the_error { poll_ready_err }
            }

            when inner_service_is_pending {
                let state = ReadyState::Pending;
                to stays_pending { poll_pending }
            }
        }

        expect(layered_service_debug()) {
            to wraps_the_inner_service_with_the_pool { debug_mentions_lock_service }
        }

        expect(middleware_auto_ack_enabled(auto_ack)) {
            let auto_ack = true;

            when config_enables_auto_ack {
                to installs_the_acknowledgement_layer { equal(true) }
            }

            when config_disables_auto_ack {
                let auto_ack = false;
                to leaves_acknowledgement_to_the_caller { equal(false) }
            }
        }
    }

    lets_expect! {
        expect(ack_missing_field(has_task_id, has_lock_by, has_queue, has_lock_at)) {
            let has_task_id = true;
            let has_lock_by = true;
            let has_queue = true;
            let has_lock_at = true;

            when task_id_is_missing {
                let has_task_id = false;
                to rejects_before_querying_the_database { be_err_and missing_field("task_id") }
            }

            when lock_owner_is_missing {
                let has_lock_by = false;
                to rejects_before_querying_the_database { be_err_and missing_field("lock_by") }
            }

            when queue_is_missing {
                let has_queue = false;
                to rejects_before_querying_the_database { be_err_and missing_field("queue") }
            }

            when lock_timestamp_is_missing {
                let has_lock_at = false;
                to rejects_before_querying_the_database { be_err_and missing_field("lock_at") }
            }
        }

        expect(lock_service_call_missing_field(has_worker, has_task_id)) {
            let has_worker = true;
            let has_task_id = true;

            when worker_context_is_missing {
                let has_worker = false;
                to aborts_before_locking_the_task { be_err_and abort_contains("worker_context") }
            }

            when task_id_is_missing {
                let has_task_id = false;
                to aborts_before_locking_the_task { be_err_and abort_contains("task_id") }
            }
        }

        expect(truncated_payload_length(input_len)) {
            let input_len = 100;

            when payload_is_shorter_than_the_eight_kib_cap {
                to leaves_the_payload_length_unchanged { equal(100) }
            }

            when payload_is_exactly_eight_kib {
                let input_len = 8 * 1024;
                to leaves_the_payload_length_unchanged { equal(8 * 1024) }
            }

            when payload_is_one_byte_above_eight_kib {
                let input_len = 8 * 1024 + 1;
                to truncates_to_eight_kib_plus_the_marker_byte_length {
                    equal(8 * 1024 + "…[truncated]".len())
                }
            }

            when payload_is_far_above_eight_kib {
                let input_len = 64 * 1024;
                to truncates_to_eight_kib_plus_the_marker_byte_length {
                    equal(8 * 1024 + "…[truncated]".len())
                }
            }
        }

        expect(truncated_payload_marker_present(input_len)) {
            let input_len = 100;

            when payload_is_within_budget {
                to does_not_append_a_truncation_marker { equal(false) }
            }

            when payload_overflows_the_budget {
                let input_len = 8 * 1024 + 1;
                to appends_the_truncation_marker { equal(true) }
            }
        }
    }

    #[cfg(feature = "tokio")]
    mod tokio_tests {
        use super::*;
        use serde::{Serialize, Serializer, ser};

        /// Drive `PgAck::ack` with an oversized attempt counter. The bounds
        /// check on `i32::try_from(attempts_raw)` (src/ack.rs:532) returns
        /// `Error::InvalidArgument`; without this branch a saturated cast
        /// would silently mismatch the row's `attempts` column and surface
        /// as a spurious `StaleAcknowledgement`.
        async fn ack_with_attempt_overflow() -> Result<(), crate::Error> {
            let mut parts = parts_for_ack(1, 3);
            // Force an overflow regardless of host pointer width.
            parts.attempt = Attempt::new_with_value(i32::MAX as usize + 1);
            parts.ctx = parts
                .ctx
                .clone()
                .with_queue("ack-queue".to_owned())
                .with_lock_by(Some("ack-worker".to_owned()))
                .with_lock_at(Some(1_700_000_000));
            let mut ack = PgAck::new(unchecked_pool());
            let result: Result<(), BoxDynError> = Ok(());
            ack.ack(&result, &parts).await
        }

        fn invalid_attempt_overflow(error: &crate::Error) -> AssertionResult {
            match error {
                crate::Error::InvalidArgument(msg) if msg.contains("attempt counter") => Ok(()),
                other => Err(AssertionError::new(vec![format!(
                    "expected InvalidArgument citing attempt counter overflow, got {other:?}"
                )])),
            }
        }

        /// Custom type that fails to serialize — drives the
        /// `serde_json::to_value(result)?` arm in `PgAck::ack`
        /// (src/ack.rs:512,549). Reachable for any job that returns a custom
        /// `Ok` payload with a fallible `Serialize` impl.
        #[derive(Debug)]
        struct PoisonOk;

        impl Serialize for PoisonOk {
            fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(ser::Error::custom("intentional serialize failure"))
            }
        }

        async fn ack_with_unserializable_result() -> Result<(), crate::Error> {
            let mut parts: Parts<PgContext, Ulid> = TaskBuilder::new(())
                .with_task_id(task_id())
                .with_attempt(Attempt::new_with_value(1))
                .with_ctx(
                    PgContext::new()
                        .with_max_attempts(3)
                        .with_queue("ack-queue".to_owned())
                        .with_lock_by(Some("ack-worker".to_owned()))
                        .with_lock_at(Some(1_700_000_000)),
                )
                .build()
                .parts;
            // Need a Parts whose payload-channel type matches PoisonOk.
            let _ = &mut parts;
            let mut ack = PgAck::new(unchecked_pool());
            let result: Result<PoisonOk, BoxDynError> = Ok(PoisonOk);
            ack.ack(&result, &parts).await
        }

        fn json_serialize_error(error: &crate::Error) -> AssertionResult {
            match error {
                crate::Error::Json(_) => Ok(()),
                other => Err(AssertionError::new(vec![format!(
                    "expected Error::Json from a failing Serialize impl, got {other:?}"
                )])),
            }
        }

        /// Drive `LockTaskService::call` with a task whose `lock_by` already
        /// matches the worker context and `lock_at` is populated. The
        /// `preclaimed` branch at src/ack.rs:793-810 must bypass the SQL
        /// `lock_task` call entirely, so this exercise succeeds even with a
        /// pool that cannot connect.
        async fn lock_service_call_preclaimed() -> Result<(), BoxDynError> {
            let mut task = TaskBuilder::new(())
                .with_task_id(task_id())
                .with_ctx(
                    PgContext::new()
                        .with_queue("lock-service-unit".to_owned())
                        .with_lock_by(Some("lock-service-worker".to_owned()))
                        .with_lock_at(Some(1_700_000_000)),
                )
                .build();
            task.parts
                .data
                .insert(WorkerContext::new::<()>("lock-service-worker"));

            let mut service = LockTaskService {
                inner: ReadyService {
                    state: ReadyState::Ready,
                },
                pool: unchecked_pool(),
            };
            service.call(task).await
        }

        fn lock_service_call_preclaimed_succeeds(
            result: &Result<(), BoxDynError>,
        ) -> AssertionResult {
            match result {
                Ok(()) => Ok(()),
                Err(error) => Err(AssertionError::new(vec![format!(
                    "expected the preclaimed branch to bypass lock_task and succeed, got {error}"
                )])),
            }
        }

        lets_expect! { #tokio_test
            expect(lock_service_call_async(true, true).await) {
                when task_has_worker_and_id_but_the_database_is_unavailable {
                    to aborts_with_the_lock_error { be_err_and abort_contains("failed to acquire PostgreSQL connection") }
                }
            }

            expect(ack_with_attempt_overflow().await) {
                when the_attempt_counter_exceeds_i32_max {
                    to surfaces_invalid_argument_before_touching_the_database {
                        be_err_and invalid_attempt_overflow
                    }
                }
            }

            expect(ack_with_unserializable_result().await) {
                when the_jobs_ok_payload_fails_to_serialize {
                    to surfaces_an_error_json_before_touching_the_database {
                        be_err_and json_serialize_error
                    }
                }
            }

            expect(lock_service_call_preclaimed().await) {
                when the_task_already_carries_a_matching_lock_by_and_lock_at {
                    to bypasses_the_sql_lock_task_round_trip_and_completes {
                        lock_service_call_preclaimed_succeeds
                    }
                }
            }
        }
    }
}

impl PgAck {
    /// Create a PostgreSQL acknowledger without lease-token binding.
    ///
    /// Ack writes are gated only by `(lock_by, lock_at, attempts)`; prefer
    /// [`PgAck::with_lease_token`] for the defense-in-depth variant that also
    /// checks the per-process token. This constructor exists for test harnesses
    /// and admin tooling that do not own a lease token.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            lease_token: None,
        }
    }

    /// Create a PostgreSQL acknowledger bound to a specific worker lease token.
    ///
    /// The token is added to the ack SQL as an `EXISTS` check against
    /// `apalis.workers.lease_token`, mirroring the heartbeat path. A storage
    /// handle's `middleware()` wires this automatically; manual callers should
    /// reuse the token they passed to `initial_heartbeat`/`keep_alive`.
    #[must_use]
    pub fn with_lease_token(pool: PgPool, lease_token: Arc<str>) -> Self {
        Self {
            pool,
            lease_token: Some(lease_token),
        }
    }
}

// Cap persisted error strings so a misbehaving job that returns a
// multi-megabyte `Display` cannot balloon `apalis.jobs.last_result` (a
// JSONB column) and exhaust storage. 8 KiB preserves diagnostic value
// without unbounded growth; truncated strings get a clear marker.
const MAX_ERROR_PAYLOAD_LEN: usize = 8 * 1024;
const TRUNCATION_MARKER: &str = "…[truncated]";

pub(crate) fn truncate_error_payload(mut text: String) -> String {
    if text.len() > MAX_ERROR_PAYLOAD_LEN {
        // `String::truncate` panics if the cut index is not at a UTF-8 char
        // boundary; walk back to the nearest boundary so multi-byte sequences
        // are never split mid-codepoint. `str::floor_char_boundary` would
        // replace this loop but is only stable since 1.91 (crate MSRV 1.88).
        let mut cut = MAX_ERROR_PAYLOAD_LEN;
        while cut > 0 && !text.is_char_boundary(cut) {
            cut -= 1;
        }
        text.truncate(cut);
        text.push_str(TRUNCATION_MARKER);
    }
    text
}

impl<Res: Serialize> Acknowledge<Res, PgContext, Ulid> for PgAck {
    type Error = Error;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<PgContext, Ulid>,
    ) -> Self::Future {
        let task_id = parts.task_id;
        let worker_id = parts.ctx.lock_by().clone();
        let queue = parts.ctx.queue().clone();
        let lock_at = *parts.ctx.lock_at();
        let response = serde_json::to_value(
            res.as_ref()
                .map_err(|error| truncate_error_payload(error.to_string())),
        );
        let status = calculate_status(parts, res);
        // `last_result` is always persisted as the externally-tagged
        // `Result<O, String>` JSON (`{"Ok": ...}` or `{"Err": "..."}`).
        // `WaitForCompletion::wait_for` reads it back with `serde_json::from_value`
        // and the spec (queries/mod.rs tests::last_result_is_missing) requires
        // a SQL NULL to surface as `MissingField("last_result")`. So we wrap
        // every serialized value in `Some(...)` rather than collapsing the
        // trivial-Ok case to SQL NULL, which would make completed Ok(()) jobs
        // appear unread to consumers of `WaitForCompletion`.
        let response = response.map(Some);
        // Silent saturation would corrupt the ack's lock-check predicate:
        // `ack_task` matches on `attempts = $started_attempts`, so a capped
        // value would silently mismatch the stored row and the ack would be
        // reported as `StaleAcknowledgement` for a non-stale task. Surface
        // overflow as `InvalidArgument` instead.
        let attempts_raw = parts.attempt.current();
        let attempts = i32::try_from(attempts_raw);
        let pool = self.pool.clone();
        let lease_token = self.lease_token.clone();

        async move {
            let attempts = attempts.map_err(|_| {
                Error::InvalidArgument(format!(
                    "task attempt counter {attempts_raw} exceeds i32::MAX and cannot be stored"
                ))
            })?;
            let started_attempts = attempts.saturating_sub(1);
            queries::ack_task(
                pool,
                queries::AckTaskUpdate {
                    task_id: task_id.ok_or(Error::MissingField("task_id"))?,
                    attempts,
                    started_attempts,
                    result: response?,
                    status,
                    worker_id: worker_id.ok_or(Error::MissingField("lock_by"))?,
                    queue: queue.ok_or(Error::MissingField("queue"))?,
                    lock_at: lock_at.ok_or(Error::MissingField("lock_at"))?,
                    lease_token: lease_token.as_deref().map(str::to_owned),
                },
            )
            .await
        }
        .boxed()
    }
}

/// Calculate the persisted task status from a task execution result.
///
/// Negative `max_attempts` values (which the database schema rejects) are
/// treated as terminal so a corrupt row cannot drive an infinite retry loop.
#[must_use]
pub(crate) fn calculate_status<Res>(
    parts: &Parts<PgContext, Ulid>,
    res: &Result<Res, BoxDynError>,
) -> Status {
    match res {
        Ok(_) => Status::Done,
        Err(_) => match usize::try_from(parts.ctx.max_attempts()) {
            Ok(max) if max > parts.attempt.current() => Status::Failed,
            _ => Status::Killed,
        },
    }
}

/// Lock a due task for a worker.
///
/// The worker must already be registered for the task queue. The task must be
/// due and in a lockable state: `Pending`, retryable `Failed`, or `Queued` by
/// the same worker.
///
/// # Cross-queue semantics
///
/// This entry point does **not** filter by `job_type`: a caller holding a
/// task's `Ulid` can lock it regardless of which queue it belongs to. Prefer
/// [`lock_task_in_queue`] which scopes the lock to a specific queue and
/// prevents a caller that learned a `Ulid` from logs or dashboards from
/// claiming it under an unrelated queue.
pub async fn lock_task(pool: &PgPool, task_id: &Ulid, worker_id: &str) -> Result<(), Error> {
    queries::lock_task(pool.clone(), *task_id, worker_id.to_owned(), None).await
}

/// Lock a due task scoped to a specific queue.
///
/// Like [`lock_task`] but restricts the lock to `queue` so admin tooling that
/// knows the task's `Ulid` cannot accidentally (or maliciously) lock a task
/// belonging to another queue. Use this in any code path that does not derive
/// the queue from a trusted `WorkerContext`.
pub async fn lock_task_in_queue(
    pool: &PgPool,
    task_id: &Ulid,
    worker_id: &str,
    queue: &str,
) -> Result<(), Error> {
    queries::lock_task(
        pool.clone(),
        *task_id,
        worker_id.to_owned(),
        Some(queue.to_owned()),
    )
    .await
}

/// Middleware layer that transitions queued jobs to `Running` before execution.
///
/// Crate-private: external callers use [`PgMiddleware`], which composes this
/// layer with the optional auto-ack layer. Exposed only to the crate so the
/// `Layer<S>` impl on `PgMiddleware` can reference its `Service` type without
/// leaking via a public trait bound.
#[derive(Debug, Clone)]
pub(crate) struct LockTaskLayer {
    pool: PgPool,
}

impl LockTaskLayer {
    /// Create a lock middleware layer.
    #[must_use]
    pub(crate) fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl<S> Layer<S> for LockTaskLayer {
    type Service = LockTaskService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        LockTaskService {
            inner,
            pool: self.pool.clone(),
        }
    }
}

/// Middleware layer used by the PostgreSQL backend.
///
/// The lock step always runs. The acknowledge step is installed only when the
/// queue config has automatic acknowledgement enabled.
#[derive(Debug, Clone)]
pub struct PgMiddleware {
    lock: LockTaskLayer,
    ack: Option<AcknowledgeLayer<PgAck>>,
}

impl PgMiddleware {
    /// Create the PostgreSQL backend middleware.
    #[must_use]
    pub fn new(pool: PgPool, auto_ack: bool) -> Self {
        Self {
            lock: LockTaskLayer::new(pool.clone()),
            ack: auto_ack.then(|| AcknowledgeLayer::new(PgAck::new(pool))),
        }
    }

    /// Create the PostgreSQL backend middleware with lease-token binding for
    /// the auto-ack path. Used by [`crate::PostgresStorage`] so completed jobs
    /// can only be acknowledged by a worker possessing the per-storage token.
    #[must_use]
    pub fn with_lease_token(pool: PgPool, auto_ack: bool, lease_token: Arc<str>) -> Self {
        Self {
            lock: LockTaskLayer::new(pool.clone()),
            ack: auto_ack
                .then(|| AcknowledgeLayer::new(PgAck::with_lease_token(pool, lease_token))),
        }
    }

    /// Return whether this middleware will acknowledge tasks after execution.
    #[must_use]
    pub fn auto_ack(&self) -> bool {
        self.ack.is_some()
    }
}

impl<S> Layer<S> for PgMiddleware
where
    AcknowledgeLayer<PgAck>: Layer<LockTaskService<S>>,
{
    type Service = PgMiddlewareService<
        <AcknowledgeLayer<PgAck> as Layer<LockTaskService<S>>>::Service,
        LockTaskService<S>,
    >;

    fn layer(&self, inner: S) -> Self::Service {
        // Construct `LockTaskService` directly rather than going through
        // `LockTaskLayer::layer` so the where-bound on this public `Layer<S>`
        // impl does not reference the crate-private `LockTaskLayer` type
        // (which would trigger E0446). The `lock` field is kept on
        // `PgMiddleware` to centralise pool ownership and avoid duplicating
        // the constructor's pool-clone logic.
        let locked = LockTaskService {
            inner,
            pool: self.lock.pool.clone(),
        };
        match &self.ack {
            Some(ack) => PgMiddlewareService::AutoAck(ack.layer(locked)),
            None => PgMiddlewareService::ManualAck(locked),
        }
    }
}

/// Service produced by [`PgMiddleware`].
#[derive(Debug, Clone)]
pub enum PgMiddlewareService<AutoAck, ManualAck> {
    /// Lock tasks and acknowledge them automatically.
    AutoAck(AutoAck),
    /// Lock tasks only, leaving acknowledgement to the caller.
    ManualAck(ManualAck),
}

impl<Req, AutoAck, ManualAck> Service<Req> for PgMiddlewareService<AutoAck, ManualAck>
where
    AutoAck: Service<Req>,
    ManualAck: Service<Req, Response = AutoAck::Response, Error = AutoAck::Error>,
{
    type Response = AutoAck::Response;
    type Error = AutoAck::Error;
    type Future = Either<AutoAck::Future, ManualAck::Future>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        match self {
            Self::AutoAck(service) => service.poll_ready(cx),
            Self::ManualAck(service) => service.poll_ready(cx),
        }
    }

    fn call(&mut self, req: Req) -> Self::Future {
        match self {
            Self::AutoAck(service) => Either::Left(service.call(req)),
            Self::ManualAck(service) => Either::Right(service.call(req)),
        }
    }
}

/// Service produced by [`LockTaskLayer`].
#[derive(Debug, Clone)]
pub struct LockTaskService<S> {
    inner: S,
    pool: PgPool,
}

impl<S, Args> Service<PgTask<Args>> for LockTaskService<S>
where
    S: Service<PgTask<Args>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    Args: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: PgTask<Args>) -> Self::Future {
        let pool = self.pool.clone();
        let worker_id = req
            .parts
            .data
            .get::<WorkerContext>()
            .map(|worker| worker.name().to_owned());
        let queue = req.parts.ctx.queue().clone();
        let task_id = req.parts.task_id.map(|id| *id.inner());
        // Skip the lock_task round-trip for tasks that the fetcher already
        // transitioned to `Running` and locked to this worker (`fetch_next`
        // and `queue_by_id` set both `lock_by` and `lock_at` in the dequeue
        // UPDATE). In that case the SQL `lock_task` would only rewrite the
        // same values, paying a full per-job round-trip + HOT-tuple write
        // for nothing. External `lock_task` callers (and any future fetcher
        // that does not pre-lock) still go through the SQL path because they
        // arrive without `lock_by`/`lock_at` populated in the context.
        let preclaimed = matches!(
            (req.parts.ctx.lock_by().as_deref(), worker_id.as_deref()),
            (Some(stored), Some(current)) if stored == current
        ) && req.parts.ctx.lock_at().is_some();
        // Tower service contract: `poll_ready` reserves capacity on
        // `self.inner`; that exact instance MUST be the one that consumes the
        // reservation via `call`. Take ownership of the ready instance and
        // leave a clone behind so subsequent `poll_ready`/`call` cycles work.
        // The clone is treated as a fresh, not-yet-ready instance — the caller
        // will `poll_ready` it again before sending the next request.
        let clone = self.inner.clone();
        let mut ready_inner = std::mem::replace(&mut self.inner, clone);

        async move {
            let worker_id =
                worker_id.ok_or_else(|| AbortError::new(Error::MissingField("worker_context")))?;
            let task_id = task_id.ok_or_else(|| AbortError::new(Error::MissingField("task_id")))?;
            if !preclaimed {
                queries::lock_task(pool, task_id, worker_id, queue)
                    .await
                    .map_err(AbortError::new)?;
            }
            ready_inner.call(req).await.map_err(Into::into)
        }
        .boxed()
    }
}
