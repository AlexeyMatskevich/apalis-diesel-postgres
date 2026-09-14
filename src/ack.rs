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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use crate::{Error, PgContext, PgPool, PgTask, PgTaskId, queries};

/// Acknowledges task completion by updating `apalis.jobs`.
///
/// When constructed via [`PgAck::with_lease_token`], the acknowledge SQL is
/// additionally bound to the worker's `lease_token` so the storage registration secret
/// that already protects heartbeat refreshes (migration `20260521000002`) also
/// guards ack writes. Callers that hold only `(task_id, queue, worker_id,
/// lock_at, attempts)` — values that appear in dashboards and admin payloads —
/// cannot forge an ack without also possessing the token.
///
/// Tasks returned by a storage claim carry an in-memory snapshot of their
/// persisted attempt count. Keep their `Parts` (including `data`) when
/// acknowledging manually: the snapshot accounts for one completed execution
/// even without an Apalis Tracker. It survives `Parts::clone` and ack retries,
/// but is not serialized. For manually assembled Parts without that snapshot,
/// the caller supplies an `Attempt` including the completed execution, as in
/// the low-level acknowledgement contract.
#[derive(Clone)]
pub struct PgAck {
    pool: PgPool,
    lease_token: Option<Arc<str>>,
    leases: Option<crate::lease::LeaseRegistry>,
}

/// The persisted history returned by one SQL claim, independent of Apalis's
/// mutable execution counter. Keep it across Parts clones and ack retries:
/// consuming it would lose the original predicate after a cancelled ack.
#[derive(Clone)]
struct ClaimAttempt {
    task_id: Option<PgTaskId>,
    lock_at: Option<i64>,
    completed: usize,
    /// Shared by every clone of the task, so a re-dispatch of the same claim
    /// is refused instead of running the handler again and reporting a stale
    /// acknowledgement.
    acknowledgement: Arc<ClaimAcknowledgement>,
}

/// The acknowledgement state of one claim: the marker set once an
/// acknowledgement commits, and the lock that serializes concurrent
/// acknowledgements of the claim so the second observes the first instead
/// of racing it to a stale result.
struct ClaimAcknowledgement {
    serial: futures::lock::Mutex<()>,
    done: AtomicBool,
    /// Set once `LockTaskService` moved this claim's row to `Running`. A
    /// re-dispatch of the same claim may start it again; a different claim
    /// that happens to share the epoch may not.
    started: AtomicBool,
}

impl ClaimAttempt {
    fn is_acknowledged(&self) -> bool {
        self.acknowledgement.done.load(Ordering::Acquire)
    }

    fn mark_acknowledged(&self) {
        self.acknowledgement.done.store(true, Ordering::Release);
    }

    fn has_started(&self) -> bool {
        self.acknowledgement.started.load(Ordering::Acquire)
    }

    fn mark_started(&self) {
        self.acknowledgement.started.store(true, Ordering::Release);
    }

    async fn serialize(&self) -> futures::lock::MutexGuard<'_, ()> {
        self.acknowledgement.serial.lock().await
    }
}

/// A completion obligation held while a claim is dispatched or acknowledged.
/// Dropping it armed retires the worker, unless the claim's acknowledgement
/// committed in the meantime: then nothing was lost.
struct ClaimObligation {
    guard: Option<crate::lease::LeaseGuard>,
    claim: Option<ClaimAttempt>,
}

impl ClaimObligation {
    fn disarm(&mut self) {
        if let Some(guard) = &mut self.guard {
            guard.disarm();
        }
    }
}

impl Drop for ClaimObligation {
    fn drop(&mut self) {
        if self
            .claim
            .as_ref()
            .is_some_and(ClaimAttempt::is_acknowledged)
        {
            self.disarm();
        }
    }
}

pub(crate) fn record_claim(parts: &mut Parts<PgContext, Ulid>) {
    parts.data.insert(ClaimAttempt {
        task_id: parts.task_id,
        lock_at: *parts.ctx.lock_at(),
        completed: parts.attempt.current(),
        acknowledgement: Arc::new(ClaimAcknowledgement {
            serial: futures::lock::Mutex::new(()),
            done: AtomicBool::new(false),
            started: AtomicBool::new(false),
        }),
    });
}

fn acknowledgement_attempt(parts: &Parts<PgContext, Ulid>) -> Result<usize, Error> {
    let Some(claim) = parts.data.get::<ClaimAttempt>() else {
        // Manually assembled acknowledgements retain the existing contract:
        // their caller supplies the count including the completed execution.
        return Ok(parts.attempt.current());
    };
    let task_id = parts.task_id.ok_or(Error::MissingField("task_id"))?;
    let worker_id = parts
        .ctx
        .lock_by()
        .as_deref()
        .ok_or(Error::MissingField("lock_by"))?;
    let queue = parts
        .ctx
        .queue()
        .as_deref()
        .ok_or(Error::MissingField("queue"))?;
    let lock_at = parts.ctx.lock_at().ok_or(Error::MissingField("lock_at"))?;
    if claim.task_id != Some(task_id) || claim.lock_at != Some(lock_at) {
        return Err(Error::stale_acknowledgement(
            task_id.to_string(),
            queue,
            worker_id,
        ));
    }
    claim.completed.checked_add(1).ok_or_else(|| {
        Error::InvalidArgument("task attempt counter cannot be incremented".to_owned())
    })
}

// Manual impl so the lease token never reaches log output: `PgAck` is
// embedded in the public `PgMiddleware` (whose derived `Debug` recurses down
// to here via `AcknowledgeLayer`), and a derived impl would print the
// storage registration secret verbatim — defeating the redaction `PostgresStorage`'s
// own `Debug` already performs.
impl std::fmt::Debug for PgAck {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgAck")
            .field("lease_token", &self.lease_token.as_ref().map(|_| "<set>"))
            .finish_non_exhaustive()
    }
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
    use futures::{executor::block_on, task::noop_waker_ref};
    use lets_expect::{AssertionError, AssertionResult, *};

    use super::*;
    use crate::unreachable::unreachable_pool;

    mod claim_attempt {
        use super::*;

        #[derive(Clone, Copy)]
        enum Counter {
            Tracked,
            Unchanged,
            Replaced,
        }

        #[derive(Clone, Copy)]
        enum Identity {
            Valid,
            MissingId,
            MissingOwner,
            MissingQueue,
            MissingTimestamp,
            DifferentId,
            DifferentTimestamp,
        }

        fn select(
            completed: usize,
            counter: Counter,
            identity: Identity,
        ) -> Result<(usize, usize), Error> {
            let mut parts = parts_for_ack(completed, 3);
            parts.task_id = Some(PgTaskId::new(Ulid::from(0u128)));
            parts.ctx = parts
                .ctx
                .with_queue("claim-queue".to_owned())
                .with_lock_by(Some("claim-worker".to_owned()))
                .with_lock_at(Some(1_700_000_000));
            record_claim(&mut parts);
            match counter {
                Counter::Tracked => {
                    let _ = parts.attempt.increment();
                }
                Counter::Unchanged => {}
                Counter::Replaced => parts.attempt = Attempt::new_with_value(99),
            }
            match identity {
                Identity::Valid => {}
                Identity::MissingId => parts.task_id = None,
                Identity::MissingOwner => parts.ctx = parts.ctx.with_lock_by(None),
                Identity::MissingQueue => {
                    parts.ctx = PgContext::new()
                        .with_lock_by(Some("claim-worker".to_owned()))
                        .with_lock_at(Some(1_700_000_000));
                }
                Identity::MissingTimestamp => parts.ctx = parts.ctx.with_lock_at(None),
                Identity::DifferentId => {
                    parts.task_id = Some(PgTaskId::new(Ulid::from(1u128)));
                }
                Identity::DifferentTimestamp => {
                    parts.ctx = parts.ctx.with_lock_at(Some(1_700_000_001));
                }
            }
            Ok((
                acknowledgement_attempt(&parts)?,
                acknowledgement_attempt(&parts.clone())?,
            ))
        }

        fn missing(field: &'static str) -> impl Fn(&Error) -> AssertionResult {
            move |error| match error {
                Error::MissingField(found) if *found == field => Ok(()),
                other => Err(AssertionError::new(vec![format!(
                    "expected missing {field}, got {other:?}"
                )])),
            }
        }

        fn stale(error: &Error) -> AssertionResult {
            match error {
                Error::StaleAcknowledgement { .. } => Ok(()),
                other => Err(AssertionError::new(vec![format!(
                    "expected stale claim identity, got {other:?}"
                )])),
            }
        }

        fn overflow(error: &Error) -> AssertionResult {
            match error {
                Error::InvalidArgument(message) if message.contains("attempt counter") => Ok(()),
                other => Err(AssertionError::new(vec![format!(
                    "expected attempt overflow, got {other:?}"
                )])),
            }
        }

        lets_expect! {
            expect(select(completed, counter, identity)) as claimed_acknowledgement_attempt {
                let completed = 0;
                let counter = Counter::Tracked;
                let identity = Identity::Valid;
                to counts_the_execution_and_preserves_the_snapshot { be_ok_and(equal((1, 1))) }
                when no_tracker_advances_the_counter {
                    let counter = Counter::Unchanged;
                    to counts_the_execution_and_preserves_the_snapshot { be_ok_and(equal((1, 1))) }
                }
                when the_counter_is_replaced {
                    let counter = Counter::Replaced;
                    to uses_the_claim_history { be_ok_and(equal((1, 1))) }
                }
                when an_earlier_execution_has_completed {
                    let completed = 1;
                    to counts_the_new_execution_once { be_ok_and(equal((2, 2))) }
                    when no_tracker_advances_the_counter {
                        let counter = Counter::Unchanged;
                        to counts_the_new_execution_once { be_ok_and(equal((2, 2))) }
                    }
                    when the_counter_is_replaced {
                        let counter = Counter::Replaced;
                        to uses_the_claim_history { be_ok_and(equal((2, 2))) }
                    }
                }
                when the_task_id_is_missing {
                    let identity = Identity::MissingId;
                    to rejects_the_incomplete_identity { be_err_and(missing("task_id")) }
                }
                when the_owner_is_missing {
                    let identity = Identity::MissingOwner;
                    to rejects_the_incomplete_identity { be_err_and(missing("lock_by")) }
                }
                when the_queue_is_missing {
                    let identity = Identity::MissingQueue;
                    to rejects_the_incomplete_identity { be_err_and(missing("queue")) }
                }
                when the_timestamp_is_missing {
                    let identity = Identity::MissingTimestamp;
                    to rejects_the_incomplete_identity { be_err_and(missing("lock_at")) }
                }
                when the_task_id_belongs_to_another_task {
                    let identity = Identity::DifferentId;
                    to rejects_the_mixed_claim { be_err_and(stale) }
                }
                when the_timestamp_belongs_to_another_claim {
                    let identity = Identity::DifferentTimestamp;
                    to rejects_the_mixed_claim { be_err_and(stale) }
                }
                when the_history_cannot_be_incremented {
                    let completed = usize::MAX;
                    let counter = Counter::Unchanged;
                    to reports_overflow { be_err_and(overflow) }
                }
            }
            expect(acknowledgement_attempt(&parts_for_ack(current, 3))) as manual_acknowledgement_attempt {
                let current = 1;
                to retains_the_callers_completed_count { be_ok_and(equal(1)) }
                when the_caller_keeps_the_default_count {
                    let current = 0;
                    to retains_the_legacy_count { be_ok_and(equal(0)) }
                }
            }
        }
    }

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

            let mut ack = PgAck::new(&unreachable_pool());
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

    // `€` is a 3-byte UTF-8 codepoint, so a string of `char_count` of them is
    // `3 * char_count` bytes. With the 8 KiB cap, `MAX_ERROR_PAYLOAD_LEN` (8192)
    // is not a multiple of 3 (`8192 % 3 == 2`), so byte index 8192 lands inside
    // a codepoint and forces the walk-back loop in `truncate_error_payload` to
    // step back to the nearest boundary (8190). ASCII fixtures can never reach
    // this branch because every byte is its own char boundary.
    fn truncated_three_byte_char_payload_length(char_count: usize) -> usize {
        truncate_error_payload("€".repeat(char_count)).len()
    }

    fn poll_lock_ready(state: ReadyState) -> Poll<Result<(), BoxDynError>> {
        let mut service = LockTaskService {
            inner: ReadyService { state },
            pool: unreachable_pool(),
            leases: None,
            lease_token: None,
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        service.poll_ready(&mut cx)
    }

    fn layered_service_debug() -> String {
        let layer = LockTaskLayer::new(unreachable_pool());
        let service = layer.layer(ReadyService {
            state: ReadyState::Ready,
        });
        format!("{service:?}")
    }

    fn middleware_auto_ack_enabled(auto_ack: bool) -> bool {
        PgMiddleware::new(&unreachable_pool(), auto_ack).auto_ack()
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
            pool: unreachable_pool(),
            leases: None,
            lease_token: None,
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

    /// Sentinel secret for the Debug-redaction specs below: must never appear
    /// in any Debug output reachable from the public API.
    const SECRET_LEASE_TOKEN: &str = "01SECRET-LEASE-TOKEN-MUST-NOT-LEAK";

    fn pg_ack_debug(with_token: bool) -> String {
        let ack = if with_token {
            PgAck::with_lease_token(&unreachable_pool(), Arc::from(SECRET_LEASE_TOKEN))
        } else {
            PgAck::new(&unreachable_pool())
        };
        format!("{ack:?}")
    }

    /// Debug output from the public middleware must redact the lease token,
    /// including the nested `PgAck` inside `AcknowledgeLayer`.
    fn middleware_debug() -> String {
        format!(
            "{:?}",
            PgMiddleware::with_lease_token(
                &unreachable_pool(),
                true,
                Arc::from(SECRET_LEASE_TOKEN)
            )
        )
    }

    fn never_contains_the_secret(output: &String) -> AssertionResult {
        if output.contains(SECRET_LEASE_TOKEN) {
            Err(AssertionError::new(vec![format!(
                "expected the lease token to be redacted, but Debug output leaked it: {output}"
            )]))
        } else {
            Ok(())
        }
    }

    fn mentions(needle: &'static str) -> impl Fn(&String) -> AssertionResult {
        move |output| {
            if output.contains(needle) {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected Debug output to mention {needle:?}, got {output}"
                )]))
            }
        }
    }

    /// Serialize a successful `Ok(())` job through the exact path `PgAck::ack`
    /// uses to build the persisted `last_result`. `()` renders as JSON `null`,
    /// so the externally-tagged result is `{"Ok": null}` — the case the
    /// `build_ack_response` doc-comment warns must never collapse to SQL NULL.
    fn unit_ok_ack_response() -> Result<Option<serde_json::Value>, serde_json::Error> {
        let result: Result<(), BoxDynError> = Ok(());
        build_ack_response(&result)
    }

    /// The persisted value for `Ok(())` must be `Some({"Ok": null})`. A
    /// regression collapsing the trivial-Ok case to `None` (SQL NULL) would
    /// make the completed job look unread to `WaitForCompletion`; a regression
    /// dropping the `{"Ok": ...}` wrapper would break `from_value` round-trips.
    fn is_persisted_ok_null(
        result: &Result<Option<serde_json::Value>, serde_json::Error>,
    ) -> AssertionResult {
        match result {
            Ok(Some(value)) if *value == serde_json::json!({ "Ok": null }) => Ok(()),
            Ok(Some(value)) => Err(AssertionError::new(vec![format!(
                "expected Some({{\"Ok\": null}}), got Some({value})"
            )])),
            Ok(None) => Err(AssertionError::new(vec![
                "expected Some({\"Ok\": null}) but response collapsed to None (SQL NULL)"
                    .to_owned(),
            ])),
            Err(error) => Err(AssertionError::new(vec![format!(
                "expected Some({{\"Ok\": null}}), got serialization error {error}"
            )])),
        }
    }

    #[cfg(feature = "tokio")]
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

    fn abort_missing_field(expected: &'static str) -> impl Fn(&BoxDynError) -> AssertionResult {
        move |error| {
            let cause = error
                .downcast_ref::<apalis_core::error::AbortError>()
                .and_then(std::error::Error::source)
                .and_then(|source| source.downcast_ref::<crate::Error>());
            if matches!(cause, Some(crate::Error::MissingField(found)) if *found == expected)
                && error.to_string().contains(expected)
            {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected AbortError with MissingField({expected:?}), got {error:?}"
                )]))
            }
        }
    }

    lets_expect! {
        expect(is_preclaimed(lock_by, worker_id, has_lock_at)) as existing_claim {
            let lock_by: Option<&str> = Some("worker-1");
            let worker_id: Option<&str> = Some("worker-1");
            let has_lock_at = true;

            to recognizes_the_preclaimed_task_and_skips_the_sql_lock { be_true }

            when the_lock_timestamp_is_absent {
                let has_lock_at = false;
                to is_not_preclaimed_so_the_sql_lock_still_runs { be_false }
            }

            when the_stored_owner_differs_from_the_current_worker {
                let lock_by = Some("other-worker");
                to is_not_preclaimed_when_another_worker_holds_the_lock { be_false }
            }

            when the_context_carries_no_lock_owner {
                let lock_by: Option<&str> = None;
                to is_not_preclaimed_without_a_stored_owner { be_false }
                when the_worker_context_is_also_absent {
                    let worker_id: Option<&str> = None;
                    to requires_a_new_claim { be_false }
                }
            }

            when there_is_no_current_worker_context {
                let worker_id: Option<&str> = None;
                to is_not_preclaimed_without_a_current_worker { be_false }
            }
        }
    }

    lets_expect! {
        expect(pg_ack_debug(with_token)) as pg_ack_debug {
            let with_token = true;

            when the_ack_carries_a_lease_token {
                to never_prints_the_token_value { never_contains_the_secret }
                to marks_the_token_as_set { mentions("<set>") }
            }

            when the_ack_has_no_lease_token {
                let with_token = false;
                to shows_the_token_as_absent { mentions("None") }
            }
        }

        expect(middleware_debug()) as middleware_debug {
            when the_public_middleware_with_a_lease_token_is_formatted {
                to never_prints_the_token_value { never_contains_the_secret }
            }
        }
    }

    lets_expect! {
        expect(calculate_status(max_attempts, attempts, &result)) as acknowledged_task_state {
            let result: Result<(), BoxDynError> = Ok(());
            let attempts = 1;
            let max_attempts = 3;
            to completes_a_successful_task { equal(Status::Done) }
            when the_handler_fails {
                let result: Result<(), BoxDynError> = Err(box_error("execution failed"));
                to permits_another_attempt { equal(Status::Failed) }
                when the_attempt_budget_is_exactly_exhausted {
                    let attempts = 3;
                    to kills_the_task { equal(Status::Killed) }
                }
                when the_attempt_budget_has_been_exceeded {
                    let attempts = 4;
                    to kills_the_task { equal(Status::Killed) }
                }
                when the_attempt_budget_is_zero {
                    let max_attempts = 0;
                    let attempts = 0;
                    to kills_the_task { equal(Status::Killed) }
                }
                when the_attempt_budget_is_negative {
                    let max_attempts = -1;
                    let attempts = 0;
                    to kills_the_corrupt_task { equal(Status::Killed) }
                }
            }
        }

        expect(poll_lock_ready(state)) as poll_lock_ready {
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

        expect(layered_service_debug()) as layered_service_debug {
            to wraps_the_inner_service_with_the_pool { debug_mentions_lock_service }
        }

        expect(middleware_auto_ack_enabled(auto_ack)) as middleware_auto_ack_enabled {
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
        expect(ack_missing_field(has_task_id, has_lock_by, has_queue, has_lock_at)) as ack_missing_field {
            let has_task_id = true;
            let has_lock_by = true;
            let has_queue = true;
            let has_lock_at = true;

            when task_id_is_missing {
                let has_task_id = false;
                to rejects_before_querying_the_database { be_err_and missing_field("task_id") }

                when the_other_required_metadata_is_also_absent {
                    let has_lock_by = false;
                    let has_queue = false;
                    let has_lock_at = false;
                    to reports_the_missing_identifier_before_later_metadata {
                        be_err_and missing_field("task_id")
                    }
                }
            }

            when lock_owner_is_missing {
                let has_lock_by = false;
                to rejects_before_querying_the_database { be_err_and missing_field("lock_by") }

                when the_queue_and_lock_timestamp_are_also_absent {
                    let has_queue = false;
                    let has_lock_at = false;
                    to reports_the_missing_owner_before_queue_and_timestamp {
                        be_err_and missing_field("lock_by")
                    }
                }
            }

            when queue_is_missing {
                let has_queue = false;
                to rejects_before_querying_the_database { be_err_and missing_field("queue") }

                when the_lock_timestamp_is_also_absent {
                    let has_lock_at = false;
                    to reports_the_missing_queue_before_the_timestamp {
                        be_err_and missing_field("queue")
                    }
                }
            }

            when lock_timestamp_is_missing {
                let has_lock_at = false;
                to rejects_before_querying_the_database { be_err_and missing_field("lock_at") }
            }
        }

        expect(lock_service_call_missing_field(has_worker, has_task_id)) as lock_service_call_missing_field {
            let has_worker = true;
            let has_task_id = true;

            when worker_context_is_missing {
                let has_worker = false;
                to aborts_before_locking_the_task { be_err_and abort_missing_field("worker_context") }

                when the_task_identifier_is_also_absent {
                    let has_task_id = false;
                    to reports_the_missing_worker_before_the_identifier {
                        be_err_and abort_missing_field("worker_context")
                    }
                }
            }

            when task_id_is_missing {
                let has_task_id = false;
                to aborts_before_locking_the_task { be_err_and abort_missing_field("task_id") }
            }
        }

        expect(truncated_payload_length(input_len)) as truncated_payload_length {
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

        expect(truncated_payload_marker_present(input_len)) as truncated_payload_marker_present {
            let input_len = 100;

            when payload_is_within_budget {
                to does_not_append_a_truncation_marker { equal(false) }
            }

            when payload_overflows_the_budget {
                let input_len = 8 * 1024 + 1;
                to appends_the_truncation_marker { equal(true) }
            }
        }

        expect(unit_ok_ack_response()) as unit_ok_ack_response {
            when the_job_returns_a_trivial_unit_ok {
                to persists_the_ok_null_object_rather_than_sql_null { is_persisted_ok_null }
            }
        }

        expect(truncated_three_byte_char_payload_length(char_count)) as truncated_three_byte_char_payload_length {
            // 12_000 bytes of `€`, well over the 8 KiB cap; the cut at byte 8192
            // splits a codepoint (8192 % 3 == 2) so the walk-back fires.
            let char_count = 4000;

            when the_truncation_cut_falls_in_the_middle_of_a_multibyte_codepoint {
                to walks_back_to_the_nearest_char_boundary_before_appending_the_marker {
                    // Cut walks 8192 -> 8190 (the boundary), then the marker is
                    // appended. A naive `truncate(8192)` would instead panic.
                    equal(8190 + "…[truncated]".len())
                }
            }
        }
    }

    #[cfg(feature = "tokio")]
    mod tokio_tests {
        use super::*;
        use serde::{Serialize, Serializer, ser};
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Drive `PgAck::ack` with an oversized attempt counter. The bounds
        /// check on `i32::try_from(attempts_raw)` (src/ack.rs:532) returns
        /// `Error::InvalidArgument`; without this branch a saturated cast
        /// would silently mismatch the row's `attempts` column and surface
        /// as a spurious `StaleAcknowledgement`.
        async fn ack_with_attempt_overflow(claimed: bool) -> Result<(), crate::Error> {
            let mut parts = parts_for_ack(1, 3);
            // Force an overflow regardless of host pointer width.
            parts.attempt = Attempt::new_with_value(i32::MAX as usize + 1);
            parts.ctx = parts
                .ctx
                .clone()
                .with_queue("ack-queue".to_owned())
                .with_lock_by(Some("ack-worker".to_owned()))
                .with_lock_at(Some(1_700_000_000));
            if claimed {
                parts.attempt = Attempt::new_with_value(i32::MAX as usize);
                record_claim(&mut parts);
                // A handler counter must not hide the unrepresentable next
                // attempt selected from the persisted claim history.
                parts.attempt = Attempt::new();
            }
            let mut ack = PgAck::new(&unreachable_pool());
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
            let mut ack = PgAck::new(&unreachable_pool());
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
                pool: unreachable_pool(),
                leases: None,
                lease_token: None,
            };
            service.call(task).await
        }

        /// A preclaimed task is `Queued` in the database: the service must
        /// start it there before the handler runs, so with no database the
        /// dispatch aborts on the start and the handler never runs.
        fn lock_service_call_preclaimed_starts_in_the_database(
            result: &Result<(), BoxDynError>,
        ) -> AssertionResult {
            abort_contains("failed to acquire PostgreSQL connection")(
                result
                    .as_ref()
                    .err()
                    .ok_or_else(|| {
                        AssertionError::new(vec![
                            "expected the start of the claim to reach the database, but the handler ran without it".to_owned(),
                        ])
                    })?,
            )
        }

        #[derive(Clone)]
        struct RecordingHandler(Arc<AtomicUsize>);

        impl Service<PgTask<()>> for RecordingHandler {
            type Response = ();
            type Error = std::io::Error;
            type Future = Ready<Result<(), Self::Error>>;

            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _: PgTask<()>) -> Self::Future {
                self.0.fetch_add(1, Ordering::SeqCst);
                ready(Ok(()))
            }
        }

        #[derive(Debug)]
        struct ValidationObservation {
            result: Result<(), BoxDynError>,
            handler_calls: usize,
            /// The task's attempt counter after the dispatch.
            attempt: usize,
            owner_retired: bool,
            cloned_owner_retired: bool,
            unrelated_retired: bool,
        }

        async fn managed_task_validation(
            acquired: bool,
            has_id: bool,
            has_queue: bool,
        ) -> ValidationObservation {
            let registry = crate::lease::LeaseRegistry::default();
            let owner = registry.for_worker("validation-owner");
            let unrelated = registry.for_worker("validation-unrelated");
            let handler_calls = Arc::new(AtomicUsize::new(0));
            let mut context = PgContext::new();
            if has_queue {
                context = context.with_queue("validation-queue".to_owned());
            }
            if acquired {
                context = context
                    .with_lock_by(Some("validation-owner".to_owned()))
                    .with_lock_at(Some(1_700_000_000));
            }
            let mut task = TaskBuilder::new(())
                .with_task_id(task_id())
                .with_ctx(context)
                .build();
            if !has_id {
                // Model identity lost after claim as well as malformed input
                // before claim; only the former has a completion obligation.
                task.parts.task_id = None;
            }
            task.parts
                .data
                .insert(WorkerContext::new::<()>("validation-owner"));
            let mut service = LockTaskService {
                inner: RecordingHandler(handler_calls.clone()),
                pool: unreachable_pool(),
                leases: Some(registry.clone()),
                lease_token: Some(ClaimToken(Arc::from("validation-token"))),
            };
            futures::future::poll_fn(|cx| service.poll_ready(cx))
                .await
                .expect("recording handler is ready");
            let attempt = task.parts.attempt.clone();
            let result = service.call(task).await;
            ValidationObservation {
                result,
                handler_calls: handler_calls.load(Ordering::SeqCst),
                attempt: attempt.current(),
                owner_retired: owner.is_retired(),
                cloned_owner_retired: registry.clone().for_worker("validation-owner").is_retired(),
                unrelated_retired: unrelated.is_retired(),
            }
        }

        fn abort_pool_error(error: &BoxDynError) -> AssertionResult {
            let cause = error
                .downcast_ref::<AbortError>()
                .and_then(std::error::Error::source)
                .and_then(|source| source.downcast_ref::<crate::Error>());
            if matches!(cause, Some(crate::Error::Pool(_))) {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected AbortError with a pool acquisition failure, got {error:?}"
                )]))
            }
        }

        #[derive(Debug)]
        struct RedispatchObservation {
            result: Option<Result<(), BoxDynError>>,
            handler_calls: usize,
            attempt: usize,
            owner_retired: bool,
        }

        #[derive(Clone, Copy)]
        enum Marker {
            Unset,
            SetBeforeCall,
            SetAfterCall,
        }

        #[derive(Clone, Copy)]
        enum Dispatch {
            Polled,
            DroppedUnpolled,
        }

        /// A retry layer outside the backend middleware re-dispatches a clone
        /// of the task; the clone shares the claim's acknowledgement marker,
        /// which may be set before the dispatch is created, between its
        /// creation and its first poll, or never.
        async fn preclaimed_redispatch(
            marker: Marker,
            dispatch: Dispatch,
        ) -> RedispatchObservation {
            let registry = crate::lease::LeaseRegistry::default();
            let owner = registry.for_worker("redispatch-owner");
            let handler_calls = Arc::new(AtomicUsize::new(0));
            let mut task = TaskBuilder::new(())
                .with_task_id(task_id())
                .with_ctx(
                    PgContext::new()
                        .with_queue("redispatch-queue".to_owned())
                        .with_lock_by(Some("redispatch-owner".to_owned()))
                        .with_lock_at(Some(1_700_000_000)),
                )
                .build();
            task.parts
                .data
                .insert(WorkerContext::new::<()>("redispatch-owner"));
            record_claim(&mut task.parts);
            let claim = task
                .parts
                .data
                .get::<ClaimAttempt>()
                .expect("claim recorded")
                .clone();
            if matches!(marker, Marker::SetBeforeCall) {
                claim.mark_acknowledged();
            }
            let redispatched = task.clone();
            let mut service = LockTaskService {
                inner: RecordingHandler(handler_calls.clone()),
                pool: unreachable_pool(),
                leases: Some(registry.clone()),
                lease_token: Some(ClaimToken(Arc::from("redispatch-token"))),
            };
            futures::future::poll_fn(|cx| service.poll_ready(cx))
                .await
                .expect("recording handler is ready");
            let future = service.call(redispatched);
            if matches!(marker, Marker::SetAfterCall) {
                claim.mark_acknowledged();
            }
            let result = match dispatch {
                Dispatch::Polled => Some(future.await),
                Dispatch::DroppedUnpolled => {
                    drop(future);
                    None
                }
            };
            RedispatchObservation {
                result,
                handler_calls: handler_calls.load(Ordering::SeqCst),
                attempt: task.parts.attempt.current(),
                owner_retired: owner.is_retired(),
            }
        }

        fn aborted_on_the_pool(result: &Option<Result<(), BoxDynError>>) -> AssertionResult {
            match result {
                Some(Err(error)) => abort_pool_error(error),
                other => Err(AssertionError::new(vec![format!(
                    "expected the dispatch to abort on the pool, got {other:?}"
                )])),
            }
        }

        fn never_polled(result: &Option<Result<(), BoxDynError>>) -> AssertionResult {
            match result {
                None => Ok(()),
                other => Err(AssertionError::new(vec![format!(
                    "expected no dispatch result, got {other:?}"
                )])),
            }
        }

        fn refused_as_already_acknowledged(
            result: &Option<Result<(), BoxDynError>>,
        ) -> AssertionResult {
            let cause = result
                .as_ref()
                .and_then(|result| result.as_ref().err())
                .and_then(|error| error.downcast_ref::<AbortError>())
                .and_then(std::error::Error::source)
                .and_then(|source| source.downcast_ref::<crate::Error>());
            if matches!(cause, Some(crate::Error::AlreadyAcknowledged { .. })) {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected AbortError with AlreadyAcknowledged, got {result:?}"
                )]))
            }
        }

        #[derive(Debug)]
        struct RepeatedAck {
            result: Result<(), crate::Error>,
            owner_retired: bool,
        }

        async fn manual_ack_of_claim(acknowledged: bool) -> RepeatedAck {
            let registry = crate::lease::LeaseRegistry::default();
            let owner = registry.for_worker("ack-worker");
            let mut parts = parts_for_ack(0, 3);
            parts.ctx = parts
                .ctx
                .with_queue("ack-queue".to_owned())
                .with_lock_by(Some("ack-worker".to_owned()))
                .with_lock_at(Some(1_700_000_000));
            record_claim(&mut parts);
            if acknowledged {
                parts
                    .data
                    .get::<ClaimAttempt>()
                    .expect("claim recorded")
                    .mark_acknowledged();
            }
            let mut ack = PgAck::new(&unreachable_pool());
            ack.leases = Some(registry);
            let result: Result<(), BoxDynError> = Ok(());
            RepeatedAck {
                result: ack.ack(&result, &parts.clone()).await,
                owner_retired: owner.is_retired(),
            }
        }

        #[derive(Clone, Copy)]
        enum Acknowledger {
            Storage,
            TokenFree,
            TokenOnly,
        }

        /// A manual acknowledgement fails against an unreachable pool. Only
        /// the storage-bound acknowledger retires the worker's registration.
        async fn failed_manual_ack(acknowledger: Acknowledger) -> bool {
            let storage = crate::PostgresStorage::<()>::new_with_config(
                &unreachable_pool(),
                &crate::Config::new("manual-ack-queue"),
            );
            let mut parts = parts_for_ack(0, 3);
            parts.ctx = parts
                .ctx
                .with_queue("manual-ack-queue".to_owned())
                .with_lock_by(Some("manual-ack-worker".to_owned()))
                .with_lock_at(Some(1_700_000_000));
            record_claim(&mut parts);
            let mut ack = match acknowledger {
                Acknowledger::Storage => storage.acknowledger(),
                Acknowledger::TokenFree => PgAck::new(storage.pool()),
                Acknowledger::TokenOnly => {
                    PgAck::with_lease_token(storage.pool(), Arc::from("held-token"))
                }
            };
            let result: Result<(), BoxDynError> = Ok(());
            assert!(ack.ack(&result, &parts).await.is_err());
            storage.leases.for_worker("manual-ack-worker").is_retired()
        }

        fn already_acknowledged(error: &crate::Error) -> AssertionResult {
            match error {
                crate::Error::AlreadyAcknowledged { .. } => Ok(()),
                other => Err(AssertionError::new(vec![format!(
                    "expected Error::AlreadyAcknowledged, got {other:?}"
                )])),
            }
        }

        fn pool_error(error: &crate::Error) -> AssertionResult {
            match error {
                crate::Error::Pool(_) => Ok(()),
                other => Err(AssertionError::new(vec![format!(
                    "expected Error::Pool, got {other:?}"
                )])),
            }
        }

        /// A `Service` that records whether a given instance was made ready by
        /// `poll_ready` before `call` was invoked on it. `Clone` deliberately
        /// resets the flag, so a fresh clone starts *unreserved*. This lets a spec
        /// assert the Tower reservation contract in `LockTaskService::call`: the
        /// instance handed to `call` must be the exact one `poll_ready` reserved,
        /// not the clone left behind by `std::mem::replace`.
        ///
        /// The `Clone` impl is hand-written on purpose: a `#[derive(Clone)]` would
        /// copy `reserved` verbatim, so a clone of an already-reserved instance
        /// would *also* report `reserved = true`. That would make the spec
        /// tautological — a regression that ran `call` on the leftover clone (e.g.
        /// `self.inner.clone().call(req)`, or a swapped `std::mem::replace`) would
        /// still see `reserved = true` and pass. Resetting to `false` here is what
        /// makes "only the poll_ready-reserved instance succeeds" an observable,
        /// falsifiable property.
        #[derive(Debug, Default)]
        struct ReservationService {
            reserved: bool,
        }

        impl Clone for ReservationService {
            fn clone(&self) -> Self {
                // A fresh clone is a not-yet-ready instance: the reservation made
                // by `poll_ready` on the source does NOT carry over. This mirrors
                // the Tower contract that each instance must be `poll_ready`-ed
                // before it may be `call`-ed.
                Self { reserved: false }
            }
        }

        impl Service<PgTask<()>> for ReservationService {
            type Response = ();
            type Error = std::io::Error;
            type Future = Ready<Result<(), Self::Error>>;

            fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                self.reserved = true;
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, _req: PgTask<()>) -> Self::Future {
                if self.reserved {
                    ready(Ok(()))
                } else {
                    ready(Err(std::io::Error::other(
                        "call reached an instance that was never reserved by poll_ready",
                    )))
                }
            }
        }

        /// What the database row looks like when the dispatch starts it.
        #[derive(Clone, Copy, Debug)]
        enum RowAtDispatch {
            /// `Queued` under the dispatched claim's epoch.
            Claimed,
            /// Handed back to `Pending` by a sweep, a takeover or a release.
            Recovered,
            /// Already `Running` under the same epoch.
            Started,
            /// Claimed again one second later: a different epoch.
            ReclaimedLater,
        }

        /// What this process knows about the dispatched claim.
        #[derive(Clone, Copy, Debug)]
        enum ClaimHistory {
            /// Recorded by the claim and never started.
            Fresh,
            /// A re-dispatch of a claim that this process already started.
            AlreadyStarted,
            /// A task assembled by hand, without the claim record.
            Unrecorded,
        }

        /// Who holds the registration when the dispatch starts the claim.
        #[derive(Clone, Copy, Debug)]
        enum Holder {
            /// The dispatching storage's token.
            Current,
            /// Another token: the name was released and taken over.
            Replaced,
            /// Another token, and the middleware binds no token of its own.
            ReplacedUnbound,
            /// The dispatching storage's token, but the name is retired locally.
            RetiredLocally,
        }

        #[derive(Clone)]
        struct StatusRecordingHandler(Arc<std::sync::Mutex<Vec<Status>>>);

        impl Service<PgTask<()>> for StatusRecordingHandler {
            type Response = ();
            type Error = std::io::Error;
            type Future = Ready<Result<(), Self::Error>>;

            fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }

            fn call(&mut self, req: PgTask<()>) -> Self::Future {
                self.0
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(req.parts.status.load());
                ready(Ok(()))
            }
        }

        #[derive(Debug)]
        struct StartObservation {
            outcome: String,
            handled: Vec<Status>,
            /// The task's attempt counter after the dispatch; the claim was
            /// fetched with a count of one.
            attempt: usize,
            row_status: String,
            owner_retired: bool,
        }

        const START_TOKEN: &str = "start-token";

        /// A registration of `worker` in `queue` and one `Queued` claim of a
        /// task by it; returns the task id and the claim's `lock_at` seconds.
        async fn seed_claim(
            pool: PgPool,
            queue: String,
            worker: String,
            registered_token: &'static str,
            row: RowAtDispatch,
        ) -> Result<(Ulid, i64), String> {
            use diesel::{
                QueryableByName, RunQueryDsl, sql_query,
                sql_types::{BigInt, Text},
            };
            #[derive(QueryableByName)]
            struct Epoch {
                #[diesel(sql_type = BigInt)]
                lock_at: i64,
            }
            let id = Ulid::new();
            crate::test_support::with_conn(pool, move |conn| {
                sql_query(
                    "INSERT INTO apalis.workers (id, worker_type, storage_name, layers, last_seen, started_at, lease_token)
                     VALUES ($1, $2, 'PostgresStorage', '', clock_timestamp(), clock_timestamp(), $3)",
                )
                .bind::<Text, _>(&worker)
                .bind::<Text, _>(&queue)
                .bind::<Text, _>(registered_token)
                .execute(conn)
                .map_err(|e| e.to_string())?;
                let epoch = sql_query(
                    "INSERT INTO apalis.jobs (id, job_type, job, status, attempts, max_attempts, run_at, lock_by, lock_at)
                     VALUES ($1, $2, '\\x00'::bytea, 'Queued', 1, 3, clock_timestamp(), $3, date_trunc('second', clock_timestamp()))
                     RETURNING extract(epoch FROM lock_at)::bigint AS lock_at",
                )
                .bind::<Text, _>(id.to_string())
                .bind::<Text, _>(&queue)
                .bind::<Text, _>(&worker)
                .get_result::<Epoch>(conn)
                .map_err(|e| e.to_string())?;
                let change = match row {
                    RowAtDispatch::Claimed => None,
                    RowAtDispatch::Recovered => Some(
                        "UPDATE apalis.jobs SET status = 'Pending', lock_by = NULL, lock_at = NULL WHERE id = $1",
                    ),
                    RowAtDispatch::Started => {
                        Some("UPDATE apalis.jobs SET status = 'Running' WHERE id = $1")
                    }
                    RowAtDispatch::ReclaimedLater => Some(
                        "UPDATE apalis.jobs SET lock_at = lock_at + INTERVAL '1 second' WHERE id = $1",
                    ),
                };
                if let Some(change) = change {
                    sql_query(change)
                        .bind::<Text, _>(id.to_string())
                        .execute(conn)
                        .map_err(|e| e.to_string())?;
                }
                Ok((id, epoch.lock_at))
            })
            .await
        }

        async fn row_status(pool: PgPool, id: Ulid) -> Result<String, String> {
            use diesel::{QueryableByName, RunQueryDsl, sql_query, sql_types::Text};
            #[derive(QueryableByName)]
            struct Row {
                #[diesel(sql_type = Text)]
                status: String,
            }
            crate::test_support::with_conn(pool, move |conn| {
                sql_query("SELECT status FROM apalis.jobs WHERE id = $1")
                    .bind::<Text, _>(id.to_string())
                    .get_result::<Row>(conn)
                    .map(|row| row.status)
                    .map_err(|e| e.to_string())
            })
            .await
        }

        async fn remove_queue(pool: PgPool, queue: String) -> Result<(), String> {
            use diesel::{RunQueryDsl, sql_query, sql_types::Text};
            crate::test_support::with_conn(pool, move |conn| {
                sql_query("DELETE FROM apalis.jobs WHERE job_type = $1")
                    .bind::<Text, _>(&queue)
                    .execute(conn)
                    .map_err(|e| e.to_string())?;
                sql_query("DELETE FROM apalis.workers WHERE worker_type = $1")
                    .bind::<Text, _>(&queue)
                    .execute(conn)
                    .map(drop)
                    .map_err(|e| e.to_string())
            })
            .await
        }

        /// The task a fetcher hands to the middleware for the seeded claim.
        fn claimed_task(
            id: Ulid,
            queue: &str,
            worker: &str,
            lock_at: i64,
            history: ClaimHistory,
        ) -> PgTask<()> {
            let mut task = TaskBuilder::new(())
                .with_task_id(TaskId::new(id))
                .with_attempt(Attempt::new_with_value(1))
                .with_ctx(
                    PgContext::new()
                        .with_queue(queue.to_owned())
                        .with_lock_by(Some(worker.to_owned()))
                        .with_lock_at(Some(lock_at)),
                )
                .build();
            task.parts.data.insert(WorkerContext::new::<()>(worker));
            if !matches!(history, ClaimHistory::Unrecorded) {
                record_claim(&mut task.parts);
            }
            if matches!(history, ClaimHistory::AlreadyStarted) {
                task.parts
                    .data
                    .get::<ClaimAttempt>()
                    .expect("claim recorded")
                    .mark_started();
            }
            task
        }

        async fn start_a_claim(
            row: RowAtDispatch,
            history: ClaimHistory,
            holder: Holder,
        ) -> Result<crate::test_support::Outcome<StartObservation>, String> {
            let Some(pool) = crate::test_support::shared_pool().await? else {
                return Ok(crate::test_support::Outcome::Skipped);
            };
            let queue = format!("start-claim-{}", Ulid::new());
            let worker = format!("start-worker-{}", Ulid::new());
            let registered_token = match holder {
                Holder::Current | Holder::RetiredLocally => START_TOKEN,
                Holder::Replaced | Holder::ReplacedUnbound => "successor-token",
            };
            let observation = async {
                let (id, lock_at) = seed_claim(
                    pool.clone(),
                    queue.clone(),
                    worker.clone(),
                    registered_token,
                    row,
                )
                .await?;
                let registry = crate::lease::LeaseRegistry::default();
                let owner = registry.for_worker(&worker);
                if matches!(holder, Holder::RetiredLocally) {
                    owner.retire();
                }
                let handled = Arc::new(std::sync::Mutex::new(Vec::new()));
                let mut service = LockTaskService {
                    inner: StatusRecordingHandler(handled.clone()),
                    pool: pool.clone(),
                    leases: Some(registry.clone()),
                    lease_token: match holder {
                        Holder::ReplacedUnbound => None,
                        _ => Some(ClaimToken(Arc::from(START_TOKEN))),
                    },
                };
                futures::future::poll_fn(|cx| service.poll_ready(cx))
                    .await
                    .map_err(|e| e.to_string())?;
                let task = claimed_task(id, &queue, &worker, lock_at, history);
                let attempt = task.parts.attempt.clone();
                let result = service.call(task).await;
                let outcome = match &result {
                    Ok(()) => "ran".to_owned(),
                    Err(error) => match error
                        .downcast_ref::<AbortError>()
                        .and_then(std::error::Error::source)
                        .and_then(|source| source.downcast_ref::<crate::Error>())
                    {
                        Some(crate::Error::ClaimLost { .. }) => "claim_lost".to_owned(),
                        Some(crate::Error::WorkerRetired { .. }) => "retired".to_owned(),
                        _ => format!("unexpected: {error:?}"),
                    },
                };
                let handled = handled
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                Ok::<_, String>(StartObservation {
                    outcome,
                    handled,
                    attempt: attempt.current(),
                    row_status: row_status(pool.clone(), id).await?,
                    owner_retired: owner.is_retired(),
                })
            }
            .await;
            remove_queue(pool, queue).await?;
            observation.map(crate::test_support::Outcome::Completed)
        }

        fn started(
            outcome: &'static str,
            handled: &'static [Status],
            attempt: usize,
            row_status: &'static str,
            owner_retired: bool,
        ) -> impl Fn(&Result<crate::test_support::Outcome<StartObservation>, String>) -> AssertionResult
        {
            crate::test_support::observe::<StartObservation, _>("start of a claim", move |run| {
                if run.outcome == outcome
                    && run.handled == handled
                    && run.attempt == attempt
                    && run.row_status == row_status
                    && run.owner_retired == owner_retired
                {
                    Ok(())
                } else {
                    Err(format!(
                        "expected {outcome} with the handler seeing {handled:?}, attempt counter {attempt}, a {row_status} row and owner_retired={owner_retired}; got {run:?}"
                    ))
                }
            })
        }

        /// The Tower cycle against a real claim: `poll_ready` reserves the
        /// inner instance, and only that instance may serve `call`.
        async fn reserved_inner_serves_the_claim()
        -> Result<crate::test_support::Outcome<Result<(), String>>, String> {
            let Some(pool) = crate::test_support::shared_pool().await? else {
                return Ok(crate::test_support::Outcome::Skipped);
            };
            let queue = format!("start-reserved-{}", Ulid::new());
            let worker = format!("start-reserved-worker-{}", Ulid::new());
            let result = async {
                let (id, lock_at) = seed_claim(
                    pool.clone(),
                    queue.clone(),
                    worker.clone(),
                    START_TOKEN,
                    RowAtDispatch::Claimed,
                )
                .await?;
                let mut service = LockTaskService {
                    inner: ReservationService::default(),
                    pool: pool.clone(),
                    leases: None,
                    lease_token: Some(ClaimToken(Arc::from(START_TOKEN))),
                };
                let mut cx = Context::from_waker(noop_waker_ref());
                assert!(matches!(service.poll_ready(&mut cx), Poll::Ready(Ok(()))));
                Ok::<_, String>(
                    service
                        .call(claimed_task(
                            id,
                            &queue,
                            &worker,
                            lock_at,
                            ClaimHistory::Fresh,
                        ))
                        .await
                        .map_err(|error| error.to_string()),
                )
            }
            .await;
            remove_queue(pool, queue).await?;
            result.map(crate::test_support::Outcome::Completed)
        }

        fn served_by_the_reserved_instance()
        -> impl Fn(&Result<crate::test_support::Outcome<Result<(), String>>, String>) -> AssertionResult
        {
            crate::test_support::observe::<Result<(), String>, _>("reserved inner", |run| {
                run.clone().map_err(|error| {
                    format!(
                        "expected call to consume the poll_ready-reserved inner instance, got {error}"
                    )
                })
            })
        }

        lets_expect! { #tokio_test
            expect(managed_task_validation(acquired, has_id, has_queue).await) as managed_task_validation {
                let acquired = true;
                let has_id = true;
                let has_queue = true;

                // The claim is owned but cannot be started: the obligation to
                // run it is lost, so only the owner is retired and recovery
                // hands the claim back after the stale deadline.
                to retires_only_the_owner_when_the_start_cannot_reach_the_database {
                    have(result) be_err_and abort_pool_error,
                    have(handler_calls) equal(0),
                        have(attempt) equal(1),
                    have(owner_retired) be_true,
                    have(cloned_owner_retired) be_true,
                    have(unrelated_retired) be_false
                }

                when the_acquired_task_carries_no_queue {
                    let has_queue = false;
                    to retires_only_the_owner_without_calling_the_handler {
                        have(result) be_err_and abort_missing_field("queue"),
                        have(handler_calls) equal(0),
                        have(attempt) equal(1),
                        have(owner_retired) be_true,
                        have(cloned_owner_retired) be_true,
                        have(unrelated_retired) be_false
                    }
                }

                when the_acquired_task_identifier_is_missing {
                    let has_id = false;
                    to retires_only_the_owner_without_calling_the_handler {
                        have(result) be_err_and abort_missing_field("task_id"),
                        have(handler_calls) equal(0),
                        have(attempt) equal(1),
                        have(owner_retired) be_true,
                        have(cloned_owner_retired) be_true,
                        have(unrelated_retired) be_false
                    }
                }

                when the_task_has_not_been_acquired {
                    let acquired = false;
                    to preserves_workers_after_a_known_acquisition_failure {
                        have(result) be_err_and abort_pool_error,
                        have(handler_calls) equal(0),
                        have(attempt) equal(1),
                        have(owner_retired) be_false,
                        have(cloned_owner_retired) be_false,
                        have(unrelated_retired) be_false
                    }
                    when the_task_identifier_is_missing {
                        let has_id = false;
                        to rejects_without_creating_a_completion_obligation {
                            have(result) be_err_and abort_missing_field("task_id"),
                            have(handler_calls) equal(0),
                        have(attempt) equal(1),
                            have(owner_retired) be_false,
                            have(cloned_owner_retired) be_false,
                            have(unrelated_retired) be_false
                        }
                    }
                }
            }

            expect(lock_service_call_async(true, true).await) as acquiring_a_task {
                when task_has_worker_and_id_but_the_database_is_unavailable {
                    to aborts_with_the_lock_error { be_err_and abort_contains("failed to acquire PostgreSQL connection") }
                }
            }

            expect(ack_with_attempt_overflow(claimed).await) as an_unrepresentable_attempt {
                let claimed = false;
                when the_attempt_counter_exceeds_i32_max {
                    to surfaces_invalid_argument_before_touching_the_database {
                        be_err_and invalid_attempt_overflow
                    }
                }
                when the_claim_has_no_representable_next_attempt {
                    let claimed = true;
                    to rejects_the_counter_before_accessing_the_pool { be_err_and(invalid_attempt_overflow) }
                }
            }

            expect(ack_with_unserializable_result().await) as an_unencodable_result {
                when the_jobs_ok_payload_fails_to_serialize {
                    to surfaces_an_error_json_before_touching_the_database {
                        be_err_and json_serialize_error
                    }
                }
            }

            expect(preclaimed_redispatch(marker, dispatch).await) as a_claim_dispatched_again_in_process {
                let marker = Marker::Unset;
                let dispatch = Dispatch::Polled;
                to reaches_the_database_to_start_the_claim_counts_the_dispatch_and_retires_the_worker_when_that_fails {
                    have(result) aborted_on_the_pool,
                    have(handler_calls) equal(0),
                    have(attempt) equal(1),
                    have(owner_retired) be_true
                }
                when the_dispatch_is_dropped_before_its_first_poll {
                    let dispatch = Dispatch::DroppedUnpolled;
                    to retires_the_worker_for_the_lost_obligation {
                        have(result) never_polled,
                        have(handler_calls) equal(0),
                        have(attempt) equal(0),
                        have(owner_retired) be_true
                    }
                }
                when the_claim_was_acknowledged_before_the_dispatch_was_created {
                    let marker = Marker::SetBeforeCall;
                    to refuses_before_the_handler_counts_the_dispatch_and_keeps_the_worker_active {
                        have(result) refused_as_already_acknowledged,
                        have(handler_calls) equal(0),
                        have(attempt) equal(1),
                        have(owner_retired) be_false
                    }
                    when the_dispatch_is_dropped_before_its_first_poll {
                        let dispatch = Dispatch::DroppedUnpolled;
                        to keeps_the_worker_active {
                            have(result) never_polled,
                            have(handler_calls) equal(0),
                            have(attempt) equal(0),
                            have(owner_retired) be_false
                        }
                    }
                }
                when the_claim_was_acknowledged_after_the_dispatch_was_created {
                    let marker = Marker::SetAfterCall;
                    to refuses_before_the_handler_counts_the_dispatch_and_keeps_the_worker_active {
                        have(result) refused_as_already_acknowledged,
                        have(handler_calls) equal(0),
                        have(attempt) equal(1),
                        have(owner_retired) be_false
                    }
                    when the_dispatch_is_dropped_before_its_first_poll {
                        let dispatch = Dispatch::DroppedUnpolled;
                        to keeps_the_worker_active {
                            have(result) never_polled,
                            have(handler_calls) equal(0),
                            have(attempt) equal(0),
                            have(owner_retired) be_false
                        }
                    }
                }
            }

            expect(failed_manual_ack(acknowledger).await) as a_failed_manual_acknowledgement {
                let acknowledger = Acknowledger::Storage;
                to retires_the_storages_registration_for_that_worker { be_true }
                when the_acknowledger_binds_no_token {
                    let acknowledger = Acknowledger::TokenFree;
                    to leaves_the_registration_active { be_false }
                }
                when the_acknowledger_binds_only_a_token {
                    let acknowledger = Acknowledger::TokenOnly;
                    to leaves_the_registration_active { be_false }
                }
            }

            expect(manual_ack_of_claim(acknowledged).await) as a_manual_acknowledgement_of_a_claim {
                let acknowledged = false;
                to reaches_the_database_and_retires_the_worker_when_that_fails {
                    have(result) be_err_and pool_error,
                    have(owner_retired) be_true
                }
                when the_claim_was_already_acknowledged {
                    let acknowledged = true;
                    to refuses_the_repeat_without_the_database_and_keeps_the_worker_active {
                        have(result) be_err_and already_acknowledged,
                        have(owner_retired) be_false
                    }
                }
            }

            expect(start_a_claim(row, history, holder).await) as starting_a_fetched_claim {
                let row = RowAtDispatch::Claimed;
                let history = ClaimHistory::Fresh;
                let holder = Holder::Current;
                to moves_the_row_to_running_before_the_handler_runs_once {
                    started("ran", &[Status::Running], 1, "Running", false)
                }
                when the_claim_was_recovered_before_the_dispatch {
                    let row = RowAtDispatch::Recovered;
                    to refuses_the_dispatch_and_keeps_the_worker_active {
                        started("claim_lost", &[], 2, "Pending", false)
                    }
                }
                when the_task_was_claimed_again_at_a_later_second {
                    let row = RowAtDispatch::ReclaimedLater;
                    to refuses_the_dispatch_and_leaves_the_newer_claim_alone {
                        started("claim_lost", &[], 2, "Queued", false)
                    }
                }
                when another_claim_of_the_same_epoch_already_started_the_row {
                    let row = RowAtDispatch::Started;
                    to refuses_the_second_start {
                        started("claim_lost", &[], 2, "Running", false)
                    }
                    when the_dispatch_repeats_a_claim_this_process_started {
                        let history = ClaimHistory::AlreadyStarted;
                        to runs_the_handler_again {
                            started("ran", &[Status::Running], 1, "Running", false)
                        }
                    }
                    when the_task_carries_no_claim_record {
                        let history = ClaimHistory::Unrecorded;
                        to starts_it_without_a_claim_identity_to_compare {
                            started("ran", &[Status::Running], 1, "Running", false)
                        }
                    }
                }
                when the_task_carries_no_claim_record {
                    let history = ClaimHistory::Unrecorded;
                    to moves_the_row_to_running_before_the_handler_runs_once {
                        started("ran", &[Status::Running], 1, "Running", false)
                    }
                }
                when another_token_owns_the_registration {
                    let holder = Holder::Replaced;
                    to refuses_the_dispatch_and_keeps_the_worker_active {
                        started("claim_lost", &[], 2, "Queued", false)
                    }
                    when the_middleware_binds_no_token {
                        let holder = Holder::ReplacedUnbound;
                        to starts_the_claim_without_a_registration_fence {
                            started("ran", &[Status::Running], 1, "Running", false)
                        }
                    }
                }
                when the_worker_is_retired_locally {
                    let holder = Holder::RetiredLocally;
                    to starts_nothing_and_leaves_the_claim_for_recovery {
                        started("retired", &[], 2, "Queued", true)
                    }
                }
            }

            expect(reserved_inner_serves_the_claim().await) as the_tower_reservation_of_a_started_claim {
                to calls_the_reserved_instance_not_the_clone_left_behind {
                    served_by_the_reserved_instance()
                }
            }

            expect(lock_service_call_preclaimed().await) as an_existing_claim {
                when the_task_already_carries_a_matching_lock_by_and_lock_at {
                    to starts_the_claim_in_the_database_before_the_handler {
                        lock_service_call_preclaimed_starts_in_the_database
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
    /// checks the storage registration token. This constructor exists for test harnesses
    /// and admin tooling that do not own a lease token.
    ///
    /// Like [`lock_task`], this acknowledger does not manage local worker
    /// retirement: a failed acknowledgement leaves the worker's heartbeat
    /// running. Acknowledge tasks of a storage-registered worker through
    /// [`PostgresStorage::acknowledger`] or the middleware returned by
    /// `Backend::middleware`, which carry that registration's liveness.
    ///
    /// [`PostgresStorage::acknowledger`]: crate::PostgresStorage::acknowledger
    #[must_use]
    pub fn new(pool: &PgPool) -> Self {
        Self {
            pool: pool.clone(),
            lease_token: None,
            leases: None,
        }
    }

    /// Create a PostgreSQL acknowledger bound to a specific worker lease token.
    ///
    /// The token is checked while holding a shared row lock on
    /// `apalis.workers.lease_token`, mirroring the heartbeat path. This is for
    /// callers that registered the worker outside this crate's storage and
    /// hold its token; a storage's own token is not exposed, and its
    /// acknowledger comes from [`PostgresStorage::acknowledger`]. This
    /// constructor binds the token only and does not manage local worker
    /// retirement.
    ///
    /// [`PostgresStorage::acknowledger`]: crate::PostgresStorage::acknowledger
    #[must_use]
    pub fn with_lease_token(pool: &PgPool, lease_token: Arc<str>) -> Self {
        Self {
            pool: pool.clone(),
            lease_token: Some(lease_token),
            leases: None,
        }
    }

    pub(crate) fn with_lease_registry(
        pool: &PgPool,
        lease_token: Arc<str>,
        leases: crate::lease::LeaseRegistry,
    ) -> Self {
        Self {
            pool: pool.clone(),
            lease_token: Some(lease_token),
            leases: Some(leases),
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

/// Serialize a job result into the value persisted in `apalis.jobs.last_result`.
///
/// The value is always the externally-tagged `Result<O, String>` JSON
/// (`{"Ok": ...}` or `{"Err": "..."}`), with the error `Display` capped by
/// [`truncate_error_payload`], and is always wrapped in `Some(...)`.
///
/// `WaitForCompletion::wait_for` reads `last_result` back with
/// `serde_json::from_value` and the spec (`queries/mod.rs
/// tests::last_result_is_missing`) requires a SQL NULL to surface as
/// `MissingField("last_result")`. A trivial `Ok(())` job serializes to the JSON
/// object `{"Ok": null}` (serde renders `()` as JSON `null`); wrapping in
/// `Some(...)` — rather than collapsing that inner `null` to a SQL NULL — keeps
/// a completed `Ok(())` job visible as read to consumers of `WaitForCompletion`
/// instead of looking unacknowledged.
pub(crate) fn build_ack_response<Res: Serialize>(
    res: &Result<Res, BoxDynError>,
) -> Result<Option<serde_json::Value>, serde_json::Error> {
    serde_json::to_value(
        res.as_ref()
            .map_err(|error| truncate_error_payload(error.to_string())),
    )
    .map(Some)
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
        let claim = parts.data.get::<ClaimAttempt>().cloned();
        let mut completion_guard = ClaimObligation {
            guard: self
                .leases
                .as_ref()
                .and_then(|leases| worker_id.as_deref().map(|id| leases.for_worker(id).guard())),
            claim: claim.clone(),
        };
        let response = build_ack_response(res);
        // SQL stores completed history; Tracker increments a shared Attempt
        // only when it is present. Use the claim snapshot for both the retry
        // decision and the write so direct middleware calls behave identically.
        let attempt_and_status = acknowledgement_attempt(parts).map(|attempts| {
            (
                attempts,
                calculate_status(parts.ctx.max_attempts(), attempts, res),
            )
        });
        // Silent saturation would corrupt the ack's lock-check predicate:
        // `ack_task` matches on `attempts = $started_attempts`, so a capped
        // value would silently mismatch the stored row and the ack would be
        // reported as `StaleAcknowledgement` for a non-stale task. Surface
        // overflow as `InvalidArgument` instead.
        let pool = self.pool.clone();
        let lease_token = self.lease_token.clone();

        async move {
            // Acknowledgements of one claim run one at a time, so a concurrent
            // repeat waits for the first and then observes its marker.
            let _serial = match &claim {
                Some(claim) => Some(claim.serialize().await),
                None => None,
            };
            if let Some(claim) = &claim
                && claim.is_acknowledged()
            {
                // The earlier acknowledgement committed, so no obligation is
                // lost; refuse the repeat without touching the row.
                completion_guard.disarm();
                return Err(Error::already_acknowledged(
                    task_id.ok_or(Error::MissingField("task_id"))?.to_string(),
                    queue.ok_or(Error::MissingField("queue"))?,
                    worker_id.ok_or(Error::MissingField("lock_by"))?,
                ));
            }
            let (attempts_raw, status) = attempt_and_status?;
            let attempts = i32::try_from(attempts_raw).map_err(|_| {
                Error::InvalidArgument(format!(
                    "task attempt counter {attempts_raw} exceeds i32::MAX and cannot be stored"
                ))
            })?;
            let started_attempts = attempts.saturating_sub(1);
            let result = queries::ack_task(
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
                    // Forward the `Arc<str>` clone straight through — no
                    // per-ack `String` allocation; bound to SQL by reference.
                    lease_token,
                },
            )
            .await;
            if result.is_ok() {
                if let Some(claim) = &claim {
                    claim.mark_acknowledged();
                }
                completion_guard.disarm();
            }
            result
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
    max_attempts: i32,
    attempts: usize,
    res: &Result<Res, BoxDynError>,
) -> Status {
    match res {
        Ok(_) => Status::Done,
        Err(_) => match usize::try_from(max_attempts) {
            Ok(max) if max > attempts => Status::Failed,
            _ => Status::Killed,
        },
    }
}

/// Lock a due task for a worker.
///
/// The worker must already be registered for the task queue. The task must be
/// due and in a lockable state: `Pending` or `Failed` with retry budget remaining,
/// or `Queued`/`Running` owned by the same worker. A manual retry must restore
/// retry budget before making an exhausted task `Pending`.
///
/// A registration created through the admin `RegisterWorker` trait has no
/// heartbeat: re-register within `reenqueue_orphaned_after` of the previous
/// call, or the locked task is recovered as an orphan.
///
/// # Cross-queue semantics
///
/// This entry point does **not** filter by `job_type`: a caller holding a
/// task's [`PgTaskId`] can lock it regardless of which queue it belongs to.
/// Prefer [`lock_task_in_queue`] which scopes the lock to a specific queue and
/// prevents a caller that learned a task id from logs or dashboards from
/// claiming it under an unrelated queue.
///
/// # Errors
/// - [`Error::TaskNotFound`] if the task is absent or not currently lockable
///   (delayed, exhausted, completed, or already locked by another worker). This entry
///   point does not filter by queue, so a task in another queue is still
///   locked rather than reported missing.
/// - [`Error::Database`] for SQL/driver failures, including the foreign-key
///   violation raised when `worker_id` is not registered for the queue.
/// - [`Error::Pool`] if a pooled connection cannot be acquired.
/// - [`Error::Blocking`] if the blocking task carrying the query fails to
///   complete (a panic in the worker thread, or runtime shutdown).
/// - [`Error::ClaimOutcomeUnknown`] if the transaction produced a claim but
///   its commit could not be confirmed. The task may already be `Running`;
///   the original error is retained as its source. This low-level API does
///   not manage worker retirement: stop renewing this worker's heartbeat and
///   resolve the claim or allow orphan recovery before restarting. Retrying
///   this call alone does not establish that the previous claim rolled back.
pub async fn lock_task(pool: &PgPool, task_id: &PgTaskId, worker_id: &str) -> Result<(), Error> {
    queries::lock_task(pool.clone(), *task_id.inner(), worker_id.to_owned(), None)
        .await
        .map(|_| ())
}

/// Lock a due task scoped to a specific queue.
///
/// Like [`lock_task`] but restricts the lock to `queue` so admin tooling that
/// knows the task's `Ulid` cannot accidentally (or maliciously) lock a task
/// belonging to another queue. Use this in any code path that does not derive
/// the queue from a trusted `WorkerContext`.
///
/// # Errors
/// Same as [`lock_task`], including [`Error::ClaimOutcomeUnknown`] and its
/// recovery obligations. [`Error::TaskNotFound`] is also returned when the
/// task exists but belongs to a different queue.
pub async fn lock_task_in_queue(
    pool: &PgPool,
    task_id: &PgTaskId,
    worker_id: &str,
    queue: &str,
) -> Result<(), Error> {
    queries::lock_task(
        pool.clone(),
        *task_id.inner(),
        worker_id.to_owned(),
        Some(queue.to_owned()),
    )
    .await
    .map(|_| ())
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
    leases: Option<crate::lease::LeaseRegistry>,
    lease_token: Option<ClaimToken>,
}

/// Keep claim credentials out of the derived middleware/service Debug output.
#[derive(Clone)]
struct ClaimToken(Arc<str>);
impl std::fmt::Debug for ClaimToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

impl LockTaskLayer {
    /// Create a lock middleware layer.
    #[must_use]
    pub(crate) fn new(pool: PgPool) -> Self {
        Self {
            pool,
            leases: None,
            lease_token: None,
        }
    }
}

impl<S> Layer<S> for LockTaskLayer {
    type Service = LockTaskService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        LockTaskService {
            inner,
            pool: self.pool.clone(),
            leases: self.leases.clone(),
            lease_token: self.lease_token.clone(),
        }
    }
}

/// Middleware layer used by the PostgreSQL backend.
///
/// The lock step always runs. The acknowledge step is installed only when the
/// queue config has automatic acknowledgement enabled.
///
/// A successful claim supplies the persisted attempt history used by automatic
/// acknowledgement. This works both inside an Apalis worker and when this layer
/// wraps a service directly. A direct service does not install Apalis's Tracker:
/// its task counter initially contains the previous completed count; a worker's
/// Tracker increases that counter on the first poll. Either way, acknowledgement
/// consumes exactly one attempt from the claim's history.
///
/// Each claim is acknowledged at most once. A layer outside this middleware
/// that re-dispatches a task in-process, such as a retry layer, receives
/// [`Error::AlreadyAcknowledged`] inside an `AbortError` for the repeat, and
/// the handler does not run again: the persisted retry budget schedules the
/// next attempt. The refusal counts as a dispatch on the task's attempt
/// counter so counter-based retry policies terminate. Every other refusal
/// before the handler, such as [`Error::ClaimLost`] or
/// [`Error::WorkerRetired`], counts the same way.
#[derive(Debug, Clone)]
pub struct PgMiddleware {
    lock: LockTaskLayer,
    ack: Option<AcknowledgeLayer<PgAck>>,
}

impl PgMiddleware {
    /// Create the PostgreSQL backend middleware without a registration token.
    ///
    /// A fallback claim checks only that the worker is registered for the
    /// task's queue, and an acknowledgement is gated by `(lock_by, lock_at,
    /// attempts)` alone; neither is fenced by a registration token. Like
    /// [`lock_task`], this middleware does not manage local worker
    /// retirement: a dropped or failed acknowledgement leaves the worker's
    /// heartbeat running. The middleware for a storage-registered worker is
    /// `Backend::middleware`, which carries that registration's token and
    /// liveness.
    #[must_use]
    pub fn new(pool: &PgPool, auto_ack: bool) -> Self {
        Self {
            lock: LockTaskLayer::new(pool.clone()),
            ack: auto_ack.then(|| AcknowledgeLayer::new(PgAck::new(pool))),
        }
    }

    /// Bind new fallback claims and automatic acknowledgements to a worker
    /// registration token. Existing preclaimed work may still finish; its
    /// acknowledgement must match the registration that owns the claim.
    ///
    /// This is for callers that registered the worker outside this crate's
    /// storage and hold its token. It binds the token only and does not
    /// manage local worker retirement; `Backend::middleware` does both for a
    /// storage-registered worker.
    #[must_use]
    pub fn with_lease_token(pool: &PgPool, auto_ack: bool, lease_token: Arc<str>) -> Self {
        let mut lock = LockTaskLayer::new(pool.clone());
        lock.lease_token = Some(ClaimToken(lease_token.clone()));
        Self {
            lock,
            ack: auto_ack
                .then(|| AcknowledgeLayer::new(PgAck::with_lease_token(pool, lease_token))),
        }
    }

    pub(crate) fn with_lease_registry(
        pool: &PgPool,
        auto_ack: bool,
        lease_token: Arc<str>,
        leases: crate::lease::LeaseRegistry,
    ) -> Self {
        let acknowledger = PgAck::with_lease_registry(pool, lease_token.clone(), leases.clone());
        let mut lock = LockTaskLayer::new(pool.clone());
        lock.leases = Some(leases);
        lock.lease_token = Some(ClaimToken(lease_token));
        Self {
            lock,
            ack: auto_ack.then(|| AcknowledgeLayer::new(acknowledger)),
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
    AcknowledgeLayer<PgAck>: Layer<S>,
{
    type Service = PgMiddlewareService<
        LockTaskService<<AcknowledgeLayer<PgAck> as Layer<S>>::Service>,
        LockTaskService<S>,
    >;
    fn layer(&self, inner: S) -> Self::Service {
        // Acquire ownership before AcknowledgeService snapshots task Parts.
        match &self.ack {
            Some(ack) => PgMiddlewareService::AutoAck(LockTaskService {
                inner: ack.layer(inner),
                pool: self.lock.pool.clone(),
                leases: self.lock.leases.clone(),
                lease_token: self.lock.lease_token.clone(),
            }),
            None => PgMiddlewareService::ManualAck(LockTaskService {
                inner,
                pool: self.lock.pool.clone(),
                leases: self.lock.leases.clone(),
                lease_token: self.lock.lease_token.clone(),
            }),
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
    leases: Option<crate::lease::LeaseRegistry>,
    lease_token: Option<ClaimToken>,
}

/// Whether a task arriving at `LockTaskService` was already claimed for this
/// worker by the fetcher's dequeue UPDATE (`fetch_next` / `queue_by_id` set both
/// `lock_by` and `lock_at`), so it is started instead of locked.
///
/// Pre-claimed requires BOTH that the stored lock owner equals the current
/// worker AND that a lock timestamp is present — a half-populated context (only
/// one of the two) must still go through the SQL path. Extracted as a pure
/// predicate so that conjunction is unit-testable without a backend.
fn is_preclaimed(lock_by: Option<&str>, worker_id: Option<&str>, has_lock_at: bool) -> bool {
    matches!(
        (lock_by, worker_id),
        (Some(stored), Some(current)) if stored == current
    ) && has_lock_at
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

    fn call(&mut self, mut req: PgTask<Args>) -> Self::Future {
        let pool = self.pool.clone();
        let lease_token = self.lease_token.as_ref().map(|token| token.0.clone());
        let worker_id = req
            .parts
            .data
            .get::<WorkerContext>()
            .map(|worker| worker.name().to_owned());
        let local_lease = self
            .leases
            .as_ref()
            .and_then(|leases| worker_id.as_deref().map(|id| leases.for_worker(id)));
        let queue = req.parts.ctx.queue().clone();
        let task_id = req.parts.task_id.map(|id| *id.inner());
        // A task the fetcher already claimed for this worker (`fetch_next`
        // and `queue_by_id` set both `lock_by` and `lock_at` and leave the
        // row `Queued`) only needs to be started under its claim epoch.
        // Tasks that arrive without `lock_by`/`lock_at` in the context go
        // through `lock_task`, which claims and starts in one statement.
        let preclaimed = is_preclaimed(
            req.parts.ctx.lock_by().as_deref(),
            worker_id.as_deref(),
            req.parts.ctx.lock_at().is_some(),
        );
        let claim = req.parts.data.get::<ClaimAttempt>().cloned();
        let acknowledged_at_call =
            preclaimed && claim.as_ref().is_some_and(ClaimAttempt::is_acknowledged);
        // A preclaimed task carries a completion obligation from this call
        // on, unless its acknowledgement already committed: then there is
        // nothing to lose, so nothing is armed.
        let mut completion_guard = ClaimObligation {
            guard: if preclaimed && !acknowledged_at_call {
                local_lease.as_ref().map(|lease| lease.guard())
            } else {
                None
            },
            claim: claim.clone(),
        };
        // Tower service contract: `poll_ready` reserves capacity on
        // `self.inner`; that exact instance MUST be the one that consumes the
        // reservation via `call`. Take ownership of the ready instance and
        // leave a clone behind so subsequent `poll_ready`/`call` cycles work.
        // The clone is treated as a fresh, not-yet-ready instance — the caller
        // will `poll_ready` it again before sending the next request.
        let clone = self.inner.clone();
        let mut ready_inner = std::mem::replace(&mut self.inner, clone);

        async move {
            // Every refusal below happens before the handler, where the
            // worker's Tracker would have counted the dispatch. Count it here,
            // in one place, so a counter-based retry layer outside this
            // middleware stops after its budget instead of re-dispatching the
            // refusal.
            let attempt = req.parts.attempt.clone();
            let admitted: Result<PgTask<Args>, BoxDynError> = async {
                let worker_id = worker_id
                    .ok_or_else(|| AbortError::new(Error::MissingField("worker_context")))?;
                let task_id =
                    task_id.ok_or_else(|| AbortError::new(Error::MissingField("task_id")))?;
                // Read the marker when the dispatch runs: an acknowledgement in
                // flight when this future was created may have committed since.
                if preclaimed && claim.as_ref().is_some_and(ClaimAttempt::is_acknowledged) {
                    // This claim's acknowledgement already committed: the database
                    // retry budget schedules any further attempt. Refuse before the
                    // handler runs and keep the lease: no obligation was lost.
                    completion_guard.disarm();
                    return Err(AbortError::new(Error::already_acknowledged(
                        task_id.to_string(),
                        queue.unwrap_or_default(),
                        worker_id,
                    ))
                    .into());
                }
                if preclaimed {
                    // A retired worker starts nothing: its claims are handed
                    // back by the release or by recovery, without consuming an
                    // attempt while they are still `Queued`.
                    if let Some(lease) = &local_lease {
                        if let Err(error) = lease.ensure_active() {
                            completion_guard.disarm();
                            return Err(AbortError::new(error).into());
                        }
                    }
                    // The claim is `Queued`; start it under its epoch. A row that
                    // a sweep, a takeover or a release recovered in the meantime,
                    // or that another claim of the same epoch already started,
                    // matches nothing: the task runs elsewhere, so this dispatch
                    // is refused and nothing is owed.
                    let (lock_at, attempts, restart) = match claim.as_ref() {
                        Some(claim) => (
                            claim.lock_at,
                            i32::try_from(claim.completed).ok(),
                            claim.has_started(),
                        ),
                        // A manually assembled task has no claim identity to tell
                        // a re-dispatch from a second claim; it starts either way.
                        None => (
                            *req.parts.ctx.lock_at(),
                            i32::try_from(req.parts.attempt.current()).ok(),
                            true,
                        ),
                    };
                    // None of these refusals starts anything, so the claim stays
                    // owned and `Queued`: the armed guard retires the worker and
                    // recovery hands the claim back without charging it.
                    let Some(queue) = queue else {
                        return Err(AbortError::new(Error::MissingField("queue")).into());
                    };
                    let Some(lock_at) = lock_at else {
                        return Err(AbortError::new(Error::MissingField("lock_at")).into());
                    };
                    let Some(attempts) = attempts else {
                        return Err(AbortError::new(Error::InvalidArgument(
                            "task attempt counter does not fit the attempts column".to_owned(),
                        ))
                        .into());
                    };
                    let started = match queries::start_task(
                        pool.clone(),
                        queries::StartClaim {
                            task_id: PgTaskId::new(task_id),
                            queue: queue.clone(),
                            worker_id: worker_id.clone(),
                            lock_at,
                            attempts,
                            restart,
                            lease_token: lease_token.clone(),
                        },
                    )
                    .await
                    {
                        Ok(started) => started,
                        Err(error) => {
                            // The claim may still be owned and not started. The
                            // armed guard retires the worker, so recovery hands
                            // the claim back once the registration is stale.
                            return Err(AbortError::new(error).into());
                        }
                    };
                    if !started {
                        completion_guard.disarm();
                        return Err(AbortError::new(Error::claim_lost(
                            task_id.to_string(),
                            queue,
                            worker_id,
                        ))
                        .into());
                    }
                    if let Some(claim) = claim.as_ref() {
                        claim.mark_started();
                    }
                    req.parts.status.store(Status::Running);
                } else {
                    if let Some(lease) = &local_lease {
                        lease.ensure_active().map_err(AbortError::new)?;
                    }
                    completion_guard.guard = local_lease.as_ref().map(|lease| lease.guard());
                    let claimed = match queries::fetch::lock_task_with_token(
                        pool,
                        task_id,
                        worker_id,
                        queue,
                        lease_token,
                    )
                    .await
                    {
                        Ok(claimed) => claimed,
                        Err(error) => {
                            // A rejected transaction did not transfer ownership;
                            // an unconfirmed commit may have. Keep its obligation
                            // armed, including a panic after COMMIT instrumentation.
                            if !matches!(&error, Error::ClaimOutcomeUnknown { .. }) {
                                completion_guard.disarm();
                            }
                            return Err(AbortError::new(error).into());
                        }
                    };
                    req.parts.ctx = claimed.parts.ctx;
                    req.parts.attempt = claimed.parts.attempt;
                    req.parts.status.store(Status::Running);
                    record_claim(&mut req.parts);
                    // `lock_task` claims and starts in one statement.
                    let recorded = req.parts.data.get::<ClaimAttempt>().cloned();
                    if let Some(recorded) = recorded.as_ref() {
                        recorded.mark_started();
                    }
                    completion_guard.claim = recorded;
                }
                Ok(req)
            }
            .await;
            let req = match admitted {
                Ok(req) => req,
                Err(refusal) => {
                    let _ = attempt.increment();
                    return Err(refusal);
                }
            };
            let result = ready_inner.call(req).await.map_err(Into::into);
            // A completed handler error is a valid acknowledged outcome. PgAck
            // retires on acknowledgement failure; this guard covers cancellation.
            completion_guard.disarm();
            result
        }
        .boxed()
    }
}
