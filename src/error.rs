use std::borrow::Cow;

use apalis_core::error::BoxDynError;

/// Error type returned by the Diesel PostgreSQL backend.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Diesel query failed while running a named backend operation.
    #[error("database error while {operation}: {source}{hint}", hint = database_hint(source))]
    Database {
        /// Backend operation that was running when Diesel returned the error.
        operation: Cow<'static, str>,
        /// Original Diesel error.
        #[source]
        source: diesel::result::Error,
    },

    /// Acquiring a pooled connection failed.
    #[error(
        "failed to acquire PostgreSQL connection from r2d2 pool: {0}; check that DATABASE_URL points to a reachable PostgreSQL server and that the pool has enough connections"
    )]
    Pool(#[from] diesel::r2d2::PoolError),

    /// A blocking runtime task failed to complete.
    #[error("blocking task failed: {0}")]
    Blocking(#[source] BoxDynError),

    /// Database migrations failed.
    #[error("failed to run embedded migrations: {0}")]
    Migration(#[source] BoxDynError),

    /// A task row could not be converted into an Apalis task.
    #[error("failed to convert database row into an Apalis task: {0}")]
    Row(#[source] BoxDynError),

    /// A caller-supplied argument was out of range for the backend.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// One or more tasks in an enqueue batch collided with the
    /// `(job_type, idempotency_key)` unique constraint.
    ///
    /// The **whole batch is rolled back, not only the colliding rows**: the
    /// duplicates are dropped by `ON CONFLICT DO NOTHING`, then the post-insert
    /// check rolls the batch's SAVEPOINT back, so every task in the batch is
    /// undone — including the non-conflicting ones. A surrounding (outer)
    /// transaction stays alive, so the caller can decide whether to commit the
    /// rest of its work or roll back. `conflicting_keys` lists exactly which
    /// `idempotency_key`s collided. A key collides either against an
    /// already-stored row or against another task **in the same batch**, so to
    /// re-enqueue, *deduplicate* rather than drop: keep exactly one task per
    /// conflicting key (dropping every task carrying the key would silently
    /// lose intra-batch duplicates that have no stored row yet) and resubmit;
    /// keys that still collide on the retry are duplicates of stored rows and
    /// can then be dropped. Match this variant instead of the
    /// [`Error::InvalidArgument`] message text to tell a benign duplicate apart
    /// from a real failure.
    #[error(
        "idempotency_key conflict in queue `{job_type}`: keys {conflicting_keys:?} collided with the unique constraint; {total} task(s) in the batch were all rolled back"
    )]
    IdempotencyConflict {
        /// Queue (`job_type`) the conflicting batch targeted.
        job_type: String,
        /// The distinct `idempotency_key`s that collided — either against an
        /// already-stored row or another task in the same batch.
        conflicting_keys: Vec<String>,
        /// Total tasks in the batch; all of them were rolled back.
        total: usize,
    },

    /// A task payload or task result could not be decoded.
    #[error("failed to decode task payload or result with the configured codec: {0}")]
    Decode(#[source] BoxDynError),

    /// JSON encoding or decoding failed.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// A required task field was missing.
    #[error(
        "task metadata is missing required field `{0}`; this usually means the task did not go through the expected poll/lock/ack lifecycle"
    )]
    MissingField(&'static str),

    /// A worker registration already exists for the queue, or is being
    /// registered concurrently.
    #[error(
        "worker `{worker_id}` is already registered for queue `{queue}`, or is being registered concurrently"
    )]
    AlreadyRegistered {
        /// Worker id whose registration collided.
        worker_id: String,
        /// Queue (`worker_type`) the worker was registering for.
        queue: String,
    },

    /// A task could not be locked because it was absent or not currently lockable.
    #[error("task not found while {operation} (task_id: {task_id}, queue: {queue}); {hint}")]
    TaskNotFound {
        /// Backend operation that failed.
        operation: Cow<'static, str>,
        /// Task id involved in the operation.
        task_id: String,
        /// Queue involved in the operation, or a placeholder when unconstrained.
        queue: String,
        /// Human-readable next step.
        hint: &'static str,
    },

    /// A task acknowledgement no longer matches the stored lock state.
    #[error(
        "stale acknowledgement for task {task_id} in queue {queue} by worker {worker_id}; the task is no longer Running with the same lock owner, attempt, and lock timestamp"
    )]
    StaleAcknowledgement {
        /// Task id involved in the acknowledgement.
        task_id: String,
        /// Queue involved in the acknowledgement.
        queue: String,
        /// Worker id involved in the acknowledgement.
        worker_id: String,
    },

    /// A worker heartbeat could not be recorded because the worker row is absent.
    #[error(
        "worker not registered while {operation} (worker_id: {worker_id}, queue: {queue}); {hint}"
    )]
    WorkerNotRegistered {
        /// Backend operation that failed.
        operation: Cow<'static, str>,
        /// Worker id involved in the operation.
        worker_id: String,
        /// Queue involved in the operation.
        queue: String,
        /// Human-readable next step.
        hint: &'static str,
    },

    /// PostgreSQL notification listener failed.
    #[error(
        "PostgreSQL notification listener failed: {0}; polling fallback can still fetch jobs, but LISTEN/NOTIFY wakeups are disabled until the stream is recreated"
    )]
    NotifyListener(String),

    /// A sink producer attempted to send without observing backpressure.
    #[error("sink buffer is full; call poll_ready before start_send (capacity: {0})")]
    SinkBufferFull(usize),
}

// `diesel::Connection::transaction` requires the closure error type to be
// `From<diesel::result::Error>` so its transaction_manager can lift begin /
// commit / rollback failures (`connection/transaction_manager.rs`) into our
// error type. **Inside the closure**, every Diesel call should still use
// `.map_err(Error::database("specific op"))` so the operation label reflects
// the failing statement; this `From` only fires when an unhandled
// `diesel::result::Error` reaches `transaction()` itself (begin/commit/
// rollback, or an inner statement whose `?` was not explicitly mapped). The
// generic label below makes the fallback path unambiguous in logs.
impl From<diesel::result::Error> for Error {
    fn from(source: diesel::result::Error) -> Self {
        Self::Database {
            operation: Cow::Borrowed(
                "diesel transaction begin/commit/rollback (unlabeled — use map_err inside the closure)",
            ),
            source,
        }
    }
}

impl Error {
    pub(crate) fn database(
        operation: impl Into<Cow<'static, str>>,
    ) -> impl FnOnce(diesel::result::Error) -> Self {
        let operation = operation.into();
        move |source| Self::Database { operation, source }
    }

    pub(crate) fn task_not_found(
        operation: impl Into<Cow<'static, str>>,
        task_id: impl Into<String>,
        queue: Option<String>,
        hint: &'static str,
    ) -> Self {
        Self::TaskNotFound {
            operation: operation.into(),
            task_id: task_id.into(),
            queue: queue.unwrap_or_else(|| "<not constrained>".to_owned()),
            hint,
        }
    }

    pub(crate) fn stale_acknowledgement(
        task_id: impl Into<String>,
        queue: impl Into<String>,
        worker_id: impl Into<String>,
    ) -> Self {
        Self::StaleAcknowledgement {
            task_id: task_id.into(),
            queue: queue.into(),
            worker_id: worker_id.into(),
        }
    }

    pub(crate) fn worker_not_registered(
        operation: impl Into<Cow<'static, str>>,
        worker_id: impl Into<String>,
        queue: impl Into<String>,
        hint: &'static str,
    ) -> Self {
        Self::WorkerNotRegistered {
            operation: operation.into(),
            worker_id: worker_id.into(),
            queue: queue.into(),
            hint,
        }
    }

    pub(crate) fn already_registered(
        worker_id: impl Into<String>,
        queue: impl Into<String>,
    ) -> Self {
        Self::AlreadyRegistered {
            worker_id: worker_id.into(),
            queue: queue.into(),
        }
    }

    pub(crate) fn idempotency_conflict(
        job_type: impl Into<String>,
        conflicting_keys: Vec<String>,
        total: usize,
    ) -> Self {
        Self::IdempotencyConflict {
            job_type: job_type.into(),
            conflicting_keys,
            total,
        }
    }
}

fn database_hint(error: &diesel::result::Error) -> &'static str {
    use diesel::result::Error as DieselError;
    match error {
        // Locale-independent: diesel maps undefined_table errors into NotFound
        // variants for queries that expect a result; but structured DatabaseError
        // matches happen here. Prefer `table_name()` and `constraint_name()`
        // (locale-independent) over `message()` substring matching, which fails
        // on non-English PostgreSQL servers.
        DieselError::DatabaseError(_, info) => {
            if matches!(info.table_name(), Some(name) if name == "jobs")
                && matches!(
                    info.constraint_name(),
                    Some(name)
                        if name == "jobs_lock_by_worker_type_fkey"
                            || name == "jobs_lock_by_fkey"
                )
            {
                return "; register the worker for this queue before locking or acknowledging jobs";
            }
            // Fallback: message-based detection for installations where neither
            // table_name nor constraint_name is populated (e.g. when the
            // relation itself does not yet exist).
            let message = info.message();
            if message.contains("apalis.jobs")
                && (message.contains("does not exist") || message.contains("relation"))
            {
                "; run apalis_diesel_postgres::setup(&pool).await before using the storage"
            } else if message.contains("foreign key")
                || message.contains("jobs_lock_by_worker_type_fkey")
                || message.contains("jobs_lock_by_fkey")
            {
                "; register the worker for this queue before locking or acknowledging jobs"
            } else {
                ""
            }
        }
        _ => "",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::result::{DatabaseErrorInformation, DatabaseErrorKind, Error as DieselError};
    use lets_expect::{AssertionError, AssertionResult, *};
    use std::error::Error as StdError;

    struct StubInfo {
        message: &'static str,
        table_name: Option<&'static str>,
        constraint_name: Option<&'static str>,
    }

    impl DatabaseErrorInformation for StubInfo {
        fn message(&self) -> &str {
            self.message
        }
        fn details(&self) -> Option<&str> {
            None
        }
        fn hint(&self) -> Option<&str> {
            None
        }
        fn table_name(&self) -> Option<&str> {
            self.table_name
        }
        fn column_name(&self) -> Option<&str> {
            None
        }
        fn constraint_name(&self) -> Option<&str> {
            self.constraint_name
        }
        fn statement_position(&self) -> Option<i32> {
            None
        }
    }

    fn database_error_with(
        message: &'static str,
        table_name: Option<&'static str>,
        constraint_name: Option<&'static str>,
    ) -> DieselError {
        DieselError::DatabaseError(
            DatabaseErrorKind::Unknown,
            Box::new(StubInfo {
                message,
                table_name,
                constraint_name,
            }),
        )
    }

    fn hint_for(
        message: &'static str,
        table_name: Option<&'static str>,
        constraint_name: Option<&'static str>,
    ) -> &'static str {
        database_hint(&database_error_with(message, table_name, constraint_name))
    }

    fn non_database_hint() -> &'static str {
        database_hint(&DieselError::NotFound)
    }

    fn json_error() -> serde_json::Error {
        serde_json::from_str::<serde_json::Value>("not json").unwrap_err()
    }

    fn boxed_error(message: &'static str) -> BoxDynError {
        Box::new(std::io::Error::other(message))
    }

    fn database_error() -> Error {
        Error::Database {
            operation: Cow::Borrowed("fetching jobs"),
            source: diesel::result::Error::NotFound,
        }
    }

    /// A `Database` error whose diesel source carries a non-empty
    /// `database_hint` (the missing-`apalis.jobs`-relation signal), so the
    /// Display assertion exercises the `{source}{hint}` concatenation with a
    /// NON-empty hint — the path `database_error()` (empty hint) cannot reach.
    fn database_error_with_setup_hint() -> Error {
        Error::Database {
            operation: Cow::Borrowed("locking task"),
            source: database_error_with("relation \"apalis.jobs\" does not exist", None, None),
        }
    }

    fn displays_as(expected: &'static str) -> impl Fn(&Error) -> AssertionResult {
        move |error| {
            let actual = error.to_string();
            if actual == expected {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected display {expected:?}, got {actual:?}"
                )]))
            }
        }
    }

    fn has_source_containing(expected: &'static str) -> impl Fn(&Error) -> AssertionResult {
        move |error| match StdError::source(error) {
            Some(source) if source.to_string().contains(expected) => Ok(()),
            Some(source) => Err(AssertionError::new(vec![format!(
                "expected source containing {expected:?}, got {:?}",
                source.to_string()
            )])),
            None => Err(AssertionError::new(vec![format!(
                "expected source containing {expected:?}, got no source"
            )])),
        }
    }

    fn has_no_source(error: &Error) -> AssertionResult {
        match StdError::source(error) {
            None => Ok(()),
            Some(source) => Err(AssertionError::new(vec![format!(
                "expected no source, got {:?}",
                source.to_string()
            )])),
        }
    }

    fn is_task_not_found(error: &Error) -> AssertionResult {
        match error {
            Error::TaskNotFound {
                operation,
                task_id,
                queue,
                ..
            } if *operation == "locking task" && task_id == "task-1" && queue == "queue-1" => {
                Ok(())
            }
            other => Err(AssertionError::new(vec![format!(
                "expected task not found, got {other:?}"
            )])),
        }
    }

    fn is_idempotency_conflict(error: &Error) -> AssertionResult {
        match error {
            Error::IdempotencyConflict {
                job_type,
                conflicting_keys,
                total,
            } if job_type == "emails"
                && conflicting_keys.len() == 2
                && conflicting_keys[0] == "k-1"
                && conflicting_keys[1] == "k-2"
                && *total == 3 =>
            {
                Ok(())
            }
            other => Err(AssertionError::new(vec![format!(
                "expected idempotency conflict {{emails, [k-1, k-2], 3}}, got {other:?}"
            )])),
        }
    }

    lets_expect! {
        expect(database_error()) {
            to displays_the_operation_context { displays_as("database error while fetching jobs: Record not found") }
            to exposes_the_database_error_as_the_source { has_source_containing("Record not found") }
        }

        expect(database_error_with_setup_hint()) {
            to displays_the_operation_context_with_the_setup_hint_appended {
                displays_as("database error while locking task: relation \"apalis.jobs\" does not exist; run apalis_diesel_postgres::setup(&pool).await before using the storage")
            }
        }

        expect(Error::Blocking(boxed_error("join cancelled"))) {
            to displays_the_blocking_error { displays_as("blocking task failed: join cancelled") }
            to exposes_the_blocking_error_as_the_source { has_source_containing("join cancelled") }
        }

        expect(Error::Migration(boxed_error("missing migration"))) {
            to displays_the_migration_error { displays_as("failed to run embedded migrations: missing migration") }
            to exposes_the_migration_error_as_the_source { has_source_containing("missing migration") }
        }

        expect(Error::Row(boxed_error("bad task row"))) {
            to displays_the_row_conversion_error { displays_as("failed to convert database row into an Apalis task: bad task row") }
            to exposes_the_row_error_as_the_source { has_source_containing("bad task row") }
        }

        expect(Error::Decode(boxed_error("bad payload"))) {
            to displays_the_decode_error { displays_as("failed to decode task payload or result with the configured codec: bad payload") }
            to exposes_the_decode_error_as_the_source { has_source_containing("bad payload") }
        }

        expect(Error::Json(json_error())) {
            to displays_the_json_error { displays_as("json error: expected ident at line 1 column 2") }
            to exposes_the_json_error_as_the_source { has_source_containing("expected ident") }
        }

        expect(Error::MissingField("run_at")) {
            to displays_the_missing_field_error { displays_as("task metadata is missing required field `run_at`; this usually means the task did not go through the expected poll/lock/ack lifecycle") }
            to has_no_error_source { has_no_source }
        }

        expect(Error::already_registered("worker-1", "emails")) {
            to displays_the_registration_error_with_worker_and_queue {
                displays_as("worker `worker-1` is already registered for queue `emails`, or is being registered concurrently")
            }
            to has_no_error_source { has_no_source }
        }

        expect(Error::idempotency_conflict(
            "emails",
            vec!["k-1".to_owned(), "k-2".to_owned()],
            3,
        )) {
            to identifies_the_conflicting_queue_and_keys { is_idempotency_conflict }
            to displays_the_conflict_and_rollback_semantics { displays_as("idempotency_key conflict in queue `emails`: keys [\"k-1\", \"k-2\"] collided with the unique constraint; 3 task(s) in the batch were all rolled back") }
            to has_no_error_source { has_no_source }
        }

        // Empty conflicting_keys is the boundary of the Vec axis. In production
        // this state is effectively unreachable (only keyed submissions can
        // collide), so this leaf pins the pure Display rendering of the `[]`
        // branch rather than a reachable conflict.
        expect(Error::idempotency_conflict("emails", vec![], 0)) {
            to displays_an_empty_conflict_batch {
                displays_as("idempotency_key conflict in queue `emails`: keys [] collided with the unique constraint; 0 task(s) in the batch were all rolled back")
            }
            to has_no_error_source { has_no_source }
        }

        expect(Error::task_not_found(
            "locking task",
            "task-1",
            Some("queue-1".to_owned()),
            "the task may be delayed, already locked by another worker, completed, or in another queue",
        )) {
            to returns_a_contextual_task_not_found_error { is_task_not_found }
            to displays_the_next_step { displays_as("task not found while locking task (task_id: task-1, queue: queue-1); the task may be delayed, already locked by another worker, completed, or in another queue") }
            to has_no_error_source { has_no_source }
        }

        expect(Error::task_not_found(
            "locking task",
            "task-1",
            None,
            "the task may be delayed, already locked by another worker, or completed",
        )) {
            when queue_is_not_constrained {
                // Mirrors the unscoped `lock_task` path (queries/fetch.rs):
                // a `None` queue both renders the `<not constrained>` placeholder
                // and drops the "or in another queue" reason, which cannot apply
                // when the lock is not filtered by queue.
                to renders_the_unconstrained_placeholder {
                    displays_as("task not found while locking task (task_id: task-1, queue: <not constrained>); the task may be delayed, already locked by another worker, or completed")
                }
            }
        }

        expect(Error::stale_acknowledgement("task-1", "queue-1", "worker-1")) {
            to displays_the_ack_conflict { displays_as("stale acknowledgement for task task-1 in queue queue-1 by worker worker-1; the task is no longer Running with the same lock owner, attempt, and lock timestamp") }
            to has_no_error_source { has_no_source }
        }

        expect(Error::worker_not_registered(
            "updating worker heartbeat",
            "worker-1",
            "queue-1",
            "recreate the worker stream so registration can run again",
        )) {
            to displays_the_worker_registration_problem { displays_as("worker not registered while updating worker heartbeat (worker_id: worker-1, queue: queue-1); recreate the worker stream so registration can run again") }
            to has_no_error_source { has_no_source }
        }

        expect(Error::NotifyListener("LISTEN failed".to_owned())) {
            to displays_the_notify_degradation { displays_as("PostgreSQL notification listener failed: LISTEN failed; polling fallback can still fetch jobs, but LISTEN/NOTIFY wakeups are disabled until the stream is recreated") }
            to has_no_error_source { has_no_source }
        }

        expect(Error::SinkBufferFull(1)) {
            to displays_the_sink_buffer_error { displays_as("sink buffer is full; call poll_ready before start_send (capacity: 1)") }
            to has_no_error_source { has_no_source }
        }

        expect(Error::InvalidArgument("limit 99 exceeds i32::MAX".to_owned())) {
            to displays_the_invalid_argument_error {
                displays_as("invalid argument: limit 99 exceeds i32::MAX")
            }
            to has_no_error_source { has_no_source }
        }

        expect(Error::from(diesel::result::Error::NotFound)) {
            to labels_the_unlabeled_transaction_path {
                displays_as("database error while diesel transaction begin/commit/rollback (unlabeled — use map_err inside the closure): Record not found")
            }
            to exposes_the_diesel_error_as_the_source { has_source_containing("Record not found") }
        }

        expect(non_database_hint()) {
            when diesel_error_is_not_a_database_variant {
                to returns_no_hint { equal("") }
            }
        }

        expect(hint_for(message, table_name, constraint_name)) {
            let message = "irrelevant";
            let table_name: Option<&'static str> = None;
            let constraint_name: Option<&'static str> = None;

            when structured_info_points_at_the_worker_type_foreign_key {
                let table_name = Some("jobs");
                let constraint_name = Some("jobs_lock_by_worker_type_fkey");
                to recommends_registering_the_worker {
                    equal(
                        "; register the worker for this queue before locking or acknowledging jobs",
                    )
                }
            }

            when structured_info_points_at_the_legacy_lock_by_foreign_key {
                let table_name = Some("jobs");
                let constraint_name = Some("jobs_lock_by_fkey");
                to recommends_registering_the_worker_via_the_legacy_constraint {
                    equal(
                        "; register the worker for this queue before locking or acknowledging jobs",
                    )
                }
            }

            when message_indicates_a_missing_apalis_jobs_relation_with_does_not_exist {
                let message = "relation \"apalis.jobs\" does not exist";
                to recommends_running_setup {
                    equal(
                        "; run apalis_diesel_postgres::setup(&pool).await before using the storage",
                    )
                }
            }

            when message_indicates_a_missing_apalis_jobs_relation_via_the_word_relation {
                let message = "missing relation apalis.jobs from schema";
                to recommends_running_setup_via_the_relation_match {
                    equal(
                        "; run apalis_diesel_postgres::setup(&pool).await before using the storage",
                    )
                }
            }

            when message_mentions_a_generic_foreign_key_violation {
                let message = "foreign key constraint violated";
                to recommends_registering_the_worker_via_message {
                    equal(
                        "; register the worker for this queue before locking or acknowledging jobs",
                    )
                }
            }

            when message_mentions_the_worker_foreign_key_by_name {
                let message = "jobs_lock_by_worker_type_fkey conflict";
                to recommends_registering_the_worker_via_named_constraint {
                    equal(
                        "; register the worker for this queue before locking or acknowledging jobs",
                    )
                }
            }

            when message_mentions_the_legacy_foreign_key_by_name {
                let message = "jobs_lock_by_fkey conflict";
                to recommends_registering_the_worker_via_legacy_named_constraint {
                    equal(
                        "; register the worker for this queue before locking or acknowledging jobs",
                    )
                }
            }

            when message_is_unrelated_to_any_known_signal {
                let message = "deadlock detected on update";
                to returns_no_hint { equal("") }
            }

            // The setup-hint predicate is a conjunction: BOTH the table-name
            // signal ("apalis.jobs") AND a missing-relation signal
            // ("does not exist" / "relation") must be present. These two
            // siblings pin each operand alone so the `&&` cannot silently
            // degrade to `||` — which would over-eagerly recommend `setup` on
            // any unrelated relation/lookup error or on a benign mention of the
            // jobs table.
            when message_names_apalis_jobs_without_a_missing_relation_signal {
                let message = "could not obtain lock on apalis.jobs";
                to returns_no_hint { equal("") }
            }

            when message_signals_a_missing_relation_for_a_different_table {
                let message = "relation \"audit_log\" does not exist";
                to returns_no_hint { equal("") }
            }

            when structured_constraint_matches_an_fk_name_but_table_is_not_jobs {
                // The combined predicate at error.rs:177-184 requires BOTH
                // table_name == "jobs" AND constraint matching a known FK.
                // When the table differs (e.g. a custom mirror table that
                // happens to reuse the FK name) the structured arm must NOT
                // fire; the message-based fallback then decides.
                let table_name = Some("custom_mirror");
                let constraint_name = Some("jobs_lock_by_worker_type_fkey");
                let message = "deadlock detected on update";
                to falls_through_to_message_matching_and_returns_no_hint { equal("") }
            }

            when structured_table_is_jobs_but_constraint_is_an_unrelated_name {
                // Sibling to the two FK-matching `when`s above: table is
                // "jobs" but the constraint is something else (e.g. a check
                // constraint). The structured arm must NOT fire.
                let table_name = Some("jobs");
                let constraint_name = Some("jobs_status_check");
                let message = "violates check constraint";
                to falls_through_to_message_matching_and_returns_no_hint { equal("") }
            }
        }
    }
}
