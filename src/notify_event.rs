//! NOTIFY payload type for the `apalis::job::insert` channel.
//!
//! Both the per-row legacy trigger (`{job_type, id}`) and the statement-level
//! trigger introduced in migration `20260521000001` (`{job_type, ids: [...]}`)
//! serialize into this struct via `serde(default)` on the optional fields.

use serde::Deserialize;

/// One pending listener failure per consumer, separate from lossy wakeup ids.
/// Repeated failures coalesce until observed; a full hint buffer cannot hide
/// the fact that notifications stopped. Never holds task payloads.
#[derive(Default)]
pub(crate) struct NotificationErrors {
    pending: std::sync::Mutex<Option<crate::Error>>,
    waker: futures::task::AtomicWaker,
}

impl NotificationErrors {
    pub(crate) fn publish(&self, error: crate::Error) {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert(error);
        self.waker.wake();
    }

    pub(crate) fn take(&self, cx: &std::task::Context<'_>) -> Option<crate::Error> {
        self.waker.register(cx.waker());
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

use crate::PgTaskId;

/// Maximum number of task ids accepted in a single NOTIFY payload.
///
/// The statement-level trigger batches all ids from one INSERT statement; in
/// practice this is bounded by the application's batch size. The cap exists
/// to bound memory in the rare-but-possible scenario where a third party with
/// `pg_notify` privilege fabricates a payload with millions of ids — that
/// would otherwise force the listener to allocate a `Vec<PgTaskId>` of
/// attacker-controlled size before any downstream channel-full guard fires.
/// 64 KiB ids is several orders of magnitude above any realistic insert
/// batch.
pub(crate) const INSERT_EVENT_IDS_CAP: usize = 65_536;

/// Payload of an `apalis::job::insert` NOTIFY.
///
/// The statement-level trigger (migration `20260521000001`) emits one event
/// per (queue, INSERT statement) with all inserted ids batched in `ids`. The
/// legacy per-row trigger emitted `{job_type, id}` instead; both shapes
/// remain accepted so the listener works across migration states.
#[derive(Debug, Deserialize)]
pub(crate) struct InsertEvent {
    pub(crate) job_type: String,
    #[serde(default)]
    pub(crate) id: Option<PgTaskId>,
    #[serde(default)]
    pub(crate) ids: Vec<PgTaskId>,
}

impl InsertEvent {
    pub(crate) fn into_ids(self) -> (String, Vec<PgTaskId>) {
        let Self {
            job_type,
            id,
            mut ids,
        } = self;
        if ids.len() > INSERT_EVENT_IDS_CAP {
            ids.truncate(INSERT_EVENT_IDS_CAP);
        }
        if !ids.is_empty() {
            (job_type, ids)
        } else {
            (job_type, id.into_iter().collect())
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use futures::{Stream, channel::mpsc};
    use lets_expect::{AssertionError, AssertionResult};

    use crate::{Error, PgTaskId};

    /// Both listener specs use the same accepted IDs and the same multiset oracle.
    pub(crate) fn hint_channel(
        full: bool,
    ) -> (
        mpsc::Sender<PgTaskId>,
        mpsc::Receiver<PgTaskId>,
        Vec<PgTaskId>,
    ) {
        let (mut sender, receiver) = mpsc::channel(1);
        let mut accepted = Vec::new();
        if full {
            for value in 1..=16_u128 {
                let id = PgTaskId::new(ulid::Ulid::from(value));
                match sender.try_send(id) {
                    Ok(()) => accepted.push(id),
                    Err(error) if error.is_full() => break,
                    Err(error) => panic!("fresh hint receiver disconnected: {error}"),
                }
            }
            assert!(!accepted.is_empty() && accepted.len() < 16);
        }
        (sender, receiver, accepted)
    }

    #[derive(Debug)]
    pub(crate) struct HintObservation {
        expected: Vec<String>,
        delivered: Vec<String>,
        failures: Vec<String>,
        unexpected_errors: Vec<String>,
        failure_before_hints: bool,
    }

    pub(crate) fn observe_hints<S>(mut source: S, expected: Vec<PgTaskId>) -> HintObservation
    where
        S: Stream<Item = Result<PgTaskId, Error>> + Unpin,
    {
        let mut observed = HintObservation {
            expected: expected.into_iter().map(|id| id.to_string()).collect(),
            delivered: Vec::new(),
            failures: Vec::new(),
            unexpected_errors: Vec::new(),
            failure_before_hints: true,
        };
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        while let Poll::Ready(Some(item)) = Pin::new(&mut source).poll_next(&mut cx) {
            match item {
                Ok(id) => observed.delivered.push(id.to_string()),
                Err(Error::NotifyListener(message)) => {
                    observed.failure_before_hints &= observed.delivered.is_empty();
                    observed.failures.push(message);
                }
                Err(error) => observed.unexpected_errors.push(error.to_string()),
            }
        }
        observed.expected.sort();
        observed.delivered.sort();
        observed
    }

    pub(crate) fn preserves_hints_and_failure(
        failure: Option<&'static str>,
    ) -> impl Fn(&HintObservation) -> AssertionResult {
        move |observed| {
            let expected_errors = failure.into_iter().map(str::to_owned).collect::<Vec<_>>();
            if observed.delivered == observed.expected
                && observed.failures == expected_errors
                && observed.unexpected_errors.is_empty()
                && observed.failure_before_hints
            {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected every accepted ID exactly once and failure {failure:?}: {observed:?}"
                )]))
            }
        }
    }

    #[cfg(feature = "tokio")]
    pub(crate) fn has_pending_error(errors: &super::NotificationErrors) -> bool {
        errors.pending.lock().unwrap().is_some()
    }
}

#[cfg(test)]
mod tests {
    use lets_expect::lets_expect;
    use ulid::Ulid;

    use super::*;

    fn resolved_ids(ids_len: usize, has_legacy: bool) -> (String, Vec<PgTaskId>) {
        InsertEvent {
            job_type: "emails".into(),
            id: has_legacy.then(|| PgTaskId::new(Ulid::from(99_999u128))),
            ids: (0..ids_len)
                .map(|id| PgTaskId::new(Ulid::from(id as u128)))
                .collect(),
        }
        .into_ids()
    }

    fn expected_ids(ids_len: usize, has_legacy: bool) -> (String, Vec<PgTaskId>) {
        let ids = if ids_len == 0 && has_legacy {
            vec![PgTaskId::new(Ulid::from(99_999u128))]
        } else {
            (0..ids_len.min(INSERT_EVENT_IDS_CAP))
                .map(|id| PgTaskId::new(Ulid::from(id as u128)))
                .collect()
        };
        ("emails".into(), ids)
    }

    lets_expect! {
        expect(resolved_ids(ids_len, has_legacy)) as notified_task_identifiers {
            let ids_len = 3_usize;
            let has_legacy = false;
            to retains_the_queue_and_every_ordered_batch_id { equal(expected_ids(ids_len, has_legacy)) }
            when a_legacy_id_is_also_present {
                let has_legacy = true;
                to retains_only_the_ordered_batch_ids { equal(expected_ids(ids_len, has_legacy)) }
            }
            when the_batch_contains_one_id {
                let ids_len = 1_usize;
                to retains_the_single_batch_id { equal(expected_ids(ids_len, has_legacy)) }
                when a_legacy_id_is_also_present {
                    let has_legacy = true;
                    to prefers_the_single_batch_id { equal(expected_ids(ids_len, has_legacy)) }
                }
            }
            when the_batch_is_empty {
                let ids_len = 0_usize;
                to retains_the_queue_without_any_ids { equal(expected_ids(ids_len, has_legacy)) }
                when a_legacy_id_is_present {
                    let has_legacy = true;
                    to retains_the_exact_legacy_id { equal(expected_ids(ids_len, has_legacy)) }
                }
            }
            when the_batch_length_equals_the_cap {
                let ids_len = INSERT_EVENT_IDS_CAP;
                to retains_every_ordered_batch_id { equal(expected_ids(ids_len, has_legacy)) }
                when a_legacy_id_is_also_present {
                    let has_legacy = true;
                    to retains_every_batch_id_without_the_legacy_id { equal(expected_ids(ids_len, has_legacy)) }
                }
            }
            when the_batch_length_exceeds_the_cap {
                let ids_len = INSERT_EVENT_IDS_CAP + 1;
                to retains_the_exact_prefix_up_to_the_cap { equal(expected_ids(ids_len, has_legacy)) }
                when a_legacy_id_is_also_present {
                    let has_legacy = true;
                    to retains_the_capped_prefix_without_the_legacy_id { equal(expected_ids(ids_len, has_legacy)) }
                }
            }
        }
    }

    /// Parse a raw NOTIFY payload exactly as the LISTEN loops in
    /// `queries::notify` and `shared` do. Deserialization is the module's whole
    /// reason to exist: payloads arrive from any sender holding `pg_notify`, so
    /// the negative/default paths that the `let Ok(event) = from_str… else`
    /// guards depend on must be pinned here.
    fn parse(
        payload: &str,
    ) -> Result<(String, Option<PgTaskId>, Vec<PgTaskId>), serde_json::Error> {
        serde_json::from_str::<InsertEvent>(payload)
            .map(|event| (event.job_type, event.id, event.ids))
    }

    fn sample_id() -> PgTaskId {
        PgTaskId::new("01AN4Z07BY79KA1307SR9X4MV3".parse().unwrap())
    }

    lets_expect! {
        expect(parse(payload)) as notification_payload_decode {
            let payload = r#"{"job_type":"emails","ids":["01AN4Z07BY79KA1307SR9X4MV3"]}"#;

            // New statement-level shape `{job_type, ids:[…]}` deserializes.
            to accepts_the_statement_level_shape {
                be_ok_and equal(("emails".into(), None, vec![sample_id()]))
            }

            when the_payload_uses_the_legacy_single_id_shape {
                let payload = r#"{"job_type":"emails","id":"01AN4Z07BY79KA1307SR9X4MV3"}"#;
                // Legacy `{job_type, id}` still deserializes; `ids` defaults to [].
                to accepts_the_legacy_shape_and_defaults_ids_to_empty {
                    be_ok_and equal(("emails".into(), Some(sample_id()), vec![]))
                }
            }

            when only_the_job_type_is_present {
                let payload = r#"{"job_type":"emails"}"#;
                // `id`/`ids` carry `#[serde(default)]`, so a bare job_type is
                // valid and both optional fields take their empty defaults.
                to applies_the_serde_defaults_for_both_id_fields {
                    be_ok_and equal(("emails".into(), None, vec![]))
                }
            }

            when the_job_type_field_is_missing {
                let payload = r#"{"ids":["01AN4Z07BY79KA1307SR9X4MV3"]}"#;
                // `job_type` is NOT `#[serde(default)]`; without it the payload
                // must fail so the `let Ok(event) = from_str… else` guards skip
                // it instead of processing a `job_type=""` phantom event.
                to fails_to_deserialize { be_err }
            }

            when the_payload_is_the_empty_wake_up_string {
                let payload = "";
                // Legacy installations may send empty wake-ups without task ids.
                to fails_to_deserialize { be_err }
            }

            when the_payload_is_not_valid_json {
                let payload = "not json";
                to fails_to_deserialize { be_err }
            }
        }
    }
}
