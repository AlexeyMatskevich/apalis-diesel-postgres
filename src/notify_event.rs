//! NOTIFY payload type for the `apalis::job::insert` channel.
//!
//! Both the per-row legacy trigger (`{job_type, id}`) and the statement-level
//! trigger introduced in migration `20260521000001` (`{job_type, ids: [...]}`)
//! serialize into this struct via `serde(default)` on the optional fields.

use serde::Deserialize;

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
mod tests {
    use lets_expect::lets_expect;
    use ulid::Ulid;

    use super::*;

    fn task_id() -> PgTaskId {
        PgTaskId::new(Ulid::new())
    }

    /// Build an `InsertEvent` carrying `ids_len` distinct batched ids plus an
    /// optional legacy `id`. Sizing the id vector is data preparation and lives
    /// here in the fixture, never inside the spec.
    fn event(id: Option<PgTaskId>, ids_len: usize) -> InsertEvent {
        InsertEvent {
            job_type: "emails".to_string(),
            id,
            ids: (0..ids_len).map(|_| task_id()).collect(),
        }
    }

    fn resolved_id_count(id: Option<PgTaskId>, ids_len: usize) -> usize {
        event(id, ids_len).into_ids().1.len()
    }

    fn resolved_job_type(id: Option<PgTaskId>, ids_len: usize) -> String {
        event(id, ids_len).into_ids().0
    }

    lets_expect! {
        expect(resolved_id_count(id, ids_len)) {
            let id: Option<PgTaskId> = None;
            let ids_len = 3_usize;

            // Default state: a batched ids array comfortably below the cap.
            to returns_every_id { equal(3) }

            when the_ids_array_length_equals_the_cap {
                // `ids.len() > CAP` is strict, so exactly CAP is NOT truncated.
                let ids_len = INSERT_EVENT_IDS_CAP;
                to keeps_every_id_without_truncating { equal(INSERT_EVENT_IDS_CAP) }
            }

            when the_ids_array_length_is_one_above_the_cap {
                let ids_len = INSERT_EVENT_IDS_CAP + 1;
                to truncates_to_exactly_the_cap { equal(INSERT_EVENT_IDS_CAP) }
            }

            when both_an_ids_array_and_a_legacy_id_are_present {
                // Precedence: a non-empty `ids` wins and the legacy `id` is
                // dropped (result is the 3 ids, not 1 and not 4).
                let id = Some(task_id());
                to prefers_the_ids_array_and_ignores_the_legacy_id { equal(3) }
            }

            when only_a_legacy_id_is_present {
                let ids_len = 0;
                let id = Some(task_id());
                to falls_back_to_the_single_legacy_id { equal(1) }
            }

            when neither_an_ids_array_nor_a_legacy_id_is_present {
                let ids_len = 0;
                to returns_no_ids { equal(0) }
            }
        }

        expect(resolved_job_type(None, 1)) {
            to passes_the_job_type_through_unchanged { equal("emails".to_string()) }
        }
    }

    /// Parse a raw NOTIFY payload exactly as the LISTEN loops in
    /// `queries::notify` and `shared` do. Deserialization is the module's whole
    /// reason to exist: payloads arrive from any sender holding `pg_notify`, so
    /// the negative/default paths that the `let Ok(event) = from_str… else`
    /// guards depend on must be pinned here.
    fn parse(payload: &str) -> Result<InsertEvent, serde_json::Error> {
        serde_json::from_str::<InsertEvent>(payload)
    }

    lets_expect! {
        expect(parse(payload)) {
            let payload = r#"{"job_type":"emails","ids":["01AN4Z07BY79KA1307SR9X4MV3"]}"#;

            // New statement-level shape `{job_type, ids:[…]}` deserializes.
            to accepts_the_statement_level_shape {
                be_ok,
                have(as_ref().unwrap().job_type.as_str()) { equal("emails") },
                have(as_ref().unwrap().ids.len()) { equal(1) },
                have(as_ref().unwrap().id.is_none()) { be_true }
            }

            when the_payload_uses_the_legacy_single_id_shape {
                let payload = r#"{"job_type":"emails","id":"01AN4Z07BY79KA1307SR9X4MV3"}"#;
                // Legacy `{job_type, id}` still deserializes; `ids` defaults to [].
                to accepts_the_legacy_shape_and_defaults_ids_to_empty {
                    be_ok,
                    have(as_ref().unwrap().id.is_some()) { be_true },
                    have(as_ref().unwrap().ids.len()) { equal(0) }
                }
            }

            when only_the_job_type_is_present {
                let payload = r#"{"job_type":"emails"}"#;
                // `id`/`ids` carry `#[serde(default)]`, so a bare job_type is
                // valid and both optional fields take their empty defaults.
                to applies_the_serde_defaults_for_both_id_fields {
                    be_ok,
                    have(as_ref().unwrap().id.is_none()) { be_true },
                    have(as_ref().unwrap().ids.len()) { equal(0) }
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
                // Both Drop impls send an empty payload purely as a wake-up; it
                // must fail to parse so it is never mistaken for a real event.
                to fails_to_deserialize { be_err }
            }

            when the_payload_is_not_valid_json {
                let payload = "not json";
                to fails_to_deserialize { be_err }
            }
        }
    }
}
