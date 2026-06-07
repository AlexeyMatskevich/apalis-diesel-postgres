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
}
