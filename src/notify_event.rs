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
