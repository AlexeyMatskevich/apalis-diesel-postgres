//! Lifecycle helpers — schema setup, schema verification, and snapshot
//! refresh. Kept in a dedicated module so `lib.rs` stays focused on the
//! `PostgresStorage` public facade.

use crate::{Error, PgPool, queries};

/// Refresh the `apalis.queue_stats_snapshot` materialized view.
///
/// `list_queues` and `metrics` run unbounded `FILTER` aggregates over the
/// whole `apalis.jobs` table on every call; on busy queues this is O(rows)
/// per dashboard hit. Calling this periodically (e.g. once a minute from an
/// admin task) lets dashboards read pre-aggregated rows from the snapshot
/// view instead. Uses `REFRESH MATERIALIZED VIEW CONCURRENTLY`, so readers
/// of the view are never blocked.
pub async fn refresh_queue_stats_snapshot(pool: &PgPool) -> Result<(), Error> {
    queries::refresh_queue_stats_snapshot(pool.clone()).await
}

/// Run the embedded Apalis-compatible migrations.
///
/// Call this before workers use the storage. The function consumes one pooled
/// connection while migrations run.
pub async fn setup(pool: &PgPool) -> Result<(), Error> {
    queries::migrations::setup(pool.clone()).await
}

/// Verify that every embedded migration has been applied to the target
/// database. Useful as a boot-time guard for deployments that run migrations
/// out-of-band (CI step, ops tooling) rather than calling [`setup`] from the
/// application process: a missing migration is surfaced here as
/// [`Error::Migration`] instead of as opaque `Database` errors against
/// columns or tables that runtime queries assume exist.
pub async fn verify_schema(pool: &PgPool) -> Result<(), Error> {
    queries::migrations::verify_schema(pool.clone()).await
}
