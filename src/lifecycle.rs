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
/// view instead. Prefers `REFRESH MATERIALIZED VIEW CONCURRENTLY` so readers
/// of the view are not blocked; the very first refresh uses a blocking
/// `REFRESH` instead, because PostgreSQL rejects `CONCURRENTLY` on a
/// materialized view that has never been populated.
///
/// # Errors
/// - [`Error::Pool`] if a pooled connection cannot be acquired.
/// - [`Error::Database`] if the refresh fails — most often because the
///   snapshot view does not exist yet (run [`setup`] first).
/// - [`Error::Blocking`] if the blocking task carrying the query fails to
///   complete (a panic in the worker thread, or runtime shutdown).
pub async fn refresh_queue_stats_snapshot(pool: &PgPool) -> Result<(), Error> {
    queries::refresh_queue_stats_snapshot(pool.clone()).await
}

/// Run the embedded Apalis-compatible migrations.
///
/// Call this before workers use the storage. The function consumes one pooled
/// connection while migrations run. The whole series is one transaction under
/// an advisory transaction lock. Migration history is owned by the trusted
/// `apalis_diesel_postgres` schema; application journals are left alone.
/// Existing supported schemas are validated before history adoption. See
/// `docs/upgrading.md` for supported generations and external FK restrictions.
///
/// # Errors
/// - [`Error::Pool`] if a pooled connection cannot be acquired.
/// - [`Error::Database`] if the migration advisory lock cannot be acquired.
/// - [`Error::Migration`] if a migration fails, an existing schema is not
///   supported, or the resulting required structure is incompatible.
/// - [`Error::Blocking`] if the blocking task carrying the migration run fails
///   to complete (a panic in the worker thread, or runtime shutdown).
pub async fn setup(pool: &PgPool) -> Result<(), Error> {
    queries::migrations::setup(pool.clone()).await
}

/// Verify the private migration history and required schema structure.
/// This checks columns, types, nullability, keys, check constraints, indexes,
/// trigger registration, and function signatures/security settings. It does
/// not checksum arbitrary function or view bodies. Use this boot-time guard
/// for deployments that run migrations
/// out-of-band (CI step, ops tooling) rather than calling [`setup`] from the
/// application process: a missing migration is surfaced here as
/// [`Error::Migration`] instead of as opaque `Database` errors against
/// columns or tables that runtime queries assume exist.
///
/// # Errors
/// - [`Error::Pool`] if a pooled connection cannot be acquired.
/// - [`Error::Migration`] if private migration history is missing or unreadable,
///   a migration is pending, or required objects are structurally incompatible.
/// - [`Error::Blocking`] if the blocking task carrying the check fails to
///   complete (a panic in the worker thread, or runtime shutdown).
pub async fn verify_schema(pool: &PgPool) -> Result<(), Error> {
    queries::migrations::verify_schema(pool.clone()).await
}
