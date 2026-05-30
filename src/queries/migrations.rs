use std::panic::{AssertUnwindSafe, catch_unwind};

use diesel::{RunQueryDsl, sql_query};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::{Error, PgPool, queries::with_conn};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

/// Acquire/release a **session-level** advisory lock that serializes concurrent
/// `setup()` calls. It must be session-scoped (`pg_advisory_lock`), not
/// transaction-scoped like the worker-registration locks in
/// `src/queries/admin.rs` / `src/queries/worker.rs`: diesel runs each migration
/// in its own transaction, so a `pg_advisory_xact_lock` would release after the
/// first migration and stop covering the rest of the run. The two-part
/// `hashtext` key mirrors the advisory-lock style used elsewhere in the crate.
const ACQUIRE_MIGRATION_LOCK: &str =
    "SELECT pg_advisory_lock(hashtext('apalis_diesel_postgres'), hashtext('migrations'))";
const RELEASE_MIGRATION_LOCK: &str =
    "SELECT pg_advisory_unlock(hashtext('apalis_diesel_postgres'), hashtext('migrations'))";

pub(crate) async fn setup(pool: PgPool) -> Result<(), Error> {
    with_conn(pool, |conn| {
        // Serialize concurrent `setup()` — e.g. several application replicas
        // booting against a fresh database at once. Without this lock both
        // callers observe migration `00000000000000` as pending and race
        // through its non-idempotent `CREATE SCHEMA` / `CREATE FUNCTION` /
        // `CREATE TRIGGER` DDL (and the `__diesel_schema_migrations` version
        // insert), so all-but-one crash with a duplicate-key `Error::Migration`.
        sql_query(ACQUIRE_MIGRATION_LOCK)
            .execute(conn)
            .map_err(Error::database("acquiring the migration advisory lock"))?;
        // r2d2 returns this connection to the pool with its backing session
        // intact, so a leaked session lock would persist and let the next
        // `setup()` re-enter it (lock count 2) without ever blocking — silently
        // defeating the serialization. Release on every path, including a panic
        // inside the migration runner: catch the unwind, release the lock, then
        // resume unwinding.
        let migrated = catch_unwind(AssertUnwindSafe(|| {
            conn.run_pending_migrations(MIGRATIONS).map(|_| ())
        }));
        let released = sql_query(RELEASE_MIGRATION_LOCK).execute(conn);
        match migrated {
            Err(panic) => std::panic::resume_unwind(panic),
            // A migration failure is the more useful error to surface; the lock
            // was still released above.
            Ok(Err(error)) => Err(Error::Migration(error)),
            Ok(Ok(())) => released
                .map(|_| ())
                .map_err(Error::database("releasing the migration advisory lock")),
        }
    })
    .await
}

/// Check that every embedded migration has already been recorded as applied.
///
/// If `setup` was forgotten, runtime queries (heartbeat, dequeue, etc.) would
/// otherwise fail mid-flight with opaque `Database` errors against columns or
/// tables the running code expects but the schema does not yet have. This
/// helper surfaces that mismatch up front as a single `Error::Migration` so
/// applications can fail fast on boot.
pub(crate) async fn verify_schema(pool: PgPool) -> Result<(), Error> {
    with_conn(pool, |conn| {
        let pending = conn
            .has_pending_migration(MIGRATIONS)
            .map_err(Error::Migration)?;
        if pending {
            Err(Error::Migration(
                "embedded migrations have not been applied — call `apalis_diesel_postgres::setup` first".into(),
            ))
        } else {
            Ok(())
        }
    })
    .await
}
