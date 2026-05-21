use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::{Error, PgPool, queries::with_conn};

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

pub(crate) async fn setup(pool: PgPool) -> Result<(), Error> {
    with_conn(pool, |conn| {
        conn.run_pending_migrations(MIGRATIONS)
            .map(|_| ())
            .map_err(Error::Migration)
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
