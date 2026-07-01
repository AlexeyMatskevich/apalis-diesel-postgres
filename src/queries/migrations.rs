use std::panic::{AssertUnwindSafe, catch_unwind};

use diesel::{PgConnection, QueryableByName, RunQueryDsl, sql_query};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::{Error, PgPool, queries::with_conn};

/// Embedded Apalis-compatible migrations bundled into the crate binary.
///
/// [`setup`](crate::setup) applies these for you; this constant is exposed for
/// callers that run migrations out-of-band through their own
/// [`diesel_migrations`] harness (a dedicated CI/ops step, a shared migration
/// runner) instead of from the application process. After such a run,
/// [`verify_schema`](crate::verify_schema) confirms the database is up to date.
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
/// Report whether the *current backend* holds the migration advisory lock. A
/// two-integer `pg_advisory_lock(k1, k2)` surfaces in `pg_locks` as an
/// `advisory` row with `classid = k1`, `objid = k2`, `objsubid = 2`; scoping to
/// `pg_backend_pid()` and the current database ignores locks held by other
/// sessions or in other databases. Both `hashtext` keys are positive for these
/// identifiers, so the `::oid` casts are exact.
///
/// `pg_locks` collapses every reentrant hold of the same key by the same backend
/// into a *single* row, so this reports presence (held / not held), not depth —
/// which is why the drain below unlocks until the row disappears rather than a
/// fixed number of times.
const MIGRATION_LOCK_HELD: &str = "\
     SELECT count(*)::bigint AS held FROM pg_locks \
      WHERE locktype = 'advisory' \
        AND classid = hashtext('apalis_diesel_postgres')::oid \
        AND objid = hashtext('migrations')::oid \
        AND objsubid = 2 \
        AND pid = pg_backend_pid() \
        AND database = (SELECT oid FROM pg_database WHERE datname = current_database())";

#[derive(QueryableByName)]
struct LockHeld {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    held: i64,
}

fn migration_lock_held(conn: &mut PgConnection) -> Result<bool, Error> {
    let rows = sql_query(MIGRATION_LOCK_HELD)
        .load::<LockHeld>(conn)
        .map_err(Error::database("inspecting the migration advisory lock"))?
        .into_iter()
        .next()
        .map(|row| row.held)
        .ok_or_else(|| Error::Migration("advisory-lock presence query returned no row".into()))?;
    Ok(rows > 0)
}

/// Fully release the migration advisory lock on `conn`, draining **every** hold
/// the backend has on it and confirming none remain.
///
/// `setup()` acquires the lock exactly once, so a healthy connection holds one
/// copy here and a single unlock suffices. It can hold *more*: PostgreSQL's
/// session advisory locks are reentrant, and a prior `setup()` on this pooled
/// connection whose release failed while the session stayed alive would leave a
/// residual hold that this run's acquire re-entered (depth 2). A single
/// `pg_advisory_unlock` returns `true` yet leaves that extra copy, so the
/// connection would return to the pool still owning the lock — letting the next
/// `setup()` re-enter it without ever blocking and silently defeating the
/// serialization the lock exists to provide. Drain to zero instead.
///
/// `pg_locks` does not expose the reentrancy depth (see [`MIGRATION_LOCK_HELD`]),
/// so unlock while the lock is still reported held rather than a fixed number of
/// times. Checking presence *before* each unlock keeps every `pg_advisory_unlock`
/// on a lock the backend actually holds, avoiding the "lock is not held" server
/// warning an unlock-until-`false` loop would emit on the final call.
fn release_migration_lock(conn: &mut PgConnection) -> Result<(), Error> {
    // Capped so a logic error can never spin forever. The depth is 1 on a clean
    // connection and only exceeds it after a leak, which this drain then clears;
    // each iteration releases one held level, so the loop ends in `depth` steps.
    const MAX_DRAIN: usize = 1024;
    let mut drained = 0usize;
    while migration_lock_held(conn)? {
        sql_query(RELEASE_MIGRATION_LOCK)
            .execute(conn)
            .map_err(Error::database("releasing the migration advisory lock"))?;
        drained += 1;
        if drained >= MAX_DRAIN {
            return Err(Error::Migration(
                "migration advisory lock could not be drained".into(),
            ));
        }
    }
    if drained == 0 {
        // We acquired the lock above, so the backend must have held it here.
        // Zero copies means the serialization invariant was broken out of band;
        // surface it rather than report a clean run.
        return Err(Error::Migration(
            "migration advisory lock was not held at release time".into(),
        ));
    }
    Ok(())
}

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
        // Release the lock on every path, including a panic inside the migration
        // runner: catch the unwind, drain the lock, then resume unwinding. r2d2
        // returns this connection to the pool with its backing session intact,
        // so a leaked session lock would persist and let the next `setup()`
        // re-enter it (higher lock count) without ever blocking — silently
        // defeating the serialization. `release_migration_lock` drains every
        // hold and verifies none remain.
        let migrated = catch_unwind(AssertUnwindSafe(|| {
            conn.run_pending_migrations(MIGRATIONS).map(|_| ())
        }));
        let released = release_migration_lock(conn);
        match migrated {
            Err(panic) => std::panic::resume_unwind(panic),
            // A migration failure is the more useful error to surface; the lock
            // was still drained above.
            Ok(Err(error)) => Err(Error::Migration(error)),
            // On a clean run, surface any failure to drain or verify the lock.
            Ok(Ok(())) => released,
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
