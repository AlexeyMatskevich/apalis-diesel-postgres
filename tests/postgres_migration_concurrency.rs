//! Regression spec for concurrent `setup()`.
//!
//! `apalis_diesel_postgres::setup` runs the embedded migrations. Several
//! application replicas booting against a *fresh* database at once each call
//! `setup()` concurrently; without the session-level advisory lock added to
//! `src/queries/migrations.rs`, all-but-one racer crashes — migration `0`'s
//! non-idempotent `CREATE SCHEMA`/`CREATE FUNCTION`/`CREATE TRIGGER` DDL and the
//! `__diesel_schema_migrations` version insert collide on PostgreSQL catalog
//! unique indexes, surfacing as `Error::Migration` (`duplicate key ...`).
//!
//! This spec creates a throwaway database, fires N concurrent `setup()` calls
//! against it from cold, and asserts every one succeeds. It is isolated from the
//! shared test database so it never disturbs the other integration suites.
//!
//! Gating: skips when `DATABASE_URL` is unset (like the other suites) and also
//! when the connecting role lacks `CREATEDB` (the throwaway database cannot be
//! provisioned). Set `APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE=1` to turn the
//! missing-`DATABASE_URL` skip into a hard failure.

#![cfg(feature = "tokio")]

mod support;

use apalis_diesel_postgres::{build_pool_with, setup};
use diesel::{Connection, PgConnection, RunQueryDsl, sql_query};
use lets_expect::{AssertionError, AssertionResult, *};
use ulid::Ulid;

/// Number of replicas racing `setup()` against the cold database.
const RACERS: usize = 8;

#[derive(Debug)]
enum Outcome {
    Skipped,
    Completed(Vec<Result<(), String>>),
}

/// Swap the database name in a libpq URL, preserving scheme/host/port and any
/// `?query` parameters. `postgres://h:5432/old?sslmode=disable` becomes
/// `postgres://h:5432/<new>?sslmode=disable`.
fn with_database_name(url: &str, database: &str) -> Result<String, String> {
    let scheme_end = url.find("://").ok_or("database URL has no scheme")? + 3;
    let rest = &url[scheme_end..];
    let path_start = rest.find('/').ok_or("database URL has no path")?;
    let authority = &rest[..path_start];
    let after_path = &rest[path_start + 1..];
    let query = after_path.find('?').map(|q| &after_path[q..]).unwrap_or("");
    Ok(format!(
        "{}{authority}/{database}{query}",
        &url[..scheme_end]
    ))
}

fn maintenance_conn(url: &str) -> Result<PgConnection, String> {
    PgConnection::establish(url).map_err(|error| error.to_string())
}

/// `true` when the current role may `CREATE DATABASE`.
fn can_create_database(conn: &mut PgConnection) -> Result<bool, String> {
    #[derive(diesel::QueryableByName)]
    struct Flag {
        #[diesel(sql_type = diesel::sql_types::Bool)]
        rolcreatedb: bool,
    }
    sql_query("SELECT rolcreatedb FROM pg_roles WHERE rolname = current_user")
        .load::<Flag>(conn)
        .map_err(|error| error.to_string())?
        .into_iter()
        .next()
        .map(|row| row.rolcreatedb)
        .ok_or_else(|| "current_user not found in pg_roles".to_owned())
}

/// A provisioned throwaway database, isolated from the shared test database so
/// these specs never disturb the other suites. `url` connects to the throwaway
/// database; `maintenance_url` connects to the original database and is used to
/// `DROP` it during teardown.
struct ColdDb {
    name: String,
    url: String,
    maintenance_url: String,
}

/// Provision a cold throwaway database named `<prefix>_<ulid>`. DDL identifiers
/// cannot be bound, so the generated Ulid is interpolated; its Crockford-base32
/// charset is injection-safe. Returns `Ok(None)` when the environment cannot
/// provide one — `DATABASE_URL` unset, the role lacks `CREATEDB`, the URL cannot
/// be rewritten, or (a libpq `?dbname=` safety guard) the rewritten URL resolves
/// back to the main database. The caller must
/// `drop_temp_db(&db.maintenance_url, &db.name)` on every path.
async fn provision_cold_db(prefix: &str) -> Result<Option<ColdDb>, String> {
    let Some(maintenance_url) = support::database_url_or_skip()? else {
        return Ok(None);
    };

    let name = format!("{prefix}_{}", Ulid::new().to_string().to_lowercase());
    let provisioned = {
        let create_url = maintenance_url.clone();
        let name = name.clone();
        tokio::task::spawn_blocking(move || -> Result<bool, String> {
            let mut conn = maintenance_conn(&create_url)?;
            if !can_create_database(&mut conn)? {
                return Ok(false);
            }
            sql_query(format!("CREATE DATABASE \"{name}\""))
                .execute(&mut conn)
                .map_err(|error| error.to_string())?;
            Ok(true)
        })
        .await
        .map_err(|error| error.to_string())??
    };
    if !provisioned {
        return Ok(None);
    }

    let url = match with_database_name(&maintenance_url, &name) {
        Ok(url) => url,
        Err(_) => {
            drop_temp_db(&maintenance_url, &name).await;
            return Ok(None);
        }
    };

    // SAFETY: confirm `url` resolves to the throwaway database before anyone
    // runs setup() against it. A `DATABASE_URL` carrying a `?dbname=` query
    // parameter (or other libpq form) can resolve back to the main database
    // despite the swapped path; refuse to proceed if so.
    let on_temp_db = {
        let url = url.clone();
        let expected = name.clone();
        tokio::task::spawn_blocking(move || -> Result<bool, String> {
            let mut conn = maintenance_conn(&url)?;
            #[derive(diesel::QueryableByName)]
            struct Db {
                #[diesel(sql_type = diesel::sql_types::Text)]
                db: String,
            }
            let actual = sql_query("SELECT current_database()::text AS db")
                .load::<Db>(&mut conn)
                .map_err(|e| e.to_string())?
                .into_iter()
                .next()
                .map(|row| row.db)
                .ok_or_else(|| "current_database() returned no row".to_owned())?;
            Ok(actual == expected)
        })
        .await
        .map_err(|e| e.to_string())??
    };
    if !on_temp_db {
        drop_temp_db(&maintenance_url, &name).await;
        return Ok(None);
    }

    Ok(Some(ColdDb {
        name,
        url,
        maintenance_url,
    }))
}

/// Mirror of the crate's `ACQUIRE_MIGRATION_LOCK` (`src/queries/migrations.rs`),
/// kept in lock-step so the pre-leak in `run_setup_drains_a_leaked_lock` targets
/// the exact key `setup()` uses.
const ACQUIRE_MIGRATION_LOCK: &str =
    "SELECT pg_advisory_lock(hashtext('apalis_diesel_postgres'), hashtext('migrations'))";
/// Mirror of the crate's `RELEASE_MIGRATION_LOCK`, used to drain the advisory lock
/// in `run_panic_unwinds_and_leaves_lock_clean` — the panic-safety reproduction —
/// after the injected panic, exactly as `setup()`'s panic-handling body does
/// before it re-raises.
const RELEASE_MIGRATION_LOCK: &str =
    "SELECT pg_advisory_unlock(hashtext('apalis_diesel_postgres'), hashtext('migrations'))";
/// Mirror of the crate's `MIGRATION_LOCK_HELD`: report whether the *current
/// backend* still holds the migration advisory lock. Scoped to `pg_backend_pid()`
/// so it observes the connection under test, matching the drain loop in
/// `release_migration_lock`, which polls presence before each unlock.
const MIGRATION_LOCK_HELD_ON_CONN: &str = "\
     SELECT count(*)::bigint AS n FROM pg_locks \
      WHERE locktype = 'advisory' \
        AND classid = hashtext('apalis_diesel_postgres')::oid \
        AND objid = hashtext('migrations')::oid \
        AND objsubid = 2 \
        AND pid = pg_backend_pid() \
        AND database = (SELECT oid FROM pg_database WHERE datname = current_database())";

/// `true` when the backend behind `conn` still holds the migration advisory lock.
/// Mirrors the crate's `migration_lock_held` presence check.
fn migration_lock_held_on_conn(conn: &mut PgConnection) -> Result<bool, String> {
    #[derive(diesel::QueryableByName)]
    struct Count {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        n: i64,
    }
    sql_query(MIGRATION_LOCK_HELD_ON_CONN)
        .load::<Count>(conn)
        .map_err(|error| error.to_string())?
        .into_iter()
        .next()
        .map(|row| row.n > 0)
        .ok_or_else(|| "advisory-lock presence query returned no row".to_owned())
}

/// Count copies of the migration advisory lock held in the *current* database,
/// across all backends. Mirrors the crate's key derivation (a two-integer
/// advisory lock on `hashtext('apalis_diesel_postgres')` / `hashtext('migrations')`
/// surfaces as an `advisory` row with `objsubid = 2`). Scoping to that key and
/// the current (throwaway) database — rather than counting every advisory lock
/// in the cluster — keeps the assertion robust under concurrent test runners and
/// unrelated advisory locks held elsewhere. There is no `pid` filter on purpose:
/// the lock under test lives on the pool's backend, a different session than the
/// one running this query.
const MIGRATION_LOCK_HELD_IN_DB: &str = "\
     SELECT count(*)::bigint AS n FROM pg_locks \
      WHERE locktype = 'advisory' \
        AND classid = hashtext('apalis_diesel_postgres')::oid \
        AND objid = hashtext('migrations')::oid \
        AND objsubid = 2 \
        AND database = (SELECT oid FROM pg_database WHERE datname = current_database())";

/// Count the migration advisory locks held in the throwaway database reachable
/// through `url`, from a *separate* maintenance session, so a lock left on the
/// setup pool's (still-open) connection is visible.
async fn migration_locks_held_in_db(url: &str) -> Result<i64, String> {
    let url = url.to_owned();
    tokio::task::spawn_blocking(move || -> Result<i64, String> {
        #[derive(diesel::QueryableByName)]
        struct Count {
            #[diesel(sql_type = diesel::sql_types::BigInt)]
            n: i64,
        }
        let mut conn = maintenance_conn(&url)?;
        sql_query(MIGRATION_LOCK_HELD_IN_DB)
            .load::<Count>(&mut conn)
            .map_err(|error| error.to_string())?
            .into_iter()
            .next()
            .map(|row| row.n)
            .ok_or_else(|| "pg_locks count returned no row".to_owned())
    })
    .await
    .map_err(|error| error.to_string())?
}

async fn run_concurrent_setup() -> Result<Outcome, String> {
    let Some(cold) = provision_cold_db("apalis_mig_race").await? else {
        return Ok(Outcome::Skipped);
    };

    // Fire N concurrent `setup()` calls against the cold database. Keep each
    // pool tiny so the regression test itself cannot exhaust connections.
    let outcomes = futures::future::join_all((0..RACERS).map(|_| {
        let temp_url = cold.url.clone();
        async move {
            let pool = build_pool_with(&temp_url, |builder| builder.max_size(2).min_idle(Some(0)))
                .map_err(|error| error.to_string())?;
            let result = setup(&pool).await.map_err(|error| error.to_string());
            drop(pool); // close this racer's sessions before the database is dropped
            result
        }
    }))
    .await;

    // Teardown runs on every path after CREATE so an error cannot leak the DB.
    drop_temp_db(&cold.maintenance_url, &cold.name).await;
    Ok(Outcome::Completed(outcomes))
}

/// Outcome of the contended-cleanup scenario: whether every racing `setup()`
/// succeeded, and how many copies of the migration advisory lock remained held in
/// the throwaway database once every racer returned but *before* the pool was
/// dropped.
#[derive(Debug)]
enum ContendedOutcome {
    Skipped,
    Completed {
        results: Vec<Result<(), String>>,
        advisory_locks_held: i64,
    },
}

/// Race N concurrent `setup()` calls that genuinely contend for the migration
/// advisory lock at the *PostgreSQL* level — each racer on its own backend
/// connection — then assert the lock is drained to zero on every one of those
/// backends while they are all still checked out.
///
/// A single `max_size(1)` pool would serialize the racers inside r2d2 (they'd
/// queue for the one connection and never overlap on the server), so they'd
/// contend for the r2d2 slot, not the Postgres advisory lock. Instead this uses a
/// pool sized for every racer at once (`max_size(RACERS)`) and *pre-warms* it to
/// `RACERS` distinct live backends before any `setup()` runs. With every racer
/// holding a separate backend, the advisory lock is the only thing serializing
/// them: exactly one holds `pg_advisory_lock` while the rest block on the server,
/// which is the real contention this scenario exists to exercise.
///
/// The advisory-lock count is taken from a separate maintenance session *before*
/// the pool is dropped and while all `RACERS` connections are still idle in the
/// pool, so a hold that any racer's release failed to drain would still be sitting
/// on its backend and be counted. Asserting cleanup here — not just "all
/// succeeded" — mirrors the single-caller and reentrant scenarios, which already
/// check the lock is drained to zero.
async fn run_contended_setup_releases_lock() -> Result<ContendedOutcome, String> {
    let Some(cold) = provision_cold_db("apalis_mig_contend").await? else {
        return Ok(ContendedOutcome::Skipped);
    };

    let outcome = async {
        // Room for every racer to hold its own backend simultaneously, so they
        // contend on the Postgres advisory lock rather than on an r2d2 slot.
        let pool = build_pool_with(&cold.url, |builder| {
            builder.max_size(RACERS as u32).min_idle(Some(0))
        })
        .map_err(|error| error.to_string())?;

        // Pre-warm to RACERS distinct live backends: check out every connection at
        // once and hold them all before returning them to the pool. r2d2 hands out
        // a fresh backend for each concurrent checkout, so this guarantees the pool
        // is backed by RACERS separate PostgreSQL sessions. Without this, r2d2 can
        // satisfy sequential `setup()` calls by reusing one warmed connection and
        // the racers would never actually overlap on the server.
        {
            let pool = pool.clone();
            tokio::task::spawn_blocking(move || -> Result<(), String> {
                let mut held = Vec::with_capacity(RACERS);
                for _ in 0..RACERS {
                    held.push(pool.get().map_err(|error| error.to_string())?);
                }
                drop(held); // all RACERS backends now live and returned to the pool
                Ok(())
            })
            .await
            .map_err(|error| error.to_string())??;
        }

        let results = futures::future::join_all((0..RACERS).map(|_| {
            let pool = pool.clone();
            async move { setup(&pool).await.map_err(|error| error.to_string()) }
        }))
        .await;
        // Query while the pool (and its RACERS idle connections) is still open, so
        // a lock any racer left on its backend under contention shows up.
        let advisory_locks_held = migration_locks_held_in_db(&cold.url).await?;
        drop(pool);
        Ok::<ContendedOutcome, String>(ContendedOutcome::Completed {
            results,
            advisory_locks_held,
        })
    }
    .await;

    drop_temp_db(&cold.maintenance_url, &cold.name).await;
    outcome
}

/// Best-effort drop of a throwaway database (`WITH (FORCE)` terminates lingering
/// sessions). A leaked test database is harmless but undesirable.
async fn drop_temp_db(maintenance_url: &str, db_name: &str) {
    let maintenance_url = maintenance_url.to_owned();
    let db_name = db_name.to_owned();
    let _ = tokio::task::spawn_blocking(move || {
        if let Ok(mut conn) = maintenance_conn(&maintenance_url) {
            let _ = sql_query(format!(
                "DROP DATABASE IF EXISTS \"{db_name}\" WITH (FORCE)"
            ))
            .execute(&mut conn);
        }
    })
    .await;
}

fn all_setups_succeed() -> impl Fn(&Result<Outcome, String>) -> AssertionResult {
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "concurrent setup scenario failed to run: {error}"
        )])),
        Ok(Outcome::Skipped) => Ok(()),
        Ok(Outcome::Completed(outcomes)) => {
            let failures: Vec<String> = outcomes
                .iter()
                .enumerate()
                .filter_map(|(i, r)| r.as_ref().err().map(|e| format!("racer {i}: {e}")))
                .collect();
            if failures.is_empty() {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected all {} concurrent setup() calls to succeed, {} failed:\n{}",
                    outcomes.len(),
                    failures.len(),
                    failures.join("\n")
                )]))
            }
        }
    }
}

/// Run `setup()` once against a cold throwaway database and report how many
/// copies of the migration advisory lock remain held afterwards. The lock is
/// session-scoped and released inside `setup()`; were the release skipped it
/// would linger on the pooled (still-open) connection. Querying from a
/// *separate* maintenance session while the pool is alive detects such a leak.
/// Returns `None` when the environment cannot provision a database.
async fn run_setup_releases_lock() -> Result<Option<i64>, String> {
    let Some(cold) = provision_cold_db("apalis_mig_release").await? else {
        return Ok(None);
    };

    let count = async {
        let pool = build_pool_with(&cold.url, |builder| builder.max_size(2).min_idle(Some(0)))
            .map_err(|error| error.to_string())?;
        setup(&pool).await.map_err(|error| error.to_string())?;
        // Query while the setup pool (and its now-idle connection) is still open,
        // so a lock left on that connection shows up.
        let held = migration_locks_held_in_db(&cold.url).await?;
        drop(pool);
        Ok::<i64, String>(held)
    }
    .await;

    drop_temp_db(&cold.maintenance_url, &cold.name).await;
    count.map(Some)
}

/// Reproduce and guard against a *reentrant* advisory-lock leak. PostgreSQL
/// session advisory locks are reentrant: if a connection handed to `setup()`
/// already holds the migration lock — as it would after a prior `setup()` whose
/// release failed while the session stayed alive — this run's acquire bumps the
/// hold count to 2, and a single `pg_advisory_unlock` only brings it back to 1.
/// The connection then returns to the pool still owning the lock, letting the
/// next `setup()` re-enter without blocking and defeating the serialization.
///
/// Pin `setup()` to one connection with a `max_size(1)` pool, pre-acquire the
/// migration lock on it (simulating the leak) and return it to the pool, then
/// run `setup()`. A correct `setup()` drains every hold to zero; the pre-fix
/// single release left one behind. Reports how many copies remain, queried from
/// a separate session while the pool is alive. Returns `None` when the
/// environment cannot provision a database.
async fn run_setup_drains_a_leaked_lock() -> Result<Option<i64>, String> {
    let Some(cold) = provision_cold_db("apalis_mig_leak").await? else {
        return Ok(None);
    };

    let count = async {
        let pool = build_pool_with(&cold.url, |builder| builder.max_size(1).min_idle(Some(0)))
            .map_err(|error| error.to_string())?;

        // Pre-leak: acquire the migration lock on the pool's single connection
        // and return it to the pool with the session-level lock still held, so
        // the following `setup()` checks out a connection that already owns it.
        {
            let pool = pool.clone();
            tokio::task::spawn_blocking(move || -> Result<(), String> {
                let mut conn = pool.get().map_err(|error| error.to_string())?;
                sql_query(ACQUIRE_MIGRATION_LOCK)
                    .execute(&mut conn)
                    .map_err(|error| error.to_string())?;
                Ok(()) // conn returns to the pool here, session lock still held
            })
            .await
            .map_err(|error| error.to_string())??;
        }

        setup(&pool).await.map_err(|error| error.to_string())?;
        let held = migration_locks_held_in_db(&cold.url).await?;
        drop(pool);
        Ok::<i64, String>(held)
    }
    .await;

    drop_temp_db(&cold.maintenance_url, &cold.name).await;
    count.map(Some)
}

/// Outcome of a `setup()` run that is expected to fail inside the migration
/// runner. `error` is the surfaced `setup()` error rendered as a string;
/// `advisory_locks_held` is how many copies of the migration advisory lock remain
/// held in the throwaway database afterwards, queried from a separate maintenance
/// session while the setup pool is still alive.
#[derive(Debug)]
struct FailedSetup {
    error: String,
    advisory_locks_held: i64,
}

/// Force the `Ok(Err(error))` branch of `setup()` — a migration that *fails*
/// (rather than panics) inside `run_pending_migrations` — and observe both what
/// error surfaces and whether the advisory lock is still drained.
///
/// Pre-create `apalis.jobs` as a stub table missing the columns migration
/// `00000000000000` indexes. The migration's `CREATE SCHEMA IF NOT EXISTS` /
/// `CREATE TABLE IF NOT EXISTS` become no-ops, but `CREATE INDEX IF NOT EXISTS
/// jobs_dequeue_idx ON apalis.jobs(job_type, ...)` then fails with a
/// "column does not exist" error, so `run_pending_migrations` returns `Ok(Err(_))`.
///
/// `setup()` must surface that migration failure as `Error::Migration` — not a
/// release/lock error — and must still drain the advisory lock to zero on this
/// failure path (the unconditional `release_migration_lock` before the match).
/// Returns `None` when the environment cannot provision a database.
async fn run_setup_against_incompatible_schema() -> Result<Option<FailedSetup>, String> {
    let Some(cold) = provision_cold_db("apalis_mig_fail").await? else {
        return Ok(None);
    };

    let outcome = async {
        // Seed a conflicting `apalis.jobs` table so migration 0's index DDL fails.
        {
            let url = cold.url.clone();
            tokio::task::spawn_blocking(move || -> Result<(), String> {
                let mut conn = maintenance_conn(&url)?;
                sql_query("CREATE SCHEMA IF NOT EXISTS apalis")
                    .execute(&mut conn)
                    .map_err(|error| error.to_string())?;
                sql_query("CREATE TABLE apalis.jobs (id TEXT NOT NULL PRIMARY KEY)")
                    .execute(&mut conn)
                    .map_err(|error| error.to_string())?;
                Ok(())
            })
            .await
            .map_err(|error| error.to_string())??;
        }

        let pool = build_pool_with(&cold.url, |builder| builder.max_size(2).min_idle(Some(0)))
            .map_err(|error| error.to_string())?;
        let error = match setup(&pool).await {
            Ok(()) => {
                return Err("expected setup() to fail against the incompatible schema, \
                    but it succeeded"
                    .to_owned());
            }
            Err(error) => error.to_string(),
        };
        // Query while the setup pool (and its now-idle connection) is still open,
        // so a lock left on that connection shows up.
        let advisory_locks_held = migration_locks_held_in_db(&cold.url).await?;
        drop(pool);
        Ok::<FailedSetup, String>(FailedSetup {
            error,
            advisory_locks_held,
        })
    }
    .await;

    drop_temp_db(&cold.maintenance_url, &cold.name).await;
    outcome.map(Some)
}

/// Outcome of the panic-safety scenario.
///
/// * `panic_propagated` — the injected panic unwound out of `spawn_blocking`
///   (`JoinError::is_panic`) rather than being swallowed, matching `setup()`'s
///   `resume_unwind`.
/// * `locks_after_unwind` — copies of the migration advisory lock still held in
///   the throwaway database *after* the panic unwound and the connection went back
///   to the pool, observed from a **separate** maintenance session (the same
///   external observation that detects a real `setup()` leak).
/// * `subsequent_setup` — result of running the *real* production `setup()` on the
///   same pool afterwards: it must succeed, proving the connection came back
///   genuinely reusable and lock-clean, not merely reporting a local counter of 0.
#[derive(Debug)]
struct PanicSafety {
    panic_propagated: bool,
    locks_after_unwind: i64,
    subsequent_setup: Result<(), String>,
}

/// Exercise the panic-safety contract of `setup()`'s migration body
/// (`src/queries/migrations.rs:129-134`), which catches an unwind from the
/// migration runner, drains the advisory lock, then `resume_unwind`s so r2d2 never
/// returns a connection to the pool carrying a leaked session lock.
///
/// A genuine panic inside diesel's migration runner cannot be injected through the
/// public `setup()` API (diesel surfaces SQL failures as `Err`, never `panic!`),
/// so the acquire + panic + drain + resume ordering is driven directly on a pooled
/// connection. The distinguishing move from a self-checking mirror: the outcome is
/// **not** read from the copied drain loop's own counter (which would be a
/// tautology). Instead —
///
/// 1. the panic is left to unwind the whole `spawn_blocking` closure, so the
///    connection returns to the pool exactly as it would after `setup()`'s
///    `resume_unwind`, and whether it *propagated* is read from `JoinError`; and
/// 2. the lock count is then read from a **separate maintenance session** while
///    the connection sits idle in the pool — the same external observation used by
///    every other spec here to catch a real leak — and a fresh **real `setup()`**
///    is run on the same pool, which must succeed.
///
/// A drain regression that left the lock held would show up as a non-zero
/// cross-session count *and*, because the residual hold survives on the pooled
/// connection, as observable state a maintenance session can see — neither of
/// which a copied local counter could ever detect.
///
/// Returns `None` when the environment cannot provision a database.
async fn run_panic_unwinds_and_leaves_lock_clean() -> Result<Option<PanicSafety>, String> {
    let Some(cold) = provision_cold_db("apalis_mig_panic").await? else {
        return Ok(None);
    };

    let outcome = async {
        let pool = build_pool_with(&cold.url, |builder| builder.max_size(1).min_idle(Some(0)))
            .map_err(|error| error.to_string())?;

        // Drive acquire → panic → drain → resume on the pool's single connection,
        // letting the panic unwind the whole closure so the connection returns to
        // the pool the same way it would after setup()'s `resume_unwind`.
        let panic_propagated = {
            let pool = pool.clone();
            tokio::task::spawn_blocking(move || {
                let mut conn = pool.get().expect("check out the pooled connection");
                // Acquire the lock exactly as setup() does.
                sql_query(ACQUIRE_MIGRATION_LOCK)
                    .execute(&mut *conn)
                    .expect("acquire the migration advisory lock");
                // Panicking stand-in for run_pending_migrations, caught the same way
                // setup() catches a runner panic.
                let migrated: std::thread::Result<()> =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        panic!("migration runner blew up");
                    }));
                // Drain the lock on every path, exactly as setup() does before the
                // match on `migrated`. Presence-checked so each unlock targets a
                // held copy.
                let mut drained = 0usize;
                while migration_lock_held_on_conn(&mut conn)
                    .expect("inspect the migration advisory lock")
                {
                    sql_query(RELEASE_MIGRATION_LOCK)
                        .execute(&mut *conn)
                        .expect("release the migration advisory lock");
                    drained += 1;
                    assert!(drained < 1024, "advisory lock could not be drained");
                }
                // Re-raise, exactly as setup() does: this unwinds the whole
                // spawn_blocking closure and returns `conn` to the pool.
                if let Err(panic) = migrated {
                    std::panic::resume_unwind(panic);
                }
            })
            .await
            .is_err() // JoinError::is_panic — the panic propagated, not swallowed
        };

        // Observe from a SEPARATE maintenance session while the connection is idle
        // in the pool: a leaked hold would still be sitting on that backend.
        let locks_after_unwind = migration_locks_held_in_db(&cold.url).await?;

        // The connection must be genuinely reusable: a real production setup()
        // on the same pool has to succeed on the (now clean) backend.
        let subsequent_setup = setup(&pool).await.map_err(|error| error.to_string());

        drop(pool);
        Ok::<PanicSafety, String>(PanicSafety {
            panic_propagated,
            locks_after_unwind,
            subsequent_setup,
        })
    }
    .await;

    drop_temp_db(&cold.maintenance_url, &cold.name).await;
    outcome.map(Some)
}

/// Guard the *inverse* of `release_migration_lock`'s `drained == 0` branch: a
/// normal `setup()` — which acquires the lock and therefore holds exactly one
/// copy at release time — must drain that copy and return `Ok`, never mistaking a
/// properly-held lock for the "not held at release time" broken-invariant case.
///
/// The `drained == 0` arm itself (`src/queries/migrations.rs:100-107`) is only
/// reachable when the backend holds zero copies at release time, which cannot
/// happen through `setup()`: it always acquires the lock on the same connection in
/// the same `with_conn` operation, so `release_migration_lock` is a private helper
/// with no public entry point that reaches its zero-holds arm. Rather than copy
/// that helper's drain loop and assert on the copy — a tautology that would keep
/// passing even if the production helper regressed — this drives the *real*
/// `setup()` and pins the production contract we can observe: the guard must not
/// fire on a genuinely-held lock. A regression that inverted the comparison (e.g.
/// `drained != 0` or `drained > 0`) would make this real `setup()` surface the
/// "not held at release time" `Error::Migration` and fail the assertion.
///
/// To make the observation robust, `setup()` runs on a connection that *already*
/// holds a residual copy of the lock (a `max_size(1)` pool pre-seeded exactly like
/// [`run_setup_drains_a_leaked_lock`]), so `release_migration_lock` executes its
/// loop with a real, non-zero hold count — the state under which the guard must
/// stay silent — and drains every copy.
///
/// Returns `Some(error_message)` when `setup()` fails (so the assertion can
/// inspect what surfaced), `Some(String::new())` on success, and `None` when the
/// environment cannot provision a database.
async fn run_setup_does_not_flag_a_held_lock() -> Result<Option<String>, String> {
    let Some(cold) = provision_cold_db("apalis_mig_nolock").await? else {
        return Ok(None);
    };

    let outcome = async {
        let pool = build_pool_with(&cold.url, |builder| builder.max_size(1).min_idle(Some(0)))
            .map_err(|error| error.to_string())?;

        // Pre-seed a residual hold on the pool's single connection, so the real
        // `release_migration_lock` inside `setup()` runs its drain loop against a
        // genuinely-held lock (depth 2 after setup()'s own acquire) — the exact
        // state in which the `drained == 0` guard must stay silent.
        {
            let pool = pool.clone();
            tokio::task::spawn_blocking(move || -> Result<(), String> {
                let mut conn = pool.get().map_err(|error| error.to_string())?;
                sql_query(ACQUIRE_MIGRATION_LOCK)
                    .execute(&mut conn)
                    .map_err(|error| error.to_string())?;
                Ok(()) // conn returns to the pool, session lock still held
            })
            .await
            .map_err(|error| error.to_string())??;
        }

        // Real production path. Must return Ok and must NOT surface the
        // "not held at release time" invariant error.
        let outcome = match setup(&pool).await {
            Ok(()) => String::new(),
            Err(error) => error.to_string(),
        };
        drop(pool);
        Ok::<String, String>(outcome)
    }
    .await;

    drop_temp_db(&cold.maintenance_url, &cold.name).await;
    outcome.map(Some)
}

fn setup_releases_the_advisory_lock() -> impl Fn(&Result<Option<i64>, String>) -> AssertionResult {
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "setup-release scenario failed to run: {error}"
        )])),
        Ok(None) => Ok(()),
        Ok(Some(0)) => Ok(()),
        Ok(Some(held)) => Err(AssertionError::new(vec![format!(
            "expected setup() to release its advisory lock, but {held} advisory lock(s) remain held"
        )])),
    }
}

fn setup_drains_the_leaked_lock() -> impl Fn(&Result<Option<i64>, String>) -> AssertionResult {
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "leaked-lock scenario failed to run: {error}"
        )])),
        Ok(None) => Ok(()),
        Ok(Some(0)) => Ok(()),
        Ok(Some(held)) => Err(AssertionError::new(vec![format!(
            "expected setup() to drain the pre-leaked migration lock to zero, but {held} copy/copies remain held"
        )])),
    }
}

/// A failed migration must surface as `Error::Migration` — i.e. carry the
/// underlying DDL failure, not a "releasing/inspecting the migration advisory
/// lock" (`Error::Database`) or "not held at release time" message. The
/// `Display` prefix of `Error::Migration` is "failed to run embedded migrations".
fn surfaces_the_migration_failure()
-> impl Fn(&Result<Option<FailedSetup>, String>) -> AssertionResult {
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "incompatible-schema scenario failed to run: {error}"
        )])),
        Ok(None) => Ok(()),
        Ok(Some(failed)) => {
            let msg = &failed.error;
            let is_migration_error = msg.contains("failed to run embedded migrations");
            let is_lock_error = msg.contains("advisory lock");
            if is_migration_error && !is_lock_error {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected setup() to surface the migration failure as Error::Migration, \
                     but got: {msg}"
                )]))
            }
        }
    }
}

/// Even when the migration *fails*, `setup()` must still drain the advisory lock
/// to zero (the unconditional `release_migration_lock` before the match arm).
fn failed_setup_still_releases_the_lock()
-> impl Fn(&Result<Option<FailedSetup>, String>) -> AssertionResult {
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "incompatible-schema scenario failed to run: {error}"
        )])),
        Ok(None) => Ok(()),
        Ok(Some(FailedSetup {
            advisory_locks_held: 0,
            ..
        })) => Ok(()),
        Ok(Some(failed)) => Err(AssertionError::new(vec![format!(
            "expected setup() to drain the advisory lock even on a migration failure, \
             but {} advisory lock(s) remain held",
            failed.advisory_locks_held
        )])),
    }
}

/// A panic inside the migration runner must be re-raised, not swallowed.
fn re_raises_the_panic() -> impl Fn(&Result<Option<PanicSafety>, String>) -> AssertionResult {
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "panicking-migration scenario failed to run: {error}"
        )])),
        Ok(None) => Ok(()),
        Ok(Some(PanicSafety {
            panic_propagated: true,
            ..
        })) => Ok(()),
        Ok(Some(_)) => Err(AssertionError::new(vec![
            "expected the migration-runner panic to be re-raised, but it was swallowed".to_owned(),
        ])),
    }
}

/// After the panic unwinds, the connection must return to the pool lock-clean:
/// a separate maintenance session sees zero held copies, and a fresh real
/// `setup()` on the same pool succeeds.
fn leaves_the_connection_lock_clean()
-> impl Fn(&Result<Option<PanicSafety>, String>) -> AssertionResult {
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "panicking-migration scenario failed to run: {error}"
        )])),
        Ok(None) => Ok(()),
        Ok(Some(safety)) => {
            let mut failures = Vec::new();
            if safety.locks_after_unwind != 0 {
                failures.push(format!(
                    "expected no advisory lock held after the panic unwound, \
                     but a separate session saw {} copy/copies",
                    safety.locks_after_unwind
                ));
            }
            if let Err(error) = &safety.subsequent_setup {
                failures.push(format!(
                    "expected a fresh setup() on the same pool to succeed after the panic, \
                     but it failed with: {error}"
                ));
            }
            if failures.is_empty() {
                Ok(())
            } else {
                Err(AssertionError::new(failures))
            }
        }
    }
}

/// A normal `setup()` holding the lock at release time must drain it and return
/// `Ok`, never mistaking a genuinely-held lock for the `drained == 0`
/// "not held at release time" broken-invariant case.
fn does_not_flag_a_held_lock() -> impl Fn(&Result<Option<String>, String>) -> AssertionResult {
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "held-lock scenario failed to run: {error}"
        )])),
        Ok(None) => Ok(()),
        Ok(Some(msg)) if msg.is_empty() => Ok(()),
        Ok(Some(msg)) => Err(AssertionError::new(vec![format!(
            "expected setup() to drain the held lock and succeed, but it failed with: {msg}"
        )])),
    }
}

fn contended_setups_all_succeed() -> impl Fn(&Result<ContendedOutcome, String>) -> AssertionResult {
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "contended-cleanup scenario failed to run: {error}"
        )])),
        Ok(ContendedOutcome::Skipped) => Ok(()),
        Ok(ContendedOutcome::Completed { results, .. }) => {
            let failures: Vec<String> = results
                .iter()
                .enumerate()
                .filter_map(|(i, r)| r.as_ref().err().map(|e| format!("racer {i}: {e}")))
                .collect();
            if failures.is_empty() {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected all {} contended setup() calls to succeed, {} failed:\n{}",
                    results.len(),
                    failures.len(),
                    failures.join("\n")
                )]))
            }
        }
    }
}

fn contended_setup_leaves_no_lock_held()
-> impl Fn(&Result<ContendedOutcome, String>) -> AssertionResult {
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "contended-cleanup scenario failed to run: {error}"
        )])),
        Ok(ContendedOutcome::Skipped) => Ok(()),
        Ok(ContendedOutcome::Completed {
            advisory_locks_held: 0,
            ..
        }) => Ok(()),
        Ok(ContendedOutcome::Completed {
            advisory_locks_held,
            ..
        }) => Err(AssertionError::new(vec![format!(
            "expected the shared connection to hold no advisory lock after contended setup(), \
             but {advisory_locks_held} copy/copies remain held"
        )])),
    }
}

lets_expect! { #tokio_test
    expect(run_setup_releases_lock().await) {
        when setup_completes_against_a_cold_database {
            to leaves_no_advisory_lock_held { setup_releases_the_advisory_lock() }
        }
    }

    expect(run_setup_drains_a_leaked_lock().await) {
        when setup_runs_on_a_connection_that_already_holds_the_migration_lock {
            to drains_every_reentrant_hold_to_zero { setup_drains_the_leaked_lock() }
        }
    }

    expect(run_setup_against_incompatible_schema().await) {
        when a_migration_fails_inside_the_runner {
            to surfaces_error_as_a_migration_failure { surfaces_the_migration_failure() }
            to still_drains_the_advisory_lock_to_zero { failed_setup_still_releases_the_lock() }
        }
    }

    expect(run_panic_unwinds_and_leaves_lock_clean().await) {
        when the_migration_runner_panics {
            to re_raises_the_panic_instead_of_swallowing_it { re_raises_the_panic() }
            to leaves_the_connection_lock_clean_for_the_pool { leaves_the_connection_lock_clean() }
        }
    }

    expect(run_setup_does_not_flag_a_held_lock().await) {
        when the_lock_is_held_when_release_runs {
            to drains_it_without_flagging_a_broken_invariant { does_not_flag_a_held_lock() }
        }
    }

    expect(run_concurrent_setup().await) {
        when many_replicas_call_setup_concurrently_against_a_cold_database {
            to applies_the_migrations_without_a_race { all_setups_succeed() }
        }
    }

    expect(run_contended_setup_releases_lock().await) {
        when many_replicas_contend_for_one_shared_connection {
            to complete_every_setup_call { contended_setups_all_succeed() }
            to leave_no_advisory_lock_held_on_the_shared_connection {
                contended_setup_leaves_no_lock_held()
            }
        }
    }
}
