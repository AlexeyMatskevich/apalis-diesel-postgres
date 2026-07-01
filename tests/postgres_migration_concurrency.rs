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

    expect(run_concurrent_setup().await) {
        when many_replicas_call_setup_concurrently_against_a_cold_database {
            to applies_the_migrations_without_a_race { all_setups_succeed() }
        }
    }
}
