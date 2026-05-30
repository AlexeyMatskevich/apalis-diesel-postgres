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

async fn run_concurrent_setup() -> Result<Outcome, String> {
    let Some(database_url) = support::database_url_or_skip()? else {
        return Ok(Outcome::Skipped);
    };

    // Provision a cold throwaway database. DDL identifiers cannot be bound, so
    // the generated Ulid name is interpolated; its Crockford-base32 charset is
    // injection-safe. Skip when the role cannot CREATE DATABASE.
    let db_name = format!("apalis_mig_race_{}", Ulid::new().to_string().to_lowercase());
    let provisioned = {
        let create_url = database_url.clone();
        let db_name = db_name.clone();
        tokio::task::spawn_blocking(move || -> Result<bool, String> {
            let mut conn = maintenance_conn(&create_url)?;
            if !can_create_database(&mut conn)? {
                return Ok(false);
            }
            sql_query(format!("CREATE DATABASE \"{db_name}\""))
                .execute(&mut conn)
                .map_err(|error| error.to_string())?;
            Ok(true)
        })
        .await
        .map_err(|error| error.to_string())??
    };
    if !provisioned {
        return Ok(Outcome::Skipped);
    }

    let temp_url = match with_database_name(&database_url, &db_name) {
        Ok(url) => url,
        Err(_) => {
            drop_temp_db(&database_url, &db_name).await;
            return Ok(Outcome::Skipped);
        }
    };

    // SAFETY: confirm `temp_url` actually resolves to the throwaway database
    // before running setup() against it. A `DATABASE_URL` carrying a `?dbname=`
    // query parameter (or other libpq form) can resolve back to the main
    // database despite the swapped path; refuse to proceed if so.
    let on_temp_db = {
        let temp_url = temp_url.clone();
        let expected = db_name.clone();
        tokio::task::spawn_blocking(move || -> Result<bool, String> {
            let mut conn = maintenance_conn(&temp_url)?;
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
        drop_temp_db(&database_url, &db_name).await;
        return Ok(Outcome::Skipped);
    }

    // Fire N concurrent `setup()` calls against the cold database. Keep each
    // pool tiny so the regression test itself cannot exhaust connections.
    let outcomes = futures::future::join_all((0..RACERS).map(|_| {
        let temp_url = temp_url.clone();
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
    drop_temp_db(&database_url, &db_name).await;
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

lets_expect! { #tokio_test
    expect(run_concurrent_setup().await) {
        when many_replicas_call_setup_concurrently_against_a_cold_database {
            to applies_the_migrations_without_a_race { all_setups_succeed() }
        }
    }
}
