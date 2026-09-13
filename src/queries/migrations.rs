use diesel::{
    Connection, PgConnection, QueryableByName, RunQueryDsl,
    connection::SimpleConnection,
    migration::MigrationSource,
    pg::Pg,
    sql_query,
    sql_types::{Bool, Integer, Text},
};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};

use crate::{Error, PgPool, queries::with_conn};

/// Embedded migrations, including upgrades for already installed schemas.
///
/// Out-of-band Diesel runners must use one transaction, the same advisory lock,
/// and `SET LOCAL search_path = apalis_diesel_postgres, pg_catalog, pg_temp`.
/// Create the trusted `apalis_diesel_postgres` schema first. The migration
/// journal belongs there; an application's public Diesel journal is unrelated.
/// See `docs/upgrading.md` for the existing-schema adoption and upgrade path.
pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

// The outer transaction contains every nested migration SAVEPOINT. PostgreSQL
// releases the lock and SET LOCAL on commit/rollback; a panicking pooled
// connection is discarded by Diesel/r2d2, which rolls back its transaction.
const ACQUIRE_MIGRATION_LOCK: &str = "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('apalis_diesel_postgres'), pg_catalog.hashtext('migrations'))";
const OWNED_SEARCH_PATH: &str =
    "SET LOCAL search_path = apalis_diesel_postgres, pg_catalog, pg_temp";

const BASELINE_VERSIONS: &[&str] = &[
    "00000000000000",
    "20260521000000",
    "20260521000001",
    "20260521000002",
    "20260521000003",
    "20260521000004",
    "20260521000005",
    "20260521000006",
];

// This catalog generation could have been installed by the formerly supported
// public Diesel harness, without our private journal. Keep its versions fixed:
// later migrations can change function bodies or data without changing the
// catalog shape, and must still run after adoption.
const CURRENT_FINAL_ADOPT_VERSIONS: &[&str] = &[
    "00000000000000",
    "20260520000000",
    "20260521000000",
    "20260521000001",
    "20260521000002",
    "20260521000003",
    "20260521000004",
    "20260521000005",
    "20260521000006",
    "20260910000000",
    "20260910000001",
];

#[derive(QueryableByName)]
struct Problem {
    #[diesel(sql_type = Text)]
    problem: String,
}

#[derive(QueryableByName)]
struct Flag {
    #[diesel(sql_type = Bool)]
    value: bool,
}

fn flag(conn: &mut PgConnection, query: &str) -> Result<bool, Error> {
    sql_query(query)
        .get_result::<Flag>(conn)
        .map(|row| row.value)
        .map_err(|error| Error::Migration(Box::new(error)))
}

/// The catalog generations `setup` recognizes. The final generations share
/// every index and function signature; they differ only in the constraints
/// the later migrations add, so each is identified by the constraints it must
/// carry and by the absence of the ones it must not.
#[derive(Clone, Copy)]
enum SchemaGeneration {
    Legacy,
    ReleasedBaseline,
    /// Eleven versions: the final catalog before the active-owner constraint.
    Eleven,
    /// Thirteen versions: the active-owner constraint, no state-shape constraint.
    Thirteen,
    /// Fourteen versions: the complete current contract.
    Current,
}

impl SchemaGeneration {
    /// The generation number whose constraints the contract requires.
    fn required_constraints(self) -> i32 {
        match self {
            Self::Legacy | Self::ReleasedBaseline => 0,
            Self::Eleven => 11,
            Self::Thirteen => 13,
            Self::Current => 14,
        }
    }
}

fn schema_problems(
    conn: &mut PgConnection,
    generation: SchemaGeneration,
) -> Result<Vec<String>, Error> {
    sql_query(include_str!("schema_contract.sql"))
        .bind::<Bool, _>(matches!(
            generation,
            SchemaGeneration::Eleven | SchemaGeneration::Thirteen | SchemaGeneration::Current
        ))
        .bind::<Bool, _>(matches!(generation, SchemaGeneration::Legacy))
        .bind::<Integer, _>(generation.required_constraints())
        .load::<Problem>(conn)
        .map(|rows| rows.into_iter().map(|row| row.problem).collect())
        .map_err(|error| Error::Migration(Box::new(error)))
}

fn constraint_absent(conn: &mut PgConnection, name: &str) -> Result<bool, Error> {
    flag(
        conn,
        &format!(
            "SELECT NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'apalis.jobs'::regclass AND conname = '{name}') AS value"
        ),
    )
}

fn verify_structure(conn: &mut PgConnection) -> Result<(), Error> {
    let problems = schema_problems(conn, SchemaGeneration::Current)?;
    if problems.is_empty() {
        Ok(())
    } else {
        Err(Error::Migration(
            format!(
                "schema does not satisfy the runtime contract: {}",
                problems.join(", ")
            )
            .into(),
        ))
    }
}

/// Recognize the released bytea/JSONB upstream generation with an id-only
/// worker key, or this crate's initial migration before hardening. A malformed
/// current generation must not be relabelled as legacy and silently repaired.
fn recognized_legacy(conn: &mut PgConnection) -> Result<bool, Error> {
    if !schema_problems(conn, SchemaGeneration::Legacy)?.is_empty() {
        return Ok(false);
    }
    flag(
        conn,
        "SELECT \
        NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'apalis.workers'::regclass AND attname = 'lease_token' AND NOT attisdropped) \
        AND to_regclass('apalis.queue_stats_snapshot') IS NULL \
        AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'apalis.jobs'::regclass AND contype = 'p' AND pg_get_constraintdef(oid) = 'PRIMARY KEY (id)') \
        AND ((EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'apalis.workers'::regclass AND contype = 'p' AND pg_get_constraintdef(oid) = 'PRIMARY KEY (id)') \
          AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'apalis.jobs'::regclass AND conname IN ('fk_worker_lock_by','jobs_lock_by_fkey') AND pg_get_constraintdef(oid) = 'FOREIGN KEY (lock_by) REFERENCES apalis.workers(id)')) \
         OR (EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'apalis.workers'::regclass AND contype = 'p' AND pg_get_constraintdef(oid) = 'PRIMARY KEY (id, worker_type)') \
          AND EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'apalis.jobs'::regclass AND conname = 'jobs_lock_by_worker_type_fkey' AND pg_get_constraintdef(oid) = 'FOREIGN KEY (lock_by, job_type) REFERENCES apalis.workers(id, worker_type)') \
          AND (SELECT count(*) FROM pg_constraint WHERE conrelid = 'apalis.jobs'::regclass AND contype = 'c' AND conname IN ('jobs_status_check','jobs_attempts_check','jobs_max_attempts_check','jobs_attempts_lte_max_attempts_check','jobs_priority_check')) = 5)) AS value",
    )
}

fn adopt_or_check_existing(conn: &mut PgConnection) -> Result<(), Error> {
    // applied_migrations creates only OUR journal, under the local trusted path.
    let applied = conn.applied_migrations().map_err(Error::Migration)?;
    if !applied.is_empty() {
        return Ok(());
    }
    if !flag(
        conn,
        "SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='apalis') AS value",
    )? {
        return Ok(());
    }
    let current_matches = schema_problems(conn, SchemaGeneration::Current)?.is_empty();
    // An absent later constraint distinguishes each previous supported
    // catalog. A malformed present constraint must not be relabelled as an
    // earlier generation.
    let thirteen_matches = !current_matches
        && schema_problems(conn, SchemaGeneration::Thirteen)?.is_empty()
        && constraint_absent(conn, "jobs_state_shape_check")?;
    let eleven_matches = !current_matches
        && !thirteen_matches
        && schema_problems(conn, SchemaGeneration::Eleven)?.is_empty()
        && constraint_absent(conn, "jobs_active_owner_check")?
        && constraint_absent(conn, "jobs_state_shape_check")?;
    let final_matches = current_matches || thirteen_matches || eleven_matches;
    let baseline_problems = if final_matches {
        Vec::new()
    } else {
        schema_problems(conn, SchemaGeneration::ReleasedBaseline)?
    };
    if final_matches || baseline_problems.is_empty() {
        let versions = if final_matches {
            CURRENT_FINAL_ADOPT_VERSIONS
        } else {
            BASELINE_VERSIONS
        };
        for version in versions {
            sql_query("INSERT INTO apalis_diesel_postgres.__diesel_schema_migrations(version) VALUES ($1)")
                .bind::<Text, _>(*version).execute(conn)
                .map_err(|error| Error::Migration(Box::new(error)))?;
        }
        return Ok(());
    }
    if recognized_legacy(conn)? {
        return Ok(());
    }
    Err(Error::Migration(format!(
        "unsupported existing apalis schema; no migration history was adopted: {}; restore a supported baseline or migrate explicitly (docs/upgrading.md)",
        baseline_problems.join(", ")
    ).into()))
}

pub(crate) async fn setup(pool: PgPool) -> Result<(), Error> {
    with_conn(pool, |conn| {
        let migrations = <EmbeddedMigrations as MigrationSource<Pg>>::migrations(&MIGRATIONS)
            .map_err(Error::Migration)?;
        if migrations
            .iter()
            .any(|migration| !migration.metadata().run_in_transaction())
        {
            return Err(Error::Migration(
                "setup requires transactional migrations".into(),
            ));
        }
        conn.transaction(|conn| {
            sql_query(ACQUIRE_MIGRATION_LOCK)
                .execute(conn)
                .map_err(Error::database("acquiring the migration advisory lock"))?;
            // PostgreSQL checks CREATE on the database even when IF NOT EXISTS
            // finds the schema. Under our advisory lock, existing installations
            // need only their schema/journal privileges.
            if !flag(conn, "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname='apalis_diesel_postgres') AS value")? {
                conn.batch_execute("CREATE SCHEMA apalis_diesel_postgres")
                    .map_err(|error| Error::Migration(Box::new(error)))?;
            }
            conn.batch_execute(OWNED_SEARCH_PATH)
                .map_err(|error| Error::Migration(Box::new(error)))?;
            adopt_or_check_existing(conn)?;
            conn.run_pending_migrations(MIGRATIONS)
                .map_err(Error::Migration)?;
            verify_structure(conn)
        })
    })
    .await
}

pub(crate) async fn verify_schema(pool: PgPool) -> Result<(), Error> {
    with_conn(pool, |conn| conn.transaction(|conn| {
        conn.batch_execute(OWNED_SEARCH_PATH)
            .map_err(|error| Error::Migration(Box::new(error)))?;
        if !flag(conn, "SELECT to_regclass('apalis_diesel_postgres.__diesel_schema_migrations') IS NOT NULL AS value")? {
            return Err(Error::Migration("owned migration journal is missing; call setup to migrate or adopt the schema".into()));
        }
        #[derive(QueryableByName)]
        struct Version { #[diesel(sql_type = Text)] version: String }
        let applied = sql_query("SELECT version::text FROM apalis_diesel_postgres.__diesel_schema_migrations")
            .load::<Version>(conn).map_err(|error| Error::Migration(Box::new(error)))?;
        let migrations = <EmbeddedMigrations as MigrationSource<Pg>>::migrations(&MIGRATIONS)
            .map_err(Error::Migration)?;
        if migrations.iter().any(|migration| !applied.iter().any(|row| row.version == migration.name().version().to_string())) {
            return Err(Error::Migration("embedded migrations have not been applied; call setup first".into()));
        }
        verify_structure(conn)
    })).await
}
