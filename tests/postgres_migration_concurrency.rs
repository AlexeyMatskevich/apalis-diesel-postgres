//! Actual setup/verify contracts against one task-owned database per leaf.
//! The test role must create/drop databases and create, assume, and drop its own
//! roles (use a superuser only in a disposable test cluster). Infrastructure
//! failure is a test failure. Only explicit optional mode may omit DATABASE_URL.
#![cfg(feature = "tokio")]
mod support;

use apalis_diesel_postgres::{Error, MIGRATIONS, PgPool, build_pool_with, setup, verify_schema};
use diesel::{
    Connection, PgConnection, QueryableByName, RunQueryDsl,
    connection::{InstrumentationEvent, SimpleConnection},
    r2d2::CustomizeConnection,
    sql_query,
};
use diesel_migrations::MigrationHarness;
use lets_expect::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use support::{Outcome, observe, with_conn, with_isolated_database};

#[derive(Debug)]
struct Observations(Vec<(String, bool)>);
impl Observations {
    fn new() -> Self {
        Self(Vec::new())
    }
    fn check(&mut self, name: &str, passed: bool) {
        self.0.push((name.into(), passed));
    }
}
fn satisfies_contract() -> impl Fn(&Result<Outcome<Observations>, String>) -> AssertionResult {
    observe("schema lifecycle", |run: &Observations| {
        let failed: Vec<_> = run
            .0
            .iter()
            .filter(|(_, passed)| !passed)
            .map(|(name, _)| name.clone())
            .collect();
        if run.0.is_empty() {
            Err("scenario made no observations".into())
        } else if failed.is_empty() {
            Ok(())
        } else {
            Err(failed.join("; "))
        }
    })
}
fn pool(url: &str) -> Result<PgPool, String> {
    build_pool_with(url, |b| b.max_size(1)).map_err(|e| e.to_string())
}
async fn sql(pool: &PgPool, text: impl Into<String>) -> Result<(), String> {
    let text = text.into();
    with_conn(pool.clone(), move |conn| {
        conn.batch_execute(&text).map_err(|e| e.to_string())
    })
    .await
}
async fn count(pool: &PgPool, text: &str) -> Result<i64, String> {
    #[derive(QueryableByName)]
    struct N {
        #[diesel(sql_type=diesel::sql_types::BigInt)]
        n: i64,
    }
    let text = text.to_owned();
    with_conn(pool.clone(), move |conn| {
        sql_query(text)
            .get_result::<N>(conn)
            .map(|v| v.n)
            .map_err(|e| e.to_string())
    })
    .await
}
async fn locks(pool: &PgPool) -> Result<i64, String> {
    count(pool, "SELECT count(*)::bigint AS n FROM pg_locks WHERE locktype='advisory' AND classid=hashtext('apalis_diesel_postgres')::oid AND objid=hashtext('migrations')::oid AND objsubid=2 AND database=(SELECT oid FROM pg_database WHERE datname=current_database())").await
}
// Local panic completion does not acknowledge remote backend rollback. Taking
// the same lock proves the next migration can proceed; timeout remains a failure.
async fn require_migration_progress(pool: &PgPool) -> Result<(), String> {
    with_conn(pool.clone(), |conn| {
        conn.transaction::<_, diesel::result::Error, _>(|conn| {
            conn.batch_execute("SET LOCAL lock_timeout = '5s'; SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('apalis_diesel_postgres'), pg_catalog.hashtext('migrations'))")
        }).map_err(|error| error.to_string())
    }).await
}

async fn absent_private_schema(pool: &PgPool) -> Result<bool, String> {
    Ok(count(
        pool,
        "SELECT count(*)::bigint AS n FROM pg_namespace WHERE nspname='apalis_diesel_postgres'",
    )
    .await?
        == 0)
}

#[derive(Clone, Copy)]
enum Scenario {
    Fresh,
    ForeignHistory,
    ForgedHistory,
    Baseline,
    Initial,
    Legacy,
    ExternalLegacy,
    Unsupported,
    DriftColumn,
    DriftConstraint,
    DriftIndex,
    TemporaryJournal,
    Concurrent,
    Contended,
    DowngradeCollision,
    DowngradeUnique,
}

async fn migration_scenario(kind: Scenario) -> Result<Outcome<Observations>, String> {
    with_isolated_database(move |url| async move {
        let pool = pool(&url)?;
        let mut out = Observations::new();
        match kind {
            Scenario::Fresh | Scenario::ForeignHistory | Scenario::TemporaryJournal => {
                if matches!(kind, Scenario::ForeignHistory) {
                    sql(&pool, "CREATE TABLE public.__diesel_schema_migrations(version varchar(50) PRIMARY KEY, run_on timestamp NOT NULL DEFAULT now()); INSERT INTO public.__diesel_schema_migrations(version) VALUES ('00000000000000')").await?;
                }
                if matches!(kind, Scenario::TemporaryJournal) {
                    sql(&pool, "CREATE TEMP TABLE __diesel_schema_migrations(version varchar(50) PRIMARY KEY, run_on timestamp NOT NULL DEFAULT now()); INSERT INTO __diesel_schema_migrations(version) VALUES ('00000000000000')").await?;
                }
                out.check("uninitialized verification fails", matches!(verify_schema(&pool).await, Err(Error::Migration(_))));
                setup(&pool).await.map_err(|e| e.to_string())?;
                out.check("verification accepts the completed schema", verify_schema(&pool).await.is_ok());
                out.check("all migrations are recorded privately", count(&pool, "SELECT count(*)::bigint AS n FROM apalis_diesel_postgres.__diesel_schema_migrations").await? == 13);
                out.check("search_path is restored", count(&pool, "SELECT (current_setting('search_path') = '\"$user\", public')::integer::bigint AS n").await? == 1);
                if matches!(kind, Scenario::ForeignHistory) {
                    out.check("foreign history is unchanged", count(&pool, "SELECT count(*)::bigint AS n FROM public.__diesel_schema_migrations WHERE version='00000000000000'").await? == 1);
                }
                if matches!(kind, Scenario::TemporaryJournal) {
                    out.check("temporary history is unchanged", count(&pool, "SELECT count(*)::bigint AS n FROM pg_temp.__diesel_schema_migrations WHERE version='00000000000000'").await? == 1);
                }
                out.check("setup is repeatable", setup(&pool).await.is_ok());
            }
            Scenario::ForgedHistory => {
                sql(&pool, "CREATE SCHEMA apalis_diesel_postgres; CREATE TABLE apalis_diesel_postgres.__diesel_schema_migrations(version varchar(50) PRIMARY KEY, run_on timestamp NOT NULL DEFAULT now()); INSERT INTO apalis_diesel_postgres.__diesel_schema_migrations(version) VALUES ('00000000000000'),('20260520000000'),('20260521000000'),('20260521000001'),('20260521000002'),('20260521000003'),('20260521000004'),('20260521000005'),('20260521000006'),('20260910000000'),('20260910000001')").await?;
                out.check("stamps without tables fail verification", matches!(verify_schema(&pool).await, Err(Error::Migration(_))));
                out.check("setup rejects stamped schema damage", matches!(setup(&pool).await, Err(Error::Migration(_))));
            }
            Scenario::Baseline | Scenario::Initial => {
                sql(&pool, if matches!(kind,Scenario::Baseline) { include_str!("fixtures/apalis-diesel-postgres-0.4.1.sql") }
                           else { include_str!("fixtures/apalis-diesel-postgres-initial.sql") }).await?;
                sql(&pool, "CREATE TABLE public.application_worker_ref(worker_id text, queue text, CONSTRAINT application_worker_fk FOREIGN KEY(worker_id,queue) REFERENCES apalis.workers(id,worker_type)); INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES ('same','queue-a','fixture'),('same','queue-b','fixture'); INSERT INTO public.application_worker_ref VALUES ('same','queue-a')").await?;
                setup(&pool).await.map_err(|e| e.to_string())?;
                out.check("baseline adopts and verifies", verify_schema(&pool).await.is_ok());
                out.check("external composite FK survives adoption", count(&pool,"SELECT count(*)::bigint AS n FROM pg_constraint WHERE conrelid='public.application_worker_ref'::regclass AND conname='application_worker_fk'").await? == 1);
                out.check("both queue registrations survive", count(&pool,"SELECT count(*)::bigint AS n FROM apalis.workers WHERE id='same'").await? == 2);
            }
            Scenario::Legacy | Scenario::ExternalLegacy => {
                sql(&pool, include_str!("fixtures/apalis-postgres-1.0.0-rc.8.sql")).await?;
                if matches!(kind, Scenario::ExternalLegacy) {
                    sql(&pool, "CREATE TABLE public.application_worker_ref(worker_id text REFERENCES apalis.workers(id)); INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES ('worker','other','fixture'); INSERT INTO public.application_worker_ref VALUES ('worker')").await?;
                    out.check("incompatible external dependency refuses upgrade", matches!(setup(&pool).await, Err(Error::Migration(_))));
                    out.check("failed upgrade leaves no private history", absent_private_schema(&pool).await?);
                    out.check("external FK and row survive", count(&pool,"SELECT count(*)::bigint AS n FROM pg_constraint WHERE conrelid='public.application_worker_ref'::regclass AND contype='f'").await? == 1 && count(&pool,"SELECT count(*)::bigint AS n FROM public.application_worker_ref").await? == 1);
                    // Only this fixture owns this dependency; removing it allows retry.
                    sql(&pool,"DROP TABLE public.application_worker_ref").await?;
                    setup(&pool).await.map_err(|e| e.to_string())?;
                    out.check("retry succeeds after explicit dependency resolution", verify_schema(&pool).await.is_ok());
                } else {
                    sql(&pool, "INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES ('worker','other','fixture'); INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,lock_by,done_at,last_result) SELECT s,'target',decode('22','hex'),s,3,3,'worker','2020-01-01'::timestamptz,'\"retained\"'::jsonb FROM unnest(ARRAY['Pending','Queued','Running','Done','Failed','Killed']) s; INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,lock_by) VALUES ('has-budget','target',decode('22','hex'),'Running',0,3,'worker'),('no-owner','target',decode('22','hex'),'Running',0,3,NULL)").await?;
                    sql(&pool,"INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES ('healthy','target','fixture'); INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,lock_by) VALUES ('healthy','target',decode('22','hex'),'Running',0,3,'healthy')").await?;
                    setup(&pool).await.map_err(|e| e.to_string())?;
                    out.check("valid legacy attribution is retained",count(&pool,"SELECT count(*)::bigint AS n FROM apalis.jobs WHERE id='healthy' AND status='Running' AND attempts=0 AND lock_by='healthy'").await? == 1);
                    out.check("upstream schema verifies after upgrade", verify_schema(&pool).await.is_ok());
                    out.check("terminal status and history survive", count(&pool,"SELECT count(*)::bigint AS n FROM apalis.jobs WHERE id IN ('Done','Failed','Killed','Pending') AND status=id AND attempts=3 AND last_result='\"retained\"'::jsonb AND done_at='2020-01-01'::timestamptz AND lock_by IS NULL").await? == 4);
                    out.check("exhausted active executions become terminal", count(&pool,"SELECT count(*)::bigint AS n FROM apalis.jobs WHERE id IN ('Running','Queued') AND status='Killed' AND attempts=3 AND done_at IS NOT NULL AND last_result ? 'Err' AND lock_by IS NULL").await? == 2);
                    out.check("lost active executions consume one attempt and recover", count(&pool,"SELECT count(*)::bigint AS n FROM apalis.jobs WHERE id IN ('has-budget','no-owner') AND status='Pending' AND attempts=1 AND done_at IS NULL AND lock_by IS NULL").await? == 2);
                    sql(&pool,"INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES ('new','target','fixture')").await?;
                    out.check("only jobs with remaining budget can be claimed", count(&pool,"SELECT count(*)::bigint AS n FROM apalis.get_jobs('new','target',100)").await? == 2);
                }
            }
            Scenario::Unsupported => {
                sql(&pool,"CREATE SCHEMA apalis; CREATE TABLE apalis.jobs(id integer PRIMARY KEY); INSERT INTO apalis.jobs VALUES(42)").await?;
                out.check("unsupported partial schema fails explicitly", matches!(setup(&pool).await, Err(Error::Migration(_))));
                out.check("unsupported data survives", count(&pool,"SELECT count(*)::bigint AS n FROM apalis.jobs WHERE id=42").await? == 1);
                out.check("failed setup rolls back journal creation", absent_private_schema(&pool).await?);
            }
            Scenario::DriftColumn | Scenario::DriftConstraint | Scenario::DriftIndex => {
                setup(&pool).await.map_err(|e| e.to_string())?;
                let change = match kind {
                    Scenario::DriftColumn => "ALTER TABLE apalis.workers DROP COLUMN lease_token",
                    Scenario::DriftConstraint => "ALTER TABLE apalis.jobs DROP CONSTRAINT jobs_attempts_check; ALTER TABLE apalis.jobs ADD CONSTRAINT jobs_attempts_check CHECK(attempts >= -1)",
                    _ => "DROP INDEX apalis.idx_jobs_idempotency_key; CREATE INDEX idx_jobs_idempotency_key ON apalis.jobs(job_type,idempotency_key)",
                };
                sql(&pool,change).await?;
                out.check("schema drift fails verification despite current history", matches!(verify_schema(&pool).await,Err(Error::Migration(_))));
                out.check("setup does not bless drift", matches!(setup(&pool).await,Err(Error::Migration(_))));
            }
            Scenario::Concurrent | Scenario::Contended => {
                let barrier = Arc::new(tokio::sync::Barrier::new(8));
                let mut racers = Vec::new();
                for _ in 0..8 {
                    let pool = if matches!(kind,Scenario::Contended) { pool.clone() } else { self::pool(&url)? };
                    let barrier = barrier.clone();
                    racers.push(tokio::spawn(async move { barrier.wait().await; setup(&pool).await.map_err(|e|e.to_string()) }));
                }
                for racer in racers { racer.await.map_err(|e|e.to_string())??; }
                out.check("all racers produce one valid schema",verify_schema(&pool).await.is_ok());
                out.check("history contains one copy of every migration",count(&pool,"SELECT count(*)::bigint AS n FROM apalis_diesel_postgres.__diesel_schema_migrations").await? == 13);
            }
            Scenario::DowngradeCollision | Scenario::DowngradeUnique => {
                setup(&pool).await.map_err(|e|e.to_string())?;
                sql(&pool, if matches!(kind,Scenario::DowngradeCollision) {
                    "INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES ('same','a','fixture'),('same','b','fixture')"
                } else { "INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES ('one','a','fixture'),('two','b','fixture')" }).await?;
                let result = with_conn(pool.clone(), |conn| conn.transaction::<_,diesel::result::Error,_>(|conn| conn.batch_execute(include_str!("../migrations/20260521000000_harden_apalis_sql/down.sql"))).map_err(|e|e.to_string())).await;
                out.check("downgrade result follows representable identity",result.is_err() == matches!(kind,Scenario::DowngradeCollision));
                out.check("downgrade preserves every worker",count(&pool,"SELECT count(*)::bigint AS n FROM apalis.workers").await? == 2);
                if matches!(kind,Scenario::DowngradeCollision) { out.check("rejected downgrade leaves current schema intact",verify_schema(&pool).await.is_ok()); }
                else {
                    sql(&pool,include_str!("../migrations/20260521000000_harden_apalis_sql/up.sql")).await?;
                    sql(&pool,include_str!("../migrations/20260910000000_reconcile_schema_contract/up.sql")).await?;
                    sql(&pool,include_str!("../migrations/20260910000001_listing_id_tie_breaker/up.sql")).await?;
                    sql(&pool,include_str!("../migrations/20260912000000_worker_key_share/up.sql")).await?;
                    sql(&pool,include_str!("../migrations/20260912000001_require_active_owner/up.sql")).await?;
                    out.check("supported downgrade can be upgraded again",verify_schema(&pool).await.is_ok());
                }
            }
        }
        out.check("no migration advisory lock survives",locks(&pool).await? == 0);
        Ok(out)
    }).await
}

#[derive(Debug)]
struct PanicOnce(Arc<AtomicBool>);
impl CustomizeConnection<PgConnection, diesel::r2d2::Error> for PanicOnce {
    fn on_acquire(&self, conn: &mut PgConnection) -> Result<(), diesel::r2d2::Error> {
        let armed = self.0.clone();
        conn.set_instrumentation(move |event: InstrumentationEvent<'_>| {
            if let InstrumentationEvent::StartQuery { query, .. } = event
                && query.to_string().contains("Reconcile deployed schemas")
                && armed.swap(false, Ordering::SeqCst)
            {
                panic!("injected panic inside actual setup after earlier migrations");
            }
        });
        Ok(())
    }
}
async fn panicking_setup() -> Result<Outcome<Observations>, String> {
    with_isolated_database(|url| async move {
        let armed = Arc::new(AtomicBool::new(true));
        let flag = armed.clone();
        let pool = build_pool_with(&url, |b| b.max_size(1).connection_customizer(Box::new(PanicOnce(flag)))).map_err(|e|e.to_string())?;
        let result = setup(&pool).await;
        let observer = self::pool(&url)?;
        require_migration_progress(&observer).await?;
        let mut out = Observations::new();
        out.check("instrumentation actually reached a late migration",!armed.load(Ordering::SeqCst));
        out.check("public setup reports blocking panic",matches!(result,Err(Error::Blocking(_))));
        out.check("all earlier migrations rolled back",count(&observer,"SELECT count(*)::bigint AS n FROM pg_namespace WHERE nspname IN ('apalis','apalis_diesel_postgres')").await? == 0);
        out.check("panicking setup releases its transaction lock",locks(&observer).await? == 0);
        setup(&pool).await.map_err(|e|e.to_string())?;
        out.check("same pool recovers for subsequent setup",verify_schema(&pool).await.is_ok());
        Ok(out)
    }).await
}

const LISTING_MIGRATION_VERSION: &str = "20260910000001";

async fn previous_listing_generation(pool: &PgPool) -> Result<(), String> {
    with_conn(pool.clone(), |conn| {
        conn.transaction::<_, Error, _>(|conn| {
            conn.batch_execute(
                "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('apalis_diesel_postgres'), pg_catalog.hashtext('migrations')); \
                 CREATE SCHEMA apalis_diesel_postgres; \
                 SET LOCAL search_path = apalis_diesel_postgres, pg_catalog, pg_temp",
            )
            .map_err(|error| Error::Migration(Box::new(error)))?;
            let mut migrations = conn.pending_migrations(MIGRATIONS).map_err(Error::Migration)?;
            migrations.retain(|migration| {
                migration.name().version().to_string().as_str() < LISTING_MIGRATION_VERSION
            });
            conn.run_migrations(&migrations).map_err(Error::Migration)?;
            Ok(())
        })
        .map_err(|error| error.to_string())
    })
    .await
}

async fn catalog_text(pool: &PgPool, query: &str) -> Result<String, String> {
    #[derive(QueryableByName)]
    struct Value {
        #[diesel(sql_type = diesel::sql_types::Text)]
        value: String,
    }
    let query = query.to_owned();
    with_conn(pool.clone(), move |conn| {
        sql_query(query)
            .get_result::<Value>(conn)
            .map(|row| row.value)
            .map_err(|error| error.to_string())
    })
    .await
}

// Include every job/worker field and the original journal timestamps, so an
// index-only migration cannot silently rewrite either application data or history.
async fn listing_data_and_history(pool: &PgPool) -> Result<String, String> {
    catalog_text(pool,
        "SELECT jsonb_build_object( \
          'jobs', (SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM apalis.jobs j), \
          'workers', (SELECT jsonb_agg(to_jsonb(w) ORDER BY id,worker_type) FROM apalis.workers w), \
          'history', (SELECT jsonb_agg(to_jsonb(h) ORDER BY version) \
                      FROM apalis_diesel_postgres.__diesel_schema_migrations h \
                      WHERE version < '20260910000001'))::text AS value").await
}

#[derive(Clone, Copy)]
enum ListingIndex {
    Queue,
    Global,
}
impl ListingIndex {
    fn name(self) -> &'static str {
        match self {
            Self::Queue => "jobs_list_by_queue_idx",
            Self::Global => "jobs_list_all_idx",
        }
    }
    fn definition(self, tie_breaker: &str) -> String {
        let equality_prefix = match self {
            Self::Queue => "job_type, status",
            Self::Global => "status",
        };
        format!(
            "CREATE INDEX {} ON apalis.jobs USING btree ({equality_prefix}, done_at DESC, run_at DESC{tie_breaker})",
            self.name()
        )
    }
}

async fn listing_indexes_match(pool: &PgPool, tie_breaker: &str) -> Result<bool, String> {
    let mut matched = true;
    for index in [ListingIndex::Queue, ListingIndex::Global] {
        let definition = catalog_text(
            pool,
            &format!(
                "SELECT COALESCE(pg_get_indexdef(to_regclass('apalis.{}')), '<missing>') AS value",
                index.name()
            ),
        )
        .await?;
        matched &= definition == index.definition(tie_breaker);
    }
    Ok(matched)
}

#[derive(Debug)]
struct PanicAfterListingDdl(Arc<AtomicBool>);
impl CustomizeConnection<PgConnection, diesel::r2d2::Error> for PanicAfterListingDdl {
    fn on_acquire(&self, conn: &mut PgConnection) -> Result<(), diesel::r2d2::Error> {
        let armed = self.0.clone();
        conn.set_instrumentation(move |event: InstrumentationEvent<'_>| {
            if let InstrumentationEvent::FinishQuery {
                query, error: None, ..
            } = event
                && query.to_string().contains("Extend listing indexes")
                && armed.swap(false, Ordering::SeqCst)
            {
                // PostgreSQL has accepted both new index definitions, but the
                // migration journal and outer setup transaction have not committed.
                panic!("injected panic after successful listing index DDL");
            }
        });
        Ok(())
    }
}

async fn listing_index_upgrade(panics: bool) -> Result<Outcome<Observations>, String> {
    with_isolated_database(move |url| async move {
        let observer = pool(&url)?;
        previous_listing_generation(&observer).await?;
        sql(&observer,
            "INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES \
             ('retained-worker','queue-a','fixture'),('retained-worker','queue-b','fixture'); \
             INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at,done_at,last_result) VALUES \
             ('pending','queue-a',decode('2222','hex'),'Pending',0,3,'2024-01-01',NULL,NULL), \
             ('done','queue-a',decode('22646f6e6522','hex'),'Done',1,3,'2024-01-01','2024-01-02','{\"Ok\":\"retained\"}'), \
             ('failed','queue-b',decode('226661696c656422','hex'),'Failed',2,3,'2024-01-01','2024-01-02','{\"Err\":\"retained\"}')").await?;
        let before = listing_data_and_history(&observer).await?;
        let mut out = Observations::new();
        out.check("fixture has the previous ten migration records", count(&observer, "SELECT count(*)::bigint AS n FROM apalis_diesel_postgres.__diesel_schema_migrations").await? == 10);
        out.check("fixture has both previous listing indexes", listing_indexes_match(&observer, "").await?);
        let armed = Arc::new(AtomicBool::new(panics));
        let flag = armed.clone();
        let migrator = build_pool_with(&url, |builder| builder.max_size(1).connection_customizer(Box::new(PanicAfterListingDdl(flag)))).map_err(|error| error.to_string())?;
        let result = setup(&migrator).await;
        if panics {
            out.check("the successful DDL result actually triggers the panic", !armed.load(Ordering::SeqCst));
            out.check("public setup reports the blocking panic", matches!(result, Err(Error::Blocking(_))));
            // A fresh connection sees only durable state, independently of the
            // panicked connection's local transaction/session state.
            let after_panic = pool(&url)?;
            require_migration_progress(&after_panic).await?;
            out.check("rollback restores both previous index definitions", listing_indexes_match(&after_panic, "").await?);
            out.check("rollback preserves every job, worker, and original journal row", listing_data_and_history(&after_panic).await? == before);
            out.check("rollback adds no successful migration record", count(&after_panic, "SELECT count(*)::bigint AS n FROM apalis_diesel_postgres.__diesel_schema_migrations").await? == 10);
            out.check("rollback releases the migration advisory lock", locks(&after_panic).await? == 0);
            setup(&migrator).await.map_err(|error| error.to_string())?;
        } else {
            out.check("setup upgrades the previous owned generation", result.is_ok());
        }
        out.check("upgrade installs both complete descending listing indexes", listing_indexes_match(&observer, ", id DESC").await?);
        out.check("the upgraded schema verifies", verify_schema(&observer).await.is_ok());
        out.check("upgrade preserves all original data and journal timestamps", listing_data_and_history(&observer).await? == before);
        out.check("upgrade records exactly thirteen migrations", count(&observer, "SELECT count(*)::bigint AS n FROM apalis_diesel_postgres.__diesel_schema_migrations").await? == 13);
        let upgraded_history = catalog_text(&observer, "SELECT jsonb_agg(to_jsonb(h) ORDER BY version)::text AS value FROM apalis_diesel_postgres.__diesel_schema_migrations h").await?;
        out.check("repeated setup succeeds on the same pool", setup(&migrator).await.is_ok());
        out.check("repeated setup preserves the complete index contract", listing_indexes_match(&observer, ", id DESC").await?);
        out.check("repeated setup preserves data and old journal rows", listing_data_and_history(&observer).await? == before);
        out.check("repeated setup preserves the entire upgraded journal", catalog_text(&observer, "SELECT jsonb_agg(to_jsonb(h) ORDER BY version)::text AS value FROM apalis_diesel_postgres.__diesel_schema_migrations h").await? == upgraded_history);
        if !panics {
            with_conn(migrator.clone(), |conn| {
                conn.transaction::<_, diesel::result::Error, _>(|conn| {
                    conn.batch_execute(include_str!("../migrations/20260910000001_listing_id_tie_breaker/down.sql"))?;
                    conn.batch_execute("DELETE FROM apalis_diesel_postgres.__diesel_schema_migrations WHERE version='20260910000001'")
                }).map_err(|error| error.to_string())
            }).await?;
            out.check("down restores exactly both previous index definitions", listing_indexes_match(&observer, "").await?);
            out.check("down preserves data and all previous journal records", listing_data_and_history(&observer).await? == before);
            out.check("down removes only the listing migration record", count(&observer, "SELECT count(*)::bigint AS n FROM apalis_diesel_postgres.__diesel_schema_migrations").await? == 12);
            setup(&migrator).await.map_err(|error| error.to_string())?;
            out.check("public setup upgrades the downgraded schema again", verify_schema(&observer).await.is_ok() && listing_indexes_match(&observer, ", id DESC").await?);
            out.check("reupgrade preserves application data and old history", listing_data_and_history(&observer).await? == before);
        }
        out.check("no migration advisory lock survives the scenario", locks(&observer).await? == 0);
        Ok(out)
    }).await
}

#[derive(Clone, Copy)]
enum Install {
    Fresh,
    Released,
}

/// Reverting the migrations that follow the released 0.4.1 generation must
/// leave the schema of that generation: the index predicate, and for a
/// database installed by that release, its own function definitions.
async fn reconciliation_downgrade(install: Install) -> Result<Outcome<Observations>, String> {
    with_isolated_database(move |url| async move {
        let pool = pool(&url)?;
        let mut out = Observations::new();
        let dequeue = "SELECT pg_get_indexdef(to_regclass('apalis.jobs_dequeue_idx')) AS value";
        let functions = "SELECT string_agg(pg_get_functiondef(p.oid) || ' CONFIG ' || coalesce(array_to_string(p.proconfig, ','), ''), E'\n' ORDER BY p.proname) AS value \
            FROM pg_proc p WHERE p.oid IN (to_regprocedure('apalis.get_jobs(text,text,integer)'), to_regprocedure('apalis.notify_new_jobs()'))";
        let released = if matches!(install, Install::Released) {
            sql(&pool, include_str!("fixtures/apalis-diesel-postgres-0.4.1.sql")).await?;
            Some(catalog_text(&pool, functions).await?)
        } else {
            None
        };
        setup(&pool).await.map_err(|e| e.to_string())?;
        let upgraded = catalog_text(&pool, dequeue).await?;
        with_conn(pool.clone(), |conn| {
            conn.transaction::<_, diesel::result::Error, _>(|conn| {
                conn.batch_execute(include_str!("../migrations/20260912000001_require_active_owner/down.sql"))?;
                conn.batch_execute(include_str!("../migrations/20260912000000_worker_key_share/down.sql"))?;
                conn.batch_execute(include_str!("../migrations/20260910000001_listing_id_tie_breaker/down.sql"))?;
                conn.batch_execute(include_str!("../migrations/20260910000000_reconcile_schema_contract/down.sql"))?;
                conn.batch_execute("DELETE FROM apalis_diesel_postgres.__diesel_schema_migrations WHERE version IN ('20260912000001','20260912000000','20260910000001','20260910000000')")
            }).map_err(|e| e.to_string())
        }).await?;
        out.check(
            "down restores the previous generation's dequeue predicate",
            catalog_text(&pool, dequeue).await? == "CREATE INDEX jobs_dequeue_idx ON apalis.jobs USING btree (job_type, priority DESC, run_at, id) WHERE ((status = 'Pending'::text) OR ((status = 'Failed'::text) AND (attempts < max_attempts)))",
        );
        out.check(
            "down leaves the nine records of the previous generation",
            count(&pool, "SELECT count(*)::bigint AS n FROM apalis_diesel_postgres.__diesel_schema_migrations").await? == 9,
        );
        let downgraded_functions = catalog_text(&pool, functions).await?;
        match released {
            Some(released) => out.check(
                "down restores the function definitions the release installed",
                downgraded_functions == released,
            ),
            None => out.check(
                "down installs the released function definitions",
                downgraded_functions.matches("CONFIG search_path=pg_catalog, apalis\n").count() == 1
                    && downgraded_functions.ends_with("CONFIG search_path=pg_catalog, apalis")
                    && downgraded_functions.contains("(status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts))")
                    && !downgraded_functions.contains("FOR SHARE")
                    && !downgraded_functions.contains("pg_temp"),
            ),
        }
        setup(&pool).await.map_err(|e| e.to_string())?;
        out.check(
            "setup upgrades the downgraded schema again",
            verify_schema(&pool).await.is_ok() && catalog_text(&pool, dequeue).await? == upgraded,
        );
        Ok(out)
    })
    .await
}

#[derive(Clone, Copy)]
enum IndexDrift {
    MissingTieBreaker,
    AscendingTieBreaker,
    MissingIndex,
}

fn reports_index_contract(result: &Result<(), Error>, index: ListingIndex) -> bool {
    matches!(result, Err(Error::Migration(error)) if error.to_string() == format!(
        "schema does not satisfy the runtime contract: index {}", index.name()
    ))
}

async fn listing_index_verification(
    index: ListingIndex,
    drift: IndexDrift,
) -> Result<Outcome<Observations>, String> {
    with_isolated_database(move |url| async move {
        let pool = pool(&url)?;
        setup(&pool).await.map_err(|error| error.to_string())?;
        let mut change = format!("DROP INDEX apalis.{};", index.name());
        match drift {
            IndexDrift::MissingTieBreaker => change.push_str(&index.definition("")),
            IndexDrift::AscendingTieBreaker => change.push_str(&index.definition(", id ASC")),
            IndexDrift::MissingIndex => {}
        }
        sql(&pool, change).await?;
        let mut out = Observations::new();
        out.check("every migration remains recorded despite index drift", count(&pool, "SELECT count(*)::bigint AS n FROM apalis_diesel_postgres.__diesel_schema_migrations").await? == 13);
        let verification = verify_schema(&pool).await;
        out.check(&format!("verify identifies only the damaged listing index: {verification:?}"), reports_index_contract(&verification, index));
        let repeated_setup = setup(&pool).await;
        out.check(&format!("setup rejects the same structural damage: {repeated_setup:?}"), reports_index_contract(&repeated_setup, index));
        out.check("rejected setup releases the migration advisory lock", locks(&pool).await? == 0);
        Ok(out)
    }).await
}

// Hold the registration row while the real SQL function tries to claim. A
// third backend can lock the job NOWAIT only if the function waits on the
// worker before taking any job lock; no copied claim/recovery algorithm.
async fn sql_claim_lock_order() -> Result<Outcome<Observations>, String> {
    with_isolated_database(|url| async move {
        let pool = pool(&url)?;
        setup(&pool).await.map_err(|e|e.to_string())?;
        sql(&pool,"INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES('worker','queue','fixture'); INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,done_at) VALUES('claim','queue',decode('2222','hex'),'Failed',1,3,'2020-01-01')").await?;
        let blocker_url=url.clone();
        let blocker=tokio::task::spawn_blocking(move || {
            let mut conn=PgConnection::establish(&blocker_url).map_err(|e|e.to_string())?;
            conn.batch_execute("BEGIN; SELECT id FROM apalis.workers WHERE id='worker' AND worker_type='queue' FOR UPDATE").map_err(|e|e.to_string())?;
            Ok::<_,String>(conn)
        }).await.map_err(|e|e.to_string())??;
        let claimant=self::pool(&url)?;
        sql(&claimant,"SET application_name='apalis_sql_claim_order'").await?;
        let claim=tokio::spawn(async move { count(&claimant,"SELECT count(*)::bigint AS n FROM apalis.get_jobs('worker','queue',1)").await });
        let waiting=tokio::time::timeout(std::time::Duration::from_secs(5),async {
            loop {
                if count(&pool,"SELECT count(*)::bigint AS n FROM pg_stat_activity WHERE datname=current_database() AND application_name='apalis_sql_claim_order' AND wait_event_type='Lock'").await? == 1 { return Ok::<_,String>(()); }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }).await;
        let job_is_available=with_conn(pool.clone(),|conn| {
            Ok(conn.transaction::<_,diesel::result::Error,_>(|conn| {
                sql_query("SELECT id FROM apalis.jobs WHERE id='claim' FOR UPDATE NOWAIT").execute(conn)
            }).is_ok())
        }).await?;
        tokio::task::spawn_blocking(move || { let mut blocker=blocker;blocker.batch_execute("ROLLBACK").map_err(|e|e.to_string()) }).await.map_err(|e|e.to_string())??;
        let claimed=claim.await.map_err(|e|e.to_string())??;
        let mut out=Observations::new();
        out.check("claim was observed waiting for a registration lock",matches!(waiting,Ok(Ok(()))));
        out.check("waiting claim has not locked any job",job_is_available);
        out.check("claim proceeds after registration lock release",claimed==1);
        out.check("fresh queued attempt clears prior completion",count(&pool,"SELECT count(*)::bigint AS n FROM apalis.jobs WHERE id='claim' AND status='Queued' AND attempts=1 AND done_at IS NULL").await?==1);
        Ok(out)
    }).await
}

lets_expect! { #tokio_test
    expect(migration_scenario(kind).await) as schema_upgrade {
        when(kind=Scenario::Fresh) as the_database_is_empty { to creates_and_verifies_the_owned_schema { satisfies_contract() } }
        when(kind=Scenario::ForeignHistory) as an_application_owns_the_public_journal { to preserves_foreign_history_and_applies_every_migration { satisfies_contract() } }
        when(kind=Scenario::TemporaryJournal) as a_temp_table_shadows_the_journal { to uses_only_the_private_journal { satisfies_contract() } }
        when(kind=Scenario::ForgedHistory) as private_versions_exist_without_runtime_tables { to rejects_the_forged_state { satisfies_contract() } }
        when(kind=Scenario::Baseline) as the_released_crate_schema_has_external_dependants { to adopts_the_complete_baseline_without_destructive_replay { satisfies_contract() } }
        when(kind=Scenario::Initial) as the_initial_crate_schema_has_external_dependants { to preserves_the_composite_fk_through_hardening { satisfies_contract() } }
        when(kind=Scenario::Legacy) as the_released_upstream_schema_contains_lost_owners { to preserves_terminal_history_and_recovers_only_active_work { satisfies_contract() } }
        when(kind=Scenario::ExternalLegacy) as an_external_fk_requires_the_legacy_identity { to refuses_safely_and_allows_an_explicitly_resolved_retry { satisfies_contract() } }
        when(kind=Scenario::Unsupported) as the_schema_is_an_unknown_partial_install { to refuses_without_altering_existing_data { satisfies_contract() } }
        when(kind=Scenario::DriftColumn) as a_required_column_is_missing { to rejects_current_history_with_structural_damage { satisfies_contract() } }
        when(kind=Scenario::DriftConstraint) as a_named_constraint_has_the_wrong_rule { to rejects_current_history_with_a_weakened_invariant { satisfies_contract() } }
        when(kind=Scenario::DriftIndex) as the_deduplication_index_is_not_unique { to rejects_current_history_without_enqueue_uniqueness { satisfies_contract() } }
        when(kind=Scenario::Concurrent) as eight_independent_backends_start_together { to serializes_the_entire_migration_series { satisfies_contract() } }
        when(kind=Scenario::Contended) as eight_callers_share_one_connection { to completes_every_call_without_leaking_the_lock { satisfies_contract() } }
        when(kind=Scenario::DowngradeCollision) as worker_ids_repeat_across_queues { to refuses_downgrade_without_deleting_registrations { satisfies_contract() } }
        when(kind=Scenario::DowngradeUnique) as worker_ids_are_globally_unique { to preserves_workers_through_downgrade_and_upgrade { satisfies_contract() } }
    }
    expect(sql_claim_lock_order().await) as legacy_claim_lock_order {
        when registration_is_locked_by_another_backend {
            to waits_before_locking_jobs_and_clears_completion_on_the_new_attempt { satisfies_contract() }
        }
    }
    expect(panicking_setup().await) as migration_panic {
        when a_late_migration_panics { to rolls_back_the_whole_series_and_recovers_the_pool { satisfies_contract() } }
    }
    expect(listing_index_upgrade(panics).await) as listing_index_upgrade {
        let panics = false;
        to preserves_data_and_history_through_upgrade_downgrade_and_reupgrade { satisfies_contract() }
        when delivery_of_the_migration_result_panics {
            let panics = true;
            to restores_the_previous_schema_and_allows_the_same_pool_to_retry { satisfies_contract() }
        }
    }
    expect(reconciliation_downgrade(install).await) as schema_contract_downgrade {
        let install = Install::Fresh;
        when the_reconciliation_series_is_reverted {
            to restores_the_previous_generation_and_upgrades_again { satisfies_contract() }
            when the_database_was_installed_by_the_released_crate {
                let install = Install::Released;
                to restores_exactly_the_schema_that_release_installed { satisfies_contract() }
            }
        }
    }
    expect(listing_index_verification(index, drift).await) as listing_index_verification {
        let index = ListingIndex::Queue;
        let drift = IndexDrift::MissingTieBreaker;
        to rejects_the_damaged_index_despite_complete_migration_history { satisfies_contract() }
        when the_tie_breaker_is_ascending {
            let drift = IndexDrift::AscendingTieBreaker;
            to rejects_the_damaged_index_despite_complete_migration_history { satisfies_contract() }
        }
        when the_index_is_missing {
            let drift = IndexDrift::MissingIndex;
            to rejects_the_damaged_index_despite_complete_migration_history { satisfies_contract() }
        }
        when the_view_covers_all_queues {
            let index = ListingIndex::Global;
            to rejects_the_damaged_index_despite_complete_migration_history { satisfies_contract() }
            when the_tie_breaker_is_ascending {
                let drift = IndexDrift::AscendingTieBreaker;
                to rejects_the_damaged_index_despite_complete_migration_history { satisfies_contract() }
            }
            when the_index_is_missing {
                let drift = IndexDrift::MissingIndex;
                to rejects_the_damaged_index_despite_complete_migration_history { satisfies_contract() }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, QueryableByName)]
struct SqlClaimRow {
    #[diesel(sql_type = diesel::sql_types::Text)]
    id: String,
    #[diesel(sql_type = diesel::sql_types::Text)]
    status: String,
    #[diesel(sql_type = diesel::sql_types::Integer)]
    attempts: i32,
    #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Text>)]
    lock_by: Option<String>,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    locked: bool,
    #[diesel(sql_type = diesel::sql_types::Bool)]
    unfinished: bool,
}

async fn downgraded_claim_batch(
    pool: &PgPool,
    revisits_candidates: bool,
) -> Result<Vec<SqlClaimRow>, String> {
    with_conn(pool.clone(), move |conn| {
        conn.transaction::<_, diesel::result::Error, _>(|conn| {
            if revisits_candidates {
                conn.batch_execute(
                    "SET LOCAL enable_hashagg=off; SET LOCAL enable_hashjoin=off; \
                     SET LOCAL enable_mergejoin=off; SET LOCAL enable_material=off; \
                     SET LOCAL enable_sort=off",
                )?;
            }
            sql_query(
                "SELECT id, status, attempts, lock_by, lock_at IS NOT NULL AS locked, \
                 done_at IS NULL AS unfinished \
                 FROM apalis.get_jobs('bounded-worker','bounded-queue',1000)",
            )
            .load::<SqlClaimRow>(conn)
        })
        .map_err(|error| error.to_string())
    })
    .await
}

async fn bounded_downgraded_claim(
    revisits_candidates: bool,
) -> Result<Outcome<Observations>, String> {
    with_isolated_database(move |url| async move {
        let claimant = pool(&url)?;
        setup(&claimant).await.map_err(|error| error.to_string())?;
        with_conn(claimant.clone(), |conn| {
            conn.transaction::<_, diesel::result::Error, _>(|conn| {
                conn.batch_execute(include_str!(
                    "../migrations/20260521000000_harden_apalis_sql/down.sql"
                ))
            })
            .map_err(|error| error.to_string())
        })
        .await?;
        sql(&claimant, "INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES ('bounded-worker','bounded-queue','fixture'); INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at) SELECT 'job-'||n,'bounded-queue',decode('2222','hex'),'Pending',0,3,'2020-01-01'::timestamptz FROM generate_series(1,1005) n; ANALYZE apalis.jobs").await?;
        // This second pool reads only committed effects from a different backend.
        let observer = pool(&url)?;
        let mut out = Observations::new();
        out.check(
            "durable observations use an independent backend",
            count(&claimant, "SELECT pg_backend_pid()::bigint AS n").await?
                != count(&observer, "SELECT pg_backend_pid()::bigint AS n").await?,
        );
        let all_ids: std::collections::BTreeSet<_> =
            (1..=1005).map(|id| format!("job-{id}")).collect();
        let mut claimed_ids = std::collections::BTreeSet::new();
        for (call, expected) in [1000, 5, 0].into_iter().enumerate() {
            let returned = downgraded_claim_batch(&claimant, revisits_candidates).await?;
            out.check(
                &format!("claim {} returns {expected} jobs, observed {}", call + 1, returned.len()),
                returned.len() == expected,
            );
            out.check(
                "returned jobs retain attempts and identify the queued owner",
                returned.iter().all(|row| {
                    row.status == "Queued"
                        && row.attempts == 0
                        && row.lock_by.as_deref() == Some("bounded-worker")
                        && row.locked
                        && row.unfinished
                }),
            );
            out.check(
                "a subsequent claim never returns an earlier job",
                returned.iter().all(|row| claimed_ids.insert(row.id.clone())),
            );
            let durable = with_conn(observer.clone(), |conn| {
                sql_query("SELECT id, status, attempts, lock_by, lock_at IS NOT NULL AS locked, done_at IS NULL AS unfinished FROM apalis.jobs ORDER BY id")
                    .load::<SqlClaimRow>(conn)
                    .map_err(|error| error.to_string())
            }).await?;
            out.check(
                "every original job survives each committed claim",
                durable.iter().map(|row| row.id.clone()).collect::<std::collections::BTreeSet<_>>() == all_ids,
            );
            out.check(
                "only returned jobs have committed queued ownership",
                durable.iter().all(|row| {
                    row.attempts == 0 && row.unfinished && if claimed_ids.contains(&row.id) {
                        row.status == "Queued"
                            && row.lock_by.as_deref() == Some("bounded-worker")
                            && row.locked
                    } else {
                        row.status == "Pending" && row.lock_by.is_none() && !row.locked
                    }
                }),
            );
            let expected_queued = if call == 0 { 1000 } else { 1005 };
            out.check(
                &format!("committed claim {} leaves exactly {expected_queued} queued jobs", call + 1),
                durable.iter().filter(|row| row.status == "Queued").count() == expected_queued,
            );
        }
        Ok(out)
    })
    .await
}

lets_expect! { #tokio_test
    expect(bounded_downgraded_claim(revisits_candidates).await) as bounded_downgraded_claim {
        let revisits_candidates = false;
        to caps_each_batch_and_preserves_the_remaining_jobs { satisfies_contract() }
        when the_database_prefers_repeated_candidate_scans {
            let revisits_candidates = true;
            to caps_each_batch_and_preserves_the_remaining_jobs { satisfies_contract() }
        }
    }
}

#[derive(Clone, Copy)]
enum UnjournaledGeneration {
    KnownFinal,
    Latest,
}

#[derive(Debug)]
struct ObserveWorkerMigration {
    worker: Arc<AtomicUsize>,
    owner: Arc<AtomicUsize>,
}
impl CustomizeConnection<PgConnection, diesel::r2d2::Error> for ObserveWorkerMigration {
    fn on_acquire(&self, conn: &mut PgConnection) -> Result<(), diesel::r2d2::Error> {
        let worker_calls = self.worker.clone();
        let owner_calls = self.owner.clone();
        conn.set_instrumentation(move |event: InstrumentationEvent<'_>| {
            if let InstrumentationEvent::FinishQuery {
                query, error: None, ..
            } = event
            {
                let query = query.to_string();
                if query.contains("CREATE OR REPLACE FUNCTION apalis.get_jobs")
                    && query.contains("FOR KEY SHARE")
                {
                    if query.contains("worker_id must not be null") {
                        owner_calls.fetch_add(1, Ordering::SeqCst);
                    } else {
                        worker_calls.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        });
        Ok(())
    }
}

async fn unjournaled_final_schema(
    generation: UnjournaledGeneration,
    public_history: bool,
    concurrent: bool,
    damaged: bool,
) -> Result<Outcome<Observations>, String> {
    with_isolated_database(move |url| async move {
        let observer = pool(&url)?;
        // Exercise the old public API using the real embedded SQL, without
        // calling setup and without manufacturing private migration records.
        with_conn(observer.clone(), move |conn| {
            let mut migrations = conn.pending_migrations(MIGRATIONS).map_err(|e| e.to_string())?;
            if matches!(generation, UnjournaledGeneration::KnownFinal) {
                migrations.retain(|migration| migration.name().version().to_string().as_str() <= LISTING_MIGRATION_VERSION);
            }
            conn.run_migrations(&migrations).map_err(|e| e.to_string())?;
            Ok(())
        }).await?;
        let mut out = Observations::new();
        let expected_public_count = if matches!(generation, UnjournaledGeneration::KnownFinal) { 11 } else { 13 };
        out.check("raw harness applied the intended generation", count(&observer, "SELECT count(*)::bigint AS n FROM public.__diesel_schema_migrations").await? == expected_public_count);
        if !public_history {
            sql(&observer, "DROP TABLE public.__diesel_schema_migrations").await?;
        }
        if damaged {
            sql(&observer, "DROP INDEX apalis.jobs_list_all_idx").await?;
        }
        sql(&observer, "INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES ('preserved-worker','preserved-queue','fixture'); INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at,done_at,last_result) VALUES ('preserved-job','preserved-queue',decode('2222','hex'),'Done',1,3,'2024-01-01','2024-01-02','{\"Ok\":\"retained\"}')").await?;
        let data_query = "SELECT jsonb_build_object('jobs',(SELECT jsonb_agg(to_jsonb(j) ORDER BY id) FROM apalis.jobs j),'workers',(SELECT jsonb_agg(to_jsonb(w) ORDER BY id,worker_type) FROM apalis.workers w))::text AS value";
        let public_query = "SELECT jsonb_agg(to_jsonb(h) ORDER BY version)::text AS value FROM public.__diesel_schema_migrations h";
        let before_data = catalog_text(&observer, data_query).await?;
        let before_public = if public_history { Some(catalog_text(&observer, public_query).await?) } else { None };
        out.check("fixture has no private namespace", absent_private_schema(&observer).await?);
        let migration_calls = Arc::new(AtomicUsize::new(0));
        let owner_migration_calls = Arc::new(AtomicUsize::new(0));
        let callers = if concurrent { 4 } else { 1 };
        let barrier = Arc::new(tokio::sync::Barrier::new(callers));
        let mut racers = Vec::new();
        for _ in 0..callers {
            let calls = migration_calls.clone();
            let owner_calls = owner_migration_calls.clone();
            let migrator = build_pool_with(&url, |builder| builder.max_size(1).connection_customizer(Box::new(ObserveWorkerMigration { worker: calls, owner: owner_calls }))).map_err(|e| e.to_string())?;
            let barrier = barrier.clone();
            racers.push(async move { barrier.wait().await; setup(&migrator).await });
        }
        let results = futures::future::join_all(racers).await;
        if damaged {
            out.check("unsupported damage is reported", results.iter().all(|result| matches!(result, Err(Error::Migration(error)) if error.to_string().contains("jobs_list_all_idx"))));
            out.check("rejected adoption rolls back its private namespace", absent_private_schema(&observer).await?);
            out.check("rejected adoption never executes later migrations", migration_calls.load(Ordering::SeqCst) == 0 && owner_migration_calls.load(Ordering::SeqCst) == 0);
        } else {
            out.check("every setup accepts the complete catalog", results.iter().all(Result::is_ok));
            out.check("adopted schema verifies", verify_schema(&observer).await.is_ok());
            out.check("fixed eleven-version adoption executes each later migration exactly once", migration_calls.load(Ordering::SeqCst) == 1 && owner_migration_calls.load(Ordering::SeqCst) == 1);
            out.check("private history records all thirteen migrations", count(&observer, "SELECT count(*)::bigint AS n FROM apalis_diesel_postgres.__diesel_schema_migrations").await? == 13);
            out.check("the upgraded SQL function rejects NULL ownership", catalog_text(&observer, "SELECT pg_get_functiondef('apalis.get_jobs(text,text,integer)'::regprocedure)::text AS value").await?.contains("worker_id must not be null"));
            out.check("the upgraded SQL function uses the current worker lock", catalog_text(&observer, "SELECT pg_get_functiondef('apalis.get_jobs(text,text,integer)'::regprocedure)::text AS value").await?.contains("FOR KEY SHARE"));
            let private_query = "SELECT jsonb_agg(to_jsonb(h) ORDER BY version)::text AS value FROM apalis_diesel_postgres.__diesel_schema_migrations h";
            let before_repeat = catalog_text(&observer, private_query).await?;
            out.check("subsequent setup succeeds", setup(&observer).await.is_ok());
            out.check("subsequent setup preserves all private timestamps", catalog_text(&observer, private_query).await? == before_repeat);
        }
        out.check("all application data survives", catalog_text(&observer, data_query).await? == before_data);
        if let Some(before_public) = before_public {
            out.check("every foreign journal row and timestamp survives", catalog_text(&observer, public_query).await? == before_public);
        } else {
            out.check("setup does not create a public journal", count(&observer, "SELECT (to_regclass('public.__diesel_schema_migrations') IS NULL)::integer::bigint AS n").await? == 1);
        }
        out.check("setup releases the migration lock", locks(&observer).await? == 0);
        Ok(out)
    }).await
}

#[derive(Debug)]
struct AssumeMigrationRole(String);
impl CustomizeConnection<PgConnection, diesel::r2d2::Error> for AssumeMigrationRole {
    fn on_acquire(&self, conn: &mut PgConnection) -> Result<(), diesel::r2d2::Error> {
        conn.batch_execute(&format!("SET ROLE \"{}\"", self.0))
            .map_err(diesel::r2d2::Error::QueryError)
    }
}

async fn namespace_privileges(
    namespace_exists: bool,
    database_create: bool,
) -> Result<Outcome<Observations>, String> {
    with_isolated_database(move |url| async move {
        let owner = pool(&url)?;
        let role = format!("apalis_migration_role_{}", ulid::Ulid::new().to_string().to_lowercase());
        sql(&owner, format!("CREATE ROLE \"{role}\" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION")).await?;
        // Keep all fallible scenario work inside this future so cleanup also
        // executes on an infrastructure error, before the isolated DB is removed.
        let result = async {
            if namespace_exists {
                setup(&owner).await.map_err(|e| e.to_string())?;
                sql(&owner, format!("GRANT USAGE, CREATE ON SCHEMA apalis, apalis_diesel_postgres TO \"{role}\"; GRANT ALL ON ALL TABLES IN SCHEMA apalis, apalis_diesel_postgres TO \"{role}\"")).await?;
            }
            let database = catalog_text(&owner, "SELECT current_database()::text AS value").await?;
            if database_create {
                // Both identifiers are created by this fixture, not caller input.
                sql(&owner, format!("GRANT CREATE ON DATABASE \"{database}\" TO \"{role}\"")).await?;
            }
            let caller = build_pool_with(&url, |builder| builder.max_size(1).connection_customizer(Box::new(AssumeMigrationRole(role.clone())))).map_err(|e| e.to_string())?;
            let mut out = Observations::new();
            out.check("caller has exactly the requested database CREATE privilege", count(&caller, "SELECT has_database_privilege(current_user,current_database(),'CREATE')::integer::bigint AS n").await? == i64::from(database_create));
            let setup_result = setup(&caller).await;
            if namespace_exists || database_create {
                out.check("setup needs CREATE only for an absent namespace", setup_result.is_ok());
                out.check("the resulting schema verifies for the same role", verify_schema(&caller).await.is_ok());
            } else {
                out.check("an absent namespace reports the missing privilege", matches!(setup_result, Err(Error::Migration(error)) if error.to_string().contains("permission denied for database")));
                out.check("permission failure creates no private namespace", absent_private_schema(&owner).await?);
            }
            out.check("permission handling releases the advisory lock", locks(&owner).await? == 0);
            drop(caller);
            Ok::<_, String>(out)
        }.await;
        let cleanup = sql(&owner, format!("DROP OWNED BY \"{role}\"; DROP ROLE \"{role}\"")).await;
        match (result, cleanup) {
            (Ok(out), Ok(())) => Ok(out),
            (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
            (Err(error), Err(cleanup)) => Err(format!("{error}; role cleanup also failed: {cleanup}")),
        }
    }).await
}

lets_expect! { #tokio_test
    expect(unjournaled_final_schema(generation, public_history, concurrent, false).await) as adopting_a_complete_unjournaled_schema {
        let generation = UnjournaledGeneration::KnownFinal;
        let public_history = true;
        let concurrent = false;
        to preserves_data_and_foreign_history_and_runs_later_migrations { satisfies_contract() }
        when four_migration_replicas_start_together {
            let concurrent = true;
            to adopts_once_and_completes_every_replica { satisfies_contract() }
        }
        when no_public_history_exists {
            let public_history = false;
            to adopts_from_the_verified_catalog { satisfies_contract() }
            when four_migration_replicas_start_together {
                let concurrent = true;
                to adopts_once_and_completes_every_replica { satisfies_contract() }
            }
        }
        when the_raw_harness_has_already_applied_later_migrations {
            let generation = UnjournaledGeneration::Latest;
            to safely_reapplies_only_migrations_after_the_known_adoption_generation { satisfies_contract() }
            when four_migration_replicas_start_together {
                let concurrent = true;
                to adopts_once_and_completes_every_replica { satisfies_contract() }
            }
            when no_public_history_exists {
                let public_history = false;
                to adopts_from_the_verified_catalog { satisfies_contract() }
                when four_migration_replicas_start_together {
                    let concurrent = true;
                    to adopts_once_and_completes_every_replica { satisfies_contract() }
                }
            }
        }
    }
    expect(unjournaled_final_schema(UnjournaledGeneration::Latest, public_history, false, true).await) as adopting_a_damaged_unjournaled_schema {
        let public_history = true;
        to refuses_without_blessing_history_or_modifying_data { satisfies_contract() }
        when no_public_history_exists {
            let public_history = false;
            to refuses_without_creating_private_history { satisfies_contract() }
        }
    }
    expect(namespace_privileges(namespace_exists, database_create).await) as migration_namespace_privileges {
        let namespace_exists = true;
        let database_create = true;
        to reuses_the_existing_namespace { satisfies_contract() }
        when database_create_is_denied {
            let database_create = false;
            to reuses_the_existing_namespace_without_extra_privileges { satisfies_contract() }
        }
        when the_private_namespace_is_absent {
            let namespace_exists = false;
            to creates_and_initializes_the_namespace { satisfies_contract() }
            when database_create_is_denied {
                let database_create = false;
                to rejects_creation_without_leaving_a_namespace_or_lock { satisfies_contract() }
            }
        }
    }
}

const ACTIVE_OWNER_MIGRATION_VERSION: &str = "20260912000001";

// Apply the real previous embedded generation before inserting states that the
// next migration deliberately makes unrepresentable. A public journal models
// the formerly supported raw Diesel harness; setup must leave it untouched.
async fn previous_owner_generation(pool: &PgPool, private: bool) -> Result<(), String> {
    with_conn(pool.clone(), move |conn| {
        conn.transaction::<_, Error, _>(|conn| {
            if private {
                conn.batch_execute("CREATE SCHEMA apalis_diesel_postgres; SET LOCAL search_path=apalis_diesel_postgres,pg_catalog,pg_temp")
                    .map_err(|e| Error::Migration(Box::new(e)))?;
            }
            let mut migrations=conn.pending_migrations(MIGRATIONS).map_err(Error::Migration)?;
            migrations.retain(|migration|migration.name().version().to_string().as_str()<ACTIVE_OWNER_MIGRATION_VERSION);
            conn.run_migrations(&migrations).map_err(Error::Migration)?;
            Ok(())
        }).map_err(|e|e.to_string())
    }).await
}

async fn owner_rows(pool: &PgPool) -> Result<serde_json::Value, String> {
    let text = catalog_text(
        pool,
        "SELECT jsonb_object_agg(id,to_jsonb(j))::text AS value FROM apalis.jobs j",
    )
    .await?;
    serde_json::from_str(&text).map_err(|e| e.to_string())
}

const LOST_OWNER_ROWS:&str="INSERT INTO apalis.workers(id,worker_type,storage_name) VALUES('retained-owner','owner-queue','fixture');
INSERT INTO apalis.jobs(id,job_type,job,status,attempts,max_attempts,run_at,lock_by,lock_at,done_at,last_result) VALUES
('room','owner-queue',decode('2222','hex'),'Queued',0,3,'2000-01-01',NULL,'2000-01-01',NULL,NULL),
('exhausted','owner-queue',decode('2222','hex'),'Running',3,3,'2000-01-01',NULL,'2000-01-01',NULL,NULL),
('max-remaining','owner-queue',decode('2222','hex'),'Running',2147483646,2147483647,'2000-01-01',NULL,'2000-01-01',NULL,NULL),
('max-exhausted','owner-queue',decode('2222','hex'),'Running',2147483647,2147483647,'2000-01-01',NULL,'2000-01-01',NULL,NULL),
('valid','owner-queue',decode('2222','hex'),'Running',1,3,'2000-01-01','retained-owner','2000-01-01',NULL,'{\"Ok\":\"retained\"}'),
('done','owner-queue',decode('2222','hex'),'Done',1,3,'2000-01-01',NULL,NULL,'2000-01-02','{\"Ok\":\"history\"}');";

async fn rejects_active_ownerless_insert(pool: &PgPool) -> Result<bool, String> {
    let error=sql(pool,"INSERT INTO apalis.jobs(id,job_type,job,status) VALUES('invalid-owner','owner-queue',decode('2222','hex'),'Running')").await.err();
    Ok(
        error.is_some_and(|error| error.contains("jobs_active_owner_check"))
            && count(
                pool,
                "SELECT count(*)::bigint AS n FROM apalis.jobs WHERE id='invalid-owner'",
            )
            .await?
                == 0,
    )
}

#[derive(Clone, Copy)]
enum OwnerScenario {
    PrivatePrevious,
    PublicPrevious,
    MissingConstraint,
    WrongConstraint,
    UnvalidatedConstraint,
    WrongUnjournaledConstraint,
    UnvalidatedUnjournaledConstraint,
    LatestDown,
}

async fn owner_schema_scenario(kind: OwnerScenario) -> Result<Outcome<Observations>, String> {
    with_isolated_database(move|url|async move{
        let pool=pool(&url)?;
        let private=!matches!(kind,OwnerScenario::PublicPrevious);
        previous_owner_generation(&pool,private).await?;
        let function_query="SELECT pg_get_functiondef('apalis.get_jobs(text,text,integer)'::regprocedure)::text AS value";
        let previous_function=catalog_text(&pool,function_query).await?;
        let foreign_history=if private{None}else{Some(catalog_text(&pool,"SELECT jsonb_agg(to_jsonb(h) ORDER BY version)::text AS value FROM public.__diesel_schema_migrations h").await?)};
        sql(&pool,LOST_OWNER_ROWS).await?;
        let before=owner_rows(&pool).await?;
        setup(&pool).await.map_err(|e|e.to_string())?;
        let after=owner_rows(&pool).await?;
        let mut out=Observations::new();
        out.check("the upgraded schema verifies",verify_schema(&pool).await.is_ok());
        out.check("a lost queued execution with remaining budget becomes pending",
            after["room"]["status"]=="Pending"&&after["room"]["attempts"]==1&&after["room"]["lock_by"].is_null()
            &&after["room"]["lock_at"].is_null()&&after["room"]["done_at"].is_null()&&after["room"]["last_result"]["Err"].is_string());
        for(id,attempts)in[("exhausted",3),("max-remaining",i32::MAX),("max-exhausted",i32::MAX)]{
            out.check(&format!("lost execution {id} terminates within its attempt budget"),
                after[id]["status"]=="Killed"&&after[id]["attempts"]==attempts&&after[id]["lock_by"].is_null()
                &&after[id]["lock_at"].is_null()&&after[id]["done_at"].is_string()&&after[id]["last_result"]["Err"].is_string());
        }
        out.check("all fields of valid ownership and terminal history remain unchanged",before["valid"]==after["valid"]&&before["done"]==after["done"]);
        out.check("the current schema rejects active NULL ownership without inserting a row",rejects_active_ownerless_insert(&pool).await?);
        let history_query="SELECT jsonb_agg(to_jsonb(h) ORDER BY version)::text AS value FROM apalis_diesel_postgres.__diesel_schema_migrations h";
        let history=catalog_text(&pool,history_query).await?;
        setup(&pool).await.map_err(|e|e.to_string())?;
        out.check("repeated setup preserves every row and journal timestamp",after==owner_rows(&pool).await?&&history==catalog_text(&pool,history_query).await?);
        if let Some(foreign_history)=foreign_history{
            out.check("adoption preserves all foreign versions and timestamps",foreign_history==catalog_text(&pool,"SELECT jsonb_agg(to_jsonb(h) ORDER BY version)::text AS value FROM public.__diesel_schema_migrations h").await?);
        }
        match kind{
            OwnerScenario::PrivatePrevious|OwnerScenario::PublicPrevious=>{},
            OwnerScenario::LatestDown=>{
                let function=catalog_text(&pool,function_query).await?;
                with_conn(pool.clone(),|conn|conn.transaction::<_,diesel::result::Error,_>(|conn|{
                    conn.batch_execute(include_str!("../migrations/20260912000001_require_active_owner/down.sql"))?;
                    conn.batch_execute("DELETE FROM apalis_diesel_postgres.__diesel_schema_migrations WHERE version='20260912000001'")
                }).map_err(|e|e.to_string())).await?;
                out.check("verification detects the pending active-owner migration",matches!(verify_schema(&pool).await,Err(Error::Migration(_))));
                out.check("down removes only the new constraint",count(&pool,"SELECT count(*)::bigint AS n FROM pg_constraint WHERE conrelid='apalis.jobs'::regclass AND conname='jobs_active_owner_check'").await?==0);
                out.check("down restores the complete previous function before reupgrade",previous_function==catalog_text(&pool,function_query).await?);
                setup(&pool).await.map_err(|e|e.to_string())?;
                out.check("reapplying restores the complete current function body",function==catalog_text(&pool,function_query).await?);
                out.check("reapplying validates the constraint without changing repaired history",verify_schema(&pool).await.is_ok()&&after==owner_rows(&pool).await?&&rejects_active_ownerless_insert(&pool).await?);
            },
            _=>{
                sql(&pool,"ALTER TABLE apalis.jobs DROP CONSTRAINT jobs_active_owner_check").await?;
                match kind{
                    OwnerScenario::WrongConstraint|OwnerScenario::WrongUnjournaledConstraint=>sql(&pool,"ALTER TABLE apalis.jobs ADD CONSTRAINT jobs_active_owner_check CHECK(true)").await?,
                    OwnerScenario::UnvalidatedConstraint|OwnerScenario::UnvalidatedUnjournaledConstraint=>sql(&pool,"ALTER TABLE apalis.jobs ADD CONSTRAINT jobs_active_owner_check CHECK(status NOT IN('Queued','Running') OR lock_by IS NOT NULL) NOT VALID").await?,
                    _=>{},
                }
                let unjournaled=matches!(kind,OwnerScenario::WrongUnjournaledConstraint|OwnerScenario::UnvalidatedUnjournaledConstraint);
                if unjournaled{sql(&pool,"DROP SCHEMA apalis_diesel_postgres CASCADE").await?;}
                out.check("verification rejects the damaged invariant",matches!(verify_schema(&pool).await,Err(Error::Migration(_))));
                out.check("setup refuses the damaged invariant",matches!(setup(&pool).await,Err(Error::Migration(_))));
                out.check("refusal leaves all job history unchanged",after==owner_rows(&pool).await?);
                if unjournaled{out.check("refused adoption leaves no private namespace",absent_private_schema(&pool).await?);}
                else{out.check("refusal preserves the current private history",history==catalog_text(&pool,history_query).await?);}
            }
        }
        out.check("no migration lock survives",locks(&pool).await?==0);
        Ok(out)
    }).await
}

async fn runtime_catalog(pool: &PgPool) -> Result<String, String> {
    catalog_text(pool,"SELECT jsonb_build_object(
        'functions',(SELECT jsonb_object_agg(proname,pg_get_functiondef(oid)) FROM pg_proc WHERE pronamespace='apalis'::regnamespace AND proname IN('get_jobs','notify_new_jobs')),
        'indexes',(SELECT jsonb_agg(indexdef ORDER BY indexname) FROM pg_indexes WHERE schemaname='apalis'),
        'constraints',(SELECT jsonb_agg(pg_get_constraintdef(oid) ORDER BY conrelid::regclass::text,conname) FROM pg_constraint WHERE connamespace='apalis'::regnamespace),
        'versions',(SELECT jsonb_agg(version ORDER BY version) FROM apalis_diesel_postgres.__diesel_schema_migrations))::text AS value").await
}

async fn completed_generation(released: bool) -> Result<Outcome<String>, String> {
    with_isolated_database(move |url| async move {
        let pool = pool(&url)?;
        if released {
            sql(
                &pool,
                include_str!("fixtures/apalis-diesel-postgres-0.4.1.sql"),
            )
            .await?;
        }
        setup(&pool).await.map_err(|e| e.to_string())?;
        verify_schema(&pool).await.map_err(|e| e.to_string())?;
        let first = runtime_catalog(&pool).await?;
        setup(&pool).await.map_err(|e| e.to_string())?;
        let repeated = runtime_catalog(&pool).await?;
        if first != repeated {
            return Err("repeated setup changed the runtime catalog".into());
        }
        Ok(first)
    })
    .await
}

async fn released_and_fresh_equivalence() -> Result<Outcome<Observations>, String> {
    match (
        completed_generation(false).await?,
        completed_generation(true).await?,
    ) {
        (Outcome::Completed(fresh), Outcome::Completed(released)) => {
            let mut out = Observations::new();
            out.check("fresh and released upgrades have identical functions, indexes, constraints and versions",fresh==released);
            Ok(Outcome::Completed(out))
        }
        (Outcome::Skipped, Outcome::Skipped) => Ok(Outcome::Skipped),
        _ => Err("database availability changed between the two schema origins".into()),
    }
}

async fn untrusted_notify_search_path() -> Result<Outcome<Observations>, String> {
    with_isolated_database(|url| async move {
        let pool = pool(&url)?;
        setup(&pool).await.map_err(|e| e.to_string())?;
        sql(
            &pool,
            "ALTER FUNCTION apalis.notify_new_jobs() RESET search_path",
        )
        .await?;
        let mut out = Observations::new();
        out.check(
            "verify rejects a current function without the required search_path",
            matches!(verify_schema(&pool).await, Err(Error::Migration(_))),
        );
        out.check(
            "setup rejects the same unsafe function attributes",
            matches!(setup(&pool).await, Err(Error::Migration(_))),
        );
        out.check(
            "rejected setup releases the advisory lock",
            locks(&pool).await? == 0,
        );
        Ok(out)
    })
    .await
}

lets_expect! { #tokio_test
    expect(owner_schema_scenario(kind).await) as upgrading_active_owner_invariants{
        let kind=OwnerScenario::PrivatePrevious;
        to repairs_only_lost_active_executions_and_preserves_other_history{satisfies_contract()}
        when the_previous_generation_has_only_a_public_journal{
            let kind=OwnerScenario::PublicPrevious;
            to adopts_then_repairs_without_rewriting_foreign_history{satisfies_contract()}
        }
    }
    expect(owner_schema_scenario(kind).await) as verifying_active_owner_invariants{
        let kind=OwnerScenario::MissingConstraint;
        to rejects_the_missing_constraint_without_changing_history{satisfies_contract()}
        when the_constraint_definition_is_wrong{let kind=OwnerScenario::WrongConstraint;to rejects_the_wrong_definition{satisfies_contract()}}
        when the_constraint_is_not_validated{let kind=OwnerScenario::UnvalidatedConstraint;to refuses_to_treat_unchecked_rows_as_valid{satisfies_contract()}}
    }
    expect(owner_schema_scenario(kind).await) as adopting_an_invalid_active_owner_invariant{
        let kind=OwnerScenario::WrongUnjournaledConstraint;
        to refuses_the_wrong_definition_without_blessing_history{satisfies_contract()}
        when the_constraint_is_not_validated{let kind=OwnerScenario::UnvalidatedUnjournaledConstraint;to refuses_without_creating_private_history{satisfies_contract()}}
    }
    expect(owner_schema_scenario(OwnerScenario::LatestDown).await) as downgrading_active_owner_invariants{
        to restores_the_current_function_and_constraint_on_reupgrade{satisfies_contract()}
    }
    expect(released_and_fresh_equivalence().await) as the_released_and_fresh_schema{
        to converges_to_the_same_complete_runtime_catalog{satisfies_contract()}
    }
    expect(untrusted_notify_search_path().await) as unsafe_notification_function_attributes{
        to rejects_unsafe_attributes_during_verification_and_setup{satisfies_contract()}
    }
}
