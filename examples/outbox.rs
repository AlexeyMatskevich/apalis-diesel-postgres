//! End-to-end demo of the transactional outbox pattern.
//!
//! Run with a reachable PostgreSQL database:
//!
//! ```sh
//! DATABASE_URL=postgres://127.0.0.1:5432/apalis_diesel_postgres \
//!     cargo run --example outbox --features tokio
//! ```
//!
//! The example shows three behaviours, each on a single `&mut PgConnection`
//! shared between business data and the apalis task:
//!
//! 1. **Commit** — business INSERT and the apalis task are both visible after
//!    the outer transaction commits.
//! 2. **Rollback** — the same flow returning `Err` from the closure leaves
//!    both tables empty.
//! 3. **Idempotency conflict** — a duplicate `idempotency_key` surfaces
//!    `Error::InvalidArgument`; the surrounding business write still commits.
//!
//! Two separate r2d2 pools are built against the same database: a "backend"
//! pool that the imagined HTTP handler uses, and a smaller "apalis" pool that
//! the worker/sink machinery would normally consume. See
//! [`PostgresStorage::push_with_conn`] for the API surface, and the README
//! section "Connection pool isolation" for the sizing rationale.
//!
//! No apalis worker is started — running the queue consumer is orthogonal to
//! the outbox API and would only clutter this example. The console output
//! reports queue/business-table counts so the effect of each scenario is
//! visible without a worker.

use std::time::Duration;

use apalis_diesel_postgres::{
    Config, Error as PgError, PgPool, PgTask, PostgresStorage, build_pool_with, setup,
};
use diesel::{
    Connection, PgConnection, QueryableByName, RunQueryDsl, sql_query, sql_types::Text,
};
use ulid::Ulid;

const BUSINESS_TABLE: &str = "outbox_example_orders";

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct SendConfirmationEmail {
    order_id: String,
    to: String,
}

#[derive(QueryableByName, Debug)]
struct CountRow {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    n: i64,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let Ok(database_url) = std::env::var("DATABASE_URL") else {
        eprintln!(
            "set DATABASE_URL to a reachable PostgreSQL server before running this example, e.g.:\n\
             DATABASE_URL=postgres://127.0.0.1:5432/apalis_diesel_postgres cargo run --example outbox"
        );
        return Ok(());
    };

    // Two pools, one database. The backend pool is what the imagined HTTP
    // handler uses; the apalis pool is what the worker / lifecycle would
    // consume. They MUST be separate — sharing them is the recipe for
    // cascading failure described in the README.
    let backend_pool = build_pool_with(database_url.clone(), |builder| {
        builder
            .max_size(8)
            .connection_timeout(Duration::from_secs(2))
    })?;
    let apalis_pool = build_pool_with(database_url, |builder| {
        builder
            .max_size(4)
            .connection_timeout(Duration::from_secs(2))
    })?;

    setup(&apalis_pool).await?;

    let queue = format!("outbox-example-{}", Ulid::new());
    let storage = PostgresStorage::<SendConfirmationEmail>::new_with_config(
        &apalis_pool,
        &Config::new(&queue),
    );
    ensure_business_table(backend_pool.clone()).await?;
    cleanup(&backend_pool, &apalis_pool, &queue).await?;

    println!("queue: {queue}\n");
    // Wipe both tables between scenarios so each `print_counts` reports only
    // the rows produced by the scenario the caller just ran — otherwise
    // scenario 2's "rolled back, both tables empty" message would be muddied
    // by scenario 1's committed rows.
    scenario_commit(&backend_pool, &storage, &queue).await?;
    cleanup(&backend_pool, &apalis_pool, &queue).await?;
    scenario_rollback(&backend_pool, &storage, &queue).await?;
    cleanup(&backend_pool, &apalis_pool, &queue).await?;
    scenario_idempotency_conflict(&backend_pool, &storage, &queue).await?;

    cleanup(&backend_pool, &apalis_pool, &queue).await?;
    Ok(())
}

async fn scenario_commit(
    backend_pool: &PgPool,
    storage: &PostgresStorage<SendConfirmationEmail>,
    queue: &str,
) -> Result<(), BoxError> {
    println!("scenario 1: commit");
    let order_id = Ulid::new().to_string();

    let task_id = tokio::task::spawn_blocking({
        let storage = storage.clone();
        let backend_pool = backend_pool.clone();
        let order_id = order_id.clone();
        move || -> Result<String, PgError> {
            let mut conn = backend_pool.get().map_err(PgError::Pool)?;
            let id = conn.transaction(|c| {
                insert_order(c, &order_id, "alice@example.com")?;
                storage.push_with_conn(
                    c,
                    SendConfirmationEmail {
                        order_id: order_id.clone(),
                        to: "alice@example.com".to_owned(),
                    },
                )
            })?;
            Ok(id.to_string())
        }
    })
    .await??;

    println!("  enqueued task_id={task_id}");
    println!("  order row id={order_id}");
    print_counts(backend_pool, queue.to_owned()).await?;
    println!();
    Ok(())
}

async fn scenario_rollback(
    backend_pool: &PgPool,
    storage: &PostgresStorage<SendConfirmationEmail>,
    queue: &str,
) -> Result<(), BoxError> {
    println!("scenario 2: rollback");
    let order_id = Ulid::new().to_string();

    // The closure simulates a downstream check that fails AFTER the apalis
    // enqueue — e.g. a domain validation. Returning `Err` from the closure
    // makes Diesel roll back the transaction; neither the order row nor the
    // apalis task is persisted, and no NOTIFY is delivered.
    let outcome = tokio::task::spawn_blocking({
        let storage = storage.clone();
        let backend_pool = backend_pool.clone();
        let order_id = order_id.clone();
        move || -> Result<(), diesel::result::Error> {
            let mut conn = backend_pool
                .get()
                .map_err(|_| diesel::result::Error::BrokenTransactionManager)?;
            conn.transaction(|c| {
                insert_order(c, &order_id, "bob@example.com")
                    .map_err(|_| diesel::result::Error::RollbackTransaction)?;
                storage
                    .push_with_conn(
                        c,
                        SendConfirmationEmail {
                            order_id: order_id.clone(),
                            to: "bob@example.com".to_owned(),
                        },
                    )
                    .map_err(|_| diesel::result::Error::RollbackTransaction)?;
                Err(diesel::result::Error::RollbackTransaction)
            })
        }
    })
    .await?;

    match outcome {
        Err(diesel::result::Error::RollbackTransaction) => {
            println!("  closure returned Err → outer transaction rolled back");
        }
        other => panic!("unexpected outcome: {other:?}"),
    }
    print_counts(backend_pool, queue.to_owned()).await?;
    println!();
    Ok(())
}

async fn scenario_idempotency_conflict(
    backend_pool: &PgPool,
    storage: &PostgresStorage<SendConfirmationEmail>,
    queue: &str,
) -> Result<(), BoxError> {
    println!("scenario 3: idempotency conflict");
    let order_id = Ulid::new().to_string();
    let idempotency_key = format!("order:{order_id}");

    // Seed: first push with the idempotency_key, in its own transaction.
    {
        let storage = storage.clone();
        let backend_pool = backend_pool.clone();
        let idempotency_key = idempotency_key.clone();
        let order_id_for_seed = order_id.clone();
        tokio::task::spawn_blocking(move || -> Result<(), PgError> {
            let mut conn = backend_pool.get().map_err(PgError::Pool)?;
            let mut task = PgTask::<SendConfirmationEmail>::new(SendConfirmationEmail {
                order_id: order_id_for_seed,
                to: "carol@example.com".to_owned(),
            });
            task.parts.idempotency_key = Some(idempotency_key);
            conn.transaction(|c| storage.push_task_with_conn(c, task).map(|_| ()))
        })
        .await??;
    }

    // Conflict scenario: an outer transaction writes a business row, then
    // tries to enqueue with the same idempotency_key. The push fails via
    // SAVEPOINT rollback, but the outer transaction stays alive — the caller
    // chooses what to do. Here we commit the business write deliberately to
    // show that the savepoint only rolled back the apalis batch.
    let outcome = tokio::task::spawn_blocking({
        let storage = storage.clone();
        let backend_pool = backend_pool.clone();
        let order_id = order_id.clone();
        let idempotency_key = idempotency_key.clone();
        move || -> Result<&'static str, PgError> {
            let mut conn = backend_pool.get().map_err(PgError::Pool)?;
            conn.transaction(|c| {
                insert_order(c, &order_id, "carol@example.com")?;
                let mut task = PgTask::<SendConfirmationEmail>::new(SendConfirmationEmail {
                    order_id: order_id.clone(),
                    to: "carol@example.com".to_owned(),
                });
                task.parts.idempotency_key = Some(idempotency_key);
                match storage.push_task_with_conn(c, task) {
                    Ok(_) => Ok("no conflict observed"),
                    Err(PgError::InvalidArgument(_)) => {
                        // The savepoint already rolled back the apalis batch.
                        // We commit the surrounding business write anyway.
                        Ok("conflict surfaced; business write committed")
                    }
                    Err(other) => Err(other),
                }
            })
        }
    })
    .await??;

    println!("  outcome: {outcome}");
    print_counts(backend_pool, queue.to_owned()).await?;
    println!();
    Ok(())
}

fn insert_order(
    conn: &mut PgConnection,
    id: &str,
    email: &str,
) -> Result<(), diesel::result::Error> {
    let sql = format!(
        "INSERT INTO {BUSINESS_TABLE} (id, recipient) VALUES ($1, $2) \
         ON CONFLICT (id) DO NOTHING"
    );
    sql_query(sql)
        .bind::<Text, _>(id)
        .bind::<Text, _>(email)
        .execute(conn)
        .map(|_| ())
}

async fn ensure_business_table(pool: PgPool) -> Result<(), BoxError> {
    tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = pool.get()?;
        sql_query(format!(
            "CREATE TABLE IF NOT EXISTS {BUSINESS_TABLE} (
                id TEXT PRIMARY KEY,
                recipient TEXT NOT NULL
            )"
        ))
        .execute(&mut conn)?;
        Ok(())
    })
    .await??;
    Ok(())
}

async fn cleanup(
    backend_pool: &PgPool,
    apalis_pool: &PgPool,
    queue: &str,
) -> Result<(), BoxError> {
    let queue = queue.to_owned();
    let backend_pool = backend_pool.clone();
    let apalis_pool = apalis_pool.clone();
    tokio::task::spawn_blocking(move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut conn = backend_pool.get()?;
        sql_query(format!("DELETE FROM {BUSINESS_TABLE}")).execute(&mut conn)?;
        let mut conn = apalis_pool.get()?;
        sql_query("DELETE FROM apalis.jobs WHERE job_type = $1")
            .bind::<Text, _>(&queue)
            .execute(&mut conn)?;
        Ok(())
    })
    .await??;
    Ok(())
}

async fn print_counts(pool: &PgPool, queue: String) -> Result<(), BoxError> {
    let pool = pool.clone();
    let (jobs, orders) = tokio::task::spawn_blocking(
        move || -> Result<(i64, i64), Box<dyn std::error::Error + Send + Sync>> {
            let mut conn = pool.get()?;
            let jobs = sql_query("SELECT COUNT(*)::bigint AS n FROM apalis.jobs WHERE job_type = $1")
                .bind::<Text, _>(&queue)
                .get_result::<CountRow>(&mut conn)?
                .n;
            let orders = sql_query(format!(
                "SELECT COUNT(*)::bigint AS n FROM {BUSINESS_TABLE}"
            ))
            .get_result::<CountRow>(&mut conn)?
            .n;
            Ok((jobs, orders))
        },
    )
    .await??;
    println!("  apalis.jobs rows for this queue: {jobs}");
    println!("  {BUSINESS_TABLE} rows total:       {orders}");
    Ok(())
}
