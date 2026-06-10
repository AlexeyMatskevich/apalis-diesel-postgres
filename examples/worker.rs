//! Two `WorkerBuilder` workers + in-handler `push_with_conn` (transactional
//! fan-out) with the two-pool layout from the README's "Connection pool
//! isolation" section: the apalis pool serves fetch/ack/heartbeat only, and
//! business transactions (including the outbox enqueue) run on a separate
//! backend pool. Run: `DATABASE_URL=... cargo run --example worker --features tokio`.

use apalis::prelude::*;
use apalis_diesel_postgres::{
    Config, Error as PgError, PgPool, PostgresStorage, build_pool, setup,
};
use diesel::Connection;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct SendEmail {
    to: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct LogActivity {
    kind: String,
    target: String,
}

async fn handle_email(
    job: SendEmail,
    backend_pool: Data<PgPool>,
    log_storage: Data<PostgresStorage<LogActivity>>,
) -> Result<(), BoxDynError> {
    println!("sending email to {}", job.to);

    let pool = (*backend_pool).clone();
    let storage = (*log_storage).clone();
    let to = job.to.clone();

    // Diesel is synchronous — hop onto spawn_blocking so we don't stall the
    // async runtime while holding a pooled PG connection. The transaction
    // borrows a connection from the *backend* pool: business work on the
    // apalis pool would compete with fetch/ack/heartbeat.
    let result = tokio::task::spawn_blocking(move || -> Result<(), PgError> {
        let mut conn = pool.get().map_err(PgError::Pool)?;
        conn.transaction(|c| {
            storage.push_with_conn(
                c,
                LogActivity {
                    kind: "email_sent".to_owned(),
                    target: to,
                },
            )?;
            Ok::<_, PgError>(())
        })
    })
    .await?;
    result?;

    Ok(())
}

async fn handle_log(job: LogActivity) -> Result<(), BoxDynError> {
    println!("logged {} -> {}", job.kind, job.target);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), BoxDynError> {
    let url = std::env::var("DATABASE_URL").map_err(|_| -> BoxDynError {
        "DATABASE_URL must be set, e.g. postgres://127.0.0.1:5432/apalis_diesel_postgres".into()
    })?;

    // Two pools against the same database: one dedicated to apalis worker
    // plumbing, one for the application's own transactions.
    let apalis_pool: PgPool = build_pool(&url)?;
    let backend_pool: PgPool = build_pool(&url)?;
    setup(&apalis_pool).await?;

    let emails: PostgresStorage<SendEmail> =
        PostgresStorage::new_with_config(&apalis_pool, &Config::new("emails"));
    let activity: PostgresStorage<LogActivity> =
        PostgresStorage::new_with_config(&apalis_pool, &Config::new("activity"));

    // Seed one `SendEmail` so the worker has something to consume on first
    // run — enqueued from the "business side", i.e. on the backend pool.
    {
        let emails = emails.clone();
        let seed_pool = backend_pool.clone();
        tokio::task::spawn_blocking(move || -> Result<(), PgError> {
            let mut conn = seed_pool.get().map_err(PgError::Pool)?;
            emails.push_with_conn(
                &mut conn,
                SendEmail {
                    to: "ada@example.com".to_owned(),
                },
            )?;
            Ok(())
        })
        .await??;
    }

    let emails_worker = WorkerBuilder::new("emails")
        .backend(emails)
        .data(backend_pool)
        .data(activity.clone())
        .build(handle_email);

    let activity_worker = WorkerBuilder::new("activity")
        .backend(activity)
        .build(handle_log);

    // Two independent workers, joined so either failure stops the example.
    tokio::try_join!(
        async {
            emails_worker
                .run()
                .await
                .map_err(|e| -> BoxDynError { Box::new(e) })
        },
        async {
            activity_worker
                .run()
                .await
                .map_err(|e| -> BoxDynError { Box::new(e) })
        },
    )?;

    Ok(())
}
