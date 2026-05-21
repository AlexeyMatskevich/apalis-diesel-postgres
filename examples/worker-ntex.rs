//! Same shape as `worker.rs`, but driven by ntex (neon runtime) instead of
//! tokio. Run:
//!   `DATABASE_URL=... cargo run --example worker-ntex \
//!        --no-default-features --features ntex`
//!
//! Demonstrates that `apalis-diesel-postgres` is runtime-agnostic: the storage
//! routes blocking Diesel calls through `ntex_rt::spawn_blocking` (see
//! `src/runtime.rs`), and `apalis-core`'s scheduler uses `futures-timer` for
//! sleeps, so the entire pipeline runs on the ntex executor with no tokio.

use apalis::prelude::*;
use apalis_diesel_postgres::{
    Config, Error as PgError, PgPool, PostgresStorage, build_pool, setup,
};
use diesel::Connection;
use futures::future::try_join;
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
    log_storage: Data<PostgresStorage<LogActivity>>,
) -> Result<(), BoxDynError> {
    println!("sending email to {}", job.to);

    let storage = (*log_storage).clone();
    let to = job.to.clone();

    let result = ntex_rt::spawn_blocking(move || -> Result<(), PgError> {
        let mut conn = storage.pool().get().map_err(PgError::Pool)?;
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
    .await
    .map_err(|e| -> BoxDynError { Box::new(e) })?;
    result?;

    Ok(())
}

async fn handle_log(job: LogActivity) -> Result<(), BoxDynError> {
    println!("logged {} -> {}", job.kind, job.target);
    Ok(())
}

#[ntex::main]
async fn main() -> Result<(), BoxDynError> {
    let url = std::env::var("DATABASE_URL").map_err(|_| -> BoxDynError {
        "DATABASE_URL must be set, e.g. postgres://127.0.0.1:5432/apalis_diesel_postgres".into()
    })?;

    let pool: PgPool = build_pool(&url)?;
    setup(&pool).await?;

    let emails: PostgresStorage<SendEmail> =
        PostgresStorage::new_with_config(&pool, &Config::new("emails"));
    let activity: PostgresStorage<LogActivity> =
        PostgresStorage::new_with_config(&pool, &Config::new("activity"));

    {
        let emails = emails.clone();
        ntex_rt::spawn_blocking(move || -> Result<(), PgError> {
            let mut conn = emails.pool().get().map_err(PgError::Pool)?;
            emails.push_with_conn(
                &mut conn,
                SendEmail {
                    to: "ada@example.com".to_owned(),
                },
            )?;
            Ok(())
        })
        .await
        .map_err(|e| -> BoxDynError { Box::new(e) })??;
    }

    let emails_worker = WorkerBuilder::new("emails")
        .backend(emails)
        .data(activity.clone())
        .build(handle_email);

    let activity_worker = WorkerBuilder::new("activity")
        .backend(activity)
        .build(handle_log);

    try_join(
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
    )
    .await?;

    Ok(())
}
