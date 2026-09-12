//! Listener readiness observed on task-owned connections, without timing assumptions.
use apalis_diesel_postgres::{PgPool, build_pool_with};
use diesel::{
    PgConnection, RunQueryDsl, sql_query,
    sql_types::{BigInt, Text},
};
use std::time::{Duration, Instant};

#[derive(Debug)]
struct ApplicationName(String);
impl diesel::r2d2::CustomizeConnection<PgConnection, diesel::r2d2::Error> for ApplicationName {
    fn on_acquire(&self, conn: &mut PgConnection) -> Result<(), diesel::r2d2::Error> {
        sql_query("SELECT set_config('application_name', $1, false)")
            .bind::<Text, _>(&self.0)
            .execute(conn)
            .map(|_| ())
            .map_err(diesel::r2d2::Error::QueryError)
    }
}

pub async fn isolated_listener_pool() -> Result<Option<PgPool>, String> {
    let Some(url) = super::support::database_url_or_skip()? else {
        return Ok(None);
    };
    // Reuse the binary's once-only schema setup, then isolate session identity.
    super::support::shared_pool().await?;
    let name = format!("notify-spec-{}", ulid::Ulid::new());
    let pool = build_pool_with(url, |builder| {
        builder
            .max_size(4)
            .min_idle(Some(0))
            .connection_customizer(Box::new(ApplicationName(name)))
    })
    .map_err(|error| error.to_string())?;
    Ok(Some(pool))
}

#[derive(diesel::QueryableByName)]
struct Count {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

pub async fn wait_for_listeners(pool: &PgPool, count: i64) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let pool = pool.clone();
        let observed = tokio::task::spawn_blocking(move || {
            let mut conn = pool.get().map_err(|error| error.to_string())?;
            sql_query("SELECT count(*)::bigint AS n FROM pg_stat_activity WHERE application_name = current_setting('application_name') AND query = 'LISTEN \"apalis::job::insert\"' AND state = 'idle'")
                .get_result::<Count>(&mut conn).map(|row| row.n).map_err(|error| error.to_string())
        }).await.map_err(|error| error.to_string())??;
        if observed >= count {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "expected {count} active LISTEN sessions, observed {observed}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
