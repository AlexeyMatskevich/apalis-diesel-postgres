use std::sync::OnceLock;

use apalis_diesel_postgres::{PgPool, build_pool_with, setup};
use diesel::PgConnection;
use lets_expect::{AssertionError, AssertionResult};

pub fn database_url_or_skip() -> Result<Option<String>, String> {
    let database_url = std::env::var("DATABASE_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());

    if database_url.is_none() && require_database() {
        Err(
            "DATABASE_URL must be set when APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE is enabled"
                .to_owned(),
        )
    } else {
        Ok(database_url)
    }
}

fn require_database() -> bool {
    matches!(
        std::env::var("APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE").as_deref(),
        Ok("1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON")
    )
}

/// One bounded pool per test binary.
///
/// `cargo test` runs test binaries sequentially, so at most one binary's pool is
/// live at a time; capping it well under a default `max_connections = 100` keeps
/// the whole suite's aggregate connection demand bounded regardless of how many
/// tests run in parallel inside the binary. (Under a binary-parallel runner such
/// as `cargo nextest`, cap `--test-threads`/jobs so `binaries * SHARED_POOL_SIZE`
/// stays under the server limit.) Previously every test built its own
/// `max_size = 10` pool, so a binary running ~N tests in parallel could request
/// up to `N * 10` connections and intermittently exhaust the server.
const SHARED_POOL_SIZE: u32 = 32;

static SHARED_POOL: OnceLock<Result<Option<PgPool>, String>> = OnceLock::new();

/// Build (once) and return the per-binary pool, running `setup` on every call.
///
/// The pool is memoized in a `OnceLock`; the build + `DATABASE_URL` check run
/// exactly once. `setup` is re-run per call rather than once because it is
/// idempotent and serialized by an advisory lock (see
/// `src/queries/migrations.rs`), so repeated calls are cheap no-ops after the
/// first and the cost of skipping the de-dup is not worth the async-once
/// machinery. Returns `Ok(None)` when `DATABASE_URL` is unset, so callers keep
/// their existing skip path.
#[allow(dead_code)] // not every test binary that includes `support` calls this
pub async fn shared_pool() -> Result<Option<PgPool>, String> {
    let built = SHARED_POOL.get_or_init(|| {
        let Some(url) = database_url_or_skip()? else {
            return Ok(None);
        };
        let pool = build_pool_with(url, |builder| {
            builder.max_size(SHARED_POOL_SIZE).min_idle(Some(0))
        })
        .map_err(|error| error.to_string())?;
        Ok(Some(pool))
    });
    let pool = match built {
        Ok(Some(pool)) => pool.clone(),
        Ok(None) => return Ok(None),
        Err(error) => return Err(error.clone()),
    };
    setup(&pool).await.map_err(|error| error.to_string())?;
    Ok(Some(pool))
}

/// Result of a DB-gated scenario: `Skipped` when `DATABASE_URL` is unset (so
/// every assertion passes vacuously), `Completed(T)` with the captured
/// observations otherwise. Shared by the `lets_expect` integration specs so the
/// skip-gating shape lives in one place instead of being copy-pasted per file.
#[derive(Debug)]
#[allow(dead_code)] // not every test binary that includes `support` uses this
pub enum Outcome<T> {
    Skipped,
    Completed(T),
}

/// Adapt a scenario's captured observations into a `lets_expect` assertion: a
/// `Skipped` run passes, a failed run surfaces the error, and a completed run is
/// handed to `body`. Centralised so the adapter is defined once across the specs.
#[allow(dead_code)] // not every test binary that includes `support` uses this
pub fn observe<T, F>(
    label: &'static str,
    body: F,
) -> impl Fn(&Result<Outcome<T>, String>) -> AssertionResult
where
    F: Fn(&T) -> Result<(), String>,
{
    move |result| match result {
        Err(error) => Err(AssertionError::new(vec![format!(
            "{label}: scenario failed: {error}"
        )])),
        Ok(Outcome::Skipped) => Ok(()),
        Ok(Outcome::Completed(run)) => {
            body(run).map_err(|reason| AssertionError::new(vec![format!("{label}: {reason}")]))
        }
    }
}

/// Run a blocking diesel closure on a pooled connection from an async context.
/// Shared by the integration specs so the `spawn_blocking` + pool-get + error
/// mapping is defined once.
#[allow(dead_code)] // not every test binary that includes `support` uses this
pub async fn with_conn<F, T>(pool: PgPool, work: F) -> Result<T, String>
where
    F: FnOnce(&mut PgConnection) -> Result<T, String> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let mut conn = pool.get().map_err(|e| e.to_string())?;
        work(&mut conn)
    })
    .await
    .map_err(|e| e.to_string())?
}
