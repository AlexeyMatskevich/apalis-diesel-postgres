use std::sync::OnceLock;

use apalis_diesel_postgres::{PgPool, build_pool_with, setup};
use diesel::PgConnection;
use lets_expect::{AssertionError, AssertionResult};

pub fn database_url_or_skip() -> Result<Option<String>, String> {
    let database_url = std::env::var("DATABASE_URL").ok().and_then(normalize_url);

    if database_url.is_none() && require_database() {
        Err(
            "DATABASE_URL must be set when APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE is enabled"
                .to_owned(),
        )
    } else {
        Ok(database_url)
    }
}

/// Normalize a raw `DATABASE_URL` value: trim surrounding whitespace and treat a
/// value that is empty after trimming as unset (`None`). The trimmed form is what
/// gets returned so surrounding whitespace never reaches `build_pool_with` /
/// `ConnectionManager::new` (libpq does not strip whitespace around the whole URI).
fn normalize_url(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}

fn require_database() -> bool {
    std::env::var("APALIS_DIESEL_POSTGRES_REQUIRE_DATABASE")
        .as_deref()
        .map(is_truthy_flag)
        .unwrap_or(false)
}

/// Whether an environment-flag value means "enabled". Trims surrounding
/// whitespace and matches case-insensitively so common truthy spellings
/// (`True`, `Yes`, `On`, `y`, `enabled`, ...) all count — the point of the
/// require-database gate is to turn a silent skip into a hard error, so it must
/// not silently degrade on an unexpected-but-obviously-truthy spelling.
fn is_truthy_flag(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on" | "enabled"
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

// These cover the two pure helpers behind the env-driven gate without touching
// process env, so they are safe to run in parallel with the DB-gated specs.
// `mod support` is included by several integration binaries, so these run in each
// one; that is redundant but harmless.

#[test]
fn is_truthy_flag_accepts_the_canonical_enabled_spellings() {
    for value in ["1", "true", "yes", "y", "on", "enabled"] {
        assert!(
            is_truthy_flag(value),
            "{value:?} should enable the require-database gate"
        );
    }
}

#[test]
fn is_truthy_flag_accepts_mixed_case_and_surrounding_whitespace() {
    // The regression this guards: a boolean YAML value rendered as `True`, or a
    // hand-exported `Yes`/`On`/`  true  `, must not silently degrade the gate.
    for value in [
        "True", "TRUE", "Yes", "On", "Y", "Enabled", "  true  ", "\tyes\n",
    ] {
        assert!(
            is_truthy_flag(value),
            "{value:?} should enable the require-database gate"
        );
    }
}

#[test]
fn is_truthy_flag_rejects_falsy_and_unrelated_values() {
    for value in [
        "", "0", "false", "no", "off", "disabled", "  ", "truthy", "onward",
    ] {
        assert!(
            !is_truthy_flag(value),
            "{value:?} must not enable the require-database gate"
        );
    }
}

#[test]
fn normalize_url_strips_surrounding_whitespace_from_a_real_url() {
    // Whitespace around the whole URI must be trimmed before it reaches
    // `ConnectionManager::new`, since libpq does not strip it itself.
    assert_eq!(
        normalize_url(" postgres://user@host/db ".to_owned()),
        Some("postgres://user@host/db".to_owned())
    );
    assert_eq!(
        normalize_url("\tpostgres://user@host/db\n".to_owned()),
        Some("postgres://user@host/db".to_owned())
    );
}

#[test]
fn normalize_url_treats_whitespace_only_and_empty_values_as_unset() {
    assert_eq!(normalize_url(String::new()), None);
    assert_eq!(normalize_url("   ".to_owned()), None);
    assert_eq!(normalize_url("\t\n".to_owned()), None);
}

#[test]
fn normalize_url_leaves_a_clean_url_untouched() {
    assert_eq!(
        normalize_url("postgres://user@host/db".to_owned()),
        Some("postgres://user@host/db".to_owned())
    );
}
