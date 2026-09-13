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

pub fn require_database() -> bool {
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
        Ok(Outcome::Skipped) if require_database() => Err(AssertionError::new(vec![format!(
            "{label}: a required database scenario was skipped"
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
    let work = move || {
        let mut conn = pool.get().map_err(|e| e.to_string())?;
        work(&mut conn)
    };
    #[cfg(feature = "ntex")]
    if tokio::runtime::Handle::try_current().is_err() {
        return ntex_rt::spawn_blocking(work)
            .await
            .map_err(|e| e.to_string())?;
    }
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| e.to_string())?
}

/// Run a scenario against its own database and remove only that database.
/// A supplied DATABASE_URL must support CREATE DATABASE and use URI syntax.
/// Infrastructure failures are errors even in optional mode once a URL is set.
#[allow(dead_code)]
pub async fn with_isolated_database<T, F, Fut>(work: F) -> Result<Outcome<T>, String>
where
    F: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<T, String>>,
{
    use diesel::{Connection, RunQueryDsl, sql_query};
    let Some(maintenance_url) = database_url_or_skip()? else {
        return Ok(Outcome::Skipped);
    };
    let name = format!(
        "apalis_test_{}",
        ulid::Ulid::new().to_string().to_lowercase()
    );
    let scheme_end = maintenance_url
        .find("://")
        .ok_or("isolated tests require a PostgreSQL URI")?
        + 3;
    let rest = &maintenance_url[scheme_end..];
    let path_start = rest
        .find('/')
        .ok_or("isolated tests require a database URI path")?;
    let query = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    let url = format!(
        "{}{}/{name}{query}",
        &maintenance_url[..scheme_end],
        &rest[..path_start]
    );
    let create_url = maintenance_url.clone();
    let create_name = name.clone();
    tokio::task::spawn_blocking(move || {
        let mut conn = PgConnection::establish(&create_url).map_err(|e| e.to_string())?;
        sql_query(format!("CREATE DATABASE \"{create_name}\""))
            .execute(&mut conn)
            .map_err(|e| format!("creating isolated database (CREATEDB required): {e}"))?;
        Ok::<_, String>(())
    })
    .await
    .map_err(|e| e.to_string())??;
    let check_url = url.clone();
    let expected_name = name.clone();
    let validated = tokio::task::spawn_blocking(move || {
        #[derive(diesel::QueryableByName)]
        struct DatabaseName {
            #[diesel(sql_type = diesel::sql_types::Text)]
            name: String,
        }
        let mut conn = PgConnection::establish(&check_url).map_err(|e| e.to_string())?;
        let actual = sql_query("SELECT current_database()::text AS name")
            .get_result::<DatabaseName>(&mut conn)
            .map_err(|e| e.to_string())?
            .name;
        if actual != expected_name {
            return Err(
                "derived URI did not resolve to the isolated database; refusing to modify it"
                    .to_owned(),
            );
        }
        Ok(())
    })
    .await
    .map_err(|e| e.to_string())
    .and_then(std::convert::identity);
    // A panicking scenario must not leak its database: catch the unwind,
    // remove the database, then resume the panic.
    let outcome = match validated {
        Ok(()) => {
            use futures::FutureExt as _;
            std::panic::AssertUnwindSafe(work(url)).catch_unwind().await
        }
        Err(error) => Ok(Err(error)),
    };
    let cleaned = tokio::task::spawn_blocking(move || {
        let mut conn = PgConnection::establish(&maintenance_url).map_err(|e| e.to_string())?;
        sql_query(format!("DROP DATABASE \"{name}\" WITH (FORCE)"))
            .execute(&mut conn)
            .map_err(|e| format!("removing owned isolated database {name}: {e}"))?;
        Ok::<_, String>(())
    })
    .await
    .map_err(|e| e.to_string())
    .and_then(std::convert::identity);
    let outcome = match outcome {
        Ok(outcome) => outcome,
        Err(panic) => {
            if let Err(cleanup) = cleaned {
                eprintln!("isolated database cleanup failed after a scenario panic: {cleanup}");
            }
            std::panic::resume_unwind(panic);
        }
    };
    match (outcome, cleaned) {
        (Ok(value), Ok(())) => Ok(Outcome::Completed(value)),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(format!("{error}; cleanup also failed: {cleanup}")),
    }
}

// Pure specifications for the environment gate helpers. They do not mutate
// process environment and can run alongside database scenarios.
#[cfg(test)]
mod helper_specs {
    use super::{is_truthy_flag, normalize_url};
    use lets_expect::*;

    #[derive(Clone, Copy)]
    enum FlagMembership {
        Accepted,
        Rejected,
        Empty,
    }
    #[derive(Clone, Copy)]
    enum LetterCase {
        Lower,
        Upper,
        Mixed,
    }
    #[derive(Clone, Copy)]
    enum Padding {
        None,
        Spaces,
        Controls,
        Unicode,
    }
    #[derive(Clone, Copy)]
    enum UrlContent {
        Uri,
        SingleCharacter,
        Empty,
    }

    fn flag_samples(membership: FlagMembership, case: LetterCase, padding: Padding) -> Vec<String> {
        let words: &[&str] = match membership {
            FlagMembership::Accepted => &["1", "true", "yes", "y", "on", "enabled"],
            FlagMembership::Rejected => {
                &["0", "false", "no", "off", "disabled", "truthy", "onward"]
            }
            FlagMembership::Empty => {
                return vec![
                    match padding {
                        Padding::None => "",
                        Padding::Spaces => "  ",
                        Padding::Controls => "\t\n",
                        Padding::Unicode => "\u{00a0}\u{2003}",
                    }
                    .to_owned(),
                ];
            }
        };
        words
            .iter()
            .map(|word| {
                let word = match case {
                    LetterCase::Lower => (*word).to_owned(),
                    LetterCase::Upper => word.to_ascii_uppercase(),
                    LetterCase::Mixed => {
                        let mut chars = word.chars();
                        chars
                            .next()
                            .map(|first| first.to_ascii_uppercase().to_string())
                            .unwrap_or_default()
                            + chars.as_str()
                    }
                };
                match padding {
                    Padding::None => word,
                    Padding::Spaces => format!("  {word}  "),
                    Padding::Controls => format!("\t{word}\n"),
                    Padding::Unicode => format!("\u{00a0}{word}\u{2003}"),
                }
            })
            .collect()
    }

    fn flag_observations(values: &[String]) -> Vec<(String, bool)> {
        values
            .iter()
            .map(|value| (value.clone(), is_truthy_flag(value)))
            .collect()
    }

    fn url_content(content: UrlContent) -> &'static str {
        match content {
            UrlContent::Uri => "postgres://user@host/db",
            UrlContent::SingleCharacter => "x",
            UrlContent::Empty => "",
        }
    }

    fn url_sample(content: UrlContent, padding: Padding) -> String {
        let value = url_content(content);
        match padding {
            Padding::None => value.to_owned(),
            Padding::Spaces if matches!(content, UrlContent::Empty) => "   ".to_owned(),
            Padding::Spaces => format!(" {value} "),
            Padding::Controls => format!("\t{value}\n"),
            Padding::Unicode => format!("\u{00a0}{value}\u{2003}"),
        }
    }

    lets_expect! {
        expect(flag_observations(&values)) as database_requirement_flag {
            let membership = FlagMembership::Accepted;
            let case = LetterCase::Lower;
            let padding = Padding::None;
            let values = flag_samples(membership, case, padding);
            let enabled = matches!(membership, FlagMembership::Accepted);
            let expected = values.iter().map(|value| (value.clone(), enabled)).collect::<Vec<_>>();
            to enables_every_recognized_spelling { equal(expected) }
            when ascii_spaces_surround_the_value {
                let padding = Padding::Spaces;
                to enables_every_recognized_spelling { equal(expected) }
            }
            when tabs_and_newlines_surround_the_value {
                let padding = Padding::Controls;
                to enables_every_recognized_spelling { equal(expected) }
            }
            when unicode_whitespace_surrounds_the_value {
                let padding = Padding::Unicode;
                to enables_every_recognized_spelling { equal(expected) }
            }
            when the_letters_are_uppercase {
                let case = LetterCase::Upper;
                to enables_every_recognized_spelling { equal(expected) }
                when ascii_spaces_surround_the_value {
                    let padding = Padding::Spaces;
                    to enables_every_recognized_spelling { equal(expected) }
                }
                when tabs_and_newlines_surround_the_value {
                    let padding = Padding::Controls;
                    to enables_every_recognized_spelling { equal(expected) }
                }
                when unicode_whitespace_surrounds_the_value {
                    let padding = Padding::Unicode;
                    to enables_every_recognized_spelling { equal(expected) }
                }
            }
            when the_letters_have_mixed_case {
                let case = LetterCase::Mixed;
                to enables_every_recognized_spelling { equal(expected) }
                when ascii_spaces_surround_the_value {
                    let padding = Padding::Spaces;
                    to enables_every_recognized_spelling { equal(expected) }
                }
                when tabs_and_newlines_surround_the_value {
                    let padding = Padding::Controls;
                    to enables_every_recognized_spelling { equal(expected) }
                }
                when unicode_whitespace_surrounds_the_value {
                    let padding = Padding::Unicode;
                    to enables_every_recognized_spelling { equal(expected) }
                }
            }
            when the_spelling_is_not_an_enabled_value {
                let membership = FlagMembership::Rejected;
                to rejects_every_unrecognized_spelling { equal(expected) }
                when ascii_spaces_surround_the_value {
                    let padding = Padding::Spaces;
                    to rejects_every_unrecognized_spelling { equal(expected) }
                }
                when tabs_and_newlines_surround_the_value {
                    let padding = Padding::Controls;
                    to rejects_every_unrecognized_spelling { equal(expected) }
                }
                when unicode_whitespace_surrounds_the_value {
                    let padding = Padding::Unicode;
                    to rejects_every_unrecognized_spelling { equal(expected) }
                }
                when the_letters_are_uppercase {
                    let case = LetterCase::Upper;
                    to rejects_every_unrecognized_spelling { equal(expected) }
                    when ascii_spaces_surround_the_value {
                        let padding = Padding::Spaces;
                        to rejects_every_unrecognized_spelling { equal(expected) }
                    }
                    when tabs_and_newlines_surround_the_value {
                        let padding = Padding::Controls;
                        to rejects_every_unrecognized_spelling { equal(expected) }
                    }
                    when unicode_whitespace_surrounds_the_value {
                        let padding = Padding::Unicode;
                        to rejects_every_unrecognized_spelling { equal(expected) }
                    }
                }
                when the_letters_have_mixed_case {
                    let case = LetterCase::Mixed;
                    to rejects_every_unrecognized_spelling { equal(expected) }
                    when ascii_spaces_surround_the_value {
                        let padding = Padding::Spaces;
                        to rejects_every_unrecognized_spelling { equal(expected) }
                    }
                    when tabs_and_newlines_surround_the_value {
                        let padding = Padding::Controls;
                        to rejects_every_unrecognized_spelling { equal(expected) }
                    }
                    when unicode_whitespace_surrounds_the_value {
                        let padding = Padding::Unicode;
                        to rejects_every_unrecognized_spelling { equal(expected) }
                    }
                }
            }
            when the_normalized_value_is_empty {
                let membership = FlagMembership::Empty;
                to keeps_the_database_requirement_disabled { equal(expected) }
                when ascii_spaces_surround_the_value {
                    let padding = Padding::Spaces;
                    to keeps_the_database_requirement_disabled { equal(expected) }
                }
                when tabs_and_newlines_surround_the_value {
                    let padding = Padding::Controls;
                    to keeps_the_database_requirement_disabled { equal(expected) }
                }
                when unicode_whitespace_surrounds_the_value {
                    let padding = Padding::Unicode;
                    to keeps_the_database_requirement_disabled { equal(expected) }
                }
            }
        }

        expect(normalize_url(value.clone())) as normalized_database_url {
            let content = UrlContent::Uri;
            let padding = Padding::None;
            let value = url_sample(content, padding);
            let expected = match content {
                UrlContent::Empty => None,
                _ => Some(url_content(content).to_owned()),
            };
            to returns_the_exact_nonempty_value { equal(expected) }
            when ascii_spaces_surround_the_value {
                let padding = Padding::Spaces;
                to returns_the_exact_nonempty_value { equal(expected) }
            }
            when tabs_and_newlines_surround_the_value {
                let padding = Padding::Controls;
                to returns_the_exact_nonempty_value { equal(expected) }
            }
            when unicode_whitespace_surrounds_the_value {
                let padding = Padding::Unicode;
                to returns_the_exact_nonempty_value { equal(expected) }
            }
            when the_content_has_the_minimum_nonempty_length {
                let content = UrlContent::SingleCharacter;
                to returns_the_exact_nonempty_value { equal(expected) }
                when ascii_spaces_surround_the_value {
                    let padding = Padding::Spaces;
                    to returns_the_exact_nonempty_value { equal(expected) }
                }
                when tabs_and_newlines_surround_the_value {
                    let padding = Padding::Controls;
                    to returns_the_exact_nonempty_value { equal(expected) }
                }
                when unicode_whitespace_surrounds_the_value {
                    let padding = Padding::Unicode;
                    to returns_the_exact_nonempty_value { equal(expected) }
                }
            }
            when the_normalized_content_is_empty {
                let content = UrlContent::Empty;
                to treats_the_url_as_unset { equal(expected) }
                when ascii_spaces_surround_the_value {
                    let padding = Padding::Spaces;
                    to treats_the_url_as_unset { equal(expected) }
                }
                when tabs_and_newlines_surround_the_value {
                    let padding = Padding::Controls;
                    to treats_the_url_as_unset { equal(expected) }
                }
                when unicode_whitespace_surrounds_the_value {
                    let padding = Padding::Unicode;
                    to treats_the_url_as_unset { equal(expected) }
                }
            }
        }
    }
}
