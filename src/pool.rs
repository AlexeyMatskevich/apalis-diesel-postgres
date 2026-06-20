use diesel::{
    PgConnection,
    r2d2::{Builder, ConnectionManager, Pool},
};

use crate::{Error, PgPool};

/// Build a Diesel/r2d2 PostgreSQL pool from a database URL using r2d2 defaults.
///
/// Notify mode (see [`PostgresStorage::new_with_notify`](crate::PostgresStorage::new_with_notify)
/// and [`SharedPostgresStorage`](crate::SharedPostgresStorage)) holds one
/// pooled connection for the lifetime of the listener; size the pool with that
/// in mind. Use [`build_pool_with`] to override `max_size`, timeouts, or other
/// r2d2 options.
///
/// # Errors
/// Returns [`Error::Pool`] if r2d2 cannot establish the pool's initial
/// connection — for example when `database_url` is malformed or the server is
/// unreachable.
pub fn build_pool(database_url: impl AsRef<str>) -> Result<PgPool, Error> {
    build_pool_with(database_url, |builder| builder)
}

/// Build a Diesel/r2d2 PostgreSQL pool with a custom r2d2 builder configuration.
///
/// The `configure` callback receives a [`diesel::r2d2::Builder`] and can chain calls
/// such as `.max_size(32)`, `.connection_timeout(...)`, or `.min_idle(...)`.
///
/// # Errors
/// Returns [`Error::Pool`] if r2d2 cannot establish the pool's initial
/// connection under the configured builder — for example when `database_url`
/// is malformed or the server is unreachable.
pub fn build_pool_with<F>(database_url: impl AsRef<str>, configure: F) -> Result<PgPool, Error>
where
    F: FnOnce(Builder<ConnectionManager<PgConnection>>) -> Builder<ConnectionManager<PgConnection>>,
{
    let manager = ConnectionManager::<PgConnection>::new(database_url.as_ref());
    configure(Pool::builder())
        .build(manager)
        .map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;
    use std::time::Duration;

    use lets_expect::{AssertionError, AssertionResult, *};

    use super::*;

    /// Build a pool without eagerly opening connections so the unit tests can
    /// inspect the builder configuration without a reachable server.
    fn lazy_pool_with<F>(url: &'static str, configure: F) -> Result<PgPool, Error>
    where
        F: FnOnce(
            Builder<ConnectionManager<PgConnection>>,
        ) -> Builder<ConnectionManager<PgConnection>>,
    {
        build_pool_with(url, |builder| configure(builder.min_idle(Some(0))))
    }

    fn default_pool(url: &'static str) -> Result<PgPool, Error> {
        lazy_pool_with(url, |builder| builder)
    }

    fn pool_with_max_size(url: &'static str, max_size: u32) -> Result<PgPool, Error> {
        lazy_pool_with(url, move |builder| {
            builder
                .max_size(max_size)
                .connection_timeout(Duration::from_millis(5))
        })
    }

    fn equals_max_size(expected: u32) -> impl Fn(&PgPool) -> AssertionResult {
        move |pool| {
            if pool.max_size() == expected {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected max_size {expected}, got {}",
                    pool.max_size()
                )]))
            }
        }
    }

    /// Build a pool that eagerly opens its initial connection (default
    /// `min_idle`), so an unreachable server surfaces the `Err(Error::Pool(_))`
    /// path that `lazy_pool_with` deliberately avoids.
    fn eager_pool(url: &'static str) -> Result<PgPool, Error> {
        build_pool_with(url, |builder| {
            builder
                .max_size(1)
                .connection_timeout(Duration::from_millis(50))
        })
    }

    fn equals_connection_timeout(expected: Duration) -> impl Fn(&PgPool) -> AssertionResult {
        move |pool| {
            if pool.connection_timeout() == expected {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected connection_timeout {expected:?}, got {:?}",
                    pool.connection_timeout()
                )]))
            }
        }
    }

    /// Assert the `Error::Pool` Display contract: the actionable prefix and the
    /// `DATABASE_URL` guidance must both be present. The inner r2d2 timeout text
    /// is environment/timing dependent, so match by prefix + substring rather
    /// than full equality.
    fn displays_pool_acquisition_failure(error: &Error) -> AssertionResult {
        let actual = error.to_string();
        if actual.starts_with("failed to acquire PostgreSQL connection from r2d2 pool: ")
            && actual.contains("check that DATABASE_URL points to a reachable PostgreSQL server")
        {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected pool acquisition failure display, got {actual:?}"
            )]))
        }
    }

    /// `Error::Pool` uses `#[from]`, so thiserror generates `source()` for the
    /// single wrapped `r2d2::Error`: the source chain must be present.
    fn exposes_pool_error_as_source(error: &Error) -> AssertionResult {
        match StdError::source(error) {
            Some(_) => Ok(()),
            None => Err(AssertionError::new(vec![
                "expected the r2d2 pool error to be exposed as the source, got no source"
                    .to_owned(),
            ])),
        }
    }

    lets_expect! {
        expect(default_pool(url)) {
            let url = "postgres://127.0.0.1:1/unused";

            when build_pool_with_uses_the_r2d2_defaults {
                to returns_a_pool_with_the_r2d2_default_capacity {
                    be_ok_and equals_max_size(10)
                }

                to returns_a_pool_with_the_r2d2_default_connection_timeout {
                    be_ok_and equals_connection_timeout(Duration::from_secs(30))
                }
            }
        }

        expect(pool_with_max_size(url, max_size)) {
            let url = "postgres://127.0.0.1:1/unused";
            let max_size = 4;

            when build_pool_with_applies_a_custom_max_size_and_timeout {
                to honours_the_supplied_max_size {
                    be_ok_and equals_max_size(4)
                }

                to honours_the_supplied_connection_timeout {
                    be_ok_and equals_connection_timeout(Duration::from_millis(5))
                }
            }

            when build_pool_with_applies_a_minimum_capacity {
                let max_size = 1;
                to keeps_the_supplied_capacity {
                    be_ok_and equals_max_size(1)
                }
            }
        }

        // Unlike the blocks above (which use `lazy_pool_with`'s `min_idle(0)` to
        // inspect the builder without opening a connection), `eager_pool` drives
        // an EAGER build: with default `min_idle`, r2d2 establishes its initial
        // connection during `build()`, so an unreachable server surfaces the
        // `Err(Error::Pool(_))` path — otherwise unreachable in a unit test.
        expect(eager_pool(url)) {
            let url = "postgres://127.0.0.1:1/unused";

            when the_server_is_unreachable_and_initialization_is_eager {
                to returns_a_pool_error {
                    be_err_and match_pattern!(Error::Pool(_))
                }

                to displays_the_pool_acquisition_failure {
                    be_err_and displays_pool_acquisition_failure
                }

                to exposes_the_pool_error_as_the_source {
                    be_err_and exposes_pool_error_as_source
                }
            }
        }
    }
}
