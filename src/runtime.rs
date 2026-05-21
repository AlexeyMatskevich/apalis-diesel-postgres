use crate::Error;

#[cfg(all(not(feature = "tokio"), not(feature = "ntex")))]
compile_error!(
    "apalis-diesel-postgres requires a runtime feature: enable `tokio` (default) or `ntex`. \
     Running without either would execute every Diesel query inline on the async caller, \
     stalling the executor."
);

// When both runtime features are enabled (e.g. via `cargo test --all-features`,
// or a workspace that pulls both runtimes transitively), prefer `tokio` —
// matches the README's documented precedence and avoids forcing downstream
// consumers to gate their feature combinations.
#[cfg(feature = "tokio")]
pub(crate) async fn run_blocking<F, T>(work: F) -> Result<T, Error>
where
    F: FnOnce() -> Result<T, Error> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| Error::Blocking(Box::new(error)))?
}

#[cfg(all(not(feature = "tokio"), feature = "ntex"))]
pub(crate) async fn run_blocking<F, T>(work: F) -> Result<T, Error>
where
    F: FnOnce() -> Result<T, Error> + Send + 'static,
    T: Send + 'static,
{
    ntex_rt::spawn_blocking(work)
        .await
        .map_err(|error| Error::Blocking(Box::new(error)))?
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use lets_expect::{AssertionError, AssertionResult, *};

    use super::*;

    async fn forwarded_value(work_succeeds: bool) -> Result<usize, Error> {
        run_blocking(move || {
            if work_succeeds {
                Ok(42_usize)
            } else {
                Err(Error::InvalidArgument("synthetic failure".to_owned()))
            }
        })
        .await
    }

    async fn panicked_value() -> Result<usize, Error> {
        run_blocking(|| -> Result<usize, Error> {
            panic!("synthetic blocking panic");
        })
        .await
    }

    fn equals_invalid_argument(expected: &'static str) -> impl Fn(&Error) -> AssertionResult {
        move |error| match error {
            Error::InvalidArgument(message) if message == expected => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected InvalidArgument({expected:?}), got {other:?}"
            )])),
        }
    }

    fn is_blocking_join_error(error: &Error) -> AssertionResult {
        match error {
            Error::Blocking(_) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected Error::Blocking from a panicked task, got {other:?}"
            )])),
        }
    }

    lets_expect! { #tokio_test
        expect(forwarded_value(work_succeeds).await) {
            let work_succeeds = true;

            when blocking_work_returns_ok {
                to forwards_the_ok_value {
                    be_ok_and equal(42)
                }
            }

            when blocking_work_returns_err {
                let work_succeeds = false;
                to forwards_the_err_value {
                    be_err_and equals_invalid_argument("synthetic failure")
                }
            }
        }

        expect(panicked_value().await) {
            when blocking_work_panics {
                to maps_the_join_error_to_error_blocking {
                    be_err_and is_blocking_join_error
                }
            }
        }
    }
}
