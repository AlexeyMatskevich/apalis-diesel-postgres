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
    // `tokio::task::spawn_blocking` panics when no Tokio runtime is entered.
    // With both runtime features enabled and the caller running on the ntex
    // executor that panic would be guaranteed on the very first query, so
    // "tokio wins" only while its runtime is actually present — otherwise
    // fall back to ntex's blocking pool.
    #[cfg(feature = "ntex")]
    if tokio::runtime::Handle::try_current().is_err() {
        return ntex_rt::spawn_blocking(work)
            .await
            .map_err(|error| Error::Blocking(Box::new(error)))?;
    }
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

// Assertion helpers shared by every `run_blocking` spec, regardless of which
// runtime feature is active. They live in a `#[cfg(test)]` module (not behind a
// specific feature) so the tokio lets_expect spec, the both-features ntex
// fallback spec, and the ntex-only spec can all reuse the exact same domain
// assertions instead of re-deriving them per feature combination.
#[cfg(test)]
mod test_assertions {
    use lets_expect::{AssertionError, AssertionResult};

    use super::Error;

    /// Closure invoked through `run_blocking` that returns `Ok(42)` on success
    /// and a fixed `InvalidArgument` error on failure, so specs can assert that
    /// both the value and the error are forwarded unchanged.
    pub(super) fn forwarded_work(work_succeeds: bool) -> impl FnOnce() -> Result<usize, Error> {
        move || {
            if work_succeeds {
                Ok(42_usize)
            } else {
                Err(Error::InvalidArgument("synthetic failure".to_owned()))
            }
        }
    }

    /// Closure invoked through `run_blocking` that always panics, so specs can
    /// assert the panic is mapped to `Error::Blocking`.
    pub(super) fn panicking_work() -> impl FnOnce() -> Result<usize, Error> {
        || -> Result<usize, Error> {
            panic!("synthetic blocking panic");
        }
    }

    pub(super) fn equals_invalid_argument(
        expected: &'static str,
    ) -> impl Fn(&Error) -> AssertionResult {
        move |error| match error {
            Error::InvalidArgument(message) if message == expected => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected InvalidArgument({expected:?}), got {other:?}"
            )])),
        }
    }

    pub(super) fn is_blocking_join_error(error: &Error) -> AssertionResult {
        match error {
            Error::Blocking(_) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected Error::Blocking from a panicked task, got {other:?}"
            )])),
        }
    }
}

#[cfg(all(test, feature = "tokio"))]
mod tests {
    use lets_expect::*;

    use super::test_assertions::{
        equals_invalid_argument, forwarded_work, is_blocking_join_error, panicking_work,
    };
    use super::*;

    async fn forwarded_value(work_succeeds: bool) -> Result<usize, Error> {
        run_blocking(forwarded_work(work_succeeds)).await
    }

    async fn panicked_value() -> Result<usize, Error> {
        run_blocking(panicking_work()).await
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

// When BOTH runtime features are enabled, `run_blocking` prefers tokio only
// while a tokio runtime is actually entered; otherwise it falls back to ntex's
// blocking pool. The tokio lets_expect spec above always runs inside a tokio
// runtime (lets_expect emits `#[tokio::test]`), so it can never reach the
// fallback. These specs drive `run_blocking` from inside an ntex runtime with
// no tokio runtime entered (`#[ntex::test]`), exercising the
// `Handle::try_current().is_err()` branch end to end. They use a plain ntex
// harness rather than lets_expect for the same reason — a `#[tokio::test]`
// would enter a tokio runtime and defeat the very condition under test.
#[cfg(all(test, feature = "tokio", feature = "ntex"))]
mod both_features_no_tokio_runtime_tests {
    use super::test_assertions::{
        equals_invalid_argument, forwarded_work, is_blocking_join_error, panicking_work,
    };
    use super::*;

    #[ntex::test]
    async fn forwards_the_ok_value_via_ntex_fallback() {
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "the ntex fallback branch is only reached without an entered tokio runtime"
        );

        let result = run_blocking(forwarded_work(true)).await;

        assert_eq!(result.expect("ok value forwarded"), 42);
    }

    #[ntex::test]
    async fn forwards_the_err_value_via_ntex_fallback() {
        let result = run_blocking(forwarded_work(false)).await;

        let error = result.expect_err("err value forwarded");
        equals_invalid_argument("synthetic failure")(&error)
            .expect("err forwarded unchanged as InvalidArgument");
    }

    #[ntex::test]
    async fn maps_a_panic_to_error_blocking_via_ntex_fallback() {
        let result = run_blocking(panicking_work()).await;

        let error = result.expect_err("panic surfaces as Err");
        is_blocking_join_error(&error).expect("panic mapped to Error::Blocking");
    }
}

// The ntex-only `run_blocking` implementation (no tokio feature) has its own
// spec. lets_expect is tokio-feature-bound (it emits `#[tokio::test]`), so this
// module uses a plain `#[ntex::test]` harness, which runs under the existing
// `cargo test --no-default-features --features ntex --lib` CI job.
#[cfg(all(test, feature = "ntex", not(feature = "tokio")))]
mod ntex_only_tests {
    use super::test_assertions::{
        equals_invalid_argument, forwarded_work, is_blocking_join_error, panicking_work,
    };
    use super::*;

    #[ntex::test]
    async fn forwards_the_ok_value() {
        let result = run_blocking(forwarded_work(true)).await;

        assert_eq!(result.expect("ok value forwarded"), 42);
    }

    #[ntex::test]
    async fn forwards_the_err_value() {
        let result = run_blocking(forwarded_work(false)).await;

        let error = result.expect_err("err value forwarded");
        equals_invalid_argument("synthetic failure")(&error)
            .expect("err forwarded unchanged as InvalidArgument");
    }

    #[ntex::test]
    async fn maps_a_panic_to_error_blocking() {
        let result = run_blocking(panicking_work()).await;

        let error = result.expect_err("panic surfaces as Err");
        is_blocking_join_error(&error).expect("panic mapped to Error::Blocking");
    }
}
