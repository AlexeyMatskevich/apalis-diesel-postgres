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
        return run_blocking_ntex(work).await;
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
    run_blocking_ntex(work).await
}

// ntex-rt's blocking pool cancels a still-queued closure when the awaiting
// future is dropped before a worker picks it up: `execute` wraps every closure
// in `if !tx.is_closed()` (`ntex-rt/src/pool.rs`), so a dropped receiver skips
// the work entirely — directly contradicting ntex-rt's own documented contract
// ("The task will not be cancelled even if the future is dropped"). tokio's
// `spawn_blocking` honours that contract, and the crate relies on it: a dropped
// ack future must still finalize the row (otherwise it strands `Running` under
// a healthy heartbeat), and a dropped sink flush must still insert the batch it
// already drained from its buffer (otherwise the tasks are lost). Restore the
// guarantee on the ntex path by detaching the blocking submission onto its own
// background task whose lifetime is independent of the caller: that task owns
// the `BlockingResult` receiver, so the pool never sees it "closed" and always
// runs the closure, and the result is forwarded back over a fresh channel.
// Dropping the caller's future drops only that forwarding receiver — the
// detached task (and the DB write it carries) still runs to completion.
//
// This detour only applies when an ntex `System` is actually running. Without
// one, `ntex_rt::spawn` panics (`Runtime::with_current`: "not in a neon
// runtime"), so a caller that drives these futures off the ntex executor — for
// example a synchronous bootstrap `futures::executor::block_on(setup(&pool))`
// under `--no-default-features --features ntex`, or both runtime features with
// no runtime entered — would abort instead of running the query. The free
// `ntex_rt::spawn_blocking` handles that case for us: when `System::try_current`
// is `None` it runs the closure *inline* via `ThreadPool::execute_inplace`
// (`ntex-rt/src/lib.rs`), executing the work synchronously before the await
// point, which is inherently drop-safe (there is no queued closure to skip). So
// on the no-`System` path fall back to the plain `spawn_blocking` await —
// preserving the pre-fix behaviour — and only take the detached path when a
// runtime exists to host it.
#[cfg(feature = "ntex")]
async fn run_blocking_ntex<F, T>(work: F) -> Result<T, Error>
where
    F: FnOnce() -> Result<T, Error> + Send + 'static,
    T: Send + 'static,
{
    if ntex_rt::System::try_current().is_none() {
        return match ntex_rt::spawn_blocking(work).await {
            Ok(result) => result,
            Err(join_error) => Err(Error::Blocking(Box::new(join_error))),
        };
    }

    let (tx, rx) = futures::channel::oneshot::channel();
    // `ntex_rt::spawn` returns a `JoinHandle` whose `Drop` *detaches* the task
    // (async_task semantics), so dropping it here keeps the task running in the
    // background instead of cancelling it.
    ntex_rt::spawn(async move {
        let _ = tx.send(ntex_rt::spawn_blocking(work).await);
    });
    resolve_forwarded_result(rx).await
}

// Awaits the forwarding `oneshot` and folds its three outcomes onto the public
// result: the work's own `Ok`/`Err`, a mapped join error, and — the arm that is
// otherwise only reachable when the arbiter tears the forwarding task down before
// it reports back — a cancelled receiver. Extracted so that exact `rx.await`
// match (including the `Err(canceled)` branch) is driven by a deterministic
// regression test instead of relying on a racy arbiter teardown.
#[cfg(feature = "ntex")]
async fn resolve_forwarded_result<T>(
    rx: futures::channel::oneshot::Receiver<Result<Result<T, Error>, ntex_rt::BlockingError>>,
) -> Result<T, Error>
where
    T: Send + 'static,
{
    match rx.await {
        Ok(Ok(result)) => result,
        Ok(Err(join_error)) => Err(Error::Blocking(Box::new(join_error))),
        // The forwarding task was torn down before it could report back — only
        // reachable when the arbiter itself is shutting down.
        Err(canceled) => Err(Error::Blocking(Box::new(canceled))),
    }
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
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "the ntex fallback branch is only reached without an entered tokio runtime"
        );

        let result = run_blocking(forwarded_work(false)).await;

        let error = result.expect_err("err value forwarded");
        equals_invalid_argument("synthetic failure")(&error)
            .expect("err forwarded unchanged as InvalidArgument");
    }

    #[ntex::test]
    async fn maps_a_panic_to_error_blocking_via_ntex_fallback() {
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "the ntex fallback branch is only reached without an entered tokio runtime"
        );

        let result = run_blocking(panicking_work()).await;

        let error = result.expect_err("panic surfaces as Err");
        is_blocking_join_error(&error).expect("panic mapped to Error::Blocking");
    }
}

// Deterministic regression test for the ntex `spawn_blocking` cancel-on-drop
// hazard. ntex-rt's blocking pool wraps every submitted closure in
// `if !tx.is_closed()` (`ntex-rt/src/pool.rs`), so a closure still queued when
// the awaiting future is dropped is silently skipped — even though ntex-rt's
// own docs promise "the task will not be cancelled even if the future is
// dropped". `run_blocking` relied on that (false) promise, matching tokio's
// real guarantee, so a dropped ack/flush future could strand a `Running` row or
// lose accepted tasks under the `ntex` feature. A saturated one-thread pool
// makes the queueing deterministic: the occupier holds the only worker, the
// probe's closure is queued, and cancelling the probe's future before the
// worker frees exercises the exact skip window. Runs under the
// `--no-default-features --features ntex --lib` CI job and `--all-features`
// (where the ntex fallback branch is taken because no tokio runtime is entered).
#[cfg(all(test, feature = "ntex"))]
mod ntex_drop_safety_tests {
    use std::future::Future;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    use std::task::{Context, Poll};

    use futures::task::noop_waker;

    use super::*;

    // Yields control back to the ntex arbiter a bounded number of times so any
    // task spawned via `ntex_rt::spawn` (here, `run_blocking`'s detached
    // forwarding task) gets polled. This replaces the previous wall-clock
    // `sleep(50ms)` barrier: it advances the executor by *events*, not elapsed
    // time, so it can neither race a slow scheduler nor spuriously fail a loaded
    // one. `poll_fn` that returns `Pending` once (after waking itself) hands the
    // arbiter a chance to run other ready tasks before resuming.
    async fn yield_to_arbiter() {
        for _ in 0..16 {
            let mut yielded = false;
            std::future::poll_fn(|cx| {
                if yielded {
                    Poll::Ready(())
                } else {
                    yielded = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
        }
    }

    #[test]
    fn dropped_run_blocking_future_still_runs_the_work() {
        let runner = ntex::rt::System::build()
            .thread_pool_limit(1)
            .build(ntex::rt::DefaultRuntime);

        let ran = Arc::new(AtomicBool::new(false));

        runner.block_on({
            let ran = ran.clone();
            async move {
                let (busy_tx, busy_rx) = mpsc::channel::<()>();
                let (release_tx, release_rx) = mpsc::channel::<()>();
                // The probe closure signals here the instant it runs, so the
                // final barrier waits on the work itself rather than a fixed
                // sleep — otherwise a CPU-loaded worker that dequeues the closure
                // a little late would fail the test spuriously.
                let (ran_tx, ran_rx) = mpsc::channel::<()>();

                // Occupy the single blocking-pool worker until released, so the
                // next submission is queued rather than picked up immediately.
                let occupy = ntex_rt::spawn_blocking(move || {
                    busy_tx.send(()).expect("signal the worker is busy");
                    release_rx.recv().expect("await release");
                });
                busy_rx.recv().expect("the worker is running the occupier");

                // Submit run_blocking while the pool is saturated, then cancel
                // its future before the worker frees — the cancel-on-drop window.
                // Rather than dropping the future on a wall-clock timeout, poll it
                // by hand exactly until it parks (Pending): that single poll runs
                // `run_blocking_ntex` far enough to spawn the detached forwarding
                // task and start awaiting its result, which is precisely the state
                // in which the pre-fix code had already handed the closure to the
                // pool with a receiver about to be dropped. Then drop the future
                // deterministically — no elapsed time involved.
                {
                    let ran = ran.clone();
                    let ran_tx = ran_tx;
                    // `Box::pin` (not the `pin!` macro) so `probe` owns the pinned
                    // future: the `drop(probe)` below then genuinely drops the
                    // future itself, not just a borrow of it, which is what the
                    // cancel-on-drop event under test requires.
                    let mut probe = Box::pin(run_blocking(move || {
                        ran.store(true, Ordering::SeqCst);
                        let _ = ran_tx.send(());
                        Ok::<(), Error>(())
                    }));
                    let waker = noop_waker();
                    let mut cx = Context::from_waker(&waker);
                    // A first poll enters `run_blocking_ntex`, spawns the detached
                    // task, and parks on the forwarding receiver.
                    assert!(
                        probe.as_mut().poll(&mut cx).is_pending(),
                        "the probe cannot resolve while the only blocking worker is occupied"
                    );
                    // Drop the caller's future — the cancel-on-drop event under test.
                    drop(probe);
                }

                // Let the detached forwarding task run so it submits its closure to
                // the (still-occupied) pool before we free the worker, reproducing
                // the exact queued-skip window. Event-driven, not timer-driven.
                yield_to_arbiter().await;

                // Free the worker; it now dequeues the (possibly skipped)
                // closure.
                release_tx.send(()).expect("release the worker");
                let _ = occupy.await;

                // Wait for the closure's own run signal with a generous deadline:
                // as soon as the freed worker runs the work we proceed, and we
                // only fail if the closure never runs within the deadline (the
                // real regression). This deadline is a failure backstop, not a
                // sequencing barrier — nothing races against it, so it cannot
                // spuriously fail under load; it only turns a genuine "work never
                // ran" regression into a loud assertion instead of a hang.
                // `recv_timeout` blocks the single-threaded runner, but the freed
                // pool worker executes the closure on its own thread, so the
                // signal still arrives.
                let ran_before_deadline = ran_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .is_ok();

                assert!(
                    ran_before_deadline && ran.load(Ordering::SeqCst),
                    "run_blocking silently dropped the work when its future was cancelled before the ntex blocking pool picked the closure up"
                );
            }
        });
    }
}

// Regression test for the no-`System` fallback. `run_blocking_ntex` normally
// detaches through `ntex_rt::spawn`, which panics when no ntex `System` is
// running ("not in a neon runtime"). A caller that drives these futures without
// an entered ntex runtime — e.g. a synchronous bootstrap
// `futures::executor::block_on(setup(&pool))` under `--features ntex`, which the
// free `ntex_rt::spawn_blocking` historically served by running the closure
// inline via `ThreadPool::execute_inplace` — must keep working rather than
// abort. This spec polls `run_blocking` with no ntex `System` entered (and,
// under `--all-features`, no tokio runtime either, so the `Handle::try_current`
// check routes to the ntex path): before the fix the first poll panicked inside
// `ntex_rt::spawn`; after it, the closure runs inline and its value is
// forwarded. A plain `#[test]` (no `#[ntex::test]`/`#[tokio::test]`) is what
// keeps both runtimes un-entered. Runs under both the `--all-features` and
// `--no-default-features --features ntex --lib` CI jobs.
#[cfg(all(test, feature = "ntex"))]
mod ntex_no_system_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    #[test]
    fn run_blocking_without_an_ntex_system_runs_inline() {
        assert!(
            ntex_rt::System::try_current().is_none(),
            "this spec must run without an entered ntex System to exercise the fallback"
        );

        let ran = Arc::new(AtomicBool::new(false));
        let result = futures::executor::block_on(run_blocking({
            let ran = ran.clone();
            move || {
                ran.store(true, Ordering::SeqCst);
                Ok::<usize, Error>(7)
            }
        }));

        assert_eq!(result.expect("work forwarded its value"), 7);
        assert!(
            ran.load(Ordering::SeqCst),
            "the blocking closure must have run inline without an ntex System"
        );
    }
}

// Guards the assumption behind the `System::try_current()` gate in
// `run_blocking_ntex`. It is tempting to think that after an ntex
// `SystemRunner::block_on` returns the thread keeps a *current* `System` while
// its runtime is no longer entered — which would make the gate insufficient,
// since `ntex_rt::spawn` needs a running `Runtime`, not merely a `System`. In
// practice the runner clears the current `System` as it winds down (the
// arbiter's shutdown calls `unregister_arbiter`/`remove_current`), so once
// `block_on` returns `System::try_current()` is `None`. A caller that then
// drives these futures off the runtime (e.g. `futures::executor::block_on`)
// therefore takes the no-`System` inline/pool fallback, never the detached
// `ntex_rt::spawn` path that would panic without a running runtime. The only way
// to observe a current `System` with no running runtime is to call the
// `#[doc(hidden)]` `System::set_current` by hand, which is not supported usage.
// Were a future ntex release stop clearing the `System` here, this spec fails
// loudly — signalling that the gate must then probe the runtime itself.
#[cfg(all(test, feature = "ntex"))]
mod ntex_runner_returned_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    #[test]
    fn current_system_is_cleared_after_a_runner_returns_so_run_blocking_falls_back() {
        let runner = ntex::rt::System::build()
            .thread_pool_limit(1)
            .build(ntex::rt::DefaultRuntime);
        runner.block_on(async {});

        assert!(
            ntex_rt::System::try_current().is_none(),
            "an ntex runner must clear the current System when it returns; the \
             run_blocking gate relies on this to route off-runtime callers to \
             the inline/pool fallback instead of the detached spawn path"
        );

        let ran = Arc::new(AtomicBool::new(false));
        let result = futures::executor::block_on(run_blocking({
            let ran = ran.clone();
            move || {
                ran.store(true, Ordering::SeqCst);
                Ok::<usize, Error>(9)
            }
        }));

        assert_eq!(result.expect("work forwarded its value"), 9);
        assert!(
            ran.load(Ordering::SeqCst),
            "the blocking closure must have run after the runner returned"
        );
    }
}

// Covers the third `rx.await` arm of `run_blocking_ntex`'s detached path:
// `Err(canceled)`, taken when the forwarding `oneshot` is dropped before the
// detached task reports back (only reachable end to end when the arbiter itself
// is tearing down, which is not deterministically reproducible). This spec drives
// the real `resolve_forwarded_result` — the exact function `run_blocking_ntex`
// awaits, so the actual `match rx.await { .. Err(canceled) => .. }` code runs —
// against a receiver whose sender has been dropped without sending, reproducing
// the same `oneshot::Canceled` a torn-down forwarding task would leave behind and
// pinning the `canceled -> Error::Blocking` translation against regression.
#[cfg(all(test, feature = "ntex"))]
mod ntex_forwarding_canceled_tests {
    use super::test_assertions::is_blocking_join_error;
    use super::*;

    #[test]
    fn a_cancelled_forwarding_oneshot_maps_to_error_blocking() {
        // Build the exact channel `run_blocking_ntex` uses to forward the detached
        // task's outcome, then drop the sender without sending — the receiver now
        // yields the same `oneshot::Canceled` the detached forwarding task leaves
        // behind when the arbiter tears it down before it reports back. Awaiting
        // through `resolve_forwarded_result` executes the production `rx.await`
        // match, including its `Err(canceled)` arm, end to end.
        let (tx, rx) = futures::channel::oneshot::channel::<
            Result<Result<usize, Error>, ntex_rt::BlockingError>,
        >();
        drop(tx);

        let result = futures::executor::block_on(resolve_forwarded_result(rx));

        let error = result.expect_err(
            "a cancelled forwarding oneshot must surface as an Err from resolve_forwarded_result",
        );
        is_blocking_join_error(&error)
            .expect("a cancelled forwarding oneshot must surface as Error::Blocking");
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
