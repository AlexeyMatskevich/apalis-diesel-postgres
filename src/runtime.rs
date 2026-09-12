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
        expect(forwarded_value(work_succeeds).await) as blocking_work_result {
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

        expect(panicked_value().await) as panicking_blocking_work {
            when blocking_work_panics {
                to maps_the_join_error_to_error_blocking {
                    be_err_and is_blocking_join_error
                }
            }
        }
    }
}

// The synchronous lets_expect subject owns the ntex runner. That preserves
// ntex-only and both-feature fallback preconditions without entering Tokio.
// Off-runtime cases deliberately use the synchronous futures executor instead.
#[cfg(all(test, feature = "ntex"))]
mod ntex_tests {
    use std::future::Future;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc,
    };
    use std::task::{Context, Poll};

    use futures::task::noop_waker;
    use lets_expect::*;

    use super::test_assertions::{
        equals_invalid_argument, forwarded_work, is_blocking_join_error, panicking_work,
    };
    use super::*;

    fn assert_ntex_runtime() {
        assert!(
            ntex_rt::System::try_current().is_some(),
            "the blocking work must be polled inside the ntex System"
        );
        assert!(
            tokio::runtime::Handle::try_current().is_err(),
            "the ntex path requires no entered Tokio runtime, including when both features are enabled"
        );
    }

    fn on_ntex<F: Future + 'static>(future: F) -> F::Output
    where
        F::Output: 'static,
    {
        ntex::rt::System::build()
            .build(ntex::rt::DefaultRuntime)
            .block_on(async move {
                assert_ntex_runtime();
                future.await
            })
    }

    fn ntex_work_result(work_succeeds: bool) -> Result<usize, Error> {
        on_ntex(run_blocking(forwarded_work(work_succeeds)))
    }

    fn ntex_panicked_work() -> Result<usize, Error> {
        on_ntex(run_blocking(panicking_work()))
    }

    #[derive(Debug)]
    struct QueuedWorkRun {
        probe_was_pending: bool,
        work_signalled: bool,
        work_ran: bool,
    }

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

    fn cancelled_queued_work() -> QueuedWorkRun {
        let runner = ntex::rt::System::build()
            .thread_pool_limit(1)
            .build(ntex::rt::DefaultRuntime);

        let ran = Arc::new(AtomicBool::new(false));

        runner.block_on({
            let ran = ran.clone();
            async move {
                assert_ntex_runtime();
                let probe_was_pending;
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
                    probe_was_pending = probe.as_mut().poll(&mut cx).is_pending();
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

                QueuedWorkRun {
                    probe_was_pending,
                    work_signalled: ran_before_deadline,
                    work_ran: ran.load(Ordering::SeqCst),
                }
            }
        })
    }

    #[derive(Debug)]
    struct InlineWorkRun {
        system_was_absent: bool,
        tokio_was_absent: bool,
        result: Result<usize, Error>,
        work_ran: bool,
    }

    fn off_runtime_work(runner_returned: bool) -> InlineWorkRun {
        if runner_returned {
            let runner = ntex::rt::System::build()
                .thread_pool_limit(1)
                .build(ntex::rt::DefaultRuntime);
            runner.block_on(async {});
        }

        // Observe the gate before polling the operation. A stale current System
        // after runner shutdown must fail this same leaf, even if work returns.
        let system_was_absent = ntex_rt::System::try_current().is_none();
        let tokio_was_absent = tokio::runtime::Handle::try_current().is_err();
        let ran = Arc::new(AtomicBool::new(false));
        let result = futures::executor::block_on(run_blocking({
            let ran = ran.clone();
            move || {
                ran.store(true, Ordering::SeqCst);
                Ok::<usize, Error>(if runner_returned { 9 } else { 7 })
            }
        }));
        InlineWorkRun {
            system_was_absent,
            tokio_was_absent,
            result,
            work_ran: ran.load(Ordering::SeqCst),
        }
    }

    fn cancelled_forwarder() -> Result<usize, Error> {
        // Drive the actual folding operation with the exact channel type it
        // receives when the detached forwarding task goes away before sending.
        // This is a translation check, not a simulated full arbiter shutdown.
        let (tx, rx) = futures::channel::oneshot::channel::<
            Result<Result<usize, Error>, ntex_rt::BlockingError>,
        >();
        drop(tx);
        futures::executor::block_on(resolve_forwarded_result(rx))
    }

    lets_expect! {
        expect(ntex_work_result(work_succeeds)) as ntex_blocking_work_result {
            let work_succeeds = true;
            when the_work_succeeds {
                to forwards_the_successful_value { be_ok_and equal(42) }
            }
            when the_work_returns_an_error {
                let work_succeeds = false;
                to forwards_the_exact_error {
                    be_err_and equals_invalid_argument("synthetic failure")
                }
            }
        }

        expect(ntex_panicked_work()) as ntex_panicking_work {
            when the_work_panics {
                to reports_the_blocking_failure { be_err_and is_blocking_join_error }
            }
        }

        expect(cancelled_queued_work()) as queued_work_ownership {
            when the_waiter_is_dropped_while_the_pool_is_busy {
                to preserves_the_submitted_work {
                    have(probe_was_pending) be_true,
                    have(work_signalled) be_true,
                    have(work_ran) be_true
                }
            }
        }

        expect(off_runtime_work(runner_returned)) as off_runtime_work {
            let runner_returned = false;
            when no_system_has_been_started {
                to executes_the_work_without_entering_a_runtime {
                    have(system_was_absent) be_true,
                    have(tokio_was_absent) be_true,
                    have(result) be_ok_and equal(7),
                    have(work_ran) be_true
                }
            }
            when the_system_runner_has_returned {
                let runner_returned = true;
                to executes_the_work_after_the_system_is_cleared {
                    have(system_was_absent) be_true,
                    have(tokio_was_absent) be_true,
                    have(result) be_ok_and equal(9),
                    have(work_ran) be_true
                }
            }
        }

        expect(cancelled_forwarder()) as forwarding_task_ownership {
            when the_forwarder_disappears_before_sending {
                to reports_the_blocking_failure { be_err_and is_blocking_join_error }
            }
        }
    }
}
