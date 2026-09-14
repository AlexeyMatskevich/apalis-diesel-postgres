//! Local liveness is revoked when a worker loses its completion obligations.
use crate::Error;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
};

/// Shared by storage clones; different worker names have independent liveness.
#[derive(Clone, Default, Debug)]
pub(crate) struct LeaseRegistry(Arc<Mutex<HashMap<String, Arc<WorkerLease>>>>);
impl LeaseRegistry {
    pub(crate) fn for_worker(&self, worker_id: &str) -> Arc<WorkerLease> {
        let mut leases = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(lease) = leases.get(worker_id) {
            return lease.clone();
        }
        leases
            .entry(worker_id.to_owned())
            .or_insert_with(|| {
                Arc::new(WorkerLease {
                    worker_id: worker_id.to_owned(),
                    retired: AtomicBool::new(false),
                    registered: AtomicBool::new(false),
                    settled_registrations: AtomicU64::new(0),
                    pending_registrations: AtomicU64::new(0),
                    waiters: Mutex::new(Vec::new()),
                })
            })
            .clone()
    }
}
#[derive(Debug)]
pub(crate) struct WorkerLease {
    worker_id: String,
    retired: AtomicBool,
    /// Set once a task stream yielded the registration item, so a
    /// heartbeat never renews a row that does not exist yet.
    registered: AtomicBool,
    /// Registration attempts that have yielded their first item, successful
    /// or not. A heartbeat created before an attempt waits for that attempt
    /// to settle; a refused attempt lets it report the refusal.
    settled_registrations: AtomicU64,
    /// Registration attempts that began and have not settled. A refusal
    /// releases a waiting heartbeat only once none is pending, so the refused
    /// attempt of one worker does not release the heartbeat of another worker
    /// of the name whose registration is still being committed.
    pending_registrations: AtomicU64,
    /// Every waiter, so two heartbeat streams of one name (clones of one
    /// storage register with the same token) are both woken.
    waiters: Mutex<Vec<Waker>>,
}
impl WorkerLease {
    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }
    pub(crate) fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        self.wake_waiters();
    }
    pub(crate) fn is_registered(&self) -> bool {
        self.registered.load(Ordering::Acquire)
    }
    /// Begin a registration attempt, as a task stream does on its first poll.
    /// The attempt settles when it is settled or dropped.
    pub(crate) fn begin_registration(self: &Arc<Self>) -> RegistrationAttempt {
        self.pending_registrations.fetch_add(1, Ordering::AcqRel);
        RegistrationAttempt {
            lease: self.clone(),
            settled: false,
        }
    }
    fn finish_registration(&self, succeeded: bool) {
        if succeeded {
            self.registered.store(true, Ordering::Release);
        }
        // Counted as settled before it stops being pending, so a waiter that
        // reads between the two sees it as still in flight and is woken below.
        self.settled_registrations.fetch_add(1, Ordering::AcqRel);
        self.pending_registrations.fetch_sub(1, Ordering::AcqRel);
        self.wake_waiters();
    }
    fn wake_waiters(&self) {
        let waiters = std::mem::take(
            &mut *self
                .waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for waker in waiters {
            waker.wake();
        }
    }
    /// Resolves once the name is registered, once it is retired, or once a
    /// registration attempt that had not settled when this future was created
    /// has settled, refused or not, and no attempt of the name is still
    /// pending. A heartbeat stream then either renews a row that exists,
    /// reports the retirement, or reports the refusal as
    /// `WorkerNotRegistered`.
    pub(crate) fn registration(self: &Arc<Self>) -> Registration {
        Registration {
            lease: self.clone(),
            settled_before: self.settled_registrations.load(Ordering::Acquire),
        }
    }
    pub(crate) fn ensure_active(&self) -> Result<(), Error> {
        if self.is_retired() {
            Err(Error::WorkerRetired {
                worker_id: self.worker_id.clone(),
            })
        } else {
            Ok(())
        }
    }
    pub(crate) fn guard(self: &Arc<Self>) -> LeaseGuard {
        LeaseGuard {
            lease: self.clone(),
            armed: true,
        }
    }
}
/// One registration attempt of a worker name, from a task stream's first poll
/// until its first item. Dropping it unsettled settles it as refused, so a
/// heartbeat never waits for a stream that is gone.
pub(crate) struct RegistrationAttempt {
    lease: Arc<WorkerLease>,
    settled: bool,
}
impl RegistrationAttempt {
    /// Settle the attempt with its outcome.
    pub(crate) fn settle(mut self, succeeded: bool) {
        self.settled = true;
        self.lease.finish_registration(succeeded);
    }
}
impl Drop for RegistrationAttempt {
    fn drop(&mut self) {
        if !self.settled {
            self.lease.finish_registration(false);
        }
    }
}
/// See [`WorkerLease::registration`].
pub(crate) struct Registration {
    lease: Arc<WorkerLease>,
    settled_before: u64,
}
impl Registration {
    fn is_settled(&self) -> bool {
        self.lease.is_registered()
            || self.lease.is_retired()
            || (self.lease.settled_registrations.load(Ordering::Acquire) > self.settled_before
                && self.lease.pending_registrations.load(Ordering::Acquire) == 0)
    }
}
impl Future for Registration {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_settled() {
            return Poll::Ready(());
        }
        {
            let mut waiters = self
                .lease
                .waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !waiters.iter().any(|waker| waker.will_wake(cx.waker())) {
                waiters.push(cx.waker().clone());
            }
        }
        // A wake between the check above and the settlement is not lost: the
        // state is read again with the waker in place.
        if self.is_settled() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
/// Dropping a live obligation makes orphan recovery possible, even during DB outage.
pub(crate) struct LeaseGuard {
    lease: Arc<WorkerLease>,
    armed: bool,
}
impl LeaseGuard {
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}
impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if self.armed {
            self.lease.retire();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use lets_expect::*;
    #[derive(Clone, Copy)]
    enum Milestone {
        None,
        Registered,
        Refused,
        Retired,
        RegisteredThenRetired,
    }
    /// A waker that counts how often it was woken.
    fn counting_waker() -> (std::task::Waker, Arc<std::sync::atomic::AtomicUsize>) {
        struct Count(Arc<std::sync::atomic::AtomicUsize>);
        impl futures::task::ArcWake for Count {
            fn wake_by_ref(arc_self: &Arc<Self>) {
                arc_self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        (
            futures::task::waker(Arc::new(Count(counter.clone()))),
            counter,
        )
    }
    /// Two heartbeat waiters on one name, polled with counting wakers before
    /// a milestone, then polled again after it. Reports, per waiter, whether
    /// it was woken and whether it then resolved.
    fn registration_waits(milestone: Milestone) -> ((bool, bool), (bool, bool)) {
        let registry = LeaseRegistry::default();
        let lease = registry.for_worker("one");
        let mut first = registry.clone().for_worker("one").registration();
        let mut second = registry.clone().for_worker("one").registration();
        let (first_waker, first_wakes) = counting_waker();
        let (second_waker, second_wakes) = counting_waker();
        let first_before = first
            .poll_unpin(&mut Context::from_waker(&first_waker))
            .is_ready();
        let second_before = second
            .poll_unpin(&mut Context::from_waker(&second_waker))
            .is_ready();
        assert!(!first_before && !second_before);
        match milestone {
            Milestone::None => {}
            Milestone::Registered => lease.begin_registration().settle(true),
            Milestone::Refused => lease.begin_registration().settle(false),
            Milestone::Retired => lease.retire(),
            Milestone::RegisteredThenRetired => {
                lease.begin_registration().settle(true);
                lease.retire();
            }
        }
        let first_after = first
            .poll_unpin(&mut Context::from_waker(&first_waker))
            .is_ready();
        let second_after = second
            .poll_unpin(&mut Context::from_waker(&second_waker))
            .is_ready();
        (
            (first_wakes.load(Ordering::SeqCst) > 0, first_after),
            (second_wakes.load(Ordering::SeqCst) > 0, second_after),
        )
    }
    /// A waiter created after a refused attempt settled is not released by
    /// that earlier refusal: it waits for the next attempt.
    fn waiter_after_a_refusal() -> (bool, bool) {
        let lease = LeaseRegistry::default().for_worker("one");
        lease.begin_registration().settle(false);
        let mut later = lease.registration();
        let noop = Context::from_waker(futures::task::noop_waker_ref());
        let before = later
            .poll_unpin(&mut Context::from_waker(noop.waker()))
            .is_ready();
        lease.begin_registration().settle(false);
        let after = later
            .poll_unpin(&mut Context::from_waker(noop.waker()))
            .is_ready();
        (before, after)
    }
    /// Two registration attempts of one name are in flight when a heartbeat
    /// starts waiting, as with two workers of that name on clones of one
    /// storage. The second is refused, then the first settles. Reports whether
    /// the waiter resolved after the refusal and after the first attempt.
    fn waiter_beside_another_attempt(first_succeeds: bool) -> (bool, bool) {
        let lease = LeaseRegistry::default().for_worker("one");
        let first = lease.begin_registration();
        let second = lease.begin_registration();
        let mut waiter = lease.registration();
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(waiter.poll_unpin(&mut cx).is_pending());
        second.settle(false);
        let after_refusal = waiter.poll_unpin(&mut cx).is_ready();
        first.settle(first_succeeds);
        (after_refusal, waiter.poll_unpin(&mut cx).is_ready())
    }

    /// A heartbeat waiting on an attempt whose task stream is dropped before
    /// its first item. Reports whether the waiter then resolves.
    fn waiter_after_an_abandoned_attempt() -> bool {
        let lease = LeaseRegistry::default().for_worker("one");
        let attempt = lease.begin_registration();
        let mut waiter = lease.registration();
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(waiter.poll_unpin(&mut cx).is_pending());
        drop(attempt);
        waiter.poll_unpin(&mut cx).is_ready()
    }

    lets_expect! {
        expect(waiter_beside_another_attempt(first_succeeds)) as a_heartbeat_waiting_while_two_attempts_of_its_name_are_in_flight {
            let first_succeeds = true;
            to ignores_one_refusal_and_resolves_when_the_other_attempt_registers { equal((false, true)) }
            when the_other_attempt_is_refused_too {
                let first_succeeds = false;
                to resolves_to_report_the_refusal_once_no_attempt_is_pending { equal((false, true)) }
            }
        }
        expect(waiter_after_an_abandoned_attempt()) as a_heartbeat_waiting_on_an_attempt_whose_stream_was_dropped {
            to resolves_instead_of_waiting_forever { equal(true) }
        }
        expect(registration_waits(milestone)) as heartbeats_waiting_for_a_registration {
            let milestone = Milestone::None;
            to stay_pending_and_unwoken_while_nothing_happened { equal(((false, false), (false, false))) }
            when the_task_stream_registers_the_name {
                let milestone = Milestone::Registered;
                to are_both_woken_and_resolve { equal(((true, true), (true, true))) }
            }
            when the_registration_is_refused {
                let milestone = Milestone::Refused;
                to are_both_woken_and_resolve_to_report_the_refusal { equal(((true, true), (true, true))) }
            }
            when the_name_is_retired_before_registering {
                let milestone = Milestone::Retired;
                to are_both_woken_and_resolve_to_report_the_retirement { equal(((true, true), (true, true))) }
            }
            when the_name_is_registered_and_then_retired {
                let milestone = Milestone::RegisteredThenRetired;
                to are_both_woken_and_resolve { equal(((true, true), (true, true))) }
            }
        }
        expect(waiter_after_a_refusal()) as a_heartbeat_created_after_a_refused_registration {
            to waits_for_the_next_attempt_to_settle { equal((false, true)) }
        }
    }
    #[derive(Clone, Copy)]
    enum Completion {
        Outstanding,
        Completed,
        Abandoned,
    }
    fn observe(completion: Completion) -> (bool, bool, bool) {
        let registry = LeaseRegistry::default();
        let lease = registry.for_worker("one");
        if !matches!(completion, Completion::Outstanding) {
            let mut guard = lease.guard();
            if matches!(completion, Completion::Completed) {
                guard.disarm();
            }
            drop(guard);
        }
        (
            lease.is_retired(),
            registry.clone().for_worker("one").is_retired(),
            registry.for_worker("two").is_retired(),
        )
    }
    lets_expect! {
        expect(observe(completion)) as worker_completion_obligation {
            let completion=Completion::Outstanding;
            to keeps_the_worker_active { equal((false,false,false)) }
            when completion_is_confirmed {
                let completion=Completion::Completed;
                to keeps_the_worker_active { equal((false,false,false)) }
            }
            when completion_is_abandoned {
                let completion=Completion::Abandoned;
                to retires_the_same_worker_across_clones_and_preserves_other_workers { equal((true,true,false)) }
            }
        }
    }
}
