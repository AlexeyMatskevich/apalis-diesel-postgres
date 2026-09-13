//! Local liveness is revoked when a worker loses its completion obligations.
use crate::Error;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
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
                    waker: futures::task::AtomicWaker::new(),
                })
            })
            .clone()
    }
}
#[derive(Debug)]
pub(crate) struct WorkerLease {
    worker_id: String,
    retired: AtomicBool,
    /// Set once the task stream yielded the registration item, so a
    /// heartbeat never renews a row that does not exist yet.
    registered: AtomicBool,
    waker: futures::task::AtomicWaker,
}
impl WorkerLease {
    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }
    pub(crate) fn retire(&self) {
        self.retired.store(true, Ordering::Release);
        self.waker.wake();
    }
    pub(crate) fn is_registered(&self) -> bool {
        self.registered.load(Ordering::Acquire)
    }
    pub(crate) fn mark_registered(&self) {
        self.registered.store(true, Ordering::Release);
        self.waker.wake();
    }
    /// Resolves once the name is registered, or once it is retired: a
    /// retired name has nothing to renew and its stream reports
    /// [`Error::WorkerRetired`] on its next poll.
    pub(crate) fn registered(self: &Arc<Self>) -> Registered {
        Registered(self.clone())
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
/// See [`WorkerLease::registered`].
pub(crate) struct Registered(Arc<WorkerLease>);
impl Future for Registered {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let lease = &self.0;
        if lease.is_registered() || lease.is_retired() {
            return Poll::Ready(());
        }
        lease.waker.register(cx.waker());
        // A wake between the check above and the registration is not lost:
        // the state is read again with the waker in place.
        if lease.is_registered() || lease.is_retired() {
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
        Retired,
        RegisteredThenRetired,
    }
    /// Poll the registration future of a name before and after a milestone,
    /// through a clone of the registry, and report whether it resolved.
    fn registration_wait(milestone: Milestone) -> (bool, bool) {
        let registry = LeaseRegistry::default();
        let lease = registry.for_worker("one");
        let mut wait = registry.clone().for_worker("one").registered();
        let before = wait
            .poll_unpin(&mut Context::from_waker(futures::task::noop_waker_ref()))
            .is_ready();
        match milestone {
            Milestone::None => {}
            Milestone::Registered => lease.mark_registered(),
            Milestone::Retired => lease.retire(),
            Milestone::RegisteredThenRetired => {
                lease.mark_registered();
                lease.retire();
            }
        }
        let after = wait
            .poll_unpin(&mut Context::from_waker(futures::task::noop_waker_ref()))
            .is_ready();
        (before, after)
    }
    lets_expect! {
        expect(registration_wait(milestone)) as waiting_for_a_registration {
            let milestone = Milestone::None;
            to stays_pending_while_nothing_happened { equal((false, false)) }
            when the_task_stream_registers_the_name {
                let milestone = Milestone::Registered;
                to resolves_after_the_registration { equal((false, true)) }
            }
            when the_name_is_retired_before_registering {
                let milestone = Milestone::Retired;
                to resolves_so_the_stream_can_report_the_retirement { equal((false, true)) }
            }
            when the_name_is_registered_and_then_retired {
                let milestone = Milestone::RegisteredThenRetired;
                to resolves { equal((false, true)) }
            }
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
