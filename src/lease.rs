//! Local liveness is revoked when a worker loses its completion obligations.
use crate::Error;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
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
                })
            })
            .clone()
    }
}
#[derive(Debug)]
pub(crate) struct WorkerLease {
    worker_id: String,
    retired: AtomicBool,
}
impl WorkerLease {
    pub(crate) fn is_retired(&self) -> bool {
        self.retired.load(Ordering::Acquire)
    }
    pub(crate) fn retire(&self) {
        self.retired.store(true, Ordering::Release);
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
    use lets_expect::*;
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
