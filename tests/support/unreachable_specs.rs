//! Observe connection attempts through the fixture's real pool and scheduler.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc::{self, Sender},
    },
    time::Duration,
};

use diesel::r2d2::{ManageConnection, Pool};
use lets_expect::*;
use scheduled_thread_pool::{OnPoolDropBehavior, ScheduledThreadPool};

use crate::unreachable::pool_with_manager;

const WATCHDOG: Duration = Duration::from_secs(10);

#[derive(Debug)]
struct UnavailableConnection(Arc<AtomicUsize>);

impl ManageConnection for UnavailableConnection {
    type Connection = ();
    type Error = io::Error;

    fn connect(&self) -> Result<(), io::Error> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::other("connection unavailable"))
    }

    fn is_valid(&self, _: &mut ()) -> Result<(), io::Error> {
        Ok(())
    }

    fn has_broken(&self, _: &mut ()) -> bool {
        false
    }
}

#[derive(Clone, Copy)]
enum Owner {
    Original,
    RetainedClone,
}

#[derive(Clone, Copy)]
enum Demand {
    Absent,
    Nonblocking,
    ZeroTimeout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Checkout {
    NotRequested,
    Empty,
    TimedOut,
    Connected,
}

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    attempts: usize,
    checkout: Checkout,
    queue_discarded: bool,
}

struct QueueDropSignal {
    sender: Sender<bool>,
    executed: bool,
}

impl Drop for QueueDropSignal {
    fn drop(&mut self) {
        let _ = self.sender.send(!self.executed);
    }
}

fn observe_pool(owner: Owner, demand: Demand) -> Observation {
    let scheduler = Arc::new(
        ScheduledThreadPool::builder()
            .num_threads(1)
            .on_drop_behavior(OnPoolDropBehavior::DiscardPendingScheduled)
            .build(),
    );
    let (discarded_tx, discarded_rx) = mpsc::channel();
    let mut signal = QueueDropSignal {
        sender: discarded_tx,
        executed: false,
    };
    // Destruction, rather than execution, of this queued job proves teardown.
    scheduler.execute_after(Duration::from_secs(3600), move || {
        signal.executed = true;
        drop(signal);
    });

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_initial, initial_gate) = mpsc::channel::<()>();
    scheduler.execute(move || {
        let _ = entered_tx.send(());
        let _ = initial_gate.recv();
    });
    entered_rx
        .recv_timeout(WATCHDOG)
        .expect("initial gate entered");

    let attempts = Arc::new(AtomicUsize::new(0));
    let pool = pool_with_manager(
        Pool::builder().thread_pool(Arc::clone(&scheduler)),
        UnavailableConnection(Arc::clone(&attempts)),
    );
    let pool = match owner {
        Owner::Original => pool,
        Owner::RetainedClone => {
            let retained = pool.clone();
            drop(pool);
            retained
        }
    };
    let checkout = match demand {
        Demand::Absent => Checkout::NotRequested,
        Demand::Nonblocking if pool.try_get().is_none() => Checkout::Empty,
        Demand::ZeroTimeout if pool.get_timeout(Duration::ZERO).is_err() => Checkout::TimedOut,
        _ => Checkout::Connected,
    };

    let (snapshot_tx, snapshot_rx) = mpsc::channel();
    let (release_marker, marker_gate) = mpsc::channel::<()>();
    // r2d2 queues initial connects synchronously with zero delay. A positive
    // offset gives this marker a strictly later deadline even on a clock tick
    // shared with a connect job; equal deadlines have no FIFO guarantee.
    scheduler.execute_after(Duration::from_nanos(1), move || {
        let _ = snapshot_tx.send(attempts.load(Ordering::Relaxed));
        // Hold the worker until the pool is gone, before retries can execute.
        let _ = marker_gate.recv();
    });
    drop(release_initial);
    let attempts = snapshot_rx
        .recv_timeout(WATCHDOG)
        .expect("ordered marker reached");
    drop(pool);
    drop(scheduler);
    drop(release_marker);
    let queue_discarded = discarded_rx
        .recv_timeout(WATCHDOG)
        .expect("scheduler queue released");

    Observation {
        attempts,
        checkout,
        queue_discarded,
    }
}

fn have_observation(
    attempts: usize,
    checkout: Checkout,
) -> impl Fn(&Observation) -> AssertionResult {
    move |actual| {
        let expected = Observation {
            attempts,
            checkout,
            queue_discarded: true,
        };
        if *actual == expected {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected {expected:?}, got {actual:?}"
            )]))
        }
    }
}

lets_expect! {
    expect(observe_pool(owner, demand)) as lazy_unreachable_pool {
        let owner = Owner::Original;
        let demand = Demand::Absent;

        to starts_no_connection_work {
            have_observation(0, Checkout::NotRequested)
        }

        when checkout_does_not_wait {
            let demand = Demand::Nonblocking;
            to returns_none_without_connection_work {
                have_observation(0, Checkout::Empty)
            }
        }

        when checkout_wait_expires {
            let demand = Demand::ZeroTimeout;
            to starts_connection_work_only_after_demand {
                have_observation(1, Checkout::TimedOut)
            }
        }

        when only_a_clone_remains {
            let owner = Owner::RetainedClone;

            to starts_no_connection_work {
                have_observation(0, Checkout::NotRequested)
            }

            when checkout_does_not_wait {
                let demand = Demand::Nonblocking;
                to returns_none_without_connection_work {
                    have_observation(0, Checkout::Empty)
                }
            }

            when checkout_wait_expires {
                let demand = Demand::ZeroTimeout;
                to starts_connection_work_only_after_demand {
                    have_observation(1, Checkout::TimedOut)
                }
            }
        }
    }
}
