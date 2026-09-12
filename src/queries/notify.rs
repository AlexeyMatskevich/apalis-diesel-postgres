use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use diesel::{RunQueryDsl, sql_query};
use futures::{Stream, channel::mpsc};

use crate::{Error, InsertEvent, PgPool, PgTaskId};

/// Interval the LISTEN listener thread sleeps between polling
/// `notifications_iter` when the in-memory libpq buffer is empty.
///
/// Without unsafe access to libpq's raw socket FD (which diesel 2.x does not
/// expose), there is no portable way to perform a true blocking wait on the
/// connection. The interval is therefore a trade-off between CPU wakeups and
/// notification-delivery latency. 50 ms caps worst-case wakeup latency at
/// roughly the same order as a typical async tick while still keeping the
/// listener at 20 Hz per queue — negligible CPU — and well below the default
/// polling fetcher tick that runs in parallel. Users never depend on this
/// interval for correctness, only for sub-second NOTIFY-driven wakeups.
pub(crate) const NOTIFY_LISTENER_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Upper bound on the in-memory mpsc buffer used to deliver task ids from the
/// LISTEN thread to the async fetcher. `buffer_size` from `Config` is a
/// caller-controlled value, so cap it to avoid an unintentionally huge channel
/// allocation if a misconfigured `Config::with_buffer_size(usize::MAX)` is
/// passed in. Notifications are durable in `apalis.jobs` regardless, so the
/// polling fetcher recovers any wakeups dropped past this cap.
pub(crate) const NOTIFY_CHANNEL_CAPACITY_MAX: usize = 8192;

/// Clamp a caller-supplied channel capacity into the valid range
/// `[1, NOTIFY_CHANNEL_CAPACITY_MAX]`.
///
/// `buffer_size` from `Config` is caller-controlled, so both the single-queue
/// listener ([`notify_task_ids`]) and the shared listener clamp it before
/// allocating the mpsc channel: a floor of 1 keeps `mpsc::channel(0)` from
/// rejecting every send, and the ceiling bounds the allocation if a
/// misconfigured `usize::MAX` is passed. Sharing one helper keeps both call
/// sites in lock-step (a drift between them would mean one listener silently
/// used a different bound).
pub(crate) fn clamp_notify_capacity(capacity: usize) -> usize {
    capacity.clamp(1, NOTIFY_CHANNEL_CAPACITY_MAX)
}

/// Outcome of a single `try_send` from the LISTEN thread to a fetcher channel.
/// Extracted (with `classify_delivery`) so the disconnected-vs-full distinction
/// is unit-testable: the listener loop runs on a spawned thread, so an inline
/// match guard there would be unreachable from a unit test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeliveryOutcome {
    /// The id was queued for the fetcher.
    Delivered,
    /// The channel is full; the wakeup is dropped. The job stays durable in
    /// `apalis.jobs`, so the polling fetcher picks it up on its next tick.
    ChannelFull,
    /// The receiver has been dropped; the listener should stop.
    ReceiverGone,
}

fn classify_delivery<T>(result: Result<(), mpsc::TrySendError<T>>) -> DeliveryOutcome {
    match result {
        Ok(()) => DeliveryOutcome::Delivered,
        Err(error) if error.is_disconnected() => DeliveryOutcome::ReceiverGone,
        Err(_) => DeliveryOutcome::ChannelFull,
    }
}

pub(crate) fn notify_task_ids(pool: PgPool, queue: String, capacity: usize) -> NotifyTaskIds {
    let (mut sender, receiver) = mpsc::channel(clamp_notify_capacity(capacity));
    let cancel = Arc::new(AtomicBool::new(false));
    let thread_cancel = cancel.clone();
    let errors = Arc::new(crate::notify_event::NotificationErrors::default());
    let listener_errors = errors.clone();
    if let Err(error) = std::thread::Builder::new()
        .name("apalis-postgres-notify".to_owned())
        .spawn(move || {
            let mut conn = match pool.get() {
                Ok(conn) => conn,
                Err(error) => {
                    listener_errors.publish(Error::from(error));
                    return;
                }
            };
            if let Err(error) = sql_query("LISTEN \"apalis::job::insert\"").execute(&mut conn) {
                listener_errors.publish(Error::database(
                    "starting PostgreSQL LISTEN notification listener",
                )(error));
                return;
            }
            // Ensure the LISTEN subscription is removed before the pooled
            // connection is returned to r2d2; otherwise the next pool user
            // would inherit the subscription and accumulate notifications in
            // libpq's buffer.
            let unlisten = |conn: &mut diesel::PgConnection| {
                let _ = sql_query("UNLISTEN \"apalis::job::insert\"").execute(conn);
            };
            'listen: while !thread_cancel.load(Ordering::Acquire) {
                for notification in conn.notifications_iter() {
                    if thread_cancel.load(Ordering::Acquire) {
                        break 'listen;
                    }
                    let notification = match notification {
                        Ok(notification) => notification,
                        Err(error) => {
                            listener_errors.publish(Error::database(
                                "receiving PostgreSQL notification",
                            )(error));
                            break 'listen;
                        }
                    };
                    let Ok(event) = serde_json::from_str::<InsertEvent>(&notification.payload)
                    else {
                        continue;
                    };
                    let (event_queue, ids) = event.into_ids();
                    if event_queue != queue {
                        continue;
                    }
                    for id in ids {
                        match classify_delivery(sender.try_send(id)) {
                            DeliveryOutcome::Delivered => {}
                            DeliveryOutcome::ReceiverGone => break 'listen,
                            // Channel full: drop the wakeup. The job is durable
                            // in `apalis.jobs`, and the polling fetcher will
                            // pick it up on its next tick. Logging is left to
                            // the application via tracing wrappers around the
                            // returned stream.
                            DeliveryOutcome::ChannelFull => break,
                        }
                    }
                }
                // Diesel does not expose libpq's PQsocket FD safely, so a true
                // blocking wait on the connection (via `select`/`poll`) is not
                // available without unsafe FFI. Until that is added, sleep
                // long enough to keep wakeup CPU usage negligible while
                // remaining well below the polling fetcher's tick. The
                // cancel flag is checked after the interval. Diesel's
                // notifications_iter is nonblocking, so Drop needs no SQL wakeup.
                std::thread::sleep(NOTIFY_LISTENER_POLL_INTERVAL);
            }
            unlisten(&mut conn);
        })
    {
        errors.publish(Error::NotifyListener(error.to_string()));
    }
    NotifyTaskIds {
        receiver,
        cancel,
        errors,
    }
}

pub(crate) struct NotifyTaskIds {
    receiver: mpsc::Receiver<PgTaskId>,
    cancel: Arc<AtomicBool>,
    errors: Arc<crate::notify_event::NotificationErrors>,
}

impl Stream for NotifyTaskIds {
    type Item = Result<PgTaskId, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(error) = self.errors.take(cx) {
            return Poll::Ready(Some(Err(error)));
        }
        let next = Pin::new(&mut self.receiver).poll_next(cx);
        // Failure publication precedes sender drop. Recheck before reporting
        // EOF in case the listener failed between the first check and this poll.
        if matches!(next, Poll::Ready(None))
            && let Some(error) = self.errors.take(cx)
        {
            return Poll::Ready(Some(Err(error)));
        }
        next.map(|item| item.map(Ok))
    }
}

impl Drop for NotifyTaskIds {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Release);
    }
}

#[cfg(all(test, feature = "tokio"))]
pub(crate) mod test_support {
    use super::*;

    pub(crate) fn state(source: &NotifyTaskIds) -> (usize, bool) {
        (
            source.receiver.size_hint().0,
            crate::notify_event::test_support::has_pending_error(&source.errors),
        )
    }

    pub(crate) fn saturated(ids: &[PgTaskId]) -> (NotifyTaskIds, Vec<PgTaskId>) {
        let (mut sender, receiver) = mpsc::channel(1);
        let mut accepted = Vec::new();
        for &id in ids {
            match classify_delivery(sender.try_send(id)) {
                DeliveryOutcome::Delivered => accepted.push(id),
                DeliveryOutcome::ChannelFull => {}
                DeliveryOutcome::ReceiverGone => panic!("fresh receiver disconnected"),
            }
        }
        (
            NotifyTaskIds {
                receiver,
                cancel: Arc::new(AtomicBool::new(false)),
                errors: Arc::new(crate::notify_event::NotificationErrors::default()),
            },
            accepted,
        )
    }
}

#[cfg(test)]
mod tests {
    use lets_expect::*;

    use super::*;
    use crate::notify_event::test_support::{
        HintObservation, hint_channel, observe_hints, preserves_hints_and_failure,
    };

    fn error_observation(full: bool, failures: usize) -> HintObservation {
        let (_sender, receiver, expected) = hint_channel(full);
        let errors = Arc::new(crate::notify_event::NotificationErrors::default());
        let stream = NotifyTaskIds {
            receiver,
            errors: errors.clone(),
            cancel: Arc::new(AtomicBool::new(false)),
        };
        for index in 0..failures {
            errors.publish(Error::NotifyListener(format!("failure-{index}")));
        }
        observe_hints(stream, expected)
    }

    lets_expect! {
        expect(error_observation(full, failures)) as single_listener_failure {
            let full = false;
            let failures = 0_usize;
            to has_no_failure_when_the_listener_is_healthy { preserves_hints_and_failure(None) }
            when the_hint_buffer_is_full {
                let full = true;
                to preserves_the_queued_hints { preserves_hints_and_failure(None) }
                when the_listener_fails {
                    let failures = 1_usize;
                    to delivers_the_error_and_preserves_every_hint { preserves_hints_and_failure(Some("failure-0")) }
                }
                when the_listener_fails_again_before_observation {
                    let failures = 2_usize;
                    to preserves_the_first_failure_without_growing_the_error_queue { preserves_hints_and_failure(Some("failure-0")) }
                }
            }
            when the_listener_fails {
                let failures = 1_usize;
                to delivers_the_error_before_waiting_for_hints { preserves_hints_and_failure(Some("failure-0")) }
            }
            when the_listener_fails_again_before_observation {
                let failures = 2_usize;
                to coalesces_the_unobserved_failures { preserves_hints_and_failure(Some("failure-0")) }
            }
        }
    }

    fn classify_a_delivered_send() -> DeliveryOutcome {
        let (mut sender, _receiver) = mpsc::channel::<i32>(1);
        classify_delivery(sender.try_send(1))
    }

    fn classify_a_full_channel() -> DeliveryOutcome {
        let (mut sender, _receiver) = mpsc::channel::<i32>(1);
        // Fill the channel past capacity; the receiver is never drained, so the
        // next send reports `Full` (connected) rather than `Disconnected`.
        while sender.try_send(1).is_ok() {}
        classify_delivery(sender.try_send(1))
    }

    fn classify_a_dropped_receiver() -> DeliveryOutcome {
        let (mut sender, receiver) = mpsc::channel::<i32>(1);
        drop(receiver);
        classify_delivery(sender.try_send(1))
    }

    fn clamp_capacity(capacity: usize) -> usize {
        clamp_notify_capacity(capacity)
    }

    lets_expect! {
        expect(clamp_capacity(capacity)) as notification_capacity {
            let capacity = 8_usize;

            // Default state: a caller value comfortably inside the valid range.
            to preserves_the_caller_value { equal(8) }

            when the_capacity_is_below_the_minimum {
                // `mpsc::channel(0)` would reject every send, so the floor is 1.
                let capacity = 0_usize;
                to clamps_up_to_the_minimum_of_one { equal(1) }
            }

            when the_capacity_equals_the_maximum {
                let capacity = NOTIFY_CHANNEL_CAPACITY_MAX;
                to keeps_the_maximum_unchanged { equal(NOTIFY_CHANNEL_CAPACITY_MAX) }
            }

            when the_capacity_exceeds_the_maximum {
                let capacity = NOTIFY_CHANNEL_CAPACITY_MAX + 1;
                to clamps_down_to_the_channel_capacity_cap {
                    equal(NOTIFY_CHANNEL_CAPACITY_MAX)
                }
            }
        }

        expect(classify_a_delivered_send()) as delivered_notification {
            when the_channel_accepts_the_id {
                to reports_the_id_as_delivered { equal(DeliveryOutcome::Delivered) }
            }
        }

        expect(classify_a_full_channel()) as full_notification_channel {
            when the_channel_is_full_but_still_connected {
                to drops_the_wakeup_without_stopping_the_listener {
                    equal(DeliveryOutcome::ChannelFull)
                }
            }
        }

        expect(classify_a_dropped_receiver()) as closed_notification_channel {
            when the_receiver_has_been_dropped {
                to signals_the_listener_to_stop { equal(DeliveryOutcome::ReceiverGone) }
            }
        }
    }
}
