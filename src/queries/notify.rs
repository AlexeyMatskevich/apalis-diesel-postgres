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

pub(crate) fn notify_task_ids(
    pool: PgPool,
    queue: String,
    capacity: usize,
) -> impl Stream<Item = Result<PgTaskId, Error>> + Send {
    let (mut sender, receiver) = mpsc::channel(capacity.clamp(1, NOTIFY_CHANNEL_CAPACITY_MAX));
    let cancel = Arc::new(AtomicBool::new(false));
    let thread_cancel = cancel.clone();
    let mut spawn_error_sender = sender.clone();
    let thread_pool = pool.clone();
    if let Err(error) = std::thread::Builder::new()
        .name("apalis-postgres-notify".to_owned())
        .spawn(move || {
            let mut conn = match thread_pool.get() {
                Ok(conn) => conn,
                Err(error) => {
                    let _ = sender.try_send(Err(Error::from(error)));
                    return;
                }
            };
            if let Err(error) = sql_query("LISTEN \"apalis::job::insert\"").execute(&mut conn) {
                let _ = sender.try_send(Err(Error::database(
                    "starting PostgreSQL LISTEN notification listener",
                )(error)));
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
                            let _ = sender.try_send(Err(Error::database(
                                "receiving PostgreSQL notification",
                            )(error)));
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
                        match classify_delivery(sender.try_send(Ok(id))) {
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
                // companion `Drop` impl issues a `pg_notify` so that, after
                // this sleep elapses, the next `notifications_iter` call
                // returns immediately and the cancel flag is observed without
                // an additional poll interval of latency.
                std::thread::sleep(NOTIFY_LISTENER_POLL_INTERVAL);
            }
            unlisten(&mut conn);
        })
    {
        let _ = spawn_error_sender.try_send(Err(Error::NotifyListener(error.to_string())));
    }
    NotifyTaskIds {
        receiver,
        cancel,
        pool,
    }
}

pub(crate) struct NotifyTaskIds {
    receiver: mpsc::Receiver<Result<PgTaskId, Error>>,
    cancel: Arc<AtomicBool>,
    pool: PgPool,
}

impl Stream for NotifyTaskIds {
    type Item = Result<PgTaskId, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_next(cx)
    }
}

impl Drop for NotifyTaskIds {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Release);
        // Best-effort wakeup: detach the blocking NOTIFY onto a dedicated
        // thread so we never block the dropping async task (which may be
        // running on an async executor). The listener thread is parked inside
        // libpq's `notifications_iter`; we cannot interrupt it directly, but
        // sending a NOTIFY forces the iterator to return so the cancel flag is
        // re-checked. The empty payload fails `serde_json::from_str` so no
        // listener consumes it as a real wakeup.
        let pool = self.pool.clone();
        let _ = std::thread::Builder::new()
            .name("apalis-postgres-notify-drop".to_owned())
            .spawn(move || {
                if let Ok(mut conn) = pool.get() {
                    let _ =
                        sql_query("SELECT pg_notify('apalis::job::insert', '')").execute(&mut conn);
                }
            });
    }
}

#[cfg(test)]
mod tests {
    use lets_expect::*;

    use super::*;

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

    lets_expect! {
        expect(classify_a_delivered_send()) {
            when the_channel_accepts_the_id {
                to reports_the_id_as_delivered { equal(DeliveryOutcome::Delivered) }
            }
        }

        expect(classify_a_full_channel()) {
            when the_channel_is_full_but_still_connected {
                to drops_the_wakeup_without_stopping_the_listener {
                    equal(DeliveryOutcome::ChannelFull)
                }
            }
        }

        expect(classify_a_dropped_receiver()) {
            when the_receiver_has_been_dropped {
                to signals_the_listener_to_stop { equal(DeliveryOutcome::ReceiverGone) }
            }
        }
    }
}
