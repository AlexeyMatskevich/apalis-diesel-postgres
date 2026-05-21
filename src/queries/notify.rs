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
                        match sender.try_send(Ok(id)) {
                            Ok(()) => {}
                            Err(error) if error.is_disconnected() => break 'listen,
                            // Channel full: drop the wakeup. The job is durable
                            // in `apalis.jobs`, and the polling fetcher will
                            // pick it up on its next tick. Logging is left to
                            // the application via tracing wrappers around the
                            // returned stream.
                            Err(_) => break,
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
