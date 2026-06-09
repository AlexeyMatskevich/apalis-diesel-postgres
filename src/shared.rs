use std::{
    collections::HashMap,
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

/// `SharedRegistry` now stores **multiple senders per queue** instead of a
/// single `Arc<Mutex<Receiver>>` shared by clones. Each consumer
/// (`make_shared_with_config` call or `SharedFetcher::clone`) gets its own
/// mpsc channel; the listener broadcasts to every sender bound to the matching
/// `job_type`. This removes the `Arc<Mutex<Receiver>>` contention smell and
/// lets fetcher polls run without mutex acquisition.
type RegistrySender = Sender<Result<PgTaskId, Error>>;

use apalis_codec::json::JsonCodec;
use apalis_core::{backend::shared::MakeShared, worker::context::WorkerContext};
use diesel::RunQueryDsl;
use futures::{
    Stream,
    channel::mpsc::{self, Receiver, Sender},
};
use ulid::Ulid;

use crate::{
    CompactType, Config, Error, PgPool, PgTask, PgTaskId, PostgresStorage, queries, sink::PgSink,
};

/// Per-registration sender entry.
///
/// `id` uniquely identifies the `SharedRegistration` that owns this sender so
/// that `SharedRegistration::drop` can prune only its own sender from the
/// queue's Vec instead of wiping the whole entry (which would silently sever
/// every other consumer on the same queue).
type RegistryEntry = (Ulid, RegistrySender);
type RegistryMap = HashMap<String, Vec<RegistryEntry>>;
type SharedRegistry = Arc<Mutex<RegistryMap>>;

/// Factory for shared notify-backed PostgreSQL storage instances.
///
/// A shared storage factory owns one listener thread and one pooled PostgreSQL
/// connection for notifications. A queue may be registered multiple times: the
/// single listener broadcasts every notification to all consumers registered on
/// the matching queue.
pub struct SharedPostgresStorage<Codec = JsonCodec<CompactType>> {
    pool: PgPool,
    registry: SharedRegistry,
    /// Single source of truth for «listener thread is alive». `make_shared_…`
    /// CAS-swaps it to `true` and spawns a listener only on the `false → true`
    /// transition; the listener clears it on exit. Replaces the prior
    /// `registry.is_empty()` heuristic, which had a race window: when the last
    /// registration dropped and a new one was added before the old listener's
    /// next empty-check, the new caller saw `is_empty == false` and skipped
    /// spawning — leaving the new registration with no listener (or, in the
    /// mirror case, briefly running two listeners and double-delivering).
    listener_alive: Arc<AtomicBool>,
    _marker: PhantomData<Codec>,
}

impl<Codec> SharedPostgresStorage<Codec> {
    /// Create a shared storage factory.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        let registry: SharedRegistry = Arc::new(Mutex::new(HashMap::new()));
        Self {
            pool,
            registry,
            listener_alive: Arc::new(AtomicBool::new(false)),
            _marker: PhantomData,
        }
    }

    fn spawn_registry_listener(&self) {
        let pool = self.pool.clone();
        let registry = self.registry.clone();
        let listener_alive = self.listener_alive.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("apalis-postgres-shared-listener".to_owned())
            .spawn(move || {
                let mut conn = match pool.get() {
                    Ok(conn) => conn,
                    Err(error) => {
                        exit_listener(
                            &registry,
                            &listener_alive,
                            Some(format!(
                                "failed to get pooled connection for shared LISTEN: {error}"
                            )),
                        );
                        return;
                    }
                };
                if let Err(error) =
                    diesel::sql_query("LISTEN \"apalis::job::insert\"").execute(&mut conn)
                {
                    exit_listener(
                        &registry,
                        &listener_alive,
                        Some(format!("failed to start shared LISTEN listener: {error}")),
                    );
                    return;
                }
                run_listener_loop(&mut conn, &registry, &listener_alive);
                // Remove the subscription before the pooled connection drops
                // back into r2d2: the next pool user would otherwise inherit
                // it and notifications would silently accumulate in libpq's
                // receive buffer with no consumer draining them (mirrors the
                // single-queue listener cleanup in `queries/notify.rs`).
                // Best-effort: on failure the connection is in an unknown
                // state and r2d2's checkout health check decides its fate.
                let _ = diesel::sql_query("UNLISTEN \"apalis::job::insert\"").execute(&mut conn);
            })
        {
            exit_listener(
                &self.registry,
                &self.listener_alive,
                Some(format!("failed to spawn listener: {error}")),
            );
        }
    }
}

/// Body of the shared listener thread, between a successful `LISTEN` and the
/// `UNLISTEN` cleanup. Factored out of `spawn_registry_listener` so every exit
/// path — notification error, poisoned registry, empty registry — funnels
/// through a single return boundary, after which the caller removes the
/// LISTEN subscription before the pooled connection is returned to r2d2.
fn run_listener_loop(
    conn: &mut diesel::r2d2::PooledConnection<
        diesel::r2d2::ConnectionManager<diesel::PgConnection>,
    >,
    registry: &SharedRegistry,
    listener_alive: &AtomicBool,
) {
    loop {
        for notification in conn.notifications_iter() {
            let notification = match notification {
                Ok(notification) => notification,
                Err(error) => {
                    exit_listener(
                        registry,
                        listener_alive,
                        Some(format!("failed to receive shared notification: {error}")),
                    );
                    return;
                }
            };
            let Ok(event) = serde_json::from_str::<crate::InsertEvent>(&notification.payload)
            else {
                continue;
            };
            let (event_queue, ids) = event.into_ids();
            let Ok(mut registry) = registry.lock() else {
                // Poisoned: we cannot synchronize with registrants
                // any longer, fall back to a bare store.
                listener_alive.store(false, Ordering::Release);
                return;
            };
            deliver_to_queue(&mut registry, &event_queue, &ids);
        }
        match registry.lock() {
            Ok(registry) => {
                // Store `false` while still holding the registry
                // lock: a concurrent `make_shared_with_config`
                // must observe either (a) `listener_alive == true`
                // (we haven't exited yet) AND see itself appended
                // to the registry on our next loop iteration, or
                // (b) `listener_alive == false` AND therefore
                // spawn a fresh listener. The exit decision is
                // factored into `listener_should_exit` (unit-tested)
                // and applied as a plain `if` rather than a match
                // guard so the only mutable point is that function,
                // which the tests pin — an in-thread guard would be
                // unreachable from a unit test.
                if listener_should_exit(&registry) {
                    listener_alive.store(false, Ordering::Release);
                    drop(registry);
                    return;
                }
            }
            Err(_) => {
                // Poisoned: synchronization is no longer possible.
                listener_alive.store(false, Ordering::Release);
                return;
            }
        }
        std::thread::sleep(queries::NOTIFY_LISTENER_POLL_INTERVAL);
    }
}

/// Drop the listener under the registry lock so a concurrent
/// `make_shared_with_config` cannot observe `listener_alive == true` AFTER the
/// listener has decided to exit. The same lock serializes registrants'
/// `swap(true)` against our `store(false)`, leaving exactly two possible
/// orderings: (a) registrant runs first and sees `listener_alive == false`,
/// spawning a fresh listener; (b) listener runs first, sees an empty registry,
/// stores `false`, and a subsequent registrant spawns. Without this serialization
/// a registrant could observe stale `true` and skip spawn.
fn exit_listener(registry: &SharedRegistry, listener_alive: &AtomicBool, error: Option<String>) {
    match registry.lock() {
        Ok(mut guard) => {
            if let Some(message) = error {
                broadcast_notify_error_locked(&mut guard, message);
            }
            listener_alive.store(false, Ordering::Release);
            drop(guard);
        }
        Err(_) => {
            // Poisoned: best-effort store; we cannot synchronize with
            // registrants any more.
            listener_alive.store(false, Ordering::Release);
        }
    }
}

/// Broadcast every id in `ids` to all senders registered on `queue`.
///
/// A sender whose receiver has been dropped (`disconnected`) is pruned; a sender
/// whose channel is merely full is **kept** — the job is durable and the poll
/// fetcher will pick it up, so transient back-pressure must not sever the
/// consumer. When the queue's last sender is pruned the queue entry is removed
/// so the listener's empty-registry exit check can fire.
///
/// Extracted from the listener closure so the broadcast/prune decision is
/// unit-testable against a hand-built registry, without spawning the listener
/// thread (mirrors `broadcast_notify_error_locked`).
fn deliver_to_queue(registry: &mut RegistryMap, queue: &str, ids: &[PgTaskId]) {
    if let Some(senders) = registry.get_mut(queue) {
        for &id in ids {
            senders.retain_mut(|(_, sender)| match sender.try_send(Ok(id)) {
                Ok(()) => true,
                Err(error) if error.is_disconnected() => false,
                // Channel full: keep the sender (the job is durable, the poll
                // fetcher will pick it up) but stop pushing this event into a
                // saturated channel.
                Err(_) => true,
            });
        }
        if senders.is_empty() {
            registry.remove(queue);
        }
    }
}

/// Whether the shared listener should exit. It stops once no consumers remain
/// registered, releasing its pooled connection and thread. Extracted so the
/// empty-registry decision is unit-testable without spawning the listener.
fn listener_should_exit(registry: &RegistryMap) -> bool {
    registry.is_empty()
}

/// Whether THIS registration must spawn the shared listener: it does iff it
/// observed `listener_alive` transition false→true, i.e. it is the first live
/// registration. The atomic swap is the single source of truth, so exactly one
/// registration spawns. Extracted so the false→true edge is unit-testable — the
/// notify integration tests register two-or-more consumers, which masks an
/// off-by-one in *which* registration spawns (the listener still ends up
/// running, just claimed by the wrong call).
fn claim_listener_spawn(listener_alive: &AtomicBool) -> bool {
    !listener_alive.swap(true, Ordering::AcqRel)
}

#[cfg(test)]
fn broadcast_notify_error(registry: &SharedRegistry, message: String) {
    let Ok(mut guard) = registry.lock() else {
        return;
    };
    broadcast_notify_error_locked(&mut guard, message);
}

fn broadcast_notify_error_locked(registry: &mut RegistryMap, message: String) {
    registry.retain(|_, senders| {
        senders.retain_mut(|(_, sender)| {
            match sender.try_send(Err(Error::NotifyListener(message.clone()))) {
                Ok(()) => true,
                Err(error) => !error.is_disconnected(),
            }
        });
        !senders.is_empty()
    });
}

impl<Codec> std::fmt::Debug for SharedPostgresStorage<Codec> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedPostgresStorage")
            .finish_non_exhaustive()
    }
}

/// Errors returned while creating shared storage instances.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SharedPostgresError {
    /// Shared registry lock is poisoned.
    #[error("registry lock poisoned")]
    RegistryLocked,
}

impl<Args, Codec> MakeShared<Args> for SharedPostgresStorage<Codec> {
    type Backend = PostgresStorage<Args, Codec, SharedFetcher>;
    type Config = Config;
    type MakeError = SharedPostgresError;

    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError>
    where
        Self::Config: Default,
    {
        self.make_shared_with_config(Config::new(std::any::type_name::<Args>()))
    }

    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (sender, receiver) = mpsc::channel(
            config
                .buffer_size()
                .clamp(1, crate::queries::NOTIFY_CHANNEL_CAPACITY_MAX),
        );
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| SharedPostgresError::RegistryLocked)?;
        let queue = config.queue().to_string();
        // Broadcast redesign: multiple consumers per queue are now allowed —
        // each call appends its own sender to the queue's Vec, and the
        // listener broadcasts to all of them. Previously the registry held a
        // single Sender per queue and clones shared the Receiver via
        // `Arc<Mutex<Receiver>>`, which serialized polls on a mutex.
        //
        // `listener_alive` is the single source of truth for «is a listener
        // currently running». The swap and the registry mutation must happen
        // under the *same* registry lock as the listener's exit decision
        // (see `spawn_registry_listener`), otherwise the listener could store
        // `false` between our `swap(true)` and our push, leaving a non-empty
        // registry with no listener. By doing both under the lock we serialize
        // the two state transitions onto the mutex.
        let registration_id = Ulid::new();
        registry
            .entry(queue)
            .or_default()
            .push((registration_id, sender));
        let should_spawn_listener = claim_listener_spawn(&self.listener_alive);
        drop(registry);

        if should_spawn_listener {
            self.spawn_registry_listener();
        }

        let registration = Arc::new(SharedRegistration {
            id: registration_id,
            queue: config.queue().to_string(),
            registry: self.registry.clone(),
            pool: self.pool.clone(),
        });

        Ok(PostgresStorage {
            _marker: PhantomData,
            sink: PgSink::new(&self.pool, &config),
            pool: self.pool.clone(),
            config,
            fetcher: SharedFetcher {
                receiver,
                _registration: registration,
            },
            lease_token: crate::queries::worker::mint_lease_token().into(),
        })
    }
}

struct SharedRegistration {
    /// Identity of this registration's sender inside the queue's Vec.
    /// `Drop` uses it to prune only this entry — wiping the whole queue
    /// would silently sever every other consumer registered on the same
    /// queue (broadcast design allows N senders per queue).
    id: Ulid,
    queue: String,
    registry: SharedRegistry,
    pool: PgPool,
}

impl std::fmt::Debug for SharedRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedRegistration")
            .field("queue", &self.queue)
            .finish_non_exhaustive()
    }
}

impl Drop for SharedRegistration {
    fn drop(&mut self) {
        let became_empty = match self.registry.lock() {
            Ok(mut registry) => {
                // Prune only this registration's sender from the queue's
                // Vec. If the Vec becomes empty (we were the last consumer
                // on this queue), drop the queue entry too.
                if let Some(senders) = registry.get_mut(&self.queue) {
                    senders.retain(|(id, _)| *id != self.id);
                    if senders.is_empty() {
                        registry.remove(&self.queue);
                    }
                }
                registry.is_empty()
            }
            Err(_) => false,
        };
        // When the registry becomes empty the shared listener thread will exit
        // on its next loop iteration, but it is parked inside
        // `notifications_iter`. Send a best-effort NOTIFY so the iterator
        // returns and the empty-registry check runs immediately. The empty
        // payload fails `serde_json::from_str::<InsertEvent>`, so any other
        // listener simply ignores it.
        if became_empty {
            // Detach the blocking NOTIFY so the dropping task — which may be
            // running on an async executor — never blocks on libpq.
            let pool = self.pool.clone();
            let _ = std::thread::Builder::new()
                .name("apalis-postgres-shared-drop".to_owned())
                .spawn(move || {
                    if let Ok(mut conn) = pool.get() {
                        let _ = diesel::sql_query("SELECT pg_notify('apalis::job::insert', '')")
                            .execute(&mut conn);
                    }
                });
        }
    }
}

/// Fetcher used by shared storage instances.
///
/// After the broadcast redesign each `SharedFetcher` owns its own mpsc
/// `Receiver` — no `Arc<Mutex<Receiver>>` indirection. The listener broadcasts
/// every notification to every registered fetcher for that queue. As a
/// consequence `SharedFetcher` is **not** `Clone`: cloning would require
/// either splitting one receiver into two (impossible without locking) or
/// silently producing a fetcher that never receives events. Use
/// [`SharedPostgresStorage::make_shared_with_config`] to spawn additional
/// consumers explicitly.
pub struct SharedFetcher {
    receiver: Receiver<Result<PgTaskId, Error>>,
    _registration: Arc<SharedRegistration>,
}

impl std::fmt::Debug for SharedFetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedFetcher").finish_non_exhaustive()
    }
}

impl Stream for SharedFetcher {
    type Item = Result<PgTaskId, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().receiver).poll_next(cx)
    }
}

impl crate::fetcher::PgFetcherSource for SharedFetcher {
    const STORAGE_NAME: &'static str = "SharedPostgresStorage";

    fn into_compact_stream(
        self,
        pool: PgPool,
        config: Config,
        worker: WorkerContext,
        lease_token: std::sync::Arc<str>,
    ) -> apalis_core::backend::TaskStream<PgTask<CompactType>, Error> {
        crate::fetcher::notify_backed_compact_stream(
            Self::STORAGE_NAME,
            self,
            pool,
            config,
            worker,
            lease_token,
        )
    }
}

#[cfg(test)]
mod tests {
    use apalis_core::backend::{Backend, BackendExt, shared::MakeShared};
    use diesel::{
        PgConnection,
        r2d2::{ConnectionManager, Pool},
    };
    use lets_expect::{AssertionError, AssertionResult, *};

    use super::*;

    struct SharedObservation {
        queue: String,
        buffer_size: usize,
        debug: String,
    }

    fn unchecked_pool() -> PgPool {
        let manager = ConnectionManager::<PgConnection>::new("postgres://127.0.0.1:1/not-used");
        Pool::builder()
            .max_size(1)
            .connection_timeout(std::time::Duration::from_millis(10))
            .build_unchecked(manager)
    }

    fn shared_debug() -> String {
        let shared: SharedPostgresStorage = SharedPostgresStorage::new(unchecked_pool());
        format!("{shared:?}")
    }

    fn make_default_shared() -> Result<SharedObservation, SharedPostgresError> {
        let mut shared: SharedPostgresStorage = SharedPostgresStorage::new(unchecked_pool());
        let storage = <SharedPostgresStorage as MakeShared<String>>::make_shared(&mut shared)?;
        Ok(SharedObservation {
            queue: storage.config.queue().to_string(),
            buffer_size: storage.config.buffer_size(),
            debug: format!("{storage:?}"),
        })
    }

    fn make_configured_shared() -> Result<SharedObservation, SharedPostgresError> {
        let mut shared: SharedPostgresStorage = SharedPostgresStorage::new(unchecked_pool());
        let config = Config::new("shared-unit").set_buffer_size(3);
        let storage = <SharedPostgresStorage as MakeShared<String>>::make_shared_with_config(
            &mut shared,
            config,
        )?;
        Ok(SharedObservation {
            queue: storage.get_queue().to_string(),
            buffer_size: storage.config.buffer_size(),
            debug: format!("{:?}", storage.fetcher),
        })
    }

    fn shared_trait_surfaces() -> Result<(String, String), SharedPostgresError> {
        let mut shared: SharedPostgresStorage = SharedPostgresStorage::new(unchecked_pool());
        let config = Config::new("shared-traits");
        let storage = <SharedPostgresStorage as MakeShared<String>>::make_shared_with_config(
            &mut shared,
            config,
        )?;
        let worker = WorkerContext::new::<()>("shared-trait-worker");
        let middleware_name = std::any::type_name_of_val(&storage.middleware()).to_owned();
        let stream_name = std::any::type_name_of_val(&storage.poll_compact(&worker)).to_owned();
        Ok((middleware_name, stream_name))
    }

    fn registration_debug_and_drop() -> (String, bool) {
        let registry: SharedRegistry = Arc::new(Mutex::new(HashMap::new()));
        let (sender, _receiver) = mpsc::channel(1);
        let id = Ulid::new();
        registry
            .lock()
            .expect("fresh shared registry is not poisoned")
            .insert("shared-registration".to_owned(), vec![(id, sender)]);

        let debug = {
            let registration = SharedRegistration {
                id,
                queue: "shared-registration".to_owned(),
                registry: registry.clone(),
                pool: unchecked_pool(),
            };
            format!("{registration:?}")
        };

        let removed = registry
            .lock()
            .expect("fresh shared registry is not poisoned")
            .is_empty();
        (debug, removed)
    }

    /// Build a registry that contains `target_queue` plus optional sibling
    /// queues, then drop a `SharedRegistration` that points at `target_queue`.
    /// Returns the number of entries left in the registry after the drop —
    /// zero when the dropped registration was the last one (the empty-branch
    /// triggers the best-effort NOTIFY wake-up), positive when siblings remain.
    fn drop_leaves_remaining(target_queue: &str, sibling_queues: &[&str]) -> usize {
        let registry: SharedRegistry = Arc::new(Mutex::new(HashMap::new()));
        let target_id = Ulid::new();
        {
            let mut reg = registry
                .lock()
                .expect("fresh shared registry is not poisoned");
            let (sender, _r) = mpsc::channel(1);
            reg.insert(target_queue.to_owned(), vec![(target_id, sender)]);
            for sibling in sibling_queues {
                let (sender, _r) = mpsc::channel(1);
                reg.insert((*sibling).to_owned(), vec![(Ulid::new(), sender)]);
            }
        }

        {
            let registration = SharedRegistration {
                id: target_id,
                queue: target_queue.to_owned(),
                registry: registry.clone(),
                pool: unchecked_pool(),
            };
            drop(registration);
        }

        registry
            .lock()
            .expect("fresh shared registry is not poisoned")
            .len()
    }

    fn drop_when_registry_empties() -> usize {
        drop_leaves_remaining("shared-only", &[])
    }

    fn drop_when_registry_has_siblings() -> usize {
        drop_leaves_remaining("shared-target", &["shared-other-a", "shared-other-b"])
    }

    /// Drop of one registration on a queue with two consumers must leave the
    /// other sender intact. Regression test for the bug where
    /// `registry.remove(&queue)` wiped the whole entry, severing the second
    /// consumer's notify stream.
    fn drop_one_of_two_keeps_sibling_sender() -> usize {
        let registry: SharedRegistry = Arc::new(Mutex::new(HashMap::new()));
        let queue = "shared-coexist".to_owned();
        let first_id = Ulid::new();
        let second_id = Ulid::new();
        let (first_sender, _first_rx) = mpsc::channel(1);
        let (second_sender, _second_rx) = mpsc::channel(1);
        registry
            .lock()
            .expect("fresh registry is not poisoned")
            .insert(
                queue.clone(),
                vec![(first_id, first_sender), (second_id, second_sender)],
            );

        drop(SharedRegistration {
            id: first_id,
            queue: queue.clone(),
            registry: registry.clone(),
            pool: unchecked_pool(),
        });

        let guard = registry.lock().expect("registry is not poisoned");
        guard.get(&queue).map(Vec::len).unwrap_or(0)
    }

    /// Poison the registry mutex (panic while holding the lock, like
    /// `make_shared_with_poisoned_registry`), then drop a `SharedRegistration`
    /// pointing at an existing queue. The `Err(_) => false` arm of `Drop`
    /// (src/shared.rs:356) must run: it prunes nothing and fires no best-effort
    /// wake-up NOTIFY, so the queue's sender Vec is left untouched at length 1.
    /// The helper returning at all proves the drop did not panic on the poison.
    fn drop_with_poisoned_registry() -> usize {
        let registry: SharedRegistry = Arc::new(Mutex::new(HashMap::new()));
        let queue = "shared-poisoned-drop".to_owned();
        let id = Ulid::new();
        let (sender, _receiver) = mpsc::channel(1);
        registry
            .lock()
            .expect("fresh registry is not poisoned")
            .insert(queue.clone(), vec![(id, sender)]);

        let poison_target = registry.clone();
        let join = std::thread::spawn(move || {
            let _guard = poison_target
                .lock()
                .expect("fresh registry lock is not poisoned");
            panic!("synthetic poisoning panic");
        });
        let _ = join.join();

        drop(SharedRegistration {
            id,
            queue: queue.clone(),
            registry: registry.clone(),
            pool: unchecked_pool(),
        });

        // Recover past the poison to confirm the sender was left in place.
        registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&queue)
            .map(Vec::len)
            .unwrap_or(0)
    }

    /// Re-registering a namespace that already lives in the registry now
    /// succeeds: the broadcast redesign allows multiple consumers per queue, so
    /// the second `make_shared_with_config` must also return `Ok`.
    fn double_make_shared_same_queue() -> Result<(), SharedPostgresError> {
        let mut shared: SharedPostgresStorage = SharedPostgresStorage::new(unchecked_pool());
        let config = Config::new("double-make-shared");
        let _first = <SharedPostgresStorage as MakeShared<String>>::make_shared_with_config(
            &mut shared,
            config.clone(),
        )?;
        let _second = <SharedPostgresStorage as MakeShared<String>>::make_shared_with_config(
            &mut shared,
            config,
        )?;
        Ok(())
    }

    /// `broadcast_notify_error` walks the registry, delivers the error to each
    /// sender, then keeps only the queues whose sender vec is still non-empty.
    /// The returned tuple is `(retained_after_broadcast,
    /// alive_is_the_sole_survivor)`: the count alone cannot catch an inverted
    /// keep-predicate (it would retain the wrong queue but the same total), so
    /// the bool pins that the *live* queue — not the pruned-empty "dead" one —
    /// is what survives. No listener thread is touched.
    fn broadcast_notify_error_observation() -> (usize, bool) {
        let registry: SharedRegistry = Arc::new(Mutex::new(HashMap::new()));
        let (alive_sender, _alive_receiver) = mpsc::channel(1);
        let (dead_sender, dead_receiver) = mpsc::channel::<Result<PgTaskId, Error>>(1);
        drop(dead_receiver);
        {
            let mut reg = registry.lock().expect("fresh registry is not poisoned");
            reg.insert("alive".to_owned(), vec![(Ulid::new(), alive_sender)]);
            reg.insert("dead".to_owned(), vec![(Ulid::new(), dead_sender)]);
        }

        broadcast_notify_error(&registry, "synthetic listener failure".to_owned());
        let reg = registry.lock().expect("registry is not poisoned");
        let retained = reg.len();
        let alive_is_the_sole_survivor = reg.contains_key("alive") && !reg.contains_key("dead");
        (retained, alive_is_the_sole_survivor)
    }

    fn new_task_id() -> PgTaskId {
        PgTaskId::new(Ulid::new())
    }

    /// `deliver_to_queue` must prune a sender whose receiver was dropped
    /// (disconnected) and, being the queue's last consumer, remove the now-empty
    /// queue so the listener's empty-registry exit can fire. Returns the registry
    /// length after delivery — 0 once the dead sender and its queue are gone.
    fn deliver_prunes_disconnected_sender() -> usize {
        let mut registry: RegistryMap = HashMap::new();
        let (dead_sender, dead_receiver) = mpsc::channel::<Result<PgTaskId, Error>>(1);
        drop(dead_receiver);
        registry.insert(
            "shared-deliver-dead".to_owned(),
            vec![(Ulid::new(), dead_sender)],
        );
        deliver_to_queue(&mut registry, "shared-deliver-dead", &[new_task_id()]);
        registry.len()
    }

    /// `deliver_to_queue` must KEEP a sender whose channel is full (transient
    /// back-pressure) — only a disconnected sender is pruned, because the job is
    /// durable and the poll fetcher will pick it up. The receiver is held open so
    /// `try_send` reports `Full`, not `Disconnected`; more ids than the channel
    /// can ever buffer are delivered so at least one send hits the full path.
    /// Returns the number of senders still registered on the queue.
    fn deliver_keeps_full_sender() -> usize {
        let mut registry: RegistryMap = HashMap::new();
        let (full_sender, _full_receiver) = mpsc::channel::<Result<PgTaskId, Error>>(1);
        registry.insert(
            "shared-deliver-full".to_owned(),
            vec![(Ulid::new(), full_sender)],
        );
        let ids = [new_task_id(), new_task_id(), new_task_id(), new_task_id()];
        deliver_to_queue(&mut registry, "shared-deliver-full", &ids);
        registry
            .get("shared-deliver-full")
            .map(Vec::len)
            .unwrap_or(0)
    }

    fn empty_registry() -> RegistryMap {
        HashMap::new()
    }

    fn registry_with_one_consumer() -> RegistryMap {
        let mut registry: RegistryMap = HashMap::new();
        let (sender, _receiver) = mpsc::channel::<Result<PgTaskId, Error>>(1);
        registry.insert(
            "shared-still-active".to_owned(),
            vec![(Ulid::new(), sender)],
        );
        registry
    }

    /// `exit_listener` must both flip `listener_alive` to false and broadcast the
    /// failure to every registered sender. Returns `(alive_after, error_delivered)`
    /// — `(false, true)` when both effects happen; the live receiver is kept so the
    /// broadcast lands.
    fn exit_listener_observation() -> (bool, bool) {
        let registry: SharedRegistry = Arc::new(Mutex::new(HashMap::new()));
        let (sender, mut receiver) = mpsc::channel::<Result<PgTaskId, Error>>(1);
        registry
            .lock()
            .expect("fresh registry is not poisoned")
            .insert("shared-exit".to_owned(), vec![(Ulid::new(), sender)]);
        let listener_alive = AtomicBool::new(true);
        exit_listener(
            &registry,
            &listener_alive,
            Some("synthetic listener spawn failure".to_owned()),
        );
        let alive_after = listener_alive.load(Ordering::Acquire);
        let error_delivered = matches!(receiver.try_recv(), Ok(Err(_)));
        (alive_after, error_delivered)
    }

    /// First claim on a fresh flag must spawn (false→true edge), the second must
    /// not. Returns `(first_claim, second_claim)` — `(true, false)` when exactly
    /// the first registration owns the spawn.
    fn listener_spawn_claims() -> (bool, bool) {
        let listener_alive = AtomicBool::new(false);
        let first = claim_listener_spawn(&listener_alive);
        let second = claim_listener_spawn(&listener_alive);
        (first, second)
    }

    // Q6-rest removed `Arc<Mutex<Receiver>>`: each fetcher owns its receiver
    // directly. Poisoned-mutex and locked-receiver paths from the previous
    // architecture no longer exist; their dedicated tests have been removed.

    fn debug_mentions_type(expected: &'static str) -> impl Fn(&String) -> AssertionResult {
        move |debug| {
            if debug.contains(expected) {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected debug output containing {expected:?}, got {debug}"
                )]))
            }
        }
    }

    fn uses_default_queue(result: &SharedObservation) -> AssertionResult {
        if result.queue == std::any::type_name::<String>()
            && result.buffer_size == 10
            && result.debug.contains("SharedFetcher")
        {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "unexpected default shared storage: queue={:?}, buffer={}, debug={}",
                result.queue, result.buffer_size, result.debug
            )]))
        }
    }

    fn uses_configured_queue(result: &SharedObservation) -> AssertionResult {
        if result.queue == "shared-unit"
            && result.buffer_size == 3
            && result.debug.contains("SharedFetcher")
        {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "unexpected configured shared storage: queue={:?}, buffer={}, debug={}",
                result.queue, result.buffer_size, result.debug
            )]))
        }
    }

    fn constructs_backend_traits(result: &(String, String)) -> AssertionResult {
        if result.0.contains("PgMiddleware") && result.1.contains("Stream") {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "unexpected shared trait surfaces: {result:?}"
            )]))
        }
    }

    fn removes_registration(result: &(String, bool)) -> AssertionResult {
        if result.0.contains("SharedRegistration") && result.1 {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected registration debug and drop cleanup, got {result:?}"
            )]))
        }
    }

    /// Drive `make_shared_with_config` against a deliberately-poisoned
    /// registry mutex and surface the resulting error variant. The poisoning
    /// is forced by panicking inside a thread that holds the lock; that is
    /// the only documented way `make_shared_with_config` can return
    /// `SharedPostgresError::RegistryLocked` (shared.rs:170-173).
    fn make_shared_with_poisoned_registry() -> Result<(), SharedPostgresError> {
        let mut shared: SharedPostgresStorage = SharedPostgresStorage::new(unchecked_pool());
        let registry = shared.registry.clone();
        let join = std::thread::spawn(move || {
            let _guard = registry
                .lock()
                .expect("fresh registry lock is not poisoned");
            panic!("synthetic poisoning panic");
        });
        // The poisoning thread panics while holding the lock, leaving the
        // mutex in PoisonError state for the next caller.
        let _ = join.join();
        let config = Config::new("poisoned-registry");
        <SharedPostgresStorage as MakeShared<String>>::make_shared_with_config(&mut shared, config)
            .map(|_| ())
    }

    fn is_registry_locked(error: &SharedPostgresError) -> AssertionResult {
        // `RegistryLocked` is currently the only variant, so the match is
        // exhaustive without a catch-all; adding a variant later will surface
        // here as a compile error, which is the right place to revisit this.
        match error {
            SharedPostgresError::RegistryLocked => Ok(()),
        }
    }

    lets_expect! {
        expect(shared_debug()) {
            to describes_the_shared_factory { debug_mentions_type("SharedPostgresStorage") }
        }

        expect(make_default_shared()) {
            when no_config_is_supplied {
                to uses_the_task_type_as_the_namespace { be_ok_and uses_default_queue }
            }
        }

        expect(make_configured_shared()) {
            when config_is_supplied {
                to exposes_the_queue_and_fetcher { be_ok_and uses_configured_queue }
            }
        }

        expect(shared_trait_surfaces()) {
            when backend_traits_are_requested {
                to builds_middleware_and_compact_stream { be_ok_and constructs_backend_traits }
            }
        }

        expect(registration_debug_and_drop()) {
            when registration_is_dropped {
                to removes_the_namespace_from_the_registry { removes_registration }
            }
        }

        expect(drop_when_registry_empties()) {
            when dropping_the_last_registration_empties_the_registry {
                to leaves_no_remaining_registrations { equal(0) }
            }
        }

        expect(drop_when_registry_has_siblings()) {
            when dropping_one_of_several_registrations {
                to keeps_sibling_registrations_intact { equal(2) }
            }
        }

        expect(drop_one_of_two_keeps_sibling_sender()) {
            when dropping_one_of_two_consumers_on_the_same_queue {
                to leaves_the_other_senders_sender_in_place { equal(1) }
            }
        }

        expect(drop_with_poisoned_registry()) {
            when the_registry_mutex_is_poisoned {
                to leaves_the_registration_in_place_without_waking_the_listener {
                    equal(1)
                }
            }
        }

        expect(double_make_shared_same_queue()) {
            when the_same_queue_is_registered_twice {
                // Q6-rest broadcast redesign: multiple consumers per queue
                // are now allowed (a second registration used to be rejected).
                // The listener broadcasts each event to every registered sender.
                to accepts_the_second_registration { be_ok }
            }
        }

        expect(broadcast_notify_error_observation()) {
            when listener_broadcasts_an_error_to_a_mixed_registry {
                to keeps_the_live_queue_and_drops_the_disconnected_one { equal((1_usize, true)) }
            }
        }

        expect(deliver_prunes_disconnected_sender()) {
            when a_queues_only_sender_has_a_dropped_receiver {
                to prunes_the_disconnected_sender_and_removes_the_empty_queue { equal(0) }
            }
        }

        expect(deliver_keeps_full_sender()) {
            when a_queues_sender_channel_is_full_but_still_connected {
                to keeps_the_back_pressured_sender_registered { equal(1) }
            }
        }

        expect(listener_should_exit(&registry)) {
            let registry = empty_registry();

            when no_consumers_remain_registered {
                to signals_the_listener_to_exit { be_true }
            }

            when a_consumer_is_still_registered {
                let registry = registry_with_one_consumer();
                to keeps_the_listener_running { be_false }
            }
        }

        expect(exit_listener_observation()) {
            when the_listener_exits_after_a_spawn_or_connection_failure {
                to clears_the_alive_flag_and_broadcasts_the_error { equal((false, true)) }
            }
        }

        expect(listener_spawn_claims()) {
            when two_registrations_race_to_claim_the_listener_spawn {
                to spawns_on_the_first_registration_only { equal((true, false)) }
            }
        }

        // Q6-rest: removed `locked_fetcher_poll` / `poisoned_fetcher_poll`
        // assertions — the `Arc<Mutex<Receiver>>` they exercised no longer
        // exists. Each fetcher owns its receiver directly after the broadcast
        // redesign.

        expect(make_shared_with_poisoned_registry()) {
            when the_registry_mutex_is_poisoned_by_a_panic_in_another_thread {
                // Sibling to "the_same_queue_is_registered_twice" — covers
                // the other failure mode of make_shared_with_config: the
                // mutex lock itself is unrecoverable rather than the queue
                // being already taken.
                to surfaces_registry_locked_rather_than_panicking_or_succeeding {
                    be_err_and is_registry_locked
                }
            }
        }
    }
}
