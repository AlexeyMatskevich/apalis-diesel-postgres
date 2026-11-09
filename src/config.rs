use std::{sync::Arc, time::Duration};

use apalis_core::backend::{
    ext::poll_strategy::{BackoffConfig, BoxedPollStrategy, IntervalStrategy, PollStrategy},
    queue::Queue,
};

/// Builds a fresh poll strategy for one fetcher.
type StrategyFactory = Arc<dyn Fn() -> BoxedPollStrategy + Send + Sync>;

/// Configuration of a [`PostgresStorage`](crate::PostgresStorage) queue.
///
/// Until apalis 1.0 this type came from `apalis-sql`, which apalis 1.0 no
/// longer ships; it keeps the same builder methods except for the poll
/// strategy: [`Config::with_poll_strategy`] replaces `with_poll_interval` and
/// takes a *factory*, so every fetcher built from the config, or from any clone
/// of it, gets an independent strategy instead of sharing one single-use
/// strategy.
#[derive(Clone)]
pub struct Config {
    keep_alive: Duration,
    buffer_size: usize,
    poll_strategy: StrategyFactory,
    reenqueue_orphaned_after: Duration,
    queue: Queue,
    ack: bool,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("keep_alive", &self.keep_alive)
            .field("buffer_size", &self.buffer_size)
            .field("reenqueue_orphaned_after", &self.reenqueue_orphaned_after)
            .field("queue", &self.queue)
            .field("ack", &self.ack)
            .finish_non_exhaustive()
    }
}

/// The default poll strategy: a 100ms interval that backs off while the queue
/// stays empty, as `apalis-sql`'s default did.
fn default_poll_strategy() -> BoxedPollStrategy {
    Box::new(
        IntervalStrategy::new(Duration::from_millis(100)).with_backoff(BackoffConfig::default()),
    )
}

impl Default for Config {
    fn default() -> Self {
        Self {
            keep_alive: Duration::from_secs(30),
            buffer_size: 10,
            poll_strategy: Arc::new(default_poll_strategy),
            reenqueue_orphaned_after: Duration::from_secs(300),
            queue: Queue::from("default"),
            ack: true,
        }
    }
}

impl Config {
    /// Create a config for the queue named `queue`.
    #[must_use]
    pub fn new(queue: &str) -> Self {
        Self {
            queue: Queue::from(queue),
            ..Default::default()
        }
    }

    /// Use the strategy `factory` builds to decide when to poll for work.
    ///
    /// The factory runs once per fetcher, so clones of this config and every
    /// worker built from it get their own strategy. Defaults to a 100ms
    /// interval with backoff.
    #[must_use]
    pub fn with_poll_strategy<F, S>(mut self, factory: F) -> Self
    where
        F: Fn() -> S + Send + Sync + 'static,
        S: PollStrategy + Send + Sync + 'static,
    {
        self.poll_strategy = Arc::new(move || Box::new(factory()));
        self
    }

    /// Interval between worker keep-alive database updates.
    ///
    /// Defaults to 30s.
    #[must_use]
    pub fn set_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// Number of tasks fetched per poll.
    ///
    /// Defaults to 10.
    #[must_use]
    pub fn set_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// The keep-alive interval.
    #[must_use]
    pub fn keep_alive(&self) -> &Duration {
        &self.keep_alive
    }

    /// A mutable reference to the keep-alive interval.
    pub fn keep_alive_mut(&mut self) -> &mut Duration {
        &mut self.keep_alive
    }

    /// The number of tasks fetched per poll.
    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Build a fresh poll strategy from this config's factory.
    #[must_use]
    pub fn poll_strategy(&self) -> BoxedPollStrategy {
        (self.poll_strategy)()
    }

    /// The queue name.
    #[must_use]
    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    /// A mutable reference to the queue name.
    pub fn queue_mut(&mut self) -> &mut Queue {
        &mut self.queue
    }

    /// How long a worker may miss heartbeats before its tasks are re-enqueued.
    #[must_use]
    pub fn reenqueue_orphaned_after(&self) -> Duration {
        self.reenqueue_orphaned_after
    }

    /// A mutable reference to the orphan deadline.
    pub fn reenqueue_orphaned_after_mut(&mut self) -> &mut Duration {
        &mut self.reenqueue_orphaned_after
    }

    /// Workers sometimes die or abandon tasks because of panics. This is how
    /// long such a task waits before it goes back to the queue.
    ///
    /// Defaults to 5 minutes.
    #[must_use]
    pub fn set_reenqueue_orphaned_after(mut self, after: Duration) -> Self {
        self.reenqueue_orphaned_after = after;
        self
    }

    /// Whether tasks are acknowledged automatically after they run.
    #[must_use]
    pub fn ack(&self) -> bool {
        self.ack
    }

    /// Set whether tasks are acknowledged automatically after they run.
    ///
    /// Defaults to true.
    #[must_use]
    pub fn set_ack(mut self, auto_ack: bool) -> Self {
        self.ack = auto_ack;
        self
    }
}

/// `chrono` timestamps, as the database columns use.
pub(crate) type DateTime = chrono::DateTime<chrono::Utc>;

/// Unix-timestamp helpers for [`DateTime`], as `apalis-sql` provided.
#[allow(dead_code)]
pub(crate) trait DateTimeExt {
    fn now() -> Self;
    fn to_unix_timestamp(&self) -> i64;
    fn from_unix_timestamp(secs: i64) -> Self;
}

impl DateTimeExt for DateTime {
    fn now() -> Self {
        chrono::Utc::now()
    }

    fn to_unix_timestamp(&self) -> i64 {
        self.timestamp()
    }

    fn from_unix_timestamp(secs: i64) -> Self {
        Self::from_timestamp(secs, 0).unwrap_or_default()
    }
}
