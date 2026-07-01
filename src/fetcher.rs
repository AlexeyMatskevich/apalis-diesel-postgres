use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use apalis_core::{
    backend::{
        TaskStream,
        codec::Codec,
        poll_strategy::{PollContext, PollStrategyExt},
    },
    task::Task,
    timer::Delay,
    worker::context::WorkerContext,
};
use futures::{
    FutureExt, Stream, StreamExt, TryFutureExt,
    future::{BoxFuture, ready},
    stream,
};

use crate::{CompactType, Config, Error, PgContext, PgPool, PgTask, PgTaskId, queries};

/// A fetcher that waits for PostgreSQL NOTIFY events.
#[derive(Debug, Clone, Default)]
pub struct PgNotify;

/// Gate `body` behind `register`: emit the registration outcome as the first
/// stream item (preserving the wire contract that consumers observe it), and
/// only proceed to drain `body` when registration succeeded. On failure the
/// body is never polled — fixing the pre-fix shape `once(register).chain(body)`
/// which emitted the registration error but still ran the body afterwards,
/// masking the original error under follow-up FK/lock errors.
///
/// `flat_map` is called at most once (upstream is a 1-item stream), so an
/// `Option::take` is sufficient to move the body out of the `FnMut` closure on
/// its single invocation.
pub(crate) fn register_then_stream<S>(
    register: impl Future<Output = Result<Option<PgTask<CompactType>>, Error>> + Send + 'static,
    body: S,
) -> TaskStream<PgTask<CompactType>, Error>
where
    S: Stream<Item = Result<Option<PgTask<CompactType>>, Error>> + Send + 'static,
{
    let mut body_slot = Some(body);
    stream::once(register)
        .flat_map(move |res| match res {
            Ok(none) => {
                let b = body_slot
                    .take()
                    .expect("registration flat_map invoked twice");
                stream::once(ready(Ok(none))).chain(b).left_stream()
            }
            Err(e) => stream::once(ready(Err(e))).right_stream(),
        })
        .boxed()
}

/// Decode a compact task stream into an `Args`-typed task stream by mapping
/// every yielded row through the configured codec. Shared between the polling
/// and notify backends so the decode logic exists in exactly one place.
///
/// Decode runs *after* the dequeue SQL has already claimed the row as
/// `Running`, so a decode failure must not just surface the error: it also
/// fails the claimed row (best-effort) via `fail_undecodable_task`, otherwise
/// the row would stay `Running` for as long as this worker keeps heartbeating
/// — unackable (ack needs a decoded task) and invisible to orphan recovery
/// (which only reclaims rows of stale workers).
pub(crate) fn decode_task_stream<Args, Decode>(
    compact: TaskStream<PgTask<CompactType>, Error>,
    pool: PgPool,
    worker_id: String,
) -> TaskStream<PgTask<Args>, Error>
where
    Args: Send + 'static,
    Decode: Codec<Args, Compact = CompactType> + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    compact
        .then(move |row| {
            let pool = pool.clone();
            let worker_id = worker_id.clone();
            async move {
                match row {
                    Ok(Some(task)) => {
                        // Claim-epoch identity for the release predicate: the
                        // decode stage runs before the worker increments the
                        // attempt counter, so `attempt.current()` still holds
                        // the row's stored value from the claim.
                        let task_id = task.parts.task_id;
                        let lock_at = *task.parts.ctx.lock_at();
                        let attempts = i32::try_from(task.parts.attempt.current());
                        match task
                            .try_map(|t| Decode::decode(&t).map_err(|e| Error::Decode(e.into())))
                        {
                            Ok(decoded) => Ok(Some(decoded)),
                            Err(error) => {
                                // Best-effort: the decode error is the primary
                                // signal and must surface either way; a missing
                                // claim identity or a failed UPDATE falls back
                                // to the pre-release behaviour (stranded until
                                // the worker stops heartbeating).
                                if let (Some(task_id), Some(lock_at), Ok(attempts)) =
                                    (task_id, lock_at, attempts)
                                {
                                    let _ = queries::fail_undecodable_task(
                                        pool,
                                        task_id,
                                        worker_id,
                                        lock_at,
                                        attempts,
                                        error.to_string(),
                                    )
                                    .await;
                                }
                                Err(error)
                            }
                        }
                    }
                    Ok(None) => Ok(None),
                    Err(error) => Err(error),
                }
            }
        })
        .boxed()
}

impl PgFetcherSource for PgNotify {
    const STORAGE_NAME: &'static str = "PostgresStorageWithNotify";

    fn into_compact_stream(
        self,
        pool: PgPool,
        config: Config,
        worker: WorkerContext,
        lease_token: Arc<str>,
    ) -> TaskStream<PgTask<CompactType>, Error> {
        let ids = queries::notify_task_ids(
            pool.clone(),
            config.queue().to_string(),
            config.buffer_size().max(1),
        );
        notify_backed_compact_stream(Self::STORAGE_NAME, ids, pool, config, worker, lease_token)
    }
}

/// Shared pipeline composition for the two notify-driven fetcher modes
/// (`PgNotify` and `SharedFetcher`): initial registration gate, the id→task
/// batching fetcher fed by `ids`, and the eager polling fetcher merged
/// alongside as the durable fallback. Factored out so the two impls cannot
/// drift apart — only the source of notified ids differs between them.
///
/// Real batching is provided upstream by the statement-level NOTIFY trigger
/// (migration 20260521000001), which emits one event per (queue, INSERT
/// statement) carrying all inserted ids in `ids`. By the time those ids land
/// in the mpsc channel they are already contiguous, so `ready_chunks` (inside
/// `batch_ids_into_tasks`) folds them into one batch in the common bursty
/// case.
pub(crate) fn notify_backed_compact_stream<Ids>(
    storage_name: &'static str,
    ids: Ids,
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
    lease_token: Arc<str>,
) -> TaskStream<PgTask<CompactType>, Error>
where
    Ids: Stream<Item = Result<PgTaskId, Error>> + Send + 'static,
{
    let register_worker = queries::initial_heartbeat(
        pool.clone(),
        config.clone(),
        worker.clone(),
        storage_name,
        lease_token,
    )
    .map_ok(|_| None);

    let lazy_fetcher = queries::batch_ids_into_tasks(
        pool.clone(),
        config.queue().to_string(),
        worker.name().to_owned(),
        config.buffer_size().max(1),
        ids,
    )
    .boxed();

    let eager_fetcher = PgPollFetcher::<CompactType>::new(&pool, &config, &worker);
    let combined = futures::stream::select(lazy_fetcher, eager_fetcher);
    register_then_stream(register_worker, combined)
}

/// Internal contract for the concrete fetcher modes (`PgFetcher`, `PgNotify`,
/// `SharedFetcher`). Lets a single generic `Backend`/`BackendExt` impl on
/// `PostgresStorage` cover every mode by delegating the pipeline construction
/// here, instead of repeating identical heartbeat/middleware/poll code three
/// times. Not part of the public API: downstream code keeps using
/// `PostgresStorage<Args, Codec, Fetcher>` exactly as before.
pub(crate) trait PgFetcherSource: Sized + Send + 'static {
    const STORAGE_NAME: &'static str;

    fn into_compact_stream(
        self,
        pool: PgPool,
        config: Config,
        worker: apalis_core::worker::context::WorkerContext,
        lease_token: Arc<str>,
    ) -> TaskStream<PgTask<CompactType>, Error>;
}

impl<Decode> PgFetcherSource for PgFetcher<CompactType, Decode>
where
    Decode: Send + 'static,
{
    const STORAGE_NAME: &'static str = crate::STORAGE_NAME;

    fn into_compact_stream(
        self,
        pool: PgPool,
        config: Config,
        worker: apalis_core::worker::context::WorkerContext,
        lease_token: Arc<str>,
    ) -> TaskStream<PgTask<CompactType>, Error> {
        let register_worker = queries::initial_heartbeat(
            pool.clone(),
            config.clone(),
            worker.clone(),
            Self::STORAGE_NAME,
            lease_token,
        )
        .map_ok(|_| None);
        let fetcher = PgPollFetcher::<CompactType>::new(&pool, &config, &worker);
        register_then_stream(register_worker, fetcher)
    }
}

type Poller = Pin<Box<dyn Stream<Item = ()> + Send>>;

/// The configured poll strategy, built exactly once per fetcher and then polled
/// for the fetcher's whole lifetime. `Fuse` keeps it safe to poll past
/// exhaustion: a finite custom strategy that yields `None` stays `None` on
/// subsequent polls instead of risking a panic.
type FusedPoller = futures::stream::Fuse<Poller>;

enum StreamState<Args> {
    /// Waiting for the persistent `poller` (see [`PgPollFetcher::poller`]) to
    /// signal that the next fetch should run.
    WaitForPoll,
    StrategyEnded(Delay),
    Fetch(BoxFuture<'static, Result<Vec<PgTask<Args>>, Error>>),
    Buffered(VecDeque<PgTask<Args>>),
}

/// Marker fetcher used by the default polling backend.
#[derive(Clone, Debug, Default)]
pub struct PgFetcher<Compact, Decode> {
    _marker: PhantomData<(Compact, Decode)>,
}

/// Polling stream that fetches and buffers queued tasks.
pub(crate) struct PgPollFetcher<Compact> {
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
    /// The configured poll strategy, built **once** at construction and polled
    /// for the fetcher's whole lifetime. Rebuilding it per cycle (the pre-fix
    /// behaviour) drained the shared `MultiStrategy` after the first build, so
    /// every later cycle fell back to a hardcoded delay. It reads
    /// `previous_task_count` live through the `PollContext` Arc, so backoff
    /// keeps adapting to fetch sizes without a rebuild.
    poller: FusedPoller,
    state: StreamState<Compact>,
    previous_task_count: Arc<AtomicUsize>,
}

impl<Compact> Clone for PgPollFetcher<Compact> {
    fn clone(&self) -> Self {
        let previous_task_count = Arc::new(AtomicUsize::new(0));
        Self {
            pool: self.pool.clone(),
            config: self.config.clone(),
            worker: self.worker.clone(),
            poller: build_poller(&self.config, &self.worker, previous_task_count.clone()),
            state: StreamState::WaitForPoll,
            previous_task_count,
        }
    }
}

impl PgPollFetcher<CompactType> {
    /// Create a polling fetcher.
    #[must_use]
    pub fn new(pool: &PgPool, config: &Config, worker: &WorkerContext) -> Self {
        let previous_task_count = Arc::new(AtomicUsize::new(0));
        Self {
            pool: pool.clone(),
            config: config.clone(),
            worker: worker.clone(),
            poller: build_poller(config, worker, previous_task_count.clone()),
            state: StreamState::WaitForPoll,
            previous_task_count,
        }
    }
}

/// Delay applied after the configured `PollStrategy` reports exhaustion, before
/// re-issuing a fetch. Hard-coded rather than configurable because the stream
/// already self-tunes via `previous_task_count`; the value just smooths a
/// single edge case (strategy returns `Ready(None)`).
const STRATEGY_EXHAUSTED_BACKOFF: Duration = Duration::from_millis(100);

impl PgPollFetcher<CompactType> {
    fn start_fetch(&self) -> StreamState<CompactType> {
        StreamState::Fetch(
            queries::fetch_next(self.pool.clone(), self.config.clone(), self.worker.clone())
                .boxed(),
        )
    }
}

impl<Compact> PgPollFetcher<Compact> {
    /// Drain buffered tasks that were already fetched but not yet yielded.
    /// Used by tests to verify the buffered state of the poll fetcher.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn take_pending(&mut self) -> VecDeque<PgTask<Compact>> {
        match &mut self.state {
            StreamState::Buffered(tasks) => std::mem::take(tasks),
            _ => VecDeque::new(),
        }
    }
}

impl Stream for PgPollFetcher<CompactType> {
    type Item = Result<Option<Task<CompactType, PgContext, ulid::Ulid>>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                // Poll the persistent strategy stream (built once in `new`),
                // not a freshly rebuilt one — rebuilding drained the shared
                // `MultiStrategy` after the first cycle.
                StreamState::WaitForPoll => match this.poller.poll_next_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(())) => {
                        this.state = this.start_fetch();
                    }
                    Poll::Ready(None) => {
                        this.state =
                            StreamState::StrategyEnded(Delay::new(STRATEGY_EXHAUSTED_BACKOFF));
                    }
                },
                StreamState::StrategyEnded(delay) => match Pin::new(delay).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => {
                        this.state = this.start_fetch();
                    }
                },
                StreamState::Fetch(fetch) => match fetch.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(tasks)) if tasks.is_empty() => {
                        this.previous_task_count.store(0, Ordering::Relaxed);
                        this.state = StreamState::WaitForPoll;
                    }
                    Poll::Ready(Ok(tasks)) => {
                        this.previous_task_count
                            .store(tasks.len(), Ordering::Relaxed);
                        this.state = StreamState::Buffered(VecDeque::from(tasks));
                    }
                    Poll::Ready(Err(error)) => {
                        this.previous_task_count.store(0, Ordering::Relaxed);
                        this.state = StreamState::WaitForPoll;
                        return Poll::Ready(Some(Err(error)));
                    }
                },
                StreamState::Buffered(buffer) => {
                    if let Some(task) = buffer.pop_front() {
                        if buffer.is_empty() {
                            // Buffer drained: always return to the configured
                            // poll strategy for the next fetch. Fetching again
                            // on our own the moment a limit-filling batch drains
                            // would drain a backlog faster, but the fetcher only
                            // holds an opaque `MultiStrategy` and cannot tell the
                            // default backoff apart from a user-supplied hard
                            // gate (a rate limiter, or a readiness
                            // `StreamStrategy`/`FutureStrategy`). A self-issued
                            // re-fetch would let a single strategy permit claim
                            // several batches and silently bypass those limits,
                            // so honour the strategy and wait for its next
                            // signal. The default `BackoffStrategy` re-reads the
                            // batch count after each delay and resets to its base
                            // interval the cycle after work reappears; that
                            // one-cycle lag is a property of the strategy, not
                            // something the fetcher can short-circuit without
                            // breaking custom gates.
                            this.state = StreamState::WaitForPoll;
                        }
                        return Poll::Ready(Some(Ok(Some(task))));
                    }
                    this.state = StreamState::WaitForPoll;
                }
            }
        }
    }
}

fn build_poller(
    config: &Config,
    worker: &WorkerContext,
    previous_task_count: Arc<AtomicUsize>,
) -> FusedPoller {
    let context = PollContext::new(worker.clone(), previous_task_count);
    // `build_stream` consumes the `MultiStrategy` — its `poll_strategy` drains
    // the shared `Arc<Mutex<Vec<_>>>` — so this must run exactly once per
    // fetcher. The resulting stream reads `previous_task_count` live through the
    // `PollContext` Arc, so backoff keeps adapting without a rebuild.
    config.poll_strategy().clone().build_stream(&context).fuse()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::Duration,
    };

    use apalis_core::{task::builder::TaskBuilder, worker::context::WorkerContext};
    use diesel::{
        PgConnection,
        r2d2::{ConnectionManager, Pool},
    };
    use futures::{FutureExt, StreamExt, future, stream, task::noop_waker_ref};
    use lets_expect::{AssertionError, AssertionResult, *};

    use super::*;

    struct PollObservation {
        poll: &'static str,
        state: &'static str,
        previous_task_count: usize,
    }

    fn unchecked_pool() -> PgPool {
        let manager = ConnectionManager::<PgConnection>::new("postgres://127.0.0.1:1/not-used");
        Pool::builder()
            .max_size(1)
            .connection_timeout(Duration::from_millis(10))
            .build_unchecked(manager)
    }

    /// A poll-strategy stream that never yields, used as a placeholder poller
    /// for fetchers whose tests drive the state machine directly rather than
    /// through the configured strategy.
    fn pending_poller() -> FusedPoller {
        let poller: Poller = Box::pin(stream::pending::<()>());
        poller.fuse()
    }

    fn buffered_fetcher() -> PgPollFetcher<CompactType> {
        PgPollFetcher {
            pool: unchecked_pool(),
            config: Config::new("fetcher-test"),
            worker: WorkerContext::new::<()>("fetcher-worker"),
            poller: pending_poller(),
            state: StreamState::Buffered(VecDeque::new()),
            previous_task_count: Arc::new(AtomicUsize::new(12)),
        }
    }

    fn state_name(fetcher: &PgPollFetcher<CompactType>) -> &'static str {
        match &fetcher.state {
            StreamState::WaitForPoll => "wait_for_poll",
            StreamState::StrategyEnded(_) => "strategy_ended",
            StreamState::Fetch(_) => "fetch",
            StreamState::Buffered(_) => "buffered",
        }
    }

    fn poll_observation(fetcher: &mut PgPollFetcher<CompactType>) -> PollObservation {
        let mut cx = Context::from_waker(noop_waker_ref());
        let poll = match Pin::new(&mut *fetcher).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(Some(_)))) => "task",
            Poll::Ready(Some(Ok(None))) => "empty",
            Poll::Ready(Some(Err(_))) => "error",
            Poll::Ready(None) => "closed",
            Poll::Pending => "pending",
        };
        PollObservation {
            poll,
            state: state_name(fetcher),
            previous_task_count: fetcher.previous_task_count.load(Ordering::Relaxed),
        }
    }

    fn pending_poll_strategy_observation() -> PollObservation {
        let mut fetcher = buffered_fetcher();
        let poller: Poller = Box::pin(stream::pending());
        fetcher.poller = poller.fuse();
        fetcher.state = StreamState::WaitForPoll;
        poll_observation(&mut fetcher)
    }

    fn exhausted_poll_strategy_observation() -> PollObservation {
        // A poller that yields `Ready(None)` must transition the fetcher into
        // `StrategyEnded` — the only way out of WaitForPoll besides starting a
        // fetch.
        let mut fetcher = buffered_fetcher();
        let poller: Poller = Box::pin(stream::empty::<()>());
        fetcher.poller = poller.fuse();
        fetcher.state = StreamState::WaitForPoll;
        poll_observation(&mut fetcher)
    }

    fn observed_strategy_exhaustion(result: &PollObservation) -> AssertionResult {
        match (result.poll, result.state) {
            // After the strategy ends, the fetcher enters StrategyEnded and
            // its Delay (100 ms, fetcher.rs:108) has not yet elapsed in this
            // synchronous test — so the outer poll returns Pending.
            ("pending", "strategy_ended") => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected exhausted strategy to transition into strategy_ended/pending, got {other:?}"
            )])),
        }
    }

    fn fetch_error_observation() -> PollObservation {
        let mut fetcher = buffered_fetcher();
        fetcher.state = StreamState::Fetch(future::ready(Err(Error::SinkBufferFull(1))).boxed());
        poll_observation(&mut fetcher)
    }

    fn empty_fetch_observation() -> PollObservation {
        let mut fetcher = buffered_fetcher();
        fetcher.state = StreamState::Fetch(future::ready(Ok(Vec::new())).boxed());
        poll_observation(&mut fetcher)
    }

    fn successful_fetch_observation() -> PollObservation {
        let mut fetcher = buffered_fetcher();
        let task = TaskBuilder::new(vec![1, 2, 3])
            .with_ctx(PgContext::new())
            .build();
        fetcher.state = StreamState::Fetch(future::ready(Ok(vec![task])).boxed());
        poll_observation(&mut fetcher)
    }

    fn fetch_pending_observation() -> PollObservation {
        let mut fetcher = buffered_fetcher();
        fetcher.state = StreamState::Fetch(future::pending().boxed());
        poll_observation(&mut fetcher)
    }

    fn cloned_state(fetcher: &PgPollFetcher<CompactType>) -> &'static str {
        match &fetcher.clone().state {
            StreamState::WaitForPoll => "wait_for_poll",
            StreamState::StrategyEnded(_) => "strategy_ended",
            StreamState::Fetch(_) => "fetch",
            StreamState::Buffered(_) => "buffered",
        }
    }

    fn cloned_previous_task_count(fetcher: &PgPollFetcher<CompactType>) -> usize {
        fetcher.clone().previous_task_count.load(Ordering::Relaxed)
    }

    fn observed_fetch_error(result: &PollObservation) -> AssertionResult {
        match (result.poll, result.state, result.previous_task_count) {
            ("error", "wait_for_poll", 0) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected fetch error to reset the poll strategy, got {other:?}"
            )])),
        }
    }

    fn observed_empty_fetch(result: &PollObservation) -> AssertionResult {
        match (result.poll, result.state, result.previous_task_count) {
            ("pending", "wait_for_poll", 0) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected empty fetch to wait for configured polling, got {other:?}"
            )])),
        }
    }

    fn observed_successful_fetch(result: &PollObservation) -> AssertionResult {
        match (result.poll, result.state, result.previous_task_count) {
            ("task", "wait_for_poll", 1) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected successful fetch to yield one task and remember the count, got {other:?}"
            )])),
        }
    }

    fn observed_pending_fetch(result: &PollObservation) -> AssertionResult {
        // The in-flight fetch future is still Pending (fetcher.rs:349): the
        // poll returns Pending without mutating the state slot or the
        // previously remembered batch count (12 from `buffered_fetcher`).
        match (result.poll, result.state, result.previous_task_count) {
            ("pending", "fetch", 12) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected an in-flight fetch to wait without touching the batch count, got {other:?}"
            )])),
        }
    }

    fn observed_pending_strategy(result: &PollObservation) -> AssertionResult {
        match (result.poll, result.state, result.previous_task_count) {
            ("pending", "wait_for_poll", 12) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected pending strategy to prevent a database fetch, got {other:?}"
            )])),
        }
    }

    fn buffered_with(tasks: Vec<PgTask<CompactType>>) -> PgPollFetcher<CompactType> {
        let mut fetcher = buffered_fetcher();
        fetcher.state = StreamState::Buffered(VecDeque::from(tasks));
        fetcher
    }

    fn synthetic_task(payload: &[u8]) -> PgTask<CompactType> {
        TaskBuilder::new(payload.to_vec())
            .with_ctx(PgContext::new())
            .build()
    }

    fn take_pending_count(state_kind: &'static str) -> usize {
        let mut fetcher = match state_kind {
            "buffered_two" => buffered_with(vec![synthetic_task(b"one"), synthetic_task(b"two")]),
            "buffered_empty" => buffered_with(Vec::new()),
            "wait_for_poll" => {
                let mut fetcher = buffered_fetcher();
                let poller: Poller = Box::pin(stream::pending());
                fetcher.poller = poller.fuse();
                fetcher.state = StreamState::WaitForPoll;
                fetcher
            }
            "fetch" => {
                let mut fetcher = buffered_fetcher();
                fetcher.state = StreamState::Fetch(future::ready(Ok(Vec::new())).boxed());
                fetcher
            }
            "strategy_ended" => {
                let mut fetcher = buffered_fetcher();
                fetcher.state = StreamState::StrategyEnded(Delay::new(Duration::from_secs(60)));
                fetcher
            }
            other => panic!("unknown state kind: {other}"),
        };
        fetcher.take_pending().len()
    }

    /// After `take_pending` drains the buffer, the fetcher should still be in
    /// the same Buffered state slot (we only stole the inner VecDeque). The
    /// follow-up observation confirms the buffer is now empty and the next
    /// `poll_next` would transition to WaitForPoll.
    fn take_pending_drains_then_reports_empty() -> (usize, usize, &'static str) {
        let mut fetcher = buffered_with(vec![synthetic_task(b"alpha"), synthetic_task(b"beta")]);
        let drained = fetcher.take_pending().len();
        let remaining = match &fetcher.state {
            StreamState::Buffered(tasks) => tasks.len(),
            _ => panic!("take_pending changed the state slot"),
        };
        (drained, remaining, state_name(&fetcher))
    }

    fn buffered_pop_front_observation() -> PollObservation {
        let mut fetcher = buffered_with(vec![synthetic_task(b"first"), synthetic_task(b"second")]);
        poll_observation(&mut fetcher)
    }

    fn observed_buffered_pop_front(result: &PollObservation) -> AssertionResult {
        // `buffered_fetcher` is constructed with `previous_task_count=12`; a
        // pop from the buffered state should NOT touch that counter (only a
        // fresh fetch_next outcome updates it). Yields the task while the
        // buffer still holds a sibling task.
        match (result.poll, result.state, result.previous_task_count) {
            ("task", "buffered", 12) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected pop_front to yield a task while remaining buffered, got {other:?}"
            )])),
        }
    }

    /// Drain a single-element Buffered state and report the state the fetcher
    /// transitions into. Whether or not the prior batch filled the fetch limit
    /// (`previous_task_count` vs the default `buffer_size` of 10), draining
    /// always returns to the configured poll strategy (`wait_for_poll`): the
    /// fetcher holds an opaque `MultiStrategy` and must not issue a fetch on its
    /// own, or a user-supplied rate limiter / readiness gate would be bypassed
    /// by a single permit claiming multiple batches.
    fn buffered_drain_observation(previous_task_count: usize) -> &'static str {
        let mut fetcher = buffered_with(vec![synthetic_task(b"only")]);
        fetcher
            .previous_task_count
            .store(previous_task_count, Ordering::Relaxed);
        let mut cx = Context::from_waker(noop_waker_ref());
        let _ = Pin::new(&mut fetcher).poll_next(&mut cx);
        state_name(&fetcher)
    }

    /// A full batch must not let the fetcher bypass its poll strategy. The
    /// drain-aggressive shortcut jumped straight to a fresh `Fetch` when a
    /// limit-filling batch drained, so one strategy permit could claim several
    /// batches — silently bypassing a user-supplied rate limiter or readiness
    /// gate. Model an already-spent gate with a strategy that never yields
    /// again: after draining a full batch (`previous_task_count == buffer_size`)
    /// the fetcher must return to `wait_for_poll` and, on the next poll, stay
    /// Pending (no fetch) until the strategy grants another permit. Reports
    /// `(state after draining, poll after that)`; the pre-fix shortcut parked in
    /// `fetch` instead.
    fn full_batch_respects_a_spent_poll_strategy() -> (&'static str, &'static str) {
        let mut fetcher = buffered_with(vec![synthetic_task(b"only")]);
        // Spent gate: no further permits will ever be granted.
        fetcher.poller = pending_poller();
        // The just-drained batch filled the fetch limit.
        let full = fetcher.config.buffer_size().max(1);
        fetcher.previous_task_count.store(full, Ordering::Relaxed);
        let mut cx = Context::from_waker(noop_waker_ref());
        // First poll drains the one buffered task and parks the fetcher.
        let _ = Pin::new(&mut fetcher).poll_next(&mut cx);
        let parked = state_name(&fetcher);
        // Second poll consults the strategy; its permit is already spent, so a
        // correct fetcher waits instead of issuing another fetch.
        let follow_up = match Pin::new(&mut fetcher).poll_next(&mut cx) {
            Poll::Pending => "pending",
            Poll::Ready(Some(Ok(Some(_)))) => "task",
            Poll::Ready(_) => "other",
        };
        (parked, follow_up)
    }

    /// Build a fetcher through its public constructor (so it owns whatever poll
    /// strategy `Config` carries), run it through one completed fetch cycle, and
    /// report the state it parks in. The configured `MultiStrategy` must keep
    /// governing the fetcher: the pre-fix code rebuilt the strategy every cycle
    /// via `config.poll_strategy().clone().build_stream(...)`, and because
    /// `MultiStrategy::poll_strategy` *drains* its shared `Arc<Mutex<Vec<_>>>`,
    /// the second build saw an empty strategy — collapsing to the hardcoded
    /// 100ms `StrategyEnded` fallback (losing the configured interval/backoff)
    /// after a single cycle. Parking back in `wait_for_poll` proves the
    /// configured strategy still drives polling.
    fn state_after_one_empty_fetch_cycle() -> &'static str {
        let pool = unchecked_pool();
        let config = Config::new("poll-strategy-drain");
        let worker = WorkerContext::new::<()>("poll-strategy-drain-worker");
        let mut fetcher = PgPollFetcher::new(&pool, &config, &worker);
        // Inject a completed, empty fetch so the fetcher schedules its next
        // poll through the configured strategy.
        fetcher.state = StreamState::Fetch(future::ready(Ok(Vec::new())).boxed());
        let mut cx = Context::from_waker(noop_waker_ref());
        let _ = Pin::new(&mut fetcher).poll_next(&mut cx);
        state_name(&fetcher)
    }

    lets_expect! {
        expect(cloned_state(&fetcher)) {
            let fetcher = buffered_fetcher();

            when original_stream_has_buffered_state {
                to resets_the_clone_to_poll_strategy { equal("wait_for_poll") }
            }
        }

        expect(cloned_previous_task_count(&fetcher)) {
            let fetcher = buffered_fetcher();

            when original_stream_remembers_a_previous_batch {
                to starts_the_clone_with_no_previous_count { equal(0) }
            }
        }

        expect(pending_poll_strategy_observation()) {
            when the_configured_poll_strategy_is_not_ready {
                to does_not_start_a_fetch { observed_pending_strategy }
            }
        }

        expect(exhausted_poll_strategy_observation()) {
            when the_configured_poll_strategy_returns_ready_none {
                to transitions_into_strategy_ended_and_waits_for_the_delay {
                    observed_strategy_exhaustion
                }
            }
        }

        expect(fetch_error_observation()) {
            when fetch_query_fails {
                to yields_the_error_and_waits_for_the_next_poll_signal { observed_fetch_error }
            }
        }

        expect(empty_fetch_observation()) {
            when fetch_returns_no_tasks {
                to waits_for_the_next_configured_poll_signal { observed_empty_fetch }
            }
        }

        expect(successful_fetch_observation()) {
            when fetch_returns_tasks {
                to yields_a_task_and_records_the_batch_size { observed_successful_fetch }
            }
        }

        expect(fetch_pending_observation()) {
            when fetch_query_is_still_in_flight {
                to waits_without_touching_the_batch_count { observed_pending_fetch }
            }
        }

        expect(take_pending_count(state_kind)) {
            let state_kind = "buffered_two";

            when fetcher_is_in_buffered_state_with_two_tasks {
                to drains_every_buffered_task { equal(2) }
            }

            when fetcher_is_in_buffered_state_with_no_tasks {
                let state_kind = "buffered_empty";
                to returns_an_empty_drained_queue { equal(0) }
            }

            when fetcher_is_in_wait_for_poll_state {
                let state_kind = "wait_for_poll";
                to ignores_states_other_than_buffered { equal(0) }
            }

            when fetcher_is_in_fetch_state {
                let state_kind = "fetch";
                to ignores_states_other_than_buffered { equal(0) }
            }

            when fetcher_is_in_strategy_ended_state {
                let state_kind = "strategy_ended";
                to ignores_states_other_than_buffered { equal(0) }
            }
        }

        expect(take_pending_drains_then_reports_empty()) {
            when buffered_state_is_drained_via_take_pending {
                to leaves_the_fetcher_in_the_buffered_state_with_zero_tasks {
                    equal((2, 0, "buffered"))
                }
            }
        }

        expect(buffered_pop_front_observation()) {
            when buffer_holds_multiple_tasks {
                to pops_a_task_and_stays_in_buffered { observed_buffered_pop_front }
            }
        }

        expect(buffered_drain_observation(previous_task_count)) {
            let previous_task_count = 10; // == default buffer_size: the batch filled the limit

            when the_drained_batch_had_filled_the_fetch_limit {
                to returns_to_the_configured_poll_strategy { equal("wait_for_poll") }
            }

            when the_drained_batch_was_shorter_than_the_fetch_limit {
                let previous_task_count = 3;
                to also_returns_to_the_configured_poll_strategy { equal("wait_for_poll") }
            }
        }

        expect(full_batch_respects_a_spent_poll_strategy()) {
            when a_limit_filling_batch_is_drained_and_the_strategy_has_no_further_permit {
                to returns_to_the_strategy_and_stays_pending_without_fetching {
                    equal(("wait_for_poll", "pending"))
                }
            }
        }

        expect(state_after_one_empty_fetch_cycle()) {
            when a_fetch_cycle_completes_and_the_next_poll_is_scheduled {
                to keeps_being_governed_by_the_configured_poll_strategy {
                    equal("wait_for_poll")
                }
            }
        }
    }
}
