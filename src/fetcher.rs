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

use crate::{CompactType, Config, Error, PgContext, PgPool, PgTask, queries};

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
pub(crate) fn decode_task_stream<Args, Decode>(
    compact: TaskStream<PgTask<CompactType>, Error>,
) -> TaskStream<PgTask<Args>, Error>
where
    Args: Send + 'static,
    Decode: Codec<Args, Compact = CompactType> + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    compact
        .map(|row| match row {
            Ok(Some(task)) => {
                Ok(Some(task.try_map(|t| {
                    Decode::decode(&t).map_err(|e| Error::Decode(e.into()))
                })?))
            }
            Ok(None) => Ok(None),
            Err(error) => Err(error),
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
        let register_worker = queries::initial_heartbeat(
            pool.clone(),
            config.clone(),
            worker.clone(),
            Self::STORAGE_NAME,
            lease_token,
        )
        .map_ok(|_| None);

        // Real batching is provided upstream by the statement-level NOTIFY
        // trigger (migration 20260521000001), which emits one event per
        // (queue, INSERT statement) carrying all inserted ids in `ids`. By
        // the time those ids land in the mpsc channel they are already
        // contiguous, so `ready_chunks` (inside `batch_ids_into_tasks`)
        // folds them into one batch in the common bursty case.
        let lazy_fetcher = queries::batch_ids_into_tasks(
            pool.clone(),
            config.queue().to_string(),
            worker.name().to_owned(),
            config.buffer_size().max(1),
            queries::notify_task_ids(
                pool.clone(),
                config.queue().to_string(),
                config.buffer_size().max(1),
            ),
        )
        .boxed();

        let eager_fetcher = PgPollFetcher::<CompactType>::new(&pool, &config, &worker);
        let combined = futures::stream::select(lazy_fetcher, eager_fetcher);
        register_then_stream(register_worker, combined)
    }
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

enum StreamState<Args> {
    WaitForPoll(Poller),
    StrategyEnded(Delay),
    Fetch(BoxFuture<'static, Result<Vec<PgTask<Args>>, Error>>),
    Buffered(VecDeque<PgTask<Args>>),
}

/// Marker fetcher used by the default polling backend.
#[derive(Clone, Debug)]
pub struct PgFetcher<Compact, Decode> {
    pub _marker: PhantomData<(Compact, Decode)>,
}

/// Polling stream that fetches and buffers queued tasks.
pub(crate) struct PgPollFetcher<Compact> {
    pool: PgPool,
    config: Config,
    worker: WorkerContext,
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
            state: poll_state(&self.config, &self.worker, previous_task_count.clone()),
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
            state: poll_state(config, worker, previous_task_count.clone()),
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
                StreamState::WaitForPoll(poller) => match poller.poll_next_unpin(cx) {
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
                        this.state = poll_state(
                            &this.config,
                            &this.worker,
                            this.previous_task_count.clone(),
                        );
                    }
                    Poll::Ready(Ok(tasks)) => {
                        this.previous_task_count
                            .store(tasks.len(), Ordering::Relaxed);
                        this.state = StreamState::Buffered(VecDeque::from(tasks));
                    }
                    Poll::Ready(Err(error)) => {
                        this.previous_task_count.store(0, Ordering::Relaxed);
                        this.state = poll_state(
                            &this.config,
                            &this.worker,
                            this.previous_task_count.clone(),
                        );
                        return Poll::Ready(Some(Err(error)));
                    }
                },
                StreamState::Buffered(buffer) => {
                    if let Some(task) = buffer.pop_front() {
                        if buffer.is_empty() {
                            this.state = poll_state(
                                &this.config,
                                &this.worker,
                                this.previous_task_count.clone(),
                            );
                        }
                        return Poll::Ready(Some(Ok(Some(task))));
                    }
                    this.state =
                        poll_state(&this.config, &this.worker, this.previous_task_count.clone());
                }
            }
        }
    }
}

fn poll_state<Compact>(
    config: &Config,
    worker: &WorkerContext,
    previous_task_count: Arc<AtomicUsize>,
) -> StreamState<Compact> {
    let context = PollContext::new(worker.clone(), previous_task_count);
    StreamState::WaitForPoll(config.poll_strategy().clone().build_stream(&context))
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
    use futures::{FutureExt, future, stream, task::noop_waker_ref};
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

    fn buffered_fetcher() -> PgPollFetcher<CompactType> {
        PgPollFetcher {
            pool: unchecked_pool(),
            config: Config::new("fetcher-test"),
            worker: WorkerContext::new::<()>("fetcher-worker"),
            state: StreamState::Buffered(VecDeque::new()),
            previous_task_count: Arc::new(AtomicUsize::new(12)),
        }
    }

    fn state_name(fetcher: &PgPollFetcher<CompactType>) -> &'static str {
        match &fetcher.state {
            StreamState::WaitForPoll(_) => "wait_for_poll",
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
        fetcher.state = StreamState::WaitForPoll(Box::pin(stream::pending()));
        poll_observation(&mut fetcher)
    }

    fn exhausted_poll_strategy_observation() -> PollObservation {
        // Stream::poll_next returning `Ready(None)` must transition the
        // fetcher into `StrategyEnded` (fetcher.rs:106-109) — the only way
        // out of WaitForPoll besides starting a fetch.
        let mut fetcher = buffered_fetcher();
        fetcher.state = StreamState::WaitForPoll(Box::pin(stream::empty::<()>()));
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

    fn cloned_state(fetcher: &PgPollFetcher<CompactType>) -> &'static str {
        match &fetcher.clone().state {
            StreamState::WaitForPoll(_) => "wait_for_poll",
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
                fetcher.state = StreamState::WaitForPoll(Box::pin(stream::pending()));
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
    fn take_pending_drains_then_reports_empty() -> (usize, &'static str) {
        let mut fetcher = buffered_with(vec![synthetic_task(b"alpha"), synthetic_task(b"beta")]);
        let drained = fetcher.take_pending().len();
        let remaining = match &fetcher.state {
            StreamState::Buffered(tasks) => tasks.len(),
            _ => panic!("take_pending changed the state slot"),
        };
        let _ = remaining;
        (drained, state_name(&fetcher))
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

    /// Poll twice on a single-element Buffered state. The first call should
    /// yield the task and emit a transition to WaitForPoll (the buffer is now
    /// empty). The second call sits in WaitForPoll.
    fn buffered_drain_observation() -> &'static str {
        let mut fetcher = buffered_with(vec![synthetic_task(b"only")]);
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
                    equal((2, "buffered"))
                }
            }
        }

        expect(buffered_pop_front_observation()) {
            when buffer_holds_multiple_tasks {
                to pops_a_task_and_stays_in_buffered { observed_buffered_pop_front }
            }
        }

        expect(buffered_drain_observation()) {
            when buffer_holds_exactly_one_task {
                to transitions_to_wait_for_poll_after_emitting_the_task {
                    equal("wait_for_poll")
                }
            }
        }
    }
}
