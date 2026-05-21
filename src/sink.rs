use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll},
};

use apalis_codec::json::JsonCodec;
use futures::{FutureExt, Sink};

use crate::{CompactType, Config, Error, PgPool, PgTask, PostgresStorage, queries};

// Wrapped in `Mutex` upstream so `PgSink: Sync` even when the inner future
// isn't (ntex's `BlockingResult` is `Send`-only). `Mutex::get_mut` keeps the
// hot path lock-free.
type FlushFuture = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>;

/// Buffered task sink used by [`PostgresStorage`].
pub struct PgSink<Args, Codec = JsonCodec<CompactType>> {
    pool: PgPool,
    config: Config,
    buffer: Vec<PgTask<CompactType>>,
    flush_future: Mutex<Option<FlushFuture>>,
    _marker: PhantomData<(Args, Codec)>,
}

impl<Args, Codec> std::fmt::Debug for PgSink<Args, Codec> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgSink")
            .field("config", &self.config)
            .field("buffer_len", &self.buffer.len())
            .finish_non_exhaustive()
    }
}

impl<Args, Codec> Clone for PgSink<Args, Codec> {
    /// Returns a fresh sink sharing the same pool/config; the buffer and any
    /// in-flight flush are intentionally **not** cloned. Each `PgSink` owns its
    /// pipeline state: cloning a sink that holds buffered tasks would either
    /// silently duplicate (double-insert) or silently drop them on flush. The
    /// clone starts empty, so callers responsible for pending work should
    /// flush before cloning.
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            config: self.config.clone(),
            buffer: Vec::new(),
            flush_future: Mutex::new(None),
            _marker: PhantomData,
        }
    }
}

impl<Args, Codec> PgSink<Args, Codec> {
    /// Create a sink for the given pool and config.
    #[must_use]
    pub fn new(pool: &PgPool, config: &Config) -> Self {
        Self {
            pool: pool.clone(),
            config: config.clone(),
            buffer: Vec::new(),
            flush_future: Mutex::new(None),
            _marker: PhantomData,
        }
    }
}

impl<Args, Codec> PgSink<Args, Codec> {
    /// Buffer capacity from the underlying config (clamped to ≥1 so a
    /// misconfigured `buffer_size(0)` does not deadlock the sink).
    fn capacity(&self) -> usize {
        self.config.buffer_size().max(1)
    }

    /// Whether `poll_ready` must drive a flush before accepting more work —
    /// either a flush is already in flight, or the buffer is at capacity.
    fn needs_flush_before_ready(&mut self) -> bool {
        self.flush_future.get_mut().expect("flush_future mutex poisoned").is_some()
            || self.buffer.len() >= self.capacity()
    }

    /// Try to enqueue a single task into the buffer, returning
    /// `Error::SinkBufferFull` when capacity has been reached.
    fn try_push(&mut self, item: PgTask<CompactType>) -> Result<(), Error> {
        let cap = self.capacity();
        if self.buffer.len() >= cap {
            return Err(Error::SinkBufferFull(cap));
        }
        self.buffer.push(item);
        Ok(())
    }

    /// Drive the buffered batch toward completion. Starts a new flush future
    /// when none is in flight and the buffer is non-empty; otherwise polls the
    /// existing future and clears it once it resolves.
    fn poll_flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        // `&mut self` makes `Mutex::get_mut` infallible-by-borrow — no lock
        // acquisition, just unique-borrow projection. The mutex exists purely
        // to satisfy `PgSink: Sync` when the inner future is not `Sync` (ntex).
        let flush_future = self
            .flush_future
            .get_mut()
            .expect("flush_future mutex poisoned");

        if flush_future.is_none() && self.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        if flush_future.is_none() {
            let pool = self.pool.clone();
            let config = self.config.clone();
            let buffer = std::mem::take(&mut self.buffer);
            *flush_future = Some(Box::pin(queries::push_tasks(pool, config, buffer)));
        }

        let Some(future) = flush_future.as_mut() else {
            return Poll::Ready(Ok(()));
        };

        match future.poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                *flush_future = None;
                Poll::Ready(result)
            }
        }
    }
}

impl<Args, Encode, Fetcher> Sink<PgTask<CompactType>> for PostgresStorage<Args, Encode, Fetcher>
where
    Args: Send + Sync + 'static,
    Fetcher: Unpin,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        if this.sink.needs_flush_before_ready() {
            this.sink.poll_flush_inner(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(self: Pin<&mut Self>, item: PgTask<CompactType>) -> Result<(), Self::Error> {
        self.get_mut().sink.try_push(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.get_mut().sink.poll_flush_inner(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use diesel::{
        PgConnection,
        r2d2::{ConnectionManager, Pool},
    };
    use futures::{Sink, future, task::noop_waker_ref};
    use lets_expect::{AssertionError, AssertionResult, *};

    use super::*;

    fn unchecked_pool() -> PgPool {
        let manager = ConnectionManager::<PgConnection>::new("postgres://127.0.0.1:1/not-used");
        Pool::builder()
            .max_size(1)
            .connection_timeout(std::time::Duration::from_millis(10))
            .build_unchecked(manager)
    }

    fn task() -> PgTask<CompactType> {
        PgTask::new(b"payload".to_vec())
    }

    fn sink(buffer_size: usize) -> PgSink<Vec<u8>> {
        PgSink::new(
            &unchecked_pool(),
            &Config::new("sink-unit").set_buffer_size(buffer_size),
        )
    }

    fn storage(buffer_size: usize) -> PostgresStorage<Vec<u8>> {
        let pool = unchecked_pool();
        let config = Config::new("sink-unit").set_buffer_size(buffer_size);
        PostgresStorage::<Vec<u8>>::new_with_config(&pool, &config)
    }

    /// `start_send_via_storage` exercises the public `Sink` impl. The returned
    /// `len` is the buffer length after the final send (only set on success).
    fn start_send_via_storage(buffer_size: usize, existing_items: usize) -> Result<usize, Error> {
        let mut storage = storage(buffer_size);
        for _ in 0..existing_items {
            storage.sink.buffer.push(task());
        }
        Pin::new(&mut storage).start_send(task())?;
        Ok(storage.sink.buffer.len())
    }

    fn poll_ready_via_storage(
        buffer_size: usize,
        existing_items: usize,
    ) -> Poll<Result<(), Error>> {
        let mut storage = storage(buffer_size);
        for _ in 0..existing_items {
            storage.sink.buffer.push(task());
        }
        let mut cx = Context::from_waker(noop_waker_ref());
        Pin::new(&mut storage).poll_ready(&mut cx)
    }

    struct ReadyObservation {
        poll: Poll<Result<(), Error>>,
        buffer_len: usize,
        has_flush_future: bool,
    }

    fn poll_ready_in_flight() -> ReadyObservation {
        let mut storage = storage(2);
        storage.sink.flush_future =
            Mutex::new(Some(Box::pin(future::pending::<Result<(), Error>>())));
        let mut cx = Context::from_waker(noop_waker_ref());
        let poll = Pin::new(&mut storage).poll_ready(&mut cx);
        let has_flush_future = storage
            .sink
            .flush_future
            .get_mut()
            .expect("flush_future mutex poisoned")
            .is_some();
        ReadyObservation {
            poll,
            buffer_len: storage.sink.buffer.len(),
            has_flush_future,
        }
    }

    /// `poll_flush_observation` captures the state of `poll_flush_sink` after a
    /// single poll: the poll result and whether the in-flight future was cleared.
    struct FlushObservation {
        poll: Poll<Result<(), Error>>,
        future_cleared: bool,
        buffer_len: usize,
    }

    fn poll_flush_sink_with_state(
        buffer_size: usize,
        buffered: usize,
        future: Option<FlushFuture>,
    ) -> FlushObservation {
        let mut sink = sink(buffer_size);
        for _ in 0..buffered {
            sink.buffer.push(task());
        }
        sink.flush_future = Mutex::new(future);
        let mut cx = Context::from_waker(noop_waker_ref());
        let poll = sink.poll_flush_inner(&mut cx);
        let future_cleared = sink
            .flush_future
            .get_mut()
            .expect("flush_future mutex poisoned")
            .is_none();
        FlushObservation {
            poll,
            future_cleared,
            buffer_len: sink.buffer.len(),
        }
    }

    fn poll_flush_idle() -> FlushObservation {
        poll_flush_sink_with_state(1, 0, None)
    }

    fn poll_flush_in_flight_ready(result: Result<(), Error>) -> FlushObservation {
        poll_flush_sink_with_state(1, 0, Some(Box::pin(future::ready(result))))
    }

    fn poll_flush_in_flight_pending() -> FlushObservation {
        poll_flush_sink_with_state(1, 0, Some(Box::pin(future::pending())))
    }

    /// `poll_flush_creates_future` exercises the `flush_future.is_none() &&
    /// !buffer.is_empty()` branch: the function builds a new flush future from
    /// the buffer and immediately polls it. Against an unreachable pool the
    /// inner `push_tasks` future resolves to Err on first poll, so this returns
    /// a `Ready(Err(...))` observation with the buffer drained.
    fn poll_flush_creates_future() -> FlushObservation {
        poll_flush_sink_with_state(2, 1, None)
    }

    fn poll_close_via_storage(buffered: usize) -> Poll<Result<(), Error>> {
        let mut storage = storage(2);
        for _ in 0..buffered {
            storage.sink.buffer.push(task());
        }
        let mut cx = Context::from_waker(noop_waker_ref());
        Pin::new(&mut storage).poll_close(&mut cx)
    }

    fn cloned_sink_buffer_len(buffered_items: usize) -> usize {
        let mut sink = sink(3);
        for _ in 0..buffered_items {
            sink.buffer.push(task());
        }
        sink.clone().buffer.len()
    }

    fn cloned_sink_state_drops_flush_future() -> bool {
        let mut sink = sink(3);
        sink.buffer.push(task());
        sink.flush_future =
            Mutex::new(Some(Box::pin(future::pending::<Result<(), Error>>())));
        sink.clone()
            .flush_future
            .get_mut()
            .expect("flush_future mutex poisoned")
            .is_none()
    }

    fn cloned_sink_buffer_size(buffer_size: usize) -> usize {
        sink(buffer_size).clone().config.buffer_size()
    }

    fn sink_debug(buffered_items: usize) -> String {
        let mut sink = sink(3);
        for _ in 0..buffered_items {
            sink.buffer.push(task());
        }
        format!("{sink:?}")
    }

    fn sink_buffer_full(result: &Result<usize, Error>) -> AssertionResult {
        match result {
            Err(Error::SinkBufferFull(1)) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected sink buffer full at capacity 1, got {other:?}"
            )])),
        }
    }

    fn poll_ready_ok(result: &Poll<Result<(), Error>>) -> AssertionResult {
        match result {
            Poll::Ready(Ok(())) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected ready ok, got {other:?}"
            )])),
        }
    }

    fn poll_started_flush(result: &Poll<Result<(), Error>>) -> AssertionResult {
        match result {
            Poll::Pending | Poll::Ready(Err(_)) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected backpressure to start flushing, got {other:?}"
            )])),
        }
    }

    fn observation_is_idle_ok(obs: &FlushObservation) -> AssertionResult {
        match (&obs.poll, obs.future_cleared, obs.buffer_len) {
            (Poll::Ready(Ok(())), true, 0) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected idle Ready(Ok), got {other:?}"
            )])),
        }
    }

    fn observation_is_ready_ok_and_cleared(obs: &FlushObservation) -> AssertionResult {
        match (&obs.poll, obs.future_cleared) {
            (Poll::Ready(Ok(())), true) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected Ready(Ok) with cleared future, got {other:?}"
            )])),
        }
    }

    fn observation_is_ready_err_and_cleared(obs: &FlushObservation) -> AssertionResult {
        match (&obs.poll, obs.future_cleared) {
            (Poll::Ready(Err(_)), true) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected Ready(Err) with cleared future, got {other:?}"
            )])),
        }
    }

    fn observation_stays_pending(obs: &FlushObservation) -> AssertionResult {
        match (&obs.poll, obs.future_cleared) {
            (Poll::Pending, false) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected Pending with future retained, got {other:?}"
            )])),
        }
    }

    fn observation_drained_buffer_into_future(obs: &FlushObservation) -> AssertionResult {
        if obs.buffer_len != 0 {
            return Err(AssertionError::new(vec![format!(
                "expected buffer to be drained into the flush future, got {} items",
                obs.buffer_len
            )]));
        }
        // The flush future is created from the buffer. Either it is still
        // running (Pending + future retained) or it has resolved (Ready +
        // future cleared). Both observations confirm the drain happened; we
        // reject the inconsistent combinations explicitly so the test cannot
        // pass with stale state.
        match (&obs.poll, obs.future_cleared) {
            (Poll::Pending, false) => Ok(()),
            (Poll::Ready(_), true) => Ok(()),
            (Poll::Pending, true) => Err(AssertionError::new(vec![
                "flush returned Pending but the future was cleared".to_owned(),
            ])),
            (Poll::Ready(_), false) => Err(AssertionError::new(vec![
                "flush returned Ready but the future was retained".to_owned(),
            ])),
        }
    }

    fn keeps_in_flight_flush(observation: &ReadyObservation) -> AssertionResult {
        match (
            &observation.poll,
            observation.buffer_len,
            observation.has_flush_future,
        ) {
            (Poll::Pending, 0, true) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected pending in-flight flush, got {other:?}"
            )])),
        }
    }

    fn debug_mentions_public_fields(result: &String) -> AssertionResult {
        if result.contains("PgSink") && result.contains("config") && result.contains("buffer_len") {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected sink debug output with public fields, got {result}"
            )]))
        }
    }

    lets_expect! {
        expect(start_send_via_storage(buffer_size, existing_items)) {
            let buffer_size = 2;
            let existing_items = 0;

            when buffer_has_room_below_capacity {
                to buffers_the_task { be_ok_and equal(1) }
            }

            when buffer_is_at_capacity_already {
                let buffer_size = 1;
                let existing_items = 1;
                to rejects_the_send { sink_buffer_full }
            }

            when configured_capacity_is_zero_and_minimum_one_is_full {
                let buffer_size = 0;
                let existing_items = 1;
                to rejects_the_send_via_the_minimum_capacity { sink_buffer_full }
            }
        }

        expect(poll_ready_via_storage(buffer_size, existing_items)) {
            let buffer_size = 2;
            let existing_items = 0;

            when buffer_is_below_capacity_and_no_flush_is_in_flight {
                to returns_ready_without_flushing { poll_ready_ok }
            }
        }

        expect(poll_ready_in_flight()) {
            when an_earlier_flush_is_still_in_flight {
                to waits_for_the_flush_to_complete { keeps_in_flight_flush }
            }
        }

        expect(poll_flush_idle()) {
            when there_is_neither_a_pending_flush_nor_buffered_work {
                to completes_immediately_without_touching_the_database {
                    observation_is_idle_ok
                }
            }
        }

        expect(poll_flush_in_flight_ready(result)) {
            let result = Ok(());

            when the_in_flight_flush_resolves_successfully {
                to returns_ready_ok_and_clears_the_future {
                    observation_is_ready_ok_and_cleared
                }
            }

            when the_in_flight_flush_resolves_with_an_error {
                let result = Err(Error::SinkBufferFull(1));
                to surfaces_the_error_and_clears_the_future {
                    observation_is_ready_err_and_cleared
                }
            }
        }

        expect(poll_flush_in_flight_pending()) {
            when the_in_flight_flush_is_still_pending {
                to stays_pending_and_keeps_the_future {
                    observation_stays_pending
                }
            }
        }

        expect(poll_close_via_storage(buffered)) {
            let buffered = 0;

            when the_sink_is_already_drained {
                to delegates_to_flush_and_completes { poll_ready_ok }
            }
        }

        expect(cloned_sink_buffer_len(buffered_items)) {
            let buffered_items = 2;

            when the_original_sink_has_buffered_tasks {
                to starts_the_clone_with_an_empty_buffer { equal(0) }
            }
        }

        expect(cloned_sink_state_drops_flush_future()) {
            when the_original_sink_has_an_in_flight_flush {
                to does_not_share_the_in_flight_flush_future { equal(true) }
            }
        }

        expect(cloned_sink_buffer_size(buffer_size)) {
            let buffer_size = 4;

            when the_original_sink_has_custom_capacity {
                to keeps_the_capacity_configuration { equal(4) }
            }
        }

        expect(sink_debug(buffered_items)) {
            let buffered_items = 2;

            when the_sink_has_buffered_items {
                to describes_the_sink_without_exposing_the_pool {
                    debug_mentions_public_fields
                }
            }
        }
    }

    #[cfg(feature = "tokio")]
    mod tokio_tests {
        use super::*;

        lets_expect! { #tokio_test
            expect(poll_ready_via_storage(buffer_size, existing_items)) {
                let buffer_size = 1;
                let existing_items = 1;

                when buffer_is_at_capacity_without_a_flush_in_flight {
                    to starts_flushing_before_accepting_more_work { poll_started_flush }
                }
            }
        }

        lets_expect! { #tokio_test
            expect(poll_flush_sink_with_state(buffer_size, buffered, None).poll) {
                let buffer_size = 2;
                let buffered = 1;

                when poll_flush_runs_on_a_real_runtime_with_buffered_work {
                    to resolves_to_an_error_against_an_unreachable_pool { poll_started_flush }
                }
            }
        }

        lets_expect! { #tokio_test
            expect(poll_flush_creates_future()) {
                when there_is_no_in_flight_flush_but_the_buffer_has_work {
                    to drains_the_buffer_into_a_new_flush_future {
                        observation_drained_buffer_into_future
                    }
                }
            }

            expect(poll_close_via_storage(1)) {
                when there_is_buffered_work_to_flush_before_closing {
                    to starts_flushing_the_buffered_work_before_completing { poll_started_flush }
                }
            }
        }
    }
}
