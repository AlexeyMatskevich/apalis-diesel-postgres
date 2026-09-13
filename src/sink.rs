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
type FlushFuture =
    Pin<Box<dyn Future<Output = Result<(), queries::FlushFailure>> + Send + 'static>>;

/// Buffered task sink used internally by [`PostgresStorage`]. Not part of the
/// public API: the `Sink<PgTask>` impl lives on `PostgresStorage` itself.
pub(crate) struct PgSink<Args, Codec = JsonCodec<CompactType>> {
    pool: PgPool,
    config: Config,
    buffer: Vec<PgTask<CompactType>>,
    flush_future: Mutex<Option<FlushFuture>>,
    failed: bool,
    _marker: PhantomData<(Args, Codec)>,
}

impl<Args, Codec> std::fmt::Debug for PgSink<Args, Codec> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgSink")
            .field("config", &self.config)
            .field("buffer_len", &self.buffer.len())
            .field("failed", &self.failed)
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
            failed: false,
            _marker: PhantomData,
        }
    }
}

impl<Args, Codec> PgSink<Args, Codec> {
    /// Create a sink for the given pool and config.
    #[must_use]
    pub(crate) fn new(pool: &PgPool, config: &Config) -> Self {
        Self {
            pool: pool.clone(),
            config: config.clone(),
            buffer: Vec::new(),
            flush_future: Mutex::new(None),
            failed: false,
            _marker: PhantomData,
        }
    }
}

impl<Args, Codec> PgSink<Args, Codec> {
    /// Re-type the sink's `Args`/`Codec` markers while carrying over the
    /// buffered tasks and any in-flight flush. The buffer stores
    /// codec-independent `PgTask<CompactType>` values, so a codec swap via
    /// [`crate::PostgresStorage::with_codec`] must not silently drop pending
    /// work — only the phantom markers change.
    pub(crate) fn retype<NewArgs, NewCodec>(self) -> PgSink<NewArgs, NewCodec> {
        PgSink {
            pool: self.pool,
            config: self.config,
            buffer: self.buffer,
            flush_future: self.flush_future,
            failed: self.failed,
            _marker: PhantomData,
        }
    }

    /// Buffer capacity from the underlying config (clamped to ≥1 so a
    /// misconfigured `buffer_size(0)` does not deadlock the sink).
    fn capacity(&self) -> usize {
        self.config.buffer_size().max(1)
    }

    /// Whether `poll_ready` must drive a flush before accepting more work —
    /// either a flush is already in flight, or the buffer is at capacity.
    fn needs_flush_before_ready(&mut self) -> bool {
        self.failed
            || self
                .flush_future
                .get_mut()
                .expect("flush_future mutex poisoned")
                .is_some()
            || self.buffer.len() >= self.capacity()
    }

    /// Try to enqueue a single task into the buffer, returning
    /// `Error::SinkBufferFull` when capacity has been reached and
    /// `Error::InvalidArgument` for a task that no batch could write.
    fn try_push(&mut self, item: PgTask<CompactType>) -> Result<(), Error> {
        if self.failed {
            return Err(Error::SinkFailed);
        }
        let cap = self.capacity();
        if self.buffer.len() >= cap {
            return Err(Error::SinkBufferFull(cap));
        }
        // A rejected task never enters the buffer, so a buffered batch can
        // only fail for database reasons.
        queries::validate_task(&self.config, &item)?;
        self.buffer.push(item);
        Ok(())
    }

    /// Drive the buffered batch toward completion. Starts a new flush future
    /// when none is in flight and the buffer is non-empty; otherwise polls the
    /// existing future. Successful completion covers any tasks accepted while
    /// that future was pending. A flush that issued its statement and failed
    /// permanently fails this pipeline: the batch may have been written. A
    /// flush that could not obtain a connection hands the batch back to the
    /// buffer, and one refused by validation drops it; both leave the
    /// pipeline usable because nothing was written.
    fn poll_flush_inner(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.failed {
            return Poll::Ready(Err(Error::SinkFailed));
        }
        // A readiness grant can be followed by an intermediate flush before
        // start_send uses that grant. The active batch and the later buffer
        // therefore both belong to this completion request.
        loop {
            // Unique field borrows keep this projection lock-free. The mutex
            // only makes the stored Send future compatible with PgSink: Sync.
            let flush_future = self
                .flush_future
                .get_mut()
                .expect("flush_future mutex poisoned");
            if flush_future.is_none() && self.buffer.is_empty() {
                return Poll::Ready(Ok(()));
            }
            let future = flush_future.get_or_insert_with(|| {
                let pool = self.pool.clone();
                let config = self.config.clone();
                let buffer = std::mem::take(&mut self.buffer);
                Box::pin(queries::flush_tasks(pool, config, buffer))
            });
            match future.poll_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(queries::FlushFailure::NotStarted { mut tasks, error })) => {
                    // Nothing was issued: the batch precedes any task accepted
                    // while it was in flight, so ordering is preserved.
                    *flush_future = None;
                    tasks.append(&mut self.buffer);
                    self.buffer = tasks;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Err(queries::FlushFailure::Rejected(error))) => {
                    *flush_future = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Err(queries::FlushFailure::Uncertain(error))) => {
                    self.failed = true;
                    *flush_future = None;
                    return Poll::Ready(Err(error));
                }
                Poll::Ready(Ok(())) => {
                    *flush_future = None;
                    // Revisit the buffer before promising completion. With
                    // this exclusive borrow no new tasks enter the loop.
                }
            }
        }
    }
}

impl<Args, Encode, Fetcher> Sink<PgTask<CompactType>> for PostgresStorage<Args, Encode, Fetcher>
where
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

    use futures::{Sink, future, task::noop_waker_ref};
    use lets_expect::{AssertionError, AssertionResult, *};

    use super::*;
    use crate::unreachable::unreachable_pool;

    fn task() -> PgTask<CompactType> {
        PgTask::new(b"payload".to_vec())
    }

    fn sink(buffer_size: usize) -> PgSink<Vec<u8>> {
        PgSink::new(
            &unreachable_pool(),
            &Config::new("sink-unit").set_buffer_size(buffer_size),
        )
    }

    fn storage(buffer_size: usize) -> PostgresStorage<Vec<u8>> {
        let pool = unreachable_pool();
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
        storage.sink.flush_future = Mutex::new(Some(Box::pin(future::pending::<
            Result<(), queries::FlushFailure>,
        >())));
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
    /// single poll: the poll result, whether the in-flight future was cleared,
    /// the buffer length and whether the pipeline failed permanently.
    #[derive(Debug)]
    struct FlushObservation {
        poll: Poll<Result<(), Error>>,
        future_cleared: bool,
        buffer_len: usize,
        failed: bool,
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
            failed: sink.failed,
        }
    }

    fn poll_flush_idle() -> FlushObservation {
        poll_flush_sink_with_state(1, 0, None)
    }

    #[derive(Clone, Copy)]
    enum FlushOutcome {
        Written,
        ConnectionUnavailable,
        RejectedBeforeStatement,
        FailedAfterStatement,
    }

    /// An in-flight flush of a two-task batch resolves. For a failure, one
    /// more task was accepted into the buffer behind it, so the observation
    /// shows where the batch ends up relative to that task. A success
    /// continues with buffered work on a runtime (`buffered_enqueue_flush`),
    /// so it resolves with an empty buffer here.
    fn poll_flush_in_flight_ready(outcome: FlushOutcome) -> FlushObservation {
        let behind = usize::from(!matches!(outcome, FlushOutcome::Written));
        let result = match outcome {
            FlushOutcome::Written => Ok(()),
            FlushOutcome::ConnectionUnavailable => Err(queries::FlushFailure::NotStarted {
                tasks: vec![task(), task()],
                error: Error::SinkBufferFull(1),
            }),
            FlushOutcome::RejectedBeforeStatement => {
                Err(queries::FlushFailure::Rejected(Error::SinkBufferFull(1)))
            }
            FlushOutcome::FailedAfterStatement => {
                Err(queries::FlushFailure::Uncertain(Error::SinkBufferFull(1)))
            }
        };
        poll_flush_sink_with_state(4, behind, Some(Box::pin(future::ready(result))))
    }

    fn poll_flush_in_flight_pending() -> FlushObservation {
        poll_flush_sink_with_state(1, 0, Some(Box::pin(future::pending())))
    }

    #[derive(Clone, Copy)]
    enum Cap {
        None,
        Payload,
        Metadata,
        IdempotencyKey,
        RunAt,
        QueueName,
    }

    /// `start_send` with a task that violates one cap, behind one already
    /// buffered task. Returns the send result and the buffer length after it.
    fn start_send_violating(cap: Cap) -> (Result<(), Error>, usize) {
        let queue = if matches!(cap, Cap::QueueName) {
            "q".repeat(queries::MAX_QUEUE_NAME_LEN + 1)
        } else {
            "sink-unit".to_owned()
        };
        let mut storage = PostgresStorage::<Vec<u8>>::new_with_config(
            &unreachable_pool(),
            &Config::new(&queue).set_buffer_size(4),
        );
        storage.sink.buffer.push(task());
        let mut item = task();
        match cap {
            Cap::None | Cap::QueueName => {}
            Cap::Payload => item.args = vec![0; queries::MAX_JOB_PAYLOAD_LEN + 1],
            Cap::Metadata => {
                let mut meta = item.parts.ctx.meta().clone();
                meta.insert(
                    "blob".to_owned(),
                    serde_json::Value::String("m".repeat(queries::MAX_METADATA_PAYLOAD_LEN)),
                );
                item.parts.ctx = item.parts.ctx.with_meta(meta);
            }
            Cap::IdempotencyKey => {
                item.parts.idempotency_key = Some("k".repeat(queries::MAX_IDEMPOTENCY_KEY_LEN + 1));
            }
            Cap::RunAt => item.parts.run_at = u64::MAX,
        }
        let result = Pin::new(&mut storage).start_send(item);
        (result, storage.sink.buffer.len())
    }

    fn rejected_without_buffering(observation: &(Result<(), Error>, usize)) -> AssertionResult {
        match observation {
            (Err(Error::InvalidArgument(_)), 1) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected InvalidArgument with the buffer unchanged, got {other:?}"
            )])),
        }
    }

    /// `poll_flush_creates_future` exercises the `flush_future.is_none() &&
    /// !buffer.is_empty()` branch: the function builds a new flush future from
    /// the buffer and immediately polls it. The buffer is drained into the
    /// future regardless of the poll outcome. Because `push_tasks` runs the
    /// blocking diesel work via `spawn_blocking`, the first poll typically
    /// returns `Poll::Pending` (the connection attempt is still in flight); it
    /// only later resolves to `Ready(Err(...))` once the unreachable pool's
    /// connect times out. Either observation confirms the drain happened.
    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
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
        sink.flush_future = Mutex::new(Some(Box::pin(future::pending::<
            Result<(), queries::FlushFailure>,
        >())));
        sink.clone()
            .flush_future
            .get_mut()
            .expect("flush_future mutex poisoned")
            .is_none()
    }

    fn cloned_sink_buffer_size(buffer_size: usize) -> usize {
        sink(buffer_size).clone().config.buffer_size()
    }

    /// State of the sink after `PostgresStorage::with_codec` re-types the
    /// storage. The buffer holds already-encoded `PgTask<CompactType>`
    /// values, so — unlike `clone`, which deliberately starts empty — a codec
    /// swap must carry both the buffer and any in-flight flush over.
    struct RetypedObservation {
        buffer_len: usize,
        kept_in_flight_flush: bool,
        first_payload: Option<CompactType>,
    }

    const RETYPE_SENTINEL: &[u8] = b"retype-sentinel";

    fn with_codec_observation(buffered_items: usize, flush_in_flight: bool) -> RetypedObservation {
        let mut storage = storage(3);
        for _ in 0..buffered_items {
            storage
                .sink
                .buffer
                .push(PgTask::new(RETYPE_SENTINEL.to_vec()));
        }
        if flush_in_flight {
            storage.sink.flush_future = Mutex::new(Some(Box::pin(future::pending::<
                Result<(), queries::FlushFailure>,
            >())));
        }
        let mut retyped = storage.with_codec::<()>();
        let kept_in_flight_flush = retyped
            .sink
            .flush_future
            .get_mut()
            .expect("flush_future mutex poisoned")
            .is_some();
        RetypedObservation {
            buffer_len: retyped.sink.buffer.len(),
            kept_in_flight_flush,
            first_payload: retyped.sink.buffer.first().map(|t| t.args.clone()),
        }
    }

    fn carried_buffer_len(expected: usize) -> impl Fn(&RetypedObservation) -> AssertionResult {
        move |obs| {
            if obs.buffer_len == expected {
                Ok(())
            } else {
                Err(AssertionError::new(vec![format!(
                    "expected {expected} buffered task(s) to survive with_codec, got {}",
                    obs.buffer_len
                )]))
            }
        }
    }

    fn kept_the_in_flight_flush(obs: &RetypedObservation) -> AssertionResult {
        if obs.kept_in_flight_flush {
            Ok(())
        } else {
            Err(AssertionError::new(vec![
                "expected with_codec to carry the in-flight flush over, but it was dropped"
                    .to_owned(),
            ]))
        }
    }

    fn carried_the_exact_task_bytes(obs: &RetypedObservation) -> AssertionResult {
        match obs.first_payload.as_deref() {
            Some(RETYPE_SENTINEL) => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected the buffered task's payload bytes to survive with_codec verbatim, got {other:?}"
            )])),
        }
    }

    fn sink_debug(buffered_items: usize) -> String {
        let mut sink = sink(3);
        for _ in 0..buffered_items {
            sink.buffer.push(task());
        }
        format!("{sink:?}")
    }

    fn sink_buffer_full_at(expected: usize) -> impl Fn(&Result<usize, Error>) -> AssertionResult {
        move |result| match result {
            Err(Error::SinkBufferFull(c)) if *c == expected => Ok(()),
            other => Err(AssertionError::new(vec![format!(
                "expected sink buffer full at capacity {expected}, got {other:?}"
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

    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
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

    /// Asserts the flush surfaced the *specific* error carried by the flush
    /// failure, with the future cleared. Injecting `SinkBufferFull(1)` must
    /// surface exactly that variant — a regression that substitutes a
    /// different `Err` would slip past a wildcard `Err(_)` match. The buffer
    /// length and the permanent-failure flag distinguish the phases.
    fn observation_surfaces_buffer_full_at(
        expected: usize,
        buffer_len: usize,
        failed: bool,
    ) -> impl Fn(&FlushObservation) -> AssertionResult {
        move |obs| match (&obs.poll, obs.future_cleared, obs.buffer_len, obs.failed) {
            (Poll::Ready(Err(Error::SinkBufferFull(c))), true, len, flag)
                if *c == expected && len == buffer_len && flag == failed =>
            {
                Ok(())
            }
            _ => Err(AssertionError::new(vec![format!(
                "expected Ready(Err(SinkBufferFull({expected}))) with cleared future, {buffer_len} buffered and failed={failed}, got {obs:?}"
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

    #[cfg_attr(not(feature = "tokio"), allow(dead_code))]
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
        if result.contains("pool") {
            return Err(AssertionError::new(vec![format!(
                "debug output unexpectedly exposes the pool, got {result}"
            )]));
        }
        if result.contains("PgSink") && result.contains("config") && result.contains("buffer_len") {
            Ok(())
        } else {
            Err(AssertionError::new(vec![format!(
                "expected sink debug output with public fields, got {result}"
            )]))
        }
    }

    lets_expect! {
        expect(start_send_via_storage(buffer_size, existing_items)) as buffered_enqueue {
            let buffer_size = 2;
            let existing_items = 0;
            to buffers_the_first_task { be_ok_and equal(1) }
            when the_buffer_has_one_slot_left {
                let existing_items = 1;
                to accepts_the_last_task { be_ok_and equal(2) }
            }
            when the_buffer_is_full {
                let existing_items = 2;
                to rejects_the_task_with_the_effective_capacity { sink_buffer_full_at(2) }
            }
            when capacity_is_one {
                let buffer_size = 1;
                to buffers_the_first_task { be_ok_and equal(1) }
                when the_buffer_is_full {
                    let existing_items = 1;
                    to rejects_the_task_with_the_effective_capacity { sink_buffer_full_at(1) }
                }
            }
            when configured_capacity_is_zero {
                let buffer_size = 0;
                to buffers_the_first_task_using_the_minimum_capacity { be_ok_and equal(1) }
                when the_buffer_is_full {
                    let existing_items = 1;
                    to rejects_the_task_with_the_minimum_capacity { sink_buffer_full_at(1) }
                }
            }
        }

        expect(poll_ready_via_storage(buffer_size, existing_items)) as enqueue_readiness {
            let buffer_size = 2;
            let existing_items = 0;

            when buffer_is_below_capacity_and_no_flush_is_in_flight {
                to returns_ready_without_flushing { poll_ready_ok }
            }

            when buffer_has_exactly_one_slot_left {
                // len == capacity - 1 with no in-flight flush: one free slot
                // remains, so there is no backpressure and poll_ready must
                // return Ready(Ok) without driving a flush (pins the `>= cap`
                // guard against a `>= cap - 1` off-by-one).
                let buffer_size = 2;
                let existing_items = 1;
                to returns_ready_without_flushing { poll_ready_ok }
            }

            when configured_capacity_is_zero_and_the_buffer_is_empty {
                // Positive side of the capacity clamp on the readiness path:
                // `buffer_size(0)` clamps to 1, so an empty buffer has a free
                // slot and `poll_ready` must return Ready(Ok) rather than
                // wedging. A regression to `min(buffer_size, 1)` (== 0) would
                // make `len >= cap` true at len 0 and drive a flush instead.
                let buffer_size = 0;
                let existing_items = 0;
                to returns_ready_without_flushing { poll_ready_ok }
            }
        }

        expect(poll_ready_in_flight()) as ongoing_enqueue_readiness {
            when an_earlier_flush_is_still_in_flight {
                to waits_for_the_flush_to_complete { keeps_in_flight_flush }
            }
        }

        expect(poll_flush_idle()) as idle_enqueue_flush {
            when there_is_neither_a_pending_flush_nor_buffered_work {
                to completes_immediately_without_touching_the_database {
                    observation_is_idle_ok
                }
            }
        }

        expect(poll_flush_in_flight_ready(outcome)) as completed_enqueue_flush {
            let outcome = FlushOutcome::Written;

            when the_in_flight_flush_resolves_successfully {
                to returns_ready_ok_and_clears_the_future {
                    observation_is_ready_ok_and_cleared
                }
            }

            when the_in_flight_flush_could_not_obtain_a_connection {
                let outcome = FlushOutcome::ConnectionUnavailable;
                to surfaces_the_error_once_and_keeps_the_batch_ahead_of_later_tasks {
                    observation_surfaces_buffer_full_at(1, 3, false)
                }
            }

            when the_in_flight_flush_was_rejected_before_any_statement {
                let outcome = FlushOutcome::RejectedBeforeStatement;
                to surfaces_the_error_and_keeps_the_pipeline_usable {
                    observation_surfaces_buffer_full_at(1, 1, false)
                }
            }

            when the_in_flight_flush_failed_after_its_statement_was_issued {
                let outcome = FlushOutcome::FailedAfterStatement;
                to surfaces_the_error_and_fails_the_pipeline {
                    observation_surfaces_buffer_full_at(1, 1, true)
                }
            }
        }

        expect(start_send_violating(cap)) as validated_enqueue {
            let cap = Cap::None;
            to buffers_a_task_within_every_cap { have(1) equal(2) }
            when the_payload_exceeds_its_cap {
                let cap = Cap::Payload;
                to rejects_the_task_without_buffering_it { rejected_without_buffering }
            }
            when the_metadata_exceeds_its_cap {
                let cap = Cap::Metadata;
                to rejects_the_task_without_buffering_it { rejected_without_buffering }
            }
            when the_idempotency_key_exceeds_its_cap {
                let cap = Cap::IdempotencyKey;
                to rejects_the_task_without_buffering_it { rejected_without_buffering }
            }
            when the_run_at_is_unrepresentable {
                let cap = Cap::RunAt;
                to rejects_the_task_without_buffering_it { rejected_without_buffering }
            }
            when the_queue_name_exceeds_its_cap {
                let cap = Cap::QueueName;
                to rejects_the_task_without_buffering_it { rejected_without_buffering }
            }
        }

        expect(poll_flush_in_flight_pending()) as ongoing_enqueue_flush {
            when the_in_flight_flush_is_still_pending {
                to stays_pending_and_keeps_the_future {
                    observation_stays_pending
                }
            }
        }

        expect(poll_close_via_storage(buffered)) as enqueue_close {
            let buffered = 0;

            when the_sink_is_already_drained {
                to delegates_to_flush_and_completes { poll_ready_ok }
            }
        }

        expect(cloned_sink_buffer_len(buffered_items)) as cloned_enqueue_buffer {
            let buffered_items = 2;

            when the_original_sink_has_buffered_tasks {
                to starts_the_clone_with_an_empty_buffer { equal(0) }
            }
        }

        expect(cloned_sink_state_drops_flush_future()) as cloned_enqueue_operation {
            when the_original_sink_has_an_in_flight_flush {
                to does_not_share_the_in_flight_flush_future { equal(true) }
            }
        }

        expect(cloned_sink_buffer_size(buffer_size)) as cloned_enqueue_capacity {
            let buffer_size = 4;

            when the_original_sink_has_custom_capacity {
                to keeps_the_capacity_configuration { equal(4) }
            }
        }

        expect(sink_debug(buffered_items)) as enqueue_description {
            let buffered_items = 2;

            when the_sink_has_buffered_items {
                to describes_the_sink_without_exposing_the_pool {
                    debug_mentions_public_fields
                }
            }
        }

        expect(with_codec_observation(buffered_items, flush_in_flight)) as storage_codec_change {
            let buffered_items = 1;
            let flush_in_flight = false;
            to preserves_the_buffer_and_flush_ownership {
                carried_buffer_len(1),
                carried_the_exact_task_bytes,
                have(kept_in_flight_flush) { be_false }
            }
            when a_flush_is_in_flight {
                let flush_in_flight = true;
                to preserves_the_buffer_and_in_flight_flush {
                    carried_buffer_len(1),
                    carried_the_exact_task_bytes,
                    kept_the_in_flight_flush
                }
            }
            when the_buffer_is_empty {
                let buffered_items = 0;
                to preserves_the_empty_sink {
                    carried_buffer_len(0),
                    have(first_payload.as_ref()) { be_none },
                    have(kept_in_flight_flush) { be_false }
                }
                when a_flush_is_in_flight {
                    let flush_in_flight = true;
                    to preserves_the_flush_after_its_buffer_was_drained {
                        carried_buffer_len(0),
                        have(first_payload.as_ref()) { be_none },
                        kept_the_in_flight_flush
                    }
                }
            }
        }
    }

    #[cfg(feature = "tokio")]
    mod tokio_tests {
        use super::*;

        lets_expect! { #tokio_test
            expect(poll_ready_via_storage(buffer_size, existing_items)) as enqueue_readiness {
                let buffer_size = 1;
                let existing_items = 1;

                when buffer_is_at_capacity_without_a_flush_in_flight {
                    to starts_flushing_before_accepting_more_work { poll_started_flush }
                }
            }
        }

        lets_expect! { #tokio_test
            expect(poll_flush_sink_with_state(buffer_size, buffered, None).poll) as buffered_enqueue_flush {
                let buffer_size = 2;
                let buffered = 1;

                when poll_flush_runs_on_a_real_runtime_with_buffered_work {
                    to starts_flushing_against_the_unreachable_pool { poll_started_flush }
                }
            }
        }

        lets_expect! { #tokio_test
            expect(poll_flush_creates_future()) as new_enqueue_flush {
                when there_is_no_in_flight_flush_but_the_buffer_has_work {
                    to drains_the_buffer_into_a_new_flush_future {
                        observation_drained_buffer_into_future
                    }
                }
            }

            expect(poll_close_via_storage(1)) as enqueue_close {
                when there_is_buffered_work_to_flush_before_closing {
                    to starts_flushing_the_buffered_work_before_completing { poll_started_flush }
                }
            }
        }
    }
}
