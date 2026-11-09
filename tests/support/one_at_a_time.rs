//! A worker middleware that admits one task at a time, so the rest of a
//! claimed batch stays claimed and undispatched while a task runs.
use std::sync::{Arc, atomic::Ordering};

/// Admits one task into the worker at a time. While a task is in flight the
/// worker takes nothing more from the stream, so the rest of a claimed batch
/// stays claimed and undispatched.
#[derive(Clone, Default)]
pub struct OneAtATimeLayer {
    gate: Arc<Gate>,
}

#[derive(Default)]
pub struct Gate {
    busy: std::sync::atomic::AtomicBool,
    waiter: std::sync::Mutex<Option<std::task::Waker>>,
}

impl<S> apalis_core::layers::Layer<S> for OneAtATimeLayer {
    type Service = OneAtATime<S>;

    fn layer(&self, inner: S) -> Self::Service {
        OneAtATime {
            inner,
            gate: self.gate.clone(),
        }
    }
}

#[derive(Clone)]
pub struct OneAtATime<S> {
    inner: S,
    gate: Arc<Gate>,
}

impl<S, Request> apalis_core::layers::Service<Request> for OneAtATime<S>
where
    S: apalis_core::layers::Service<Request>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if self.gate.busy.load(Ordering::SeqCst) {
            *self
                .gate
                .waiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(cx.waker().clone());
            // Read again with the waker in place so a completion in between
            // is not missed.
            if self.gate.busy.load(Ordering::SeqCst) {
                return std::task::Poll::Pending;
            }
        }
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request) -> Self::Future {
        self.gate.busy.store(true, Ordering::SeqCst);
        let gate = self.gate.clone();
        let response = self.inner.call(request);
        Box::pin(async move {
            let result = response.await;
            gate.busy.store(false, Ordering::SeqCst);
            let waiter = gate
                .waiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            if let Some(waiter) = waiter {
                waiter.wake();
            }
            result
        })
    }
}

/// A backend whose middleware runs `layer` *outside* the backend's own
/// middleware. apalis 1.0 installs `WorkerBuilder::layer` layers inside it;
/// tests that need the pre-1.0 order (a gate or a retry layer around the
/// backend middleware) wrap the backend instead.
pub struct WithOuterLayer<B, L> {
    inner: B,
    layer: L,
}

/// Wrap `backend` so `layer` runs outside its middleware.
#[allow(dead_code)]
pub fn with_outer_layer<B, L>(backend: B, layer: L) -> WithOuterLayer<B, L> {
    WithOuterLayer {
        inner: backend,
        layer,
    }
}

/// Wrap `backend` so its worker admits one task at a time. Inside the backend
/// middleware the gate would close only once that middleware had started the
/// claim, so the worker could admit the rest of the batch meanwhile.
#[allow(dead_code)]
pub fn one_at_a_time<B>(backend: B) -> WithOuterLayer<B, OneAtATimeLayer> {
    with_outer_layer(backend, OneAtATimeLayer::default())
}

impl<B: apalis_core::backend::Backend, L> apalis_core::backend::Backend for WithOuterLayer<B, L> {
    type Task = B::Task;
    type Error = B::Error;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
        worker: &apalis_core::worker::context::WorkerContext,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx, worker)
    }

    fn poll_next(
        &mut self,
        cx: &mut std::task::Context<'_>,
        worker: &apalis_core::worker::context::WorkerContext,
    ) -> std::task::Poll<Option<Result<Self::Task, Self::Error>>> {
        self.inner.poll_next(cx, worker)
    }

    fn poll_close(
        &mut self,
        cx: &mut std::task::Context<'_>,
        worker: &apalis_core::worker::context::WorkerContext,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_close(cx, worker)
    }
}

impl<B: apalis_core::backend::BackendConfig, L: Clone> apalis_core::backend::BackendConfig
    for WithOuterLayer<B, L>
{
    type Args = B::Args;
    type Id = B::Id;
    type Kind = B::Kind;
    type Config = B::Config;
    type Layer = apalis_core::layers::Stack<B::Layer, L>;

    fn config(&self) -> &Self::Config {
        self.inner.config()
    }

    fn middleware(
        &mut self,
        worker: &mut apalis_core::worker::context::WorkerContext,
    ) -> Self::Layer {
        apalis_core::layers::Stack::new(self.inner.middleware(worker), self.layer.clone())
    }
}

impl<B: apalis_core::backend::WireFormatBackend, L> apalis_core::backend::WireFormatBackend
    for WithOuterLayer<B, L>
{
    type Codec = B::Codec;
    type Compact = B::Compact;

    fn codec(&self) -> &Self::Codec {
        self.inner.codec()
    }
}
