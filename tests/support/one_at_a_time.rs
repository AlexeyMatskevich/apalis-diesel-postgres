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
