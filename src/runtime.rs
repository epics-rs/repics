//! The one tokio runtime this extension owns.
//!
//! Both execution flavours share it: the blocking flavour `block_on`s a
//! future with the GIL released, the asyncio flavour hands the same future
//! to `pyo3_async_runtimes`, which drives it on this runtime and resolves an
//! asyncio future. A `CaContext` built through either flavour therefore
//! lives on the same reactor and may be used from both.
//!
//! Python callbacks are never invoked from a runtime worker: every
//! subscription is drained by Python calling `recv`, so no Python code can
//! re-enter `block_on` from inside the runtime.

use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyList;
use tokio::task::JoinError;

use crate::error::timeout_err;

/// Configure the runtime before its first use (module import).
///
/// One worker thread by default: a CA client is a few sockets and many
/// small tasks, and every hop between workers is a futex wake that shows
/// up as CPU per monitor update (measured: 33 us per callback with one
/// worker against 40-50 with four, on 100 monitored PVs). Value
/// conversion and user callbacks run on Python's threads regardless.
/// `EPICSRS_WORKERS` overrides the count.
pub fn configure() {
    let workers = std::env::var_os("EPICSRS_WORKERS")
        .and_then(|v| v.into_string().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1);
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .enable_all()
        .worker_threads(workers)
        .thread_name("epicsrs-rt");
    pyo3_async_runtimes::tokio::init(builder);
}

pub fn runtime() -> &'static tokio::runtime::Runtime {
    pyo3_async_runtimes::tokio::get_runtime()
}

/// Run `fut` to completion on the shared runtime with the GIL released.
pub fn block_on<F, T>(py: Python<'_>, fut: F) -> T
where
    F: Future<Output = T> + Send,
    T: Send,
{
    py.detach(|| runtime().block_on(fut))
}

/// Hand `fut` to asyncio as an awaitable.
///
/// The future is polled once first, inside the runtime's context so its
/// timers and channels register with the driver. If it is already done
/// the result comes back as a completed asyncio future and the event loop
/// is never woken from another thread: a `put` without `wait`, a read of
/// an already-queued monitor batch, a wait on a channel that is already
/// connected all take this path. Otherwise the future runs on the runtime
/// as usual.
pub fn into_py_future<'py, F, T>(py: Python<'py>, fut: F) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    let mut fut = Box::pin(fut);
    let ready = {
        let _guard = runtime().enter();
        let mut cx = Context::from_waker(Waker::noop());
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(r) => Some(r),
            Poll::Pending => None,
        }
    };
    match ready {
        Some(r) => {
            let value = r?.into_py_any(py)?;
            let done = pyo3_async_runtimes::get_running_loop(py)?.call_method0("create_future")?;
            done.call_method1("set_result", (value,))?;
            Ok(done)
        }
        None => pyo3_async_runtimes::tokio::future_into_py(py, fut),
    }
}

/// Bound `fut` by `timeout` seconds; `None` means wait forever.
pub async fn bounded<F, T>(timeout: Option<f64>, fut: F) -> PyResult<T>
where
    F: Future<Output = PyResult<T>>,
{
    match timeout {
        None => fut.await,
        Some(secs) => match tokio::time::timeout(Duration::from_secs_f64(secs.max(0.0)), fut).await
        {
            Ok(r) => r,
            Err(_) => Err(timeout_err(secs)),
        },
    }
}

/// Run every future concurrently on the runtime; results keep input order.
/// A task that panicked reports through `on_join_error`, so each front end
/// raises its own exception family.
pub(crate) async fn join_all<T, F>(
    futs: Vec<F>,
    on_join_error: fn(JoinError) -> PyErr,
) -> Vec<PyResult<T>>
where
    T: Send + 'static,
    F: Future<Output = PyResult<T>> + Send + 'static,
{
    let handles: Vec<_> = futs.into_iter().map(|f| runtime().spawn(f)).collect();
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(match h.await {
            Ok(r) => r,
            Err(e) => Err(on_join_error(e)),
        });
    }
    out
}

/// A list of results where a failure is the exception object itself, so the
/// Python side can raise the first one (`throw=True`) or return each in
/// place (`throw=False`).
pub(crate) fn results_to_py<'py, T>(
    py: Python<'py>,
    results: Vec<PyResult<T>>,
) -> PyResult<Bound<'py, PyList>>
where
    T: for<'a> IntoPyObject<'a>,
{
    let items = results
        .into_iter()
        .map(|r| match r {
            Ok(v) => v.into_py_any(py),
            Err(e) => Ok(e.into_value(py).into_any()),
        })
        .collect::<PyResult<Vec<_>>>()?;
    PyList::new(py, items)
}

/// [`results_to_py`] from a runtime task: takes the GIL for the conversion.
pub(crate) fn results_to_py_owned<T>(results: Vec<PyResult<T>>) -> PyResult<Py<PyList>>
where
    T: for<'a> IntoPyObject<'a>,
{
    Python::attach(|py| results_to_py(py, results).map(Bound::unbind))
}
