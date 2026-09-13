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
use std::pin::Pin;
use std::sync::{Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use tokio::sync::oneshot;
use tokio::task::JoinError;

use crate::error::timeout_err;

/// Configure the runtime before its first use (module import).
///
/// One worker thread by default: a CA client is a few sockets and many
/// small tasks, and every hop between workers is a futex wake that shows
/// up as CPU per monitor update (measured: 33 us per callback with one
/// worker against 40-50 with four, on 100 monitored PVs). Value
/// conversion and user callbacks run on Python's threads regardless.
/// `REPICS_WORKERS` overrides the count.
pub fn configure() {
    let workers = std::env::var_os("REPICS_WORKERS")
        .and_then(|v| v.into_string().ok())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1)
        .max(1);
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder
        .enable_all()
        .worker_threads(workers)
        .thread_name("repics-rt");
    pyo3_async_runtimes::tokio::init(builder);
}

/// Send the libraries' `tracing` output to stderr when `REPICS_LOG` is
/// set; its value is an env-filter directive list (`epics_pva_rs=debug`),
/// as `PVXS_LOG` is for pvxs. Unset, nothing is logged.
pub fn init_logging() {
    if let Ok(spec) = std::env::var("REPICS_LOG") {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(spec)
            .with_writer(std::io::stderr)
            .try_init();
    }
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
        None => hand_to_asyncio(py, fut),
    }
}

/// Python-bound completions the runtime still owes: one per future handed
/// to asyncio, from the hand-over until its outcome has been posted to the
/// loop or its cancellation noticed. `wait_idle` lets interpreter shutdown
/// wait for the count to reach zero, so no runtime thread attaches to an
/// interpreter that is already finalized.
static IN_FLIGHT: (Mutex<usize>, Condvar) = (Mutex::new(0), Condvar::new());

struct InFlight;

impl InFlight {
    fn new() -> Self {
        *IN_FLIGHT.0.lock().unwrap() += 1;
        InFlight
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        let mut n = IN_FLIGHT.0.lock().unwrap();
        *n -= 1;
        if *n == 0 {
            IN_FLIGHT.1.notify_all();
        }
    }
}

/// Block until the runtime owes asyncio nothing, or `timeout` passes;
/// true when idle. Call with the GIL released: the completions need it.
pub fn wait_idle(timeout: Duration) -> bool {
    let (lock, cv) = &IN_FLIGHT;
    let g = lock.lock().unwrap();
    let (g, _) = cv.wait_timeout_while(g, timeout, |n| *n > 0).unwrap();
    *g == 0
}

/// Done callback of the asyncio future: a cancel from Python drops the
/// Rust future.
#[pyclass]
struct CancelRelay {
    cancel_tx: Option<oneshot::Sender<()>>,
}

#[pymethods]
impl CancelRelay {
    fn __call__(&mut self, fut: &Bound<'_, PyAny>) -> PyResult<()> {
        if fut.call_method0("cancelled")?.is_truthy()? {
            if let Some(tx) = self.cancel_tx.take() {
                let _ = tx.send(());
            }
        }
        Ok(())
    }
}

/// Runs on the loop: settle the future unless it was cancelled meanwhile.
#[pyclass]
struct Completor;

#[pymethods]
impl Completor {
    fn __call__(
        &self,
        fut: &Bound<'_, PyAny>,
        method: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if fut.call_method0("cancelled")?.is_truthy()? {
            return Ok(());
        }
        fut.call_method1(method, (value,))?;
        Ok(())
    }
}

/// Run `fut` on the runtime and settle an asyncio future with its outcome
/// (pyo3_async_runtimes' `future_into_py`, with the in-flight accounting
/// above). The outcome is posted from a blocking thread so a worker never
/// waits for the GIL, and that thread holds the in-flight guard until it
/// has posted, which is what `wait_idle` waits for.
fn hand_to_asyncio<'py, F, T>(py: Python<'py>, mut fut: Pin<Box<F>>) -> PyResult<Bound<'py, PyAny>>
where
    F: Future<Output = PyResult<T>> + Send + 'static,
    T: for<'a> IntoPyObject<'a> + Send + 'static,
{
    let locals = pyo3_async_runtimes::tokio::get_current_locals(py)?;
    let py_fut = locals.event_loop(py).call_method0("create_future")?;
    let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
    py_fut.call_method1(
        "add_done_callback",
        (CancelRelay {
            cancel_tx: Some(cancel_tx),
        },),
    )?;
    let target: Py<PyAny> = py_fut.clone().unbind();
    let guard = InFlight::new();
    runtime().spawn(async move {
        let inner = runtime().spawn(async move {
            tokio::select! {
                biased;
                r = &mut fut => Some(r),
                c = &mut cancel_rx => match c {
                    Ok(()) => None,
                    // The asyncio future went away without settling; nobody
                    // can cancel any more, so run to completion.
                    Err(_) => Some(fut.await),
                },
            }
        });
        let result = match inner.await {
            Ok(r) => r,
            Err(e) => Some(Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "rust future failed: {e}"
            )))),
        };
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let Some(result) = result else {
                return;
            };
            // SAFETY: a plain flag read; the only defence left when the
            // interpreter has gone before `wait_idle` could run.
            if unsafe { pyo3::ffi::Py_IsInitialized() } == 0 {
                return;
            }
            Python::attach(|py| {
                let fut = target.bind(py);
                if fut
                    .call_method0("cancelled")
                    .and_then(|c| c.is_truthy())
                    .unwrap_or(true)
                {
                    return;
                }
                let (method, value) = match result.and_then(|v| v.into_py_any(py)) {
                    Ok(v) => ("set_result", v),
                    Err(e) => ("set_exception", e.into_value(py).into_any()),
                };
                let kwargs = PyDict::new(py);
                let _ = kwargs.set_item("context", locals.context(py));
                // A closed loop refuses the call; the outcome is then moot.
                let _ = locals.event_loop(py).call_method(
                    "call_soon_threadsafe",
                    (Completor, fut, method, value),
                    Some(&kwargs),
                );
            });
        });
    });
    Ok(py_fut)
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
