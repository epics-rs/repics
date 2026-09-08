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
use std::time::Duration;

use pyo3::prelude::*;

use crate::error::CaTimeout;

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
            Err(_) => Err(CaTimeout::new_err(format!("timed out after {secs} s"))),
        },
    }
}
