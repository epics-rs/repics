//! The pvAccess exception family and the `PvaError` mapping.
//!
//! Kept apart from the CA family so `except CaError` never catches a PVA
//! failure and vice versa; both derive from `Exception` only.

use epics_pva_rs::PvaError as RsPvaError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(epicsrs, PvaError, PyException, "pvAccess operation failed.");
create_exception!(
    epicsrs,
    PvaTimeout,
    PvaError,
    "pvAccess operation timed out."
);
create_exception!(
    epicsrs,
    PvaDisconnected,
    PvaError,
    "The channel is not connected."
);
create_exception!(
    epicsrs,
    PvaRemoteError,
    PvaError,
    "The server rejected the operation."
);

pub fn map_pva(e: RsPvaError) -> PyErr {
    match e {
        // p4p reports an unresolved name as a timeout too: the search
        // simply never answered within the deadline.
        RsPvaError::Timeout | RsPvaError::ChannelNotFound(_) => PvaTimeout::new_err(e.to_string()),
        RsPvaError::Disconnected => PvaDisconnected::new_err(e.to_string()),
        RsPvaError::RemoteError(_) => PvaRemoteError::new_err(e.to_string()),
        other => PvaError::new_err(other.to_string()),
    }
}

/// Bound `fut` by `timeout` seconds, raising `PvaTimeout` on the deadline.
/// The CA flavour in `crate::runtime::bounded` raises `CaTimeout`, which a
/// PVA caller must never see.
pub async fn bounded<F, T>(timeout: Option<f64>, fut: F) -> PyResult<T>
where
    F: std::future::Future<Output = PyResult<T>>,
{
    match timeout {
        None => fut.await,
        Some(secs) => {
            let d = std::time::Duration::from_secs_f64(secs.max(0.0));
            match tokio::time::timeout(d, fut).await {
                Ok(r) => r,
                Err(_) => Err(PvaTimeout::new_err(format!("timed out after {secs} s"))),
            }
        }
    }
}
