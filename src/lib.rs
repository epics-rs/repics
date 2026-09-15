//! `repics._repics` — the Rust half of repics.
//!
//! This module is deliberately thin: it owns the one tokio runtime, wraps
//! the epics-rs client handles as Python classes, and converts values at the
//! boundary. Policy (default contexts, augmented values, list shapes, the
//! pyepics/aioca/p4p-shaped front ends) lives in the pure-Python package.

mod ca;
mod ca_server;
mod error;
mod hub;
mod pva;
mod runtime;
mod value;

use pyo3::prelude::*;

/// The libca message text for an ECA status code.
#[pyfunction]
fn ca_message(status: u32) -> &'static str {
    epics_ca_rs::protocol::eca_message(status)
}

/// Wait up to `timeout` seconds for the runtime to settle every asyncio
/// future it still owes; true when it did. Interpreter shutdown calls this
/// so no runtime thread touches a finalized interpreter.
#[pyfunction]
fn _wait_idle(py: Python<'_>, timeout: f64) -> bool {
    py.detach(|| runtime::wait_idle(std::time::Duration::from_secs_f64(timeout.max(0.0))))
}

#[pymodule]
fn _repics(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    runtime::configure();
    runtime::init_logging();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_function(wrap_pyfunction!(ca_message, m)?)?;
    m.add_function(wrap_pyfunction!(_wait_idle, m)?)?;
    m.add("CaError", py.get_type::<error::CaError>())?;
    m.add("CaTimeout", py.get_type::<error::CaTimeout>())?;
    m.add("CaDisconnected", py.get_type::<error::CaDisconnected>())?;
    m.add_class::<ca::CaContext>()?;
    m.add_class::<ca::CaChannel>()?;
    m.add_class::<ca::CaSubscription>()?;
    m.add_class::<ca::CaEvents>()?;
    m.add_class::<ca::ConnectionEvent>()?;
    m.add_class::<ca::Snapshot>()?;
    m.add_class::<ca::ChannelInfo>()?;
    m.add_class::<hub::MonitorHub>()?;
    m.add_class::<ca_server::CaServerOperation>()?;
    m.add_class::<ca_server::CaWorkQueue>()?;
    m.add_class::<ca_server::CaSharedPV>()?;
    m.add_class::<ca_server::CaProvider>()?;
    m.add_class::<ca_server::CaServer>()?;
    m.add("PvaError", py.get_type::<pva::error::PvaError>())?;
    m.add("PvaTimeout", py.get_type::<pva::error::PvaTimeout>())?;
    m.add(
        "PvaDisconnected",
        py.get_type::<pva::error::PvaDisconnected>(),
    )?;
    m.add(
        "PvaRemoteError",
        py.get_type::<pva::error::PvaRemoteError>(),
    )?;
    m.add_class::<pva::value::Type>()?;
    m.add_class::<pva::value::Value>()?;
    m.add_class::<pva::client::PvaContext>()?;
    m.add_class::<pva::client::PutOp>()?;
    m.add_class::<pva::hub::PvaMonitorHub>()?;
    m.add_class::<pva::server::ServerOperation>()?;
    m.add_class::<pva::server::PvaWorkQueue>()?;
    m.add_class::<pva::server::PvaSharedPV>()?;
    m.add_class::<pva::server::PvaProvider>()?;
    m.add_class::<pva::server::PvaServer>()?;
    Ok(())
}
