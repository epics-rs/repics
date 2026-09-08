//! `epicsrs._epicsrs` — the Rust half of epicsrs.
//!
//! This module is deliberately thin: it owns the one tokio runtime, wraps
//! the epics-rs client handles as Python classes, and converts values at the
//! boundary. Policy (default contexts, augmented values, list shapes, the
//! pyepics/aioca/p4p-shaped front ends) lives in the pure-Python package.

mod ca;
mod error;
mod pva;
mod runtime;
mod value;

use pyo3::prelude::*;

#[pymodule]
fn _epicsrs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("CaError", py.get_type::<error::CaError>())?;
    m.add("CaTimeout", py.get_type::<error::CaTimeout>())?;
    m.add("CaDisconnected", py.get_type::<error::CaDisconnected>())?;
    m.add_class::<ca::CaContext>()?;
    m.add_class::<ca::CaChannel>()?;
    m.add_class::<ca::CaSubscription>()?;
    m.add_class::<ca::Snapshot>()?;
    m.add_class::<ca::ChannelInfo>()?;
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
    Ok(())
}
