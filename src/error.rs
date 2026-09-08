//! Python exception types and the `CaError` mapping.
//!
//! Every raised `CaError` carries `args == (message, eca_status)`, so a
//! Python caller can recover the ECA status the way libca reports it
//! (`ECA_TIMEOUT`, `ECA_DISCONN`, `ECA_NOWTACCESS`, ...) without parsing
//! the message.

use epics_ca_rs::protocol::{
    ECA_DISCONN, ECA_NORDACCESS, ECA_NOWTACCESS, ECA_TIMEOUT, eca_message,
};
use epics_ca_rs::{CaError as RsCaError, CaOp};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    epicsrs,
    CaError,
    PyException,
    "Channel Access operation failed; args are (message, eca_status)."
);
create_exception!(
    epicsrs,
    CaTimeout,
    CaError,
    "Channel Access operation timed out."
);
create_exception!(
    epicsrs,
    CaDisconnected,
    CaError,
    "The channel is not connected."
);

pub fn timeout_err(secs: f64) -> PyErr {
    CaTimeout::new_err((format!("timed out after {secs} s"), ECA_TIMEOUT))
}

/// libca refuses in `nciu::read`/`nciu::write` when the cached access
/// rights say no, with `ECA_NORDACCESS`/`ECA_NOWTACCESS`. epics-ca-rs has
/// the same gate but reports it as a protocol error, so the status is
/// decided here, before the library is asked.
pub fn access_denied(op: CaOp) -> PyErr {
    let code = match op {
        CaOp::Write => ECA_NOWTACCESS,
        _ => ECA_NORDACCESS,
    };
    CaError::new_err((eca_message(code).to_string(), code))
}

/// Map a read-side error (get, monitor, connect).
pub fn map_ca(e: RsCaError) -> PyErr {
    map_ca_op(e, CaOp::Read)
}

/// Map a write-side error (put).
pub fn map_ca_write(e: RsCaError) -> PyErr {
    map_ca_op(e, CaOp::Write)
}

fn map_ca_op(e: RsCaError, op: CaOp) -> PyErr {
    match e {
        // `CaChannel::wait_connected` reports its deadline as
        // `ChannelNotFound(name)`, so that variant is the connect timeout.
        RsCaError::Timeout | RsCaError::ChannelNotFound(_) => {
            CaTimeout::new_err((e.to_string(), ECA_TIMEOUT))
        }
        RsCaError::Disconnected | RsCaError::Shutdown => {
            CaDisconnected::new_err((e.to_string(), ECA_DISCONN))
        }
        // A status decided by the peer: its libca text is the message. A
        // monitor reports the circuit going down this way.
        RsCaError::ServerError(code) | RsCaError::WriteFailed(code) => {
            let text = eca_message(code).to_string();
            if code == ECA_DISCONN {
                CaDisconnected::new_err((text, code))
            } else {
                CaError::new_err((text, code))
            }
        }
        other => {
            let status = other.to_eca_status(op);
            CaError::new_err((other.to_string(), status))
        }
    }
}
