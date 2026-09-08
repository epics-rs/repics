//! Python exception types and the `CaError` mapping.

use epics_ca_rs::CaError as RsCaError;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

create_exception!(
    epicsrs,
    CaError,
    PyException,
    "Channel Access operation failed."
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

pub fn map_ca(e: RsCaError) -> PyErr {
    match e {
        RsCaError::Timeout => CaTimeout::new_err(e.to_string()),
        // `CaChannel::wait_connected` reports its deadline as
        // `ChannelNotFound(name)`, so that variant is the connect timeout.
        RsCaError::ChannelNotFound(_) => CaTimeout::new_err(e.to_string()),
        RsCaError::Disconnected => CaDisconnected::new_err(e.to_string()),
        other => CaError::new_err(other.to_string()),
    }
}
