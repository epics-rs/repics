//! Channel Access: `CaContext`, `CaChannel`, `CaSubscription`, `Snapshot`.
//!
//! Every network operation exists in two flavours over one implementation:
//! the plain method blocks with the GIL released, the `*_async` method
//! returns an asyncio awaitable. Both run on the runtime in
//! [`crate::runtime`].

use std::sync::Arc;
use std::time::Duration;

use epics_base_rs::server::snapshot::{DbrClass, Snapshot as RsSnapshot};
use epics_ca_rs::client::{
    CaChannel as RsChannel, CaClient, ChannelInfo as RsChannelInfo, MonitorHandle,
};
use epics_ca_rs::protocol::{DBE_ALARM, DBE_LOG, DBE_VALUE};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio_util::sync::CancellationToken;

use crate::error::{CaError, map_ca};
use crate::runtime::{block_on, bounded};
use crate::value::{self, PutRequest};

/// `None` means no deadline. tokio clamps far-future instants itself.
fn duration_or_forever(timeout: Option<f64>) -> Duration {
    match timeout {
        Some(secs) => Duration::from_secs_f64(secs.max(0.0)),
        None => Duration::from_secs(u32::MAX as u64),
    }
}

fn dbr_class(form: &str) -> PyResult<DbrClass> {
    Ok(match form {
        "plain" => DbrClass::Plain,
        "sts" => DbrClass::Sts,
        "time" => DbrClass::Time,
        "gr" => DbrClass::Gr,
        "ctrl" => DbrClass::Ctrl,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown form {other:?}; expected plain, sts, time, gr or ctrl"
            )));
        }
    })
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// One read of a channel: the value plus whatever metadata the requested
/// form carried. Fields the form did not carry are `None`.
#[pyclass(frozen, module = "epicsrs._epicsrs")]
pub struct Snapshot {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    value: Py<PyAny>,
    #[pyo3(get)]
    datatype: &'static str,
    #[pyo3(get)]
    element_count: u32,
    #[pyo3(get)]
    status: u16,
    #[pyo3(get)]
    severity: u16,
    #[pyo3(get)]
    raw_stamp: (u64, u32),
    #[pyo3(get)]
    units: Option<String>,
    #[pyo3(get)]
    precision: Option<i16>,
    #[pyo3(get)]
    upper_disp_limit: Option<f64>,
    #[pyo3(get)]
    lower_disp_limit: Option<f64>,
    #[pyo3(get)]
    upper_alarm_limit: Option<f64>,
    #[pyo3(get)]
    lower_alarm_limit: Option<f64>,
    #[pyo3(get)]
    upper_warning_limit: Option<f64>,
    #[pyo3(get)]
    lower_warning_limit: Option<f64>,
    #[pyo3(get)]
    upper_ctrl_limit: Option<f64>,
    #[pyo3(get)]
    lower_ctrl_limit: Option<f64>,
    #[pyo3(get)]
    enums: Option<Py<PyList>>,
    #[pyo3(get)]
    ackt: Option<u16>,
    #[pyo3(get)]
    acks: Option<u16>,
}

impl Snapshot {
    fn from_rs(py: Python<'_>, name: &str, snap: RsSnapshot) -> PyResult<Self> {
        let datatype = snap.value.db_field_type().dbf_code().name();
        let element_count = snap.value.count();
        let (units, precision, disp) = match &snap.display {
            Some(d) => (
                Some(d.units.as_str_lossy().into_owned()),
                Some(d.precision),
                Some((
                    d.upper_disp_limit,
                    d.lower_disp_limit,
                    d.upper_alarm_limit,
                    d.lower_alarm_limit,
                    d.upper_warning_limit,
                    d.lower_warning_limit,
                )),
            ),
            None => (None, None, None),
        };
        let enums = match &snap.enums {
            Some(e) => {
                let items = e
                    .strings
                    .iter()
                    .map(|s| value::pv_string_to_py(py, s))
                    .collect::<PyResult<Vec<_>>>()?;
                Some(PyList::new(py, items)?.unbind())
            }
            None => None,
        };
        Ok(Self {
            name: name.to_string(),
            datatype,
            element_count,
            status: snap.alarm.status,
            severity: snap.alarm.severity,
            raw_stamp: (snap.timestamp.unix_secs(), snap.timestamp.subsec_nanos()),
            units,
            precision,
            upper_disp_limit: disp.map(|d| d.0),
            lower_disp_limit: disp.map(|d| d.1),
            upper_alarm_limit: disp.map(|d| d.2),
            lower_alarm_limit: disp.map(|d| d.3),
            upper_warning_limit: disp.map(|d| d.4),
            lower_warning_limit: disp.map(|d| d.5),
            upper_ctrl_limit: snap.control.as_ref().map(|c| c.upper_ctrl_limit),
            lower_ctrl_limit: snap.control.as_ref().map(|c| c.lower_ctrl_limit),
            enums,
            ackt: snap.alarm.ackt,
            acks: snap.alarm.acks,
            value: value::to_py(py, snap.value)?,
        })
    }
}

#[pymethods]
impl Snapshot {
    /// Seconds since the Unix epoch, as `time.time()` would report it.
    #[getter]
    fn timestamp(&self) -> f64 {
        self.raw_stamp.0 as f64 + self.raw_stamp.1 as f64 * 1e-9
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!(
            "Snapshot(name={:?}, value={}, datatype={:?}, status={}, severity={})",
            self.name,
            self.value.bind(py).repr()?,
            self.datatype,
            self.status,
            self.severity
        ))
    }
}

// ---------------------------------------------------------------------------
// ChannelInfo
// ---------------------------------------------------------------------------

/// Channel-level facts known without a read.
#[pyclass(frozen, module = "epicsrs._epicsrs")]
pub struct ChannelInfo {
    #[pyo3(get)]
    name: String,
    #[pyo3(get)]
    host: String,
    #[pyo3(get)]
    datatype: &'static str,
    #[pyo3(get)]
    element_count: u32,
    #[pyo3(get)]
    read_access: bool,
    #[pyo3(get)]
    write_access: bool,
}

impl From<RsChannelInfo> for ChannelInfo {
    fn from(i: RsChannelInfo) -> Self {
        Self {
            name: i.pv_name,
            host: i.server_addr.to_string(),
            datatype: i.native_type.dbf_code().name(),
            element_count: i.element_count,
            read_access: i.access_rights.read,
            write_access: i.access_rights.write,
        }
    }
}

// ---------------------------------------------------------------------------
// CaContext
// ---------------------------------------------------------------------------

/// A CA client: search engine, virtual circuits and their channels.
///
/// Configuration comes from the `EPICS_CA_*` environment at construction.
#[pyclass(frozen, module = "epicsrs._epicsrs")]
pub struct CaContext {
    client: Arc<CaClient>,
}

#[pymethods]
impl CaContext {
    #[new]
    fn new(py: Python<'_>) -> PyResult<Self> {
        let client = block_on(py, CaClient::new()).map_err(map_ca)?;
        Ok(Self {
            client: Arc::new(client),
        })
    }

    /// Create a channel handle. The search starts immediately; use
    /// `wait_connected` before reading.
    fn channel(&self, name: &str) -> CaChannel {
        CaChannel {
            inner: self.client.create_channel(name),
            name: name.to_string(),
        }
    }

    /// Tear the client down. Channels created from it stop working.
    fn close(&self, py: Python<'_>) {
        block_on(py, self.client.shutdown());
    }

    /// Number of live IOC circuits.
    fn connection_count(&self, py: Python<'_>) -> usize {
        block_on(py, self.client.ioc_connection_count())
    }
}

// ---------------------------------------------------------------------------
// CaChannel
// ---------------------------------------------------------------------------

#[pyclass(frozen, module = "epicsrs._epicsrs")]
pub struct CaChannel {
    inner: RsChannel,
    name: String,
}

impl CaChannel {
    async fn do_get(
        ch: RsChannel,
        name: String,
        class: DbrClass,
        count: u32,
        timeout: Option<f64>,
    ) -> PyResult<Snapshot> {
        let snap = bounded(timeout, async {
            ch.get_with_metadata_count(class, count)
                .await
                .map_err(map_ca)
        })
        .await?;
        Python::attach(|py| Snapshot::from_rs(py, &name, snap))
    }

    async fn do_put(
        ch: RsChannel,
        req: PutRequest,
        wait: bool,
        timeout: Option<f64>,
    ) -> PyResult<()> {
        let dur = duration_or_forever(timeout);
        match (req, wait) {
            (PutRequest::Str(s), true) => {
                bounded(timeout, async { ch.put_string(&s).await.map_err(map_ca) }).await
            }
            (PutRequest::Str(s), false) => ch.put_string_nowait(&s).await.map_err(map_ca),
            (PutRequest::StrArray(a), true) => {
                bounded(timeout, async {
                    ch.put_string_array(&a).await.map_err(map_ca)
                })
                .await
            }
            (PutRequest::StrArray(a), false) => {
                ch.put_string_array_nowait(&a).await.map_err(map_ca)
            }
            (PutRequest::Value(v), true) => ch.put_with_timeout(&v, dur).await.map_err(map_ca),
            (PutRequest::Value(v), false) => ch.put_nowait(&v).await.map_err(map_ca),
        }
    }

    async fn do_wait_connected(ch: RsChannel, timeout: Option<f64>) -> PyResult<()> {
        ch.wait_connected(duration_or_forever(timeout))
            .await
            .map_err(map_ca)
    }

    async fn do_subscribe(
        ch: RsChannel,
        name: String,
        deadband: f64,
        mask: u16,
    ) -> PyResult<CaSubscription> {
        let handle = ch
            .subscribe_with_mask(deadband, mask)
            .await
            .map_err(map_ca)?;
        Ok(CaSubscription {
            handle: Arc::new(tokio::sync::Mutex::new(Some(handle))),
            closed: CancellationToken::new(),
            name,
        })
    }
}

#[pymethods]
impl CaChannel {
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    /// True once the channel has a live circuit.
    #[getter]
    fn connected(&self) -> bool {
        self.inner.native_field_type().is_ok()
    }

    /// Facts known once connected; raises `CaDisconnected` before that.
    fn info(&self, py: Python<'_>) -> PyResult<ChannelInfo> {
        block_on(py, self.inner.info())
            .map(ChannelInfo::from)
            .map_err(map_ca)
    }

    #[pyo3(signature = (timeout=None))]
    fn wait_connected(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<()> {
        block_on(py, Self::do_wait_connected(self.inner.clone(), timeout))
    }

    #[pyo3(signature = (timeout=None))]
    fn wait_connected_async<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, Self::do_wait_connected(self.inner.clone(), timeout))
    }

    /// Read the channel. `form` selects the DBR class (`plain`, `sts`,
    /// `time`, `gr`, `ctrl`); `count=0` reads the full native count.
    #[pyo3(signature = (form="time", count=0, timeout=None))]
    fn get(
        &self,
        py: Python<'_>,
        form: &str,
        count: u32,
        timeout: Option<f64>,
    ) -> PyResult<Snapshot> {
        let class = dbr_class(form)?;
        block_on(
            py,
            Self::do_get(self.inner.clone(), self.name.clone(), class, count, timeout),
        )
    }

    #[pyo3(signature = (form="time", count=0, timeout=None))]
    fn get_async<'py>(
        &self,
        py: Python<'py>,
        form: &str,
        count: u32,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let class = dbr_class(form)?;
        future_into_py(
            py,
            Self::do_get(self.inner.clone(), self.name.clone(), class, count, timeout),
        )
    }

    /// Write the channel. `wait=True` returns after the server has
    /// processed the record (`ca_put_callback`); `wait=False` returns once
    /// the request is on the wire.
    #[pyo3(signature = (value, wait=true, timeout=None))]
    fn put(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        wait: bool,
        timeout: Option<f64>,
    ) -> PyResult<()> {
        let req = value::from_py(value)?;
        block_on(py, Self::do_put(self.inner.clone(), req, wait, timeout))
    }

    #[pyo3(signature = (value, wait=true, timeout=None))]
    fn put_async<'py>(
        &self,
        py: Python<'py>,
        value: &Bound<'py, PyAny>,
        wait: bool,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let req = value::from_py(value)?;
        future_into_py(py, Self::do_put(self.inner.clone(), req, wait, timeout))
    }

    /// Subscribe. The subscription is drained with `CaSubscription.recv`.
    #[pyo3(signature = (deadband=0.0, mask=None))]
    fn subscribe(
        &self,
        py: Python<'_>,
        deadband: f64,
        mask: Option<u16>,
    ) -> PyResult<CaSubscription> {
        let mask = mask.unwrap_or(DBE_VALUE | DBE_LOG | DBE_ALARM);
        block_on(
            py,
            Self::do_subscribe(self.inner.clone(), self.name.clone(), deadband, mask),
        )
    }

    #[pyo3(signature = (deadband=0.0, mask=None))]
    fn subscribe_async<'py>(
        &self,
        py: Python<'py>,
        deadband: f64,
        mask: Option<u16>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mask = mask.unwrap_or(DBE_VALUE | DBE_LOG | DBE_ALARM);
        future_into_py(
            py,
            Self::do_subscribe(self.inner.clone(), self.name.clone(), deadband, mask),
        )
    }

    fn __repr__(&self) -> String {
        format!("CaChannel({:?}, connected={})", self.name, self.connected())
    }
}

// ---------------------------------------------------------------------------
// CaSubscription
// ---------------------------------------------------------------------------

/// A monitor. Python drains it by calling `recv`; nothing is delivered on
/// a runtime thread. Dropping or closing it unsubscribes.
///
/// `recv` holds the handle lock across its await, so `close` cannot take the
/// lock to abort while a `recv` is parked. The token is level-triggered: a
/// parked `recv` wakes and returns `None`, and every later `recv` sees it
/// before touching the lock, which is what lets `close` then take the lock.
#[pyclass(frozen, module = "epicsrs._epicsrs")]
pub struct CaSubscription {
    handle: Arc<tokio::sync::Mutex<Option<MonitorHandle>>>,
    closed: CancellationToken,
    name: String,
}

impl CaSubscription {
    async fn do_recv(
        handle: Arc<tokio::sync::Mutex<Option<MonitorHandle>>>,
        closed: CancellationToken,
        name: String,
        timeout: Option<f64>,
    ) -> PyResult<Option<Snapshot>> {
        let next = bounded(timeout, async {
            if closed.is_cancelled() {
                return Ok(None);
            }
            let mut guard = handle.lock().await;
            let Some(h) = guard.as_mut() else {
                return Ok(None);
            };
            tokio::select! {
                r = h.recv() => Ok(r),
                _ = closed.cancelled() => Ok(None),
            }
        })
        .await?;
        match next {
            None => Ok(None),
            Some(Err(e)) => Err(map_ca(e)),
            Some(Ok(snap)) => Python::attach(|py| Snapshot::from_rs(py, &name, snap).map(Some)),
        }
    }

    async fn do_close(
        handle: Arc<tokio::sync::Mutex<Option<MonitorHandle>>>,
        closed: CancellationToken,
    ) {
        closed.cancel();
        // Dropping a `MonitorHandle` sends the unsubscribe.
        drop(handle.lock().await.take());
    }

    async fn with_handle<R>(
        handle: Arc<tokio::sync::Mutex<Option<MonitorHandle>>>,
        f: impl FnOnce(&MonitorHandle) -> R,
    ) -> PyResult<R> {
        match handle.lock().await.as_ref() {
            Some(m) => Ok(f(m)),
            None => Err(CaError::new_err("subscription is closed")),
        }
    }
}

#[pymethods]
impl CaSubscription {
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    /// Next update, or `None` once the subscription is closed. With a
    /// `timeout`, raises `CaTimeout` if nothing arrives in time.
    #[pyo3(signature = (timeout=None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Snapshot>> {
        block_on(
            py,
            Self::do_recv(
                self.handle.clone(),
                self.closed.clone(),
                self.name.clone(),
                timeout,
            ),
        )
    }

    #[pyo3(signature = (timeout=None))]
    fn recv_async<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(
            py,
            Self::do_recv(
                self.handle.clone(),
                self.closed.clone(),
                self.name.clone(),
                timeout,
            ),
        )
    }

    fn pause(&self, py: Python<'_>) -> PyResult<()> {
        block_on(py, Self::with_handle(self.handle.clone(), |m| m.pause()))
    }

    fn resume(&self, py: Python<'_>) -> PyResult<()> {
        block_on(py, Self::with_handle(self.handle.clone(), |m| m.resume()))
    }

    /// Unsubscribe. A parked `recv` returns `None`.
    fn close(&self, py: Python<'_>) {
        block_on(py, Self::do_close(self.handle.clone(), self.closed.clone()));
    }

    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let (handle, closed) = (self.handle.clone(), self.closed.clone());
        future_into_py(py, async move {
            Self::do_close(handle, closed).await;
            Ok(())
        })
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) {
        self.close(py);
    }
}
