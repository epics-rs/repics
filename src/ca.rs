//! Channel Access: `CaContext`, `CaChannel`, `CaSubscription`, `CaEvents`,
//! `Snapshot`, `ChannelInfo`.
//!
//! Every network operation exists in two flavours over one implementation:
//! the plain method blocks with the GIL released, the `*_async` method
//! returns an asyncio awaitable. Both run on the runtime in
//! [`crate::runtime`].
//!
//! The Rust classes are primitives; policy (default context, augmented
//! values, list shapes, `throw=False`) lives in the Python package.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use epics_base_rs::server::snapshot::Snapshot as RsSnapshot;
use epics_ca_rs::CaResult;
use epics_ca_rs::client::{
    CaChannel as RsChannel, CaClient, ChannelInfo as RsChannelInfo,
    ConnectionEvent as RsConnectionEvent, EnumReadback, MonitorHandle, ReqCount,
};
use epics_ca_rs::protocol::{DBE_ALARM, DBE_LOG, DBE_VALUE, ECA_INTERNAL};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::PyList;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

use crate::error::{CaError, map_ca, map_ca_write};
use crate::runtime::{block_on, bounded, into_py_future, runtime};
use crate::value::{self, PutRequest};

/// `None` means no deadline. tokio clamps far-future instants itself.
pub(crate) fn duration_or_forever(timeout: Option<f64>) -> Duration {
    match timeout {
        Some(secs) => Duration::from_secs_f64(secs.max(0.0)),
        None => Duration::from_secs(u32::MAX as u64),
    }
}

/// The element count a read asks for. `0` is CA autosize (the server sends
/// the record's current count), a negative count is the full native count,
/// and a positive count is clamped to the native count as libca's tools
/// do before issuing the request.
fn read_count(ch: &RsChannel, count: i64) -> CaResult<ReqCount> {
    Ok(match count {
        0 => ReqCount::Autosize(0),
        n if n < 0 => ReqCount::Fixed(0),
        n => ReqCount::Fixed(
            u32::try_from(n)
                .unwrap_or(u32::MAX)
                .min(ch.element_count()?),
        ),
    })
}

const DBR_STRING_BASE: u16 = 0;
const DBR_ENUM_BASE: u16 = 3;

/// What a read asks for, resolved against the channel's native type once
/// it is connected (see [`CaChannel::do_get`]).
#[derive(Clone, Copy)]
struct GetRequest {
    dbr: Option<u16>,
    form: u16,
    enum_as_string: bool,
}

/// Wait for the channel to connect. An already-connected channel returns
/// at once from its own state; only a pending one round-trips through the
/// engine's coordinator (`wait_connected` always does, which costs a
/// cross-thread hop per call).
async fn ensure_connected(ch: &RsChannel) -> PyResult<()> {
    if ch.native_field_type().is_ok() {
        return Ok(());
    }
    ch.wait_connected(duration_or_forever(None))
        .await
        .map_err(map_ca)
}

/// The element cap a monitor asks for, same convention as [`read_count`].
/// The client clamps a positive cap itself.
pub(crate) fn monitor_count(ch: &RsChannel, count: i64) -> CaResult<Option<u32>> {
    Ok(match count {
        0 => None,
        n if n < 0 => Some(ch.element_count()?),
        n => Some(u32::try_from(n).unwrap_or(u32::MAX)),
    })
}

/// Run every future concurrently on the runtime; results keep input order.
async fn join_all<T, F>(futs: Vec<F>) -> Vec<PyResult<T>>
where
    T: Send + 'static,
    F: Future<Output = PyResult<T>> + Send + 'static,
{
    let handles: Vec<_> = futs.into_iter().map(|f| runtime().spawn(f)).collect();
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(match h.await {
            Ok(r) => r,
            Err(e) => Err(CaError::new_err((
                format!("task failed: {e}"),
                ECA_INTERNAL,
            ))),
        });
    }
    out
}

/// A list of results where a failure is the exception object itself, so the
/// Python side can raise the first one (`throw=True`) or turn each into a
/// `CaNothing` (`throw=False`).
fn results_to_py<'py, T>(py: Python<'py>, results: Vec<PyResult<T>>) -> PyResult<Bound<'py, PyList>>
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
fn results_to_py_owned<T>(results: Vec<PyResult<T>>) -> PyResult<Py<PyList>>
where
    T: for<'a> IntoPyObject<'a>,
{
    Python::attach(|py| results_to_py(py, results).map(Bound::unbind))
}

// ---------------------------------------------------------------------------
// Drain: the one owner of "closed" for anything Python pulls from
// ---------------------------------------------------------------------------

/// A handle Python drains by repeated `recv`, plus the way to close it.
///
/// `recv` holds the slot lock across its await, so `close` cannot take the
/// lock to drop the handle while a `recv` is parked. The token is
/// level-triggered: `close` cancels it first, which wakes the parked `recv`
/// (it returns `None`), and every later `recv` sees it before touching the
/// lock. Only then does `close` take the lock and drop the handle.
pub(crate) struct Drain<T> {
    slot: Arc<tokio::sync::Mutex<Option<T>>>,
    closed: CancellationToken,
}

impl<T> Clone for Drain<T> {
    fn clone(&self) -> Self {
        Self {
            slot: self.slot.clone(),
            closed: self.closed.clone(),
        }
    }
}

type Pulled<'a, R> = Pin<Box<dyn Future<Output = Option<R>> + Send + 'a>>;

impl<T: Send> Drain<T> {
    pub(crate) fn new(t: T) -> Self {
        Self {
            slot: Arc::new(tokio::sync::Mutex::new(Some(t))),
            closed: CancellationToken::new(),
        }
    }

    /// Run `f` on the handle. `None` once closed, including when `close`
    /// interrupts a parked `f`.
    pub(crate) async fn pull<R, F>(&self, f: F) -> Option<R>
    where
        F: for<'a> FnOnce(&'a mut T) -> Pulled<'a, R>,
    {
        if self.closed.is_cancelled() {
            return None;
        }
        let mut guard = self.slot.lock().await;
        let t = guard.as_mut()?;
        tokio::select! {
            r = f(t) => r,
            _ = self.closed.cancelled() => None,
        }
    }

    /// Borrow the handle without waiting on it.
    async fn with<R>(&self, f: impl FnOnce(&T) -> R) -> PyResult<R> {
        match self.slot.lock().await.as_ref() {
            Some(t) => Ok(f(t)),
            None => Err(CaError::new_err(("closed", ECA_INTERNAL))),
        }
    }

    pub(crate) async fn close(&self) {
        self.closed.cancel();
        drop(self.slot.lock().await.take());
    }
}

/// Poll `fut` exactly once. `Some` if it was already ready.
fn poll_once<F: Future>(fut: F) -> Option<F::Output> {
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(r) => Some(r),
        Poll::Pending => None,
    }
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// The CA wire base types by code, spelled as `caget -a` and pyepics do.
const DBR_NAMES: [&str; 7] = ["string", "short", "float", "enum", "char", "long", "double"];

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
    /// The CA wire type code (0..=6) of the payload as delivered.
    #[pyo3(get)]
    dbr: u16,
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
    pub(crate) fn from_rs(py: Python<'_>, name: &str, snap: RsSnapshot) -> PyResult<Self> {
        let dbr = snap.value.db_field_type().ca_wire_type();
        let datatype = DBR_NAMES[usize::from(dbr)];
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
            dbr,
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
    /// The native CA wire type code (0..=6).
    #[pyo3(get)]
    dbr: u16,
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
            dbr: i.native_type.ca_wire_type(),
            element_count: i.element_count,
            read_access: i.access_rights.read,
            write_access: i.access_rights.write,
        }
    }
}

// ---------------------------------------------------------------------------
// ConnectionEvent / CaEvents
// ---------------------------------------------------------------------------

/// One channel lifecycle event: `kind` is `"connected"`, `"disconnected"`,
/// `"access_rights"` (with `read`/`write`), `"type_changed"` (with `dbr`,
/// the new native wire type) or `"lagged"` (the receiver fell behind; poll
/// `CaChannel.info` for the current state).
#[pyclass(frozen, module = "epicsrs._epicsrs")]
pub struct ConnectionEvent {
    #[pyo3(get)]
    kind: &'static str,
    #[pyo3(get)]
    read: Option<bool>,
    #[pyo3(get)]
    write: Option<bool>,
    #[pyo3(get)]
    dbr: Option<u16>,
}

impl ConnectionEvent {
    fn from_rs(r: Result<RsConnectionEvent, broadcast::error::RecvError>) -> Option<Self> {
        let (kind, read, write, dbr) = match r {
            Ok(RsConnectionEvent::Connected) => ("connected", None, None, None),
            Ok(RsConnectionEvent::Disconnected) => ("disconnected", None, None, None),
            Ok(RsConnectionEvent::AccessRightsChanged { read, write }) => {
                ("access_rights", Some(read), Some(write), None)
            }
            Ok(RsConnectionEvent::NativeTypeChanged { current, .. }) => {
                ("type_changed", None, None, Some(current.ca_wire_type()))
            }
            Err(broadcast::error::RecvError::Lagged(_)) => ("lagged", None, None, None),
            Err(broadcast::error::RecvError::Closed) => return None,
        };
        Some(Self {
            kind,
            read,
            write,
            dbr,
        })
    }
}

#[pymethods]
impl ConnectionEvent {
    fn __repr__(&self) -> String {
        match (self.read, self.write, self.dbr) {
            (Some(r), Some(w), _) => {
                format!("ConnectionEvent({:?}, read={r}, write={w})", self.kind)
            }
            (_, _, Some(t)) => format!("ConnectionEvent({:?}, dbr={t})", self.kind),
            _ => format!("ConnectionEvent({:?})", self.kind),
        }
    }
}

/// A channel's lifecycle event stream, drained with `recv`.
#[pyclass(frozen, module = "epicsrs._epicsrs")]
pub struct CaEvents {
    drain: Drain<broadcast::Receiver<RsConnectionEvent>>,
}

impl CaEvents {
    async fn do_recv(
        drain: Drain<broadcast::Receiver<RsConnectionEvent>>,
        timeout: Option<f64>,
    ) -> PyResult<Option<ConnectionEvent>> {
        bounded(timeout, async {
            Ok(drain
                .pull(|rx| Box::pin(async { ConnectionEvent::from_rs(rx.recv().await) }))
                .await)
        })
        .await
    }
}

#[pymethods]
impl CaEvents {
    /// Next event, or `None` once closed. With a `timeout`, raises
    /// `CaTimeout` if nothing arrives in time.
    #[pyo3(signature = (timeout=None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<ConnectionEvent>> {
        block_on(py, Self::do_recv(self.drain.clone(), timeout))
    }

    #[pyo3(signature = (timeout=None))]
    fn recv_async<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        into_py_future(py, Self::do_recv(self.drain.clone(), timeout))
    }

    fn close(&self, py: Python<'_>) {
        block_on(py, self.drain.close());
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

    /// Wait for every channel concurrently. One entry per channel: `None`
    /// on success, else the exception.
    #[pyo3(signature = (channels, timeout=None))]
    fn wait_connected_many<'py>(
        &self,
        py: Python<'py>,
        channels: Vec<PyRef<'py, CaChannel>>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyList>> {
        let results = block_on(py, join_all(Self::wait_futs(&channels, timeout)));
        results_to_py(py, results)
    }

    #[pyo3(signature = (channels, timeout=None))]
    fn wait_connected_many_async<'py>(
        &self,
        py: Python<'py>,
        channels: Vec<PyRef<'py, CaChannel>>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let futs = Self::wait_futs(&channels, timeout);
        into_py_future(py, async move { results_to_py_owned(join_all(futs).await) })
    }

    /// Read every channel concurrently, each connecting first, with the
    /// arguments of `CaChannel.get`. One entry per channel: a `Snapshot`
    /// or the exception.
    #[pyo3(signature = (channels, dbr=None, form=0, enum_as_string=false, count=0, timeout=None))]
    #[allow(clippy::too_many_arguments)]
    fn get_many<'py>(
        &self,
        py: Python<'py>,
        channels: Vec<PyRef<'py, CaChannel>>,
        dbr: Option<u16>,
        form: u16,
        enum_as_string: bool,
        count: i64,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyList>> {
        let req = GetRequest {
            dbr,
            form,
            enum_as_string,
        };
        let results = block_on(py, join_all(Self::get_futs(&channels, req, count, timeout)));
        results_to_py(py, results)
    }

    #[pyo3(signature = (channels, dbr=None, form=0, enum_as_string=false, count=0, timeout=None))]
    #[allow(clippy::too_many_arguments)]
    fn get_many_async<'py>(
        &self,
        py: Python<'py>,
        channels: Vec<PyRef<'py, CaChannel>>,
        dbr: Option<u16>,
        form: u16,
        enum_as_string: bool,
        count: i64,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let req = GetRequest {
            dbr,
            form,
            enum_as_string,
        };
        let futs = Self::get_futs(&channels, req, count, timeout);
        into_py_future(py, async move { results_to_py_owned(join_all(futs).await) })
    }

    /// Write every channel concurrently. One entry per channel: `None` on
    /// success, else the exception.
    #[pyo3(signature = (channels, values, wait=true, timeout=None))]
    fn put_many<'py>(
        &self,
        py: Python<'py>,
        channels: Vec<PyRef<'py, CaChannel>>,
        values: Vec<Bound<'py, PyAny>>,
        wait: bool,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyList>> {
        let futs = Self::put_futs(&channels, &values, wait, timeout)?;
        let results = block_on(py, join_all(futs));
        results_to_py(py, results)
    }

    #[pyo3(signature = (channels, values, wait=true, timeout=None))]
    fn put_many_async<'py>(
        &self,
        py: Python<'py>,
        channels: Vec<PyRef<'py, CaChannel>>,
        values: Vec<Bound<'py, PyAny>>,
        wait: bool,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let futs = Self::put_futs(&channels, &values, wait, timeout)?;
        into_py_future(py, async move { results_to_py_owned(join_all(futs).await) })
    }
}

/// A unit outcome as Python `None` (a bare `()` would become an empty
/// tuple), so a many-op list reads `None` on success, an exception else.
async fn none_on_success<F: Future<Output = PyResult<()>>>(f: F) -> PyResult<Option<bool>> {
    f.await.map(|()| None)
}

impl CaContext {
    fn wait_futs(
        channels: &[PyRef<'_, CaChannel>],
        timeout: Option<f64>,
    ) -> Vec<impl Future<Output = PyResult<Option<bool>>> + Send + 'static + use<>> {
        channels
            .iter()
            .map(|c| none_on_success(CaChannel::do_wait_connected(c.inner.clone(), timeout)))
            .collect()
    }

    fn get_futs(
        channels: &[PyRef<'_, CaChannel>],
        req: GetRequest,
        count: i64,
        timeout: Option<f64>,
    ) -> Vec<impl Future<Output = PyResult<Snapshot>> + Send + 'static + use<>> {
        channels
            .iter()
            .map(|c| CaChannel::do_get(c.inner.clone(), c.name.clone(), req, count, timeout))
            .collect()
    }

    fn put_futs(
        channels: &[PyRef<'_, CaChannel>],
        values: &[Bound<'_, PyAny>],
        wait: bool,
        timeout: Option<f64>,
    ) -> PyResult<Vec<impl Future<Output = PyResult<Option<bool>>> + Send + 'static + use<>>> {
        if channels.len() != values.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "{} channels but {} values",
                channels.len(),
                values.len()
            )));
        }
        channels
            .iter()
            .zip(values)
            .map(|(c, v)| {
                let req = value::from_py(v)?;
                Ok(none_on_success(CaChannel::do_put(
                    c.inner.clone(),
                    req,
                    wait,
                    timeout,
                )))
            })
            .collect()
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
    /// What a subscription listens for unless told otherwise: what
    /// `camonitor` asks for.
    pub(crate) const DEFAULT_MASK: u16 = DBE_VALUE | DBE_LOG | DBE_ALARM;

    pub(crate) fn inner(&self) -> &RsChannel {
        &self.inner
    }

    pub(crate) fn pv_name(&self) -> &str {
        &self.name
    }

    /// The wire DBR type a read asks for: `dbr` (a base code 0..=6) or the
    /// channel's native base, plus the `form` class offset (0, 7, 14, 21,
    /// 28). `enum_as_string` reads an ENUM channel as DBR_STRING. Waits
    /// for the connection first; `timeout` bounds connect and read.
    async fn do_get(
        ch: RsChannel,
        name: String,
        req: GetRequest,
        count: i64,
        timeout: Option<f64>,
    ) -> PyResult<Snapshot> {
        let snap = bounded(timeout, async {
            ensure_connected(&ch).await?;
            let native = ch.native_field_type().map_err(map_ca)?.ca_wire_type();
            let base = match req.dbr {
                Some(b) => b,
                None if req.enum_as_string && native == DBR_ENUM_BASE => DBR_STRING_BASE,
                None => native,
            };
            let count = read_count(&ch, count).map_err(map_ca)?;
            ch.get_with_dbr_type(base + req.form, count)
                .await
                .map_err(map_ca)
        })
        .await?;
        Python::attach(|py| Snapshot::from_rs(py, &name, snap))
    }

    /// Waits for the connection first; `timeout` bounds connect and write.
    async fn do_put(
        ch: RsChannel,
        req: PutRequest,
        wait: bool,
        timeout: Option<f64>,
    ) -> PyResult<()> {
        bounded(timeout, async {
            ensure_connected(&ch).await?;
            match (req, wait) {
                (PutRequest::Str(s), true) => ch.put_string(&s).await,
                (PutRequest::Str(s), false) => ch.put_string_nowait(&s).await,
                (PutRequest::StrArray(a), true) => ch.put_string_array(&a).await,
                (PutRequest::StrArray(a), false) => ch.put_string_array_nowait(&a).await,
                (PutRequest::Value(v), true) => {
                    ch.put_with_timeout(&v, duration_or_forever(None)).await
                }
                (PutRequest::Value(v), false) => ch.put_nowait(&v).await,
            }
            .map_err(map_ca_write)
        })
        .await
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
        enum_as_string: bool,
        float_as_string: bool,
        count: i64,
    ) -> PyResult<CaSubscription> {
        let readback = if enum_as_string {
            EnumReadback::Label
        } else {
            EnumReadback::Native
        };
        let cap = monitor_count(&ch, count).map_err(map_ca)?;
        let handle = ch
            .subscribe_with_mask_readback_count(deadband, mask, readback, float_as_string, cap)
            .await
            .map_err(map_ca)?;
        Ok(CaSubscription {
            drain: Drain::new(Monitor {
                handle,
                stashed: None,
            }),
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

    /// The native CA wire type code (0..=6), or `None` before connection.
    #[getter]
    fn dbr(&self) -> Option<u16> {
        self.inner
            .native_field_type()
            .ok()
            .map(|t| t.ca_wire_type())
    }

    /// The native element count, or `None` before connection.
    #[getter]
    fn element_count(&self) -> Option<u32> {
        self.inner.element_count().ok()
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
        into_py_future(py, Self::do_wait_connected(self.inner.clone(), timeout))
    }

    /// Read the channel, connecting first. `dbr` is a base DBR code
    /// (0..=6) or `None` for the native type; `form` the class offset (0
    /// plain, 7 sts, 14 time, 21 gr, 28 ctrl); `enum_as_string` reads an
    /// ENUM as its label. `count`: 0 autosize, negative full native
    /// count, positive a cap. `timeout` bounds connect and read.
    #[pyo3(signature = (dbr=None, form=0, enum_as_string=false, count=0, timeout=None))]
    fn get(
        &self,
        py: Python<'_>,
        dbr: Option<u16>,
        form: u16,
        enum_as_string: bool,
        count: i64,
        timeout: Option<f64>,
    ) -> PyResult<Snapshot> {
        let req = GetRequest {
            dbr,
            form,
            enum_as_string,
        };
        block_on(
            py,
            Self::do_get(self.inner.clone(), self.name.clone(), req, count, timeout),
        )
    }

    #[pyo3(signature = (dbr=None, form=0, enum_as_string=false, count=0, timeout=None))]
    fn get_async<'py>(
        &self,
        py: Python<'py>,
        dbr: Option<u16>,
        form: u16,
        enum_as_string: bool,
        count: i64,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let req = GetRequest {
            dbr,
            form,
            enum_as_string,
        };
        into_py_future(
            py,
            Self::do_get(self.inner.clone(), self.name.clone(), req, count, timeout),
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
        into_py_future(py, Self::do_put(self.inner.clone(), req, wait, timeout))
    }

    /// Subscribe. The subscription is drained with `CaSubscription.recv`.
    /// Updates are the TIME class of the native type; `enum_as_string`
    /// monitors an ENUM channel as its state label, `float_as_string` a
    /// FLOAT/DOUBLE channel as the server's rendering at record precision;
    /// `count` follows `get`.
    #[pyo3(signature = (deadband=0.0, mask=None, enum_as_string=false, float_as_string=false, count=0))]
    fn subscribe(
        &self,
        py: Python<'_>,
        deadband: f64,
        mask: Option<u16>,
        enum_as_string: bool,
        float_as_string: bool,
        count: i64,
    ) -> PyResult<CaSubscription> {
        let mask = mask.unwrap_or(Self::DEFAULT_MASK);
        block_on(
            py,
            Self::do_subscribe(
                self.inner.clone(),
                self.name.clone(),
                deadband,
                mask,
                enum_as_string,
                float_as_string,
                count,
            ),
        )
    }

    #[pyo3(signature = (deadband=0.0, mask=None, enum_as_string=false, float_as_string=false, count=0))]
    fn subscribe_async<'py>(
        &self,
        py: Python<'py>,
        deadband: f64,
        mask: Option<u16>,
        enum_as_string: bool,
        float_as_string: bool,
        count: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mask = mask.unwrap_or(Self::DEFAULT_MASK);
        into_py_future(
            py,
            Self::do_subscribe(
                self.inner.clone(),
                self.name.clone(),
                deadband,
                mask,
                enum_as_string,
                float_as_string,
                count,
            ),
        )
    }

    /// The channel's lifecycle events from now on.
    fn events(&self) -> CaEvents {
        CaEvents {
            drain: Drain::new(self.inner.connection_events()),
        }
    }

    fn __repr__(&self) -> String {
        format!("CaChannel({:?}, connected={})", self.name, self.connected())
    }
}

// ---------------------------------------------------------------------------
// CaSubscription
// ---------------------------------------------------------------------------

/// The monitor handle plus the one item `recv_batch` looked at but must
/// hand out in order on the next call (an error, or the end of the stream).
struct Monitor {
    handle: MonitorHandle,
    stashed: Option<Option<CaResult<RsSnapshot>>>,
}

impl Monitor {
    async fn next(&mut self) -> Option<CaResult<RsSnapshot>> {
        match self.stashed.take() {
            Some(item) => item,
            None => self.handle.recv().await,
        }
    }

    /// The next item if it is already there.
    fn try_next(&mut self) -> Option<Option<CaResult<RsSnapshot>>> {
        if let Some(item) = self.stashed.take() {
            return Some(item);
        }
        poll_once(self.handle.recv())
    }
}

/// A monitor. Python drains it by calling `recv`; nothing is delivered on
/// a runtime thread. Dropping or closing it unsubscribes.
#[pyclass(frozen, module = "epicsrs._epicsrs")]
pub struct CaSubscription {
    drain: Drain<Monitor>,
    name: String,
}

impl CaSubscription {
    fn convert(name: &str, item: Option<CaResult<RsSnapshot>>) -> PyResult<Option<Snapshot>> {
        match item {
            None => Ok(None),
            Some(Err(e)) => Err(map_ca(e)),
            Some(Ok(snap)) => Python::attach(|py| Snapshot::from_rs(py, name, snap).map(Some)),
        }
    }

    async fn do_recv(
        drain: Drain<Monitor>,
        name: String,
        timeout: Option<f64>,
    ) -> PyResult<Option<Snapshot>> {
        let item = bounded(timeout, async {
            Ok(drain.pull(|m| Box::pin(m.next())).await)
        })
        .await?;
        Self::convert(&name, item)
    }

    /// Like `recv`, but hands back every update already queued behind the
    /// first one as well, oldest first, at most `max_items` of them (0 for
    /// no cap). An error or the end of the stream is never folded into a
    /// batch: it is held for the next call. `None` once the subscription
    /// is closed.
    async fn do_recv_batch(
        drain: Drain<Monitor>,
        name: String,
        max_items: usize,
        timeout: Option<f64>,
    ) -> PyResult<Option<Vec<Snapshot>>> {
        let cap = if max_items == 0 {
            usize::MAX
        } else {
            max_items
        };
        let batch = bounded(timeout, async {
            let r = drain
                .pull(|m| {
                    Box::pin(async move {
                        let first = match m.next().await {
                            None => return Some(Ok(None)),
                            Some(Err(e)) => return Some(Err(e)),
                            Some(Ok(v)) => v,
                        };
                        let mut items = vec![first];
                        while items.len() < cap {
                            match m.try_next() {
                                None => break,
                                Some(Some(Ok(v))) => items.push(v),
                                Some(other) => {
                                    m.stashed = Some(other);
                                    break;
                                }
                            }
                        }
                        Some(Ok(Some(items)))
                    })
                })
                .await;
            Ok(r.unwrap_or(Ok(None)))
        })
        .await?;
        match batch {
            Err(e) => Err(map_ca(e)),
            Ok(None) => Ok(None),
            Ok(Some(items)) => Python::attach(|py| {
                items
                    .into_iter()
                    .map(|snap| Snapshot::from_rs(py, &name, snap))
                    .collect::<PyResult<Vec<_>>>()
                    .map(Some)
            }),
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
    /// `timeout`, raises `CaTimeout` if nothing arrives in time. A
    /// disconnect arrives as a raised `CaDisconnected`; the subscription
    /// stays open and resumes after reconnection.
    #[pyo3(signature = (timeout=None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Snapshot>> {
        block_on(
            py,
            Self::do_recv(self.drain.clone(), self.name.clone(), timeout),
        )
    }

    #[pyo3(signature = (timeout=None))]
    fn recv_async<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        into_py_future(
            py,
            Self::do_recv(self.drain.clone(), self.name.clone(), timeout),
        )
    }

    /// Next update plus every update already queued behind it, oldest
    /// first, at most `max_items` (0 for no cap). `None` once closed. One
    /// GIL round trip per batch instead of one per update.
    #[pyo3(signature = (max_items=0, timeout=None))]
    fn recv_batch(
        &self,
        py: Python<'_>,
        max_items: usize,
        timeout: Option<f64>,
    ) -> PyResult<Option<Vec<Snapshot>>> {
        block_on(
            py,
            Self::do_recv_batch(self.drain.clone(), self.name.clone(), max_items, timeout),
        )
    }

    #[pyo3(signature = (max_items=0, timeout=None))]
    fn recv_batch_async<'py>(
        &self,
        py: Python<'py>,
        max_items: usize,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        into_py_future(
            py,
            Self::do_recv_batch(self.drain.clone(), self.name.clone(), max_items, timeout),
        )
    }

    fn pause(&self, py: Python<'_>) -> PyResult<()> {
        block_on(py, self.drain.with(|m| m.handle.pause()))
    }

    fn resume(&self, py: Python<'_>) -> PyResult<()> {
        block_on(py, self.drain.with(|m| m.handle.resume()))
    }

    /// Unsubscribe. A parked `recv` returns `None`.
    fn close(&self, py: Python<'_>) {
        block_on(py, self.drain.close());
    }

    fn close_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let drain = self.drain.clone();
        into_py_future(py, async move {
            drain.close().await;
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
