//! pvAccess client: `PvaContext`.
//!
//! Same two-flavour shape as `crate::ca`: every network method blocks with
//! the GIL released or, as `*_async`, returns an asyncio awaitable. Both
//! run on the one runtime in `crate::runtime`. Monitors are opened through
//! `super::hub::PvaMonitorHub`.

use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use epics_pva_rs::PvaError as RsPvaError;
use epics_pva_rs::client_native::ops_v2::{self, MarkedRead, PutLeaf};
use epics_pva_rs::client_native::{PvaClient, PvaClientBuilder};
use epics_pva_rs::config::parse_endpoints_with_port;
use epics_pva_rs::proto::BitSet;
use epics_pva_rs::pv_request::PvRequestExpr;
use epics_pva_rs::pvdata::encode::marked_changed_bitset;
use epics_pva_rs::pvdata::{FieldDesc, PvField, ScalarValue};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyList;

use super::error::{PvaError, bounded, map_pva};
use super::value::{Type, Value};
use crate::runtime::{block_on, into_py_future, join_all, results_to_py, results_to_py_owned};

/// Ops carry their own deadline (`bounded`), so the client's internal
/// op-timeout only has to be long enough never to fire first.
const INNER_TIMEOUT: Duration = Duration::from_secs(3600);

pub(super) fn parse_request(request: Option<&str>) -> PyResult<Option<PvRequestExpr>> {
    match request {
        None => Ok(None),
        Some(s) if s.trim().is_empty() => Ok(None),
        Some(s) => PvRequestExpr::parse(s)
            .map(Some)
            .map_err(|e| PyValueError::new_err(format!("bad pvRequest {s:?}: {e}"))),
    }
}

fn marks_for(desc: &FieldDesc, marked: Option<&[String]>) -> BitSet {
    match marked {
        None => {
            let mut all = BitSet::new();
            for b in 0..desc.total_bits() {
                all.set(b);
            }
            all
        }
        Some(paths) => marked_changed_bitset(desc, paths),
    }
}

fn read_to_value(read: MarkedRead) -> Value {
    let marks = marks_for(&read.desc, read.marked.as_deref());
    Value::from_parts(Arc::new(read.desc), read.value, marks)
}

fn yes(s: &str) -> bool {
    matches!(
        s.trim().to_ascii_lowercase().as_str(),
        "yes" | "y" | "1" | "true" | "on"
    )
}

fn port(conf: &HashMap<String, String>, key: &str) -> PyResult<Option<u16>> {
    match conf.get(key) {
        None => Ok(None),
        Some(v) if v.trim().is_empty() => Ok(None),
        Some(v) => v
            .trim()
            .parse::<u16>()
            .map(Some)
            .map_err(|e| PyValueError::new_err(format!("{key}={v:?}: {e}"))),
    }
}

/// Apply p4p-style `EPICS_PVA_*` settings from `conf` on top of `b`.
fn configure(
    mut b: PvaClientBuilder,
    conf: &HashMap<String, String>,
) -> PyResult<PvaClientBuilder> {
    let bcast = port(conf, "EPICS_PVA_BROADCAST_PORT")?;
    if let Some(p) = bcast {
        b = b.broadcast_port(p);
    }
    if let Some(p) = port(conf, "EPICS_PVA_SERVER_PORT")? {
        b = b.server_port(p);
    }
    if let Some(v) = conf.get("EPICS_PVA_AUTO_ADDR_LIST") {
        b = b.auto_addr_list(yes(v));
    }
    if let Some(v) = conf.get("EPICS_PVA_ADDR_LIST") {
        let default_port = bcast.unwrap_or(5076);
        b = b.addr_list(parse_endpoints_with_port(v, default_port));
    }
    if let Some(v) = conf.get("EPICS_PVA_NAME_SERVERS") {
        let default_port = port(conf, "EPICS_PVA_SERVER_PORT")?.unwrap_or(5075);
        let mut servers = Vec::new();
        for tok in v.split_whitespace() {
            let with_port = if tok.contains(':') {
                tok.to_string()
            } else {
                format!("{tok}:{default_port}")
            };
            match with_port.to_socket_addrs() {
                Ok(mut it) => servers.extend(it.next()),
                Err(e) => {
                    return Err(PyValueError::new_err(format!(
                        "EPICS_PVA_NAME_SERVERS entry {tok:?}: {e}"
                    )));
                }
            }
        }
        b = b.name_servers(servers);
    }
    Ok(b)
}

// ---------------------------------------------------------------------------
// PvaContext
// ---------------------------------------------------------------------------

/// A pvAccess client context.
#[pyclass(frozen, module = "repics._repics")]
pub struct PvaContext {
    client: Arc<PvaClient>,
}

impl PvaContext {
    pub(super) fn client(&self) -> Arc<PvaClient> {
        self.client.clone()
    }

    /// One [`Self::do_put`] per entry, all against one deadline set now.
    fn put_futs(
        &self,
        entries: Vec<(String, Option<String>, bool)>,
        build: Py<PyAny>,
        timeout: Option<f64>,
        connect_timeout: Option<f64>,
    ) -> PyResult<Vec<impl Future<Output = PyResult<Option<bool>>> + Send + 'static>> {
        let build = Arc::new(build);
        let bounds = PutBounds::new(timeout, connect_timeout);
        let mut futs = Vec::with_capacity(entries.len());
        for (index, (name, request, fetch)) in entries.into_iter().enumerate() {
            let req = parse_request(request.as_deref())?;
            let put = Self::do_put(
                self.client.clone(),
                name,
                req,
                fetch,
                build.clone(),
                index,
                bounds,
            );
            // `None` in the list, not an empty tuple
            futs.push(async move { put.await.map(|()| None) });
        }
        Ok(futs)
    }
}

impl PvaContext {
    async fn do_get(
        client: Arc<PvaClient>,
        name: String,
        request: Option<PvRequestExpr>,
        timeout: Option<f64>,
    ) -> PyResult<Value> {
        bounded(timeout, async {
            let read = match request {
                None => client.pvget_marked(&name).await,
                Some(req) => {
                    client
                        .pvget_pv_field_with_request_value_marked(&name, &req.to_pv_field())
                        .await
                }
            }
            .map_err(map_pva)?;
            Ok(read_to_value(read))
        })
        .await
    }

    async fn do_info(client: Arc<PvaClient>, name: String, timeout: Option<f64>) -> PyResult<Type> {
        bounded(timeout, async {
            let desc = client.pvinfo(&name).await.map_err(map_pva)?;
            Ok(Type::from_desc(Arc::new(desc)))
        })
        .await
    }

    async fn do_put_begin(
        client: Arc<PvaClient>,
        name: String,
        request: Option<PvRequestExpr>,
        fetch: bool,
        timeout: Option<f64>,
    ) -> PyResult<PutOp> {
        bounded(timeout, async {
            let mut op = client
                .pvput_begin(&name, request.as_ref(), fetch)
                .await
                .map_err(map_pva)?;
            Ok(PutOp {
                ty: op.introspection().clone(),
                present: Mutex::new(op.take_present()),
                inner: Mutex::new(Some(op)),
            })
        })
        .await
    }

    /// One put from open to commit; `build(index, op)` returns the `Value`
    /// to send through the opened op. A put whose circuit is lost before
    /// its commit completes is opened again while `bounds` has time left.
    async fn do_put(
        client: Arc<PvaClient>,
        name: String,
        request: Option<PvRequestExpr>,
        fetch: bool,
        build: Arc<Py<PyAny>>,
        index: usize,
        bounds: PutBounds,
    ) -> PyResult<()> {
        loop {
            let op = Self::do_put_begin(
                client.clone(),
                name.clone(),
                request.clone(),
                fetch,
                bounds.open(),
            )
            .await?;
            let (op, leaves) = build_put(&build, index, op)?;
            let sent = bounded(bounds.left(), async {
                Ok(op.commit_fields_typed(&leaves).await)
            })
            .await?;
            match sent {
                Ok(()) => return Ok(()),
                Err(RsPvaError::Disconnected) if !bounds.passed() => continue,
                Err(e) => return Err(map_pva(e)),
            }
        }
    }

    async fn do_rpc(
        client: Arc<PvaClient>,
        name: String,
        desc: Arc<FieldDesc>,
        field: PvField,
        request: Option<PvRequestExpr>,
        timeout: Option<f64>,
    ) -> PyResult<Option<Value>> {
        bounded(timeout, async {
            let reply = match request {
                None => client.pvrpc(&name, &desc, &field).await,
                Some(req) => {
                    client
                        .pvrpc_with_request(
                            &name,
                            &req.to_field_desc(),
                            &req.to_pv_field(),
                            &desc,
                            &field,
                        )
                        .await
                }
            }
            .map_err(map_pva)?;
            Ok(reply
                .into_value()
                .map(|(d, v)| Value::from_parts(Arc::new(d), v, BitSet::new())))
        })
        .await
    }

    async fn do_connect(
        client: Arc<PvaClient>,
        name: String,
        timeout: Option<f64>,
    ) -> PyResult<String> {
        bounded(timeout, async {
            client
                .pvconnect(&name)
                .await
                .map(|a| a.to_string())
                .map_err(map_pva)
        })
        .await
    }
}

#[pymethods]
impl PvaContext {
    /// `conf` holds `EPICS_PVA_*` settings that override the environment.
    #[new]
    #[pyo3(signature = (conf=None))]
    fn new(py: Python<'_>, conf: Option<HashMap<String, String>>) -> PyResult<Self> {
        let mut builder = PvaClientBuilder::new().timeout(INNER_TIMEOUT);
        if let Some(conf) = conf {
            builder = configure(builder, &conf)?;
        }
        // The pool captures the current reactor, so build on the runtime.
        let client = block_on(py, async move { builder.build() });
        Ok(PvaContext {
            client: Arc::new(client),
        })
    }

    #[pyo3(signature = (name, request=None, timeout=None))]
    fn get(
        &self,
        py: Python<'_>,
        name: String,
        request: Option<&str>,
        timeout: Option<f64>,
    ) -> PyResult<Value> {
        let req = parse_request(request)?;
        block_on(py, Self::do_get(self.client.clone(), name, req, timeout))
    }

    #[pyo3(signature = (name, request=None, timeout=None))]
    fn get_async<'py>(
        &self,
        py: Python<'py>,
        name: String,
        request: Option<&str>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let req = parse_request(request)?;
        into_py_future(py, Self::do_get(self.client.clone(), name, req, timeout))
    }

    /// Read every name concurrently, each with its own `request` (one per
    /// name) and the same `timeout`. One entry per name: a `Value` or the
    /// exception.
    #[pyo3(signature = (names, requests, timeout=None))]
    fn get_many<'py>(
        &self,
        py: Python<'py>,
        names: Vec<String>,
        requests: Vec<Option<String>>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyList>> {
        if names.len() != requests.len() {
            return Err(PyValueError::new_err(format!(
                "{} names need {} requests, got {}",
                names.len(),
                names.len(),
                requests.len()
            )));
        }
        let mut futs = Vec::with_capacity(names.len());
        for (name, request) in names.into_iter().zip(requests) {
            let req = parse_request(request.as_deref())?;
            futs.push(Self::do_get(self.client.clone(), name, req, timeout));
        }
        let results = block_on(py, join_all(futs, join_error));
        results_to_py(py, results)
    }

    /// Put every `(name, request, fetch)` entry concurrently, each on its
    /// own from open to commit, so a slow entry never eats into another's
    /// time. `fetch` reads the current value on the put's own operation;
    /// `build(index, op)` returns the `Value` to send. `timeout` bounds each
    /// put, `connect_timeout` each opening of it (connect and INIT, before
    /// any value is sent). One entry per name: `None` or the exception.
    #[pyo3(signature = (entries, build, timeout=None, connect_timeout=None))]
    fn put_many<'py>(
        &self,
        py: Python<'py>,
        entries: Vec<(String, Option<String>, bool)>,
        build: Py<PyAny>,
        timeout: Option<f64>,
        connect_timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyList>> {
        let futs = self.put_futs(entries, build, timeout, connect_timeout)?;
        let results = block_on(py, join_all(futs, join_error));
        results_to_py(py, results)
    }

    /// [`Self::put_many`] as an asyncio awaitable.
    #[pyo3(signature = (entries, build, timeout=None, connect_timeout=None))]
    fn put_many_async<'py>(
        &self,
        py: Python<'py>,
        entries: Vec<(String, Option<String>, bool)>,
        build: Py<PyAny>,
        timeout: Option<f64>,
        connect_timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let futs = self.put_futs(entries, build, timeout, connect_timeout)?;
        into_py_future(py, async move {
            results_to_py_owned(join_all(futs, join_error).await)
        })
    }

    #[pyo3(signature = (name, timeout=None))]
    fn info(&self, py: Python<'_>, name: String, timeout: Option<f64>) -> PyResult<Type> {
        block_on(py, Self::do_info(self.client.clone(), name, timeout))
    }

    #[pyo3(signature = (name, timeout=None))]
    fn info_async<'py>(
        &self,
        py: Python<'py>,
        name: String,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        into_py_future(py, Self::do_info(self.client.clone(), name, timeout))
    }

    /// Call `name` with `value` as the argument; `None` for an empty reply.
    #[pyo3(signature = (name, value, request=None, timeout=None))]
    fn rpc(
        &self,
        py: Python<'_>,
        name: String,
        value: &Value,
        request: Option<&str>,
        timeout: Option<f64>,
    ) -> PyResult<Option<Value>> {
        let (desc, field, _) = value.snapshot()?;
        let req = parse_request(request)?;
        block_on(
            py,
            Self::do_rpc(self.client.clone(), name, desc, field, req, timeout),
        )
    }

    #[pyo3(signature = (name, value, request=None, timeout=None))]
    fn rpc_async<'py>(
        &self,
        py: Python<'py>,
        name: String,
        value: &Value,
        request: Option<&str>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (desc, field, _) = value.snapshot()?;
        let req = parse_request(request)?;
        into_py_future(
            py,
            Self::do_rpc(self.client.clone(), name, desc, field, req, timeout),
        )
    }

    /// Wait until `name` is connected; returns the server address.
    #[pyo3(signature = (name, timeout=None))]
    fn connect(&self, py: Python<'_>, name: String, timeout: Option<f64>) -> PyResult<String> {
        block_on(py, Self::do_connect(self.client.clone(), name, timeout))
    }

    #[pyo3(signature = (name, timeout=None))]
    fn connect_async<'py>(
        &self,
        py: Python<'py>,
        name: String,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        into_py_future(py, Self::do_connect(self.client.clone(), name, timeout))
    }

    /// Tear down every channel. Live subscriptions see `disconnected`.
    fn close(&self, py: Python<'_>) {
        let client = self.client.clone();
        block_on(py, async move { client.close() });
    }
}

// ---------------------------------------------------------------------------
// PutOp
// ---------------------------------------------------------------------------

/// A put between its two phases, as `build` sees it: opened (the server's
/// type is known and, with `fetch`, the current value has been read on the
/// same operation) and not yet committed. The value `build` returns is
/// sent through it.
///
/// The circuit may be lost between the phases; the put is then opened
/// again (pvxs's non-autoExec rule, `clientget.cpp:380-404`).
#[pyclass(frozen, module = "repics._repics")]
pub struct PutOp {
    ty: Arc<FieldDesc>,
    present: Mutex<Option<(PvField, BitSet)>>,
    inner: Mutex<Option<ops_v2::PutOp>>,
}

impl PutOp {
    fn take(&self) -> PyResult<ops_v2::PutOp> {
        self.inner
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| PvaError::new_err("put already committed"))
    }
}

/// How long one put may still take: `timeout` from when the call began,
/// of which each opening gets at most `connect_timeout`.
#[derive(Clone, Copy)]
struct PutBounds {
    deadline: Option<Instant>,
    connect: Option<f64>,
}

impl PutBounds {
    fn new(timeout: Option<f64>, connect_timeout: Option<f64>) -> Self {
        // A timeout too large for an `Instant` is no deadline at all.
        let deadline = timeout
            .and_then(|t| Duration::try_from_secs_f64(t.max(0.0)).ok())
            .and_then(|d| Instant::now().checked_add(d));
        PutBounds {
            deadline,
            connect: connect_timeout,
        }
    }

    fn left(&self) -> Option<f64> {
        self.deadline
            .map(|d| d.saturating_duration_since(Instant::now()).as_secs_f64())
    }

    fn open(&self) -> Option<f64> {
        match (self.left(), self.connect) {
            (Some(left), Some(connect)) => Some(left.min(connect)),
            (left, connect) => left.or(connect),
        }
    }

    fn passed(&self) -> bool {
        self.deadline.is_some_and(|d| Instant::now() >= d)
    }
}

/// Call `build(index, op)` for the value to send and take the op back to
/// commit it. Attaches on the runtime worker, as the result conversion
/// does: a hop to a blocking thread costs about 60 us a put.
fn build_put(
    build: &Py<PyAny>,
    index: usize,
    op: PutOp,
) -> PyResult<(ops_v2::PutOp, Vec<(String, PutLeaf)>)> {
    // SAFETY: a plain flag read; an async put can outlive the interpreter's
    // shutdown wait.
    if unsafe { pyo3::ffi::Py_IsInitialized() } == 0 {
        return Err(PvaError::new_err("interpreter is shutting down"));
    }
    Python::attach(|py| {
        let op = Py::new(py, op)?;
        let value = build.call1(py, (index, op.clone_ref(py)))?;
        let value: PyRef<'_, Value> = value.bind(py).extract()?;
        let leaves = put_leaves(&value)?;
        Ok((op.get().take()?, leaves))
    })
}

#[pymethods]
impl PutOp {
    /// The server's type for the put.
    #[getter]
    fn r#type(&self) -> Type {
        Type::from_desc(self.ty.clone())
    }

    /// The current value read when the op was begun with `fetch`, handed
    /// out once; `None` afterwards or without `fetch`.
    fn current(&self) -> Option<Value> {
        self.present
            .lock()
            .unwrap()
            .take()
            .map(|(field, marks)| Value::from_parts(self.ty.clone(), field, marks))
    }
}

pub(super) fn queue_limit(req: Option<&PvRequestExpr>, explicit: Option<usize>) -> usize {
    if let Some(n) = explicit {
        return n.max(1);
    }
    let from_request = req.and_then(|r| {
        r.record_options
            .iter()
            .find(|(k, _)| k == "queueSize")
            .and_then(|(_, v)| match v {
                ScalarValue::Int(n) => Some(*n as usize),
                ScalarValue::Long(n) => Some(*n as usize),
                ScalarValue::UInt(n) => Some(*n as usize),
                ScalarValue::String(s) => s.as_str_lossy().parse::<usize>().ok(),
                _ => None,
            })
    });
    from_request.unwrap_or(4).max(1)
}

/// A runtime task that panicked, as the PVA family reports it.
fn join_error(e: tokio::task::JoinError) -> PyErr {
    PvaError::new_err(format!("task failed: {e}"))
}

fn put_leaves(value: &Value) -> PyResult<Vec<(String, PutLeaf)>> {
    let leaves = value.marked_leaves()?;
    if leaves.is_empty() {
        return Err(PvaError::new_err(
            "nothing to put: no field of the Value is marked changed",
        ));
    }
    Ok(leaves
        .into_iter()
        .map(|(p, f)| (p, PutLeaf::Typed(f)))
        .collect())
}
