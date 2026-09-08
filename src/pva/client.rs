//! pvAccess client: `PvaContext` and `PvaSubscription`.
//!
//! Same two-flavour shape as `crate::ca`: every network method blocks with
//! the GIL released or, as `*_async`, returns an asyncio awaitable. Both
//! run on the one runtime in `crate::runtime`.
//!
//! A monitor's wire callback runs on a runtime worker, so it never touches
//! Python: it decodes the frame, merges it against the previous value and
//! pushes it onto a bounded queue that Python drains through `recv`. When
//! the queue is full the newest update is squashed into the tail — newer
//! values win, changed sets are unioned — which is the pvxs client rule;
//! memory is bounded no matter how slowly Python drains.

use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use epics_pva_rs::client_native::ops_v2::{
    MarkedRead, MonitorConnEvent, PutLeaf, SubscriptionHandle,
};
use epics_pva_rs::client_native::{PvaClient, PvaClientBuilder};
use epics_pva_rs::config::Endpoint;
use epics_pva_rs::proto::{BitSet, ByteOrder};
use epics_pva_rs::pv_request::PvRequestExpr;
use epics_pva_rs::pvdata::encode::{
    decode_pv_field_with_bitset, fill_unmarked_from_prior, marked_changed_bitset,
};
use epics_pva_rs::pvdata::{FieldDesc, PvField, ScalarValue};
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use tokio_util::sync::CancellationToken;

use super::error::{PvaError, bounded, map_pva};
use super::value::{Type, Value};
use crate::runtime::block_on;

/// Ops carry their own deadline (`bounded`), so the client's internal
/// op-timeout only has to be long enough never to fire first.
const INNER_TIMEOUT: Duration = Duration::from_secs(3600);

fn parse_request(request: Option<&str>) -> PyResult<Option<PvRequestExpr>> {
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
        let list: Vec<Endpoint> = v
            .split_whitespace()
            .filter_map(|tok| Endpoint::parse(tok, default_port))
            .collect();
        b = b.addr_list(list);
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
#[pyclass(frozen, module = "epicsrs._epicsrs")]
pub struct PvaContext {
    client: Arc<PvaClient>,
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

    async fn do_put(
        client: Arc<PvaClient>,
        name: String,
        leaves: Vec<(String, PutLeaf)>,
        request: Option<PvRequestExpr>,
        timeout: Option<f64>,
    ) -> PyResult<()> {
        bounded(timeout, async {
            client
                .pvput_fields_typed(&name, &leaves, request.as_ref())
                .await
                .map_err(map_pva)
        })
        .await
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

    async fn do_monitor(
        client: Arc<PvaClient>,
        name: String,
        request: Option<PvRequestExpr>,
        limit: usize,
    ) -> PyResult<PvaSubscription> {
        let shared = Arc::new(Shared {
            queue: Mutex::new(Queue {
                items: VecDeque::new(),
                limit,
            }),
            notify: tokio::sync::Notify::new(),
        });
        let pv_request = request
            .unwrap_or_else(|| PvRequestExpr::parse("field()").expect("field() parses"))
            .to_pv_field();
        let mut decoder = Decoder::default();
        let producer = shared.clone();
        let conn = shared.clone();
        let handle = client
            .pvmonitor_raw_frames_handle_with_request(
                &name,
                pv_request,
                move |desc: &FieldDesc, body, order: ByteOrder| {
                    if let Some(u) = decoder.decode(desc, body.as_ref(), order) {
                        producer.push_value(u);
                    }
                },
                move |ev: MonitorConnEvent| {
                    conn.push(match ev {
                        MonitorConnEvent::Connected { peer } => Update::Connected(peer),
                        MonitorConnEvent::Disconnected => Update::Disconnected,
                        MonitorConnEvent::Finished => Update::Finished,
                    });
                },
            )
            .await
            .map_err(map_pva)?;
        Ok(PvaSubscription {
            handle: Arc::new(tokio::sync::Mutex::new(Some(handle))),
            shared,
            closed: CancellationToken::new(),
            name,
        })
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
        future_into_py(py, Self::do_get(self.client.clone(), name, req, timeout))
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
        future_into_py(py, Self::do_info(self.client.clone(), name, timeout))
    }

    /// Send the marked fields of `value`.
    #[pyo3(signature = (name, value, request=None, timeout=None))]
    fn put(
        &self,
        py: Python<'_>,
        name: String,
        value: &Value,
        request: Option<&str>,
        timeout: Option<f64>,
    ) -> PyResult<()> {
        let leaves = put_leaves(value)?;
        let req = parse_request(request)?;
        block_on(
            py,
            Self::do_put(self.client.clone(), name, leaves, req, timeout),
        )
    }

    #[pyo3(signature = (name, value, request=None, timeout=None))]
    fn put_async<'py>(
        &self,
        py: Python<'py>,
        name: String,
        value: &Value,
        request: Option<&str>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let leaves = put_leaves(value)?;
        let req = parse_request(request)?;
        future_into_py(
            py,
            Self::do_put(self.client.clone(), name, leaves, req, timeout),
        )
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
        future_into_py(
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
        future_into_py(py, Self::do_connect(self.client.clone(), name, timeout))
    }

    /// Subscribe. `limit` bounds the update queue Python drains through
    /// `PvaSubscription.recv`; it defaults to the request's `queueSize`
    /// record option, else 4.
    #[pyo3(signature = (name, request=None, limit=None))]
    fn monitor(
        &self,
        py: Python<'_>,
        name: String,
        request: Option<&str>,
        limit: Option<usize>,
    ) -> PyResult<PvaSubscription> {
        let req = parse_request(request)?;
        let limit = queue_limit(req.as_ref(), limit);
        block_on(py, Self::do_monitor(self.client.clone(), name, req, limit))
    }

    #[pyo3(signature = (name, request=None, limit=None))]
    fn monitor_async<'py>(
        &self,
        py: Python<'py>,
        name: String,
        request: Option<&str>,
        limit: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let req = parse_request(request)?;
        let limit = queue_limit(req.as_ref(), limit);
        future_into_py(py, Self::do_monitor(self.client.clone(), name, req, limit))
    }

    /// Tear down every channel. Live subscriptions see `disconnected`.
    fn close(&self, py: Python<'_>) {
        let client = self.client.clone();
        block_on(py, async move { client.close() });
    }
}

fn queue_limit(req: Option<&PvRequestExpr>, explicit: Option<usize>) -> usize {
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

// ---------------------------------------------------------------------------
// Monitor queue
// ---------------------------------------------------------------------------

enum Update {
    Value {
        desc: Arc<FieldDesc>,
        value: PvField,
        changed: BitSet,
    },
    Connected(SocketAddr),
    Disconnected,
    Finished,
}

struct Queue {
    items: VecDeque<Update>,
    limit: usize,
}

struct Shared {
    queue: Mutex<Queue>,
    notify: tokio::sync::Notify,
}

impl Shared {
    fn push(&self, u: Update) {
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .items
            .push_back(u);
        self.notify.notify_one();
    }

    /// Squash into the tail when the queue holds `limit` value updates
    /// and the tail is a value: newer wins, changed sets union.
    fn push_value(&self, u: Update) {
        let Update::Value {
            desc,
            value,
            changed,
        } = u
        else {
            return self.push(u);
        };
        let mut q = self.queue.lock().unwrap_or_else(|e| e.into_inner());
        let values = q
            .items
            .iter()
            .filter(|i| matches!(i, Update::Value { .. }))
            .count();
        if values >= q.limit {
            if let Some(Update::Value {
                desc: tdesc,
                value: tvalue,
                changed: tchanged,
            }) = q.items.back_mut()
            {
                *tdesc = desc;
                *tvalue = value;
                for b in changed.iter() {
                    tchanged.set(b);
                }
                drop(q);
                self.notify.notify_one();
                return;
            }
        }
        q.items.push_back(Update::Value {
            desc,
            value,
            changed,
        });
        drop(q);
        self.notify.notify_one();
    }

    fn pop(&self) -> Option<Update> {
        self.queue
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .items
            .pop_front()
    }
}

/// Per-subscription decode state: the last full value, so a partial
/// update can be completed from it.
#[derive(Default)]
struct Decoder {
    desc: Option<Arc<FieldDesc>>,
    prior: Option<PvField>,
}

impl Decoder {
    fn decode(&mut self, desc: &FieldDesc, body: &[u8], order: ByteOrder) -> Option<Update> {
        let desc_arc = match &self.desc {
            Some(d) if **d == *desc => d.clone(),
            _ => {
                let d = Arc::new(desc.clone());
                self.desc = Some(d.clone());
                self.prior = None;
                d
            }
        };
        let mut cur = Cursor::new(body);
        let changed = BitSet::decode(&mut cur, order).ok()?;
        let decoded = decode_pv_field_with_bitset(desc, &changed, 0, &mut cur, order).ok()?;
        let full = match &self.prior {
            Some(prior) => fill_unmarked_from_prior(desc, &changed, 0, decoded, prior),
            None => decoded,
        };
        self.prior = Some(full.clone());
        Some(Update::Value {
            desc: desc_arc,
            value: full,
            changed,
        })
    }
}

// ---------------------------------------------------------------------------
// PvaSubscription
// ---------------------------------------------------------------------------

/// A monitor. Python drains it by calling `recv`; nothing is delivered on
/// a runtime thread. Each item is a `(kind, payload)` tuple: `("value",
/// Value)`, `("connected", "host:port")`, `("disconnected", None)` or
/// `("finished", None)`. `recv` returns `None` once closed.
///
/// `close` cancels the token before taking the handle lock, so a parked
/// `recv` wakes and returns `None` rather than holding the lock forever.
#[pyclass(frozen, module = "epicsrs._epicsrs")]
pub struct PvaSubscription {
    handle: Arc<tokio::sync::Mutex<Option<SubscriptionHandle>>>,
    shared: Arc<Shared>,
    closed: CancellationToken,
    name: String,
}

impl PvaSubscription {
    async fn do_recv(
        shared: Arc<Shared>,
        closed: CancellationToken,
        timeout: Option<f64>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let next = bounded(timeout, async {
            loop {
                if closed.is_cancelled() {
                    return Ok(None);
                }
                if let Some(u) = shared.pop() {
                    return Ok(Some(u));
                }
                tokio::select! {
                    _ = shared.notify.notified() => {}
                    _ = closed.cancelled() => return Ok(None),
                }
            }
        })
        .await?;
        let Some(u) = next else {
            return Ok(None);
        };
        Python::attach(|py| {
            let item = match u {
                Update::Value {
                    desc,
                    value,
                    changed,
                } => (
                    "value",
                    Value::from_parts(desc, value, changed).into_py_any(py)?,
                ),
                Update::Connected(peer) => ("connected", peer.to_string().into_py_any(py)?),
                Update::Disconnected => ("disconnected", py.None()),
                Update::Finished => ("finished", py.None()),
            };
            item.into_py_any(py).map(Some)
        })
    }

    async fn do_close(
        handle: Arc<tokio::sync::Mutex<Option<SubscriptionHandle>>>,
        closed: CancellationToken,
    ) {
        closed.cancel();
        if let Some(h) = handle.lock().await.take() {
            h.stop_sync().await;
        }
    }

    async fn with_handle<R>(
        handle: Arc<tokio::sync::Mutex<Option<SubscriptionHandle>>>,
        f: impl AsyncFnOnce(&SubscriptionHandle) -> R,
    ) -> PyResult<R> {
        match handle.lock().await.as_ref() {
            Some(h) => Ok(f(h).await),
            None => Err(PvaError::new_err("subscription is closed")),
        }
    }
}

#[pymethods]
impl PvaSubscription {
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    /// Next item, or `None` once closed. With a `timeout`, raises
    /// `PvaTimeout` if nothing arrives in time.
    #[pyo3(signature = (timeout=None))]
    fn recv(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Py<PyAny>>> {
        block_on(
            py,
            Self::do_recv(self.shared.clone(), self.closed.clone(), timeout),
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
            Self::do_recv(self.shared.clone(), self.closed.clone(), timeout),
        )
    }

    fn pause(&self, py: Python<'_>) -> PyResult<()> {
        block_on(
            py,
            Self::with_handle(self.handle.clone(), async |h| h.pause().await),
        )
    }

    fn resume(&self, py: Python<'_>) -> PyResult<()> {
        block_on(
            py,
            Self::with_handle(self.handle.clone(), async |h| h.resume().await),
        )
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
