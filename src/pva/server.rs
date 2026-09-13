//! pvAccess server: `PvaSharedPV`, `PvaProvider`, `PvaServer`, and the
//! `PvaWorkQueue` that hands client operations to Python.
//!
//! No Python code runs on a runtime worker. A client PUT or RPC arrives on
//! the server's task, becomes a `ServerOperation` holding a oneshot reply
//! slot, and is pushed onto a `PvaWorkQueue`; a Python-owned thread or
//! asyncio task pulls it with `recv`/`recv_async`, runs the handler, and
//! finishes it with `done()`. Dropping the operation without `done()`
//! drops the oneshot sender, which the server task reads as a failed
//! operation — an operation can never be left dangling.
//!
//! PV state (open/close, current value, channel hooks, forced disconnect
//! on close) lives in `epics_pva_rs`'s `SharedPV`/`SharedSource`. Monitor
//! subscribers are kept here instead, because the library's `SharedPV`
//! queues a bare `PvField`, so a post could only ever say "everything
//! changed". Each subscriber is a library `MonitorRing` of
//! `MonitorUpdate` — the posted value with its changed paths — which the
//! wire layer reads directly as its `MonitorStream`: a post is one hop,
//! and a slow client holds back nothing but its own ring.

use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use epics_pva_rs::config::Endpoint;
use epics_pva_rs::proto::BitSet;
use epics_pva_rs::pvdata::encode::{changed_bitset_paths, fill_unmarked_from_prior};
use epics_pva_rs::pvdata::{FieldDesc, PvField, RpcReply};
use epics_pva_rs::server_native::config::ClientCredentials;
use epics_pva_rs::server_native::runtime::PvaServer as RsServer;
use epics_pva_rs::server_native::shared_pv::{MonitorOutbox, MonitorRing, SharedPV, SharedSource};
use epics_pva_rs::server_native::source::{
    AccessChecked, AccessGate, ChannelContext, ChannelInvalidator, ChannelSource, MonitorOptions,
    MonitorStream, MonitorUpdate, OpError, RPC_NOT_IMPLEMENTED, SourceRead, SubscriptionSeed,
    WatermarkEvent, put_denied,
};
use epics_pva_rs::server_native::{CompositeSource, DynSource, PvaServerConfig};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::error::{PvaError, map_pva};
use super::value::{Type, Value};
use crate::runtime::{block_on, into_py_future};

/// Credentials for the trait's context-free entry points (`put_value`,
/// `rpc`), which the wire layer never uses.
fn no_creds() -> Arc<ClientCredentials> {
    Arc::new(ClientCredentials {
        method: "anonymous".into(),
        account: String::new(),
        host: String::new(),
        authority: String::new(),
        roles: Vec::new(),
    })
}

fn all_marks(desc: &FieldDesc) -> BitSet {
    let mut all = BitSet::new();
    for b in 0..desc.total_bits() {
        all.set(b);
    }
    all
}

// ---------------------------------------------------------------------------
// ServerOperation
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum OpKind {
    Put,
    Rpc,
}

/// What the handler answered: `Ok(None)` for a plain completion,
/// `Ok(Some(..))` for an RPC reply value, `Err(text)` for an error reply.
type OpReply = Result<Option<(FieldDesc, PvField)>, String>;

/// A client PUT or RPC waiting for the Python handler (p4p `ServerOperation`).
#[pyclass(name = "ServerOperation", module = "repics._repics", frozen)]
pub struct ServerOperation {
    kind: OpKind,
    name: String,
    desc: Arc<FieldDesc>,
    value: PvField,
    marks: BitSet,
    peer: SocketAddr,
    creds: Arc<ClientCredentials>,
    pv_request: Option<PvField>,
    reply: Mutex<Option<oneshot::Sender<OpReply>>>,
}

impl ServerOperation {
    fn finish(&self, reply: OpReply) -> PyResult<()> {
        let tx = self
            .reply
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| PvaError::new_err("done() already called"))?;
        // The server task may have given up (client gone); nothing to do.
        let _ = tx.send(reply);
        Ok(())
    }
}

#[pymethods]
impl ServerOperation {
    /// `"put"` or `"rpc"`.
    fn kind(&self) -> &'static str {
        match self.kind {
            OpKind::Put => "put",
            OpKind::Rpc => "rpc",
        }
    }

    /// The PV name the client addressed.
    fn name(&self) -> &str {
        &self.name
    }

    /// `"host:port"` of the client.
    fn peer(&self) -> String {
        self.peer.to_string()
    }

    /// The client's account as authenticated by the server.
    fn account(&self) -> &str {
        &self.creds.account
    }

    /// The authentication method (`"anonymous"`, `"ca"`, `"x509"`, ...).
    fn method(&self) -> &str {
        &self.creds.method
    }

    /// For a PUT: the PV's value with the client's fields applied and
    /// marked. For an RPC: the request argument.
    fn value(&self) -> Value {
        Value::from_parts(self.desc.clone(), self.value.clone(), self.marks.clone())
    }

    /// The pvRequest the client sent, or `None`.
    #[pyo3(name = "pvRequest")]
    fn pv_request(&self) -> Option<Value> {
        let req = self.pv_request.as_ref()?;
        let desc = Arc::new(req.descriptor());
        let marks = all_marks(&desc);
        Some(Value::from_parts(desc, req.clone(), marks))
    }

    /// Complete the operation. `error` replies a failure; `value` is the
    /// RPC reply (ignored for PUT). Calling it twice is an error.
    #[pyo3(signature = (value=None, error=None))]
    fn done(&self, value: Option<&Value>, error: Option<String>) -> PyResult<()> {
        let reply = match (error, value, self.kind) {
            (Some(msg), _, _) => Err(msg),
            (None, Some(v), OpKind::Rpc) => {
                let (desc, field, _) = v.snapshot()?;
                Ok(Some((desc.as_ref().clone(), field)))
            }
            (None, _, _) => Ok(None),
        };
        self.finish(reply)
    }
}

// ---------------------------------------------------------------------------
// PvaWorkQueue
// ---------------------------------------------------------------------------

enum EventKind {
    Put(ServerOperation),
    Rpc(ServerOperation),
    FirstConnect,
    LastDisconnect,
}

/// One item for Python: which PV (an opaque token Python registered,
/// normally a weakref to its `SharedPV`) and what happened.
struct Event {
    token: Arc<Py<PyAny>>,
    kind: EventKind,
}

/// The channel a Python drain loop pulls server events from.
#[pyclass(name = "PvaWorkQueue", module = "repics._repics", frozen)]
pub struct PvaWorkQueue {
    tx: mpsc::UnboundedSender<Event>,
    /// Behind an `Arc` so `recv_async` can move it into a `'static` future.
    rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Event>>>,
    stopped: CancellationToken,
}

type Receiver = Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Event>>>;

impl PvaWorkQueue {
    async fn do_recv(rx: Receiver, stopped: CancellationToken) -> Option<Event> {
        if stopped.is_cancelled() {
            return None;
        }
        let mut rx = rx.lock().await;
        tokio::select! {
            biased;
            _ = stopped.cancelled() => None,
            ev = rx.recv() => ev,
        }
    }
}

type PyEvent = (Py<PyAny>, &'static str, Option<Py<ServerOperation>>);

fn event_to_py(py: Python<'_>, ev: Event) -> PyResult<PyEvent> {
    let token = ev.token.clone_ref(py);
    Ok(match ev.kind {
        EventKind::Put(op) => (token, "put", Some(Py::new(py, op)?)),
        EventKind::Rpc(op) => (token, "rpc", Some(Py::new(py, op)?)),
        EventKind::FirstConnect => (token, "first", None),
        EventKind::LastDisconnect => (token, "last", None),
    })
}

#[pymethods]
impl PvaWorkQueue {
    #[new]
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        PvaWorkQueue {
            tx,
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
            stopped: CancellationToken::new(),
        }
    }

    /// Block (GIL released) for the next event: `(token, kind, op)` where
    /// `kind` is `"put"`, `"rpc"`, `"first"` or `"last"`. `None` once
    /// `stop()` was called.
    fn recv(&self, py: Python<'_>) -> PyResult<Option<PyEvent>> {
        let ev = block_on(py, Self::do_recv(self.rx.clone(), self.stopped.clone()));
        ev.map(|ev| event_to_py(py, ev)).transpose()
    }

    /// `recv` as an awaitable.
    fn recv_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.rx.clone();
        let stopped = self.stopped.clone();
        into_py_future(py, async move {
            let ev = Self::do_recv(rx, stopped).await;
            Python::attach(|py| ev.map(|ev| event_to_py(py, ev)).transpose())
        })
    }

    /// Wake the drain loop with `None`; later events are dropped, which
    /// fails their operations at the client.
    fn stop(&self) {
        self.stopped.cancel();
    }
}

// ---------------------------------------------------------------------------
// Monitor subscribers
// ---------------------------------------------------------------------------

/// One monitor subscriber's producer endpoint. The wire layer takes the
/// ring itself as its `MonitorStream`, and a full ring folds a newer
/// update into its tail by `MonitorUpdate`'s `SquashTail` rule: newest
/// value, changed sets united, the overlap as `overrun`. Two shapes,
/// because `ChannelSource` has both a marked and a plain subscribe.
enum Subscriber {
    Marked(MonitorOutbox<MonitorUpdate>),
    Plain(MonitorOutbox<PvField>),
}

impl Subscriber {
    /// False once the client's ring is gone, so the owner drops it.
    fn post(&self, value: &PvField, marked: &[String]) -> bool {
        match self {
            Subscriber::Marked(outbox) => outbox.post(
                MonitorUpdate {
                    value: value.clone(),
                    marked: Some(marked.to_vec()),
                    type_changed: false,
                    overrun: Vec::new(),
                },
                false,
            ),
            Subscriber::Plain(outbox) => outbox.post(value.clone(), false),
        }
    }
}

// ---------------------------------------------------------------------------
// PvEntry / PySource
// ---------------------------------------------------------------------------

/// What a `PvEntry` guards under one lock, held across store-and-deliver
/// in `post` and across register-and-snapshot in `subscribe`, so a
/// subscriber sees every post either in its seed or in its ring, never
/// neither.
struct PvState {
    subs: Vec<Subscriber>,
    /// The union of the open value's marks and every post's since, which
    /// is what p4p's `current()` reports: pvxs keeps the marks on the
    /// stored Value, and its `post` is `current.assign(val)`. The library
    /// stores a bare `PvField`, so the marks live here.
    marks: BitSet,
    /// The descriptor `open` was given, `None` while closed. The library
    /// stores its own copy and hands it out by clone; this is the `Arc`
    /// every `Value` built from the same `Type` shares, so a post's type
    /// check is usually a pointer comparison and `current()` needs no
    /// descriptor clone.
    opened: Option<Arc<FieldDesc>>,
}

/// The Rust side of one Python `SharedPV`.
struct PvEntry {
    pv: SharedPV,
    state: Mutex<PvState>,
    events: mpsc::UnboundedSender<Event>,
    token: Arc<Py<PyAny>>,
}

impl PvEntry {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, PvState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn open(&self, desc: Arc<FieldDesc>, field: PvField, marks: BitSet) -> PyResult<()> {
        let mut state = self.lock_state();
        self.pv
            .open(desc.as_ref().clone(), field)
            .map_err(map_pva)?;
        state.marks = marks;
        state.opened = Some(desc);
        Ok(())
    }

    fn send(&self, kind: EventKind) -> Result<(), OpError> {
        self.events
            .send(Event {
                token: self.token.clone(),
                kind,
            })
            .map_err(|_| OpError::failed("SharedPV handler queue is stopped"))
    }

    /// pvxs `SharedPV::post`: `current.assign(val)` — the marked leaves
    /// copied into the stored value in place — then one post per
    /// subscriber. The value is read under its own lock and never cloned
    /// as a whole; `post_delta` copies the marked subtrees and checks only
    /// those. This is the only writer of the stored value (the library's
    /// PUT handler is unreachable, PUTs go to Python and come back here),
    /// and it runs under the state lock, so the value `with_current` reads
    /// for the subscribers is the one this post stored.
    fn post(&self, value: &Value) -> PyResult<()> {
        value.with_root(|root| {
            let mut state = self.lock_state();
            let Some(opened) = state.opened.clone() else {
                return Err(PvaError::new_err("SharedPV not open"));
            };
            if !Arc::ptr_eq(&root.desc, &opened) && *root.desc != *opened {
                return Err(PvaError::new_err(
                    "post() value type differs from the open() type; close() first",
                ));
            }
            // p4p: a subscriber post that touches no requested field is
            // dropped by pvxs, so an unmarked Value changes nothing and
            // reaches no subscriber.
            if root.marks.is_empty() {
                return Ok(());
            }
            self.pv
                .post_delta(&root.marks, &root.field)
                .map_err(map_pva)?;
            state.marks.union_with(&root.marks);
            if !state.subs.is_empty() {
                let marked = changed_bitset_paths(&opened, &root.marks);
                self.pv
                    .with_current(|_, cur| state.subs.retain(|sub| sub.post(cur, &marked)));
            }
            Ok(())
        })?
    }

    fn close(&self) {
        let subs = {
            let mut state = self.lock_state();
            state.marks = BitSet::new();
            state.opened = None;
            std::mem::take(&mut state.subs)
        };
        self.pv.close();
        // Dropping the last producer endpoint ends each client's stream.
        drop(subs);
    }

    /// Register a subscriber and snapshot the seed under one lock. `wrap`
    /// turns the producer endpoint into the subscriber the ring serves.
    fn subscribe<T>(
        &self,
        limit: usize,
        wrap: impl FnOnce(MonitorOutbox<T>, &PvField) -> Subscriber,
    ) -> Option<(PvField, MonitorRing<T>)> {
        let mut state = self.lock_state();
        let initial = self.pv.current()?;
        let (outbox, ring) = MonitorRing::bounded(limit);
        state.subs.push(wrap(outbox, &initial));
        Some((initial, ring))
    }

    async fn run_op(&self, kind: OpKind, op: ServerOperation) -> Result<RpcReply, OpError> {
        let (tx, rx) = oneshot::channel();
        *op.reply.lock().unwrap_or_else(|e| e.into_inner()) = Some(tx);
        self.send(match kind {
            OpKind::Put => EventKind::Put(op),
            OpKind::Rpc => EventKind::Rpc(op),
        })?;
        match rx.await {
            Ok(Ok(Some((desc, value)))) => Ok(RpcReply::Value(desc, value)),
            Ok(Ok(None)) => Ok(RpcReply::Empty),
            Ok(Err(msg)) => Err(OpError::failed(msg)),
            Err(_) => Err(OpError::failed(
                "handler dropped the operation without done()",
            )),
        }
    }
}

/// A `SharedSource` whose PUT/RPC go to Python and whose monitors carry
/// marks. Everything else is the library's behaviour, delegated.
pub struct PySource {
    inner: SharedSource,
    entries: Mutex<HashMap<String, Arc<PvEntry>>>,
    /// The PV each attached channel was created against, one element per
    /// channel, keyed by channel name. `SharedSource` resolves a channel
    /// close by looking the name up in its table, so a PV removed while
    /// clients hold channels never sees its last-disconnect edge; keeping
    /// the attachment here makes remove-then-close (p4p's recipe for a
    /// `close(sync=True)` that can complete) fire it. Only when a removed
    /// PV keeps live channels while another PV is served under the same
    /// name is a close ambiguous; it is then charged oldest-first.
    attached: Mutex<HashMap<String, VecDeque<Arc<PvEntry>>>>,
}

impl PySource {
    fn entry(&self, name: &str) -> Option<Arc<PvEntry>> {
        self.entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .cloned()
    }
}

impl ChannelSource for PySource {
    fn access(&self) -> &AccessGate {
        self.inner.access()
    }

    fn beacon_change(&self) -> u64 {
        self.inner.beacon_change()
    }

    fn set_channel_invalidator(&self, invalidator: ChannelInvalidator) {
        self.inner.set_channel_invalidator(invalidator)
    }

    fn list_pvs(&self) -> impl std::future::Future<Output = Vec<String>> + Send {
        self.inner.list_pvs()
    }

    fn has_pv(&self, name: &str) -> impl std::future::Future<Output = bool> + Send {
        self.inner.has_pv(name)
    }

    fn get_introspection(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Option<FieldDesc>> + Send {
        self.inner.get_introspection(name)
    }

    fn await_introspection(
        &self,
        name: &str,
        ctx: ChannelContext,
    ) -> impl std::future::Future<Output = Option<FieldDesc>> + Send {
        self.inner.await_introspection(name, ctx)
    }

    fn get_value(&self, name: &str) -> impl std::future::Future<Output = Option<PvField>> + Send {
        self.inner.get_value(name)
    }

    fn put_value(
        &self,
        name: &str,
        value: PvField,
    ) -> impl std::future::Future<Output = Result<(), OpError>> + Send {
        // The wire layer only ever issues delta PUTs; a full-value put
        // is the same operation with every bit marked.
        let entry = self.entry(name);
        let name = name.to_string();
        async move {
            let Some(entry) = entry else {
                return Err(OpError::failed(format!("no such PV: {name}")));
            };
            let Some(desc) = entry.pv.introspection() else {
                return Err(OpError::failed("SharedPV not open"));
            };
            let desc = Arc::new(desc);
            let marks = all_marks(&desc);
            let op = ServerOperation {
                kind: OpKind::Put,
                name,
                desc,
                value,
                marks,
                peer: SocketAddr::from(([0, 0, 0, 0], 0)),
                creds: no_creds(),
                pv_request: None,
                reply: Mutex::new(None),
            };
            entry.run_op(OpKind::Put, op).await.map(|_| ())
        }
    }

    fn put_delta_checked(
        &self,
        checked: AccessChecked,
        desc: Arc<FieldDesc>,
        changed: BitSet,
        delta: &PvField,
        ctx: ChannelContext,
    ) -> impl std::future::Future<Output = Result<(), OpError>> + Send {
        let entry = self.entry(checked.pv_name());
        let delta = delta.clone();
        async move {
            if !checked.allows_write() {
                return Err(put_denied(&checked, &ctx));
            }
            let Some(entry) = entry else {
                return Err(OpError::failed(format!(
                    "no such PV: {}",
                    checked.pv_name()
                )));
            };
            let Some(prior) = entry.pv.current() else {
                return Err(OpError::failed("SharedPV not open"));
            };
            let merged = fill_unmarked_from_prior(&desc, &changed, 0, delta, &prior);
            let op = ServerOperation {
                kind: OpKind::Put,
                name: checked.pv_name().to_string(),
                desc,
                value: merged,
                marks: changed,
                peer: ctx.peer,
                creds: ctx.creds.clone(),
                pv_request: ctx.pv_request.clone(),
                reply: Mutex::new(None),
            };
            entry.run_op(OpKind::Put, op).await.map(|_| ())
        }
    }

    fn is_writable(&self, name: &str) -> impl std::future::Future<Output = bool> + Send {
        self.inner.is_writable(name)
    }

    fn subscribe(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Option<MonitorStream<PvField>>> + Send {
        let entry = self.entry(name);
        async move {
            // A plain stream carries no separate seed: the current value
            // is its first element.
            let (_, ring) = entry?.subscribe(4, |outbox, initial| {
                outbox.post(initial.clone(), false);
                Subscriber::Plain(outbox)
            })?;
            Some(MonitorStream::Ring(ring))
        }
    }

    fn subscribe_seeded(
        &self,
        checked: AccessChecked,
        _ctx: ChannelContext,
        opts: MonitorOptions,
    ) -> impl std::future::Future<Output = Option<SubscriptionSeed<MonitorUpdate>>> + Send {
        let entry = if checked.allows_read() {
            self.entry(checked.pv_name())
        } else {
            None
        };
        let limit = (opts.queue_size as usize).max(1);
        async move {
            let (initial, ring) =
                entry?.subscribe(limit, |outbox, _| Subscriber::Marked(outbox))?;
            Some(SubscriptionSeed {
                initial: Some(SourceRead::from(initial)),
                updates: MonitorStream::Ring(ring),
                on_start: None,
            })
        }
    }

    fn rpc(
        &self,
        name: &str,
        request_desc: FieldDesc,
        request_value: PvField,
    ) -> impl std::future::Future<Output = Result<RpcReply, OpError>> + Send {
        let entry = self.entry(name);
        let name = name.to_string();
        async move {
            let Some(entry) = entry else {
                return Err(OpError::failed(format!("no such PV: {name}")));
            };
            let desc = Arc::new(request_desc);
            let marks = all_marks(&desc);
            let op = ServerOperation {
                kind: OpKind::Rpc,
                name,
                desc,
                value: request_value,
                marks,
                peer: SocketAddr::from(([0, 0, 0, 0], 0)),
                creds: no_creds(),
                pv_request: None,
                reply: Mutex::new(None),
            };
            entry.run_op(OpKind::Rpc, op).await
        }
    }

    fn rpc_checked(
        &self,
        checked: AccessChecked,
        request_desc: FieldDesc,
        request_value: PvField,
        ctx: ChannelContext,
    ) -> impl std::future::Future<Output = Result<RpcReply, OpError>> + Send {
        let entry = self.entry(checked.pv_name());
        async move {
            if !checked.allows_read() {
                return Err(OpError::denied(format!(
                    "RPC denied by access security: '{}' from {}/{}/{}",
                    checked.pv_name(),
                    ctx.creds.host,
                    ctx.creds.account,
                    ctx.creds.method,
                )));
            }
            let Some(entry) = entry else {
                return Err(OpError::failed(RPC_NOT_IMPLEMENTED));
            };
            let desc = Arc::new(request_desc);
            let marks = all_marks(&desc);
            let op = ServerOperation {
                kind: OpKind::Rpc,
                name: checked.pv_name().to_string(),
                desc,
                value: request_value,
                marks,
                peer: ctx.peer,
                creds: ctx.creds.clone(),
                pv_request: ctx.pv_request.clone(),
                reply: Mutex::new(None),
            };
            entry.run_op(OpKind::Rpc, op).await
        }
    }

    fn process(&self, name: &str) -> impl std::future::Future<Output = Result<(), OpError>> + Send {
        self.inner.process(name)
    }

    fn notify_watermark(&self, name: &str, ctx: &ChannelContext, ev: WatermarkEvent) {
        self.inner.notify_watermark(name, ctx, ev)
    }

    fn notify_monitor_start(&self, name: &str, ctx: &ChannelContext, start: bool) {
        self.inner.notify_monitor_start(name, ctx, start)
    }

    fn notify_channel_open(&self, name: &str, _ctx: &ChannelContext) {
        if let Some(entry) = self.entry(name) {
            self.attached
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(name.to_string())
                .or_default()
                .push_back(entry.clone());
            entry.pv.attach_channel();
        }
    }

    fn notify_channel_close(&self, name: &str, _ctx: &ChannelContext) {
        let entry = {
            let mut attached = self.attached.lock().unwrap_or_else(|e| e.into_inner());
            let entry = attached.get_mut(name).and_then(|q| q.pop_front());
            if attached.get(name).is_some_and(|q| q.is_empty()) {
                attached.remove(name);
            }
            entry
        };
        if let Some(entry) = entry {
            entry.pv.detach_channel();
        }
    }

    fn monitor_watermarks(
        &self,
        name: &str,
    ) -> impl std::future::Future<Output = Option<(usize, usize)>> + Send {
        self.inner.monitor_watermarks(name)
    }
}

// ---------------------------------------------------------------------------
// PvaSharedPV
// ---------------------------------------------------------------------------

/// The Rust half of `repics.pva.server.SharedPV`.
#[pyclass(name = "PvaSharedPV", module = "repics._repics", frozen)]
pub struct PvaSharedPV {
    entry: Arc<PvEntry>,
}

#[pymethods]
impl PvaSharedPV {
    /// `queue` receives this PV's operations and connect events, each
    /// tagged with `token` (Python passes a weakref to its `SharedPV`).
    #[new]
    fn new(queue: &PvaWorkQueue, token: Py<PyAny>) -> Self {
        let pv = SharedPV::new();
        // PUT never reaches this handler — `PySource::put_delta_checked`
        // routes to Python first — but installing one is what makes the
        // library report the PV as writable to clients.
        pv.on_put(|_, _| Err("unreachable: PUT is routed to Python".into()));
        let entry = Arc::new(PvEntry {
            pv: pv.clone(),
            state: Mutex::new(PvState {
                subs: Vec::new(),
                marks: BitSet::new(),
                opened: None,
            }),
            events: queue.tx.clone(),
            token: Arc::new(token),
        });
        let first = Arc::downgrade(&entry);
        pv.on_first_connect(move |_| {
            if let Some(e) = first.upgrade() {
                let _ = e.send(EventKind::FirstConnect);
            }
        });
        let last = Arc::downgrade(&entry);
        pv.on_last_disconnect(move |_| {
            if let Some(e) = last.upgrade() {
                let _ = e.send(EventKind::LastDisconnect);
            }
        });
        PvaSharedPV { entry }
    }

    /// Declare the type and initial value; clients may connect afterwards.
    fn open(&self, value: &Value) -> PyResult<()> {
        let (desc, field, marks) = value.snapshot()?;
        self.entry.open(desc, field, marks)
    }

    /// Drop the value and force-disconnect every client.
    fn close(&self) {
        self.entry.close();
    }

    #[pyo3(name = "isOpen")]
    fn is_open(&self) -> bool {
        self.entry.pv.is_open()
    }

    /// Apply the marked fields of `value` and deliver them to every
    /// subscriber. An unmarked Value is a no-op, as in p4p.
    fn post(&self, py: Python<'_>, value: &Value) -> PyResult<()> {
        // Store-and-deliver touches no Python object, so run it without
        // the GIL: a thread posting in a loop then hands the interpreter
        // to the handler threads and in-process client callbacks on every
        // post instead of once per switch interval (p4p does the same).
        py.detach(|| self.entry.post(value))
    }

    /// The current value carrying the open value's marks and every
    /// post's since (p4p `current()`), or `None` while closed.
    fn current(&self) -> Option<Value> {
        let state = self.entry.lock_state();
        let desc = state.opened.clone()?;
        let field = self.entry.pv.current()?;
        Some(Value::from_parts(desc, field, state.marks.clone()))
    }

    /// The opened type, or `None` while closed.
    fn r#type(&self) -> Option<Type> {
        self.entry.lock_state().opened.clone().map(Type::from_desc)
    }
}

// ---------------------------------------------------------------------------
// PvaProvider
// ---------------------------------------------------------------------------

/// A named table of PVs (p4p `StaticProvider`).
#[pyclass(name = "PvaProvider", module = "repics._repics", frozen)]
pub struct PvaProvider {
    name: String,
    source: Arc<PySource>,
}

#[pymethods]
impl PvaProvider {
    #[new]
    fn new(name: String) -> Self {
        PvaProvider {
            name,
            source: Arc::new(PySource {
                inner: SharedSource::new(),
                entries: Mutex::new(HashMap::new()),
                attached: Mutex::new(HashMap::new()),
            }),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn add(&self, name: String, pv: &PvaSharedPV) -> PyResult<()> {
        let mut entries = self
            .source
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if entries.contains_key(&name) {
            return Err(PyValueError::new_err(format!("PV {name:?} already added")));
        }
        self.source
            .inner
            .try_add(name.clone(), pv.entry.pv.clone())
            .map_err(|e| PyValueError::new_err(format!("PV {:?} already added", e.0)))?;
        entries.insert(name, pv.entry.clone());
        Ok(())
    }

    fn remove(&self, name: &str) -> bool {
        let removed = self
            .source
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(name)
            .is_some();
        self.source.inner.remove(name);
        removed
    }

    fn keys(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .source
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();
        names.sort();
        names
    }
}

// ---------------------------------------------------------------------------
// PvaServer
// ---------------------------------------------------------------------------

fn yes(s: &str) -> bool {
    matches!(
        s.trim().to_ascii_lowercase().as_str(),
        "yes" | "y" | "1" | "true" | "on"
    )
}

fn first<'a>(conf: &'a HashMap<String, String>, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|k| conf.get(*k).map(|v| v.trim()))
        .filter(|v| !v.is_empty())
}

fn port_of(conf: &HashMap<String, String>, keys: &[&str]) -> PyResult<Option<u16>> {
    match first(conf, keys) {
        None => Ok(None),
        Some(v) => v
            .parse::<u16>()
            .map(Some)
            .map_err(|e| PyValueError::new_err(format!("{}={v:?}: {e}", keys[0]))),
    }
}

/// Apply the p4p-style `EPICS_PVAS_*` / `EPICS_PVA_*` keys of `conf`.
fn configure(mut c: PvaServerConfig, conf: &HashMap<String, String>) -> PyResult<PvaServerConfig> {
    if let Some(p) = port_of(conf, &["EPICS_PVAS_SERVER_PORT", "EPICS_PVA_SERVER_PORT"])? {
        c.tcp_port = p;
    }
    if let Some(p) = port_of(
        conf,
        &["EPICS_PVAS_BROADCAST_PORT", "EPICS_PVA_BROADCAST_PORT"],
    )? {
        c.udp_port = p;
    }
    if let Some(v) = first(conf, &["EPICS_PVAS_INTF_ADDR_LIST"]) {
        let mut ifaces = Vec::new();
        for tok in v.split_whitespace() {
            let host = tok.rsplit_once(':').map(|(h, _)| h).unwrap_or(tok);
            let ip: IpAddr = host.parse().map_err(|e| {
                PyValueError::new_err(format!("EPICS_PVAS_INTF_ADDR_LIST entry {tok:?}: {e}"))
            })?;
            ifaces.push(ip);
        }
        c.interfaces = ifaces;
    }
    if let Some(v) = first(
        conf,
        &[
            "EPICS_PVAS_AUTO_BEACON_ADDR_LIST",
            "EPICS_PVA_AUTO_ADDR_LIST",
        ],
    ) {
        c.auto_beacon = yes(v);
    }
    if let Some(v) = first(
        conf,
        &["EPICS_PVAS_BEACON_ADDR_LIST", "EPICS_PVA_ADDR_LIST"],
    ) {
        let default_port = if c.udp_port == 0 { 5076 } else { c.udp_port };
        c.beacon_destinations = v
            .split_whitespace()
            .filter_map(|tok| Endpoint::parse(tok, default_port))
            .collect();
    }
    Ok(c)
}

/// A running pvAccess server over one or more providers.
#[pyclass(name = "PvaServer", module = "repics._repics", frozen)]
pub struct PvaServer {
    inner: Mutex<Option<RsServer>>,
    tcp: SocketAddr,
    udp_port: u16,
    addr: IpAddr,
}

#[pymethods]
impl PvaServer {
    /// `providers` are `(provider, order)` pairs; lower `order` is
    /// searched first. `isolate` binds loopback on ephemeral ports with
    /// no beacons and ignores `conf`/`useenv`.
    #[new]
    #[pyo3(signature = (providers, conf=None, useenv=true, isolate=false))]
    fn new(
        py: Python<'_>,
        providers: Vec<(PyRef<'_, PvaProvider>, i32)>,
        conf: Option<HashMap<String, String>>,
        useenv: bool,
        isolate: bool,
    ) -> PyResult<Self> {
        let composite = CompositeSource::new();
        for (p, order) in &providers {
            let source: DynSource = p.source.clone();
            composite
                .add_source(&p.name, source, *order)
                .map_err(PyValueError::new_err)?;
        }
        let config = if isolate {
            None
        } else {
            let base = if useenv {
                PvaServerConfig::default().with_env()
            } else {
                PvaServerConfig::default()
            };
            Some(configure(base, &conf.unwrap_or_default())?)
        };
        let server = block_on(py, async move {
            match config {
                None => RsServer::isolated(composite),
                Some(c) => RsServer::start(composite, c),
            }
        })
        .map_err(map_pva)?;
        let tcp = server.tcp_addr();
        let cfg = server.config();
        let udp_port = cfg.udp_port;
        let addr = if isolate {
            IpAddr::from([127, 0, 0, 1])
        } else if let Some(ip) = cfg.interfaces.iter().find(|ip| !ip.is_unspecified()) {
            *ip
        } else if cfg.bind_ip.is_unspecified() {
            IpAddr::from([127, 0, 0, 1])
        } else {
            cfg.bind_ip
        };
        Ok(PvaServer {
            inner: Mutex::new(Some(server)),
            tcp,
            udp_port,
            addr,
        })
    }

    /// Stop serving and disconnect every client. Idempotent.
    fn stop(&self, py: Python<'_>) {
        let server = self.inner.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(s) = server {
            block_on(py, async move {
                s.stop();
                drop(s);
            });
        }
    }

    fn running(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    fn tcp_port(&self) -> u16 {
        self.tcp.port()
    }

    fn udp_port(&self) -> u16 {
        self.udp_port
    }

    /// A p4p-style `conf()` dict a client can be built from to reach
    /// exactly this server: the search address carries the UDP port
    /// explicitly, and a name server entry gives a UDP-free path.
    ///
    /// `EPICS_PVA_BROADCAST_PORT` is deliberately absent. p4p sets it to
    /// the server's UDP port, but a pvxs client also *binds* that port
    /// for beacons, and the ephemeral UDP socket epics-pva-rs binds has
    /// no SO_REUSEADDR, so the client's bind fails with EADDRINUSE.
    fn conf<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        let addr = self.addr.to_string();
        d.set_item("EPICS_PVA_ADDR_LIST", format!("{addr}:{}", self.udp_port))?;
        d.set_item("EPICS_PVA_AUTO_ADDR_LIST", "NO")?;
        d.set_item("EPICS_PVA_SERVER_PORT", self.tcp.port().to_string())?;
        d.set_item(
            "EPICS_PVA_NAME_SERVERS",
            format!("{addr}:{}", self.tcp.port()),
        )?;
        d.set_item("EPICS_PVAS_INTF_ADDR_LIST", &addr)?;
        d.set_item("EPICS_PVAS_SERVER_PORT", self.tcp.port().to_string())?;
        d.set_item("EPICS_PVAS_BROADCAST_PORT", self.udp_port.to_string())?;
        Ok(d)
    }
}
