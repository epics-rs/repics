//! Channel Access server: `CaSharedPV`, `CaProvider`, `CaServer`, and the
//! `CaWorkQueue` that hands client PUTs to Python.
//!
//! CA has no application channel-source trait; a served PV is a bare
//! `epics_base_rs` `ProcessVariable` value cell inside one `PvDatabase`. A
//! client PUT reaches Python through the PV's `WriteHook`: the hook turns the
//! write into a `CaServerOperation` on a `CaWorkQueue`, a Python-owned drain
//! loop runs the handler and finishes it, and the hook returns the handler's
//! verdict to the CA client. A monitor is served by the `ProcessVariable`'s
//! own subscriber fan-out — a `post()` is one `pv.set()`, and every
//! subscriber sees it, exactly as pvxs `SharedPV::post` does for PVA.
//!
//! No Python code runs on a runtime worker: the hook only enqueues and awaits
//! a oneshot, and the handler runs on a Python thread (or asyncio task) that
//! pulls the operation with `recv`/`recv_async`.
//!
//! CA has no RPC and no per-PV connect edge, so the queue carries only PUTs;
//! p4p's `onFirstConnect`/`onLastDisconnect`/`rpc` have no CA counterpart.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, OnceLock};

use epics_base_rs::server::access_security::new_acf_cell;
use epics_base_rs::server::database::PvDatabase;
use epics_base_rs::server::pv::{ProcessVariable, WriteContext, WriteHook};
use epics_base_rs::types::{EpicsValue, PvString};
use epics_ca_rs::CaError as RsCaError;
use epics_ca_rs::server::CaServer as RsServer;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::error::{CaError, map_ca, map_ca_write};
use crate::runtime::{block_on, into_py_future, runtime};
use crate::value::{self, PutRequest};

/// Convert a Python value into the `EpicsValue` a served PV stores. A `str`
/// is a scalar string; a list of `str` a string array; everything else the
/// scalar/array a client PUT converts to (`crate::value::from_py`).
fn py_to_value(obj: &Bound<'_, PyAny>) -> PyResult<EpicsValue> {
    Ok(match value::from_py(obj)? {
        PutRequest::Str(s) => EpicsValue::String(PvString::from(s.as_str())),
        PutRequest::StrArray(v) => {
            EpicsValue::StringArray(v.iter().map(|s| PvString::from(s.as_str())).collect())
        }
        PutRequest::Value(ev) => ev,
    })
}

// ---------------------------------------------------------------------------
// CaServerOperation
// ---------------------------------------------------------------------------

/// `Ok(())` accepts the write (the handler is expected to have `post()`ed the
/// new value); `Err(text)` rejects it, and the client's `WRITE_NOTIFY` fails.
type OpReply = Result<(), String>;

/// A client PUT waiting for the Python handler (p4p `ServerOperation`).
#[pyclass(name = "CaServerOperation", module = "repics._repics", frozen)]
pub struct CaServerOperation {
    name: String,
    value: EpicsValue,
    ctx: WriteContext,
    reply: Mutex<Option<oneshot::Sender<OpReply>>>,
}

#[pymethods]
impl CaServerOperation {
    /// Always `"put"` — CA has no RPC.
    fn kind(&self) -> &'static str {
        "put"
    }

    /// The PV name the client addressed.
    fn name(&self) -> &str {
        &self.name
    }

    /// `"host:port"` of the client.
    fn peer(&self) -> &str {
        &self.ctx.peer
    }

    /// The client's CA `CLIENT_NAME` username, or empty.
    fn account(&self) -> &str {
        &self.ctx.user
    }

    /// The client's CA `HOST_NAME`, or the peer IP.
    fn host(&self) -> &str {
        &self.ctx.host
    }

    /// The value the client wrote, as a Python scalar/array/str.
    fn value(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        value::to_py(py, self.value.clone())
    }

    /// Complete the operation. `error` rejects the write (the client sees the
    /// failure); the default accepts it. Calling it twice is an error.
    #[pyo3(signature = (error=None))]
    fn done(&self, error: Option<String>) -> PyResult<()> {
        let tx = self
            .reply
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .ok_or_else(|| PyValueError::new_err("done() already called"))?;
        // The server task may have given up (client gone); nothing to do.
        let _ = tx.send(match error {
            Some(msg) => Err(msg),
            None => Ok(()),
        });
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// CaWorkQueue
// ---------------------------------------------------------------------------

enum EventKind {
    Put(CaServerOperation),
}

/// One item for Python: which PV (an opaque token Python registered, normally
/// a weakref to its `SharedPV`) and what happened.
struct Event {
    token: Arc<Py<PyAny>>,
    kind: EventKind,
}

/// The channel a Python drain loop pulls server PUTs from.
#[pyclass(name = "CaWorkQueue", module = "repics._repics", frozen)]
pub struct CaWorkQueue {
    tx: mpsc::UnboundedSender<Event>,
    /// Behind an `Arc` so `recv_async` can move it into a `'static` future.
    rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Event>>>,
    stopped: CancellationToken,
}

type Receiver = Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Event>>>;

impl CaWorkQueue {
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

type PyEvent = (Py<PyAny>, &'static str, Option<Py<CaServerOperation>>);

fn event_to_py(py: Python<'_>, ev: Event) -> PyResult<PyEvent> {
    let token = ev.token.clone_ref(py);
    Ok(match ev.kind {
        EventKind::Put(op) => (token, "put", Some(Py::new(py, op)?)),
    })
}

#[pymethods]
impl CaWorkQueue {
    #[new]
    fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        CaWorkQueue {
            tx,
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
            stopped: CancellationToken::new(),
        }
    }

    /// Block (GIL released) for the next event: `(token, "put", op)`. `None`
    /// once `stop()` was called.
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

    /// Wake the drain loop with `None`; later events are dropped, which fails
    /// their operations at the client.
    fn stop(&self) {
        self.stopped.cancel();
    }
}

// ---------------------------------------------------------------------------
// CaPvEntry
// ---------------------------------------------------------------------------

/// Where a served PV lives once a `CaServer` has adopted it: the database and
/// the name it was registered under, so `close`/`open` can remove and re-add
/// it while the server runs.
#[derive(Clone)]
struct Registration {
    db: Arc<PvDatabase>,
    name: String,
}

/// The value state of one `CaSharedPV`, single-owner per phase.
///
/// The value lives in exactly one place: `Detached` holds it before the PV is
/// served, `Serving` delegates to the live `ProcessVariable` cell, and
/// `Closed` has none. There is no value+flag pair whose meaning depends on
/// context.
enum ValueState {
    /// Never opened, or closed: not served, no live value.
    Closed,
    /// Opened but not yet adopted by a running server; the value is held here.
    Detached(EpicsValue),
    /// Adopted: the `ProcessVariable` cell in the database is the value.
    Serving(Arc<ProcessVariable>),
}

/// The Rust side of one Python `SharedPV`, shared (`Arc`) between the
/// `CaSharedPV` handle and the `CaProvider` that lists it.
struct CaPvEntry {
    state: Mutex<ValueState>,
    reg: Mutex<Option<Registration>>,
    /// Set once, at adoption, so the write hook can name the PV in its
    /// `CaServerOperation`.
    name: Arc<OnceLock<String>>,
    hook: WriteHook,
}

impl CaPvEntry {
    fn lock_state(&self) -> std::sync::MutexGuard<'_, ValueState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn registration(&self) -> Option<Registration> {
        self.reg.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Create the PV in `db` with its write hook and return the live cell.
    fn install(
        &self,
        py: Python<'_>,
        db: &Arc<PvDatabase>,
        name: &str,
        value: EpicsValue,
    ) -> PyResult<Arc<ProcessVariable>> {
        let hook = self.hook.clone();
        let db = db.clone();
        let name = name.to_string();
        block_on(py, async move {
            db.add_pv_with_hook(&name, value, hook)
                .await
                .map_err(map_ca_write)?;
            db.find_pv(&name)
                .await
                .ok_or_else(|| CaError::new_err(("registered PV not found", 0u32)))
        })
    }

    /// p4p `open`: declare the value; a running server serves it at once.
    fn open(&self, py: Python<'_>, value: EpicsValue) -> PyResult<()> {
        let mut st = self.lock_state();
        if let ValueState::Serving(pv) = &*st {
            let pv = pv.clone();
            drop(st);
            py.detach(|| pv.set(value));
            return Ok(());
        }
        match self.registration() {
            // Adopted but currently closed: re-create the cell in the database.
            Some(reg) => {
                let pv = self.install(py, &reg.db, &reg.name, value)?;
                *st = ValueState::Serving(pv);
            }
            // Not yet adopted: retain the value until the server picks it up.
            None => *st = ValueState::Detached(value),
        }
        Ok(())
    }

    /// pvxs `SharedPV::post`: update the value and fan it out to subscribers.
    fn post(&self, py: Python<'_>, value: EpicsValue) -> PyResult<()> {
        let mut st = self.lock_state();
        match &*st {
            ValueState::Serving(pv) => {
                let pv = pv.clone();
                drop(st);
                py.detach(|| pv.set(value));
                Ok(())
            }
            ValueState::Detached(_) => {
                *st = ValueState::Detached(value);
                Ok(())
            }
            ValueState::Closed => Err(CaError::new_err(("SharedPV not open", 0u32))),
        }
    }

    /// p4p `close`: stop serving. If adopted, remove the PV from the database,
    /// which destroys it and disconnects its clients; the registration is kept
    /// so a later `open` re-adds it.
    fn close(&self, py: Python<'_>) {
        let mut st = self.lock_state();
        if matches!(&*st, ValueState::Serving(_)) {
            if let Some(reg) = self.registration() {
                block_on(py, async move {
                    reg.db.remove_simple_pv(&reg.name).await;
                });
            }
        }
        *st = ValueState::Closed;
    }

    fn current(&self, py: Python<'_>) -> Option<EpicsValue> {
        let st = self.lock_state();
        match &*st {
            ValueState::Serving(pv) => {
                let pv = pv.clone();
                drop(st);
                Some(py.detach(|| pv.get()))
            }
            ValueState::Detached(v) => Some(v.clone()),
            ValueState::Closed => None,
        }
    }

    fn is_open(&self) -> bool {
        !matches!(&*self.lock_state(), ValueState::Closed)
    }

    /// Adopt this PV into a starting server's database under `name`. The PV
    /// must have been opened (it carries a value); a never-opened PV is an
    /// error, as a CA channel cannot be served without one.
    fn adopt(&self, py: Python<'_>, db: &Arc<PvDatabase>, name: &str) -> PyResult<()> {
        let mut st = self.lock_state();
        let value = match &*st {
            ValueState::Detached(v) => v.clone(),
            ValueState::Serving(_) => {
                return Err(PyValueError::new_err(format!(
                    "PV {name:?} is already served"
                )));
            }
            ValueState::Closed => {
                return Err(PyValueError::new_err(format!(
                    "PV {name:?} was never opened"
                )));
            }
        };
        let pv = self.install(py, db, name, value)?;
        let _ = self.name.set(name.to_string());
        *self.reg.lock().unwrap_or_else(|e| e.into_inner()) = Some(Registration {
            db: db.clone(),
            name: name.to_string(),
        });
        *st = ValueState::Serving(pv);
        Ok(())
    }
}

/// Build the write hook that routes a client PUT to Python. It captures the
/// queue and the PV token, enqueues a `CaServerOperation`, and awaits the
/// handler's verdict. Returning `Ok(())` tells the CA server the write was
/// handled (the handler `post()`ed the value); `Err` fails it at the client.
fn make_hook(
    events: mpsc::UnboundedSender<Event>,
    token: Arc<Py<PyAny>>,
    name: Arc<OnceLock<String>>,
) -> WriteHook {
    Arc::new(move |value: EpicsValue, ctx: WriteContext| {
        let events = events.clone();
        let token = token.clone();
        let name = name.clone();
        Box::pin(async move {
            let (tx, rx) = oneshot::channel();
            let op = CaServerOperation {
                name: name.get().cloned().unwrap_or_default(),
                value,
                ctx,
                reply: Mutex::new(Some(tx)),
            };
            if events
                .send(Event {
                    token,
                    kind: EventKind::Put(op),
                })
                .is_err()
            {
                return Err(RsCaError::InvalidValue(
                    "SharedPV handler queue is stopped".into(),
                ));
            }
            match rx.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(msg)) => Err(RsCaError::InvalidValue(msg)),
                Err(_) => Err(RsCaError::InvalidValue(
                    "handler dropped the operation without done()".into(),
                )),
            }
        })
    })
}

// ---------------------------------------------------------------------------
// CaSharedPV
// ---------------------------------------------------------------------------

/// The Rust half of `repics.ca.server.SharedPV`.
#[pyclass(name = "CaSharedPV", module = "repics._repics", frozen)]
pub struct CaSharedPV {
    entry: Arc<CaPvEntry>,
}

#[pymethods]
impl CaSharedPV {
    /// `queue` receives this PV's PUTs, each tagged with `token` (Python
    /// passes a weakref to its `SharedPV`).
    #[new]
    fn new(queue: &CaWorkQueue, token: Py<PyAny>) -> Self {
        let name = Arc::new(OnceLock::new());
        let hook = make_hook(queue.tx.clone(), Arc::new(token), name.clone());
        CaSharedPV {
            entry: Arc::new(CaPvEntry {
                state: Mutex::new(ValueState::Closed),
                reg: Mutex::new(None),
                name,
                hook,
            }),
        }
    }

    /// Declare the value; a running server serves it at once.
    fn open(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let v = py_to_value(value)?;
        self.entry.open(py, v)
    }

    /// Update the value and deliver it to every monitor.
    fn post(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let v = py_to_value(value)?;
        self.entry.post(py, v)
    }

    /// Stop serving and disconnect every client.
    fn close(&self, py: Python<'_>) {
        self.entry.close(py);
    }

    #[pyo3(name = "isOpen")]
    fn is_open(&self) -> bool {
        self.entry.is_open()
    }

    /// The current value, or `None` while closed.
    fn current(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.entry.current(py) {
            Some(v) => Ok(Some(value::to_py(py, v)?)),
            None => Ok(None),
        }
    }
}

// ---------------------------------------------------------------------------
// CaProvider
// ---------------------------------------------------------------------------

/// A named, ordered table of PVs (p4p `StaticProvider`). CA serves one flat
/// database, so the name is informational; the PVs of every provider are
/// merged into the server's database at start.
#[pyclass(name = "CaProvider", module = "repics._repics", frozen)]
pub struct CaProvider {
    name: String,
    pvs: Mutex<Vec<(String, Arc<CaPvEntry>)>>,
}

impl CaProvider {
    fn entries(&self) -> Vec<(String, Arc<CaPvEntry>)> {
        self.pvs.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

#[pymethods]
impl CaProvider {
    #[new]
    fn new(name: String) -> Self {
        CaProvider {
            name,
            pvs: Mutex::new(Vec::new()),
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn add(&self, name: String, pv: &CaSharedPV) -> PyResult<()> {
        let mut pvs = self.pvs.lock().unwrap_or_else(|e| e.into_inner());
        if pvs.iter().any(|(n, _)| n == &name) {
            return Err(PyValueError::new_err(format!("PV {name:?} already added")));
        }
        pvs.push((name, pv.entry.clone()));
        Ok(())
    }

    /// Remove a PV. If the server is running it is also removed from the
    /// database (its clients disconnect).
    fn remove(&self, py: Python<'_>, name: &str) -> bool {
        let entry = {
            let mut pvs = self.pvs.lock().unwrap_or_else(|e| e.into_inner());
            pvs.iter()
                .position(|(n, _)| n == name)
                .map(|i| pvs.remove(i).1)
        };
        match entry {
            Some(e) => {
                e.close(py);
                true
            }
            None => false,
        }
    }

    fn keys(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .pvs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(n, _)| n.clone())
            .collect();
        names.sort();
        names
    }
}

// ---------------------------------------------------------------------------
// CaServer
// ---------------------------------------------------------------------------

/// The default CA server port when neither `conf` nor the environment names
/// one (libca `EPICS_CA_SERVER_PORT`).
const DEFAULT_CA_PORT: u16 = 5064;

fn resolve_port(conf: &Option<HashMap<String, String>>, useenv: bool, isolate: bool) -> u16 {
    if isolate {
        return 0;
    }
    const KEYS: [&str; 2] = ["EPICS_CAS_SERVER_PORT", "EPICS_CA_SERVER_PORT"];
    if let Some(c) = conf {
        for k in KEYS {
            if let Some(v) = c.get(k) {
                if let Ok(p) = v.trim().parse::<u16>() {
                    return p;
                }
            }
        }
    }
    if useenv {
        for k in KEYS {
            if let Ok(v) = std::env::var(k) {
                if let Ok(p) = v.trim().parse::<u16>() {
                    return p;
                }
            }
        }
    }
    DEFAULT_CA_PORT
}

struct ServerState {
    server: Arc<RsServer>,
    handle: JoinHandle<()>,
    /// Kept alive so the served `ProcessVariable`s outlive the accept loop.
    _db: Arc<PvDatabase>,
}

/// A running Channel Access server over one or more providers.
#[pyclass(name = "CaServer", module = "repics._repics", frozen)]
pub struct CaServer {
    inner: Mutex<Option<ServerState>>,
    tcp_port: u16,
    udp_port: u16,
    addr: IpAddr,
}

#[pymethods]
impl CaServer {
    /// `isolate` binds ephemeral ports so a test never touches 5064;
    /// `conf`/`useenv` supply `EPICS_CA[S]_SERVER_PORT` otherwise.
    #[new]
    #[pyo3(signature = (providers, conf=None, useenv=true, isolate=false))]
    fn new(
        py: Python<'_>,
        providers: Vec<PyRef<'_, CaProvider>>,
        conf: Option<HashMap<String, String>>,
        useenv: bool,
        isolate: bool,
    ) -> PyResult<Self> {
        let db = Arc::new(PvDatabase::new());
        for p in &providers {
            for (name, entry) in p.entries() {
                entry.adopt(py, &db, &name)?;
            }
        }
        let port = resolve_port(&conf, useenv, isolate);
        let acf = new_acf_cell(None);
        let server = block_on(
            py,
            RsServer::from_parts(db.clone(), port, None, acf, None, None),
        )
        .map_err(map_ca)?;
        let tcp_port = server.tcp_port();
        let udp_port = server.udp_port();
        let server = Arc::new(server);
        let run = server.clone();
        let handle = runtime().spawn(async move {
            let _ = run.run().await;
        });
        Ok(CaServer {
            inner: Mutex::new(Some(ServerState {
                server,
                handle,
                _db: db,
            })),
            tcp_port,
            udp_port,
            addr: IpAddr::from([127, 0, 0, 1]),
        })
    }

    /// Stop serving and disconnect every client. Idempotent.
    fn stop(&self, py: Python<'_>) {
        let state = self.inner.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(s) = state {
            block_on(py, async move {
                s.handle.abort();
                let _ = s.handle.await;
                drop(s.server);
                drop(s._db);
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
        self.tcp_port
    }

    fn udp_port(&self) -> u16 {
        self.udp_port
    }

    /// A libca-style `conf()` dict that points a client at exactly this
    /// server: the search list carries the server's UDP port explicitly, so
    /// no beacon or broadcast is needed.
    fn conf<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        let addr = self.addr.to_string();
        d.set_item("EPICS_CA_ADDR_LIST", format!("{addr}:{}", self.udp_port))?;
        d.set_item("EPICS_CA_AUTO_ADDR_LIST", "NO")?;
        d.set_item("EPICS_CA_SERVER_PORT", self.udp_port.to_string())?;
        Ok(d)
    }
}
