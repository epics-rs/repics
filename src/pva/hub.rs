//! `PvaMonitorHub`: many monitors, one wake.
//!
//! Every monitor keeps its own bounded queue, filled by its wire callback
//! on a runtime worker: the frame is decoded, completed from the previous
//! value, and, when the queue already holds `limit` values, squashed into
//! the tail (newer wins, changed sets union), the pvxs client rule, so
//! memory stays bounded however slowly Python drains. The hub carries only
//! *which* monitors have something: a monitor signals its id once when its
//! queue goes from drained to non-empty, and `recv_batch` takes every
//! signalled monitor's queue under one GIL acquisition. There is no task
//! or thread per monitor, and a slow consumer squashes at its own limit
//! while the others flow.

use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use epics_pva_rs::client_native::PvaClient;
use epics_pva_rs::client_native::ops_v2::{MonitorConnEvent, Pauser, SubscriptionHandle};
use epics_pva_rs::proto::{BitSet, ByteOrder};
use epics_pva_rs::pv_request::PvRequestExpr;
use epics_pva_rs::pvdata::encode::{decode_pv_field_with_bitset, fill_unmarked_from_prior};
use epics_pva_rs::pvdata::{FieldDesc, PvField};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use tokio::sync::mpsc;

use super::client::{PvaContext, parse_request, queue_limit};
use super::error::{bounded, map_pva};
use super::value::Value;
use crate::ca::Drain;
use crate::runtime::{block_on, into_py_future};

/// The item kinds `recv_batch` yields; mirrored in `epicsrs.pva._monitor`.
pub const KIND_VALUE: u8 = 0;
pub const KIND_CONNECTED: u8 = 1;
pub const KIND_DISCONNECTED: u8 = 2;
pub const KIND_FINISHED: u8 = 3;

/// One entry of a batch: `(subscription id, kind, payload)`.
type BatchItem = (u32, u8, Py<PyAny>);

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
    /// The id is in the hub's ready channel and nothing has been taken
    /// since; the next push must not signal again.
    signalled: bool,
}

/// One monitor's queue and its line to the hub.
struct Monitor {
    id: u32,
    queue: Mutex<Queue>,
    ready: mpsc::UnboundedSender<u32>,
}

impl Monitor {
    fn lock(&self) -> MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Tell the hub, once per drain, that there is something to take.
    fn signal(&self, mut q: MutexGuard<'_, Queue>) {
        if !q.signalled {
            q.signalled = true;
            drop(q);
            // A closed hub dropped its receiver; the queue keeps filling
            // to its limit and squashing until the monitor is dropped.
            let _ = self.ready.send(self.id);
        }
    }

    fn push(&self, u: Update) {
        let mut q = self.lock();
        q.items.push_back(u);
        self.signal(q);
    }

    /// Squash into the tail when the queue holds `limit` values and the
    /// tail is a value: newer wins, changed sets union.
    fn push_value(&self, desc: Arc<FieldDesc>, value: PvField, changed: BitSet) {
        let mut q = self.lock();
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
                return self.signal(q);
            }
        }
        q.items.push_back(Update::Value {
            desc,
            value,
            changed,
        });
        self.signal(q);
    }

    /// Everything queued; the next push signals again.
    fn take(&self) -> VecDeque<Update> {
        let mut q = self.lock();
        q.signalled = false;
        std::mem::take(&mut q.items)
    }
}

/// Per-monitor decode state: the last full value, so a partial update
/// can be completed from it.
#[derive(Default)]
struct Decoder {
    desc: Option<Arc<FieldDesc>>,
    prior: Option<PvField>,
}

impl Decoder {
    fn decode(
        &mut self,
        desc: &FieldDesc,
        body: &[u8],
        order: ByteOrder,
    ) -> Option<(Arc<FieldDesc>, PvField, BitSet)> {
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
        Some((desc_arc, full, changed))
    }
}

async fn open(
    client: Arc<PvaClient>,
    name: String,
    request: Option<PvRequestExpr>,
    monitor: Arc<Monitor>,
) -> PyResult<SubscriptionHandle> {
    let pv_request = request
        .unwrap_or_else(|| PvRequestExpr::parse("field()").expect("field() parses"))
        .to_pv_field();
    let mut decoder = Decoder::default();
    let values = monitor.clone();
    let conn = monitor;
    client
        .pvmonitor_raw_frames_handle_with_request(
            &name,
            pv_request,
            move |desc: &FieldDesc, body, order: ByteOrder| {
                if let Some((desc, value, changed)) = decoder.decode(desc, body.as_ref(), order) {
                    values.push_value(desc, value, changed);
                }
            },
            move |ev: MonitorConnEvent| {
                conn.push(match ev {
                    MonitorConnEvent::Connected { peer } => Update::Connected(peer),
                    MonitorConnEvent::Disconnected => Update::Disconnected,
                    MonitorConnEvent::Finished => Update::Finished,
                })
            },
        )
        .await
        .map_err(map_pva)
}

struct Entry {
    monitor: Arc<Monitor>,
    /// `None` only while `subscribe` is opening the monitor; the entry is
    /// registered first so an item pushed during the open finds it.
    control: Option<(Pauser, SubscriptionHandle)>,
}

type Subs = Arc<Mutex<HashMap<u32, Entry>>>;

fn lock(subs: &Subs) -> MutexGuard<'_, HashMap<u32, Entry>> {
    subs.lock().unwrap_or_else(|e| e.into_inner())
}

/// One wake for any number of monitors. See the module docs.
#[pyclass(frozen, module = "epicsrs._epicsrs")]
pub struct PvaMonitorHub {
    ready: mpsc::UnboundedSender<u32>,
    drain: Drain<mpsc::UnboundedReceiver<u32>>,
    subs: Subs,
    next_id: AtomicU32,
}

impl PvaMonitorHub {
    /// Wait for the first signalled monitor, then take the queue of every
    /// monitor signalled so far. `None` once the hub is closed.
    async fn do_recv_batch(
        drain: Drain<mpsc::UnboundedReceiver<u32>>,
        subs: Subs,
        timeout: Option<f64>,
    ) -> PyResult<Option<Vec<BatchItem>>> {
        let ids = bounded(timeout, async {
            Ok(drain
                .pull(|rx| {
                    Box::pin(async move {
                        let first = rx.recv().await?;
                        let mut ids = vec![first];
                        while let Ok(id) = rx.try_recv() {
                            ids.push(id);
                        }
                        Some(ids)
                    })
                })
                .await)
        })
        .await?;
        let Some(ids) = ids else {
            return Ok(None);
        };
        let taken: Vec<(u32, VecDeque<Update>)> = {
            let subs = lock(&subs);
            ids.into_iter()
                .filter_map(|id| subs.get(&id).map(|e| (id, e.monitor.take())))
                .collect()
        };
        Python::attach(|py| {
            let mut items = Vec::new();
            for (id, updates) in taken {
                for u in updates {
                    items.push(match u {
                        Update::Value {
                            desc,
                            value,
                            changed,
                        } => (
                            id,
                            KIND_VALUE,
                            Value::from_parts(desc, value, changed).into_py_any(py)?,
                        ),
                        Update::Connected(peer) => {
                            (id, KIND_CONNECTED, peer.to_string().into_py_any(py)?)
                        }
                        Update::Disconnected => (id, KIND_DISCONNECTED, py.None()),
                        Update::Finished => (id, KIND_FINISHED, py.None()),
                    });
                }
            }
            Ok(Some(items))
        })
    }

    fn pauser(&self, id: u32) -> Option<Pauser> {
        lock(&self.subs)
            .get(&id)
            .and_then(|e| e.control.as_ref())
            .map(|(p, _)| p.clone())
    }
}

#[pymethods]
impl PvaMonitorHub {
    #[new]
    fn new() -> Self {
        let (ready, rx) = mpsc::unbounded_channel();
        Self {
            ready,
            drain: Drain::new(rx),
            subs: Arc::new(Mutex::new(HashMap::new())),
            next_id: AtomicU32::new(1),
        }
    }

    /// Open a monitor on `context`; returns its id. `limit` bounds its
    /// queue: the request's `queueSize` record option by default, else 4.
    /// The queue starts with a `disconnected` item, the state before the
    /// server answers, so a subscriber that reports disconnection sees it
    /// first.
    #[pyo3(signature = (context, name, request=None, limit=None))]
    fn subscribe(
        &self,
        py: Python<'_>,
        context: PyRef<'_, PvaContext>,
        name: String,
        request: Option<&str>,
        limit: Option<usize>,
    ) -> PyResult<u32> {
        let req = parse_request(request)?;
        let limit = queue_limit(req.as_ref(), limit);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let monitor = Arc::new(Monitor {
            id,
            queue: Mutex::new(Queue {
                items: VecDeque::from([Update::Disconnected]),
                limit,
                signalled: false,
            }),
            ready: self.ready.clone(),
        });
        lock(&self.subs).insert(
            id,
            Entry {
                monitor: monitor.clone(),
                control: None,
            },
        );
        let client = context.client();
        let handle = match block_on(py, open(client, name, req, monitor.clone())) {
            Ok(h) => h,
            Err(e) => {
                lock(&self.subs).remove(&id);
                return Err(e);
            }
        };
        if let Some(e) = lock(&self.subs).get_mut(&id) {
            e.control = Some((handle.pauser(), handle));
        }
        monitor.signal(monitor.lock());
        Ok(id)
    }

    /// Stop one monitor and wait for its teardown. Items it had queued
    /// are discarded with it.
    fn close_subscription(&self, py: Python<'_>, id: u32) {
        let entry = lock(&self.subs).remove(&id);
        if let Some(Entry {
            control: Some((_, handle)),
            ..
        }) = entry
        {
            block_on(py, handle.stop_sync());
        }
    }

    /// Hold server emissions for `id`; a queued value is kept for `resume`.
    fn pause(&self, py: Python<'_>, id: u32) {
        if let Some(p) = self.pauser(id) {
            block_on(py, async move { p.pause().await });
        }
    }

    fn resume(&self, py: Python<'_>, id: u32) {
        if let Some(p) = self.pauser(id) {
            block_on(py, async move { p.resume().await });
        }
    }

    /// Next batch of `(id, kind, payload)` tuples, each monitor's items in
    /// arrival order: kind 0 a `Value`, 1 the server address the monitor
    /// connected to, 2/3 (`None`) disconnected/finished. Waits for the
    /// first item, bounded by `timeout` (raises `PvaTimeout`); `None`
    /// once the hub is closed.
    #[pyo3(signature = (timeout=None))]
    fn recv_batch(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Option<Vec<BatchItem>>> {
        block_on(
            py,
            Self::do_recv_batch(self.drain.clone(), self.subs.clone(), timeout),
        )
    }

    #[pyo3(signature = (timeout=None))]
    fn recv_batch_async<'py>(
        &self,
        py: Python<'py>,
        timeout: Option<f64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        into_py_future(
            py,
            Self::do_recv_batch(self.drain.clone(), self.subs.clone(), timeout),
        )
    }

    /// Number of monitors not yet closed by `close_subscription`.
    fn __len__(&self) -> usize {
        lock(&self.subs).len()
    }

    /// Drop every monitor (each sends its own teardown) and close the
    /// queue. A parked `recv_batch` returns `None`.
    fn close(&self, py: Python<'_>) {
        let dropped: Vec<Entry> = lock(&self.subs).drain().map(|(_, e)| e).collect();
        block_on(py, async move {
            drop(dropped);
        });
        block_on(py, self.drain.close());
    }
}
