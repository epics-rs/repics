//! `MonitorHub`: many subscriptions, one stream.
//!
//! `camonitor` on hundreds of PVs must not cost a thread wake and a GIL
//! handoff per update. The hub runs every subscription as a task on the
//! runtime and funnels what they produce into one queue; Python drains it
//! with `recv_batch`, converting a whole batch under one GIL acquisition
//! and dispatching to callbacks by subscription id. The per-subscription
//! lifecycle (connect, connect-timeout notice, subscribe, deliver,
//! disconnect, reconnect) lives entirely in the task, so Python holds
//! nothing but the id.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use epics_base_rs::server::snapshot::Snapshot as RsSnapshot;
use epics_ca_rs::CaError as RsCaError;
use epics_ca_rs::client::{
    CaChannel as RsChannel, ConnectionEvent as RsConnectionEvent, EnumReadback,
};
use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use tokio::sync::{broadcast, mpsc};

use crate::ca::{CaChannel, Drain, Snapshot, duration_or_forever, monitor_count};
use crate::error::map_ca;
use crate::runtime::{block_on, bounded, into_py_future, runtime};

/// What a subscription task tells Python. The `u8` codes are the wire
/// between the two sides and are mirrored in `repics._monitor`.
enum Item {
    /// A monitor update, with the channel name the snapshot is stamped with.
    Value(u32, Arc<str>, Box<RsSnapshot>),
    /// A monitor error, `ECA_DISCONN` included; the subscription stays up.
    Error(u32, RsCaError),
    /// `connect_timeout` passed before the channel connected; still waiting.
    ConnectTimeout(u32),
    /// The channel connected (initially and after every reconnection).
    Connected(u32),
    /// The subscription is over: closed, or its channel was torn down.
    End(u32),
    /// The channel lost its circuit.
    Disconnected(u32),
    /// The server changed what this client may do with the channel.
    AccessRights(u32, bool, bool),
}

/// One entry of a batch: `(subscription id, kind, payload)`.
type BatchItem = (u32, u8, Py<PyAny>);

pub const KIND_VALUE: u8 = 0;
pub const KIND_ERROR: u8 = 1;
pub const KIND_CONNECT_TIMEOUT: u8 = 2;
pub const KIND_CONNECTED: u8 = 3;
pub const KIND_END: u8 = 4;
pub const KIND_DISCONNECTED: u8 = 5;
pub const KIND_ACCESS_RIGHTS: u8 = 6;

enum Ctrl {
    Pause,
    Resume,
    Close,
}

/// How many items may sit between the tasks and Python before the tasks
/// stop forwarding and the per-monitor queues fill up (and coalesce).
const QUEUE: usize = 8192;

/// Arguments of one subscription, as `CaChannel.subscribe` takes them.
pub struct SubscribeArgs {
    pub deadband: f64,
    pub mask: u16,
    pub enum_as_string: bool,
    pub float_as_string: bool,
    pub count: i64,
    pub connect_timeout: Option<f64>,
    /// False for a lifecycle-only subscription: connection and access
    /// items, no monitor.
    pub values: bool,
    /// With `values`, monitor only a channel of at most this many
    /// elements; a larger one is followed lifecycle-only. pyepics'
    /// `auto_monitor=None` rule.
    pub values_max_count: Option<u32>,
}

async fn run_subscription(
    id: u32,
    name: Arc<str>,
    ch: RsChannel,
    args: SubscribeArgs,
    tx: mpsc::Sender<Item>,
    mut ctrl: mpsc::UnboundedReceiver<Ctrl>,
) {
    // A closed hub drops its receiver; every send then fails and the task
    // ends, dropping its monitor handle, which unsubscribes.
    macro_rules! send {
        ($item:expr) => {
            if tx.send($item).await.is_err() {
                return;
            }
        };
    }

    // Connection state is forwarded as transitions only, whichever way it
    // is learned (the initial probe, the event stream, or a resync after
    // the event stream lagged).
    let mut events = ch.connection_events();
    let mut connected = ch.native_field_type().is_ok();
    if connected {
        send!(Item::Connected(id));
    }
    macro_rules! forward_event {
        ($ev:expr) => {
            match $ev {
                Ok(RsConnectionEvent::Connected)
                | Ok(RsConnectionEvent::NativeTypeChanged { .. }) => {
                    if !connected {
                        connected = true;
                        send!(Item::Connected(id));
                    }
                }
                Ok(RsConnectionEvent::Disconnected) => {
                    if connected {
                        connected = false;
                        send!(Item::Disconnected(id));
                    }
                }
                Ok(RsConnectionEvent::AccessRightsChanged { read, write }) => {
                    send!(Item::AccessRights(id, read, write));
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let now = ch.native_field_type().is_ok();
                    if now != connected {
                        connected = now;
                        send!(if now {
                            Item::Connected(id)
                        } else {
                            Item::Disconnected(id)
                        });
                    }
                }
                Err(broadcast::error::RecvError::Closed) => {
                    send!(Item::End(id));
                    return;
                }
            }
        };
    }

    // Phase 1: wait for the first connection. `connect_timeout` only
    // reports that the deadline passed; the wait goes on. Pause/resume
    // before subscribing is remembered.
    let mut paused = false;
    let connect = ch.wait_connected(duration_or_forever(None));
    tokio::pin!(connect);
    let mut deadline = args
        .connect_timeout
        .map(|secs| Box::pin(tokio::time::sleep(Duration::from_secs_f64(secs.max(0.0)))));
    let outcome = loop {
        tokio::select! {
            r = &mut connect => break r,
            () = async { deadline.as_mut().expect("guarded").await }, if deadline.is_some() => {
                deadline = None;
                send!(Item::ConnectTimeout(id));
            }
            ev = events.recv() => forward_event!(ev),
            c = ctrl.recv() => match c {
                Some(Ctrl::Pause) => paused = true,
                Some(Ctrl::Resume) => paused = false,
                Some(Ctrl::Close) | None => {
                    send!(Item::End(id));
                    return;
                }
            },
        }
    };
    if let Err(e) = outcome {
        send!(Item::Error(id, e));
        send!(Item::End(id));
        return;
    }
    if !connected {
        // Connected before the event stream reported it.
        connected = true;
        send!(Item::Connected(id));
    }

    // Phase 2: subscribe, unless this is a lifecycle-only subscription.
    let want_values = args.values
        && args
            .values_max_count
            .is_none_or(|m| ch.element_count().is_ok_and(|n| n <= m));
    let mut handle = if want_values {
        let readback = if args.enum_as_string {
            EnumReadback::Label
        } else {
            EnumReadback::Native
        };
        let cap = match monitor_count(&ch, args.count) {
            Ok(cap) => cap,
            Err(e) => {
                send!(Item::Error(id, e));
                send!(Item::End(id));
                return;
            }
        };
        match ch
            .subscribe_with_mask_readback_count(
                args.deadband,
                args.mask,
                readback,
                args.float_as_string,
                cap,
            )
            .await
        {
            Ok(h) => {
                if paused {
                    h.pause();
                }
                Some(h)
            }
            Err(e) => {
                send!(Item::Error(id, e));
                send!(Item::End(id));
                return;
            }
        }
    } else {
        None
    };

    // Phase 3: forward until closed.
    loop {
        tokio::select! {
            item = async { handle.as_mut().expect("guarded").recv().await }, if handle.is_some() => match item {
                None => {
                    send!(Item::End(id));
                    return;
                }
                Some(Ok(snap)) => send!(Item::Value(id, name.clone(), Box::new(snap))),
                Some(Err(e)) => send!(Item::Error(id, e)),
            },
            ev = events.recv() => forward_event!(ev),
            c = ctrl.recv() => match c {
                Some(Ctrl::Pause) => {
                    if let Some(h) = &handle {
                        h.pause();
                    }
                }
                Some(Ctrl::Resume) => {
                    if let Some(h) = &handle {
                        h.resume();
                    }
                }
                Some(Ctrl::Close) | None => {
                    drop(handle);
                    send!(Item::End(id));
                    return;
                }
            },
        }
    }
}

/// One queue fed by any number of subscriptions. See the module docs.
#[pyclass(frozen, module = "repics._repics")]
pub struct MonitorHub {
    tx: mpsc::Sender<Item>,
    drain: Drain<mpsc::Receiver<Item>>,
    subs: Mutex<HashMap<u32, mpsc::UnboundedSender<Ctrl>>>,
    next_id: AtomicU32,
}

impl MonitorHub {
    fn send_ctrl(&self, id: u32, c: Ctrl) {
        let subs = self.subs.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(tx) = subs.get(&id) {
            // A task that already ended dropped its receiver; nothing to do.
            let _ = tx.send(c);
        }
    }

    /// Wait for the first item, then take everything already queued, up
    /// to `max_items` (0 for no cap). `None` once the hub is closed.
    async fn do_recv_batch(
        drain: Drain<mpsc::Receiver<Item>>,
        max_items: usize,
        timeout: Option<f64>,
    ) -> PyResult<Option<Vec<BatchItem>>> {
        let cap = if max_items == 0 {
            usize::MAX
        } else {
            max_items
        };
        let items = bounded(timeout, async {
            Ok(drain
                .pull(|rx| {
                    Box::pin(async move {
                        let first = rx.recv().await?;
                        let mut items = vec![first];
                        while items.len() < cap {
                            match rx.try_recv() {
                                Ok(item) => items.push(item),
                                Err(_) => break,
                            }
                        }
                        Some(items)
                    })
                })
                .await)
        })
        .await?;
        let Some(items) = items else {
            return Ok(None);
        };
        Python::attach(|py| {
            items
                .into_iter()
                .map(|item| {
                    Ok(match item {
                        Item::Value(id, name, snap) => (
                            id,
                            KIND_VALUE,
                            Snapshot::from_rs(py, &name, *snap)?
                                .into_pyobject(py)?
                                .into_any()
                                .unbind(),
                        ),
                        Item::Error(id, e) => (id, KIND_ERROR, map_ca(e).into_value(py).into_any()),
                        Item::ConnectTimeout(id) => (id, KIND_CONNECT_TIMEOUT, py.None()),
                        Item::Connected(id) => (id, KIND_CONNECTED, py.None()),
                        Item::End(id) => (id, KIND_END, py.None()),
                        Item::Disconnected(id) => (id, KIND_DISCONNECTED, py.None()),
                        Item::AccessRights(id, read, write) => {
                            (id, KIND_ACCESS_RIGHTS, (read, write).into_py_any(py)?)
                        }
                    })
                })
                .collect::<PyResult<Vec<_>>>()
                .map(Some)
        })
    }
}

#[pymethods]
impl MonitorHub {
    #[new]
    fn new() -> Self {
        let (tx, rx) = mpsc::channel(QUEUE);
        Self {
            tx,
            drain: Drain::new(rx),
            subs: Mutex::new(HashMap::new()),
            next_id: AtomicU32::new(1),
        }
    }

    /// Start a subscription on `channel`; returns its id. The task
    /// connects first (reporting `connect_timeout` if it passes), then
    /// subscribes with the same arguments as `CaChannel.subscribe`, and
    /// reports connection and access-rights changes for as long as it
    /// runs. With `values=False` it reports only those; with
    /// `values_max_count` a channel with more elements is treated so.
    #[pyo3(signature = (channel, deadband=0.0, mask=None, enum_as_string=false, float_as_string=false, count=0, connect_timeout=None, values=true, values_max_count=None))]
    #[allow(clippy::too_many_arguments)]
    fn subscribe(
        &self,
        channel: PyRef<'_, CaChannel>,
        deadband: f64,
        mask: Option<u16>,
        enum_as_string: bool,
        float_as_string: bool,
        count: i64,
        connect_timeout: Option<f64>,
        values: bool,
        values_max_count: Option<u32>,
    ) -> u32 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (ctrl_tx, ctrl_rx) = mpsc::unbounded_channel();
        self.subs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, ctrl_tx);
        let args = SubscribeArgs {
            deadband,
            mask: mask.unwrap_or(CaChannel::DEFAULT_MASK),
            enum_as_string,
            float_as_string,
            count,
            connect_timeout,
            values,
            values_max_count,
        };
        runtime().spawn(run_subscription(
            id,
            Arc::from(channel.pv_name()),
            channel.inner().clone(),
            args,
            self.tx.clone(),
            ctrl_rx,
        ));
        id
    }

    /// Stop one subscription. Its `End` item follows whatever it had
    /// already queued.
    fn close_subscription(&self, id: u32) {
        let tx = self
            .subs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
        if let Some(tx) = tx {
            let _ = tx.send(Ctrl::Close);
        }
    }

    fn pause(&self, id: u32) {
        self.send_ctrl(id, Ctrl::Pause);
    }

    fn resume(&self, id: u32) {
        self.send_ctrl(id, Ctrl::Resume);
    }

    /// Next batch of `(id, kind, payload)` tuples, oldest first: kind 0 a
    /// `Snapshot`, 1 an exception, 2/3/4/5 (`None`) connect-timeout,
    /// connected, end, disconnected, 6 an access-rights `(read, write)`
    /// pair. Waits for the first item, bounded by `timeout` (raises
    /// `CaTimeout`); `None` once the hub is closed.
    #[pyo3(signature = (max_items=0, timeout=None))]
    fn recv_batch(
        &self,
        py: Python<'_>,
        max_items: usize,
        timeout: Option<f64>,
    ) -> PyResult<Option<Vec<BatchItem>>> {
        block_on(
            py,
            Self::do_recv_batch(self.drain.clone(), max_items, timeout),
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
            Self::do_recv_batch(self.drain.clone(), max_items, timeout),
        )
    }

    /// Number of subscriptions not yet closed by `close_subscription`.
    fn __len__(&self) -> usize {
        self.subs.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Close every subscription and the queue. A parked `recv_batch`
    /// returns `None`.
    fn close(&self, py: Python<'_>) {
        self.subs.lock().unwrap_or_else(|e| e.into_inner()).clear();
        block_on(py, self.drain.close());
    }
}
