"""Subscriptions over one hub per front end.

A hub (``MonitorHub`` for Channel Access, ``PvaMonitorHub`` for pvAccess)
runs every subscription inside the extension and queues what they
produce. A single dispatcher drains it a batch at a time and hands each
item to its subscription by id, so a thousand monitors cost one waiting
thread (or task), not a thousand, and one GIL acquisition per batch rather
than per update.

``ThreadDispatcher`` drains on a daemon thread and calls back there;
``LoopDispatcher`` drains in a task on an asyncio loop and awaits
coroutine callbacks. What a subscription makes of an item (``_event``)
and how its callback is invoked (``_deliver``) belong to the front end.
The Channel Access subscription is below; ``epicsrs.pva._monitor`` has
the pvAccess one.
"""

from __future__ import annotations

import asyncio
import atexit
import sys
import threading
import traceback
import weakref
from typing import Any, Callable

from . import _context
from ._context import Deadline, channel
from ._dbr import (
    DBE_ALARM,
    DBE_PROPERTY,
    DBE_VALUE,
    DBR_CHAR_BYTES,
    DBR_CHAR_STR,
    DBR_CHAR_UNICODE,
    DBR_ENUM_STR,
    ECA_TIMEOUT,
)
from ._epicsrs import MonitorHub, Snapshot
from ._value import CaNothing, augment

MONITOR_DATATYPES = (None, str, DBR_ENUM_STR, DBR_CHAR_STR, DBR_CHAR_BYTES, DBR_CHAR_UNICODE)

# The item kinds `MonitorHub.recv_batch` yields.
KIND_VALUE = 0
KIND_ERROR = 1
KIND_CONNECT_TIMEOUT = 2
KIND_CONNECTED = 3
KIND_END = 4
KIND_DISCONNECTED = 5
KIND_ACCESS_RIGHTS = 6

# The events a monitor asks for unless told otherwise, by form: a metadata
# form wants to hear about alarm changes, ``ctrl`` also about property
# changes (limits, units, precision).
_DEFAULT_MASK = {
    "plain": DBE_VALUE,
    "sts": DBE_VALUE | DBE_ALARM,
    "time": DBE_VALUE | DBE_ALARM,
    "gr": DBE_VALUE | DBE_ALARM | DBE_PROPERTY,
    "ctrl": DBE_VALUE | DBE_ALARM | DBE_PROPERTY,
}


def default_mask(form: str, mask: int | None) -> int:
    return _DEFAULT_MASK[form] if mask is None else mask


class Dispatcher:
    """The subscriptions of one hub, by id, and the collapse policy.

    ``add`` and the lookup in ``plan`` share one lock so an item can never
    reach the dispatcher before its subscription is registered.

    Every dispatcher is shut down at interpreter exit: a thread still
    parked in the extension when finalization starts would be torn down
    by the interpreter mid-call (Python 3.12 exits such threads with
    ``pthread_exit``, which aborts through Rust frames), so the hub is
    closed and the thread joined while the interpreter is still whole.
    """

    def __init__(self, hub: Any) -> None:
        self.hub = hub
        self.subs: dict[int, Any] = {}
        self.lock = threading.Lock()
        _live.add(self)

    def add(self, sub: Any, *args: Any, **kw: Any) -> int:
        """Open ``hub.subscribe(*args, **kw)`` for ``sub``; its id."""
        with self.lock:
            sid = self.hub.subscribe(*args, **kw)
            self.subs[sid] = sub
        self.started()
        return sid

    def remove(self, sid: int) -> None:
        with self.lock:
            self.subs.pop(sid, None)
        self.hub.close_subscription(sid)

    def started(self) -> None:
        """Make sure the drain is running; the front end supplies it."""

    def shutdown(self) -> None:
        """Close every subscription and stop the drain."""
        with self.lock:
            subs = list(self.subs.values())
            self.subs.clear()
        for sub in subs:
            sub._mark_closed()
        self.hub.close()

    def plan(self, batch: list[tuple[int, int, Any]]) -> list[tuple[Any, int, Any]]:
        """Pair each item with its subscription; for a subscription that
        does not want every update (``all_updates`` False), collapse a run
        of values (kind 0 in both hubs) into the last one and count the
        rest as dropped."""
        out: list[tuple[Any, int, Any]] = []
        last_value: dict[int, int] = {}
        with self.lock:
            for sid, kind, payload in batch:
                sub = self.subs.get(sid)
                if sub is None:
                    continue
                if kind == KIND_VALUE and not sub.all_updates:
                    at = last_value.get(sid)
                    if at is not None:
                        out[at] = (sub, kind, payload)
                        sub.dropped_callbacks += 1
                        continue
                    last_value[sid] = len(out)
                else:
                    last_value.pop(sid, None)
                out.append((sub, kind, payload))
        return out


class ThreadDispatcher(Dispatcher):
    """Drains the hub on one daemon thread, started with the first
    subscription, and calls back there."""

    def __init__(self, hub: Any, name: str) -> None:
        super().__init__(hub)
        self._name = name
        self._thread: threading.Thread | None = None

    def started(self) -> None:
        with self.lock:
            if self._thread is not None:
                return
            self._thread = threading.Thread(target=self._run, name=self._name, daemon=True)
            self._thread.start()

    def _run(self) -> None:
        while True:
            batch = self.hub.recv_batch()
            if batch is None:
                return
            for sub, kind, payload in self.plan(batch):
                value = sub._event(kind, payload)
                if value is not None:
                    sub._deliver(value)

    def shutdown(self) -> None:
        super().shutdown()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(5.0)


class LoopDispatcher(Dispatcher):
    """Drains the hub in one task on the loop that made the first
    subscription; a coroutine callback is awaited before the next item."""

    def __init__(self, hub: Any, loop: asyncio.AbstractEventLoop, name: str) -> None:
        super().__init__(hub)
        self._loop = loop
        self._name = name
        self._task: asyncio.Task[None] | None = None

    def started(self) -> None:
        if self._task is None:
            self._task = self._loop.create_task(self._run(), name=self._name)

    async def _run(self) -> None:
        try:
            while True:
                batch = await self.hub.recv_batch_async()
                if batch is None:
                    return
                for sub, kind, payload in self.plan(batch):
                    value = sub._event(kind, payload)
                    if value is not None:
                        await sub._deliver(value)
        finally:
            # The loop is going away (task cancelled at loop close): nothing
            # will drain the hub again, so let its subscriptions go.
            self.shutdown()


def per_loop(registry: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, Any]", make: Callable[[asyncio.AbstractEventLoop], Any]) -> Any:
    """The running loop's dispatcher, made on first use."""
    loop = asyncio.get_running_loop()
    d = registry.get(loop)
    if d is None:
        d = registry[loop] = make(loop)
    return d


_live: "weakref.WeakSet[Dispatcher]" = weakref.WeakSet()


def _shutdown_all() -> None:
    for d in list(_live):
        d.shutdown()


atexit.register(_shutdown_all)


class SubscriptionBase:
    """A running monitor's state; the dispatcher feeds it, the front end
    delivers.

    ``callback`` may be replaced at any time. A callback that raises closes
    the subscription and prints the traceback to stderr, as an unhandled
    error in a callback thread would.
    """

    OPENING = 0
    OPEN = 1
    CLOSED = 2

    def __init__(
        self,
        dispatcher: Dispatcher,
        name: str,
        callback: Callable[..., Any],
        *,
        form: str,
        datatype: Any,
        count: int,
        mask: int | None,
        all_updates: bool,
        notify_disconnect: bool,
        connect_timeout: Any,
        values: bool = True,
        values_max_count: int | None = None,
    ):
        if datatype not in MONITOR_DATATYPES:
            raise TypeError(
                f"datatype {datatype!r} is not supported for monitors; "
                "use None, str, DBR_ENUM_STR or a DBR_CHAR_* marker"
            )
        self.name = name
        self.callback = callback
        self.form = form
        self.datatype = datatype
        self.count = count
        self.mask = default_mask(form, mask)
        self.all_updates = all_updates
        self.notify_disconnect = notify_disconnect
        self.connect_timeout = connect_timeout
        self.state = self.OPENING
        #: Updates collapsed into a later one because the callback was busy.
        self.dropped_callbacks = 0
        self._marker = datatype if datatype in (DBR_CHAR_STR, DBR_CHAR_BYTES, DBR_CHAR_UNICODE) else None
        self._dispatcher = dispatcher
        timeout = None if connect_timeout is None else Deadline(connect_timeout).remaining()
        ch = channel(name)
        _context.add_subscriber(name, 1)
        self.id = dispatcher.add(
            self,
            ch,
            mask=self.mask,
            enum_as_string=datatype in (str, DBR_ENUM_STR),
            float_as_string=datatype is str,
            count=count,
            connect_timeout=timeout,
            values=values,
            values_max_count=values_max_count,
        )

    def _event(self, kind: int, payload: Any) -> Any | None:
        """Apply one hub item; the value to deliver, if any."""
        if self.state == self.CLOSED:
            return None
        if kind == KIND_VALUE:
            return augment(payload, self._marker)
        if kind == KIND_ERROR:
            return CaNothing.from_exception(self.name, payload) if self.notify_disconnect else None
        if kind == KIND_CONNECT_TIMEOUT:
            return CaNothing(self.name, ECA_TIMEOUT)
        if kind == KIND_CONNECTED:
            _context.note_connected(self.name)
            self.state = self.OPEN
            self.on_connection(True)
            return None
        if kind == KIND_DISCONNECTED:
            self.on_connection(False)
            return None
        if kind == KIND_ACCESS_RIGHTS:
            self.on_access(*payload)
            return None
        # KIND_END: the task is gone (channel torn down); nothing to close.
        self._mark_closed()
        self._dispatcher.remove(self.id)
        return None

    def on_connection(self, connected: bool) -> None:
        """Hook: the channel connected (initially and after each
        reconnection) or lost its circuit."""

    def on_access(self, read: bool, write: bool) -> None:
        """Hook: the server changed this client's access rights."""

    def _report(self, exc: BaseException) -> None:
        print(f"epicsrs: callback for {self.name} raised; subscription closed", file=sys.stderr)
        traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)

    def _mark_closed(self) -> bool:
        """Flip to CLOSED; True if this call did it."""
        if self.state == self.CLOSED:
            return False
        self.state = self.CLOSED
        _context.add_subscriber(self.name, -1)
        return True

    # -- the owner

    def close(self) -> None:
        """Stop the monitor. No callback starts after this returns; one
        already running finishes on its own."""
        if self._mark_closed():
            self._dispatcher.remove(self.id)

    def pause(self) -> None:
        """Hold value updates client-side (the latest is kept for ``resume``);
        errors still come through."""
        self._dispatcher.hub.pause(self.id)

    def resume(self) -> None:
        self._dispatcher.hub.resume(self.id)

    def __enter__(self) -> "SubscriptionBase":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        state = ("opening", "open", "closed")[self.state]
        return f"{type(self).__name__}({self.name!r}, {state})"


__all__ = ["Dispatcher", "ThreadDispatcher", "LoopDispatcher", "per_loop", "SubscriptionBase", "MONITOR_DATATYPES", "Snapshot"]
