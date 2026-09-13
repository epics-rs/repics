"""pvAccess subscriptions over one ``PvaMonitorHub`` per flavour.

The hub keeps a bounded queue per monitor in Rust, squashing updates at
its ``limit`` when the consumer falls behind (the pvxs client rule), and
wakes the flavour's one dispatcher (``repics._monitor``) when any monitor
has something. Nothing is collapsed in Python: ``all_updates`` is True so
the dispatcher delivers every item the Rust queue let through.
"""

from __future__ import annotations

from typing import Any, Callable

from .._monitor import Dispatcher
from ._common import Disconnected, Finished

# The item kinds `PvaMonitorHub.recv_batch` yields.
KIND_VALUE = 0
KIND_CONNECTED = 1
KIND_DISCONNECTED = 2
KIND_FINISHED = 3


class SubscriptionBase:
    """A running monitor's state; the dispatcher feeds it, the flavour
    delivers (``_deliver``).

    ``cb`` gets each update unwrapped by the context's NT policy, and,
    with ``notify_disconnect``, a ``Disconnected()`` first (the state
    before the server answers) and after every loss of the server, and a
    ``Finished()`` when the server ends the stream. ``pause`` holds server
    emissions; ``close`` stops the monitor and waits for its teardown.
    """

    all_updates = True  # the Rust queue bounds and squashes; nothing collapses here

    def __init__(
        self,
        dispatcher: Dispatcher,
        ctxt: Any,
        name: str,
        cb: Callable[[Any], Any],
        request: str | None,
        notify_disconnect: bool,
        limit: int | None,
    ):
        self.name = name
        self._ctxt = ctxt
        self._cb = cb
        self._notify = notify_disconnect
        self._dispatcher = dispatcher
        self._closed = False
        self.id = dispatcher.add(self, ctxt._raw, name, request, limit)

    def _event(self, kind: int, payload: Any) -> Any | None:
        """Apply one hub item; the value to deliver, if any."""
        if self._closed:
            return None
        if kind == KIND_VALUE:
            return self._ctxt._wrapping.unwrap(payload, self.name)
        if kind == KIND_DISCONNECTED:
            return Disconnected() if self._notify else None
        if kind == KIND_FINISHED:
            self.close()
            return Finished() if self._notify else None
        return None  # KIND_CONNECTED: nothing to tell a p4p-shaped callback

    def _mark_closed(self) -> bool:
        """Flip to closed; True if this call did it."""
        if self._closed:
            return False
        self._closed = True
        return True

    def close(self) -> None:
        """Stop the monitor. No callback starts after this returns; one
        already running finishes on its own."""
        if self._mark_closed():
            self._dispatcher.remove(self.id)

    def pause(self) -> None:
        self._dispatcher.hub.pause(self.id)

    def resume(self) -> None:
        self._dispatcher.hub.resume(self.id)

    def __enter__(self) -> "SubscriptionBase":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r}, {'closed' if self._closed else 'open'})"
