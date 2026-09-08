"""pvAccess server, callback flavour (p4p ``p4p.server.thread``).

Handlers run on a small pool of daemon threads. Each ``SharedPV`` is bound
to one of them, so its operations stay ordered; different PVs run in
parallel. Nothing here runs on the Rust runtime.
"""

from __future__ import annotations

import threading
from typing import Any, Callable

from ..._epicsrs import PvaWorkQueue
from ._base import Handler, Server, ServerOperation, SharedPVBase, StaticProvider, deliver, fail_op

__all__ = ["SharedPV", "Handler", "ServerOperation", "StaticProvider", "Server", "WorkQueue"]


class WorkQueue:
    """One drain thread over one Rust event queue."""

    def __init__(self) -> None:
        self._raw = PvaWorkQueue()
        self._thread = threading.Thread(target=self._run, name="epicsrs.pva.server", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while True:
            item = self._raw.recv()
            if item is None:
                return
            deliver(item)

    def stop(self) -> None:
        self._raw.stop()
        if threading.current_thread() is not self._thread:
            self._thread.join()


class _Pool:
    """p4p's default: four queues handed out round-robin, created lazily."""

    def __init__(self, workers: int = 4):
        self._queues: list[WorkQueue | None] = [None] * workers
        self._next = 0
        self._lock = threading.Lock()

    def __call__(self) -> WorkQueue:
        with self._lock:
            i = self._next
            self._next = (i + 1) % len(self._queues)
            q = self._queues[i]
            if q is None:
                q = self._queues[i] = WorkQueue()
            return q


_default_queue = _Pool()


class SharedPV(SharedPVBase):
    """A served PV whose handler methods run on a worker thread.

    ``queue`` picks the ``WorkQueue`` (default: one of the shared pool).
    """

    def __init__(self, handler: Any = None, initial: Any = None, nt: Any = None,
                 wrap: Callable | None = None, unwrap: Callable | None = None,
                 queue: WorkQueue | None = None):
        self._wq = queue or _default_queue()
        self._disconnected = threading.Event()
        self._disconnected.set()
        super().__init__(handler=handler, initial=initial, nt=nt, wrap=wrap, unwrap=unwrap)

    def _queue(self) -> WorkQueue:
        return self._wq

    def _run(self, fn: Callable, op: Any) -> None:
        try:
            fn()
        except Exception as e:  # noqa: BLE001 - reported to the client
            fail_op(op, e)

    def _connected(self, yes: bool) -> None:
        if yes:
            self._disconnected.clear()
        else:
            self._disconnected.set()

    def close(self, destroy: bool = False, sync: bool = False, timeout: float | None = None) -> None:
        """Close the PV, disconnecting clients. ``sync`` waits for the
        last-disconnect hook to have run."""
        super().close(destroy)
        if sync:
            self._disconnected.wait(timeout)
