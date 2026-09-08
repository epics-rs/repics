"""pvAccess client, blocking flavour (``p4p.client.thread`` shaped).

::

    from epicsrs import pva

    with pva.Context() as ctxt:
        v = ctxt.get("SIM:ai")              # augmented float, .severity, .timestamp, .raw
        ctxt.put("SIM:ao", 2.5)
        sub = ctxt.monitor("SIM:cnt", print)

``epicsrs.pva.asyncio`` is the same API as coroutines, ``epicsrs.pva.nt``
the Normative Type helpers, ``epicsrs.pva.server`` the server side.
"""

from __future__ import annotations

import threading
from typing import Any, Callable

from .._epicsrs import PvaContext, PvaDisconnected, PvaError, PvaRemoteError, PvaSubscription, PvaTimeout, Type, Value
from . import nt
from ._common import Cancelled, Disconnected, Finished, RemoteError, TimeoutError, Wrapping, dispatch, effective_conf, put_request

__all__ = [
    "Context",
    "Subscription",
    "Type",
    "Value",
    "nt",
    "PvaError",
    "PvaTimeout",
    "PvaDisconnected",
    "PvaRemoteError",
    "Disconnected",
    "RemoteError",
    "TimeoutError",
    "Finished",
    "Cancelled",
]


def _is_list(x: Any) -> bool:
    return isinstance(x, (list, tuple))


class Subscription:
    """A running monitor. ``cb(value)`` runs on the subscription's own thread.

    ``value`` is the unwrapped update, or a ``Disconnected()`` /
    ``Finished()`` instance when ``notify_disconnect`` is set. Updates are
    squashed in Rust when this thread falls behind; the queue never grows.
    """

    def __init__(
        self,
        ctxt: Context,
        name: str,
        cb: Callable[[Any], Any],
        request: str | None,
        notify_disconnect: bool,
        queue: Any,
        limit: int | None,
    ):
        self.name = name
        self._ctxt = ctxt
        self._cb = cb
        self._notify = notify_disconnect
        self._queue = queue
        self._sub: PvaSubscription = ctxt._raw.monitor(name, request, limit)
        self._thread = threading.Thread(target=self._run, name=f"pvmonitor {name}", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        if self._notify:
            dispatch(self._cb, Disconnected(), self._queue)
        while True:
            try:
                item = self._sub.recv()
            except Exception:  # noqa: BLE001 - a decode failure; keep draining until closed
                continue
            if item is None:
                return
            kind, payload = item
            if kind == "value":
                dispatch(self._cb, self._ctxt._wrapping.unwrap(payload, self.name), self._queue)
            elif kind == "disconnected":
                if self._notify:
                    dispatch(self._cb, Disconnected(), self._queue)
            elif kind == "finished":
                if self._notify:
                    dispatch(self._cb, Finished(), self._queue)
                return

    def pause(self) -> None:
        self._sub.pause()

    def resume(self) -> None:
        self._sub.resume()

    def close(self) -> None:
        self._sub.close()
        if threading.current_thread() is not self._thread:
            self._thread.join()

    def __enter__(self) -> "Subscription":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class Context:
    """A pvAccess client (``p4p.client.thread.Context`` shaped).

    ``conf`` overrides ``EPICS_PVA_*`` settings; ``useenv=False`` ignores
    the environment. ``nt``/``unwrap`` select the NT helpers (see
    ``epicsrs.pva.nt.buildNT``); ``unwrap=False`` returns bare ``Value``.
    """

    def __init__(
        self,
        provider: str = "pva",
        conf: dict | None = None,
        useenv: bool = True,
        nt: Any = None,
        unwrap: Any = None,
    ):
        if provider != "pva":
            raise ValueError(f"only the 'pva' provider exists, not {provider!r}")
        self._raw = PvaContext(effective_conf(conf, useenv))
        self._wrapping = Wrapping(nt, unwrap)

    def close(self) -> None:
        self._raw.close()

    def __enter__(self) -> "Context":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _one(self, fn: Callable[[], Any], throw: bool) -> Any:
        try:
            return fn()
        except PvaError as e:
            if throw:
                raise
            return e

    def get(self, name: Any, request: Any = None, timeout: float | None = 5.0, throw: bool = True) -> Any:
        """Read ``name`` (or each of a list). With ``throw=False`` an error is returned, not raised."""
        if _is_list(name):
            reqs = request if _is_list(request) else [request] * len(name)
            return [self.get(n, r, timeout, throw) for n, r in zip(name, reqs)]
        return self._one(
            lambda: self._wrapping.unwrap(self._raw.get(name, request, timeout), name), throw
        )

    def put(
        self,
        name: Any,
        values: Any,
        request: Any = None,
        timeout: float | None = 5.0,
        throw: bool = True,
        process: Any = None,
        wait: bool | None = None,
        get: bool = True,
    ) -> Any:
        """Write ``values`` to ``name`` (or each pair of two lists).

        ``values`` may be a ``Value`` (its marked fields are sent), a dict of
        fields, or a bare value for the ``value`` field. With ``get=True``
        the current value is read first so NT helpers can see it (an
        NTEnum label resolves against the live choices).
        """
        if _is_list(name):
            if not _is_list(values) or len(values) != len(name):
                raise ValueError(f"{len(name)} PVs need a list of {len(name)} values")
            reqs = request if _is_list(request) else [request] * len(name)
            return [
                self.put(n, v, r, timeout, throw, process, wait, get)
                for n, v, r in zip(name, values, reqs)
            ]

        def one() -> None:
            req = put_request(request, process, wait)
            if isinstance(values, Value):
                V = values
            else:
                V = self._raw.get(name, None, timeout) if get else Value(self._raw.info(name, timeout))
                V = self._wrapping.assign(V, values)
            self._raw.put(name, V, req, timeout)

        return self._one(one, throw)

    def rpc(self, name: str, value: Value, request: Any = None, timeout: float | None = 5.0, throw: bool = True) -> Any:
        """Call ``name`` with argument ``value`` (a ``Value``, see ``nt.NTURI``)."""
        return self._one(
            lambda: self._wrapping.unwrap(self._raw.rpc(name, value, request, timeout), name), throw
        )

    def info(self, name: str, timeout: float | None = 5.0) -> Type:
        """The server's type for ``name``."""
        return self._raw.info(name, timeout)

    def connect(self, name: str, timeout: float | None = 5.0) -> str:
        """Wait for ``name`` to connect; returns the server address."""
        return self._raw.connect(name, timeout)

    def monitor(
        self,
        name: str,
        cb: Callable[[Any], Any],
        request: Any = None,
        notify_disconnect: bool = False,
        queue: Any = None,
        limit: int | None = None,
    ) -> Subscription:
        """Subscribe; ``cb`` runs on a dedicated thread, or via ``queue.push`` if given.

        ``limit`` bounds the Rust-side update queue (default: the request's
        ``queueSize``, else 4); beyond it the newest update is merged into
        the tail rather than queued.
        """
        return Subscription(self, name, cb, request, notify_disconnect, queue, limit)
