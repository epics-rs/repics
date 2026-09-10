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

import time
from typing import Any, Callable

from .._epicsrs import PvaContext, PvaDisconnected, PvaError, PvaMonitorHub, PvaRemoteError, PvaTimeout, Type, Value
from .._monitor import ThreadDispatcher
from . import nt
from ._common import Cancelled, Disconnected, Finished, RemoteError, TimeoutError, Wrapping, dispatch, effective_conf, put_request
from ._monitor import SubscriptionBase

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


_dispatcher = ThreadDispatcher(PvaMonitorHub(), "epicsrs pvmonitor")


class Subscription(SubscriptionBase):
    """A running monitor. ``cb(value)`` runs on this flavour's one
    dispatcher thread, shared by every monitor, or is pushed to ``queue``
    (p4p style, anything with ``push`` or ``put``) when one is given.

    ``value`` is the unwrapped update, or a ``Disconnected()`` /
    ``Finished()`` instance when ``notify_disconnect`` is set. Updates are
    squashed in Rust when the consumer falls behind; the queue never grows.
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
        self._queue = queue
        super().__init__(_dispatcher, ctxt, name, cb, request, notify_disconnect, limit)

    def _deliver(self, value: Any) -> None:
        dispatch(self._cb, value, self._queue)


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

    def _settle(self, result: Any, name: str, throw: bool) -> Any:
        """One entry of a batch: the exception itself, or the unwrapped value."""
        if isinstance(result, BaseException):
            if throw:
                raise result
            return result
        return self._one(lambda: self._wrapping.unwrap(result, name), throw)

    def get(self, name: Any, request: Any = None, timeout: float | None = 5.0, throw: bool = True) -> Any:
        """Read ``name`` (or each of a list, as one concurrent batch).

        With ``throw=False`` an error is returned, not raised; in a list it
        takes the place of that entry.
        """
        if _is_list(name):
            names = list(name)
            reqs = list(request) if _is_list(request) else [request] * len(names)
            results = self._raw.get_many(names, reqs, timeout)
            return [self._settle(r, n, throw) for r, n in zip(results, names)]
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
        the current value is read on the put's own operation first so NT
        helpers can see it (an NTEnum label resolves against the live
        choices); with ``get=False`` the value is built from the type the
        put operation reports.
        """
        if _is_list(name):
            if not _is_list(values) or len(values) != len(name):
                raise ValueError(f"{len(name)} PVs need a list of {len(name)} values")
            reqs = request if _is_list(request) else [request] * len(name)
            return self._put_many(list(name), list(values), list(reqs), timeout, throw, process, wait, get)
        return self._put_many([name], [values], [request], timeout, throw, process, wait, get)[0]

    def _put_many(
        self,
        names: list[str],
        values: list[Any],
        reqs: list[Any],
        timeout: float | None,
        throw: bool,
        process: Any,
        wait: bool | None,
        get: bool,
    ) -> list[Any]:
        """``put`` over lists: every put is opened as one batch (reading the
        current values on the puts themselves), the values are built, then
        every write is committed as one batch. An entry that fails at either
        step is that exception (``throw=False``) or raises (``throw=True``).
        Each step waits at most ``timeout``. A put whose circuit is lost
        between the two steps is begun again if ``timeout`` has not passed
        since the call started."""
        results: list[Any] = [None] * len(names)
        req_strs = [put_request(r, process, wait) for r in reqs]
        deadline = None if timeout is None else time.monotonic() + timeout
        pending = list(range(len(names)))
        while pending:
            fetch = [get and not isinstance(values[i], Value) for i in pending]
            ops = self._raw.put_begin_many([names[i] for i in pending], [req_strs[i] for i in pending], fetch, timeout)
            ready: list[int] = []
            ready_ops: list[Any] = []
            ready_values: list[Value] = []
            for i, op in zip(pending, ops):
                if isinstance(op, BaseException):
                    if throw:
                        raise op
                    results[i] = op
                    continue
                V = values[i]
                if not isinstance(V, Value):
                    cur = op.current()
                    V = self._wrapping.assign(Value(op.type) if cur is None else cur, V)
                ready.append(i)
                ready_ops.append(op)
                ready_values.append(V)
            done = self._raw.put_commit_many(ready_ops, ready_values, timeout)
            pending = []
            for i, r in zip(ready, done):
                if isinstance(r, PvaDisconnected) and (deadline is None or time.monotonic() < deadline):
                    pending.append(i)
                    continue
                if isinstance(r, BaseException) and throw:
                    raise r
                results[i] = r
        return results

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
        """Subscribe; ``cb`` runs on the dispatcher thread, or via ``queue.push`` if given.

        ``limit`` bounds the Rust-side update queue (default: the request's
        ``queueSize``, else 4); beyond it the newest update is merged into
        the tail rather than queued.
        """
        return Subscription(self, name, cb, request, notify_disconnect, queue, limit)
