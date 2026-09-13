"""What the blocking and asyncio ``SharedPV`` flavours share.

Every client operation reaches Python as an item pulled from a
``PvaWorkQueue`` by a Python-owned drain loop (a thread here, a task in
``.asyncio``); the Rust server only ever enqueues. The item names its
``SharedPV`` through a weakref, so a PV the program has dropped cannot be
kept alive by its own queue.
"""

from __future__ import annotations

import logging
import uuid
import weakref
from typing import Any, Callable

from ..._repics import PvaProvider, PvaServer, PvaSharedPV, Value
from .._common import RemoteError

log = logging.getLogger("repics.pva.server")

__all__ = ["Handler", "ServerOperation", "StaticProvider", "Server", "log"]


class ServerOperation:
    """A client PUT (or RPC) waiting for ``done()``.

    Wraps the Rust operation so ``value()`` is unwrapped, and ``done(value=)``
    wrapped, with the PV's NT helpers.
    """

    def __init__(self, op: Any, wrap: Callable, unwrap: Callable):
        self._op, self._wrap, self._unwrap = op, wrap, unwrap

    def value(self) -> Any:
        return self._unwrap(self._op.value())

    def done(self, value: Any = None, error: str | None = None) -> None:
        if value is not None and not isinstance(value, Value):
            value = self._wrap(value)
        self._op.done(value=value, error=error)

    def __getattr__(self, key: str) -> Any:
        return getattr(self._op, key)

    def __repr__(self) -> str:
        return f"ServerOperation({self._op.kind()} {self._op.name()} from {self._op.peer()})"


class Handler:
    """Optional base for a ``SharedPV`` handler; every method may be omitted."""

    def put(self, pv: Any, op: ServerOperation) -> None:
        op.done(error="Put not supported")

    def rpc(self, pv: Any, op: Any) -> None:
        op.done(error="RPC not supported")

    def onFirstConnect(self, pv: Any) -> None:  # noqa: N802 - p4p name
        pass

    def onLastDisconnect(self, pv: Any) -> None:  # noqa: N802 - p4p name
        pass


class _Dummy:
    pass


def _identity(x: Any, **kws: Any) -> Any:
    return x


class SharedPVBase:
    """A served PV; subclasses supply ``_queue()`` and ``_run(fn, op)``."""

    def __init__(
        self,
        handler: Any = None,
        initial: Any = None,
        nt: Any = None,
        wrap: Callable | None = None,
        unwrap: Callable | None = None,
    ):
        self._handler = handler if handler is not None else _Dummy()
        self._wrap = wrap or (nt and nt.wrap) or _identity
        self._unwrap = unwrap or (nt and nt.unwrap) or _identity
        queue = self._queue()
        self._raw = PvaSharedPV(queue._raw, weakref.ref(self))
        if initial is not None:
            self.open(initial, nt=nt, wrap=wrap, unwrap=unwrap)

    # -- hooks the flavours implement --------------------------------------

    def _queue(self) -> Any:
        raise NotImplementedError

    def _run(self, fn: Callable, op: Any) -> None:
        """Run ``fn()``; on an exception, fail ``op`` (when given)."""
        raise NotImplementedError

    def _connected(self, yes: bool) -> None:
        pass

    # -- p4p API ------------------------------------------------------------

    def open(self, value: Any, nt: Any = None, wrap: Callable | None = None,
             unwrap: Callable | None = None, **kws: Any) -> None:
        self._wrap = wrap or (nt and nt.wrap) or self._wrap
        self._unwrap = unwrap or (nt and nt.unwrap) or self._unwrap
        self._raw.open(self._to_value(value, **kws))

    def post(self, value: Any, **kws: Any) -> None:
        self._raw.post(self._to_value(value, **kws))

    def close(self, destroy: bool = False) -> None:
        self._raw.close()

    def isOpen(self) -> bool:  # noqa: N802 - p4p name
        return self._raw.isOpen()

    def current(self) -> Any:
        V = self._raw.current()
        return None if V is None else self._unwrap(V)

    def _to_value(self, value: Any, **kws: Any) -> Value:
        if isinstance(value, Value):
            return value
        try:
            return self._wrap(value, **kws)
        except Exception as e:  # noqa: BLE001 - re-raise with the p4p wording
            raise ValueError(f"Unable to wrap {value!r} with {self._wrap!r} and {kws!r}") from e

    # -- decorators ---------------------------------------------------------

    @property
    def put(self) -> Callable:
        def decorate(fn: Callable) -> Callable:
            self._handler.put = fn
            return fn
        return decorate

    @property
    def rpc(self) -> Callable:
        def decorate(fn: Callable) -> Callable:
            self._handler.rpc = fn
            return fn
        return decorate

    @property
    def onFirstConnect(self) -> Callable:  # noqa: N802 - p4p name
        def decorate(fn: Callable) -> Callable:
            self._handler.onFirstConnect = fn
            return fn
        return decorate

    @property
    def onLastDisconnect(self) -> Callable:  # noqa: N802 - p4p name
        def decorate(fn: Callable) -> Callable:
            self._handler.onLastDisconnect = fn
            return fn
        return decorate

    # -- dispatch (called by the drain loop) -------------------------------

    def _dispatch(self, kind: str, op: Any) -> None:
        if kind == "put":
            M = getattr(self._handler, "put", None)
            if M is None:
                op.done(error="Put not supported")
                return
            wrapped = ServerOperation(op, self._wrap, self._unwrap)
            self._run(lambda: M(self, wrapped), op)
        elif kind == "rpc":
            M = getattr(self._handler, "rpc", None)
            if M is None:
                op.done(error="RPC not supported")
                return
            self._run(lambda: M(self, op), op)
        elif kind == "first":
            self._connected(True)
            M = getattr(self._handler, "onFirstConnect", None)
            if M is not None:
                self._run(lambda: M(self), None)
        elif kind == "last":
            M = getattr(self._handler, "onLastDisconnect", None)
            if M is not None:
                self._run(lambda: M(self), None)
            self._connected(False)

    def __repr__(self) -> str:
        if self.isOpen():
            return f"{type(self).__name__}(value={self.current()!r})"
        return f"{type(self).__name__}(<closed>)"

    __str__ = __repr__


def fail_op(op: Any, exc: BaseException) -> None:
    """Report a handler exception to the client (p4p ``_on_queue``)."""
    if not isinstance(exc, RemoteError):
        log.exception("Unexpected error in SharedPV handler", exc_info=exc)
    if op is not None:
        try:
            op.done(error=str(exc) or type(exc).__name__)
        except Exception:  # noqa: BLE001 - already completed by the handler
            pass


def deliver(item: Any) -> bool:
    """Route one queue item to its PV; a dead PV fails the operation.

    Returns False on the queue's end-of-stream ``None``. The item is
    consumed here, not in the caller's loop, so a handler that returns
    without ``done()`` drops the operation (and fails it) at once rather
    than when the next event happens to arrive.
    """
    if item is None:
        return False
    ref, kind, op = item
    pv = ref()
    if pv is None:
        if op is not None:
            op.done(error="SharedPV no longer exists")
        return True
    pv._dispatch(kind, op)
    return True


class StaticProvider:
    """A fixed, mutable table of name → ``SharedPV``."""

    def __init__(self, name: str | None = None):
        self._raw = PvaProvider(name or str(uuid.uuid4()))
        self._pvs: dict[str, Any] = {}

    @property
    def name(self) -> str:
        return self._raw.name()

    def add(self, name: str, pv: SharedPVBase) -> None:
        self._raw.add(name, pv._raw)
        self._pvs[name] = pv

    def remove(self, name: str) -> None:
        self._raw.remove(name)
        self._pvs.pop(name, None)

    def keys(self) -> list[str]:
        return self._raw.keys()

    def __contains__(self, name: str) -> bool:
        return name in self._pvs

    def __getitem__(self, name: str) -> Any:
        return self._pvs[name]


def _providers(providers: Any) -> tuple[list[tuple[Any, int]], list[StaticProvider]]:
    if isinstance(providers, (StaticProvider, dict)):
        providers = [providers]
    out, keep = [], []
    for p in providers:
        order = 0
        if isinstance(p, tuple):
            p, order = p
        if isinstance(p, dict):
            sp = StaticProvider()
            for name, pv in p.items():
                sp.add(name, pv)
            keep.append(sp)
            p = sp
        if not isinstance(p, StaticProvider):
            raise ValueError(
                f"providers=[] must be a list of StaticProvider or dict, not {p!r}"
            )
        out.append((p._raw, int(order)))
    return out, keep


class Server:
    """A pvAccess server. Starts on construction; ``stop()`` or use as a
    context manager.

    ``isolate=True`` binds loopback on random ports with no beacons, so a
    test never touches 5075/5076; ``conf()`` then tells a client how to
    reach it.
    """

    def __init__(
        self,
        providers: Any,
        isolate: bool = False,
        conf: dict | None = None,
        useenv: bool = True,
    ):
        raw, self._keep = _providers(providers)
        if isolate and (conf is not None or not useenv):
            raise ValueError("isolate=True cannot be combined with conf=/useenv=")
        self._raw = PvaServer(raw, conf=None if conf is None else
                              {k: str(v) for k, v in conf.items()},
                              useenv=useenv, isolate=isolate)
        self._providers = [p for p in ([providers] if isinstance(providers, StaticProvider) else providers)
                           if isinstance(p, StaticProvider)]

    def conf(self) -> dict[str, str]:
        return self._raw.conf()

    def stop(self) -> None:
        self._raw.stop()
        self._keep = []

    @property
    def running(self) -> bool:
        return self._raw.running()

    def __enter__(self) -> Server:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.stop()

    @classmethod
    def forever(cls, *args: Any, **kws: Any) -> None:
        import time

        with cls(*args, **kws):
            try:
                while True:
                    time.sleep(100)
            except KeyboardInterrupt:
                pass
