"""What the blocking and asyncio Channel Access ``SharedPV`` flavours share.

A CA server hosts bare value channels: ``open`` declares a value, ``post``
updates it and fans it out to monitors, and a client ``caput`` reaches Python
as a :class:`ServerOperation` pulled from a ``CaWorkQueue`` by a drain loop (a
thread here, a task in ``.asyncio``). The item names its ``SharedPV`` through a
weakref, so a PV the program has dropped cannot be kept alive by its own queue.

Unlike pvAccess, CA has no RPC and no per-PV connect edge, so the only handler
is ``put``. Values are plain Python scalars, strings, lists or 1-D numpy
arrays — there is no structured type to wrap.
"""

from __future__ import annotations

import logging
import uuid
import weakref
from typing import Any, Callable

from ..._repics import CaProvider, CaServer, CaSharedPV

log = logging.getLogger("repics.ca.server")

__all__ = ["Handler", "ServerOperation", "StaticProvider", "Server", "log"]


class ServerOperation:
    """A client ``caput`` waiting for ``done()``."""

    def __init__(self, op: Any):
        self._op = op

    def value(self) -> Any:
        return self._op.value()

    def done(self, error: str | None = None) -> None:
        self._op.done(error=error)

    def __getattr__(self, key: str) -> Any:
        return getattr(self._op, key)

    def __repr__(self) -> str:
        return f"ServerOperation({self._op.kind()} {self._op.name()} from {self._op.peer()})"


class Handler:
    """Optional base for a ``SharedPV`` handler; ``put`` may be omitted."""

    def put(self, pv: Any, op: ServerOperation) -> None:
        op.done(error="Put not supported")


class _Dummy:
    pass


class SharedPVBase:
    """A served CA value channel; subclasses supply ``_queue()`` and
    ``_run(fn, op)``."""

    def __init__(self, handler: Any = None, initial: Any = None):
        self._handler = handler if handler is not None else _Dummy()
        queue = self._queue()
        self._raw = CaSharedPV(queue._raw, weakref.ref(self))
        if initial is not None:
            self.open(initial)

    # -- hooks the flavours implement --------------------------------------

    def _queue(self) -> Any:
        raise NotImplementedError

    def _run(self, fn: Callable, op: Any) -> None:
        """Run ``fn()``; on an exception, fail ``op`` (when given)."""
        raise NotImplementedError

    # -- API ----------------------------------------------------------------

    def open(self, value: Any) -> None:
        self._raw.open(value)

    def post(self, value: Any) -> None:
        self._raw.post(value)

    def close(self, destroy: bool = False) -> None:
        self._raw.close()

    def isOpen(self) -> bool:  # noqa: N802 - p4p name
        return self._raw.isOpen()

    def current(self) -> Any:
        return self._raw.current()

    # -- decorators ---------------------------------------------------------

    @property
    def put(self) -> Callable:
        def decorate(fn: Callable) -> Callable:
            self._handler.put = fn
            return fn

        return decorate

    # -- dispatch (called by the drain loop) -------------------------------

    def _dispatch(self, kind: str, op: Any) -> None:
        if kind == "put":
            M = getattr(self._handler, "put", None)
            if M is None:
                op.done(error="Put not supported")
                return
            wrapped = ServerOperation(op)
            self._run(lambda: M(self, wrapped), op)

    def __repr__(self) -> str:
        if self.isOpen():
            return f"{type(self).__name__}(value={self.current()!r})"
        return f"{type(self).__name__}(<closed>)"

    __str__ = __repr__


def fail_op(op: Any, exc: BaseException) -> None:
    """Report a handler exception to the client."""
    log.exception("Unexpected error in SharedPV handler", exc_info=exc)
    if op is not None:
        try:
            op.done(error=str(exc) or type(exc).__name__)
        except Exception:  # noqa: BLE001 - already completed by the handler
            pass


def deliver(item: Any) -> bool:
    """Route one queue item to its PV; a dead PV fails the operation.

    Returns False on the queue's end-of-stream ``None``.
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
        self._raw = CaProvider(name or str(uuid.uuid4()))
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


def _providers(providers: Any) -> tuple[list[Any], list[StaticProvider]]:
    if isinstance(providers, (StaticProvider, dict)):
        providers = [providers]
    out, keep = [], []
    for p in providers:
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
        out.append(p._raw)
    return out, keep


class Server:
    """A Channel Access server. Starts on construction; ``stop()`` or use as a
    context manager.

    ``isolate=True`` binds ephemeral ports so a test never touches 5064;
    ``conf()`` then tells a client how to reach it.
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
        self._raw = CaServer(
            raw,
            conf=None if conf is None else {k: str(v) for k, v in conf.items()},
            useenv=useenv,
            isolate=isolate,
        )

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
