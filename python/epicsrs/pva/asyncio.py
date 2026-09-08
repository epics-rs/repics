"""pvAccess client, asyncio flavour (``p4p.client.asyncio`` shaped).

Every operation is a coroutine on the shared runtime; nothing blocks the
event loop. ``monitor`` returns a ``Subscription`` driven by the loop's one
dispatcher task; its callback may be a plain function or a coroutine
function.
"""

from __future__ import annotations

import asyncio
import inspect
import weakref
from typing import Any, Callable

from .._epicsrs import PvaContext, PvaError, PvaMonitorHub, Type, Value
from .._monitor import LoopDispatcher, per_loop
from ._common import Wrapping, effective_conf, put_request
from ._common import log as _log
from ._monitor import SubscriptionBase

__all__ = ["Context", "Subscription"]


def _is_list(x: Any) -> bool:
    return isinstance(x, (list, tuple))


_dispatchers: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, LoopDispatcher]" = weakref.WeakKeyDictionary()


def _dispatcher() -> LoopDispatcher:
    return per_loop(_dispatchers, lambda loop: LoopDispatcher(PvaMonitorHub(), loop, "epicsrs pvmonitor"))


class Subscription(SubscriptionBase):
    """A running monitor driven by the one dispatcher task of the loop
    that created it.

    A coroutine callback is awaited before the next item is taken, so a
    slow consumer back-pressures into the Rust queue (which squashes)
    instead of piling up Python objects.
    """

    def __init__(
        self,
        ctxt: Context,
        name: str,
        cb: Callable[[Any], Any],
        request: str | None,
        notify_disconnect: bool,
        limit: int | None,
    ):
        super().__init__(_dispatcher(), ctxt, name, cb, request, notify_disconnect, limit)

    async def _deliver(self, item: Any) -> None:
        try:
            r = self._cb(item)
            if inspect.isawaitable(r):
                await r
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - a callback must not kill the drain loop
            _log.exception("pva monitor callback failed")


class Context:
    """A pvAccess client whose operations are coroutines."""

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

    async def __aenter__(self) -> "Context":
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.close()

    def __enter__(self) -> "Context":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    async def _one(self, coro: Any, throw: bool) -> Any:
        try:
            return await coro
        except PvaError as e:
            if throw:
                raise
            return e

    async def get(self, name: Any, request: Any = None, timeout: float | None = 5.0, throw: bool = True) -> Any:
        if _is_list(name):
            reqs = request if _is_list(request) else [request] * len(name)
            return await asyncio.gather(*(self.get(n, r, timeout, throw) for n, r in zip(name, reqs)))

        async def one() -> Any:
            V = await self._raw.get_async(name, request, timeout)
            return self._wrapping.unwrap(V, name)

        return await self._one(one(), throw)

    async def put(
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
        if _is_list(name):
            if not _is_list(values) or len(values) != len(name):
                raise ValueError(f"{len(name)} PVs need a list of {len(name)} values")
            reqs = request if _is_list(request) else [request] * len(name)
            return await asyncio.gather(
                *(self.put(n, v, r, timeout, throw, process, wait, get) for n, v, r in zip(name, values, reqs))
            )

        async def one() -> None:
            req = put_request(request, process, wait)
            if isinstance(values, Value):
                V = values
            else:
                if get:
                    V = await self._raw.get_async(name, None, timeout)
                else:
                    V = Value(await self._raw.info_async(name, timeout))
                V = self._wrapping.assign(V, values)
            await self._raw.put_async(name, V, req, timeout)

        return await self._one(one(), throw)

    async def rpc(self, name: str, value: Value, request: Any = None, timeout: float | None = 5.0, throw: bool = True) -> Any:
        async def one() -> Any:
            V = await self._raw.rpc_async(name, value, request, timeout)
            return self._wrapping.unwrap(V, name)

        return await self._one(one(), throw)

    async def info(self, name: str, timeout: float | None = 5.0) -> Type:
        return await self._raw.info_async(name, timeout)

    async def connect(self, name: str, timeout: float | None = 5.0) -> str:
        return await self._raw.connect_async(name, timeout)

    def monitor(
        self,
        name: str,
        cb: Callable[[Any], Any],
        request: Any = None,
        notify_disconnect: bool = False,
        limit: int | None = None,
    ) -> Subscription:
        """Subscribe from within a running loop; ``cb`` may be a coroutine function."""
        return Subscription(self, name, cb, request, notify_disconnect, limit)
