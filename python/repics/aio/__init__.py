"""asyncio Channel Access front end. Same shapes as ``repics.ca``, awaitable.

``camonitor`` is the one plain function: it returns a ``Subscription`` at
once and connects in a task on the running loop, so it must be called from
inside a coroutine. A coroutine callback is awaited before the next update
is taken, so a slow consumer back-pressures the monitor.
"""

from __future__ import annotations

import asyncio
import inspect
import weakref
from typing import Any, Callable

from .._context import (
    Deadline,
    PVs,
    channel,
    context,
    get_channel_infos,
    is_single,
    note_connected,
    purge_channel_caches,
)
from .._dbr import *  # noqa: F401,F403 - the libca vocabulary is part of this API
from .._dbr import __all__ as _dbr_all
from .._repics import CaError, MonitorHub
from .._monitor import LoopDispatcher, SubscriptionBase, per_loop
from .._dbr import request
from .._ops import DEFAULT_TIMEOUT, collect, finish, info_or, put_value, values_for
from .._value import CAInfo, CaNothing, augment

__all__ = [
    "caget",
    "caput",
    "camonitor",
    "cainfo",
    "connect",
    "Subscription",
    "CaNothing",
    "CAInfo",
    "get_channel_infos",
    "purge_channel_caches",
    "DEFAULT_TIMEOUT",
    *_dbr_all,
]


# ---------------------------------------------------------------------------
# connect / caget / caput / cainfo (see ``repics.ca`` for the arguments)


async def connect(pv: PVs, wait: bool = True, timeout: Any = DEFAULT_TIMEOUT, throw: bool = True) -> Any:
    """Start (``wait=False``) or complete (``wait=True``) each PV's connection.

    Returns a ``CaNothing`` per PV: ``ok`` True once connected.
    """
    names = [pv] if is_single(pv) else list(pv)
    chs = [channel(n) for n in names]
    if wait:
        got = await context().wait_connected_many_async(chs, Deadline(timeout).remaining())
        out = collect(names, got, throw)
    else:
        out = [CaNothing(n) for n in names]
    return out[0] if is_single(pv) else out


async def caget(
    pv: PVs,
    form: str = "time",
    datatype: Any = None,
    count: int = 0,
    timeout: Any = DEFAULT_TIMEOUT,
    throw: bool = True,
) -> Any:
    """Read each PV; see ``repics.ca.caget`` for the arguments."""
    base, offset, enum_as_string, marker = request(datatype, form)
    remaining = Deadline(timeout).remaining()
    if is_single(pv):
        try:
            snap = await channel(pv).get_async(base, offset, enum_as_string, count, remaining)  # type: ignore[arg-type]
        except CaError as e:
            return finish(pv, e, throw)  # type: ignore[arg-type]
        note_connected(pv)  # type: ignore[arg-type]
        return augment(snap, marker)
    chs = [channel(n) for n in pv]
    got = await context().get_many_async(chs, base, offset, enum_as_string, count, remaining)
    return collect(pv, got, throw, marker)


async def caput(
    pv: PVs,
    value: Any,
    wait: bool = False,
    datatype: Any = None,
    timeout: Any = DEFAULT_TIMEOUT,
    repeat_value: bool = False,
    throw: bool = True,
) -> Any:
    """Write each PV; see ``repics.ca.caput`` for the arguments."""
    remaining = Deadline(timeout).remaining()
    if is_single(pv):
        try:
            await channel(pv).put_async(put_value(value, datatype), wait=wait, timeout=remaining)  # type: ignore[arg-type]
        except CaError as e:
            return finish(pv, e, throw)  # type: ignore[arg-type]
        note_connected(pv)  # type: ignore[arg-type]
        return CaNothing(pv)  # type: ignore[arg-type]
    values = [put_value(v, datatype) for v in values_for(pv, value, repeat_value)]
    chs = [channel(n) for n in pv]
    got = await context().put_many_async(chs, values, wait, remaining)
    return collect(pv, got, throw)


async def cainfo(pv: PVs, wait: bool = True, timeout: Any = DEFAULT_TIMEOUT, throw: bool = True) -> Any:
    """Connection state, host, access rights, native type and element count.

    With ``wait=False`` the current state is reported without connecting.
    """
    names = [pv] if is_single(pv) else list(pv)
    chs = [channel(n) for n in names]
    got = (
        await context().wait_connected_many_async(chs, Deadline(timeout).remaining())
        if wait
        else [None] * len(chs)
    )
    out = [info_or(n, ch, r, throw) for n, ch, r in zip(names, chs, got)]
    return out[0] if is_single(pv) else out


# ---------------------------------------------------------------------------
# camonitor


_dispatchers: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, LoopDispatcher]" = weakref.WeakKeyDictionary()


def _dispatcher() -> LoopDispatcher:
    return per_loop(_dispatchers, lambda loop: LoopDispatcher(MonitorHub(), loop, "repics camonitor"))


class Subscription(SubscriptionBase):
    """A running monitor driven by a task on the loop that created it.

    Semantics follow ``repics.ca.Subscription``; a coroutine callback is
    awaited, so updates that arrive while it runs are collapsed
    (``all_updates=False``) or queued (``all_updates=True``).
    """

    def __init__(self, name: str, callback: Callable[..., Any], **kw: Any):
        super().__init__(_dispatcher(), name, callback, **kw)

    async def _deliver(self, value: Any) -> None:
        try:
            r = self.callback(value)
            if inspect.isawaitable(r):
                await r
        except Exception as e:  # noqa: BLE001 - reported, then the monitor stops
            self._report(e)
            self.close()


def camonitor(
    pv: PVs,
    callback: Callable[..., Any],
    form: str = "time",
    datatype: Any = None,
    count: int = 0,
    mask: int | None = None,
    all_updates: bool = False,
    notify_disconnect: bool = False,
    connect_timeout: Any = None,
) -> Subscription | list[Subscription]:
    """Subscribe to each PV from inside a coroutine; returns at once."""
    kw = dict(
        form=form,
        datatype=datatype,
        count=count,
        mask=mask,
        all_updates=all_updates,
        notify_disconnect=notify_disconnect,
        connect_timeout=connect_timeout,
    )
    if is_single(pv):
        return Subscription(pv, callback, **kw)  # type: ignore[arg-type]
    return [Subscription(name, lambda v, i=i: callback(v, i), **kw) for i, name in enumerate(pv)]
