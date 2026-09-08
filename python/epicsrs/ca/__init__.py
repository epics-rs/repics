"""Blocking Channel Access front end.

Every function takes one PV name or a sequence of names and returns a result
of the same shape; a sequence is worked on concurrently. ``timeout`` bounds
the whole operation, connect included, and may be seconds, ``None`` (wait
forever) or a one-tuple ``(deadline,)`` of an absolute ``time.time()``.

Failures raise ``CaError`` (``CaTimeout`` for deadlines, ``CaDisconnected``
for a channel that is not connected). With ``throw=False`` a failure is
returned instead, as a ``CaNothing`` whose ``ok`` is False.
"""

from __future__ import annotations

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
from .._epicsrs import CaError, MonitorHub
from .._monitor import SubscriptionBase, ThreadDispatcher
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
# connect / caget / caput / cainfo
#
# Every operation is one Rust call per PV, connect included, all of a list
# in flight together against the one deadline; Python only shapes the
# request and the result.


def connect(pv: PVs, wait: bool = True, timeout: Any = DEFAULT_TIMEOUT, throw: bool = True) -> Any:
    """Start (``wait=False``) or complete (``wait=True``) each PV's connection.

    Returns a ``CaNothing`` per PV: ``ok`` True once connected.
    """
    names = [pv] if is_single(pv) else list(pv)
    chs = [channel(n) for n in names]
    if wait:
        got = context().wait_connected_many(chs, Deadline(timeout).remaining())
        out = collect(names, got, throw)
    else:
        out = [CaNothing(n) for n in names]
    return out[0] if is_single(pv) else out


def caget(
    pv: PVs,
    form: str = "time",
    datatype: Any = None,
    count: int = 0,
    timeout: Any = DEFAULT_TIMEOUT,
    throw: bool = True,
) -> Any:
    """Read each PV.

    ``form`` selects the metadata class (``plain``, ``sts``, ``time``,
    ``gr``, ``ctrl``); ``datatype`` overrides the wire type (``str``,
    ``int``, ``float``, a numpy dtype, a ``DBR_*`` code, ``DBR_CHAR_STR``,
    ``DBR_ENUM_STR``); ``count`` is 0 for the server's current element
    count, negative for the full native count, else a cap.
    """
    base, offset, enum_as_string, marker = request(datatype, form)
    remaining = Deadline(timeout).remaining()
    if is_single(pv):
        try:
            snap = channel(pv).get(base, offset, enum_as_string, count, remaining)  # type: ignore[arg-type]
        except CaError as e:
            return finish(pv, e, throw)  # type: ignore[arg-type]
        note_connected(pv)  # type: ignore[arg-type]
        return augment(snap, marker)
    chs = [channel(n) for n in pv]
    got = context().get_many(chs, base, offset, enum_as_string, count, remaining)
    return collect(pv, got, throw, marker)


def caput(
    pv: PVs,
    value: Any,
    wait: bool = False,
    datatype: Any = None,
    timeout: Any = DEFAULT_TIMEOUT,
    repeat_value: bool = False,
    throw: bool = True,
) -> Any:
    """Write each PV. With a sequence of PVs, ``value`` is repeated if it is a
    scalar or string (or ``repeat_value``), else zipped one per PV.

    ``wait=True`` returns after the record has processed. Returns a
    ``CaNothing`` per PV, ``ok`` True on success.
    """
    remaining = Deadline(timeout).remaining()
    if is_single(pv):
        try:
            channel(pv).put(put_value(value, datatype), wait=wait, timeout=remaining)  # type: ignore[arg-type]
        except CaError as e:
            return finish(pv, e, throw)  # type: ignore[arg-type]
        note_connected(pv)  # type: ignore[arg-type]
        return CaNothing(pv)  # type: ignore[arg-type]
    values = [put_value(v, datatype) for v in values_for(pv, value, repeat_value)]
    chs = [channel(n) for n in pv]
    got = context().put_many(chs, values, wait, remaining)
    return collect(pv, got, throw)


def cainfo(pv: PVs, wait: bool = True, timeout: Any = DEFAULT_TIMEOUT, throw: bool = True) -> Any:
    """Connection state, host, access rights, native type and element count.

    With ``wait=False`` the current state is reported without connecting.
    """
    names = [pv] if is_single(pv) else list(pv)
    chs = [channel(n) for n in names]
    got = context().wait_connected_many(chs, Deadline(timeout).remaining()) if wait else [None] * len(chs)
    out = [info_or(n, ch, r, throw) for n, ch, r in zip(names, chs, got)]
    return out[0] if is_single(pv) else out


# ---------------------------------------------------------------------------
# camonitor


_dispatcher = ThreadDispatcher(MonitorHub(), "epicsrs camonitor")


class Subscription(SubscriptionBase):
    """A running monitor. Callbacks run on the front end's dispatcher
    thread, shared by every subscription, in arrival order.

    The subscription connects in the background (reporting a ``CaNothing``
    with ``ECA_TIMEOUT`` if ``connect_timeout`` passes first, then keeps
    waiting), then delivers every update. With ``all_updates=False``
    updates that queued while the dispatcher was busy collapse into the
    latest one and ``dropped_callbacks`` counts them. A disconnect is
    delivered as a ``CaNothing`` with ``ECA_DISCONN`` when
    ``notify_disconnect`` is set; the monitor resumes on reconnection
    either way.
    """

    def __init__(self, name: str, callback: Callable[..., Any], **kw: Any):
        super().__init__(_dispatcher, name, callback, **kw)

    def _deliver(self, value: Any) -> None:
        try:
            self.callback(value)
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
    """Subscribe to each PV; returns at once, the connection completes in
    the background. For one PV ``callback(value)``; for a sequence
    ``callback(value, index)``.
    """
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
