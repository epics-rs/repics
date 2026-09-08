"""Blocking Channel Access front end.

Every function takes one PV name or a list of names and returns a result of
the same shape. ``timeout`` bounds the whole operation, connect included.
Failures raise ``CaError`` (``CaTimeout`` for deadlines).
"""

from __future__ import annotations

import threading
from typing import Any, Callable, Sequence

from .._context import Deadline, PVs, channel, is_single
from .._epicsrs import CaChannel, CaSubscription, ChannelInfo
from .._value import augment

__all__ = ["caget", "caput", "camonitor", "cainfo", "connect", "Subscription"]


def _connect_one(name: str, deadline: Deadline) -> CaChannel:
    ch = channel(name)
    if not ch.connected:
        ch.wait_connected(deadline.remaining())
    return ch


def _each(pv: PVs, fn: Callable[[str], Any]) -> Any:
    if is_single(pv):
        return fn(pv)  # type: ignore[arg-type]
    return [fn(name) for name in pv]


def connect(pv: PVs, timeout: float | None = 5.0) -> None:
    """Wait until each PV is connected."""
    deadline = Deadline(timeout)
    _each(pv, lambda name: _connect_one(name, deadline))


def caget(pv: PVs, form: str = "time", count: int = 0, timeout: float | None = 5.0) -> Any:
    """Read each PV. ``form`` selects the metadata (``plain``, ``sts``, ``time``, ``gr``, ``ctrl``)."""
    deadline = Deadline(timeout)

    def one(name: str) -> Any:
        ch = _connect_one(name, deadline)
        return augment(ch.get(form=form, count=count, timeout=deadline.remaining()))

    return _each(pv, one)


def caput(pv: PVs, value: Any, wait: bool = True, timeout: float | None = 5.0) -> None:
    """Write each PV. With a list of PVs, ``value`` is a list of the same length."""
    deadline = Deadline(timeout)
    if is_single(pv):
        _connect_one(pv, deadline).put(value, wait=wait, timeout=deadline.remaining())  # type: ignore[arg-type]
        return
    if len(value) != len(pv):
        raise ValueError(f"{len(pv)} PVs but {len(value)} values")
    for name, v in zip(pv, value):
        _connect_one(name, deadline).put(v, wait=wait, timeout=deadline.remaining())


def cainfo(pv: PVs, timeout: float | None = 5.0) -> ChannelInfo | list[ChannelInfo]:
    """Channel facts: host, native type, element count, access rights."""
    deadline = Deadline(timeout)
    return _each(pv, lambda name: _connect_one(name, deadline).info())


class Subscription:
    """A running monitor. ``callback(value)`` runs on the subscription's own thread."""

    def __init__(self, name: str, sub: CaSubscription, callback: Callable[[Any], None]):
        self.name = name
        self._sub = sub
        self._callback = callback
        self.dropped = 0
        self._thread = threading.Thread(target=self._run, name=f"camonitor {name}", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while True:
            try:
                snap = self._sub.recv()
            except Exception:  # noqa: BLE001 - the channel dropped; keep draining until closed
                continue
            if snap is None:
                return
            self._callback(augment(snap))

    def close(self) -> None:
        self._sub.close()
        if threading.current_thread() is not self._thread:
            self._thread.join()

    def __enter__(self) -> "Subscription":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def camonitor(
    pv: PVs,
    callback: Callable[..., None],
    deadband: float = 0.0,
    mask: int | None = None,
    timeout: float | None = 5.0,
) -> Subscription | list[Subscription]:
    """Subscribe. For one PV ``callback(value)``; for a list ``callback(value, index)``."""
    deadline = Deadline(timeout)

    def one(name: str, cb: Callable[[Any], None]) -> Subscription:
        ch = _connect_one(name, deadline)
        return Subscription(name, ch.subscribe(deadband=deadband, mask=mask), cb)

    if is_single(pv):
        return one(pv, callback)  # type: ignore[arg-type]
    return [one(name, lambda v, i=i: callback(v, i)) for i, name in enumerate(pv)]
