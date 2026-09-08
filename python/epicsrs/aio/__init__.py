"""asyncio Channel Access front end. Same shapes as ``epicsrs.ca``, awaitable."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any, Callable, Sequence

from .._context import Deadline, PVs, channel, is_single
from .._epicsrs import CaChannel, CaSubscription, ChannelInfo
from .._value import augment

__all__ = ["caget", "caput", "camonitor", "cainfo", "connect", "Subscription"]


async def _connect_one(name: str, deadline: Deadline) -> CaChannel:
    ch = channel(name)
    if not ch.connected:
        await ch.wait_connected_async(deadline.remaining())
    return ch


async def _each(pv: PVs, fn: Callable[[str], Any]) -> Any:
    if is_single(pv):
        return await fn(pv)  # type: ignore[arg-type]
    return list(await asyncio.gather(*(fn(name) for name in pv)))


async def connect(pv: PVs, timeout: float | None = 5.0) -> None:
    deadline = Deadline(timeout)
    await _each(pv, lambda name: _connect_one(name, deadline))


async def caget(pv: PVs, form: str = "time", count: int = 0, timeout: float | None = 5.0) -> Any:
    deadline = Deadline(timeout)

    async def one(name: str) -> Any:
        ch = await _connect_one(name, deadline)
        return augment(await ch.get_async(form=form, count=count, timeout=deadline.remaining()))

    return await _each(pv, one)


async def caput(pv: PVs, value: Any, wait: bool = True, timeout: float | None = 5.0) -> None:
    deadline = Deadline(timeout)

    async def one(name: str, v: Any) -> None:
        ch = await _connect_one(name, deadline)
        await ch.put_async(v, wait=wait, timeout=deadline.remaining())

    if is_single(pv):
        await one(pv, value)  # type: ignore[arg-type]
        return
    if len(value) != len(pv):
        raise ValueError(f"{len(pv)} PVs but {len(value)} values")
    await asyncio.gather(*(one(name, v) for name, v in zip(pv, value)))


async def cainfo(pv: PVs, timeout: float | None = 5.0) -> ChannelInfo | list[ChannelInfo]:
    deadline = Deadline(timeout)

    async def one(name: str) -> ChannelInfo:
        return (await _connect_one(name, deadline)).info()

    return await _each(pv, one)


class Subscription:
    """A running monitor driven by a task on the current loop.

    ``callback`` may be a plain function or a coroutine function; a coroutine
    callback is awaited before the next update is taken, so a slow consumer
    back-pressures the subscription instead of piling up updates.
    """

    def __init__(self, name: str, sub: CaSubscription, callback: Callable[..., Any]):
        self.name = name
        self._sub = sub
        self._callback = callback
        self._task = asyncio.get_running_loop().create_task(self._run(), name=f"camonitor {name}")

    async def _run(self) -> None:
        while True:
            try:
                snap = await self._sub.recv_async()
            except Exception:  # noqa: BLE001 - the channel dropped; keep draining until closed
                continue
            if snap is None:
                return
            r = self._callback(augment(snap))
            if inspect.isawaitable(r):
                await r

    def close(self) -> None:
        self._sub.close()
        self._task.cancel()

    def __enter__(self) -> "Subscription":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


async def camonitor(
    pv: PVs,
    callback: Callable[..., Any],
    deadband: float = 0.0,
    mask: int | None = None,
    timeout: float | None = 5.0,
) -> Subscription | list[Subscription]:
    deadline = Deadline(timeout)

    async def one(name: str, cb: Callable[..., Any]) -> Subscription:
        ch = await _connect_one(name, deadline)
        return Subscription(name, await ch.subscribe_async(deadband=deadband, mask=mask), cb)

    if is_single(pv):
        return await one(pv, callback)  # type: ignore[arg-type]
    return list(
        await asyncio.gather(
            *(one(name, lambda v, i=i: callback(v, i)) for i, name in enumerate(pv))
        )
    )
