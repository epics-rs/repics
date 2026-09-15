"""Channel Access server, asyncio flavour (p4p ``p4p.server.asyncio``).

Each ``SharedPV`` drains its own event queue with a task on the loop that
created it; the ``put`` handler may be a plain function or a coroutine. A
coroutine handler is scheduled as a task and must call ``op.done()`` itself.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import weakref
from typing import Any, Callable

from ..._repics import CaWorkQueue
from ._base import Handler, Server, ServerOperation, SharedPVBase, StaticProvider, deliver, fail_op

__all__ = ["SharedPV", "Handler", "ServerOperation", "StaticProvider", "Server"]

log = logging.getLogger("repics.ca.server")


class _TaskQueue:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._raw = CaWorkQueue()
        self._task = loop.create_task(self._drain(), name="repics.ca.server")

    async def _drain(self) -> None:
        while deliver(await self._raw.recv_async()):
            pass

    def stop(self) -> None:
        self._raw.stop()


class SharedPV(SharedPVBase):
    """A served CA value channel whose handler runs on the asyncio loop.

    Must be created inside a running loop.
    """

    def __init__(self, handler: Any = None, initial: Any = None):
        self._loop = asyncio.get_running_loop()
        self._tq = _TaskQueue(self._loop)
        self._tasks: set[asyncio.Task] = set()
        # A collected PV must not leave its drain task waiting forever.
        weakref.finalize(self, self._tq.stop)
        super().__init__(handler=handler, initial=initial)

    def _queue(self) -> _TaskQueue:
        return self._tq

    def _run(self, fn: Callable, op: Any) -> None:
        try:
            r = fn()
        except Exception as e:  # noqa: BLE001 - reported to the client
            fail_op(op, e)
            return
        if inspect.iscoroutine(r):
            task = self._loop.create_task(r)
            self._tasks.add(task)

            def finished(t: asyncio.Task) -> None:
                self._tasks.discard(t)
                if t.cancelled():
                    fail_op(op, asyncio.CancelledError("handler cancelled"))
                elif t.exception() is not None:
                    fail_op(op, t.exception())

            task.add_done_callback(finished)

    def stop(self) -> None:
        """Stop the drain task; the PV can no longer serve put."""
        self._tq.stop()
