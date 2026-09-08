"""The process-wide default context and its channel cache.

Both front ends (``epicsrs.ca`` and ``epicsrs.aio``) share one ``CaContext``
because both run on the one runtime the extension owns. Channels are cached
by name so a repeated ``caget`` does not search again; a cached channel is
never closed, matching libca's channel lifetime under pyepics and aioca.
"""

from __future__ import annotations

import threading
import time
from typing import Sequence, TypeVar, Union

from ._epicsrs import CaChannel, CaContext

T = TypeVar("T")
PVs = Union[str, Sequence[str]]

_lock = threading.Lock()
_context: CaContext | None = None
_channels: dict[str, CaChannel] = {}


def context() -> CaContext:
    """The default context, created on first use from the ``EPICS_CA_*`` environment."""
    global _context
    with _lock:
        if _context is None:
            _context = CaContext()
        return _context


def channel(name: str) -> CaChannel:
    """The cached channel for ``name``; created (and its search started) on first use."""
    ctx = context()
    with _lock:
        ch = _channels.get(name)
        if ch is None:
            ch = ctx.channel(name)
            _channels[name] = ch
        return ch


class Deadline:
    """One timeout budget across connect-then-operate; ``None`` is forever."""

    __slots__ = ("_at",)

    def __init__(self, timeout: float | None):
        self._at = None if timeout is None else time.monotonic() + timeout

    def remaining(self) -> float | None:
        if self._at is None:
            return None
        return max(0.0, self._at - time.monotonic())


def is_single(pv: PVs) -> bool:
    return isinstance(pv, str)
