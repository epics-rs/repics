"""The process-wide default context and its channel cache.

Both front ends (``epicsrs.ca`` and ``epicsrs.aio``) share one ``CaContext``
because both run on the one runtime the extension owns. Channels are cached
by name so a repeated ``caget`` does not search again; a cached channel is
never closed, matching libca's channel lifetime under pyepics and aioca.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Sequence, Union

from ._epicsrs import CaChannel, CaContext

PVs = Union[str, Sequence[str]]

# Channel states as ``cainfo`` reports them.
NEVER_CONNECTED = 0
PREVIOUSLY_CONNECTED = 1
CONNECTED = 2
CLOSED = 3


class _Entry:
    """A cached channel and what has been observed of its connection."""

    __slots__ = ("channel", "seen_connected", "subscribers")

    def __init__(self, channel: CaChannel):
        self.channel = channel
        self.seen_connected = False
        self.subscribers = 0


_lock = threading.Lock()
_context: CaContext | None = None
_entries: dict[str, _Entry] = {}


def context() -> CaContext:
    """The default context, created on first use from the ``EPICS_CA_*`` environment."""
    global _context
    with _lock:
        if _context is None:
            _context = CaContext()
        return _context


def _entry(name: str) -> _Entry:
    ctx = context()
    with _lock:
        e = _entries.get(name)
        if e is None:
            e = _Entry(ctx.channel(name))
            _entries[name] = e
        return e


def channel(name: str) -> CaChannel:
    """The cached channel for ``name``; created (and its search started) on first use."""
    return _entry(name).channel


def note_connected(name: str) -> None:
    """Record that ``name`` was seen connected, for ``cainfo``'s state."""
    _entry(name).seen_connected = True


def state(name: str) -> int:
    e = _entry(name)
    if e.channel.connected:
        e.seen_connected = True
        return CONNECTED
    return PREVIOUSLY_CONNECTED if e.seen_connected else NEVER_CONNECTED


def add_subscriber(name: str, delta: int) -> None:
    with _lock:
        e = _entries.get(name)
        if e is not None:
            e.subscribers += delta


class ChannelStatus:
    """One cached channel as ``get_channel_infos`` reports it."""

    __slots__ = ("name", "connected", "subscriber_count")

    def __init__(self, name: str, connected: bool, subscriber_count: int):
        self.name = name
        self.connected = connected
        self.subscriber_count = subscriber_count

    def __repr__(self) -> str:
        return f"ChannelStatus({self.name!r}, connected={self.connected}, subscriber_count={self.subscriber_count})"


def get_channel_infos() -> list[ChannelStatus]:
    """Every cached channel, its connection state and live subscription count."""
    with _lock:
        return [ChannelStatus(n, e.channel.connected, e.subscribers) for n, e in _entries.items()]


def purge_channel_caches() -> None:
    """Forget every cached channel. Live subscriptions keep their own handles."""
    with _lock:
        _entries.clear()


class Deadline:
    """One timeout budget across connect-then-operate; ``None`` is forever.

    ``timeout`` may be seconds, ``None``, or a one-tuple ``(deadline,)`` of
    an absolute ``time.time()`` instant (the aioca convention).
    """

    __slots__ = ("_at",)

    def __init__(self, timeout: Any):
        if timeout is None:
            self._at = None
        elif isinstance(timeout, tuple):
            self._at = time.monotonic() + (timeout[0] - time.time())
        else:
            self._at = time.monotonic() + float(timeout)

    def remaining(self) -> float | None:
        if self._at is None:
            return None
        return max(0.0, self._at - time.monotonic())


def is_single(pv: PVs) -> bool:
    return isinstance(pv, str)
