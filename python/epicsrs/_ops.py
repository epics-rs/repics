"""Policy shared by the blocking and asyncio front ends.

Everything here is pure: how a put value is shaped for the wire, how a
failure becomes a raised error or a ``CaNothing``, and how a list result is
put back into the caller's shape. The front ends supply only the waiting;
connecting is part of every read and write, done in Rust against the one
deadline. Monitors are in ``_monitor``.
"""

from __future__ import annotations

from typing import Any, Sequence

import numpy

from . import _context
from ._dbr import DBR_CHAR_BYTES, DBR_CHAR_STR, DBR_CHAR_UNICODE, DBR_ENUM_STR
from ._epicsrs import CaChannel, Snapshot
from ._value import CAInfo, CaNothing, augment

DEFAULT_TIMEOUT = 5.0


def put_value(value: Any, datatype: Any) -> Any:
    """Shape a value for the wire when ``datatype`` asks for a conversion."""
    if datatype in (DBR_CHAR_STR, DBR_CHAR_UNICODE):
        text = value if isinstance(value, str) else str(value)
        return numpy.frombuffer(text.encode("utf-8") + b"\0", dtype=numpy.uint8).copy()
    if datatype == DBR_CHAR_BYTES:
        return numpy.frombuffer(bytes(value) + b"\0", dtype=numpy.uint8).copy()
    if datatype is str or datatype == DBR_ENUM_STR:
        return value if isinstance(value, str) else str(value)
    if datatype is int:
        return int(value)
    if datatype is float:
        return float(value)
    return value


def values_for(pvs: Sequence[str], values: Any, repeat_value: bool) -> list[Any]:
    """One value per PV: a scalar/string is repeated, a sequence is zipped."""
    if repeat_value or isinstance(values, (str, bytes)) or not hasattr(values, "__iter__"):
        return [values] * len(pvs)
    values = list(values)
    if len(values) != len(pvs):
        raise ValueError(f"{len(pvs)} PVs but {len(values)} values")
    return values


def finish(name: str, result: Any, throw: bool) -> Any:
    """Turn a per-PV outcome into the returned value or the raised error."""
    if isinstance(result, BaseException):
        if throw:
            raise result
        return CaNothing.from_exception(name, result)
    return result


# ---------------------------------------------------------------------------
# list operations


def collect(names: Sequence[str], got: Sequence[Any], throw: bool, marker: int | None = None) -> list[Any]:
    """Per-PV results of a many-op back in the caller's shape: a
    ``Snapshot`` is augmented, ``None`` (a completed write) becomes a
    ``CaNothing``, an exception is raised or returned per ``throw``."""
    out = []
    for n, r in zip(names, got):
        if isinstance(r, Snapshot):
            _context.note_connected(n)
            out.append(augment(r, marker))
        elif r is None:
            _context.note_connected(n)
            out.append(CaNothing(n))
        else:
            out.append(finish(n, r, throw))
    return out


def info_or(name: str, ch: CaChannel, err: Any, throw: bool) -> Any:
    """A ``CAInfo`` for ``name``, or the outcome of a failed connect."""
    if err is not None:
        return finish(name, err, throw)
    return CAInfo(name, ch.info() if ch.connected else None, _context.state(name))
