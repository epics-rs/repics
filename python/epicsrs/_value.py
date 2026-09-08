"""Augmented values: a plain Python value carrying its EPICS metadata.

One model serves every front end. A read returns a ``float``/``int``/``str``/
``bytes``/``numpy.ndarray``/``list`` subclass whose attributes carry what
the wire carried: ``name``, ``ok``, ``datatype``, ``dbr``, ``element_count``,
``status``, ``severity``, ``timestamp``, ``raw_stamp``; and, for a ``ctrl``
read, the display/control limits, ``units``, ``precision`` and ``enums``.

The value keeps the extension's ``Snapshot`` and reads each attribute from
it on demand, so building one costs a single reference no matter how much
metadata the read carried.

Raised errors are the extension's ``CaError``/``CaTimeout``/
``CaDisconnected``. ``CaNothing`` is the *value* a ``throw=False`` call hands
back for a failure (``ok`` False, ``errorcode`` the ECA status, falsy) and
the success marker ``caput``/``connect`` return (``ok`` True).
"""

from __future__ import annotations

from datetime import datetime
from operator import attrgetter
from typing import Any

import numpy

from ._dbr import (
    DBR_CHAR_BYTES,
    DBR_CHAR_STR,
    DBR_CHAR_UNICODE,
    DBR_NO_ACCESS,
    ECA_NORMAL,
    _DATATYPE_STRINGS,
    ca_message,
    errorcode,
)
from ._epicsrs import CaError, ChannelInfo, Snapshot

_META = (
    "name",
    "datatype",
    "dbr",
    "element_count",
    "status",
    "severity",
    "raw_stamp",
    "timestamp",
    "units",
    "precision",
    "upper_disp_limit",
    "lower_disp_limit",
    "upper_alarm_limit",
    "lower_alarm_limit",
    "upper_warning_limit",
    "lower_warning_limit",
    "upper_ctrl_limit",
    "lower_ctrl_limit",
    "enums",
    "ackt",
    "acks",
)


class Augmented:
    """Mixin carrying EPICS metadata. Concrete classes pair it with a value type."""

    ok = True
    _snap: Snapshot

    name: str
    datatype: str
    dbr: int
    element_count: int
    status: int
    severity: int
    raw_stamp: tuple[int, int]
    timestamp: float
    units: str | None
    precision: int | None
    enums: list[str] | None

    @property
    def snapshot(self) -> Snapshot:
        """The extension's ``Snapshot`` this value was made from."""
        return self._snap

    @property
    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self._snap.timestamp)

    def _meta_repr(self) -> str:
        s = self._snap
        return f"name={s.name!r}, severity={s.severity}, status={s.status}"


for _k in _META:
    setattr(Augmented, _k, property(attrgetter(f"_snap.{_k}")))
del _k


class AugmentedFloat(Augmented, float):
    __str__ = float.__repr__  # the bare value; the metadata is repr's

    def __repr__(self) -> str:
        return f"{float.__repr__(self)} <{self._meta_repr()}>"


class AugmentedInt(Augmented, int):
    __str__ = int.__repr__  # the bare value; the metadata is repr's

    def __repr__(self) -> str:
        return f"{int.__repr__(self)} <{self._meta_repr()}>"


class AugmentedStr(Augmented, str):
    def __repr__(self) -> str:
        return f"{str.__repr__(self)} <{self._meta_repr()}>"


class AugmentedArray(Augmented, numpy.ndarray):
    def __array_finalize__(self, obj: Any) -> None:
        snap = getattr(obj, "_snap", None)
        if snap is not None:
            self._snap = snap

    def __repr__(self) -> str:
        return f"{numpy.ndarray.__repr__(self)} <{self._meta_repr()}>"


class AugmentedList(Augmented, list):
    __str__ = list.__repr__  # the bare value; the metadata is repr's

    def __repr__(self) -> str:
        return f"{list.__repr__(self)} <{self._meta_repr()}>"


class AugmentedBytes(Augmented, bytes):
    def __repr__(self) -> str:
        return f"{bytes.__repr__(self)} <{self._meta_repr()}>"


def _char_bytes(v: Any) -> bytes:
    """The bytes of a CHAR read, scalar or array, up to the first NUL."""
    if isinstance(v, numpy.ndarray):
        raw = v.astype(numpy.uint8).tobytes()
    else:
        raw = bytes([int(v) & 0xFF])
    nul = raw.find(b"\0")
    return raw if nul < 0 else raw[:nul]


def augment(snap: Snapshot, marker: int | None = None) -> Any:
    """Turn a ``Snapshot`` into the augmented value for its payload type.

    ``marker`` is the ``DBR_CHAR_*`` request that asked for a CHAR read to
    come back as text or bytes.
    """
    v = snap.value
    if marker == DBR_CHAR_STR or marker == DBR_CHAR_UNICODE:
        out = AugmentedStr(_char_bytes(v).decode("utf-8", errors="replace"))
    elif marker == DBR_CHAR_BYTES:
        out = AugmentedBytes(_char_bytes(v))
    elif isinstance(v, numpy.ndarray):
        out = v.view(AugmentedArray)
    elif isinstance(v, bool):
        out = AugmentedInt(int(v))
    elif isinstance(v, int):
        out = AugmentedInt(v)
    elif isinstance(v, float):
        out = AugmentedFloat(v)
    elif isinstance(v, str):
        out = AugmentedStr(v)
    elif isinstance(v, list):
        out = AugmentedList(v)
    else:  # pragma: no cover - every Rust payload type is listed above
        raise TypeError(f"unexpected payload type {type(v).__name__}")
    out._snap = snap
    return out


class CaNothing(CaError):
    """The result of an operation that produced no value.

    Returned by ``caput`` and ``connect`` on success (``ok`` True) and by
    every ``throw=False`` call on failure (``ok`` False, ``errorcode`` the
    ECA status). Falsy on failure, never iterable.
    """

    def __init__(self, name: str, errorcode: int = ECA_NORMAL):
        super().__init__(f"{name}: {ca_message(errorcode)}", errorcode)
        self.name = name
        self.ok = errorcode == ECA_NORMAL
        self.errorcode = errorcode

    @classmethod
    def from_exception(cls, name: str, exc: BaseException) -> "CaNothing":
        return cls(name, errorcode(exc))

    def __repr__(self) -> str:
        return f"CaNothing({self.name!r}, {self.errorcode})"

    def __str__(self) -> str:
        return f"{self.name}: {ca_message(self.errorcode)}"

    def __bool__(self) -> bool:
        return self.ok


class CAInfo:
    """What ``cainfo`` reports: connection state, host, access, type, count."""

    state_strings = ("never connected", "previously connected", "connected", "closed")
    datatype_strings = _DATATYPE_STRINGS

    def __init__(self, name: str, info: ChannelInfo | None, state: int):
        self.ok = True
        self.name = name
        self.state = state
        if info is None:
            self.host = "<disconnected>"
            self.read = False
            self.write = False
            self.count = 0
            self.datatype = DBR_NO_ACCESS
        else:
            self.host = info.host
            self.read = info.read_access
            self.write = info.write_access
            self.count = info.element_count
            self.datatype = info.dbr

    @property
    def datatype_name(self) -> str:
        return self.datatype_strings[self.datatype]

    def __repr__(self) -> str:
        return f"CAInfo({self.name!r}, {self.state_strings[self.state]!r}, {self.datatype_name!r}, {self.count})"

    def __str__(self) -> str:
        return f"""{self.name}:
    State: {self.state_strings[self.state]}
    Host: {self.host}
    Access: {self.read}, {self.write}
    Data type: {self.datatype_name}
    Count: {self.count}"""
