"""Augmented values: a plain Python value carrying its EPICS metadata.

One model serves every front end. A read returns a ``float``/``int``/``str``/
``numpy.ndarray``/``list`` subclass whose attributes carry what the wire
carried: ``name``, ``ok``, ``datatype``, ``element_count``, ``status``,
``severity``, ``timestamp``, ``raw_stamp``; and, for a ``ctrl`` read, the
display/control limits, ``units``, ``precision`` and ``enums``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import numpy

_META = (
    "name",
    "ok",
    "datatype",
    "element_count",
    "status",
    "severity",
    "raw_stamp",
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

    name: str
    ok: bool
    datatype: str
    element_count: int
    status: int
    severity: int
    raw_stamp: tuple[int, int]
    units: str | None
    precision: int | None
    enums: list[str] | None

    def _store(self, snap: Any) -> "Augmented":
        for k in _META:
            if k == "ok":
                continue
            setattr(self, k, getattr(snap, k))
        self.datatype = self.datatype.lower()
        self.ok = True
        return self

    @property
    def timestamp(self) -> float:
        secs, nsec = self.raw_stamp
        return secs + nsec * 1e-9

    @property
    def datetime(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp)

    def _meta_repr(self) -> str:
        return f"name={self.name!r}, severity={self.severity}, status={self.status}"


class AugmentedFloat(Augmented, float):
    def __repr__(self) -> str:
        return f"{float.__repr__(self)} <{self._meta_repr()}>"


class AugmentedInt(Augmented, int):
    def __repr__(self) -> str:
        return f"{int.__repr__(self)} <{self._meta_repr()}>"


class AugmentedStr(Augmented, str):
    def __repr__(self) -> str:
        return f"{str.__repr__(self)} <{self._meta_repr()}>"


class AugmentedArray(Augmented, numpy.ndarray):
    def __array_finalize__(self, obj: Any) -> None:
        if obj is None:
            return
        for k in _META:
            if hasattr(obj, k):
                setattr(self, k, getattr(obj, k))

    def __repr__(self) -> str:
        return f"{numpy.ndarray.__repr__(self)} <{self._meta_repr()}>"


class AugmentedList(Augmented, list):
    def __repr__(self) -> str:
        return f"{list.__repr__(self)} <{self._meta_repr()}>"


def augment(snap: Any) -> Any:
    """Turn a ``Snapshot`` into the augmented value for its payload type."""
    v = snap.value
    if isinstance(v, numpy.ndarray):
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
    return out._store(snap)
