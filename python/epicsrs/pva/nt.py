"""Normative Types: build, wrap and unwrap ``epics:nt/*`` structures.

The shape follows ``p4p.nt``: each class has ``buildType(...)`` returning a
``Type``, ``wrap(python_value, timestamp=..., severity=..., message=...)``
returning a ``Value``, ``unwrap(Value)`` returning a plain Python value, and
``assign(Value, python_value)`` used by ``Context.put``.

``unwrap`` yields the same augmented values as the Channel Access front
ends (``epicsrs.AugmentedFloat`` and friends): ``.severity``, ``.status``,
``.timestamp``, ``.raw_stamp`` and, where the structure carries them, the
display/control limits. Every unwrapped value also carries ``.raw``, the
``Value`` it came from. Arrays are views on the wire buffer, not copies.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from datetime import datetime
from typing import Any

import numpy

from .._epicsrs import Type, Value
from .._value import _META, Augmented, AugmentedInt, AugmentedList, augment

__all__ = [
    "NTScalar",
    "NTEnum",
    "NTTable",
    "NTNDArray",
    "NTURI",
    "NTBase",
    "alarm",
    "timeStamp",
    "defaultNT",
]

timeStamp = Type([("secondsPastEpoch", "l"), ("nanoseconds", "i"), ("userTag", "i")], id="time_t")
alarm = Type([("severity", "i"), ("status", "i"), ("message", "s")], id="alarm_t")

_CODE_NAMES = {
    "?": "boolean",
    "b": "byte",
    "B": "ubyte",
    "h": "short",
    "H": "ushort",
    "i": "int",
    "I": "uint",
    "l": "long",
    "L": "ulong",
    "f": "float",
    "d": "double",
    "s": "string",
}


class _Snap:
    """What ``epicsrs._value.augment`` reads: the payload plus ``_META``."""

    def __init__(self, value: Any, **meta: Any):
        self.value = value
        for k in _META:
            setattr(self, k, meta.get(k))
        self.name = meta.get("name", "")
        self.datatype = meta.get("datatype", "structure")
        self.status = meta.get("status", 0)
        self.severity = meta.get("severity", 0)
        self.raw_stamp = meta.get("raw_stamp", (0, 0))
        self.element_count = meta.get("element_count", 1)


def _stamp(V: Value, field: str = "timeStamp") -> tuple[int, int]:
    if field not in V:
        return (0, 0)
    return (int(V[field + ".secondsPastEpoch"]), int(V[field + ".nanoseconds"]))


def _alarm(V: Value) -> tuple[int, int]:
    if "alarm" not in V:
        return (0, 0)
    return (int(V["alarm.status"]), int(V["alarm.severity"]))


def _finish(out: Any, V: Value) -> Any:
    out.raw = V
    return out


class NTBase:
    """Common wrap/unwrap machinery; subclasses provide ``buildType``."""

    Value = Value

    @staticmethod
    def buildType(*args: Any, **kws: Any) -> Type:
        raise NotImplementedError("NT classes must provide buildType()")

    def __init__(self, *args: Any, **kws: Any):
        self.type = self.buildType(*args, **kws)

    def wrap(self, value: Any, **kws: Any) -> Value:
        raise NotImplementedError("NT classes must provide wrap()")

    def assign(self, V: Value, py: Any) -> None:
        """Update ``V`` in place from ``py`` (``Context.put``)."""
        V["value"] = py

    @classmethod
    def unwrap(cls, V: Value) -> Any:
        raise NotImplementedError("NT classes must provide unwrap()")

    @staticmethod
    def _annotate(
        V: Value,
        timestamp: Any = None,
        severity: int | None = None,
        message: str | None = None,
    ) -> Value:
        if severity is not None:
            V["alarm.severity"] = severity
        if message is not None:
            V["alarm.message"] = message
        if timestamp is not None:
            if isinstance(timestamp, datetime):
                timestamp = timestamp.timestamp()
            if isinstance(timestamp, (int, float)):
                sec, frac = divmod(float(timestamp), 1.0)
                timestamp = (int(sec), int(frac * 1e9))
            V["timeStamp"] = {"secondsPastEpoch": timestamp[0], "nanoseconds": timestamp[1]}
        return V


def _metaHelper(
    F: list,
    valtype: str,
    display: bool = False,
    control: bool = False,
    valueAlarm: bool = False,
    form: bool = False,
) -> None:
    isnumeric = valtype[-1:] not in "?su"
    if display and isnumeric:
        F.append(
            (
                "display",
                (
                    "S",
                    None,
                    [
                        ("limitLow", valtype[-1:]),
                        ("limitHigh", valtype[-1:]),
                        ("description", "s"),
                        ("precision", "i"),
                        ("form", ("S", "enum_t", [("index", "i"), ("choices", "as")])),
                        ("units", "s"),
                    ]
                    if form
                    else [
                        ("limitLow", valtype[-1:]),
                        ("limitHigh", valtype[-1:]),
                        ("description", "s"),
                        ("format", "s"),
                        ("units", "s"),
                    ],
                ),
            )
        )
    elif display and not isnumeric:
        F.append(("display", ("S", None, [("description", "s"), ("units", "s")])))
    if control and isnumeric:
        F.append(
            (
                "control",
                (
                    "S",
                    None,
                    [
                        ("limitLow", valtype[-1:]),
                        ("limitHigh", valtype[-1:]),
                        ("minStep", valtype[-1:]),
                    ],
                ),
            )
        )
    if valueAlarm and isnumeric:
        F.append(
            (
                "valueAlarm",
                (
                    "S",
                    None,
                    [
                        ("active", "?"),
                        ("lowAlarmLimit", valtype[-1:]),
                        ("lowWarningLimit", valtype[-1:]),
                        ("highWarningLimit", valtype[-1:]),
                        ("highAlarmLimit", valtype[-1:]),
                        ("lowAlarmSeverity", "i"),
                        ("lowWarningSeverity", "i"),
                        ("highWarningSeverity", "i"),
                        ("highAlarmSeverity", "i"),
                        ("hysteresis", "d"),
                    ],
                ),
            )
        )


def _scalar_meta(V: Value) -> dict[str, Any]:
    """The ``_META`` entries an NTScalar-like structure carries."""
    status, severity = _alarm(V)
    meta: dict[str, Any] = {"status": status, "severity": severity, "raw_stamp": _stamp(V)}
    if "display" in V:
        meta["units"] = V.get("display.units")
        meta["precision"] = V.get("display.precision")
        meta["lower_disp_limit"] = V.get("display.limitLow")
        meta["upper_disp_limit"] = V.get("display.limitHigh")
    if "control" in V:
        meta["lower_ctrl_limit"] = V.get("control.limitLow")
        meta["upper_ctrl_limit"] = V.get("control.limitHigh")
    if "valueAlarm" in V:
        meta["lower_alarm_limit"] = V.get("valueAlarm.lowAlarmLimit")
        meta["upper_alarm_limit"] = V.get("valueAlarm.highAlarmLimit")
        meta["lower_warning_limit"] = V.get("valueAlarm.lowWarningLimit")
        meta["upper_warning_limit"] = V.get("valueAlarm.highWarningLimit")
    return meta


class NTScalar(NTBase):
    """``epics:nt/NTScalar:1.0`` and ``NTScalarArray``."""

    @staticmethod
    def buildType(
        valtype: str = "d",
        extra: list = [],
        display: bool = False,
        control: bool = False,
        valueAlarm: bool = False,
        form: bool = False,
    ) -> Type:
        isarray = valtype[:1] == "a"
        F = [("value", valtype), ("alarm", alarm), ("timeStamp", timeStamp)]
        _metaHelper(F, valtype, display=display, control=control, valueAlarm=valueAlarm, form=form)
        F.extend(extra)
        return Type(F, id="epics:nt/NTScalarArray:1.0" if isarray else "epics:nt/NTScalar:1.0")

    def __init__(self, valtype: str = "d", **kws: Any):
        self.type = self.buildType(valtype, **kws)

    def wrap(self, value: Any, **kws: Any) -> Value:
        """A dict initialises fields by name; anything else goes to ``value``."""
        if isinstance(value, Value):
            pass
        elif isinstance(value, Augmented):
            kws.setdefault("timestamp", value.timestamp)
            value = getattr(value, "raw", None) or Value(self.type, {"value": value})
        elif isinstance(value, dict):
            value = Value(self.type, value)
        else:
            value = Value(self.type, {"value": value})
        return self._annotate(value, **kws)

    @classmethod
    def unwrap(cls, V: Value) -> Any:
        payload = V["value"]
        code = V.type("value")
        if not isinstance(code, str):
            raise ValueError(f"cannot unwrap a {code!r} value field")
        meta = _scalar_meta(V)
        meta["datatype"] = _CODE_NAMES.get(code[-1:], "structure")
        if code[:1] == "a":
            if payload is None:
                payload = [] if code == "as" else numpy.zeros((0,), dtype=numpy.float64)
            meta["element_count"] = len(payload)
        return _finish(augment(_Snap(payload, **meta)), V)

    def assign(self, V: Value, py: Any) -> None:
        if isinstance(py, dict):
            for k, v in py.items():
                V[k] = v
        else:
            V["value"] = py


class NTEnum(NTBase):
    """``epics:nt/NTEnum:1.0``: an index into a list of choices."""

    @staticmethod
    def buildType(
        extra: list = [],
        display: bool = False,
        control: bool = False,
        valueAlarm: bool = False,
    ) -> Type:
        F = [
            ("value", ("S", "enum_t", [("index", "i"), ("choices", "as")])),
            ("alarm", alarm),
            ("timeStamp", timeStamp),
        ]
        _metaHelper(F, "i", display=display, control=control, valueAlarm=valueAlarm)
        F.extend(extra)
        return Type(F, id="epics:nt/NTEnum:1.0")

    def __init__(self, **kws: Any):
        self.type = self.buildType(**kws)
        self._choices: list[str] = []

    def wrap(self, value: Any, choices: list[str] | None = None, **kws: Any) -> Value:
        if isinstance(value, Value):
            pass
        elif isinstance(value, Augmented):
            kws.setdefault("timestamp", value.timestamp)
            value = getattr(value, "raw", None) or self.wrap(int(value), choices=value.enums)
        elif isinstance(value, dict):
            if {"index", "choices"}.isdisjoint(value):
                value = Value(self.type, value)
            else:
                value = Value(self.type, {"value": value})
        else:
            V = Value(self.type)
            if choices is not None:
                V["value.choices"] = choices
            self.assign(V, value)
            value = V
        return self._annotate(value, **kws)

    def unwrap(self, V: Value) -> Any:
        if V.changed("value.choices"):
            self._choices = list(V["value.choices"] or [])
        choices = list(V["value.choices"] or []) or self._choices
        idx = int(V["value.index"])
        meta = _scalar_meta(V)
        meta["datatype"] = "enum"
        meta["enums"] = choices
        out = augment(_Snap(idx, **meta))
        out.choice = choices[idx] if 0 <= idx < len(choices) else None
        return _finish(out, V)

    def assign(self, V: Value, py: Any) -> None:
        if isinstance(py, str):
            for i, choice in enumerate(V["value.choices"] or self._choices):
                if py == choice:
                    V["value.index"] = i
                    return
            py = int(py, 0)
        V["value.index"] = int(py)


class NTTable(NTBase):
    """``epics:nt/NTTable:1.0``: named columns of equal length."""

    @staticmethod
    def buildType(columns: list = [], extra: list = []) -> Type:
        return Type(
            [
                ("labels", "as"),
                ("value", ("S", None, columns)),
                ("descriptor", "s"),
                ("alarm", alarm),
                ("timeStamp", timeStamp),
            ]
            + list(extra),
            id="epics:nt/NTTable:1.0",
        )

    def __init__(self, columns: list = [], extra: list = []):
        self.labels: list[str] = []
        C = []
        for col, code in columns:
            if code[:1] == "a":
                raise ValueError("NTTable column types may not be arrays")
            C.append((col, "a" + code))
            self.labels.append(col)
        self.type = self.buildType(C, extra=extra)

    def wrap(self, values: Any, **kws: Any) -> Value:
        """``values`` is an iterable of row dicts keyed by column name."""
        if isinstance(values, Value):
            return self._annotate(values, **kws)
        cols: dict[str, list] = {L: [] for L in self.labels}
        for row in values:
            for L in self.labels:
                if L in row:
                    cols[L].append(row[L])
        cols = {L: c for L, c in cols.items() if c}
        return self._annotate(Value(self.type, {"labels": self.labels, "value": cols}), **kws)

    @classmethod
    def unwrap(cls, V: Value) -> Any:
        names, cols = [], []
        for cname, cval in V["value"].items():
            names.append(cname)
            cols.append([] if cval is None else cval)
        rows = [OrderedDict(zip(names, r)) for r in zip(*cols)]
        status, severity = _alarm(V)
        out = AugmentedList(rows)._store(
            _Snap(
                rows,
                datatype="table",
                element_count=len(rows),
                status=status,
                severity=severity,
                raw_stamp=_stamp(V),
            )
        )
        out.labels = list(V["labels"] or [])
        return _finish(out, V)

    def assign(self, V: Value, py: Any) -> None:
        V["value"] = self.wrap(py)["value"].todict()


class NTNDArray(NTBase):
    """``epics:nt/NTNDArray:1.0``: an N-dimensional array with attributes."""

    _code2u = {
        "?": "booleanValue",
        "b": "byteValue",
        "h": "shortValue",
        "i": "intValue",
        "l": "longValue",
        "B": "ubyteValue",
        "H": "ushortValue",
        "I": "uintValue",
        "L": "ulongValue",
        "f": "floatValue",
        "d": "doubleValue",
    }

    _default_type = Type(
        [
            (
                "value",
                (
                    "U",
                    None,
                    [
                        ("booleanValue", "a?"),
                        ("byteValue", "ab"),
                        ("shortValue", "ah"),
                        ("intValue", "ai"),
                        ("longValue", "al"),
                        ("ubyteValue", "aB"),
                        ("ushortValue", "aH"),
                        ("uintValue", "aI"),
                        ("ulongValue", "aL"),
                        ("floatValue", "af"),
                        ("doubleValue", "ad"),
                    ],
                ),
            ),
            ("codec", ("S", "codec_t", [("name", "s"), ("parameters", "v")])),
            ("compressedSize", "l"),
            ("uncompressedSize", "l"),
            ("uniqueId", "i"),
            ("dataTimeStamp", timeStamp),
            ("alarm", alarm),
            ("timeStamp", timeStamp),
            (
                "dimension",
                (
                    "aS",
                    "dimension_t",
                    [
                        ("size", "i"),
                        ("offset", "i"),
                        ("fullSize", "i"),
                        ("binning", "i"),
                        ("reverse", "?"),
                    ],
                ),
            ),
            (
                "attribute",
                (
                    "aS",
                    "epics:nt/NTAttribute:1.0",
                    [
                        ("name", "s"),
                        ("value", "v"),
                        ("tags", "as"),
                        ("descriptor", "s"),
                        ("alarm", alarm),
                        ("timeStamp", timeStamp),
                        ("sourceType", "i"),
                        ("source", "s"),
                    ],
                ),
            ),
        ],
        id="epics:nt/NTNDArray:1.0",
    )

    @classmethod
    def buildType(cls, extra: list = []) -> Type:
        ret = cls._default_type
        if extra:
            _, ident, members = ret.aspy()
            ret = Type(list(members) + list(extra), id=ident)
        return ret

    def __init__(self, **kws: Any):
        self.type = self.buildType(**kws)

    def wrap(self, value: Any, **kws: Any) -> Value:
        """Wrap an ndarray; ``attrib`` (kw or ``value.attrib``) becomes NTAttributes."""
        if isinstance(value, Value):
            return self._annotate(value, **kws)
        attrib = dict(getattr(value, "attrib", None) or kws.pop("attrib", None) or {})
        value = numpy.asarray(value)
        dims = value.shape
        if "ColorMode" not in attrib:
            if value.ndim == 2:
                attrib["ColorMode"] = 0
            elif value.ndim == 3:
                for idx, dim in enumerate(reversed(dims)):
                    if dim == 3:
                        attrib["ColorMode"] = 2 + idx
                        break
                else:
                    raise ValueError(f"unable to deduce the color dimension from shape {dims!r}")
        size = value.nbytes
        V = Value(
            self.type,
            {
                "value": (self._code2u[value.dtype.char], value.ravel()),
                "compressedSize": size,
                "uncompressedSize": size,
                "uniqueId": 0,
                "attribute": [translateNDAttribute(k, v) for k, v in attrib.items()],
                "dimension": [
                    {"size": n, "offset": 0, "fullSize": n, "binning": 1, "reverse": False}
                    for n in reversed(dims)
                ],
            },
        )
        return self._annotate(V, **kws)

    @classmethod
    def unwrap(cls, V: Value) -> Any:
        arr = V["value"]
        if arr is None:
            arr = numpy.zeros((0,), dtype=numpy.uint8)
        shape = [int(d.size) for d in V["dimension"]]
        shape.reverse()
        # reshape() is a view: the pixels stay in the wire buffer.
        arr = arr.reshape(shape or [arr.size])
        status, severity = _alarm(V)
        out = augment(
            _Snap(
                arr,
                datatype=str(arr.dtype),
                element_count=arr.size,
                status=status,
                severity=severity,
                raw_stamp=_stamp(V),
            )
        )
        out.attrib = {a.name: a.value for a in V.get("attribute", None) or []}
        return _finish(out, V)

    def assign(self, V: Value, py: Any) -> None:
        for k, v in self.wrap(py).todict().items():
            V[k] = v


def translateNDAttribute(name: str, value: Any) -> dict:
    if isinstance(value, Value) and "value" in value:
        out = {"name": name, "value": value["value"]}
        if "alarm" in value:
            out["alarm"] = value["alarm"]
        if "timeStamp" in value:
            out["timeStamp"] = value["timeStamp"]
        return out
    return {"name": name, "value": value}


class NTURI:
    """``epics:nt/NTURI:1.0``: the conventional RPC argument structure."""

    @staticmethod
    def buildType(args: list) -> Type:
        return Type(
            [
                ("scheme", "s"),
                ("authority", "s"),
                ("path", "s"),
                ("query", ("S", None, list(args))),
            ],
            id="epics:nt/NTURI:1.0",
        )

    def __init__(self, args: list):
        self._args = list(args)
        self.type = self.buildType(args)

    def wrap(
        self,
        path: str,
        args: tuple = (),
        kws: dict | None = None,
        scheme: str = "",
        authority: str = "",
    ) -> Value:
        AV: dict[str, Any] = {k: v for k, v in (kws or {}).items() if v is not None}
        AV.update((n, v) for (n, _t), v in zip(self._args, args))
        AT = [a for a in self._args if a[0] in AV]
        return Value(
            self.buildType(AT),
            {"scheme": scheme, "authority": authority, "path": path, "query": AV},
        )


_default_nt = {
    "epics:nt/NTScalar:1.0": NTScalar,
    "epics:nt/NTScalarArray:1.0": NTScalar,
    "epics:nt/NTEnum:1.0": NTEnum,
    "epics:nt/NTTable:1.0": NTTable,
    "epics:nt/NTNDArray:1.0": NTNDArray,
}


def defaultNT() -> dict:
    """The ID -> NT class table a ``Context`` starts from."""
    return dict(_default_nt)


class ClientUnwrapper:
    """Pick the NT helper by structure ID and apply its unwrap/assign."""

    def __init__(self, nt: dict | None = None):
        self.nt = nt if nt is not None else {}
        self.id: str | None = None
        self._unwrap = lambda V: V
        self._assign = self._default_assign

    def unwrap(self, V: Value) -> Any:
        if V.getID() != self.id:
            self._update(V)
        return self._unwrap(V)

    def assign(self, V: Value, value: Any) -> None:
        if V.getID() != self.id:
            self._update(V)
        self._assign(V, value)

    def _update(self, V: Value) -> None:
        nt = self.nt.get(V.getID())
        self.id = V.getID()
        if nt is None:
            self._unwrap = lambda V: V
            self._assign = self._default_assign
            return
        inst = nt() if isinstance(nt, type) else nt
        self._unwrap = inst.unwrap
        self._assign = inst.assign

    @staticmethod
    def _default_assign(V: Value, value: Any) -> None:
        if isinstance(value, dict):
            for k, v in value.items():
                V[k] = v
        elif "value" in V:
            V["value"] = value
        else:
            raise TypeError(f"cannot assign {type(value).__name__} to {V.getID() or 'structure'}")


class _UnwrapOnly:
    def __init__(self, fn: Any):
        self.unwrap = fn
        self.assign = ClientUnwrapper._default_assign


def buildNT(nt: dict | None | bool = None, unwrap: dict | None | bool = None) -> ClientUnwrapper:
    """The unwrapper for ``Context(nt=..., unwrap=...)``.

    ``nt`` overrides or extends the default ID table; ``unwrap`` (legacy)
    maps IDs to bare unwrap callables. ``False`` for either disables
    unwrapping entirely, so reads return ``Value``.
    """
    if unwrap is False or nt is False:
        return ClientUnwrapper({})
    if unwrap is not None:
        return ClientUnwrapper({ident: _UnwrapOnly(fn) for ident, fn in unwrap.items()})
    table = dict(_default_nt)
    table.update(nt or {})
    return ClientUnwrapper(table)


def now_stamp() -> tuple[int, int]:
    sec, frac = divmod(time.time(), 1.0)
    return (int(sec), int(frac * 1e9))
