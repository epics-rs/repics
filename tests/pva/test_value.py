"""Local Value / Type / nt semantics, no network."""

from __future__ import annotations

import numpy as np
import pytest

from epicsrs.pva import Type, Value
from epicsrs.pva.nt import NTEnum, NTNDArray, NTScalar, NTTable, NTURI


def test_type_and_value_basics():
    T = Type([("value", "d"), ("a", ("S", None, [("b", "i"), ("c", "as")]))], id="x:y")
    assert T.getID() == "x:y"
    assert set(T.keys()) == {"value", "a"}
    V = Value(T, {"value": 1.5, "a": {"b": 2, "c": ["p", "q"]}})
    assert V.value == 1.5
    assert V["a.b"] == 2
    assert list(V.a.c) == ["p", "q"]
    assert set(V.changedSet()) == {"value", "a.b", "a.c"}
    V.unmark()
    assert set(V.changedSet()) == set()
    V["a.b"] = 3
    assert V.changed("a.b") and not V.changed("value")
    assert V.a.changed("b")
    assert V.todict() == {"value": 1.5, "a": {"b": 3, "c": ["p", "q"]}}
    with pytest.raises(KeyError):
        V["nope"]
    with pytest.raises(AttributeError):
        V.nope


def test_type_aspy_round_trips():
    T = Type([("value", "d"), ("a", ("S", None, [("b", "i"), ("c", "as")]))], id="x:y")
    T2 = Type(T.aspy())
    assert T2 == T and T2.getID() == "x:y"
    # An explicit id overrides the one carried in the spec.
    assert Type(T.aspy(), id="new:id").getID() == "new:id"
    # A three-member list must not be mistaken for a (code, id, members) tuple.
    assert Type([("a", "i"), ("b", "i"), ("c", "i")]).keys() == ["a", "b", "c"]


def test_ntscalar_wrap_unwrap_metadata():
    nt = NTScalar("d", display=True, control=True, valueAlarm=True)
    V = nt.wrap(2.5, timestamp=1700000000.25, severity=1, message="HIGH")
    assert V.getID() == "epics:nt/NTScalar:1.0"
    assert (V.timeStamp.secondsPastEpoch, V.timeStamp.nanoseconds) == (1700000000, 250000000)
    assert (V.alarm.severity, V.alarm.message) == (1, "HIGH")
    u = nt.unwrap(V)
    assert u == 2.5 and u.severity == 1 and u.timestamp == pytest.approx(1700000000.25)
    assert u.raw is V
    # a wrapped scalar type carries its metadata structures
    assert {"display", "control", "valueAlarm"} <= set(V.keys())
    # wrapping a marked Value is a no-op passthrough
    assert nt.wrap(V) is V


def test_ntscalar_array_and_string():
    V = NTScalar("ai").wrap([1, 2, 3])
    assert V.value.dtype == np.int32 and list(V.value) == [1, 2, 3]
    S = NTScalar("s").wrap("hi")
    assert NTScalar("s").unwrap(S) == "hi"


def test_ntenum():
    nt = NTEnum()
    V = nt.wrap({"index": 1, "choices": ["a", "b"]})
    assert (V.value.index, list(V.value.choices)) == (1, ["a", "b"])
    u = nt.unwrap(V)
    assert u == 1 and str(u) == "b" and u.choice == "b"  # an int that prints its choice, as p4p
    assert repr(u) == "ntenum(1, b)" and u.severity == 0
    W = nt.wrap("a", choices=["a", "b"])
    assert W.value.index == 0
    with pytest.raises(ValueError):
        nt.wrap("zzz", choices=["a", "b"])


def test_nttable():
    nt = NTTable([("name", "s"), ("x", "d")])
    V = nt.wrap([{"name": "n1", "x": 1.0}, {"name": "n2", "x": 2.0}])
    assert list(V.labels) == ["name", "x"]
    rows = nt.unwrap(V)
    assert [dict(r) for r in rows] == [{"name": "n1", "x": 1.0}, {"name": "n2", "x": 2.0}]


def test_ntndarray_round_trip_shape_and_dtype():
    nt = NTNDArray()
    img = np.arange(12, dtype="u2").reshape(3, 4)
    V = nt.wrap(img)
    assert V.getID() == "epics:nt/NTNDArray:1.0"
    assert [d.size for d in V.dimension] == [4, 3]  # x then y, like p4p
    out = nt.unwrap(V)
    assert out.shape == (3, 4) and out.dtype == np.uint16
    np.testing.assert_array_equal(out, img)


def test_nturi():
    nt = NTURI([("a", "d"), ("b", "s")])
    V = nt.wrap("pv:name", kws={"a": 1.5, "b": "x"})
    assert V.getID() == "epics:nt/NTURI:1.0"
    assert V.path == "pv:name" and V.scheme == ""
    assert (V.query.a, V.query.b) == (1.5, "x")
    W = nt.wrap("pv:name", args=(2.0,))
    assert set(W.query.keys()) == {"a"} and W.query.a == 2.0
    with pytest.raises(ValueError, match="Unable to initialize NTURI"):
        nt.wrap("pv:name", kws={"zzz": 1})
