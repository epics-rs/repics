"""epicsrs.pva.Context against a p4p server."""

from __future__ import annotations

import threading
import time

import numpy as np
import pytest

from epicsrs import PvaRemoteError, PvaTimeout
from epicsrs.pva import Context, Disconnected, Value
from epicsrs.pva.nt import NTScalar, NTURI

IMAGE_SHAPE = (1000, 1000)


@pytest.fixture(scope="module")
def ctxt(p4p_conf):
    with Context("pva", conf=p4p_conf, useenv=False) as C:
        yield C


@pytest.fixture(autouse=True)
def _name(pvname):
    global name
    name = pvname


def _wait(pred, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.01)
    return False


def test_get_scalar_metadata(ctxt):
    v = ctxt.get(name("scalar"))
    assert v == 1.5
    assert v.name == name("scalar")
    assert (v.severity, v.status) == (1, 3)
    assert v.raw_stamp == (1700000000, 42)
    assert abs(v.timestamp - 1700000000.000000042) < 1e-6
    assert v.units == "mm"
    assert (v.lower_disp_limit, v.upper_disp_limit) == (-10.0, 10.0)
    assert (v.lower_ctrl_limit, v.upper_ctrl_limit) == (-5.0, 5.0)
    assert (v.upper_warning_limit, v.upper_alarm_limit) == (4.0, 8.0)
    assert isinstance(v.raw, Value)
    assert v.raw.getID() == "epics:nt/NTScalar:1.0"
    assert v.raw.alarm.message == "HIGH"


def test_get_raw_value_and_type(ctxt):
    V = ctxt.get(name("scalar")).raw
    assert V["value"] == 1.5
    assert V.display.units == "mm"
    assert set(V.keys()) >= {"value", "alarm", "timeStamp", "display", "control", "valueAlarm"}
    assert "value" in V.changedSet()
    T = V.type()
    assert T.getID() == "epics:nt/NTScalar:1.0"
    assert T["value"] == "d"
    assert T["alarm"].getID() == "alarm_t"
    assert dict(V.todict()["alarm"]) == {"severity": 1, "status": 3, "message": "HIGH"}
    info = ctxt.info(name("scalar"))
    assert info.getID() == "epics:nt/NTScalar:1.0"
    assert list(info) == list(T)


def test_get_string_and_array(ctxt):
    assert ctxt.get(name("string")) == "hello"
    a = ctxt.get(name("array"))
    assert isinstance(a, np.ndarray)
    np.testing.assert_array_equal(a, np.arange(5, dtype="f8"))
    assert a.element_count == 5


def test_get_list(ctxt):
    got = ctxt.get([name("integer"), name("string")])
    assert got == [7, "hello"]
    assert [g.name for g in got] == [name("integer"), name("string")]


def test_put_scalar_and_dict(ctxt, p4p_pvs):
    _, pvs, _ = p4p_pvs
    ctxt.put(name("scalar"), 2.5)
    assert ctxt.get(name("scalar")) == 2.5
    ctxt.put(name("scalar"), {"value": 3.0, "alarm": {"severity": 2}})
    v = ctxt.get(name("scalar"))
    assert (v, v.severity) == (3.0, 2)
    # the server's stored value must carry both fields (a marked-delta put)
    cur = pvs["scalar"].current()
    assert (cur.raw.value, cur.raw.alarm.severity) == (3.0, 2)
    ctxt.put(name("scalar"), 1.5)


def test_put_value_marks_only(ctxt):
    V = ctxt.get(name("integer")).raw
    V.unmark()
    V.value = 11
    assert V.changedSet() == {"value"}
    ctxt.put(name("integer"), V)
    assert ctxt.get(name("integer")) == 11
    ctxt.put(name("integer"), 7)


def test_put_rejected_by_handler(ctxt):
    with pytest.raises(PvaRemoteError, match="too big"):
        ctxt.put(name("scalar"), 1000.0)
    err = ctxt.put(name("scalar"), 1000.0, throw=False)
    assert isinstance(err, PvaRemoteError)
    assert ctxt.get(name("scalar")) == 1.5


def test_put_wait_process(ctxt):
    ctxt.put(name("integer"), 8, wait=True)
    assert ctxt.get(name("integer")) == 8
    ctxt.put(name("integer"), 7, process=False, wait=False)
    assert ctxt.get(name("integer")) == 7
    with pytest.raises(ValueError):
        ctxt.put(name("integer"), 7, request="field()", wait=True)


def test_enum(ctxt):
    v = ctxt.get(name("enum"))
    assert v == 1
    assert v.choice == "On"
    assert v.enums == ["Off", "On", "Auto"]
    ctxt.put(name("enum"), "Auto")
    assert ctxt.get(name("enum")).choice == "Auto"
    ctxt.put(name("enum"), 0)
    assert ctxt.get(name("enum")).choice == "Off"
    ctxt.put(name("enum"), 1)


def test_table(ctxt):
    rows = ctxt.get(name("table"))
    assert rows.labels == ["name", "x", "n"]
    assert [dict(r) for r in rows] == [
        {"name": "a", "x": 1.0, "n": 1},
        {"name": "b", "x": 2.5, "n": 2},
    ]
    assert isinstance(rows[1]["x"], float)


def test_ndarray_zero_copy(ctxt, p4p_pvs):
    _, _, image = p4p_pvs
    a = ctxt.get(name("ndarray"))
    assert a.shape == IMAGE_SHAPE
    assert a.size >= 1_000_000
    assert a.dtype == np.uint16
    np.testing.assert_array_equal(a, image)
    assert a.attrib["ColorMode"] == 0
    # Zero copy: the array is a view over memory the Rust Arc<[u16]> owns.
    # The base chain ends at the PyCapsule holding that Arc, and every view
    # in the chain starts at the same address.
    chain = []
    base = a
    while isinstance(base, np.ndarray):
        chain.append(base)
        base = base.base
    assert type(base).__name__ == "PyCapsule", chain
    assert all(not x.flags.owndata for x in chain)
    assert len({x.ctypes.data for x in chain}) == 1
    assert not a.flags.writeable
    with pytest.raises(ValueError):
        a[0, 0] = 1


def test_rpc(ctxt):
    arg = NTURI([("a", "d"), ("b", "d")]).wrap(name("scalar"), kws={"a": 1.25, "b": 2.0})
    assert arg.getID() == "epics:nt/NTURI:1.0"
    r = ctxt.rpc(name("scalar"), arg)
    assert r == 3.25
    assert r.raw.getID() == "epics:nt/NTScalar:1.0"


def test_monitor(ctxt, p4p_pvs):
    _, pvs, _ = p4p_pvs
    got = []
    ready = threading.Event()

    def cb(v):
        got.append(v)
        ready.set()

    with ctxt.monitor(name("integer"), cb, notify_disconnect=True) as sub:
        assert sub.name == name("integer")
        assert _wait(lambda: len(got) >= 2)
        assert isinstance(got[0], Disconnected)
        assert got[1] == 7
        assert got[1].raw.changedSet() >= {"value"}
        pvs["integer"].post(20)
        pvs["integer"].post(21)
        assert _wait(lambda: len(got) >= 4)
        assert [int(g) for g in got[2:4]] == [20, 21]
        assert got[3].raw.changedSet() == {"value"}
        sub.pause()
        time.sleep(0.2)  # STOP is in flight; let the server apply it
        n = len(got)
        pvs["integer"].post(30)
        time.sleep(0.2)
        assert len(got) == n
        sub.resume()
        assert _wait(lambda: len(got) > n)
        assert got[-1] == 30
    pvs["integer"].post(7)


def test_monitor_squashes_when_slow(ctxt, p4p_pvs):
    _, pvs, _ = p4p_pvs
    got = []
    gate = threading.Event()

    def cb(v):
        gate.wait(5.0)
        got.append(int(v))

    with ctxt.monitor(name("integer"), cb, limit=2):
        for i in range(50):
            pvs["integer"].post(100 + i)
        time.sleep(0.2)
        gate.set()
        assert _wait(lambda: got and got[-1] == 149)
    # limit=2 plus the one in flight: far fewer than 51 deliveries, newest kept
    assert len(got) < 20
    pvs["integer"].post(7)


def test_monitor_with_queue(ctxt):
    import queue

    q = queue.Queue()
    with ctxt.monitor(name("string"), lambda v: None, queue=q):
        job = q.get(timeout=5.0)
    assert callable(job)
    job()


def test_missing_pv_times_out(ctxt):
    t0 = time.monotonic()
    with pytest.raises(PvaTimeout):
        ctxt.get(name("nope"), timeout=0.3)
    assert time.monotonic() - t0 < 2.0
    err = ctxt.get(name("nope"), timeout=0.3, throw=False)
    assert isinstance(err, PvaTimeout)


def test_connect(ctxt):
    host = ctxt.connect(name("scalar"))
    assert host.startswith("127.0.0.1:")


def test_context_useenv_false_ignores_environment(monkeypatch, p4p_conf):
    monkeypatch.setenv("EPICS_PVA_ADDR_LIST", "192.0.2.1")
    monkeypatch.setenv("EPICS_PVA_AUTO_ADDR_LIST", "NO")
    with Context("pva", conf=p4p_conf, useenv=False) as C:
        assert C.get(name("string")) == "hello"
