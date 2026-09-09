"""epicsrs.pva.server against a p4p client."""

from __future__ import annotations

import threading
import time

import numpy as np
import pytest
from p4p.client.thread import Context, RemoteError
from p4p.nt import NTURI

from epicsrs.pva import Value
from epicsrs.pva.nt import NTEnum, NTNDArray, NTScalar, NTTable
from epicsrs.pva.server import Handler, Server, SharedPV, StaticProvider

P = "epicsrs-srv-test:"


def _wait(pred, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.01)
    return False


class Recorder(Handler):
    def __init__(self):
        self.puts = []
        self.events = []
        self.first = threading.Event()
        self.last = threading.Event()

    def put(self, pv, op):
        v = op.value()
        self.puts.append((v, set(v.raw.changedSet()), op.peer(), op.account()))
        if v > 100:
            op.done(error="too big")
            return
        pv.post(v)
        op.done()

    def rpc(self, pv, op):
        V = op.value()
        assert isinstance(V, Value)
        q = V.query
        op.done(NTScalar("d").wrap(float(q.a) * float(q.b)))

    def onFirstConnect(self, pv):
        self.events.append("first")
        self.first.set()

    def onLastDisconnect(self, pv):
        self.events.append("last")
        self.last.set()


@pytest.fixture(scope="module")
def served():
    rec = Recorder()
    scalar = SharedPV(handler=rec, nt=NTScalar("d", display=True), initial=1.5)
    plain = SharedPV(nt=NTScalar("i"), initial=3)
    enum = SharedPV(nt=NTEnum(), initial={"index": 0, "choices": ["A", "B"]})
    table = SharedPV(nt=NTTable([("name", "s"), ("x", "d")]), initial=[{"name": "n", "x": 2.0}])
    image = np.arange(200 * 300, dtype="f4").reshape(200, 300)
    ndarray = SharedPV(nt=NTNDArray(), initial=image)
    closed = SharedPV(nt=NTScalar("s"))
    provider = StaticProvider("srv")
    for short, pv in [("scalar", scalar), ("plain", plain), ("enum", enum), ("table", table),
                      ("ndarray", ndarray), ("closed", closed)]:
        provider.add(P + short, pv)
    with Server(providers=[provider], isolate=True) as server:
        yield server, rec, {"scalar": scalar, "plain": plain, "enum": enum, "table": table,
                            "ndarray": ndarray, "closed": closed, "image": image}


@pytest.fixture(scope="module")
def client(served):
    server, _, _ = served
    with Context("pva", conf=server.conf(), useenv=False) as C:
        yield C


def test_conf_has_random_ports(served):
    server, _, _ = served
    conf = server.conf()
    assert conf["EPICS_PVA_SERVER_PORT"] not in ("5075", "0")
    assert conf["EPICS_PVA_ADDR_LIST"].startswith("127.0.0.1:")
    assert conf["EPICS_PVA_ADDR_LIST"] != "127.0.0.1:5076"
    assert conf["EPICS_PVA_NAME_SERVERS"] == "127.0.0.1:" + conf["EPICS_PVA_SERVER_PORT"]
    assert server.running


def test_get(client, served):
    v = client.get(P + "scalar")
    assert v == 1.5
    assert v.raw.getID() == "epics:nt/NTScalar:1.0"
    assert client.get(P + "plain") == 3
    e = client.get(P + "enum")
    assert (e.raw.value.index, list(e.raw.value.choices)) == (0, ["A", "B"])
    t = client.get(P + "table")  # p4p's client hands NTTable back raw
    assert list(t.labels) == ["name", "x"]
    assert list(t.value.name) == ["n"] and list(t.value.x) == [2.0]
    a = client.get(P + "ndarray")
    np.testing.assert_array_equal(a, served[2]["image"])


def test_put_through_handler(client, served):
    _, rec, pvs = served
    client.put(P + "scalar", 2.5)
    assert client.get(P + "scalar") == 2.5
    assert pvs["scalar"].current() == 2.5
    v, changed, peer, account = rec.puts[-1]
    assert v == 2.5
    assert changed == {"value"}
    assert peer.startswith("127.0.0.1:")
    assert isinstance(account, str)


def test_put_error_from_handler(client, served):
    with pytest.raises(RemoteError, match="too big"):
        client.put(P + "scalar", 500.0)
    assert client.get(P + "scalar") == 2.5


def test_put_without_handler_is_refused(client):
    with pytest.raises(RemoteError, match="Put not supported"):
        client.put(P + "plain", 4)
    assert client.get(P + "plain") == 3


def test_put_handler_sees_merged_value(client, served):
    _, rec, _ = served
    client.put(P + "scalar", {"value": 4.0, "display.description": "hi"})
    v, changed, _, _ = rec.puts[-1]
    assert changed == {"value", "display.description"}
    assert v.raw.display.description == "hi"
    assert v.raw.display.units == ""  # unmarked fields hold the server's current value
    assert client.get(P + "scalar").raw.display.description == "hi"


def test_rpc(client):
    arg = NTURI([("a", "d"), ("b", "d")]).wrap(P + "scalar", kws={"a": 2.0, "b": 4.0})
    assert client.rpc(P + "scalar", arg) == 8.0
    with pytest.raises(RemoteError, match="RPC not supported"):
        client.rpc(P + "plain", arg)


def test_monitor_carries_marks(client, served):
    _, _, pvs = served
    got = []
    with client.monitor(P + "scalar", lambda v: got.append((float(v), set(v.raw.changedSet())))):
        assert _wait(lambda: len(got) >= 1)
        assert "value" in got[0][1] and "display.units" in got[0][1]  # the initial is complete
        pvs["scalar"].post(7.0)
        assert _wait(lambda: len(got) >= 2)
        assert got[1] == (7.0, {"value"})
        pvs["scalar"].post(8.0, timestamp=1700000000.5)
        assert _wait(lambda: len(got) >= 3)
        assert got[2][0] == 8.0
        assert got[2][1] == {"value", "timeStamp.secondsPastEpoch", "timeStamp.nanoseconds"}
        V = pvs["scalar"].current().raw
        V.unmark()
        V["display.description"] = "only this"
        pvs["scalar"].post(V)
        assert _wait(lambda: len(got) >= 4)
        assert got[3] == (8.0, {"display.description"})


def test_connect_hooks():
    rec = Recorder()
    pv = SharedPV(handler=rec, nt=NTScalar("i"), initial=0)
    with Server(providers=[{P + "hooks": pv}], isolate=True) as S:
        assert rec.events == []
        with Context("pva", conf=S.conf(), useenv=False) as C:
            C.get(P + "hooks")
            assert rec.first.wait(5.0)
            assert not rec.last.is_set()
        assert rec.last.wait(5.0)
    assert rec.events == ["first", "last"]


def test_slow_client_is_squashed_not_buffered(client, served):
    _, _, pvs = served
    got = []
    gate = threading.Event()

    def cb(v):
        gate.wait(10.0)
        got.append(int(v))

    with client.monitor(P + "plain", cb, request="record[queueSize=3]"):
        time.sleep(0.2)
        t0 = time.monotonic()
        for i in range(500):
            pvs["plain"].post(1000 + i)
        posted_in = time.monotonic() - t0
        time.sleep(0.2)
        gate.set()
        assert _wait(lambda: got and got[-1] == 1499)
    # the poster never blocked on the slow client, and the client saw only
    # what its queue (plus the frame in flight) could hold
    assert posted_in < 1.0
    assert len(got) < 20
    pvs["plain"].post(3)


def test_closed_pv_waits_then_serves(client, served):
    _, _, pvs = served
    result = {}

    def get():
        result["v"] = client.get(P + "closed", timeout=5.0)

    t = threading.Thread(target=get)
    t.start()
    time.sleep(0.3)
    assert "v" not in result
    pvs["closed"].open("now")
    t.join(5.0)
    assert result["v"] == "now"
    assert pvs["closed"].isOpen()
    pvs["closed"].close()
    assert not pvs["closed"].isOpen()


def test_close_disconnects_client(served):
    server, _, pvs = served
    pv = SharedPV(nt=NTScalar("i"), initial=1)
    prov = StaticProvider("tmp")
    prov.add(P + "tmp", pv)
    with Server(providers=[prov], isolate=True) as S2, Context("pva", conf=S2.conf(), useenv=False) as C:
        got = []
        with C.monitor(P + "tmp", lambda v: got.append(v), notify_disconnect=True):
            assert _wait(lambda: len(got) >= 2)  # Disconnected(), then 1
            pv.close()
            assert _wait(lambda: len(got) >= 3 and isinstance(got[-1], Exception))


def test_dict_provider_and_stop():
    pv = SharedPV(nt=NTScalar("i"), initial=5)
    S = Server(providers=[{P + "d": pv}], isolate=True)
    try:
        with Context("pva", conf=S.conf(), useenv=False) as C:
            assert C.get(P + "d") == 5
    finally:
        S.stop()
    assert not S.running
    S.stop()  # idempotent


def test_decorators_and_current():
    pv = SharedPV(nt=NTScalar("i"), initial=1)
    seen = []

    @pv.put
    def onput(pv, op):
        seen.append(op.value())
        pv.post(op.value() + 1)
        op.done()

    with Server(providers=[{P + "dec": pv}], isolate=True) as S, \
            Context("pva", conf=S.conf(), useenv=False) as C:
        C.put(P + "dec", 10)
        assert C.get(P + "dec") == 11
    assert seen == [10]
    assert pv.current() == 11
    assert repr(pv).startswith("SharedPV(value=")


def test_post_type_mismatch_is_rejected():
    pv = SharedPV(nt=NTScalar("i"), initial=1)
    with pytest.raises(Exception, match="type differs"):
        pv.post(NTScalar("d").wrap(2.0))
    with pytest.raises(Exception, match="not open"):
        SharedPV(nt=NTScalar("i")).post(1)


def test_current_keeps_the_marks_and_an_unmarked_post_is_a_noop():
    nt = NTScalar("d", display=True)
    pv = SharedPV(nt=nt, initial=1.0)
    opened = set(pv.current().raw.changedSet())
    assert "value" in opened
    V = pv.current().raw
    V.unmark()
    V["display.description"] = "d"
    pv.post(V)
    assert set(pv.current().raw.changedSet()) == opened | {"display.description"}
    assert pv.current().raw["display.description"] == "d"
    U = pv.current().raw
    U["value"] = 99.0
    U.unmark()
    got = []
    with Server(providers=[{P + "marks": pv}], isolate=True) as S, \
            Context("pva", conf=S.conf(), useenv=False) as C, \
            C.monitor(P + "marks", lambda v: got.append(float(v))):
        assert _wait(lambda: len(got) >= 1)
        pv.post(U)
        pv.post(2.0)
        assert _wait(lambda: len(got) >= 2)
    assert got == [1.0, 2.0]
    assert pv.current() == 2.0
    posted = set(nt.wrap(2.0).changedSet())
    assert set(pv.current().raw.changedSet()) == opened | {"display.description"} | posted
    pv.close()
    pv.open(V)
    assert set(pv.current().raw.changedSet()) == {"display.description"}
