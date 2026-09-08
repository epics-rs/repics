"""A p4p server on loopback for the client tests.

Everything binds ephemeral ports (``isolate=True``): 5075/5076 are never
touched, so the suite can run next to a live IOC. p4p is the reference
implementation on both sides — a p4p server checks our client, a p4p
client checks our server.
"""

from __future__ import annotations

import numpy as np
import pytest

p4p = pytest.importorskip("p4p")

from p4p.nt import NTEnum, NTNDArray, NTScalar, NTTable  # noqa: E402
from p4p.server import Server, StaticProvider  # noqa: E402
from p4p.server.thread import SharedPV  # noqa: E402

PREFIX = "epicsrs-pva-test:"
IMAGE_SHAPE = (1000, 1000)  # 1e6 elements: the zero-copy proof needs a big one


class _Handler:
    """Put handler that stores, or refuses values over 100."""

    def put(self, pv, op):
        v = op.value()
        val = v.raw["value"]
        if isinstance(val, (int, float)) and val > 100:
            op.done(error="too big")
            return
        pv.post(v)
        op.done()

    def rpc(self, pv, op):
        q = op.value().query
        op.done(NTScalar("d").wrap(float(q.a) + float(q.b)))


@pytest.fixture(scope="session")
def p4p_pvs():
    nt = NTScalar("d", display=True, control=True, valueAlarm=True)
    scalar = SharedPV(handler=_Handler(), nt=nt, initial=nt.wrap({
        "value": 1.5,
        "alarm": {"severity": 1, "status": 3, "message": "HIGH"},
        "timeStamp": {"secondsPastEpoch": 1700000000, "nanoseconds": 42},
        "display": {"limitLow": -10.0, "limitHigh": 10.0, "units": "mm", "description": "test"},
        "control": {"limitLow": -5.0, "limitHigh": 5.0},
        "valueAlarm": {"highWarningLimit": 4.0, "highAlarmLimit": 8.0},
    }))
    integer = SharedPV(handler=_Handler(), nt=NTScalar("i"), initial=7)
    string = SharedPV(nt=NTScalar("s"), initial="hello")
    array = SharedPV(nt=NTScalar("ad"), initial=np.arange(5, dtype="f8"))
    enum = SharedPV(handler=_Handler(), nt=NTEnum(), initial={"index": 1, "choices": ["Off", "On", "Auto"]})
    table = SharedPV(nt=NTTable([("name", "s"), ("x", "d"), ("n", "i")]), initial=[
        {"name": "a", "x": 1.0, "n": 1},
        {"name": "b", "x": 2.5, "n": 2},
    ])
    image = np.arange(IMAGE_SHAPE[0] * IMAGE_SHAPE[1], dtype="u2").reshape(IMAGE_SHAPE)
    ndarray = SharedPV(nt=NTNDArray(), initial=image)
    pvs = {
        "scalar": scalar,
        "integer": integer,
        "string": string,
        "array": array,
        "enum": enum,
        "table": table,
        "ndarray": ndarray,
    }
    provider = StaticProvider("epicsrs-test")
    for name, pv in pvs.items():
        provider.add(PREFIX + name, pv)
    with Server(providers=[provider], isolate=True) as server:
        yield server, pvs, image


@pytest.fixture(scope="session")
def p4p_conf(p4p_pvs):
    server, _, _ = p4p_pvs
    return server.conf()


def name(short: str) -> str:
    return PREFIX + short


@pytest.fixture(scope="session")
def pvname():
    """Full PV name from a short one (``pvname("scalar")``)."""
    return name
