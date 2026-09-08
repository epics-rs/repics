import asyncio
import threading
import time

import numpy
import pytest

import epicsrs
from epicsrs import aio, ca


def test_caget_double_time_form(ioc):
    v = ca.caget(ioc + "ai")
    assert isinstance(v, epicsrs.AugmentedFloat)
    assert v == 1.5
    assert v.name == ioc + "ai"
    assert v.ok
    assert v.datatype == "double"
    assert v.element_count == 1
    assert v.severity == 0
    assert v.status == 0
    assert v.raw_stamp[0] > 1_600_000_000
    assert abs(v.timestamp - time.time()) < 60
    assert v.units is None


def test_caget_ctrl_form(ioc):
    v = ca.caget(ioc + "ai", form="ctrl")
    assert v == 1.5
    assert v.units == "mm"
    assert v.precision == 3
    assert v.upper_disp_limit == 10
    assert v.lower_disp_limit == -10
    assert v.upper_alarm_limit == 9
    assert v.lower_alarm_limit == -9


def test_caget_scalar_types(ioc):
    assert ca.caget(ioc + "long") == 42
    assert isinstance(ca.caget(ioc + "long"), epicsrs.AugmentedInt)
    s = ca.caget(ioc + "str")
    assert s == "hello"
    assert isinstance(s, epicsrs.AugmentedStr)
    e = ca.caget(ioc + "mbbo", form="ctrl")
    assert e == 1
    assert e.datatype == "enum"
    assert e.enums == ["Zero", "One", "Two"]


def test_caget_list_keeps_shape(ioc):
    vs = ca.caget([ioc + "ai", ioc + "long"])
    assert [float(vs[0]), int(vs[1])] == [1.5, 42]
    assert [v.name for v in vs] == [ioc + "ai", ioc + "long"]


def test_caput_scalar_roundtrip(ioc):
    ca.caput(ioc + "ao", 2.5)
    assert ca.caget(ioc + "ao") == 2.5
    ca.caput(ioc + "ao", "3.25")
    assert ca.caget(ioc + "ao") == 3.25
    ca.caput(ioc + "mbbo", "Two")
    assert ca.caget(ioc + "mbbo") == 2
    ca.caput(ioc + "mbbo", 0)
    assert ca.caget(ioc + "mbbo") == 0


def test_caput_array_roundtrip(ioc):
    data = numpy.arange(8, dtype=numpy.float64) * 0.5
    ca.caput(ioc + "wf", data)
    v = ca.caget(ioc + "wf")
    assert isinstance(v, epicsrs.AugmentedArray)
    assert v.dtype == numpy.float64
    numpy.testing.assert_array_equal(v, data)
    assert v.element_count == 8
    assert v[2:4].name == ioc + "wf"


def test_cainfo(ioc):
    info = ca.cainfo(ioc + "wf")
    assert info.datatype == "DOUBLE"
    assert info.element_count == 8
    assert info.read_access and info.write_access
    assert info.host.startswith("127.0.0.1:")


def test_camonitor_delivers_updates(ioc):
    got = []
    done = threading.Event()

    def cb(v):
        got.append(float(v))
        if len(got) >= 3:
            done.set()

    with ca.camonitor(ioc + "cnt", cb):
        assert done.wait(3.0), got
    assert got[1] > got[0] and got[2] > got[1]


def test_missing_pv_times_out(ioc):
    t0 = time.monotonic()
    with pytest.raises(epicsrs.CaTimeout):
        ca.caget(ioc + "no-such-pv", timeout=0.5)
    assert time.monotonic() - t0 < 2.0


def test_aio_caget_caput(ioc):
    async def main():
        await aio.caput(ioc + "ao", 1.25)
        v = await aio.caget(ioc + "ao", form="ctrl")
        assert v == 1.25
        assert v.units == "V"
        vs = await aio.caget([ioc + "ai", ioc + "str"])
        assert vs[0] == 1.5 and vs[1] == "hello"

    asyncio.run(main())


def test_aio_camonitor_async_callback(ioc):
    async def main():
        got = []
        done = asyncio.Event()

        async def cb(v):
            got.append(float(v))
            if len(got) >= 3:
                done.set()

        sub = await aio.camonitor(ioc + "cnt", cb)
        try:
            await asyncio.wait_for(done.wait(), 3.0)
        finally:
            sub.close()
        assert got[1] > got[0] and got[2] > got[1]

    asyncio.run(main())
