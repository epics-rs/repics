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
    assert ca.caput(ioc + "ao", 2.5, wait=True).ok
    assert ca.caget(ioc + "ao") == 2.5
    ca.caput(ioc + "ao", "3.25", wait=True)
    assert ca.caget(ioc + "ao") == 3.25
    ca.caput(ioc + "mbbo", "Two", wait=True)
    assert ca.caget(ioc + "mbbo") == 2
    ca.caput(ioc + "mbbo", 0, wait=True)
    assert ca.caget(ioc + "mbbo") == 0


def test_caput_array_roundtrip(ioc):
    data = numpy.arange(8, dtype=numpy.float64) * 0.5
    ca.caput(ioc + "wf", data, wait=True)
    v = ca.caget(ioc + "wf")
    assert isinstance(v, epicsrs.AugmentedArray)
    assert v.dtype == numpy.float64
    numpy.testing.assert_array_equal(v, data)
    assert v.element_count == 8
    assert v[2:4].name == ioc + "wf"


def test_cainfo(ioc):
    info = ca.cainfo(ioc + "wf")
    assert info.datatype == epicsrs.DBR_DOUBLE
    assert info.datatype_name == "double"
    assert info.count == 8
    assert info.read and info.write
    assert info.host.startswith("127.0.0.1:")
    assert info.state == 2 and str(info).startswith(ioc + "wf:\n    State: connected")


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
        await aio.caput(ioc + "ao", 1.25, wait=True)
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

        sub = aio.camonitor(ioc + "cnt", cb)
        try:
            await asyncio.wait_for(done.wait(), 3.0)
        finally:
            sub.close()
        assert got[1] > got[0] and got[2] > got[1]

    asyncio.run(main())


# ---------------------------------------------------------------------------
# boundaries: throw=False, datatype, count, connect_timeout, collapse, events


def test_throw_false_returns_ca_nothing(ioc):
    v = ca.caget(ioc + "no-such-pv", timeout=0.3, throw=False)
    assert isinstance(v, epicsrs.CaNothing)
    assert not v.ok and not v
    assert v.errorcode == epicsrs.ECA_TIMEOUT
    assert v.name == ioc + "no-such-pv"
    assert repr(v) == f"CaNothing({ioc + 'no-such-pv'!r}, {epicsrs.ECA_TIMEOUT})"
    assert str(v).endswith(epicsrs.ca_message(epicsrs.ECA_TIMEOUT))
    with pytest.raises(TypeError):
        iter(v)


def test_list_with_missing_pv_keeps_shape(ioc):
    vs = ca.caget([ioc + "ai", ioc + "missing", ioc + "long"], timeout=0.3, throw=False)
    assert vs[0] == 1.5 and vs[2] == 42
    assert isinstance(vs[1], epicsrs.CaNothing) and vs[1].errorcode == epicsrs.ECA_TIMEOUT
    with pytest.raises(epicsrs.CaTimeout):
        ca.caget([ioc + "ai", ioc + "missing"], timeout=0.3)


def test_caput_returns_ca_nothing_and_lists(ioc):
    r = ca.caput(ioc + "ao", 1.0, wait=True)
    assert isinstance(r, epicsrs.CaNothing) and r.ok and r
    rs = ca.caput([ioc + "ao", ioc + "long"], [2.0, 5], wait=True)
    assert all(x.ok for x in rs)
    assert ca.caget(ioc + "ao") == 2.0 and ca.caget(ioc + "long") == 5
    rs = ca.caput([ioc + "ao", ioc + "long"], 3, wait=True)
    assert ca.caget(ioc + "ao") == 3.0 and ca.caget(ioc + "long") == 3
    with pytest.raises(ValueError):
        ca.caput([ioc + "ao", ioc + "long"], [1.0])
    ca.caput(ioc + "long", 42, wait=True)


def test_caput_read_only_field_fails(ioc):
    with pytest.raises(epicsrs.CaError) as e:
        ca.caput(ioc + "ro", 1, wait=True, timeout=2.0)
    assert e.value.status != epicsrs.ECA_NORMAL
    r = ca.caput(ioc + "ro", 1, wait=True, timeout=2.0, throw=False)
    assert not r.ok
    assert ca.caget(ioc + "ro") == 7


def test_datatype_conversions(ioc):
    s = ca.caget(ioc + "ai", datatype=str)
    assert isinstance(s, epicsrs.AugmentedStr) and float(s) == 1.5
    assert s.dbr == epicsrs.DBR_STRING
    i = ca.caget(ioc + "ai", datatype=int)
    assert isinstance(i, epicsrs.AugmentedInt) and i == 1 and i.dbr == epicsrs.DBR_LONG
    f = ca.caget(ioc + "long", datatype=float)
    assert isinstance(f, epicsrs.AugmentedFloat) and f == 42.0
    e = ca.caget(ioc + "mbbo", datatype=epicsrs.DBR_ENUM_STR)
    assert isinstance(e, epicsrs.AugmentedStr) and e in ("Zero", "One", "Two")
    d = ca.caget(ioc + "ai", datatype=numpy.float32)
    assert d.dbr == epicsrs.DBR_FLOAT
    with pytest.raises(TypeError):
        ca.caget(ioc + "ai", datatype=complex)
    with pytest.raises(ValueError):
        ca.caget(ioc + "ai", form="bogus")


def test_char_string_roundtrip(ioc):
    assert ca.caput(ioc + "chars", "héllo", datatype=epicsrs.DBR_CHAR_STR, wait=True).ok
    v = ca.caget(ioc + "chars", datatype=epicsrs.DBR_CHAR_STR)
    assert isinstance(v, epicsrs.AugmentedStr) and v == "héllo"
    b = ca.caget(ioc + "chars", datatype=epicsrs.DBR_CHAR_BYTES)
    assert isinstance(b, epicsrs.AugmentedBytes) and b == "héllo".encode()
    raw = ca.caget(ioc + "chars")
    assert isinstance(raw, epicsrs.AugmentedArray) and raw.dtype == numpy.uint8


def test_count_conventions(ioc):
    ca.caput(ioc + "wf", numpy.arange(5.0), wait=True)
    assert len(ca.caget(ioc + "wf")) == 5  # autosize: the record's current count
    assert len(ca.caget(ioc + "wf", count=-1)) == 8  # full native count
    assert len(ca.caget(ioc + "wf", count=3)) == 3
    assert len(ca.caget(ioc + "wf", count=100)) == 8  # clamped, not ECA_BADCOUNT
    assert ca.caget(ioc + "wf").element_count == 5
    assert ca.caget(ioc + "si") == "input"


def test_cainfo_states_and_lists(ioc):
    infos = ca.cainfo([ioc + "ai", ioc + "missing"], timeout=0.3, throw=False)
    assert infos[0].state == 2 and infos[0].datatype_name == "double"
    assert isinstance(infos[1], epicsrs.CaNothing)
    info = ca.cainfo(ioc + "missing", wait=False)
    assert info.state == 0 and info.host == "<disconnected>" and info.datatype == epicsrs.DBR_NO_ACCESS
    assert "never connected" in str(info)
    names = {s.name for s in epicsrs.get_channel_infos()}
    assert ioc + "ai" in names and ioc + "missing" in names


def test_camonitor_connect_timeout_then_keeps_waiting(ioc):
    got = []
    ev = threading.Event()

    def cb(v):
        got.append(v)
        ev.set()

    with ca.camonitor(ioc + "never", cb, connect_timeout=0.3) as sub:
        assert ev.wait(2.0)
        assert isinstance(got[0], epicsrs.CaNothing) and got[0].errorcode == epicsrs.ECA_TIMEOUT
        assert sub.state == sub.OPENING
    assert sub.state == sub.CLOSED


def test_camonitor_collapses_unless_all_updates(ioc):
    seen = []
    slow = threading.Event()

    def cb(v):
        seen.append(int(v))
        time.sleep(0.25)
        slow.set()

    sub = ca.camonitor(ioc + "cnt", cb)
    try:
        time.sleep(1.5)
    finally:
        sub.close()
    assert sub.dropped_callbacks > 0
    assert seen == sorted(seen)

    every = []
    done = threading.Event()

    def cb2(v):
        every.append(int(v))
        time.sleep(0.25)
        if len(every) >= 3:
            done.set()

    with ca.camonitor(ioc + "cnt", cb2, all_updates=True) as sub2:
        assert done.wait(3.0)
        assert sub2.dropped_callbacks == 0
    assert every[1] == every[0] + 1 and every[2] == every[1] + 1


def test_camonitor_list_index_and_mask(ioc):
    got = {}
    ev = threading.Event()

    def cb(v, i):
        got[i] = float(v)
        if len(got) == 2:
            ev.set()

    subs = ca.camonitor([ioc + "ai", ioc + "ao"], cb, form="ctrl", mask=epicsrs.DBE_VALUE)
    try:
        assert ev.wait(2.0)
    finally:
        for s in subs:
            s.close()
    assert got[0] == 1.5 and 1 in got


def test_camonitor_callback_exception_closes(ioc, capfd):
    closed = threading.Event()

    def cb(v):
        raise RuntimeError("boom")

    sub = ca.camonitor(ioc + "cnt", cb)
    for _ in range(40):
        if sub.state == sub.CLOSED:
            closed.set()
            break
        time.sleep(0.05)
    assert closed.is_set()
    err = capfd.readouterr().err
    assert "boom" in err and "subscription closed" in err
    assert not any(s.subscriber_count for s in epicsrs.get_channel_infos() if s.name == ioc + "cnt")


def test_camonitor_datatype_str_and_enum(ioc):
    got = []
    ev = threading.Event()

    def cb(v):
        got.append(v)
        ev.set()

    with ca.camonitor(ioc + "mbbo", cb, datatype=epicsrs.DBR_ENUM_STR):
        assert ev.wait(2.0)
    assert isinstance(got[0], epicsrs.AugmentedStr) and got[0] in ("Zero", "One", "Two")
    with pytest.raises(TypeError):
        ca.camonitor(ioc + "ai", cb, datatype=int)


def test_subscription_pause_resume(ioc):
    got = []
    sub = ca.camonitor(ioc + "cnt", lambda v: got.append(int(v)), all_updates=True)
    try:
        time.sleep(0.5)
        sub.pause()
        time.sleep(0.1)
        n = len(got)
        time.sleep(0.5)
        assert len(got) <= n + 1
        sub.resume()
        time.sleep(0.5)
        assert len(got) > n + 1
    finally:
        sub.close()


def test_connection_events_on_a_fresh_channel(ioc):
    ch = epicsrs.context().channel(ioc + "ai")
    events = ch.events()
    try:
        ev = events.recv(timeout=2.0)
        assert ev is not None and ev.kind == "connected"
        assert ch.connected and ch.dbr == epicsrs.DBR_DOUBLE and ch.element_count == 1
        ev = events.recv(timeout=2.0)
        assert ev.kind == "access_rights" and ev.read and ev.write
        with pytest.raises(epicsrs.CaTimeout):
            events.recv(timeout=0.2)
    finally:
        events.close()
    assert events.recv() is None


def test_snapshot_pull_api(ioc):
    ch = epicsrs.context().channel(ioc + "cnt")
    ch.wait_connected(2.0)
    with ch.subscribe() as sub:
        a = sub.recv(timeout=2.0)
        b = sub.recv(timeout=2.0)
        assert isinstance(a, epicsrs.Snapshot) and a.name == ioc + "cnt"
        assert b.value > a.value
        batch = sub.recv_batch(timeout=2.0)
        assert batch and all(isinstance(s, epicsrs.Snapshot) for s in batch)
    assert sub.recv() is None


def test_augmented_metadata_is_lazy(ioc):
    v = ca.caget(ioc + "ai", form="ctrl")
    assert isinstance(v.snapshot, epicsrs.Snapshot)
    assert v.snapshot.dbr == epicsrs.DBR_DOUBLE
    assert v.units == "mm" and v.timestamp == 0.0
    assert ca.caget(ioc + "ai").datetime.year >= 2020
    w = ca.caget(ioc + "wf")
    assert w[1:].snapshot is w.snapshot


# ---------------------------------------------------------------------------
# asyncio


def test_aio_lists_and_throw(ioc):
    async def main():
        rs = await aio.caput([ioc + "ao", ioc + "long"], [4.5, 6], wait=True)
        assert all(r.ok for r in rs)
        vs = await aio.caget([ioc + "ao", ioc + "long", ioc + "missing"], timeout=0.3, throw=False)
        assert vs[0] == 4.5 and vs[1] == 6
        assert isinstance(vs[2], epicsrs.CaNothing)
        infos = await aio.cainfo([ioc + "ao", ioc + "missing"], timeout=0.3, throw=False)
        assert infos[0].state == 2 and isinstance(infos[1], epicsrs.CaNothing)
        c = await aio.connect(ioc + "ao")
        assert c.ok
        with pytest.raises(epicsrs.CaTimeout):
            await aio.caget(ioc + "missing", timeout=0.3)
        await aio.caput(ioc + "long", 42, wait=True)

    asyncio.run(main())


def test_aio_camonitor_connect_timeout_and_exception(ioc, capfd):
    async def main():
        got = []
        ev = asyncio.Event()

        async def cb(v):
            got.append(v)
            ev.set()

        sub = aio.camonitor(ioc + "never", cb, connect_timeout=0.3)
        await asyncio.wait_for(ev.wait(), 2.0)
        sub.close()
        assert isinstance(got[0], epicsrs.CaNothing) and got[0].errorcode == epicsrs.ECA_TIMEOUT

        def bad(v):
            raise RuntimeError("async boom")

        sub2 = aio.camonitor(ioc + "cnt", bad)
        for _ in range(40):
            if sub2.state == sub2.CLOSED:
                break
            await asyncio.sleep(0.05)
        assert sub2.state == sub2.CLOSED

    asyncio.run(main())
    assert "async boom" in capfd.readouterr().err


def test_aio_camonitor_collapses(ioc):
    async def main():
        seen = []

        async def cb(v):
            seen.append(int(v))
            await asyncio.sleep(0.25)

        sub = aio.camonitor(ioc + "cnt", cb)
        await asyncio.sleep(1.5)
        sub.close()
        assert sub.dropped_callbacks > 0 and seen == sorted(seen)

    asyncio.run(main())


# ---------------------------------------------------------------------------
# the pyepics-shaped PV


def test_pv_class(ioc):
    from epicsrs.pv import PV, get_pv

    calls = []
    conns = []
    pv = PV(ioc + "ao", callback=lambda **kw: calls.append(kw), connection_callback=lambda **kw: conns.append(kw["conn"]))
    assert pv.wait_for_connection(2.0)
    for _ in range(40):
        if calls:
            break
        time.sleep(0.05)
    assert calls and calls[0]["pvname"] == ioc + "ao" and "timestamp" in calls[0]
    assert conns == [True]
    pv.put(2.75, wait=True)
    for _ in range(40):
        if pv.get() == 2.75:
            break
        time.sleep(0.05)
    assert pv.value == 2.75 and pv.char_value == "2.75"
    assert pv.units == "V" and pv.precision == 2 and pv.upper_ctrl_limit == 5
    assert pv.type == "double" and pv.count == 1 and pv.access == "read/write"
    assert pv.get_timevars()["severity"] == 0
    done = threading.Event()
    pv.put(1.25, callback=lambda pvname, data: done.set(), callback_data="x")
    assert done.wait(2.0) and pv.put_complete
    idx = pv.add_callback(lambda **kw: None)
    pv.remove_callback(idx)
    pv.disconnect()

    e = PV(ioc + "mbbo", auto_monitor=False)
    assert e.get(as_string=True) in ("Zero", "One", "Two") and e.enum_strs == ["Zero", "One", "Two"]
    assert get_pv(ioc + "ai") is get_pv(ioc + "ai")
    assert get_pv(ioc + "ai").get() == 1.5
    e.disconnect()
