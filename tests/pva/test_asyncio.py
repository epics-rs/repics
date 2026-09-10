"""asyncio flavours: epicsrs.pva.asyncio.Context and epicsrs.pva.server.asyncio.SharedPV."""

from __future__ import annotations

import asyncio
import time

import numpy as np
import pytest
from p4p.client.thread import Context as P4PContext
from p4p.client.thread import RemoteError as P4PRemoteError
from p4p.nt import NTURI as P4PNTURI

from epicsrs.pva import Disconnected, PvaError, PvaRemoteError, PvaTimeout, Value
from epicsrs.pva.asyncio import Context
from epicsrs.pva.nt import NTScalar, NTURI
from epicsrs.pva.server import Server, StaticProvider
from epicsrs.pva.server.asyncio import SharedPV

P = "epicsrs-aio-test:"


async def _until(pred, timeout=5.0):
    for _ in range(int(timeout / 0.01)):
        if pred():
            return True
        await asyncio.sleep(0.01)
    return False


# --- client against the p4p server -------------------------------------------


def test_client_get_put_rpc(p4p_pvs, p4p_conf, pvname):
    async def main():
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            v = await C.get(pvname("scalar"))
            assert v == 1.5 and v.raw.display.units == "mm"
            a, s = await C.get([pvname("array"), pvname("string")])
            np.testing.assert_array_equal(a, np.arange(5))
            assert s == "hello"
            await C.put(pvname("integer"), 12)
            assert await C.get(pvname("integer")) == 12
            assert await C.put([pvname("integer")], [13]) == [None]
            assert p4p_pvs[1]["integer"].current() == 13
            with pytest.raises(PvaRemoteError, match="too big"):
                await C.put(pvname("integer"), 500)
            e = await C.put(pvname("integer"), 500, throw=False)
            assert isinstance(e, PvaRemoteError)
            await C.put(pvname("integer"), 14, get=False)
            assert p4p_pvs[1]["integer"].current() == 14
            arg = NTURI([("a", "d"), ("b", "d")]).wrap(pvname("scalar"), kws={"a": 1.5, "b": 2.0})
            assert await C.rpc(pvname("scalar"), arg) == 3.5
            assert (await C.connect(pvname("scalar"))).startswith("127.0.0.1:")
            assert "value" in (await C.info(pvname("scalar"))).keys()
            with pytest.raises(PvaTimeout):
                await C.get(pvname("nonexistent"), timeout=0.3)
            assert isinstance(await C.get(pvname("nonexistent"), timeout=0.3, throw=False), PvaTimeout)
            await C.put(pvname("integer"), 7)  # leave the shared PV as the fixture made it

    asyncio.run(main())


def test_client_monitor_with_coroutine_callback(p4p_pvs, p4p_conf, pvname):
    async def main():
        got = []
        seen = asyncio.Event()

        async def cb(v):
            await asyncio.sleep(0)  # a coroutine callback is awaited on the loop
            got.append(v)
            seen.set()

        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            with C.monitor(pvname("integer"), cb, notify_disconnect=True):
                await asyncio.wait_for(_until(lambda: len(got) >= 2), 5.0)
                assert isinstance(got[0], Disconnected)
                assert got[1] == 7
                p4p_pvs[1]["integer"].post(21)
                assert await _until(lambda: got[-1] == 21)
                assert set(got[-1].raw.changedSet()) == {"value"}
            await asyncio.sleep(0.05)
            n = len(got)
            p4p_pvs[1]["integer"].post(22)
            await asyncio.sleep(0.2)
            assert len(got) == n  # closed subscriptions deliver nothing more
            p4p_pvs[1]["integer"].post(7)

    asyncio.run(main())


def test_client_slow_coroutine_backpressures(p4p_pvs, p4p_conf, pvname):
    async def main():
        got = []
        entered = asyncio.Event()
        gate = asyncio.Event()

        async def cb(v):
            entered.set()
            await gate.wait()
            got.append(int(v))

        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            with C.monitor(pvname("integer"), cb, limit=2):
                # Post only once the monitor is running (see test_client).
                await asyncio.wait_for(entered.wait(), 5.0)
                for i in range(50):
                    p4p_pvs[1]["integer"].post(100 + i)
                await asyncio.sleep(0.2)
                gate.set()
                assert await _until(lambda: got and got[-1] == 149)
                assert len(got) < 20
        p4p_pvs[1]["integer"].post(7)

    asyncio.run(main())



def test_client_get_scalar_metadata(p4p_pvs, p4p_conf, pvname):
    async def main():
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            v = await C.get(pvname("scalar"))
            assert v == 1.5
            assert v.name == pvname("scalar")
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

    asyncio.run(main())


def test_client_get_raw_value_and_type(p4p_pvs, p4p_conf, pvname):
    async def main():
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            V = (await C.get(pvname("scalar"))).raw
            assert V["value"] == 1.5
            assert V.display.units == "mm"
            assert set(V.keys()) >= {"value", "alarm", "timeStamp", "display", "control", "valueAlarm"}
            assert "value" in V.changedSet()
            T = V.type()
            assert T.getID() == "epics:nt/NTScalar:1.0"
            assert T["value"] == "d"
            assert T["alarm"].getID() == "alarm_t"
            assert dict(V.todict()["alarm"]) == {"severity": 1, "status": 3, "message": "HIGH"}
            info = await C.info(pvname("scalar"))
            assert info.getID() == "epics:nt/NTScalar:1.0"
            assert list(info) == list(T)

    asyncio.run(main())


def test_client_get_array_and_list_names(p4p_pvs, p4p_conf, pvname):
    async def main():
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            a = await C.get(pvname("array"))
            assert isinstance(a, np.ndarray)
            np.testing.assert_array_equal(a, np.arange(5, dtype="f8"))
            assert a.element_count == 5
            got = await C.get([pvname("integer"), pvname("string")])
            assert got == [7, "hello"]
            assert [g.name for g in got] == [pvname("integer"), pvname("string")]

    asyncio.run(main())


def test_client_put_dict_is_a_marked_delta(p4p_pvs, p4p_conf, pvname):
    async def main():
        _, pvs, _ = p4p_pvs
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            try:
                await C.put(pvname("scalar"), 2.5)
                assert await C.get(pvname("scalar")) == 2.5
                await C.put(pvname("scalar"), {"value": 3.0, "alarm": {"severity": 2}})
                v = await C.get(pvname("scalar"))
                assert (v, v.severity) == (3.0, 2)
                cur = pvs["scalar"].current()
                assert (cur.raw.value, cur.raw.alarm.severity) == (3.0, 2)
            finally:
                # The fixture handler re-stamps the PV from each put (a put
                # carries no timeStamp, so it lands as 0); put the shared PV
                # back exactly as the fixture made it for the tests after this.
                pvs["scalar"].post(
                    {"value": 1.5, "alarm": {"severity": 1, "status": 3, "message": "HIGH"}},
                    timestamp=(1700000000, 42),
                )
            v = await C.get(pvname("scalar"))
            assert (v, v.severity, v.status, v.raw_stamp) == (1.5, 1, 3, (1700000000, 42))

    asyncio.run(main())


def test_client_put_value_sends_marks_only(p4p_pvs, p4p_conf, pvname):
    async def main():
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            V = (await C.get(pvname("integer"))).raw
            V.unmark()
            V.value = 11
            assert V.changedSet() == {"value"}
            await C.put(pvname("integer"), V)
            assert await C.get(pvname("integer")) == 11
            await C.put(pvname("integer"), 7)

    asyncio.run(main())


def test_client_put_wait_process(p4p_pvs, p4p_conf, pvname):
    async def main():
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            await C.put(pvname("integer"), 8, wait=True)
            assert await C.get(pvname("integer")) == 8
            await C.put(pvname("integer"), 7, process=False, wait=False)
            assert await C.get(pvname("integer")) == 7
            with pytest.raises(ValueError):
                await C.put(pvname("integer"), 7, request="field()", wait=True)

    asyncio.run(main())


def test_client_argument_errors(p4p_pvs, p4p_conf, pvname):
    with pytest.raises(ValueError):
        Context("ca", conf=p4p_conf, useenv=False)

    async def main():
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            with pytest.raises(ValueError):
                await C.put([pvname("integer"), pvname("string")], [1])
            with pytest.raises(ValueError):
                await C.put([pvname("integer")], 1)

    asyncio.run(main())


def test_client_enum(p4p_pvs, p4p_conf, pvname):
    async def main():
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            v = await C.get(pvname("enum"))
            assert v == 1
            assert v.choice == "On"
            assert v.enums == ["Off", "On", "Auto"]
            await C.put(pvname("enum"), "Auto")
            assert (await C.get(pvname("enum"))).choice == "Auto"
            await C.put(pvname("enum"), 0)
            assert (await C.get(pvname("enum"))).choice == "Off"
            await C.put(pvname("enum"), 1)

    asyncio.run(main())


def test_client_table(p4p_pvs, p4p_conf, pvname):
    async def main():
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            rows = await C.get(pvname("table"))
            assert rows.labels == ["name", "x", "n"]
            assert [dict(r) for r in rows] == [
                {"name": "a", "x": 1.0, "n": 1},
                {"name": "b", "x": 2.5, "n": 2},
            ]
            assert isinstance(rows[1]["x"], float)

    asyncio.run(main())


def test_client_ndarray_zero_copy(p4p_pvs, p4p_conf, pvname):
    async def main():
        _, _, image = p4p_pvs
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            a = await C.get(pvname("ndarray"))
        assert a.shape == image.shape
        assert a.dtype == np.uint16
        np.testing.assert_array_equal(a, image)
        assert a.attrib["ColorMode"] == 0
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

    asyncio.run(main())


def test_client_monitor_pause_resume(p4p_pvs, p4p_conf, pvname):
    async def main():
        _, pvs, _ = p4p_pvs
        got = []
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            with C.monitor(pvname("integer"), got.append, notify_disconnect=True) as sub:
                assert sub.name == pvname("integer")
                assert await _until(lambda: len(got) >= 2)
                assert isinstance(got[0], Disconnected)
                assert got[1] == 7
                pvs["integer"].post(20)
                pvs["integer"].post(21)
                assert await _until(lambda: len(got) >= 4)
                assert [int(g) for g in got[2:4]] == [20, 21]
                sub.pause()
                await asyncio.sleep(0.2)  # STOP is in flight; let the server apply it
                n = len(got)
                pvs["integer"].post(30)
                await asyncio.sleep(0.2)
                assert len(got) == n
                sub.resume()
                assert await _until(lambda: len(got) > n)
                assert got[-1] == 30
        pvs["integer"].post(7)

    asyncio.run(main())


def test_client_monitor_callback_error_does_not_stop_the_drain(p4p_pvs, p4p_conf, pvname):
    async def main():
        _, pvs, _ = p4p_pvs
        got = []
        entered = asyncio.Event()

        async def cb(v):
            entered.set()
            if len(got) == 0:
                got.append(None)
                raise RuntimeError("first delivery fails")
            got.append(int(v))

        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            with C.monitor(pvname("integer"), cb):
                await asyncio.wait_for(entered.wait(), 5.0)
                pvs["integer"].post(23)
                assert await _until(lambda: got[-1] == 23)
        pvs["integer"].post(7)

    asyncio.run(main())


def test_client_monitor_needs_a_running_loop(p4p_pvs, p4p_conf, pvname):
    C = Context("pva", conf=p4p_conf, useenv=False)
    try:
        with pytest.raises(RuntimeError):
            C.monitor(pvname("integer"), lambda v: None)
    finally:
        C.close()


def test_client_missing_pv_timeout_is_bounded(p4p_pvs, p4p_conf, pvname):
    async def main():
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            t0 = time.monotonic()
            with pytest.raises(PvaTimeout):
                await C.get(pvname("nope"), timeout=0.3)
            assert time.monotonic() - t0 < 2.0
            got = await C.get([pvname("nope"), pvname("string")], timeout=0.3, throw=False)
            assert isinstance(got[0], PvaTimeout)
            assert got[1] == "hello"

    asyncio.run(main())


def test_client_sync_context_manager(p4p_pvs, p4p_conf, pvname):
    async def main():
        with Context("pva", conf=p4p_conf, useenv=False) as C:
            assert await C.get(pvname("string")) == "hello"

    asyncio.run(main())


def test_client_useenv_false_ignores_environment(monkeypatch, p4p_pvs, p4p_conf, pvname):
    monkeypatch.setenv("EPICS_PVA_ADDR_LIST", "192.0.2.1")
    monkeypatch.setenv("EPICS_PVA_AUTO_ADDR_LIST", "NO")

    async def main():
        async with Context("pva", conf=p4p_conf, useenv=False) as C:
            assert await C.get(pvname("string")) == "hello"

    asyncio.run(main())


# --- asyncio server against both clients --------------------------------------


def test_server_coroutine_handlers():
    async def main():
        pv = SharedPV(nt=NTScalar("d", display=True), initial=1.0)
        events = []

        @pv.put
        async def onput(pv, op):
            await asyncio.sleep(0.02)
            if op.value() > 100:
                raise ValueError("too big")
            pv.post(op.value())
            op.done()

        @pv.rpc
        def onrpc(pv, op):
            op.done(NTScalar("s").wrap("hi " + str(op.value().query.x)))

        @pv.onFirstConnect
        def first(pv):
            events.append("first")

        @pv.onLastDisconnect
        def last(pv):
            events.append("last")

        prov = StaticProvider("aio")
        prov.add(P + "pv", pv)
        with Server(providers=[prov], isolate=True) as S:
            async with Context("pva", conf=S.conf(), useenv=False) as C:
                assert await C.get(P + "pv") == 1.0
                await C.put(P + "pv", 5.0)
                assert await C.get(P + "pv") == 5.0
                assert pv.current() == 5.0
                with pytest.raises(PvaRemoteError, match="too big"):
                    await C.put(P + "pv", 500.0)
                r = await C.rpc(P + "pv", NTURI([("x", "i")]).wrap(P + "pv", kws={"x": 3}))
                assert r == "hi 3"
                got = []
                with C.monitor(P + "pv", lambda v: got.append(float(v))):
                    assert await _until(lambda: got == [5.0])
                    pv.post(9.0)
                    assert await _until(lambda: got == [5.0, 9.0])
                assert await _until(lambda: events == ["first"])
            assert await _until(lambda: events == ["first", "last"])
            await pv.close(sync=True, timeout=2.0)
            assert not pv.isOpen()

    asyncio.run(main())


def test_server_read_by_p4p_thread_client():
    async def main():
        pv = SharedPV(nt=NTScalar("i"), initial=3)

        @pv.put
        async def onput(pv, op):
            pv.post(op.value() * 2)
            op.done()

        loop = asyncio.get_running_loop()
        with Server(providers=[{P + "p4p": pv}], isolate=True) as S:
            def sync_part():
                with P4PContext("pva", conf=S.conf(), useenv=False) as C:
                    assert C.get(P + "p4p") == 3
                    C.put(P + "p4p", 4)
                    assert C.get(P + "p4p") == 8
                    with pytest.raises(P4PRemoteError, match="RPC not supported"):
                        C.rpc(P + "p4p", P4PNTURI([]).wrap(P + "p4p"))

            await loop.run_in_executor(None, sync_part)
        pv.stop()

    asyncio.run(main())


def test_server_requires_running_loop():
    with pytest.raises(RuntimeError):
        SharedPV(nt=NTScalar("i"), initial=1)


def test_server_handler_without_done_fails_op():
    async def main():
        pv = SharedPV(nt=NTScalar("i"), initial=1)

        @pv.put
        async def onput(pv, op):
            await asyncio.sleep(0.01)  # returns without done(): the client must not hang

        with Server(providers=[{P + "nodone": pv}], isolate=True) as S:
            async with Context("pva", conf=S.conf(), useenv=False) as C:
                with pytest.raises(PvaRemoteError, match="without done"):
                    await C.put(P + "nodone", 2)
                assert await C.get(P + "nodone") == 1
        pv.stop()

    asyncio.run(main())


def test_server_close_sync_waits_for_handler_and_disconnect():
    async def main():
        pv = SharedPV(nt=NTScalar("i"), initial=1)
        started = asyncio.Event()
        finished = []

        @pv.put
        async def onput(pv, op):
            started.set()
            await asyncio.sleep(0.3)
            finished.append(True)
            op.done()

        prov = StaticProvider("sync")
        prov.add(P + "sync", pv)
        with Server(providers=[prov], isolate=True) as S:
            async with Context("pva", conf=S.conf(), useenv=False) as C:
                fut = asyncio.ensure_future(C.put(P + "sync", 2, timeout=1.0))
                await asyncio.wait_for(started.wait(), 5.0)
                # p4p: close(sync=True) cannot complete while clients may
                # reconnect, so take the name away from the provider first.
                prov.remove(P + "sync")
                assert P + "sync" not in prov
                await pv.close(sync=True, timeout=2.0)
                assert finished == [True]
                assert not pv.isOpen()
                with pytest.raises(PvaError):
                    await fut
        pv.stop()

    asyncio.run(main())
