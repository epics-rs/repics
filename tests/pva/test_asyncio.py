"""asyncio flavours: epicsrs.pva.asyncio.Context and epicsrs.pva.server.asyncio.SharedPV."""

from __future__ import annotations

import asyncio

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
