"""aioca's test suite (DiamondLightSource/aioca, tests/test_aioca.py) run
against ``epicsrs.aio``, as a functional check of the asyncio front end.

Names follow this package (``form=`` for ``format=``, ``CaNothing``,
``CaTimeout``), the IOC is ``softioc-rs`` on ``tests/ioc/aioca.db`` and is
killed mid-test where aioca sent it ``exit``. Not ported, because they
reach aioca internals: ``test_monitor_with_failing_dbr`` (``dbr_to_value``
hook; the callback-raises tests cover ours), ``test_value_event_raises``
and ``test_subscription`` (``_catools``), ``test_run_forever`` and the
``pending_values`` half of ``test_closing_event_loop`` (``aioca.run``).
Where aioca faked a channel's connection state to reach a disconnect
error, the port keeps the real behaviour: a read on a disconnected channel
waits for reconnection and times out.
"""

from __future__ import annotations

import asyncio
import functools
import gc
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

import epicsrs
from epicsrs import aio
from epicsrs.aio import (
    CAInfo,
    CaNothing,
    caget,
    cainfo,
    camonitor,
    caput,
    connect,
    get_channel_infos,
    purge_channel_caches,
)

TIMEOUT = 10


def async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        asyncio.run(fn(*args, **kwargs))

    return wrapper


@pytest.fixture
def pv(ioc2):
    """The PV names of ``aioca.db`` under this test's IOC; the channel cache
    is purged afterwards, as aioca's fixture does."""

    class Names:
        LONGOUT = ioc2.prefix + "longout"  # an int that starts as 42
        SI = ioc2.prefix + "si"  # a string that starts as "me"
        TICKING = ioc2.prefix + "ticking"  # increments every second
        NE = ioc2.prefix + "ne"  # does not exist
        BAD_EGUS = ioc2.prefix + "bad_egus"  # EGU is U+FFFD
        WAVEFORM = ioc2.prefix + "waveform"  # 5 shorts
        RO = ioc2.prefix + "waveform.NELM"  # read only
        SEQ = ioc2.prefix + "seq"  # 8 updates to seqout
        SEQOUT = ioc2.prefix + "seqout"

    yield Names
    purge_channel_caches()


async def poll_length(array, gt=0, timeout=TIMEOUT):
    start = time.time()
    while not len(array) > gt:
        await asyncio.sleep(0.01)
        assert time.time() - start < timeout


# ---------------------------------------------------------------------------
# connect / cainfo / caget / caput


@async_test
async def test_connect(pv) -> None:
    conn = await connect(pv.LONGOUT, timeout=TIMEOUT)
    assert type(conn) is CaNothing
    conn2 = await connect([pv.SI, pv.NE], throw=False, timeout=1.0)
    assert len(conn2) == 2
    assert type(conn2[0]) is CaNothing
    assert conn2[0].ok
    assert type(conn2[1]) is CaNothing
    assert not conn2[1].ok


@async_test
async def test_cainfo(ioc2, pv) -> None:
    conn2 = await cainfo([pv.WAVEFORM, pv.SI], timeout=TIMEOUT)
    assert conn2[0].datatype == epicsrs.DBR_SHORT
    assert conn2[1].datatype == epicsrs.DBR_STRING
    conn = await cainfo(pv.LONGOUT)
    assert type(conn) is CAInfo
    assert conn.ok is True
    assert conn.name == pv.LONGOUT
    assert conn.state_strings[conn.state] == "connected"
    assert isinstance(conn.host, str)
    assert conn.read is True
    assert conn.write is True
    assert conn.count == 1
    assert conn.datatype_strings[conn.datatype] == "long"
    ioc2.stop()
    for _ in range(50):
        await asyncio.sleep(0.1)
        conn = await cainfo(pv.LONGOUT, wait=False)
        if conn.datatype == epicsrs.DBR_NO_ACCESS:
            break
    assert conn.datatype == epicsrs.DBR_NO_ACCESS
    assert (
        str(conn)
        == f"""{pv.LONGOUT}:
    State: previously connected
    Host: <disconnected>
    Access: False, False
    Data type: no access
    Count: 0"""
    )


@async_test
async def test_get_non_existent_pvs_no_throw(ioc2, pv) -> None:
    values = await caget([pv.WAVEFORM, pv.NE], throw=False, timeout=1.0)
    assert [True, False] == [v.ok for v in values]
    assert pytest.approx([]) == values[0]
    ioc2.stop()
    await asyncio.sleep(0.5)
    values = await caget([pv.WAVEFORM, pv.NE], throw=False, timeout=0.1)
    assert [False, False] == [v.ok for v in values]
    assert [epicsrs.ECA_TIMEOUT, epicsrs.ECA_TIMEOUT] == [v.errorcode for v in values]
    with pytest.raises(epicsrs.CaTimeout):
        await caget(pv.NE, timeout=0.1)
    with pytest.raises(epicsrs.CaTimeout):
        await caget(pv.WAVEFORM, timeout=0.1)


@pytest.mark.parametrize("seq", (list, tuple))
@async_test
async def test_get_two_pvs(pv, seq) -> None:
    value = await caget(seq([pv.LONGOUT, pv.SI]), timeout=TIMEOUT)
    assert [42, "me"] == value


@async_test
async def test_get_pv_with_bad_egus(pv) -> None:
    value = await caget(pv.BAD_EGUS, form="ctrl", timeout=TIMEOUT)
    assert 32 == value
    assert value.units == "�"


@async_test
async def test_get_waveform_pv(pv) -> None:
    value = await caget(pv.WAVEFORM, timeout=TIMEOUT)
    assert len(value) == 0
    assert isinstance(value, epicsrs.AugmentedArray)
    await caput(pv.WAVEFORM, [1, 2, 3, 4])
    assert pytest.approx([1, 2, 3, 4]) == await caget(pv.WAVEFORM)
    assert pytest.approx([1, 2, 3, 4, 0]) == await caget(pv.WAVEFORM, count=6)
    assert pytest.approx([1, 2, 3, 4, 0]) == await caget(pv.WAVEFORM, count=-1)
    assert pytest.approx([1, 2]) == await caget(pv.WAVEFORM, count=2)


@async_test
async def test_caput(pv) -> None:
    v1 = await asyncio.wait_for(caput(pv.LONGOUT, 43, wait=True, timeout=None), TIMEOUT)
    assert isinstance(v1, CaNothing)
    v2 = await caget(pv.LONGOUT)
    assert 43 == v2


@async_test
async def test_caput_on_ro_pv_fails(pv) -> None:
    with pytest.raises(epicsrs.CaError):
        await caput(pv.RO, 43, timeout=TIMEOUT)
    result = await caput(pv.RO, 43, throw=False)
    assert not result.ok
    assert str(result).endswith("Write access denied")


@pytest.mark.parametrize("seq", (list, tuple))
@async_test
async def test_caput_two_pvs_same_value(pv, seq) -> None:
    pvs = seq([pv.LONGOUT, pv.SI])
    await caput(pvs, 43, timeout=TIMEOUT)
    value = await caget(pvs)
    assert [43, "43"] == value
    await caput(pvs, "44")
    value = await caget(pvs)
    assert [44, "44"] == value


@async_test
async def test_caput_two_pvs_different_value(pv) -> None:
    await caput([pv.LONGOUT, pv.SI], [44, "blah"], timeout=TIMEOUT)
    value = await caget([pv.LONGOUT, pv.SI])
    assert [44, "blah"] == value


@async_test
async def test_caget_non_existent(ca_env) -> None:
    name = "epicsrs-nowhere:ne"
    with pytest.raises(epicsrs.CaTimeout):
        await caget(name, timeout=0.1)
    v = await caget(name, timeout=0.1, throw=False)
    assert f"CaNothing('{name}', 80)" == repr(v)
    assert f"{name}: User specified timeout on IO operation expired" == str(v)
    assert False is bool(v)
    with pytest.raises(TypeError):
        for _ in v:  # type: ignore
            pass


@async_test
async def test_caget_non_existent_and_good(pv) -> None:
    await caput(pv.WAVEFORM, [1, 2, 3, 4], timeout=TIMEOUT)
    try:
        await caget([pv.NE, pv.WAVEFORM], timeout=1.0)
    except epicsrs.CaTimeout:
        pass
    await asyncio.sleep(0.5)
    gc.collect()
    x = [x for x in gc.get_objects() if isinstance(x, epicsrs.AugmentedArray)]
    assert len(x) == 0


# ---------------------------------------------------------------------------
# camonitor


@async_test
async def test_monitor(ioc2, pv) -> None:
    values: list = []
    m = camonitor(pv.LONGOUT, values.append, notify_disconnect=True)

    await poll_length(values)
    await asyncio.sleep(0.1)
    await caput(pv.LONGOUT, 43, wait=True)
    await asyncio.sleep(0.1)
    await caput(pv.LONGOUT, 44, wait=True)
    await asyncio.sleep(0.1)
    ioc2.stop()

    await poll_length(values, gt=3, timeout=5)
    m.close()

    assert [42, 43, 44] == values[:3]
    assert [True, True, True, False] == [v.ok for v in values]
    assert values[3].errorcode == epicsrs.ECA_DISCONN


@async_test
async def test_monitor_two_pvs(ioc2, pv) -> None:
    values: list = []
    await caput(pv.WAVEFORM, [1, 2], wait=True, timeout=TIMEOUT)
    ms = camonitor([pv.WAVEFORM, pv.LONGOUT], lambda v, n: values.append((v, n)), count=-1)

    await poll_length(values, gt=1)

    assert sorted(values, key=lambda t: t[1]) == [(pytest.approx([1, 2, 0, 0, 0]), 0), (42, 1)]
    values.clear()
    await caput(pv.LONGOUT, 11, wait=True)
    await asyncio.sleep(0.1)
    await caput(pv.LONGOUT, 12, wait=True)
    await asyncio.sleep(0.1)
    assert values == [(11, 1), (12, 1)]
    values.clear()

    for m in ms:
        m.close()
    ioc2.stop()
    await asyncio.sleep(1.0)

    assert values == []


@async_test
async def test_long_monitor_callback(pv) -> None:
    values = []

    async def cb(value):
        values.append(value)
        await asyncio.sleep(0.4)

    m = camonitor(pv.LONGOUT, cb, connect_timeout=(time.time() + 0.5,))
    await poll_length(values)
    assert values == [42]
    assert m.dropped_callbacks == 0
    # These two caputs happen during the sleep of the first cb and are
    # squashed together for the second cb. The drop is counted when the
    # queued updates are taken, after the first cb returns (aioca counted
    # it as they arrived).
    await caput(pv.LONGOUT, 43)
    await caput(pv.LONGOUT, 44)
    await asyncio.sleep(0.2)
    assert [42] == values
    await asyncio.sleep(0.6)
    assert [42, 44] == values
    assert m.dropped_callbacks == 1
    values.clear()
    # Add another one and close before the cb can fire
    await caput(pv.LONGOUT, 45)
    # Block the event loop so the update queues without the callback running
    time.sleep(0.4)
    m.close()
    await asyncio.sleep(0.4)
    assert [] == values
    assert m.dropped_callbacks == 1


@async_test
async def test_long_monitor_all_updates(pv) -> None:
    values = []

    async def cb(value):
        values.append(value)
        await asyncio.sleep(0.4)

    m = camonitor(pv.LONGOUT, cb, connect_timeout=(time.time() + 0.5,), all_updates=True)
    await poll_length(values)
    assert values == [42]
    assert m.dropped_callbacks == 0
    await caput(pv.LONGOUT, 43)
    await caput(pv.LONGOUT, 44)
    await asyncio.sleep(0.6)
    assert [42, 43] == values
    assert m.dropped_callbacks == 0
    await asyncio.sleep(0.6)
    assert [42, 43, 44] == values
    assert m.dropped_callbacks == 0


@async_test
async def test_sync_monitor_fast_updates(pv) -> None:
    successes = 0
    for _ in range(10):
        values: list[int] = []
        await caput(pv.SEQOUT, 0, wait=True)
        await connect(pv.SEQ)
        m = camonitor(pv.SEQOUT, values.append, connect_timeout=(time.time() + 0.5,), all_updates=True)
        await caput(pv.SEQ, 1, wait=True)
        assert await caget(pv.SEQOUT) == 8
        await asyncio.sleep(0.1)
        assert values[-1] == 8
        assert m.dropped_callbacks == 0
        # The server may coalesce the burst; count the runs where it did not
        if values == [0, 1, 2, 3, 4, 5, 6, 7, 8]:
            successes += 1
        m.close()
    assert successes > 3


@async_test
async def test_exception_raising_monitor_callback(pv, capsys) -> None:
    expected = [42]

    m = camonitor(pv.LONGOUT, lambda v: expected.remove(v))
    assert m.state == m.OPENING
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""

    await asyncio.sleep(0.5)
    assert expected == []

    await caput(pv.LONGOUT, 35)
    await asyncio.sleep(0.5)
    assert m.state == m.CLOSED
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "ValueError: list.remove(x): x not in list" in captured.err

    values: list = []
    m.callback = values.append
    await caput(pv.LONGOUT, 32)
    assert m.state == m.CLOSED
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert len(values) == 0


@async_test
async def test_async_exception_raising_monitor_callback(pv, capsys) -> None:
    async def boom_async(value) -> None:
        raise ValueError("Boom")

    m = camonitor(pv.LONGOUT, boom_async)
    assert m.state == m.OPENING
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""

    await asyncio.sleep(0.5)
    assert m.state == m.CLOSED
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "ValueError: Boom" in captured.err

    values: list = []
    m.callback = values.append
    await caput(pv.LONGOUT, 32)
    assert m.state == m.CLOSED
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert len(values) == 0


@async_test
async def test_camonitor_non_existent(pv) -> None:
    values: list = []
    m = camonitor(pv.NE, values.append, connect_timeout=0.2)
    try:
        assert len(values) == 0
        await asyncio.sleep(0.1)
        assert len(values) == 0
        await asyncio.sleep(0.5)
        assert len(values) == 1
        assert not values[0].ok
    finally:
        m.close()


@async_test
async def test_camonitor_non_existent_async(pv) -> None:
    q: asyncio.Queue = asyncio.Queue()
    m = camonitor(pv.NE, q.put, connect_timeout=0.2)
    try:
        assert q.qsize() == 0
        await asyncio.sleep(0.1)
        assert q.qsize() == 0
        await asyncio.sleep(0.5)
        assert q.qsize() == 1
        assert not q.get_nowait().ok
    finally:
        m.close()


@async_test
async def test_monitor_gc(ioc2, pv) -> None:
    values: list = []
    camonitor(pv.LONGOUT, values.append, notify_disconnect=True)

    await poll_length(values)
    assert len(values) == 1
    await caput(pv.LONGOUT, 43, wait=True)
    await asyncio.sleep(0.1)
    gc.collect()
    await asyncio.sleep(0.1)
    await caput(pv.LONGOUT, 44, wait=True)
    await asyncio.sleep(0.1)
    ioc2.stop()
    await poll_length(values, gt=3, timeout=5)

    assert [42, 43, 44] == values[:3]
    assert [True, True, True, False] == [v.ok for v in values]


def test_closing_event_loop(ioc2, pv) -> None:
    """A monitor whose loop has closed stops delivering and closes cleanly."""
    got: list = []

    async def monitor_for_a_bit():
        m = camonitor(pv.TICKING, got.append, notify_disconnect=True, all_updates=True)
        await asyncio.sleep(1.1)
        return m

    m = asyncio.run(monitor_for_a_bit())
    assert got and got[0] >= 0
    assert m.state == m.CLOSED
    n = len(got)
    time.sleep(2.0)
    assert len(got) == n
    m.close()
    ioc2.stop()
    time.sleep(0.5)
    assert len(got) == n


def test_ca_nothing_dunder_methods() -> None:
    good = CaNothing("all ok")
    assert good
    with pytest.raises(TypeError):
        for _x in good:  # type: ignore
            pass
    bad = CaNothing("not all ok", epicsrs.ECA_DISCONN)
    assert not bad
    with pytest.raises(TypeError):
        for _x in bad:  # type: ignore
            pass


# ---------------------------------------------------------------------------
# threads and channel infos


def test_import_in_a_different_thread(pv) -> None:
    output = subprocess.check_output(
        [sys.executable, str(Path(__file__).parent / "import_in_different_thread.py"), pv.LONGOUT]
    )
    assert output.strip() == b"42"


@async_test
async def test_read_pvs_from_different_threads(pv) -> None:
    returned_values = []

    async def get_value():
        returned_values.append(await caget(pv.LONGOUT, timeout=TIMEOUT))

    await get_value()

    def thread_function():
        loop = asyncio.new_event_loop()
        loop.run_until_complete(get_value())
        loop.close()

    t = threading.Thread(target=thread_function)
    t.start()
    t.join()

    assert returned_values == [42, 42]


@async_test
async def test_channel_connected(ioc2, pv) -> None:
    values: list = []
    m = camonitor(pv.LONGOUT, values.append, notify_disconnect=True)

    await poll_length(values)

    channels = get_channel_infos()
    assert len(channels) == 1

    channel = channels[0]
    assert channel.name == pv.LONGOUT
    assert channel.connected
    assert channel.subscriber_count == 1

    ioc2.stop()
    await poll_length(values, gt=1, timeout=5)

    channel = get_channel_infos()[0]
    assert not channel.connected
    assert channel.subscriber_count == 1

    m.close()

    channel = get_channel_infos()[0]
    assert channel.subscriber_count == 0


@async_test
async def test_reconnect_after_ioc_restart(ioc2, pv) -> None:
    """Not in aioca: a monitor and the channel cache survive an IOC restart."""
    values: list = []
    m = camonitor(pv.LONGOUT, values.append, notify_disconnect=True)
    await poll_length(values)
    ioc2.stop()
    await poll_length(values, gt=1, timeout=5)
    assert not values[1].ok
    ioc2.start(VAL="50")  # inside DRVL..DRVH, or PINI clamps it
    await poll_length(values, gt=2, timeout=TIMEOUT)
    assert values[2] == 50 and values[2].ok
    assert await caget(pv.LONGOUT, timeout=TIMEOUT) == 50
    m.close()


def test_aio_module_exports() -> None:
    assert aio.DEFAULT_TIMEOUT == 5.0
    assert aio.ECA_DISCONN == epicsrs.ECA_DISCONN
