"""repics.ca.server driven by the repics CA client.

The server binds ephemeral ports (``isolate=True``), so 5064 is never touched
and the suite can run next to a live IOC. The client reaches it through a
default context rebuilt from the server's ``conf()`` for this module only; the
context is reset on teardown so the softioc-backed CA tests are unaffected.
"""

from __future__ import annotations

import threading
import time

import numpy as np
import pytest

from repics import ca
from repics import _context as ctxmod
from repics.ca.server import Handler, Server, SharedPV, StaticProvider

P = f"repics-ca-srv-{__import__('os').getpid()}:"


def _wait(pred, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.01)
    return False


class Recorder(Handler):
    def __init__(self):
        self.puts: list[tuple] = []

    def put(self, pv, op):
        v = op.value()
        self.puts.append((v, op.peer(), op.account(), op.name()))
        if isinstance(v, (int, float)) and v > 100:
            op.done(error="too big")
            return
        pv.post(v)
        op.done()


def _reset_default_context():
    ctxmod.purge_channel_caches()
    with ctxmod._lock:
        ctxmod._context = None


@pytest.fixture(scope="module")
def served():
    rec = Recorder()
    scalar = SharedPV(handler=rec, initial=1.5)
    plain = SharedPV(initial=3)
    text = SharedPV(initial="hello")
    array = SharedPV(initial=np.arange(5, dtype="f8"))
    readonly = SharedPV(initial=7.0)  # no handler -> puts refused
    provider = StaticProvider("srv")
    for short, pv in [("scalar", scalar), ("plain", plain), ("text", text),
                      ("array", array), ("ro", readonly)]:
        provider.add(P + short, pv)
    with Server(providers=[provider], isolate=True) as server:
        yield server, rec, {"scalar": scalar, "plain": plain, "text": text,
                            "array": array, "ro": readonly}


@pytest.fixture(scope="module")
def client(served):
    server, _, _ = served
    conf = server.conf()
    saved = {k: __import__("os").environ.get(k) for k in conf}
    import os
    os.environ.update(conf)
    _reset_default_context()
    try:
        yield ca
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        _reset_default_context()


def test_conf_has_ephemeral_ports(served):
    server, _, _ = served
    conf = server.conf()
    assert conf["EPICS_CA_ADDR_LIST"].startswith("127.0.0.1:")
    assert conf["EPICS_CA_ADDR_LIST"] != "127.0.0.1:5064"
    assert conf["EPICS_CA_AUTO_ADDR_LIST"] == "NO"
    assert conf["EPICS_CA_SERVER_PORT"] not in ("0", "5064")
    assert server.running


def test_get_scalar(client):
    assert client.caget(P + "scalar") == 1.5
    assert client.caget(P + "plain") == 3


def test_get_string(client):
    assert client.caget(P + "text") == "hello"


def test_get_array(client):
    a = client.caget(P + "array", count=-1)
    np.testing.assert_array_equal(np.asarray(a, dtype="f8"), np.arange(5, dtype="f8"))


def test_put_through_handler(client, served):
    _, rec, pvs = served
    client.caput(P + "scalar", 2.5, wait=True)
    assert client.caget(P + "scalar") == 2.5
    assert pvs["scalar"].current() == 2.5
    v, peer, account, name = rec.puts[-1]
    assert v == 2.5
    assert peer.startswith("127.0.0.1:")
    assert name == P + "scalar"


def test_put_rejected_by_handler(client):
    with pytest.raises(ca.CaError):
        client.caput(P + "scalar", 999.0, wait=True)
    # the rejected value was never stored
    assert client.caget(P + "scalar") == 2.5


def test_put_refused_without_handler(client):
    with pytest.raises(ca.CaError):
        client.caput(P + "ro", 42.0, wait=True)
    assert client.caget(P + "ro") == 7.0


def test_monitor_sees_posts(client, served):
    _, _, pvs = served
    seen: list[float] = []
    lock = threading.Lock()

    def cb(v):
        with lock:
            seen.append(float(v))

    sub = client.camonitor(P + "plain", cb)
    try:
        assert _wait(lambda: len(seen) >= 1)  # initial value
        pvs["plain"].post(11)
        assert _wait(lambda: 11.0 in seen)
        pvs["plain"].post(22)
        assert _wait(lambda: seen[-1] == 22.0)
    finally:
        sub.close()
    # CA coalesces bursts, so intermediate posts can drop; the initial value
    # and every value the monitor settled on must appear.
    assert seen[0] == 3.0
    assert 11.0 in seen and 22.0 in seen


def test_current_and_close(served):
    _, _, pvs = served
    pv = SharedPV(initial=1.0)
    assert pv.isOpen()
    assert pv.current() == 1.0
    pv.post(2.0)
    assert pv.current() == 2.0
    pv.close()
    assert not pv.isOpen()
    assert pv.current() is None
    pv.open(5.0)
    assert pv.isOpen()
    assert pv.current() == 5.0
