"""Spawn `softioc-rs` on private ports.

The client reads `EPICS_CA_*` when the default context is first built, so
`ca_env` reserves two ports and points the address list at both before any
test imports the front ends: `ioc` serves `tests/ioc/test.db` for the whole
session on the first, `ioc2` serves `tests/ioc/aioca.db` per test on the
second so a test can kill and restart it. The IOC binary comes from
`REPICS_SOFTIOC` or `softioc-rs` on `PATH`.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

HERE = Path(__file__).parent
PREFIX = f"repics-{os.getpid()}:"
PREFIX2 = f"aioca-{os.getpid()}:"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_listening(port: int, proc: subprocess.Popen, timeout: float = 20.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"softioc-rs exited with {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"softioc-rs did not listen on {port} within {timeout} s")


def _spawn(binary: str, port: int, prefix: str, db: Path, **macros: str) -> subprocess.Popen:
    env = dict(os.environ, EPICS_CAS_SERVER_PORT=str(port), EPICS_CAS_INTF_ADDR_LIST="127.0.0.1")
    m = ",".join([f"P={prefix}", *(f"{k}={v}" for k, v in macros.items())])
    proc = subprocess.Popen(
        [binary, "-S", "-m", m, "-d", str(db)],
        env=env,
        stdout=sys.stderr,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_listening(port, proc)
    except BaseException:
        proc.kill()
        proc.wait()
        raise
    return proc


def _stop(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture(scope="session")
def ca_env() -> tuple[str, int, int]:
    binary = os.environ.get("REPICS_SOFTIOC") or shutil.which("softioc-rs")
    if not binary:
        pytest.skip("no softioc-rs: set REPICS_SOFTIOC or put softioc-rs on PATH")
    port, port2 = _free_port(), _free_port()
    os.environ["EPICS_CA_ADDR_LIST"] = f"127.0.0.1:{port} 127.0.0.1:{port2}"
    os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"
    os.environ["EPICS_CA_SERVER_PORT"] = str(port)
    return binary, port, port2


@pytest.fixture(scope="session")
def ioc(ca_env):
    binary, port, _ = ca_env
    proc = _spawn(binary, port, PREFIX, HERE / "ioc" / "test.db")
    try:
        yield PREFIX
    finally:
        _stop(proc)


class RestartableIoc:
    """The per-test IOC: `stop()` kills it mid-test, `start()` brings it back
    on the same port so cached channels reconnect."""

    def __init__(self, binary: str, port: int):
        self.binary = binary
        self.port = port
        self.prefix = PREFIX2
        self.proc: subprocess.Popen | None = None

    def start(self, **macros: str) -> None:
        assert self.proc is None or self.proc.poll() is not None
        self.proc = _spawn(self.binary, self.port, self.prefix, HERE / "ioc" / "aioca.db", **macros)

    def stop(self) -> None:
        if self.proc is not None:
            _stop(self.proc)


@pytest.fixture
def ioc2(ca_env):
    binary, _, port2 = ca_env
    ioc = RestartableIoc(binary, port2)
    ioc.start()
    try:
        yield ioc
    finally:
        ioc.stop()
