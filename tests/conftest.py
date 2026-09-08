"""Spawn one `softioc-rs` for the session on a private port.

The client reads `EPICS_CA_*` when the default context is first built, so
the environment is set here before any test imports the front ends. The
IOC binary comes from `EPICSRS_SOFTIOC` or `softioc-rs` on `PATH`.
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
PREFIX = f"epicsrs-{os.getpid()}:"


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


@pytest.fixture(scope="session")
def ioc():
    binary = os.environ.get("EPICSRS_SOFTIOC") or shutil.which("softioc-rs")
    if not binary:
        pytest.skip("no softioc-rs: set EPICSRS_SOFTIOC or put softioc-rs on PATH")
    port = _free_port()
    env = dict(os.environ, EPICS_CAS_SERVER_PORT=str(port), EPICS_CAS_INTF_ADDR_LIST="127.0.0.1")
    proc = subprocess.Popen(
        [binary, "-S", "-m", f"P={PREFIX}", "-d", str(HERE / "ioc" / "test.db")],
        env=env,
        stdout=sys.stderr,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_listening(port, proc)
        os.environ["EPICS_CA_ADDR_LIST"] = f"127.0.0.1:{port}"
        os.environ["EPICS_CA_AUTO_ADDR_LIST"] = "NO"
        os.environ["EPICS_CA_SERVER_PORT"] = str(port)
        yield PREFIX
    finally:
        proc.terminate()
        try:
            proc.wait(5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
