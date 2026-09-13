"""Interpreter exit with the runtime still owing asyncio an outcome."""

import subprocess
import sys

import pytest

SCRIPT = """
import asyncio, sys
from repics.aio import caget, caput, camonitor
pv = sys.argv[1]
async def main():
    await caput(pv, 3, wait=True)
    await caget(pv)
    sub = camonitor(pv, lambda v: None)
    await asyncio.sleep(0.2)
    sub.close()
asyncio.run(main())
print("done")
"""


@pytest.mark.parametrize("run", range(10))
def test_exit_after_aio_camonitor_is_clean(ioc, run):
    # The dispatcher task is cancelled at loop close while its wait on the
    # hub is still on the runtime; that outcome must be posted before the
    # interpreter finalizes (it panicked in 8 of 20 runs before).
    p = subprocess.run([sys.executable, "-c", SCRIPT, ioc + "ai"], capture_output=True, text=True, timeout=30)
    assert p.returncode == 0, p.stderr
    assert "panicked" not in p.stderr, p.stderr
    assert p.stdout.strip() == "done"
