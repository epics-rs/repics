"""Channel Access client benchmark: repics vs pyepics vs aioca.

Spawns one softioc-rs with N ``ao`` records and one waveform, then runs
each library in its own process against it and prints one table.

    python bench/bench_ca.py [--softioc PATH] [--pvs 100] [--reads 2000]

Measured per library:

* ``get``        sequential scalar reads, median / p99 latency
* ``get array``  sequential 10 000-element waveform reads
* ``get list``   one call reading all N PVs, per call
* ``put wait``   sequential ``caput(wait=True)`` latency
* ``put``        ``caput(wait=False)`` throughput, puts/s (final put waits)
* ``monitor``    callbacks/s and CPU per callback on one PV under a put storm
* ``monitor N``  the same across N monitored PVs under a round-robin storm

The storm writer is always repics in a separate process, four threads of
``caput(wait=True)`` so every put is a distinct event the server has
queued; ``received/sent`` shows what the server coalesced away.

A request/reply benchmark ping-pongs two threads (Python and the client's
I/O thread) that are each idle half the time, which a ``schedutil`` or
``powersave`` CPU governor answers by parking the cores at their minimum
clock: the same code measures 100 us at 800 MHz and 50 us at 2.9 GHz. To
get a reproducible number without root, ``--pin CPUS`` confines the IOC
and every client to those CPUs and ``--hot`` keeps their SMT siblings
busy for the duration, which holds the cores at full clock. The printed
footer records the governor and the clock seen on the pinned CPUs.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).parent
WF_LEN = 10_000
STORM_PUTS = 20_000
SENTINEL = -12345.0


# ---------------------------------------------------------------------------
# infrastructure


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def write_db(path: Path, prefix: str, n: int) -> None:
    with open(path, "w") as f:
        for i in range(n):
            f.write(f'record(ao, "{prefix}ao{i}") {{\n  field(PINI, "YES")\n  field(VAL, "{i}")\n}}\n')
        f.write(f'record(waveform, "{prefix}wf") {{\n  field(FTVL, "DOUBLE")\n  field(NELM, "{WF_LEN}")\n}}\n')


def spawn_ioc(softioc: str, db: Path, port: int) -> subprocess.Popen:
    env = dict(os.environ, EPICS_CAS_SERVER_PORT=str(port), EPICS_CAS_INTF_ADDR_LIST="127.0.0.1")
    proc = subprocess.Popen([softioc, "-S", "-d", str(db)], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"softioc-rs exited with {proc.returncode}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return proc
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("softioc-rs did not come up")


def client_env(port: int) -> dict[str, str]:
    return dict(
        os.environ,
        EPICS_CA_ADDR_LIST=f"127.0.0.1:{port}",
        EPICS_CA_AUTO_ADDR_LIST="NO",
        EPICS_CA_SERVER_PORT=str(port),
    )


def stats(samples_ns: list[int]) -> dict:
    s = sorted(samples_ns)
    return {
        "n": len(s),
        "median_us": statistics.median(s) / 1e3,
        "p99_us": s[int(len(s) * 0.99) - 1] / 1e3,
        "min_us": s[0] / 1e3,
    }


def timed(fn, reps: int) -> dict:
    out = []
    for _ in range(reps):
        t = time.perf_counter_ns()
        fn()
        out.append(time.perf_counter_ns() - t)
    return stats(out)


# ---------------------------------------------------------------------------
# the storm writer (always repics)


def storm(pvs: list[str], puts: int, threads: int = 4) -> None:
    """Paced writer: every put waits for the record to process, so each one
    is a distinct event the server has queued before the next arrives.
    Several threads keep the IOC busy (repics releases the GIL while it
    waits)."""
    from repics import ca

    ca.connect(pvs)
    n = len(pvs)
    per = puts // threads

    def worker(k: int) -> None:
        # Disjoint PV subsets per thread: no two puts to one PV land within
        # the server's event-queue window, so nothing coalesces server-side
        # unless the client is behind.
        mine = pvs[k::threads] or pvs
        for i in range(per):
            ca.caput(mine[i % len(mine)], float(i + 1), wait=True)

    t = time.perf_counter()
    ts = [threading.Thread(target=worker, args=(k,)) for k in range(threads)]
    for th in ts:
        th.start()
    for th in ts:
        th.join()
    for pv in pvs:
        ca.caput(pv, SENTINEL, wait=True)
    dt = time.perf_counter() - t
    total = per * threads + n
    print(json.dumps({"puts": total, "seconds": dt, "rate": total / dt}))


def run_storm(pvs: list[str], env: dict[str, str]) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, __file__, "--storm", *pvs],
        env=env,
        stdout=subprocess.PIPE,
        text=True,
    )


class MonitorCounter:
    """Counts callbacks until every PV has shown the sentinel."""

    def __init__(self, pvs: list[str]):
        self.pending = set(pvs)
        self.count = 0
        self.first_ns = 0
        self.last_ns = 0
        self.cpu0 = 0.0
        self.cpu1 = 0.0
        self.initial = set()
        self.ready = threading.Event()
        self.done = threading.Event()
        self.lock = threading.Lock()

    def __call__(self, name: str, value: float) -> None:
        now = time.perf_counter_ns()
        with self.lock:
            if name not in self.initial:
                # the first callback is the current value, not a storm update
                self.initial.add(name)
                if len(self.initial) == len(self.pending):
                    self.ready.set()
                return
            if self.first_ns == 0:
                self.first_ns = now
                self.cpu0 = time.process_time()
            self.count += 1
            if value == SENTINEL:
                self.pending.discard(name)
                if not self.pending:
                    self.last_ns = now
                    self.cpu1 = time.process_time()
                    self.done.set()

    def result(self, storm_json: str) -> dict:
        sent = json.loads(storm_json)
        dt = (self.last_ns - self.first_ns) / 1e9
        return {
            "callbacks": self.count,
            "seconds": dt,
            "rate": self.count / dt if dt else 0.0,
            "cpu_us_per_cb": (self.cpu1 - self.cpu0) / self.count * 1e6 if self.count else 0.0,
            "sent": sent["puts"],
            "storm_rate": sent["rate"],
        }


# ---------------------------------------------------------------------------
# per-library drivers


def bench_repics(pvs: list[str], wf: str, reads: int, env: dict[str, str]) -> dict:
    import numpy

    from repics import ca

    one = pvs[0]
    ca.connect(pvs + [wf])
    ca.caput(wf, numpy.arange(WF_LEN, dtype=numpy.float64), wait=True)
    r = {}
    r["get"] = timed(lambda: ca.caget(one), reads)
    r["get array"] = timed(lambda: ca.caget(wf), reads // 10)
    r["get list"] = timed(lambda: ca.caget(pvs), reads // 20)
    r["put wait"] = timed(lambda: ca.caput(one, 1.0, wait=True), reads)
    t = time.perf_counter_ns()
    for i in range(reads * 10):
        ca.caput(one, float(i))
    ca.caput(one, 0.0, wait=True)
    r["put"] = {"rate": reads * 10 / ((time.perf_counter_ns() - t) / 1e9)}
    for label, targets in (("monitor", [one]), (f"monitor {len(pvs)}", pvs)):
        counter = MonitorCounter(targets)
        subs = ca.camonitor(targets, lambda v, i, t=targets: counter(t[i], float(v)), all_updates=True)
        assert counter.ready.wait(5)
        proc = run_storm(targets, env)
        out, _ = proc.communicate()
        assert counter.done.wait(30), (counter.count, counter.pending)
        for s in subs:
            s.close()
        r[label] = counter.result(out)
    return r


def bench_repics_aio(pvs: list[str], wf: str, reads: int, env: dict[str, str]) -> dict:
    import asyncio

    import numpy

    from repics.ca import asyncio as aio

    async def atimed(fn, reps):
        out = []
        for _ in range(reps):
            t = time.perf_counter_ns()
            await fn()
            out.append(time.perf_counter_ns() - t)
        return stats(out)

    async def main():
        one = pvs[0]
        await aio.connect(pvs + [wf])
        await aio.caput(wf, numpy.arange(WF_LEN, dtype=numpy.float64), wait=True)
        r = {}
        r["get"] = await atimed(lambda: aio.caget(one), reads)
        r["get array"] = await atimed(lambda: aio.caget(wf), reads // 10)
        r["get list"] = await atimed(lambda: aio.caget(pvs), reads // 20)
        r["put wait"] = await atimed(lambda: aio.caput(one, 1.0, wait=True), reads)
        t = time.perf_counter_ns()
        for i in range(reads * 10):
            await aio.caput(one, float(i))
        await aio.caput(one, 0.0, wait=True)
        r["put"] = {"rate": reads * 10 / ((time.perf_counter_ns() - t) / 1e9)}
        for label, targets in (("monitor", [one]), (f"monitor {len(pvs)}", pvs)):
            counter = MonitorCounter(targets)
            subs = aio.camonitor(targets, lambda v, i, t=targets: counter(t[i], float(v)), all_updates=True)
            await asyncio.wait_for(asyncio.to_thread(counter.ready.wait, 5), 6)
            proc = run_storm(targets, env)
            out = await asyncio.to_thread(lambda: proc.communicate()[0])
            await asyncio.wait_for(asyncio.to_thread(counter.done.wait, 30), 31)
            for s in subs:
                s.close()
            r[label] = counter.result(out)
        return r

    return asyncio.run(main())


def bench_pyepics(pvs: list[str], wf: str, reads: int, env: dict[str, str]) -> dict:
    import numpy

    import epics

    one = pvs[0]
    # A PV with auto_monitor=False reads over the wire on every get; the
    # default epics.caget() answers from its monitor after the first call.
    pv = epics.PV(one, auto_monitor=False)
    wfpv = epics.PV(wf, auto_monitor=False)
    pvobjs = [epics.PV(p, auto_monitor=False) for p in pvs]
    for p in pvobjs + [pv, wfpv]:
        assert p.wait_for_connection(5)
    wfpv.put(numpy.arange(WF_LEN, dtype=numpy.float64), wait=True)
    r = {}
    r["get"] = timed(lambda: pv.get(), reads)
    r["get (cached monitor)"] = timed(lambda: epics.caget(one), reads)
    r["get array"] = timed(lambda: wfpv.get(), reads // 10)

    r["get list"] = timed(lambda: epics.caget_many(pvs), reads // 20)
    r["put wait"] = timed(lambda: pv.put(1.0, wait=True), reads)
    t = time.perf_counter_ns()
    for i in range(reads * 10):
        pv.put(float(i))
    pv.put(0.0, wait=True)
    r["put"] = {"rate": reads * 10 / ((time.perf_counter_ns() - t) / 1e9)}
    for label, targets in (("monitor", [one]), (f"monitor {len(pvs)}", pvs)):
        counter = MonitorCounter(targets)
        mons = [epics.PV(p, callback=lambda pvname, value, **kw: counter(pvname, float(value))) for p in targets]
        for m in mons:
            m.wait_for_connection(5)
        assert counter.ready.wait(5)
        proc = run_storm(targets, env)
        out, _ = proc.communicate()
        assert counter.done.wait(30), (counter.count, counter.pending)
        for m in mons:
            m.clear_callbacks()
            m.disconnect()
        r[label] = counter.result(out)
    return r


def bench_aioca(pvs: list[str], wf: str, reads: int, env: dict[str, str]) -> dict:
    import asyncio

    import numpy
    from aioca import caget, camonitor, caput, connect

    async def atimed(fn, reps):
        out = []
        for _ in range(reps):
            t = time.perf_counter_ns()
            await fn()
            out.append(time.perf_counter_ns() - t)
        return stats(out)

    async def main():
        one = pvs[0]
        await connect(pvs + [wf])
        await caput(wf, numpy.arange(WF_LEN, dtype=numpy.float64), wait=True)
        r = {}
        r["get"] = await atimed(lambda: caget(one), reads)
        r["get array"] = await atimed(lambda: caget(wf), reads // 10)
        r["get list"] = await atimed(lambda: caget(pvs), reads // 20)
        r["put wait"] = await atimed(lambda: caput(one, 1.0, wait=True), reads)
        t = time.perf_counter_ns()
        for i in range(reads * 10):
            await caput(one, float(i))
        await caput(one, 0.0, wait=True)
        r["put"] = {"rate": reads * 10 / ((time.perf_counter_ns() - t) / 1e9)}
        for label, targets in (("monitor", [one]), (f"monitor {len(pvs)}", pvs)):
            counter = MonitorCounter(targets)
            subs = camonitor(targets, lambda v, i, t=targets: counter(t[i], float(v)), all_updates=True)
            await asyncio.wait_for(asyncio.to_thread(counter.ready.wait, 5), 6)
            proc = run_storm(targets, env)
            out = await asyncio.to_thread(lambda: proc.communicate()[0])
            await asyncio.wait_for(asyncio.to_thread(counter.done.wait, 30), 31)
            for s in subs:
                s.close()
            r[label] = counter.result(out)
        return r

    return asyncio.run(main())


LIBS = {
    "repics": bench_repics,
    "repics.ca.asyncio": bench_repics_aio,
    "pyepics": bench_pyepics,
    "aioca": bench_aioca,
}


# ---------------------------------------------------------------------------
# driver


def fmt(row: dict | None) -> str:
    if row is None:
        return "-"
    if "median_us" in row:
        return f"{row['median_us']:8.0f} / {row['p99_us']:8.0f} us"
    if "callbacks" in row:
        return f"{row['rate']:7.0f} cb/s {row['cpu_us_per_cb']:5.1f} us cpu/cb ({row['callbacks']}/{row['sent']})"
    return f"{row['rate']:9.0f} /s"


def cpu_siblings(cpu: int) -> list[int]:
    path = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list")
    out: list[int] = []
    for part in path.read_text().strip().split(","):
        lo, _, hi = part.partition("-")
        out.extend(range(int(lo), int(hi or lo) + 1))
    return [c for c in out if c != cpu]


def cpu_clock(cpus: list[int]) -> str:
    """Governor and current clock of ``cpus`` (Linux cpufreq), for the footer."""
    try:
        gov = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor").read_text().strip()
        mhz = [int(Path(f"/sys/devices/system/cpu/cpu{c}/cpufreq/scaling_cur_freq").read_text()) // 1000 for c in cpus]
    except OSError:
        return "cpufreq: unavailable"
    return f"governor {gov}; {min(mhz)}-{max(mhz)} MHz on cpus {','.join(map(str, cpus))}"


def spin_siblings(cpus: list[int]) -> list[subprocess.Popen]:
    """One busy loop per SMT sibling of ``cpus``, to keep the cores clocked up."""
    spinners = []
    for sib in sorted({s for c in cpus for s in cpu_siblings(c)}):
        spinners.append(
            subprocess.Popen([sys.executable, "-c", f"import os\nos.sched_setaffinity(0, {{{sib}}})\nwhile True: pass"])
        )
    return spinners


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pin", help="CPUs for the IOC and every client, e.g. 2,3,4,5")
    ap.add_argument("--hot", action="store_true", help="keep the SMT siblings of --pin busy")
    ap.add_argument("--softioc", default=os.environ.get("REPICS_SOFTIOC") or shutil.which("softioc-rs"))
    ap.add_argument("--pvs", type=int, default=100)
    ap.add_argument("--reads", type=int, default=2000)
    ap.add_argument("--libs", default=",".join(LIBS))
    ap.add_argument("--lib", help=argparse.SUPPRESS)
    ap.add_argument("--prefix", help=argparse.SUPPRESS)
    ap.add_argument("--storm", nargs="*", help=argparse.SUPPRESS)
    a = ap.parse_args()

    if a.storm:
        storm(a.storm, STORM_PUTS)
        return
    if a.lib:
        pvs = [f"{a.prefix}ao{i}" for i in range(a.pvs)]
        print(json.dumps(LIBS[a.lib](pvs, f"{a.prefix}wf", a.reads, dict(os.environ))))
        return

    if not a.softioc:
        sys.exit("no softioc-rs: pass --softioc or set REPICS_SOFTIOC")
    cpus = sorted(int(c) for c in a.pin.split(",")) if a.pin else sorted(os.sched_getaffinity(0))
    if a.pin:
        os.sched_setaffinity(0, cpus)  # inherited by the IOC and the clients
    spinners = spin_siblings(cpus) if a.hot else []
    prefix = f"bench{os.getpid()}:"
    port = free_port()
    with tempfile.TemporaryDirectory() as d:
        db = Path(d) / "bench.db"
        write_db(db, prefix, a.pvs)
        ioc = spawn_ioc(a.softioc, db, port)
        env = client_env(port)
        results = {}
        try:
            for lib in a.libs.split(","):
                cmd = [sys.executable, __file__, "--lib", lib, "--prefix", prefix, "--pvs", str(a.pvs), "--reads", str(a.reads)]
                p = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, text=True)
                if p.returncode != 0:
                    print(f"{lib}: exited {p.returncode}", file=sys.stderr)
                    continue
                results[lib] = json.loads(p.stdout.splitlines()[-1])
            clock = cpu_clock(cpus)
        finally:
            ioc.terminate()
            ioc.wait()
            for sp in spinners:
                sp.kill()

    libs = list(results)
    rows = []
    for r in results.values():
        for k in r:
            if k not in rows:
                rows.append(k)
    w = max(len(k) for k in rows) + 2
    print(f"{'':{w}}" + "".join(f"{lib:>48}" for lib in libs))
    for k in rows:
        print(f"{k:{w}}" + "".join(f"{fmt(results[lib].get(k)):>48}" for lib in libs))
    print(f"\nmedian / p99 latency; {a.pvs} PVs; {a.reads} reads; waveform {WF_LEN} doubles; storm {STORM_PUTS} puts")
    print(clock)


if __name__ == "__main__":
    main()
