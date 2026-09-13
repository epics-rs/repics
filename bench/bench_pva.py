"""pvAccess benchmark: repics vs p4p, client side and server side.

    python bench/bench_pva.py [--pvs 100] [--reads 2000] [--puts 20000] [--pin 2,3,4,5 --hot]

Client rows (``--libs``): every client library in its own process against
the SAME server. ``softioc-rs`` serves Channel Access only (no QSRV), so
that server is an repics thread ``SharedPV`` server in its own process,
N ``NTScalar('d')`` PVs plus one 10 000-double ``NTScalar('ad')``, whose
put handler posts what it is given.

* ``get``        sequential scalar reads, median / p99 latency
* ``get array``  sequential 10 000-element array reads
* ``get list``   one call reading all N PVs, per call
* ``put wait``   sequential ``put(wait=True)`` latency
* ``put``        sequential ``put(wait=False)``, puts/s. pvAccess has no
                 fire-and-forget put: each one is a completed round trip
                 (two with the default ``get=True``, in both libraries)
* ``monitor``    callbacks/s and CPU per callback on one PV under a put storm
* ``monitor N``  the same across N monitored PVs under a round-robin storm

The client-row storm writer is always repics in a separate process, four
threads of ``put(wait=True)`` with a prebuilt ``Value`` (one round trip),
so every put is a distinct post the server has queued before the next
arrives; ``received/sent`` shows what was squashed on the way.

Server rows (``--servers``): every server flavour in its own process,
measured by one fixed client, the p4p thread ``Context``, in its own
process.

* ``get``        median / p99 of a scalar read
* ``put wait``   median / p99 of ``put(wait=True)`` through the put handler
* ``monitor``    one subscriber on one PV under an in-process post storm:
                 events/s seen by the client, server CPU per post, and
                 ``received/sent``
* ``monitor N``  N subscribers, one per PV, storm round-robin over the N PVs

The server-row storm is one thread in the server process posting as fast
as ``SharedPV.post`` returns (a control PV starts it, a string PV carries
its statistics back), so ``received/sent`` there is the server's own
queue squashing under a producer faster than the wire.

``--pin`` / ``--hot`` mean what they mean in ``bench_ca.py`` (same helpers).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from functools import partial

from bench_ca import SENTINEL, STORM_PUTS, WF_LEN, MonitorCounter, cpu_clock, fmt, spin_siblings, stats, timed

PROBE_CLIENT = "p4p"  # the fixed client of the server rows


# ---------------------------------------------------------------------------
# infrastructure


def names(prefix: str, n: int) -> tuple[list[str], str, str, str]:
    """(scalar PVs, waveform, storm control PV, storm statistics PV)."""
    return [f"{prefix}pv{i}" for i in range(n)], f"{prefix}wf", f"{prefix}storm", f"{prefix}stats"


def client_env(conf: dict[str, str]) -> dict[str, str]:
    env = dict(os.environ)
    env.update((k, v) for k, v in conf.items() if k.startswith("EPICS_PVA_"))
    return env


def spawn_server(kind: str, prefix: str, n: int, puts: int = STORM_PUTS) -> tuple[subprocess.Popen, dict[str, str]]:
    """Start a ``--serve`` process; its first stdout line is the client conf."""
    cmd = [sys.executable, __file__, "--serve", kind, "--prefix", prefix, "--pvs", str(n), "--puts", str(puts)]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError(f"{kind} server exited with {proc.wait()}")
    return proc, json.loads(line)


def stop_server(proc: subprocess.Popen) -> None:
    proc.stdin.close()  # the server exits when stdin ends
    try:
        proc.wait(10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# the servers (``--serve KIND``)


def storm_posts(pvs: list, stats_pv, puts: int) -> None:
    """In-process storm: one thread posting round-robin, then the sentinel
    on every PV, then the statistics (posts, seconds, process CPU) on the
    stats PV, whose value the probe polls for."""
    cpu0 = time.process_time()
    t = time.perf_counter()
    n = len(pvs)
    for i in range(puts):
        pvs[i % n].post(float(i + 1))
    for pv in pvs:
        pv.post(SENTINEL)
    dt = time.perf_counter() - t
    cpu = time.process_time() - cpu0
    total = puts + n
    stats_pv.post(json.dumps({"puts": total, "seconds": dt, "rate": total / dt, "cpu_s": cpu}))


def make_handlers(pvs_ref: list, stats_ref: list, puts: int):
    """The three put handlers, shared by all four server flavours: a data
    PV posts what it is given; the control PV starts a storm over the
    first ``k`` PVs (``k`` is the value put); the stats PV is plain
    storage the probe clears and then reads."""

    class Echo:
        def put(self, pv, op):
            pv.post(op.value())
            op.done()

    class Control:
        def put(self, pv, op):
            k = int(op.value())
            threading.Thread(target=storm_posts, args=(pvs_ref[0][:k], stats_ref[0], puts), daemon=True).start()
            op.done()

    return Echo(), Control()


def serve_thread(lib: str, prefix: str, n: int, puts: int) -> None:
    import numpy

    if lib == "repics":
        from repics.pva.nt import NTScalar
        from repics.pva.server import Server, SharedPV
    else:
        from p4p.nt import NTScalar
        from p4p.server import Server
        from p4p.server.thread import SharedPV

    pvnames, wf, ctrl, st = names(prefix, n)
    pvs_ref: list = [None]
    stats_ref: list = [None]
    echo, control = make_handlers(pvs_ref, stats_ref, puts)
    pvs = [SharedPV(handler=echo, nt=NTScalar("d"), initial=float(i)) for i in range(n)]
    wfpv = SharedPV(handler=echo, nt=NTScalar("ad"), initial=numpy.arange(WF_LEN, dtype=numpy.float64))
    ctrlpv = SharedPV(handler=control, nt=NTScalar("i"), initial=0)
    statpv = SharedPV(handler=echo, nt=NTScalar("s"), initial="")
    pvs_ref[0], stats_ref[0] = pvs, statpv
    table = dict(zip(pvnames, pvs))
    table.update({wf: wfpv, ctrl: ctrlpv, st: statpv})
    with Server(providers=[table], isolate=True) as server:
        print(json.dumps(server.conf()), flush=True)
        sys.stdin.read()  # until the driver closes our stdin


def serve_asyncio(lib: str, prefix: str, n: int, puts: int) -> None:
    import asyncio

    import numpy

    if lib == "repics":
        from repics.pva.nt import NTScalar
        from repics.pva.server import Server
        from repics.pva.server.asyncio import SharedPV
    else:
        from p4p.nt import NTScalar
        from p4p.server import Server
        from p4p.server.asyncio import SharedPV

    async def main() -> None:
        pvnames, wf, ctrl, st = names(prefix, n)
        pvs_ref: list = [None]
        stats_ref: list = [None]
        echo, control = make_handlers(pvs_ref, stats_ref, puts)
        pvs = [SharedPV(handler=echo, nt=NTScalar("d"), initial=float(i)) for i in range(n)]
        wfpv = SharedPV(handler=echo, nt=NTScalar("ad"), initial=numpy.arange(WF_LEN, dtype=numpy.float64))
        ctrlpv = SharedPV(handler=control, nt=NTScalar("i"), initial=0)
        statpv = SharedPV(handler=echo, nt=NTScalar("s"), initial="")
        pvs_ref[0], stats_ref[0] = pvs, statpv
        table = dict(zip(pvnames, pvs))
        table.update({wf: wfpv, ctrl: ctrlpv, st: statpv})
        with Server(providers=[table], isolate=True) as server:
            print(json.dumps(server.conf()), flush=True)
            await asyncio.get_running_loop().run_in_executor(None, sys.stdin.read)

    asyncio.run(main())


SERVERS = {
    "repics": partial(serve_thread, "repics"),
    "repics.asyncio": partial(serve_asyncio, "repics"),
    "p4p": partial(serve_thread, "p4p"),
    "p4p.asyncio": partial(serve_asyncio, "p4p"),
}


# ---------------------------------------------------------------------------
# the client-row storm writer (always repics)


def storm(pvs: list[str], puts: int, threads: int = 4) -> None:
    """Paced writer: every put waits for the server's put handler to have
    posted, so each one is a distinct event queued before the next
    arrives. Four threads keep the server busy (repics releases the GIL
    while it waits). A prebuilt ``Value`` per PV makes a put one round
    trip rather than get-then-put."""
    from repics.pva import Context, Value

    ctxt = Context()
    vals = {pv: Value(ctxt.info(pv)) for pv in pvs}
    n = len(pvs)
    per = puts // threads

    def worker(k: int) -> None:
        # Disjoint PV subsets per thread: no two puts to one PV are in
        # flight together, so nothing coalesces server-side unless the
        # monitoring client is behind.
        mine = pvs[k::threads] or pvs
        for i in range(per):
            V = vals[mine[i % len(mine)]]
            V["value"] = float(i + 1)
            ctxt.put(mine[i % len(mine)], V, wait=True)

    t = time.perf_counter()
    ts = [threading.Thread(target=worker, args=(k,)) for k in range(threads)]
    for th in ts:
        th.start()
    for th in ts:
        th.join()
    for pv in pvs:
        V = vals[pv]
        V["value"] = SENTINEL
        ctxt.put(pv, V, wait=True)
    dt = time.perf_counter() - t
    total = per * threads + n
    print(json.dumps({"puts": total, "seconds": dt, "rate": total / dt}))


def run_storm(pvs: list[str], puts: int, env: dict[str, str]) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, __file__, "--puts", str(puts), "--storm", *pvs],
        env=env,
        stdout=subprocess.PIPE,
        text=True,
    )


# ---------------------------------------------------------------------------
# per-client-library drivers (``--lib LIB``)


def bench_repics(pvs: list[str], wf: str, reads: int, puts: int, env: dict[str, str]) -> dict:
    from repics.pva import Context

    one = pvs[0]
    ctxt = Context()
    for pv in pvs + [wf]:
        ctxt.connect(pv)
    r = {}
    r["get"] = timed(lambda: ctxt.get(one), reads)
    r["get array"] = timed(lambda: ctxt.get(wf), max(1, reads // 10))
    r["get list"] = timed(lambda: ctxt.get(pvs), max(1, reads // 20))
    r["put wait"] = timed(lambda: ctxt.put(one, 1.0, wait=True), reads)
    t = time.perf_counter_ns()
    for i in range(reads * 10):
        ctxt.put(one, float(i), wait=False)
    r["put"] = {"rate": reads * 10 / ((time.perf_counter_ns() - t) / 1e9)}
    for label, targets in (("monitor", [one]), (f"monitor {len(pvs)}", pvs)):
        counter = MonitorCounter(targets)
        subs = [ctxt.monitor(pv, partial(lambda pv, v: counter(pv, float(v)), pv)) for pv in targets]
        assert counter.ready.wait(5)
        proc = run_storm(targets, puts, env)
        out, _ = proc.communicate()
        assert counter.done.wait(60), (counter.count, counter.pending)
        for s in subs:
            s.close()
        r[label] = counter.result(out)
    ctxt.close()
    return r


def bench_repics_asyncio(pvs: list[str], wf: str, reads: int, puts: int, env: dict[str, str]) -> dict:
    import asyncio

    from repics.pva.asyncio import Context

    async def atimed(fn, reps):
        out = []
        for _ in range(reps):
            t = time.perf_counter_ns()
            await fn()
            out.append(time.perf_counter_ns() - t)
        return stats(out)

    async def main():
        one = pvs[0]
        ctxt = Context()
        for pv in pvs + [wf]:
            await ctxt.connect(pv)
        r = {}
        r["get"] = await atimed(lambda: ctxt.get(one), reads)
        r["get array"] = await atimed(lambda: ctxt.get(wf), max(1, reads // 10))
        r["get list"] = await atimed(lambda: ctxt.get(pvs), max(1, reads // 20))
        r["put wait"] = await atimed(lambda: ctxt.put(one, 1.0, wait=True), reads)
        t = time.perf_counter_ns()
        for i in range(reads * 10):
            await ctxt.put(one, float(i), wait=False)
        r["put"] = {"rate": reads * 10 / ((time.perf_counter_ns() - t) / 1e9)}
        for label, targets in (("monitor", [one]), (f"monitor {len(pvs)}", pvs)):
            counter = MonitorCounter(targets)
            subs = [ctxt.monitor(pv, partial(lambda pv, v: counter(pv, float(v)), pv)) for pv in targets]
            await asyncio.wait_for(asyncio.to_thread(counter.ready.wait, 5), 6)
            proc = run_storm(targets, puts, env)
            out = await asyncio.to_thread(lambda: proc.communicate()[0])
            await asyncio.wait_for(asyncio.to_thread(counter.done.wait, 60), 61)
            for s in subs:
                s.close()
            r[label] = counter.result(out)
        ctxt.close()
        return r

    return asyncio.run(main())


def bench_p4p(pvs: list[str], wf: str, reads: int, puts: int, env: dict[str, str]) -> dict:
    from p4p.client.thread import Context

    one = pvs[0]
    ctxt = Context("pva")
    ctxt.get(pvs + [wf])  # connect everything first
    r = {}
    r["get"] = timed(lambda: ctxt.get(one), reads)
    r["get array"] = timed(lambda: ctxt.get(wf), max(1, reads // 10))
    r["get list"] = timed(lambda: ctxt.get(pvs), max(1, reads // 20))
    r["put wait"] = timed(lambda: ctxt.put(one, 1.0, wait=True), reads)
    t = time.perf_counter_ns()
    for i in range(reads * 10):
        ctxt.put(one, float(i), wait=False)
    r["put"] = {"rate": reads * 10 / ((time.perf_counter_ns() - t) / 1e9)}
    for label, targets in (("monitor", [one]), (f"monitor {len(pvs)}", pvs)):
        counter = MonitorCounter(targets)
        subs = [ctxt.monitor(pv, partial(lambda pv, v: counter(pv, float(v)), pv)) for pv in targets]
        assert counter.ready.wait(5)
        proc = run_storm(targets, puts, env)
        out, _ = proc.communicate()
        assert counter.done.wait(60), (counter.count, counter.pending)
        for s in subs:
            s.close()
        r[label] = counter.result(out)
    ctxt.close()
    return r


def bench_p4p_asyncio(pvs: list[str], wf: str, reads: int, puts: int, env: dict[str, str]) -> dict:
    import asyncio

    from p4p.client.asyncio import Context

    async def atimed(fn, reps):
        out = []
        for _ in range(reps):
            t = time.perf_counter_ns()
            await fn()
            out.append(time.perf_counter_ns() - t)
        return stats(out)

    async def main():
        one = pvs[0]
        ctxt = Context("pva")
        await ctxt.get(pvs + [wf])
        r = {}
        r["get"] = await atimed(lambda: ctxt.get(one), reads)
        r["get array"] = await atimed(lambda: ctxt.get(wf), max(1, reads // 10))
        r["get list"] = await atimed(lambda: ctxt.get(pvs), max(1, reads // 20))
        r["put wait"] = await atimed(lambda: ctxt.put(one, 1.0, wait=True), reads)
        t = time.perf_counter_ns()
        for i in range(reads * 10):
            await ctxt.put(one, float(i), wait=False)
        r["put"] = {"rate": reads * 10 / ((time.perf_counter_ns() - t) / 1e9)}
        for label, targets in (("monitor", [one]), (f"monitor {len(pvs)}", pvs)):
            counter = MonitorCounter(targets)

            def cb_for(pv):
                async def cb(v):
                    counter(pv, float(v))

                return cb

            subs = [ctxt.monitor(pv, cb_for(pv)) for pv in targets]
            await asyncio.wait_for(asyncio.to_thread(counter.ready.wait, 5), 6)
            proc = run_storm(targets, puts, env)
            out = await asyncio.to_thread(lambda: proc.communicate()[0])
            await asyncio.wait_for(asyncio.to_thread(counter.done.wait, 60), 61)
            for s in subs:
                s.close()
            r[label] = counter.result(out)
        ctxt.close()
        return r

    return asyncio.run(main())


LIBS = {
    "repics": bench_repics,
    "repics.asyncio": bench_repics_asyncio,
    "p4p": bench_p4p,
    "p4p.asyncio": bench_p4p_asyncio,
}


# ---------------------------------------------------------------------------
# the server-row probe (``--probe``): the p4p thread client


def probe(prefix: str, n: int, reads: int) -> dict:
    from p4p.client.thread import Context

    pvs, _, ctrl, st = names(prefix, n)
    one = pvs[0]
    ctxt = Context("pva")
    ctxt.get(pvs + [ctrl, st])
    r = {}
    r["get"] = timed(lambda: ctxt.get(one), reads)
    r["put wait"] = timed(lambda: ctxt.put(one, 1.0, wait=True), reads)
    for label, targets in (("monitor", [one]), (f"monitor {len(pvs)}", pvs)):
        counter = MonitorCounter(targets)
        subs = [ctxt.monitor(pv, partial(lambda pv, v: counter(pv, float(v)), pv)) for pv in targets]
        assert counter.ready.wait(5)
        ctxt.put(st, "", wait=True)
        ctxt.put(ctrl, len(targets), wait=True)  # start the storm over the first len(targets) PVs
        assert counter.done.wait(60), (counter.count, counter.pending)
        for s in subs:
            s.close()
        deadline = time.monotonic() + 5
        while True:
            sent = ctxt.get(st).raw["value"]  # str() of a p4p ntstr is "<ctime> <repr>"
            if sent:
                break
            assert time.monotonic() < deadline, "no storm statistics"
            time.sleep(0.01)
        row = counter.result(sent)
        row["server_cpu_us_per_post"] = json.loads(sent)["cpu_s"] / row["sent"] * 1e6
        r[label] = row
    ctxt.close()
    return r


# ---------------------------------------------------------------------------
# driver


def fmt_server(row: dict | None) -> str:
    if row is not None and "server_cpu_us_per_post" in row:
        return (
            f"{row['rate']:7.0f} ev/s {row['server_cpu_us_per_post']:5.1f} us srv cpu/post "
            f"({row['callbacks']}/{row['sent']})"
        )
    return fmt(row)


def table(title: str, results: dict[str, dict], cell) -> None:
    cols = list(results)
    rows: list[str] = []
    for r in results.values():
        for k in r:
            if k not in rows:
                rows.append(k)
    if not rows:
        return
    w = max(len(k) for k in rows) + 2
    print(f"\n{title}")
    print(f"{'':{w}}" + "".join(f"{c:>52}" for c in cols))
    for k in rows:
        print(f"{k:{w}}" + "".join(f"{cell(results[c].get(k)):>52}" for c in cols))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pin", help="CPUs for the servers and every client, e.g. 2,3,4,5")
    ap.add_argument("--hot", action="store_true", help="keep the SMT siblings of --pin busy")
    ap.add_argument("--pvs", type=int, default=100)
    ap.add_argument("--reads", type=int, default=2000)
    ap.add_argument("--puts", type=int, default=STORM_PUTS, help="posts per monitor storm")
    ap.add_argument("--libs", default=",".join(LIBS), help="client rows; '' for none")
    ap.add_argument("--servers", default=",".join(SERVERS), help="server rows; '' for none")
    ap.add_argument("--lib", help=argparse.SUPPRESS)
    ap.add_argument("--serve", help=argparse.SUPPRESS)
    ap.add_argument("--probe", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--prefix", help=argparse.SUPPRESS)
    ap.add_argument("--storm", nargs="*", help=argparse.SUPPRESS)
    a = ap.parse_args()

    if a.storm:
        storm(a.storm, a.puts)
        return
    if a.serve:
        SERVERS[a.serve](a.prefix, a.pvs, a.puts)
        return
    if a.probe:
        print(json.dumps(probe(a.prefix, a.pvs, a.reads)))
        return
    if a.lib:
        pvs, wf, _, _ = names(a.prefix, a.pvs)
        print(json.dumps(LIBS[a.lib](pvs, wf, a.reads, a.puts, dict(os.environ))))
        return

    cpus = sorted(int(c) for c in a.pin.split(",")) if a.pin else sorted(os.sched_getaffinity(0))
    if a.pin:
        os.sched_setaffinity(0, cpus)  # inherited by the servers and the clients
    spinners = spin_siblings(cpus) if a.hot else []
    prefix = f"bench{os.getpid()}:"
    common = ["--prefix", prefix, "--pvs", str(a.pvs), "--reads", str(a.reads), "--puts", str(a.puts)]
    clients: dict[str, dict] = {}
    servers: dict[str, dict] = {}
    try:
        if a.libs:
            server, conf = spawn_server("repics", prefix, a.pvs, a.puts)
            env = client_env(conf)
            try:
                for lib in a.libs.split(","):
                    p = subprocess.run([sys.executable, __file__, "--lib", lib, *common], env=env, stdout=subprocess.PIPE, text=True)
                    if p.returncode != 0:
                        print(f"{lib}: exited {p.returncode}", file=sys.stderr)
                        continue
                    clients[lib] = json.loads(p.stdout.splitlines()[-1])
            finally:
                stop_server(server)
        for kind in a.servers.split(",") if a.servers else []:
            server, conf = spawn_server(kind, prefix, a.pvs, a.puts)
            try:
                p = subprocess.run([sys.executable, __file__, "--probe", *common], env=client_env(conf), stdout=subprocess.PIPE, text=True)
            finally:
                stop_server(server)
            if p.returncode != 0:
                print(f"{kind} server: probe exited {p.returncode}", file=sys.stderr)
                continue
            servers[kind] = json.loads(p.stdout.splitlines()[-1])
        clock = cpu_clock(cpus)
    finally:
        for sp in spinners:
            sp.kill()

    table("client (against an repics thread SharedPV server)", clients, fmt)
    table(f"server (measured by the {PROBE_CLIENT} thread client)", servers, fmt_server)
    print(f"\nmedian / p99 latency; {a.pvs} PVs; {a.reads} reads; array {WF_LEN} doubles; storm {a.puts} puts")
    print(clock)


if __name__ == "__main__":
    main()
