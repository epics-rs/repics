# repics

EPICS Channel Access and pvAccess for Python, built on
[epics-rs](https://github.com/epics-rs/epics-rs). No libca, no libpvxs: the
protocol stacks are the Rust crates, compiled into one extension module.

```python
from repics import ca

ca.caput("SIM:ao", 2.5)
v = ca.caget("SIM:ai", form="ctrl")
print(v, v.units, v.severity, v.timestamp)

with ca.camonitor("SIM:cnt", print):
    ...
```

```python
from repics.ca.asyncio import caget, camonitor

async def main():
    v = await caget(["SIM:ai", "SIM:long"])
    sub = await camonitor("SIM:cnt", print)
```

Every read returns an augmented value: a `float`/`int`/`str`/`numpy.ndarray`
subclass carrying `name`, `status`, `severity`, `timestamp`, `raw_stamp`, and
for `form="ctrl"` the units, precision, limits and enum strings.

## Channel Access

`caget`, `caput`, `camonitor`, `cainfo` and `connect` take one PV name or a
sequence and return a result of the same shape; a sequence is worked on
concurrently, connect included, against one `timeout`. A failure raises
`CaError` (`CaTimeout`, `CaDisconnected`); with `throw=False` it is returned
instead as a `CaNothing`, whose `ok` is False and whose `errorcode` is the
libca `ECA_*` status. `cainfo` returns a `CAInfo`.

* `form` picks the metadata class: `"plain"`, `"sts"`, `"time"` (default),
  `"gr"`, `"ctrl"`.
* `datatype` overrides the wire type: `str`, `int`, `float`, a numpy dtype, a
  `DBR_*` code, or `DBR_CHAR_STR` / `DBR_CHAR_BYTES` / `DBR_CHAR_UNICODE` to
  read a char waveform as text, `DBR_ENUM_STR` to read an enum as its label.
* `count` is 0 for the server's current element count, negative for the
  full native count, else a cap.
* `camonitor(pv, callback, ...)` returns a `Subscription` at once and
  connects in the background; `connect_timeout` reports a `CaNothing` with
  `ECA_TIMEOUT` if it passes, and the monitor keeps waiting. Callbacks run
  on one dispatcher thread (`ca`) or one task per event loop (`ca.asyncio`), in
  arrival order. With `all_updates=False` (default) updates that queued
  while the callback ran collapse into the latest and `dropped_callbacks`
  counts them. `notify_disconnect=True` delivers a `CaNothing` with
  `ECA_DISCONN` on disconnect; the monitor resumes on reconnection.
* `repics.pv.PV` is a pyepics-shaped object (`get`, `put`, `add_callback`,
  `auto_monitor`, `char_value`, `units`, ...) over one subscription; it
  raises on failure like the rest of the package.

## pvAccess

The API follows [p4p](https://github.com/epics-base/p4p): `Context`,
`Value`/`Type`, the `nt` wrappers, and `SharedPV`/`StaticProvider`/`Server`.

```python
from repics.pva import Context
from repics.pva.nt import NTScalar, NTURI

with Context("pva") as ctxt:
    v = ctxt.get("SIM:ai")            # augmented, as for CA
    ctxt.put("SIM:ao", 2.5)
    r = ctxt.rpc("SIM:sum", NTURI([("a", "d"), ("b", "d")]).wrap("SIM:sum", kws={"a": 1, "b": 2}))
    with ctxt.monitor("SIM:cnt", print, notify_disconnect=True):
        ...
```

```python
from repics.pva.server import SharedPV, Server

pv = SharedPV(nt=NTScalar("d"), initial=1.0)

@pv.put
def onput(pv, op):
    pv.post(op.value())
    op.done()

Server.forever(providers=[{"SIM:ao": pv}])
```

`repics.pva.asyncio.Context` and `repics.pva.server.asyncio.SharedPV` are
the asyncio flavours; handlers there may be coroutines. Put and RPC handlers
run on Python-owned threads (or the event loop), never on the network runtime,
and a slow monitor consumer squashes updates in Rust instead of queueing
Python objects. NTNDArray reads are zero-copy views of the received buffer.

## Performance

The extension owns one tokio runtime with one worker thread (override with
`REPICS_WORKERS`); every blocking call releases the GIL and waits for its
future, every `ca.asyncio` call returns a future that is already done when the
answer is in hand. Metadata is attached to a value lazily, list operations
run in Rust as one concurrent batch, and all monitors of a front end feed
one bounded queue that is drained in batches, so a callback costs one queue
pop rather than one thread wake-up.

`bench/bench_ca.py` runs repics, pyepics and aioca in separate processes
against the same `softioc-rs`. On a Xeon Gold 6542Y, pinned to four cores
with the SMT siblings kept busy so the cores hold their clock
(`--pin 2,3,4,5 --hot`; unpinned, `schedutil` parks the ping-ponging
threads at 800 MHz and every library halves):

| | repics | repics.ca.asyncio | pyepics | aioca |
|---|---|---|---|---|
| get, median / p99 | 35 / 42 us | 72 / 103 us | 85 / 107 us | 59 / 69 us |
| get 10 000-double waveform | 166 / 183 us | 161 / 179 us | 182 / 269 us | 109 / 171 us |
| get list of 100 PVs, one call | 1167 / 1201 us | 932 / 976 us | 3284 / 3369 us | 3043 / 3633 us |
| put wait=True | 39 / 44 us | 56 / 63 us | 165 / 180 us | 66 / 77 us |
| put wait=False | 160 439 /s | 144 256 /s | 13 704 /s | 83 536 /s |
| monitor, 1 PV, CPU per callback | 8.3 us | 17.6 us | 32.2 us | 24.2 us |
| monitor, 100 PVs, CPU per callback | 10.3 us | 22.2 us | 19.9 us | 23.2 us |

Monitor rows are under a 20 000-put storm from a separate writer process;
every client received 20 099–20 100 of the 20 100 events on the 100-PV row.

`bench/bench_pva.py` does the same for pvAccess. Client rows run each
library in its own process against one repics thread `SharedPV` server;
server rows run each server flavour in its own process, measured by the
p4p thread client. Same machine, same pinning:

| client | repics | repics.asyncio | p4p | p4p.asyncio |
|---|---|---|---|---|
| get, median / p99 | 47 / 54 us | 69 / 92 us | 99 / 131 us | 99 / 132 us |
| get 10 000-double array | 90 / 101 us | 105 / 136 us | 122 / 184 us | 122 / 143 us |
| get list of 100 PVs, one call | 2235 / 3628 us | 4108 / 6173 us | 3980 / 4349 us | 4630 / 5110 us |
| put wait=True | 145 / 158 us | 194 / 238 us | 150 / 186 us | 156 / 198 us |
| put wait=False | 7111 /s | 5145 /s | 6506 /s | 6367 /s |
| monitor, 1 PV, CPU per callback | 30.6 us | 61.7 us | 34.1 us | 41.3 us |
| monitor, 100 PVs, CPU per callback | 34.6 us | 71.3 us | 54.9 us | 59.3 us |

| server | repics | repics.asyncio | p4p | p4p.asyncio |
|---|---|---|---|---|
| get, median / p99 | 94 / 115 us | 99 / 128 us | 99 / 133 us | 94 / 116 us |
| put wait=True, through the put handler | 147 / 170 us | 187 / 239 us | 153 / 189 us | 148 / 177 us |
| post storm, 1 PV: delivered of 20 001, server CPU per post | 15 842, 13.4 us | 17 161, 13.8 us | 8 673, 5.9 us | 8 318, 5.7 us |
| post storm, 100 PVs: delivered of 20 100, server CPU per post | 18 390, 12.6 us | 19 571, 12.5 us | 20 100, 6.7 us | 20 098, 6.6 us |

pvAccess has no fire-and-forget put: every put is a completed round trip,
and with the default `get=True` the current value is read first. repics
and pvxs both do this as one two-phase put operation, the readback riding
the put's own channel op, so the put rows are on par. The client monitor
rows are under a storm from a separate repics writer running four threads
of `put(wait=True)`, about 14 000 puts/s, and every client received
19 857 or more of the 20 001 events, so the CPU column is the comparison.
The server storm is one thread posting as fast as `post()` returns; the
repics server delivers about twice as many posts as pvxs and spends
about 13 us of CPU per post, delivery included, where pvxs spends about
6 us and, on one PV, squashes away more than half of them. Medians of the
blocking repics client moved between 44 and 79 us on `get` across runs
of the same command (thread placement inside the four cores); each table
is one run.

Reference documentation: [docs/index.md](docs/index.md).

## Building

```sh
pip install maturin
maturin develop            # into the active environment
pytest                     # needs `softioc-rs` on PATH or REPICS_SOFTIOC=<path>
```

`softioc-rs` is `cargo install epics-ca-rs --bin softioc-rs`.

## Status

Channel Access client, blocking and asyncio. pvAccess client and server,
blocking and asyncio; tested against p4p on both sides of the wire
(`pytest tests/pva` needs `p4p` installed).
