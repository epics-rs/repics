# epicsrs

EPICS Channel Access and pvAccess for Python, built on
[epics-rs](https://github.com/epics-rs/epics-rs). No libca, no libpvxs: the
protocol stacks are the Rust crates, compiled into one extension module.

```python
from epicsrs import ca

ca.caput("SIM:ao", 2.5)
v = ca.caget("SIM:ai", form="ctrl")
print(v, v.units, v.severity, v.timestamp)

with ca.camonitor("SIM:cnt", print):
    ...
```

```python
from epicsrs import aio

async def main():
    v = await aio.caget(["SIM:ai", "SIM:long"])
    sub = await aio.camonitor("SIM:cnt", print)
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
  on one dispatcher thread (`ca`) or one task per event loop (`aio`), in
  arrival order. With `all_updates=False` (default) updates that queued
  while the callback ran collapse into the latest and `dropped_callbacks`
  counts them. `notify_disconnect=True` delivers a `CaNothing` with
  `ECA_DISCONN` on disconnect; the monitor resumes on reconnection.
* `epicsrs.pv.PV` is a pyepics-shaped object (`get`, `put`, `add_callback`,
  `auto_monitor`, `char_value`, `units`, ...) over one subscription; it
  raises on failure like the rest of the package.

## pvAccess

The API follows [p4p](https://github.com/epics-base/p4p): `Context`,
`Value`/`Type`, the `nt` wrappers, and `SharedPV`/`StaticProvider`/`Server`.

```python
from epicsrs.pva import Context
from epicsrs.pva.nt import NTScalar, NTURI

with Context("pva") as ctxt:
    v = ctxt.get("SIM:ai")            # augmented, as for CA
    ctxt.put("SIM:ao", 2.5)
    r = ctxt.rpc("SIM:sum", NTURI([("a", "d"), ("b", "d")]).wrap("SIM:sum", kws={"a": 1, "b": 2}))
    with ctxt.monitor("SIM:cnt", print, notify_disconnect=True):
        ...
```

```python
from epicsrs.pva.server import SharedPV, Server

pv = SharedPV(nt=NTScalar("d"), initial=1.0)

@pv.put
def onput(pv, op):
    pv.post(op.value())
    op.done()

Server.forever(providers=[{"SIM:ao": pv}])
```

`epicsrs.pva.asyncio.Context` and `epicsrs.pva.server.asyncio.SharedPV` are
the asyncio flavours; handlers there may be coroutines. Put and RPC handlers
run on Python-owned threads (or the event loop), never on the network runtime,
and a slow monitor consumer squashes updates in Rust instead of queueing
Python objects. NTNDArray reads are zero-copy views of the received buffer.

## Performance

The extension owns one tokio runtime with one worker thread (override with
`EPICSRS_WORKERS`); every blocking call releases the GIL and waits for its
future, every `aio` call returns a future that is already done when the
answer is in hand. Metadata is attached to a value lazily, list operations
run in Rust as one concurrent batch, and all monitors of a front end feed
one bounded queue that is drained in batches, so a callback costs one queue
pop rather than one thread wake-up.

`bench/bench_ca.py` runs epicsrs, pyepics and aioca in separate processes
against the same `softioc-rs`. On a Xeon Gold 6542Y, pinned to four cores
with the SMT siblings kept busy so the cores hold their clock
(`--pin 2,3,4,5 --hot`; unpinned, `schedutil` parks the ping-ponging
threads at 800 MHz and every library halves):

| | epicsrs | epicsrs.aio | pyepics | aioca |
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
library in its own process against one epicsrs thread `SharedPV` server;
server rows run each server flavour in its own process, measured by the
p4p thread client. Same machine, same pinning:

| client | epicsrs | epicsrs.asyncio | p4p | p4p.asyncio |
|---|---|---|---|---|
| get, median / p99 | 54 / 60 us | 67 / 92 us | 95 / 114 us | 98 / 130 us |
| get 10 000-double array | 103 / 117 us | 104 / 115 us | 121 / 154 us | 123 / 166 us |
| get list of 100 PVs, one call | 3217 / 5093 us | 4136 / 6561 us | 4172 / 4403 us | 4631 / 5098 us |
| put wait=True | 186 / 201 us | 222 / 253 us | 154 / 182 us | 167 / 201 us |
| put wait=False | 5725 /s | 4631 /s | 6423 /s | 6003 /s |
| monitor, 1 PV, CPU per callback | 32.8 us | 62.0 us | 34.7 us | 41.8 us |
| monitor, 100 PVs, CPU per callback | 35.0 us | 69.0 us | 52.9 us | 57.4 us |

| server | epicsrs | epicsrs.asyncio | p4p | p4p.asyncio |
|---|---|---|---|---|
| get, median / p99 | 94 / 116 us | 94 / 117 us | 93 / 117 us | 94 / 116 us |
| put wait=True, through the put handler | 154 / 181 us | 194 / 230 us | 146 / 175 us | 150 / 179 us |
| post storm, 1 PV: delivered of 20 001, server CPU per post | 19 605, 28.3 us | 19 770, 27.4 us | 8 242, 5.5 us | 8 229, 5.5 us |
| post storm, 100 PVs: delivered of 20 100, server CPU per post | 19 975, 24.9 us | 19 895, 24.8 us | 19 996, 6.6 us | 11 279, 6.7 us |

pvAccess has no fire-and-forget put: every put is a completed round trip,
and with the default `get=True` the current value is read first, which
epicsrs does as a separate get and pvxs inside the put operation; that is
the difference on the put rows. The client monitor rows are under a storm
from a separate epicsrs writer running four threads of `put(wait=True)`,
about 14 000 puts/s, and every client received 19 890 or more of the
20 001 events, so the CPU column is the comparison. The server storm is
one thread posting as fast as `post()` returns; the epicsrs server
delivers nearly every post and spends 25–28 us of CPU per post on it,
delivery included, where pvxs spends 5.5–6.7 us and, on one PV, squashes
away more than half of them. Medians of the blocking epicsrs client moved between 44 and 79 us
on `get` across runs of the same command (thread placement inside the
four cores); each table is one run.

## Building

```sh
pip install maturin
maturin develop            # into the active environment
pytest                     # needs `softioc-rs` on PATH or EPICSRS_SOFTIOC=<path>
```

`softioc-rs` is `cargo install epics-ca-rs --bin softioc-rs`.

## Status

Channel Access client, blocking and asyncio. pvAccess client and server,
blocking and asyncio; tested against p4p on both sides of the wire
(`pytest tests/pva` needs `p4p` installed).
