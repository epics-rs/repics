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

## Building

```sh
pip install maturin
maturin develop            # into the active environment
pytest                     # needs `softioc-rs` on PATH or EPICSRS_SOFTIOC=<path>
```

`softioc-rs` is `cargo install epics-ca-rs --bin softioc-rs`.

## Status

Channel Access client, blocking and asyncio. pvAccess client and server
follow.
