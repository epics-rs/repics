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
