# Migrating from pyepics, aioca and p4p

The front ends keep the calling conventions of those three libraries where
the code implements them. This page lists the names that map across and
the differences the code has. It is not a description of the other
libraries.

## From pyepics

| pyepics | repics |
| --- | --- |
| `import epics` | `from repics import pv as epics` (the `PV` class) or `from repics import ca` (the functions) |
| `epics.PV(...)` | `repics.pv.PV(pvname, callback=None, form='time', auto_monitor=None, connection_callback=None, connection_timeout=None, access_callback=None, count=None)` |
| `epics.get_pv(...)` | `repics.pv.get_pv(pvname, form='time', connect=False, timeout=5.0, **kw)`; the cache is keyed by `(pvname, form)` and holds only PVs made through `get_pv` |
| `epics.caget(...)` | `repics.ca.caget(pv, form='time', datatype=None, count=0, timeout=5.0, throw=True)`; returns an augmented value, not a bare Python number |
| `epics.caput(...)` | `repics.ca.caput(pv, value, wait=False, datatype=None, timeout=5.0, repeat_value=False, throw=True)` |
| `epics.camonitor(...)` | `repics.ca.camonitor(pv, callback, ...)` returns a subscription object with `close()`; there is no `camonitor_clear` |
| `epics.cainfo(...)` | `repics.ca.cainfo(pv, wait=True, timeout=5.0, throw=True)` returns a `CAInfo` object instead of printing |
| `epics.ca.*` | `repics.CaContext` and `repics.CaChannel` (see [ca.md](ca.md)); the pyepics `ca` module functions do not exist |

`PV` behaviours to check against pyepics code:

| Topic | repics `PV` |
| --- | --- |
| `auto_monitor` | `None` monitors when the element count is at most `AUTOMONITOR_MAXLENGTH` (65536); `True`/`False` force it; an int is a DBE mask |
| `get(use_monitor=True)` | returns the cached monitor value when monitoring; otherwise performs a `caget` in the PV's form. A failure raises; there is no `None` return |
| `put(wait=False, timeout=30.0, use_complete=False, callback=None)` | a `callback` or `use_complete` without `wait` runs the put on a thread named `caput <pvname>`; `put_complete` goes `False` then `True` |
| `disconnect()` | closes the subscription only; the channel stays connected and `connected` stays `True` |
| `wait_for_connection(timeout=None)` | uses `connection_timeout`, else 5.0 s |
| `add_callback(callback, index=None, run_now=False, with_ctrlvars=True, **kw)` | callbacks receive `pvname=`, `value=`, `char_value=`, `cb_info=(index, pv)` and the metadata keywords listed in [pv.md](pv.md) |
| `get_ctrlvars()`, `get_timevars()` | one CA read each, with a `timeout` argument |
| `access` | one of `no access`, `read-only`, `write-only`, `read/write` |
| `info` | a text block; see [pv.md](pv.md) |

```python
from repics import pv as epics      # pyepics: import epics
p = epics.PV("demo:ai")
print(p.get(), p.units, p.value, epics.get_pv("demo:ai") is p)
```

```
1.5 mm 1.5 False
```

## From aioca

| aioca | repics |
| --- | --- |
| `from aioca import caget, caput, camonitor, cainfo, connect` | `from repics.aio import caget, caput, camonitor, cainfo, connect` |
| `from aioca import CANothing, CAInfo` | `from repics import CaNothing, CAInfo` |
| `FORMAT_RAW`, `FORMAT_TIME`, `FORMAT_CTRL` | the strings `'raw'`, `'time'`, `'ctrl'` in `form=` |
| `DBR_*`, `DBE_*`, `ECA_*` | the same names, exported from `repics` |
| `caput(..., wait=True)` | the same; `caput` returns `CaNothing(pv)` on success and raises `CaError` (or returns it with `throw=False`) on failure |
| `camonitor(pv, cb, ...)` | the same signature; the callback may be a coroutine function. Delivery is one update per callback call. `all_updates=False` (the default) coalesces to the newest value when the callback lags |
| `caget(pvs)` with a list | the same; results are a list in the same order |
| `Subscription.close()` | the same; `dropped_callbacks` counts coalesced updates |

The error hierarchy is `repics.CaError` with `CaTimeout` and
`CaDisconnected` beneath it; `CaNothing` is falsy and carries `errorcode`.

```python
import asyncio
from repics.aio import caget, caput, camonitor   # aioca: from aioca import ...
async def main():
    await caput("demo:long", 3, wait=True)
    v = await caget("demo:long")
    print(v, v.ok, v.severity)
    sub = camonitor("demo:long", lambda v: print("got", v))
    await asyncio.sleep(0.2)
    sub.close()
asyncio.run(main())
```

```
3 True 0
got 3
```

In 4 of 13 runs of this script the process also printed, after the two
lines above and while exiting, a Rust panic from the runtime thread:

```
thread 'repics-rt' (3606484) panicked at .../pyo3-0.27.2/src/interpreter_lifecycle.rs:117:13:
assertion `left != right` failed: The Python interpreter is not initialized and the `auto-initialize` feature is not enabled.
```

The exit status stayed 0. The runtime worker touched a Python object
after the interpreter had finalized; the same sequence without the
`camonitor` (the `repics.aio` example in [aio.md](aio.md)) did not show
it in 4 runs.

## From p4p

| p4p | repics |
| --- | --- |
| `from p4p.client.thread import Context` | `from repics.pva import Context` |
| `from p4p.client.asyncio import Context` | `from repics.pva.asyncio import Context` |
| `from p4p import Value, Type` | `from repics.pva import Value, Type` |
| `from p4p.nt import NTScalar, ...` | `from repics.pva.nt import NTScalar, ...` |
| `from p4p.server import Server, StaticProvider` | `from repics.pva.server import Server, StaticProvider` |
| `from p4p.server.thread import SharedPV` | `from repics.pva.server import SharedPV` |
| `from p4p.server.asyncio import SharedPV` | `from repics.pva.server.asyncio import SharedPV` |
| `from p4p.client.thread import Disconnected, RemoteError, TimeoutError` | `from repics.pva import Disconnected, RemoteError, TimeoutError` (aliases of `PvaDisconnected`, `PvaRemoteError`, `PvaTimeout`) |
| `Context('pva', conf=..., useenv=..., nt=..., unwrap=...)` | the same arguments; `provider` must be `'pva'` |
| `Context.get/put/rpc/info/connect/monitor` | the same signatures (see [pva-client.md](pva-client.md)) |
| `Server(providers, isolate=, conf=, useenv=)` | the same; `Server.forever()` exists |
| `SharedPV(handler=, initial=, nt=, wrap=, unwrap=, queue=)` | the same; `open`, `post`, `close(destroy=, sync=, timeout=)`, `current`, `isOpen`, the four decorators |
| `Value` | `Type(spec, id=None)` accepts a member list or a `Type` at top level; `Type(T.aspy())` is not accepted (see [values.md](values.md)) |

Differences a p4p program may notice:

| Topic | repics |
| --- | --- |
| Reads inside `Value` | scalar arrays are read-only numpy views of the wire buffer; string arrays are lists; nested structures are views; unions, variants and structure arrays give detached copies |
| `Value.mark()` at the root | leaves `changedSet()` empty; `changed()` is `True` and `changedSet(expand=True)` lists every leaf |
| Monitor `Finished` | delivered only when the server ends the subscription, never for a client-side `close()` |
| Monitor `queue` | blocking flavour only; the item pushed is `functools.partial(cb, value)` |
| RPC handler `op` | the raw operation: `op.value()` is a `Value`, `op.done(value=)` needs a `Value` |
| `Server.conf()` | seven keys; `EPICS_PVA_ADDR_LIST` is `127.0.0.1:<udp port>` for an isolated server |
| Threads | the handler pool is four `WorkQueue` threads created lazily; the monitor dispatcher is one thread named `repics pvmonitor` |
| Configuration | `useenv=False` still leaves crate-level settings (`EPICS_PVA_CONN_TMO`, `EPICS_PVA_AUTH_USER`, TLS) to the environment; see [config.md](config.md) |

```python
from repics.pva import Context          # p4p: from p4p.client.thread import Context
from repics.pva.nt import NTScalar      # p4p: from p4p.nt import NTScalar
from repics.pva.server import Server, StaticProvider, SharedPV   # p4p.server / p4p.server.thread
pv = SharedPV(nt=NTScalar("d"), initial=1.0)
prov = StaticProvider("p"); prov.add("demo:pva:mig", pv)
with Server([prov], isolate=True) as S, Context("pva", conf=S.conf(), useenv=False) as ctxt:
    v = ctxt.get("demo:pva:mig")
    print(v, v.severity, v.timestamp, type(v).__name__)
```

```
1.0 0 0.0 AugmentedFloat
```
