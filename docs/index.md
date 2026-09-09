# epicsrs reference

`epicsrs` is a Python package for EPICS Channel Access (CA) and pvAccess (PVA).
The network code is the `epics-ca-rs` and `epics-pva-rs` Rust crates, compiled
into one extension module, `epicsrs._epicsrs` (pyo3, abi3, Python 3.10 or
newer). The Python layer on top gives the three CA front ends, the PVA client
and server, and the normative-type helpers.

`epicsrs.__version__` is `'0.1.0'`.

## Pages

| Page | Covers |
| --- | --- |
| [ca.md](ca.md) | `epicsrs.ca`: blocking `caget`, `caput`, `camonitor`, `cainfo`, `connect`; the DBR, DBE and ECA constants; the low-level `CaContext` and `CaChannel` extension classes |
| [aio.md](aio.md) | `epicsrs.aio`: the same functions as coroutines for asyncio |
| [pv.md](pv.md) | `epicsrs.pv`: the `PV` class and `get_pv` |
| [pva-client.md](pva-client.md) | `epicsrs.pva.Context` and `epicsrs.pva.asyncio.Context`: get, put, rpc, info, connect, monitor |
| [pva-server.md](pva-server.md) | `epicsrs.pva.server`: `Server`, `StaticProvider`, `SharedPV` (thread and asyncio flavours), `Handler`, `ServerOperation` |
| [nt.md](nt.md) | `epicsrs.pva.nt`: `NTScalar`, `NTEnum`, `NTTable`, `NTNDArray`, `NTURI`, `timeStamp`, `alarm`, `defaultNT` |
| [values.md](values.md) | `epicsrs.pva.Type` and `epicsrs.pva.Value`: type specs, assignment, change marks, unions and variants |
| [config.md](config.md) | Every environment variable the package and the crates underneath it read |
| [migration.md](migration.md) | Coming from pyepics, aioca or p4p |

## Modules

| Module | What it is |
| --- | --- |
| `epicsrs` | The package. Re-exports the extension classes and exceptions, `context()`, `get_channel_infos()`, `purge_channel_caches()`, and every name from the DBR constants module |
| `epicsrs.ca` | Blocking CA client functions |
| `epicsrs.aio` | asyncio CA client functions |
| `epicsrs.pv` | `PV` and `get_pv` |
| `epicsrs.pva` | Thread-flavour PVA client `Context`, `Type`, `Value`, the PVA exceptions, and the `nt` submodule |
| `epicsrs.pva.asyncio` | asyncio-flavour PVA client `Context` |
| `epicsrs.pva.server` | Thread-flavour PVA server |
| `epicsrs.pva.server.asyncio` | asyncio-flavour `SharedPV` |
| `epicsrs.pva.nt` | Normative type helpers |
| `epicsrs._epicsrs` | The compiled extension. Not part of the documented surface except for the classes re-exported from `epicsrs` and `epicsrs.pva` |

Names exported from the top-level package:

```
aio, ca, pv, pva, context, get_channel_infos, purge_channel_caches,
ChannelStatus, CaChannel, CaContext, CaDisconnected, CaError, CaEvents,
CaNothing, CaSubscription, CaTimeout, CAInfo, ChannelInfo, ConnectionEvent,
PvaDisconnected, PvaError, PvaRemoteError, PvaTimeout, Snapshot,
Augmented, AugmentedArray, AugmentedBytes, AugmentedFloat, AugmentedInt,
AugmentedList, AugmentedStr
```

plus the constants listed under [Constants](ca.md#constants).

## Runtime model

One tokio runtime serves every front end in the process. It starts on first
use, with `EPICSRS_WORKERS` worker threads named `epicsrs-rt` (default 1,
values below 1 are raised to 1; see [config.md](config.md)).

Blocking calls (`epicsrs.ca`, `epicsrs.pv`, the thread-flavour PVA
`Context`, and every non-`_async` method of the extension classes) release
the GIL while they wait. Coroutine calls (`epicsrs.aio`, `epicsrs.pva.asyncio`)
are futures bridged from the runtime into the calling asyncio loop.

No Python callback ever runs on a runtime worker. Each front end drains a
queue on a thread or task of its own:

| Front end | Callback context |
| --- | --- |
| `epicsrs.ca.camonitor`, `epicsrs.pv` | one daemon thread named `epicsrs camonitor` |
| `epicsrs.aio.camonitor` | one task per asyncio loop |
| `epicsrs.pva.Context.monitor` | one daemon thread named `epicsrs pvmonitor`, or the `queue` you pass |
| `epicsrs.pva.asyncio.Context.monitor` | one task per asyncio loop |
| thread-flavour `SharedPV` handlers | a daemon thread named `epicsrs.pva.server` (or the `queue` you pass) |
| asyncio-flavour `SharedPV` handlers | tasks on the loop the PV was created in |

The CA front ends share one `CaContext`, built from the `EPICS_CA_*`
environment on first use, and one channel cache keyed by PV name. Channels
are never closed; `purge_channel_caches()` drops the cache entries. Set the
`EPICS_CA_*` variables before the first CA call.

## Errors

| Exception | Raised by |
| --- | --- |
| `CaError(message, eca_status)` | CA failures; `.status` is the ECA code |
| `CaTimeout` | a CA deadline expired (subclass of `CaError`) |
| `CaDisconnected` | the channel is not connected (subclass of `CaError`) |
| `CaNothing(name, errorcode)` | `throw=False` results and some monitor callbacks; a falsy `CaError` subclass |
| `PvaError` | PVA failures |
| `PvaTimeout` | a PVA deadline expired, including an unresolved name |
| `PvaDisconnected` | the channel disconnected while an operation was pending |
| `PvaRemoteError` | the server answered with an error |

`epicsrs.pva` also exports the aliases `Disconnected`, `RemoteError` and
`TimeoutError` for the three PVA subclasses, plus `Finished` and `Cancelled`.

## How the examples were produced

Every code block in these pages was run as shown and its printed output
follows it verbatim.

The CA examples ran against `softioc-rs` serving `tests/ioc/test.db` with
the macro `P=demo:` on a private port, using the recipe in
`tests/conftest.py`: `EPICS_CAS_SERVER_PORT=<port>` and
`EPICS_CAS_INTF_ADDR_LIST=127.0.0.1` for the IOC, and
`EPICS_CA_ADDR_LIST=127.0.0.1:<port>`, `EPICS_CA_AUTO_ADDR_LIST=NO`,
`EPICS_CA_SERVER_PORT=<port>` for the client, set before `epicsrs` was
imported. Host strings and timestamps in the output are whatever that run
produced.

The PVA examples start their own `Server(..., isolate=True)` and connect a
`Context("pva", conf=S.conf(), useenv=False)` to it, so they use no
well-known port and need no IOC.
