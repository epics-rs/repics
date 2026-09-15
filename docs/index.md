# repics reference

`repics` is a Python package for EPICS Channel Access (CA) and pvAccess (PVA).
The network code is the `epics-ca-rs` and `epics-pva-rs` Rust crates, compiled
into one extension module, `repics._repics` (pyo3, abi3, Python 3.10 or
newer). The Python layer on top gives the three CA front ends, the PVA client
and server, and the normative-type helpers.

`repics.__version__` is `'0.1.0'`.

## Pages

| Page | Covers |
| --- | --- |
| [ca.md](ca.md) | `repics.ca`: blocking `caget`, `caput`, `camonitor`, `cainfo`, `connect`; the DBR, DBE and ECA constants; the low-level `CaContext` and `CaChannel` extension classes |
| [ca-asyncio.md](ca-asyncio.md) | `repics.ca.asyncio`: the same functions as coroutines for asyncio |
| [pv.md](pv.md) | `repics.pv`: the `PV` class and `get_pv` |
| [pva-client.md](pva-client.md) | `repics.pva.Context` and `repics.pva.asyncio.Context`: get, put, rpc, info, connect, monitor |
| [pva-server.md](pva-server.md) | `repics.pva.server`: `Server`, `StaticProvider`, `SharedPV` (thread and asyncio flavours), `Handler`, `ServerOperation` |
| [nt.md](nt.md) | `repics.pva.nt`: `NTScalar`, `NTEnum`, `NTTable`, `NTNDArray`, `NTURI`, `timeStamp`, `alarm`, `defaultNT` |
| [values.md](values.md) | `repics.pva.Type` and `repics.pva.Value`: type specs, assignment, change marks, unions and variants |
| [config.md](config.md) | Every environment variable the package and the crates underneath it read |
| [migration.md](migration.md) | Coming from pyepics, aioca or p4p |

## Modules

| Module | What it is |
| --- | --- |
| `repics` | The package. Re-exports the extension classes and exceptions, `context()`, `get_channel_infos()`, `purge_channel_caches()`, and every name from the DBR constants module |
| `repics.ca` | Blocking CA client functions |
| `repics.ca.asyncio` | asyncio CA client functions |
| `repics.pv` | `PV` and `get_pv` |
| `repics.pva` | Thread-flavour PVA client `Context`, `Type`, `Value`, the PVA exceptions, and the `nt` submodule |
| `repics.pva.asyncio` | asyncio-flavour PVA client `Context` |
| `repics.pva.server` | Thread-flavour PVA server |
| `repics.pva.server.asyncio` | asyncio-flavour `SharedPV` |
| `repics.pva.nt` | Normative type helpers |
| `repics._repics` | The compiled extension. Not part of the documented surface except for the classes re-exported from `repics` and `repics.pva` |

Names exported from the top-level package:

```
ca, pv, pva, context, get_channel_infos, purge_channel_caches,
ChannelStatus, CaChannel, CaContext, CaDisconnected, CaError, CaEvents,
CaNothing, CaSubscription, CaTimeout, CAInfo, ChannelInfo, ConnectionEvent,
PvaDisconnected, PvaError, PvaRemoteError, PvaTimeout, Snapshot,
Augmented, AugmentedArray, AugmentedBytes, AugmentedFloat, AugmentedInt,
AugmentedList, AugmentedStr
```

plus the constants listed under [Constants](ca.md#constants).

## Runtime model

One tokio runtime serves every front end in the process. It starts on first
use, with `REPICS_WORKERS` worker threads named `repics-rt` (default 1,
values below 1 are raised to 1; see [config.md](config.md)).

Blocking calls (`repics.ca`, `repics.pv`, the thread-flavour PVA
`Context`, and every non-`_async` method of the extension classes) release
the GIL while they wait. Coroutine calls (`repics.ca.asyncio`, `repics.pva.asyncio`)
are futures bridged from the runtime into the calling asyncio loop.

No Python callback ever runs on a runtime worker. Each front end drains a
queue on a thread or task of its own:

| Front end | Callback context |
| --- | --- |
| `repics.ca.camonitor`, `repics.pv` | one daemon thread named `repics camonitor` |
| `repics.ca.asyncio.camonitor` | one task per asyncio loop |
| `repics.pva.Context.monitor` | one daemon thread named `repics pvmonitor`, or the `queue` you pass |
| `repics.pva.asyncio.Context.monitor` | one task per asyncio loop |
| thread-flavour `SharedPV` handlers | a daemon thread named `repics.pva.server` (or the `queue` you pass) |
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

`repics.pva` also exports the aliases `Disconnected`, `RemoteError` and
`TimeoutError` for the three PVA subclasses, plus `Finished` and `Cancelled`.

## How the examples were produced

Every code block in these pages was run as shown and its printed output
follows it verbatim.

The CA examples ran against `softioc-rs` serving `tests/ioc/test.db` with
the macro `P=demo:` on a private port, using the recipe in
`tests/conftest.py`: `EPICS_CAS_SERVER_PORT=<port>` and
`EPICS_CAS_INTF_ADDR_LIST=127.0.0.1` for the IOC, and
`EPICS_CA_ADDR_LIST=127.0.0.1:<port>`, `EPICS_CA_AUTO_ADDR_LIST=NO`,
`EPICS_CA_SERVER_PORT=<port>` for the client, set before `repics` was
imported. Host strings and timestamps in the output are whatever that run
produced.

The PVA examples start their own `Server(..., isolate=True)` and connect a
`Context("pva", conf=S.conf(), useenv=False)` to it, so they use no
well-known port and need no IOC.
