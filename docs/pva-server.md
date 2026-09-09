# PVA server

`epicsrs.pva.server` serves PVs over pvAccess. A `SharedPV` holds one
value and its handlers; a `StaticProvider` maps names to PVs; a `Server`
listens. Handlers run on a worker thread (`epicsrs.pva.server.SharedPV`)
or on an asyncio loop (`epicsrs.pva.server.asyncio.SharedPV`). The
network side is the `epics-pva-rs` crate; Python sees only put and rpc
requests and the connect edges.

```python
from epicsrs.pva.server import Server, StaticProvider, SharedPV, WorkQueue, Handler, ServerOperation
from epicsrs.pva.server.asyncio import SharedPV       # loop flavour; also re-exports Server, StaticProvider, Handler, ServerOperation
```

## Server

```python
class Server(providers, isolate=False, conf=None, useenv=True)
```

The server starts listening in the constructor and stops in `stop()` or
at the end of a `with` block.

| Argument | Meaning |
| --- | --- |
| `providers` | a `StaticProvider`, a dict `name -> SharedPV`, or a list of those; a list entry may be `(provider, order)` where a lower `order` is searched first. A dict becomes an anonymous `StaticProvider`. Anything else raises `ValueError` |
| `isolate` | bind loopback only, on ephemeral ports, with no beacons; the environment and `conf` are ignored. Combining it with `conf` or `useenv=False` raises `ValueError` |
| `conf` | a dict of `EPICS_PVAS_*`/`EPICS_PVA_*` settings applied over the environment; values are converted with `str()`. Keys the server reads are listed in [config.md](config.md) |
| `useenv` | with `False` the environment is not consulted; only `conf` and built-in defaults apply |

| Member | Meaning |
| --- | --- |
| `conf()` | the dict a client needs to reach this server: `EPICS_PVA_ADDR_LIST` (`127.0.0.1:<udp port>`), `EPICS_PVA_AUTO_ADDR_LIST` (`NO`), `EPICS_PVA_NAME_SERVERS` (`127.0.0.1:<tcp port>`), `EPICS_PVA_SERVER_PORT`, `EPICS_PVAS_SERVER_PORT`, `EPICS_PVAS_BROADCAST_PORT`, `EPICS_PVAS_INTF_ADDR_LIST` |
| `stop()` | stop listening and drop the providers |
| `running` | `True` between construction and `stop()` |
| `Server.forever(*args, **kws)` | construct with the same arguments and sleep until `KeyboardInterrupt` |

## StaticProvider

```python
class StaticProvider(name=None)
```

A name-to-PV table. `name` defaults to a `uuid4` string. `add(name, pv)`,
`remove(name)`, `keys()` (sorted), `name in prov` and `prov[name]`
(`KeyError` when absent) are the whole API. One `SharedPV` may be added
under several names and to several providers; adding a name that is
already present raises `ValueError("PV 'name' already added")`. `remove`
takes the name out of the table: a later search or channel open for it
fails (`PV not found` on an existing connection, a search timeout from a
new one), while channels already open on it stay attached and keep
receiving posts until the PV is closed.

## SharedPV

```python
class SharedPV(handler=None, initial=None, nt=None, wrap=None, unwrap=None, queue=None)              # epicsrs.pva.server
class SharedPV(handler=None, initial=None, nt=None, wrap=None, unwrap=None)                          # epicsrs.pva.server.asyncio
```

| Argument | Meaning |
| --- | --- |
| `handler` | an object with any of the methods `put(pv, op)`, `rpc(pv, op)`, `onFirstConnect(pv)`, `onLastDisconnect(pv)`. `Handler` is an optional base with all four as no-ops. Without a handler the decorators below install methods one at a time |
| `initial` | when given, `open(initial, nt=nt, wrap=wrap, unwrap=unwrap)` is called at once |
| `nt` | an NT helper (see [nt.md](nt.md)); its `wrap` and `unwrap` convert between Python objects and `Value` |
| `wrap`, `unwrap` | explicit callables that take precedence over `nt`. `wrap(value, **kws)` must return a `Value` |
| `queue` | thread flavour only: the `WorkQueue` the handlers run on. By default one of four shared queues, handed out round-robin and created on first use |

The asyncio flavour must be constructed inside a running loop; it binds to
that loop and starts a drain task named `epicsrs.pva.server` on it.

| Method | Meaning |
| --- | --- |
| `open(value, nt=None, wrap=None, unwrap=None, **kws)` | declare the type and initial value; clients may connect afterwards. `nt`/`wrap`/`unwrap` given here replace the constructor's. `kws` go to `wrap` (for the NT helpers: `timestamp`, `severity`, `message`) |
| `post(value, **kws)` | wrap `value`, apply its marked fields to the stored value and deliver them to every subscriber. Unmarked fields keep their stored value |
| `close(destroy=False, sync=False, timeout=None)` | drop the value and disconnect every client. Thread flavour: with `sync=True` block until the last-disconnect edge has run (`timeout` seconds). asyncio flavour: with `sync=True` return an awaitable that resolves once in-flight handler tasks and the last-disconnect edge are done, else return `None`. `destroy` is accepted and unused |
| `isOpen()` | whether a value is held |
| `current()` | the stored value with the marks accumulated by `open` and every `post`, passed through `unwrap`; `None` while closed |
| `stop()` | asyncio flavour only: stop the drain task; the PV can no longer serve put or rpc |
| `put`, `rpc`, `onFirstConnect`, `onLastDisconnect` | decorators that install the named handler method |
| `repr()` | `SharedPV(value=<current()>)` or `SharedPV(<closed>)` |

A value passed to `open` or `post` that is not a `Value` goes through
`wrap`; a failure there raises `ValueError("Unable to wrap ...")`.
Without an NT helper and without `wrap`, only `Value` objects are
accepted.

`WorkQueue()` starts one daemon drain thread; `stop()` ends it and joins
it unless called from that thread. Every PV created with the same
`queue` runs its handlers serially on that thread.

### Handlers

| Handler | When | Arguments |
| --- | --- | --- |
| `put(pv, op)` | a client PUT | `op` is a `ServerOperation` whose `value()` is unwrapped and whose `done(value=)` wraps |
| `rpc(pv, op)` | a client RPC | `op` is the raw extension operation: `value()` is the argument `Value`, `done(value=)` takes a `Value` |
| `onFirstConnect(pv)` | the first client channel opens | |
| `onLastDisconnect(pv)` | the last client channel closes | |

A put or rpc without a handler is answered with the error `Put not supported`
or `RPC not supported`. A handler must call `op.done()` or `op.done(error=...)`;
an exception escaping the handler is logged through
`logging.getLogger("epicsrs.pva.server")` (except `RemoteError`) and answered
as `op.done(error=str(exc))`. In the asyncio flavour a handler may be a
coroutine function; it runs as a task, and a cancelled task answers
`handler cancelled`. A request that arrives for a PV that has been
garbage-collected is answered `SharedPV no longer exists`.

### ServerOperation

| Method | Meaning |
| --- | --- |
| `kind()` | `"put"` or `"rpc"` |
| `name()` | the PV name the client used |
| `peer()` | `host:port` of the client |
| `account()` | the client's account as authenticated by the server |
| `method()` | the authentication method (`"anonymous"`, `"ca"`, `"x509"`, ...) |
| `pvRequest()` | the pvRequest the client sent as a `Value`, or `None` when it sent no options |
| `value()` | put: the PV's value with the client's fields applied and marked; rpc: the argument |
| `done(value=None, error=None)` | complete. `error` replies a failure text; `value` is the reply for an rpc |

`repr(op)` is `ServerOperation(put NAME from PEER)`.

## Example

The server-side example that pairs with the client is in
[pva-client.md](pva-client.md); it shows a put handler, a PV without
handlers, an rpc handler on a raw `Value`, `Server.conf()`, `current()`
and `close()`. The asyncio flavour is shown in the same page under
"asyncio flavour". Its output includes the handler print
`put from True account stevek value 2.5`, where `stevek` is the account
reported by `op.account()` for a loopback client using the plain
authentication method.

The following runs without a client and shows the isolated server's
configuration and the configuration keys a client reads:

```python
import re
from epicsrs.pva.server import Server, StaticProvider
from epicsrs.pva._common import effective_conf
with Server([StaticProvider("p")], isolate=True) as S:
    print({k: re.sub(r"\d{4,5}", "<port>", v) for k, v in S.conf().items()})
print(effective_conf(None, useenv=False))
print(effective_conf({"EPICS_PVA_SERVER_PORT": 5077}, useenv=False)["EPICS_PVA_SERVER_PORT"])
```

```
{'EPICS_PVA_ADDR_LIST': '127.0.0.1:<port>', 'EPICS_PVA_AUTO_ADDR_LIST': 'NO', 'EPICS_PVA_SERVER_PORT': '<port>', 'EPICS_PVA_NAME_SERVERS': '127.0.0.1:<port>', 'EPICS_PVAS_INTF_ADDR_LIST': '127.0.0.1', 'EPICS_PVAS_SERVER_PORT': '<port>', 'EPICS_PVAS_BROADCAST_PORT': '<port>'}
{'EPICS_PVA_ADDR_LIST': '', 'EPICS_PVA_AUTO_ADDR_LIST': 'YES', 'EPICS_PVA_BROADCAST_PORT': '5076', 'EPICS_PVA_SERVER_PORT': '5075', 'EPICS_PVA_NAME_SERVERS': ''}
5077
```

The ports differ between runs; the script masks them. `EPICS_PVA_ADDR_LIST`
carries the UDP search port and `EPICS_PVA_NAME_SERVERS` the TCP port.
