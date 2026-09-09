# PVA client

`epicsrs.pva.Context` is the blocking pvAccess client; `epicsrs.pva.asyncio.Context`
is the same API as coroutines. Values are described in [values.md](values.md)
and the normative-type helpers that unwrap them in [nt.md](nt.md).

```python
from epicsrs.pva import Context            # blocking
from epicsrs.pva.asyncio import Context    # coroutines
```

## Context

```python
class Context(provider='pva', conf=None, useenv=True, nt=None, unwrap=None)
```

| Argument | Meaning |
| --- | --- |
| `provider` | must be `'pva'`; anything else raises `ValueError` |
| `conf` | a dict of `EPICS_PVA_*` settings applied on top of the environment. Values are converted with `str()` |
| `useenv` | with `True`, every `EPICS_PVA*` variable in the process environment is read into the configuration first; with `False` the five keys below start from their built-in defaults instead |
| `nt` | a dict `structure id -> NT helper` merged over `defaultNT()`; `False` disables unwrapping so reads return `Value` |
| `unwrap` | a dict `structure id -> callable` used instead of the NT table; `False` disables unwrapping |

The client applies these keys from the resulting configuration and ignores
the rest:

| Key | Built-in default | Effect |
| --- | --- | --- |
| `EPICS_PVA_ADDR_LIST` | `""` | whitespace-separated unicast or broadcast search targets, `addr[:port][,ttl][@iface]`; the default port is `EPICS_PVA_BROADCAST_PORT`. An entry that does not parse is dropped |
| `EPICS_PVA_AUTO_ADDR_LIST` | `"YES"` | add the interface broadcast addresses to the search targets. `yes`, `y`, `1`, `true` and `on` (any case) mean yes |
| `EPICS_PVA_BROADCAST_PORT` | `"5076"` | UDP search port |
| `EPICS_PVA_SERVER_PORT` | `"5075"` | default TCP port for `EPICS_PVA_NAME_SERVERS` entries without one |
| `EPICS_PVA_NAME_SERVERS` | `""` | whitespace-separated `host[:port]` TCP endpoints searched directly. An entry that does not resolve raises `ValueError` |

A port value that is not an integer in 0 to 65535 raises `ValueError`. The
crate underneath also reads its own settings from the process environment
when the client is built (timeouts, authentication identity, TLS); those
are listed in [config.md](config.md) and are not affected by `useenv`.

`close()` tears down every channel; live monitors see a disconnect. The
context is a context manager (`with Context(...) as ctxt:`), and the
asyncio flavour is both a sync and an async one.

### Methods

Every method takes `timeout` in seconds (`None` for no deadline) and
`throw`; with `throw=False` a failure returns the exception instead of
raising it. Where `name` may be a list, the operations run concurrently
and the result is a list in the same order.

| Method | Meaning |
| --- | --- |
| `get(name, request=None, timeout=5.0, throw=True)` | read. `name` may be a list; `request` is then one string for all or a list. Returns the unwrapped value (see [nt.md](nt.md)) or the `Value` when unwrapping is off or the id is unknown |
| `put(name, values, request=None, timeout=5.0, throw=True, process=None, wait=None, get=True)` | write; see below |
| `rpc(name, value, request=None, timeout=5.0, throw=True)` | call `name` with `value`, a `Value` (see `nt.NTURI`). The reply is unwrapped like a read |
| `info(name, timeout=5.0)` | the server's `Type` for `name` |
| `connect(name, timeout=5.0)` | wait until `name` is connected; returns the server address as `host:port` |
| `monitor(name, cb, request=None, notify_disconnect=False, queue=None, limit=None)` | subscribe; see below. The asyncio flavour has no `queue` argument |

In the asyncio flavour every method except `monitor` is a coroutine; list
forms run through `asyncio.gather`.

A name that never resolves raises `PvaTimeout` when the deadline passes.
A server-side failure raises `PvaRemoteError`. A channel lost while an
operation is pending raises `PvaDisconnected`. `epicsrs.pva` exports the
aliases `TimeoutError`, `RemoteError` and `Disconnected` for those.

### put

`values` is one of:

| `values` | What is sent |
| --- | --- |
| a `Value` | its marked fields, as they are |
| a dict | the current value is read (`get=True`) or built from `info()` (`get=False`), unmarked, then each `field: value` pair is assigned and marked. Dotted keys reach nested fields |
| anything else | as the dict case, assigned through the NT helper for the structure id; for the default helpers that means the `value` field, and an `NTEnum` accepts a choice label |

`name` may be a list, in which case `values` must be a list of the same
length (else `ValueError`).

`process` and `wait` add the pvRequest record options: `process=True`,
`False` or `'passive'` becomes `record[process=...]` and `wait=True` or
`False` becomes `record[block=...]`; `process=None` leaves the server's
default and `wait=None` sends `block=false`. They cannot be combined with an
explicit `request` (`ValueError`).

### pvRequest strings

`request` is a pvRequest expression in the usual text form, for example
`field(value,alarm)`, `field()record[block=true,process=true]` or
`record[queueSize=8]`. It is parsed by the `epics-pva-rs` crate; an empty or
whitespace-only string is the same as `None`, and an unparsable one raises
`ValueError("bad pvRequest ...")`.

### monitor

`monitor(name, cb, request=None, notify_disconnect=False, queue=None, limit=None)`
returns a `Subscription` at once, before the channel connects.

| Argument | Meaning |
| --- | --- |
| `cb` | `cb(value)` for every update. In the blocking flavour it runs on one daemon thread (`epicsrs pvmonitor`) shared by every monitor of that flavour, unless `queue` is given. In the asyncio flavour it runs as a task on the loop that called `monitor`, and may be a coroutine function; a coroutine callback is awaited before the next item is taken |
| `request` | pvRequest string; `record[queueSize=N]` sets the queue bound when `limit` is `None` |
| `notify_disconnect` | with `True`, `cb` also receives a `Disconnected()` instance before the first connection and after every loss, and a `Finished()` instance when the server ends the subscription. With `False` those events are silent |
| `queue` | blocking flavour only: an object with a `push` or `put` method; `functools.partial(cb, value)` is pushed instead of calling `cb` |
| `limit` | the number of updates queued per monitor between the server and `cb`; `None` takes `queueSize` from the request, else 4. Values below 1 become 1 |

When `cb` falls behind, updates beyond `limit` are squashed on the Rust side:
the newest value wins and its changed set is the union of what it replaced.
No Python objects pile up.

The `Subscription`:

| Member | Meaning |
| --- | --- |
| `name` | the PV name |
| `close()` | stop. No callback starts after it returns; one already running finishes. No `Finished` is delivered for a client-side close |
| `pause()`, `resume()` | hold and release delivery |
| context manager | `__exit__` closes |
| `repr()` | `Subscription('name', open)` or `closed` |

An exception raised by `cb` is logged through `logging.getLogger("epicsrs.pva")`
and the monitor continues.

## Example

The server side of this example is explained in [pva-server.md](pva-server.md).

```python
import time
from epicsrs.pva import Context, Value, Type, RemoteError
from epicsrs.pva.nt import NTScalar, NTURI
from epicsrs.pva.server import Server, StaticProvider, SharedPV

x = SharedPV(nt=NTScalar("d"), initial=1.0)
@x.put
def on_put(pv, op):
    print("put from", op.peer().startswith("127.0.0.1:"), "account", op.account(), "value", op.value())
    pv.post(op.value(), timestamp=time.time())
    op.done()

ro = SharedPV(nt=NTScalar("i"), initial=0)          # no put handler

add = SharedPV(initial=Value(Type([("value", "d")]), {"value": 0.0}))   # no NT: raw Values
@add.rpc
def on_rpc(pv, op):
    q = op.value().query
    op.done(Value(Type([("value", "d")]), {"value": q.a + q.b}))

prov = StaticProvider("demo")
prov.add("demo:pva:x", x)
prov.add("demo:pva:ro", ro)
prov.add("demo:pva:add", add)
print(prov.name, prov.keys(), "demo:pva:x" in prov, prov["demo:pva:x"] is x)

with Server([prov], isolate=True) as S:
    print(S.running, sorted(S.conf()))
    with Context("pva", conf=S.conf(), useenv=False) as ctxt:
        print(ctxt.connect("demo:pva:x").startswith("127.0.0.1:"))
        v = ctxt.get("demo:pva:x")
        print(repr(v), v.severity, v.timestamp)
        print(ctxt.info("demo:pva:x"))
        ctxt.put("demo:pva:x", 2.5)
        print(ctxt.get("demo:pva:x"), x.current())
        try:
            ctxt.put("demo:pva:ro", 1)
        except RemoteError as e:
            print("RemoteError:", e)
        print(ctxt.put("demo:pva:ro", 1, throw=False))
        print(ctxt.get(["demo:pva:x", "demo:pva:ro"]))
        raw = ctxt.get("demo:pva:x", request="field(value,alarm)")
        print(repr(raw), raw.severity)
        uri = NTURI([("a", "d"), ("b", "d")])
        r = ctxt.rpc("demo:pva:add", uri.wrap("demo:pva:add", kws={"a": 1.0, "b": 2.0}))
        print(repr(r), r.value)
        print(repr(ctxt.get("demo:pva:none", timeout=0.5, throw=False)))
    print(S.running)
print(S.running, x.isOpen())
x.close(); print(x.isOpen(), x.current(), repr(x))
```

```
demo ['demo:pva:add', 'demo:pva:ro', 'demo:pva:x'] True True
True ['EPICS_PVAS_BROADCAST_PORT', 'EPICS_PVAS_INTF_ADDR_LIST', 'EPICS_PVAS_SERVER_PORT', 'EPICS_PVA_ADDR_LIST', 'EPICS_PVA_AUTO_ADDR_LIST', 'EPICS_PVA_NAME_SERVERS', 'EPICS_PVA_SERVER_PORT']
True
1.0 <name='demo:pva:x', severity=0, status=0> 0 0.0
Type(structure epics:nt/NTScalar:1.0
    value: double
    alarm: structure alarm_t
        severity: int
        status: int
        message: string

    timeStamp: structure time_t
        secondsPastEpoch: long
        nanoseconds: int
        userTag: int

)
put from True account stevek value 2.5
2.5 2.5
RemoteError: remote error: Error: Put not supported
remote error: Error: Put not supported
[2.5 <name='demo:pva:x', severity=0, status=0>, 0 <name='demo:pva:ro', severity=0, status=0>]
2.5 <name='demo:pva:x', severity=0, status=0> 0
Value("", struct {
    double value = 3
}) 3.0
PvaTimeout('timed out after 0.5 s')
True
False True
False None SharedPV(<closed>)
```

The reply of the RPC has no known structure id, so it comes back as a bare
`Value`. A read with a `request` still goes through the NT unwrapper: the
`field(value,alarm)` read above is an `AugmentedFloat` whose `timestamp`
is `0.0` because the `timeStamp` field was not requested.

### Monitors

```python
import time
from epicsrs.pva import Context, Disconnected, Finished
from epicsrs.pva.nt import NTScalar
from epicsrs.pva.server import Server, StaticProvider, SharedPV

pv = SharedPV(nt=NTScalar("d"), initial=0.0)
prov = StaticProvider("demo"); prov.add("demo:pva:m", pv)
with Server([prov], isolate=True) as S, Context("pva", conf=S.conf(), useenv=False) as ctxt:
    got = []
    sub = ctxt.monitor("demo:pva:m", got.append, notify_disconnect=True)
    time.sleep(0.3)
    pv.post(1.0)
    pv.post(2.0, severity=1, message="warn")
    time.sleep(0.3)
    print([type(g).__name__ for g in got])
    print([float(g) for g in got[1:]], got[-1].severity, got[-1].status)
    sub.close()
    time.sleep(0.1)
    print(type(got[-1]).__name__, sub)

    # queue squashing: the server side drops all but the newest when the client is slow
    slow = []
    def cb(v):
        slow.append(float(v)); time.sleep(0.2)
    sub = ctxt.monitor("demo:pva:m", cb, limit=2)
    time.sleep(0.3)
    for i in range(20):
        pv.post(float(i))
    time.sleep(1.5)
    sub.close()
    print(len(slow) < 20, slow[-1])
```

```
['PvaDisconnected', 'AugmentedFloat', 'AugmentedFloat', 'AugmentedFloat']
[0.0, 1.0, 2.0] 1 0
AugmentedFloat Subscription('demo:pva:m', closed)
True 19.0
```

`Disconnected` is the `PvaDisconnected` class, so the first item prints
under that name. The first value after it is the current value at
subscription time.

### put options

```python
from epicsrs.pva import Context
from epicsrs.pva.nt import NTScalar
from epicsrs.pva.server import Server, StaticProvider, SharedPV

pv = SharedPV(nt=NTScalar("d", display=True), initial=0.0)
@pv.put
def on_put(pv, op):
    print("pvRequest:", op.pvRequest())
    pv.post(op.value()); op.done()
prov = StaticProvider("demo"); prov.add("demo:pva:p", pv)
with Server([prov], isolate=True) as S, Context("pva", conf=S.conf(), useenv=False) as ctxt:
    ctxt.put("demo:pva:p", 1.0)
    ctxt.put("demo:pva:p", 2.0, process=True, wait=True)
    ctxt.put("demo:pva:p", 3.0, request="field(value)")
    ctxt.put("demo:pva:p", {"value": 4.0, "display.description": "dict put"})
    v = ctxt.get("demo:pva:p")
    print(v, v.raw.display.description)
    try:
        ctxt.put("demo:pva:p", 5.0, request="field(value)", process=True)
    except ValueError as e:
        print("ValueError:", e)
```

```
pvRequest: None
pvRequest: struct {
    struct {} field
    struct {
        struct {
            string block = "true"
            string process = "true"
        } _options
    } record
}

pvRequest: None
pvRequest: None
4.0 dict put
ValueError: request= cannot be combined with process=/wait=
```

`op.pvRequest()` is `None` when the client sent no options; a `field(...)`
selection alone also arrives as `None`.

In 1 of 5 runs of this script the `get` that
`put(..., process=True, wait=True)` performs first raised
`PvaTimeout('timed out after 5 s')` while the server was idle. The same
loss shows up in a minimal script that serves one PV and reads it twice
from a `Context` in the same process: with the default runtime of one
worker thread the second `get` timed out in 8 of 40 runs (2 s deadline),
and in 0 of 40 runs with `EPICSRS_WORKERS=4` (see [config.md](config.md)).
A retry on the same context succeeds. Set `EPICSRS_WORKERS` above 1 when a
process runs both a `Server` and a `Context`.

## asyncio flavour

```python
import asyncio
from epicsrs.pva.asyncio import Context
from epicsrs.pva.server import Server, StaticProvider
from epicsrs.pva.server.asyncio import SharedPV
from epicsrs.pva.nt import NTScalar

async def main():
    pv = SharedPV(nt=NTScalar("d"), initial=0.0)   # must be created inside a running loop
    @pv.put
    async def on_put(pv, op):
        await asyncio.sleep(0.01)
        pv.post(op.value())
        op.done()
    prov = StaticProvider("demo"); prov.add("demo:pva:a", pv)
    with Server([prov], isolate=True) as S:
        async with Context("pva", conf=S.conf(), useenv=False) as ctxt:
            print(await ctxt.get("demo:pva:a"))
            await ctxt.put("demo:pva:a", 9.0)
            print(await ctxt.get("demo:pva:a"))
            print(await ctxt.get(["demo:pva:a", "demo:pva:a"]))
            q = asyncio.Queue()
            sub = ctxt.monitor("demo:pva:a", q.put)
            first = await asyncio.wait_for(q.get(), 2)
            pv.post(10.0)
            second = await asyncio.wait_for(q.get(), 2)
            print(float(first), float(second))
            sub.close()
        await pv.close(sync=True)
    print(pv.isOpen())

asyncio.run(main())
```

```
0.0
9.0
[9.0 <name='demo:pva:a', severity=0, status=0>, 9.0 <name='demo:pva:a', severity=0, status=0>]
9.0 10.0
False
```

`q.put` is a coroutine function here, so the dispatcher awaits each
delivery before taking the next item.
