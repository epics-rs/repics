# repics.ca

Blocking Channel Access client functions. The asyncio versions with the same
signatures are in [aio.md](aio.md).

```python
from repics.ca import caget, caput, camonitor, cainfo, connect
```

Every function takes `pv` as one name (`str`) or a sequence of names. With a
sequence it returns a list with one entry per name, in order, and the work
for all names runs concurrently.

`DEFAULT_TIMEOUT` is `5.0`.

## Functions

### connect

```python
def connect(pv, wait=True, timeout=5.0, throw=True)
```

Open the channel for `pv` in the shared cache. With `wait=True` it blocks
until the channel is connected and returns `CaNothing(pv)` (truthy, error
code `ECA_NORMAL`). With `wait=False` it returns `CaNothing(pv)` at once,
without waiting; that value is truthy whether or not the channel has
connected yet. On timeout it
raises `CaTimeout`, or with `throw=False` returns a falsy `CaNothing`.

### caget

```python
def caget(pv, form='time', datatype=None, count=0, timeout=5.0, throw=True)
```

Read the value. The result is an augmented Python value (see
[Augmented values](#augmented-values)) carrying the metadata the `form`
asked for.

| Argument | Meaning |
| --- | --- |
| `form` | `'plain'`, `'sts'`, `'time'`, `'gr'` or `'ctrl'`. Anything else raises `ValueError` |
| `datatype` | requested wire type; see [Datatypes](#datatypes). `None` takes the channel's native type |
| `count` | `0` reads the number of elements the server reports as valid, a positive value reads at most that many, a negative value reads the full native length |
| `timeout` | seconds, `None` for no deadline, or a one-element tuple holding an absolute `time.time()` deadline |
| `throw` | with `False`, a failure returns a `CaNothing` instead of raising |

### caput

```python
def caput(pv, value, wait=False, datatype=None, timeout=5.0, repeat_value=False, throw=True)
```

Write `value`. With `wait=True` the call returns after the server has
processed the write (CA write-notify). Returns `CaNothing(pv)` on success
(truthy); with `throw=False` a failure returns a falsy `CaNothing`.

For a list of PVs, `value` is zipped with the names unless it is a scalar,
`str`, `bytes`, non-iterable, or `repeat_value=True`, in which case the same
value goes to every PV. A list of the wrong length raises `ValueError`.

`datatype` decides the conversion applied before the write:

| `datatype` | `value` is sent as |
| --- | --- |
| `None` | `str` as a string, `int` as a long, `float` as a double, arrays as arrays |
| `DBR_CHAR_STR`, `DBR_CHAR_UNICODE` | a `uint8` array of the UTF-8 bytes plus a terminating NUL |
| `DBR_CHAR_BYTES` | the bytes plus a terminating NUL |
| `str`, `DBR_ENUM_STR` | `str(value)` |
| an `int` DBR code | the value cast to that type |

### cainfo

```python
def cainfo(pv, wait=True, timeout=5.0, throw=True)
```

Returns a `CAInfo` for the channel. With `wait=True` it first connects (and
raises or returns a failed `CaNothing` per `throw`); with `wait=False` it
reports whatever is known now.

`CAInfo(name, info, state)` has `name`, `info` (a `ChannelInfo` or `None`),
`state`, `state_strings`, `datatype_strings` and `datatype_name`. `str()`
of it is the multi-line block shown in the example below.

### camonitor

```python
def camonitor(pv, callback, form='time', datatype=None, count=0, mask=None,
              all_updates=False, notify_disconnect=False, connect_timeout=None)
```

Subscribe. `callback(value)` runs on the shared monitor thread for every
update; with a list of PVs the callback is `callback(value, index)`. Returns
a `Subscription`, or a list of them.

| Argument | Meaning |
| --- | --- |
| `form`, `datatype`, `count` | as for `caget`. `datatype` must be one of `None`, `str`, `DBR_ENUM_STR`, `DBR_CHAR_STR`, `DBR_CHAR_BYTES`, `DBR_CHAR_UNICODE`; anything else raises `TypeError` |
| `mask` | DBE event mask. `None` picks `DBE_VALUE` for `'plain'`, `DBE_VALUE \| DBE_ALARM` for `'sts'` and `'time'`, `DBE_VALUE \| DBE_ALARM \| DBE_PROPERTY` for `'gr'` and `'ctrl'` |
| `all_updates` | with `False` (the default), when several updates are queued behind a slow callback only the newest is delivered; the number skipped is counted in `Subscription.dropped_callbacks`. With `True` every update is delivered |
| `notify_disconnect` | with `True` the callback also receives a `CaNothing` (built with `CaNothing.from_exception`, so `ECA_DISCONN` for a lost circuit) for every monitor error; the subscription stays up and resumes on reconnect. With `False` errors are not reported |
| `connect_timeout` | seconds to wait for the first connection; `None` waits forever. On expiry the callback receives `CaNothing(name, ECA_TIMEOUT)`, whether or not `notify_disconnect` is set, and the subscription keeps waiting (its state stays `opening`) |

`Subscription(name, callback, **kw)`:

| Member | Meaning |
| --- | --- |
| `name` | the PV name |
| `close()` | unsubscribe. No callback starts after it returns |
| `pause()`, `resume()` | hold and release delivery |
| `dropped_callbacks` | updates skipped because `all_updates=False` |
| context manager | `__exit__` calls `close()` |
| `repr()` | `Subscription('name', opening)`, `open` or `closed` |

A callback that raises gets its traceback printed to `stderr` and the
subscription is closed.

## Augmented values

`caget` and `camonitor` deliver the value as a subclass of the natural
Python type with the metadata attached:

| Class | Base | Used for |
| --- | --- | --- |
| `AugmentedFloat` | `float` | `DBR_FLOAT`, `DBR_DOUBLE` scalars |
| `AugmentedInt` | `int` | `DBR_SHORT`, `DBR_LONG`, `DBR_CHAR`, `DBR_ENUM` scalars |
| `AugmentedStr` | `str` | strings, enum labels, `DBR_CHAR_STR` |
| `AugmentedArray` | `numpy.ndarray` | arrays |
| `AugmentedList` | `list` | string arrays |
| `AugmentedBytes` | `bytes` | `DBR_CHAR_BYTES` |

Every one has `ok` (`True`), `name`, `datatype` (a lower-case name such as
`'double'`), `dbr` (the wire code), `element_count`, `status`, `severity`,
`raw_stamp` (`(seconds, nanoseconds)`), `timestamp` (float seconds),
`datetime`, `units`, `precision`, `upper_disp_limit`, `lower_disp_limit`,
`upper_alarm_limit`, `lower_alarm_limit`, `upper_warning_limit`,
`lower_warning_limit`, `upper_ctrl_limit`, `lower_ctrl_limit`, `enums`,
`ackt`, `acks`, and `snapshot` (the underlying `Snapshot`). A field the form
did not carry is `None`.

`str()` of a float, int or list is the bare value. `repr()` appends
`<name=..., severity=..., status=...>`.

### Example

```python
from repics.ca import caget, caput, DBR_CHAR_STR, DBR_ENUM_STR

v = caget("demo:ai")            # form="time" by default
print(repr(v))
print(v.name, v.datatype, v.status, v.severity, v.datetime.year > 2000)

c = caget("demo:ai", form="ctrl")
print(c.units, c.precision, c.lower_disp_limit, c.upper_disp_limit, c.upper_alarm_limit)

print(caget(["demo:long", "demo:str", "demo:mbbo"]))

e = caget("demo:mbbo")
print(repr(e), e.enums)
print(repr(caget("demo:mbbo", datatype=DBR_ENUM_STR)))
print(repr(caget("demo:mbbo", datatype=str)))

caput("demo:long", 7, wait=True)
print(caget("demo:long"))
caput(["demo:ao", "demo:long"], [2.5, 8], wait=True)
print(caget(["demo:ao", "demo:long"]))

caput("demo:wf", [1, 2, 3], wait=True)
w = caget("demo:wf")
print(repr(w), w.dtype, w.element_count, len(w))
print(repr(caget("demo:wf", count=-1)))     # the full native length
print(repr(caget("demo:wf", count=2)))

caput("demo:chars", "hello", datatype=DBR_CHAR_STR, wait=True)
print(repr(caget("demo:chars", datatype=DBR_CHAR_STR)))
print(repr(caget("demo:chars")))
```

```
1.5 <name='demo:ai', severity=0, status=0>
demo:ai double 0 0 True
mm 3 -10.0 10.0 9.0
[42 <name='demo:long', severity=0, status=0>, 'hello' <name='demo:str', severity=0, status=0>, 1 <name='demo:mbbo', severity=0, status=0>]
1 <name='demo:mbbo', severity=0, status=0> None
'One' <name='demo:mbbo', severity=0, status=0>
'One' <name='demo:mbbo', severity=0, status=0>
7
[2.5 <name='demo:ao', severity=0, status=0>, 8 <name='demo:long', severity=0, status=0>]
AugmentedArray([1., 2., 3.]) <name='demo:wf', severity=0, status=0> float64 3 3
AugmentedArray([1., 2., 3., 0., 0., 0., 0., 0.]) <name='demo:wf', severity=0, status=0>
AugmentedArray([1., 2.]) <name='demo:wf', severity=0, status=0>
'hello' <name='demo:chars', severity=0, status=0>
AugmentedArray([104, 101, 108, 108, 111,   0], dtype=uint8) <name='demo:chars', severity=0, status=0>
```

The `'time'` form carries no `enums`, so `e.enums` is `None`; the enum
labels come with `form='ctrl'` or `form='gr'`.

## Datatypes

`datatype` for `caget` and `camonitor`:

| `datatype` | Effect |
| --- | --- |
| `None` | the channel's native type |
| `DBR_STRING` .. `DBR_DOUBLE` | request that wire type |
| `str` | `DBR_STRING`, and an enum as its label |
| `int` | `DBR_LONG` |
| `float` | `DBR_DOUBLE` |
| `bool` | `DBR_LONG` |
| a numpy dtype | `float64` to `DBR_DOUBLE`, `float32` to `DBR_FLOAT`, `int32` to `DBR_LONG`, `int16` to `DBR_SHORT`, `uint8` to `DBR_CHAR`, `str_` to `DBR_STRING` |
| `DBR_ENUM_STR` | the enum label as `AugmentedStr` |
| `DBR_CHAR_STR` | a `DBR_CHAR` array decoded as UTF-8 up to the first NUL, as `AugmentedStr` |
| `DBR_CHAR_UNICODE` | as `DBR_CHAR_STR` |
| `DBR_CHAR_BYTES` | a `DBR_CHAR` array as `AugmentedBytes` |

Any other `datatype` raises `TypeError`.

## Errors

```python
from repics import CaTimeout
from repics.ca import caget, caput, connect, cainfo, CaNothing, ECA_TIMEOUT

r = caget("demo:missing", timeout=0.5, throw=False)
print(repr(r), bool(r), r.ok, r.errorcode == ECA_TIMEOUT, str(r))
try:
    caget("demo:missing", timeout=0.5)
except CaTimeout as e:
    print(type(e).__name__, e.status, e)
print(caget(["demo:ai", "demo:missing"], timeout=0.5, throw=False))
print(caput("demo:ro", 1, wait=True, throw=False))
print(connect("demo:ai"), connect("demo:missing", timeout=0.5, throw=False))
```

```
CaNothing('demo:missing', 80) False False True demo:missing: User specified timeout on IO operation expired
CaTimeout 80 ('timed out after 0.4999932670034468 s', 80)
[1.5 <name='demo:ai', severity=0, status=0>, CaNothing('demo:missing', 80)]
demo:ro: Channel write request failed
demo:ai: Normal successful completion demo:missing: User specified timeout on IO operation expired
```

`CaNothing(name, errorcode=1)` is a `CaError` whose truth value is
`errorcode == ECA_NORMAL`; `ok` is the same test. `str()` is
`name: message`. `CaNothing.from_exception(name, exc)` builds one from any
exception, taking the ECA code from a `CaError` (`errorcode(exc)`) and
`ECA_INTERNAL` otherwise. `ca_message(status)` gives the text for an ECA
code.

A write refused by the server (here `demo:ro` has `DISP=1`) is
`ECA_PUTFAIL`. A channel without write access fails with `ECA_NOWTACCESS`
before anything is sent; a read without read access, with
`ECA_NORDACCESS`.

## Channel information

```python
from repics.ca import cainfo, get_channel_infos, caget

print(cainfo("demo:ai"))
i = cainfo("demo:missing", wait=False)
print(repr(i))
print(i.datatype_name, i.state_strings[i.state])
print(get_channel_infos())
```

```
demo:ai:
    State: connected
    Host: 127.0.0.1:58981
    Access: True, True
    Data type: double
    Count: 1
CAInfo('demo:missing', 'never connected', 'no access', 0)
no access never connected
[ChannelStatus('demo:ai', connected=True, subscriber_count=0), ChannelStatus('demo:missing', connected=False, subscriber_count=0)]
```

`get_channel_infos()` lists every cached channel as
`ChannelStatus(name, connected, subscriber_count)`. `purge_channel_caches()`
empties the cache. Channel states are `0` never connected, `1` previously
connected, `2` connected, `3` closed.

## Monitors

```python
import time
from repics.ca import camonitor, caput, DBE_VALUE, DBE_ALARM

seen = []
sub = camonitor("demo:cnt", lambda v: seen.append(int(v)))
time.sleep(0.45)
print(sub, len(seen) >= 3, all(b - a == 1 for a, b in zip(seen, seen[1:])))
sub.close()
print(sub)

subs = camonitor(["demo:ai", "demo:long"], lambda v, i: print("update", i, repr(v)))
time.sleep(0.3)
for s in subs:
    s.close()

with camonitor("demo:long", lambda v: print("value", v), all_updates=True) as sub:
    time.sleep(0.2)
    caput("demo:long", 100, wait=True)
    caput("demo:long", 101, wait=True)
    time.sleep(0.2)
print(sub)

def on_update(v):
    print("ok" if v.ok else f"nothing: {v!r}")
sub = camonitor("demo:missing", on_update, connect_timeout=0.5)
time.sleep(1.0)
print(sub)
sub.close()
```

```
Subscription('demo:cnt', open) True True
Subscription('demo:cnt', closed)
update 0 1.5 <name='demo:ai', severity=0, status=0>
update 1 42 <name='demo:long', severity=0, status=0>
value 42
value 100
value 101
Subscription('demo:long', closed)
nothing: CaNothing('demo:missing', 80)
Subscription('demo:missing', opening)
```

`demo:cnt` is a calc record scanning at 0.1 s. The `demo:long` monitor
shows the initial value (42) first, then the two puts.

## Constants

All of these are exported from `repics`, `repics.ca` and `repics.aio`.

| Name | Value |
| --- | --- |
| `DBR_STRING` | 0 |
| `DBR_SHORT`, `DBR_INT` | 1 |
| `DBR_FLOAT` | 2 |
| `DBR_ENUM` | 3 |
| `DBR_CHAR` | 4 |
| `DBR_LONG` | 5 |
| `DBR_DOUBLE` | 6 |
| `DBR_NO_ACCESS` | 7 |
| `DBR_CHAR_STR` | 999 |
| `DBR_CHAR_BYTES` | 998 |
| `DBR_CHAR_UNICODE` | 997 |
| `DBR_ENUM_STR` | 996 |
| `DBE_VALUE` | 1 |
| `DBE_LOG` | 2 |
| `DBE_ALARM` | 4 |
| `DBE_PROPERTY` | 8 |
| `ECA_NORMAL` | 1 |
| `ECA_TIMEOUT` | 80 |
| `ECA_BADTYPE` | 114 |
| `ECA_INTERNAL` | 142 |
| `ECA_GETFAIL` | 152 |
| `ECA_PUTFAIL` | 160 |
| `ECA_BADCOUNT` | 176 |
| `ECA_DISCONN` | 192 |
| `ECA_NORDACCESS` | 368 |
| `ECA_NOWTACCESS` | 376 |
| `FORMS` | `('plain', 'sts', 'time', 'gr', 'ctrl')` |

Helpers in the same modules: `form_offset(form)` (0, 7, 14, 21, 28),
`request(datatype, form)` (the `(base, offset, enum_as_string, marker)`
tuple the front ends hand to the extension), `errorcode(exc)`, and
`ca_message(status)`.

## The extension classes

The front ends above are written over these classes from
`repics._repics`, re-exported from `repics`. They are usable directly
when you need a second client, a channel outside the shared cache, or
pull-style delivery. Every blocking method here has an `_async`
counterpart returning an awaitable.

### CaContext

```python
CaContext()
```

A CA client: search engine, virtual circuits and their channels.
Configuration comes from the `EPICS_CA_*` environment at construction.
`repics.context()` returns the shared instance the front ends use.

| Method | Meaning |
| --- | --- |
| `channel(name)` | a new `CaChannel`; the search starts immediately |
| `close()` | tear the client down; its channels stop working |
| `connection_count()` | number of live IOC circuits |
| `wait_connected_many(channels, timeout=None)` | wait for every channel concurrently; one entry per channel, `None` on success or the exception |
| `get_many(channels, dbr=None, form=0, enum_as_string=False, count=0, timeout=None)` | read every channel concurrently, connecting first; one `Snapshot` or exception per channel |
| `put_many(channels, values, wait=True, timeout=None)` | write every channel concurrently; `None` or the exception per channel |

### CaChannel

Created by `CaContext.channel`. Dropping the last reference closes it.

| Member | Meaning |
| --- | --- |
| `name` | the PV name |
| `connected` | `True` once the channel has a live circuit |
| `dbr` | the native wire type code (0 to 6), or `None` before connection |
| `element_count` | the native element count, or `None` before connection |
| `info()` | a `ChannelInfo`; raises `CaDisconnected` before connection |
| `wait_connected(timeout=None)` | block until connected |
| `get(dbr=None, form=0, enum_as_string=False, count=0, timeout=None)` | read, connecting first. `dbr` is a base code, `form` an offset from `form_offset`; returns a `Snapshot` |
| `put(value, wait=True, timeout=None)` | write; `wait=True` returns after the server has processed it |
| `subscribe(deadband=0.0, mask=None, enum_as_string=False, float_as_string=False, count=0)` | a `CaSubscription` |
| `events()` | a `CaEvents` stream of lifecycle events from now on |

`count` follows the `caget` rule: `0` autosizes, positive is clamped to the
native count, negative reads the full native length.

`Snapshot` has `value`, `name`, `datatype`, `dbr` (the wire code as
delivered), `element_count`, `status`, `severity`, `raw_stamp`,
`timestamp` (seconds since the Unix epoch), `units`, `precision`, the
eight limits, `enums`, `ackt`, `acks`. Fields the form did not carry are
`None`.

`ChannelInfo` has `name`, `host`, `datatype`, `dbr`, `element_count`,
`read_access`, `write_access`.

### CaSubscription

A monitor drained by calling `recv`; nothing is delivered on a runtime
thread. Dropping or closing it unsubscribes.

| Method | Meaning |
| --- | --- |
| `recv(timeout=None)` | the next `Snapshot`, or `None` once closed. With a `timeout`, raises `CaTimeout` when it expires. A disconnect is raised as `CaDisconnected`; the subscription stays open and resumes on reconnect |
| `recv_batch(max_items=0, timeout=None)` | the next update plus every update already queued behind it, oldest first; `max_items=0` means no bound |
| `pause()`, `resume()` | hold and release delivery |
| `close()` | unsubscribe; a parked `recv` returns `None` |
| context manager | `__exit__` closes |

### CaEvents and ConnectionEvent

`CaEvents.recv(timeout=None)` returns the next `ConnectionEvent`, or `None`
once closed; with a `timeout` it raises `CaTimeout`. `ConnectionEvent.kind`
is one of `"connected"`, `"disconnected"`, `"access_rights"` (with `read`
and `write`), `"type_changed"` (with `dbr`, the new native type) or
`"lagged"` (the receiver fell behind). Fields that do not apply are
`None`.

### Example

```python
from repics import context, CaContext, CaDisconnected
from repics.ca import DBR_DOUBLE, DBR_STRING, form_offset

ctx = context()                       # the CaContext behind repics.ca / aio / pv
print(type(ctx).__name__, ctx.connection_count())
ch = ctx.channel("demo:ai")
ch.wait_connected(timeout=5.0)
print(ch.name, ch.connected, ch.dbr, ch.element_count)
info = ch.info()
print(info.host.startswith("127.0.0.1:"), info.datatype, info.dbr, info.read_access, info.write_access)

snap = ch.get(form=form_offset("time"))
print(type(snap).__name__, snap.value, snap.status, snap.severity, snap.units, snap.timestamp > 0)
snap = ch.get(dbr=DBR_STRING, form=form_offset("ctrl"))
print(repr(snap.value), snap.units, snap.precision)
ch.put(1.5, wait=True, timeout=5.0)

sub = ch.subscribe()                  # drained by recv(); nothing runs on a runtime thread
first = sub.recv(timeout=2.0)
print(first.value)
sub.close()
print(sub.recv())

ch2 = ctx.channel("demo:long")
ev = ch2.events()                    # lifecycle events from now on
first = ev.recv(timeout=5.0)
print(first.kind, first.read, first.write)

try:
    ctx.channel("demo:nothere").info()
except CaDisconnected as e:
    print("CaDisconnected:", e)

ctx2 = CaContext()                    # a second, independent client
print(ctx2.get_many([ctx2.channel("demo:long"), ctx2.channel("demo:str")], timeout=5.0))
ctx2.close()
```

```
CaContext 0
demo:ai True 6 1
True DOUBLE 6 True True
Snapshot 1.5 0 0 None True
'1.500' None None
1.5
None
connected None None
CaDisconnected: ('channel disconnected', 192)
[Snapshot(name="demo:long", value=42, datatype="long", status=0, severity=0), Snapshot(name="demo:str", value='hello', datatype="string", status=0, severity=0)]
```

A `DBR_STRING` read of a double comes back formatted at the record's
precision. A `ctrl` form read with `DBR_STRING` carries no units or
precision, because the CA `DBR_CTRL_STRING` type has none.

The `CaChannel` returned by `channel()` must be kept referenced while its
`CaEvents` or `CaSubscription` is in use; dropping the channel closes them
and `recv` returns `None`.
