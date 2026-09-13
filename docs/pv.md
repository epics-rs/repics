# repics.pv

A pyepics-shaped `PV` class over the shared CA context, with a cached
value, a monitor, and keyword-argument callbacks.

```python
from repics.pv import PV, get_pv
```

## PV

```python
class PV(pvname, callback=None, form='time', auto_monitor=None,
         connection_callback=None, connection_timeout=None,
         access_callback=None, count=None)
```

| Argument | Meaning |
| --- | --- |
| `pvname` | the channel name |
| `callback` | a value callback, registered as with `add_callback` |
| `form` | the metadata a read or monitor carries: `'time'` or `'ctrl'` (any `repics.ca` form is accepted) |
| `auto_monitor` | `None` (the default) monitors channels of at most `AUTOMONITOR_MAXLENGTH` elements; `True` or `False` forces it on or off; an `int` that is not a `bool` is the DBE mask to monitor with |
| `connection_callback` | called as `cb(pvname=, conn=, pv=)` on every connection and disconnection |
| `connection_timeout` | the default for `wait_for_connection` |
| `access_callback` | called as `cb(read, write, pv=)` when the server changes access rights |
| `count` | element count for reads and the monitor; `None` uses the server's valid count |

`PV.AUTOMONITOR_MAXLENGTH` is `65536`.

Construction opens the channel through the shared cache and starts one
subscription. The subscription always follows the lifecycle (connection
and access callbacks); it delivers values only when `auto_monitor` allows.
Every update is delivered (nothing is collapsed) and stored as the cached
value before the callbacks run.

### Connection

| Member | Meaning |
| --- | --- |
| `connected` | `True` while the channel has a live circuit |
| `connect(timeout=None)` | the same as `wait_for_connection` |
| `wait_for_connection(timeout=None)` | `True` once connected, `False` when the timeout passes. `timeout=None` uses `connection_timeout`, and `5.0` when that is `None` too |
| `disconnect()` | closes the subscription. The channel stays in the shared cache |

### Values

| Member | Meaning |
| --- | --- |
| `get(count=None, as_string=False, as_numpy=True, timeout=None, use_monitor=True)` | the value. With `use_monitor=True`, `count=None` and a monitor that has delivered, the cached value, which is the last monitor update received and can trail a `put` that has already completed; otherwise a fresh `caget` in the PV's `form`. Raises `CaError` on failure. `as_string` renders as `char_value` does; `as_numpy=False` turns an array into a list |
| `put(value, wait=False, timeout=30.0, use_complete=False, callback=None, callback_data=None)` | write. With `wait=True` it returns after the record has processed. With `callback` or `use_complete`, the write is done with wait on a daemon thread named `caput <pvname>` (or inline when `wait=True`), `put_complete` goes `False` then `True`, and `callback(pvname=, data=callback_data)` runs on that thread |
| `value` | property: `get()`; assigning calls `put()` |
| `char_value` | the string rendering of the cached value, reading once if nothing is cached |
| `get_ctrlvars(timeout=5.0)` | a fresh `ctrl` read; returns a dict of the keys below that the channel has |
| `get_timevars(timeout=5.0)` | a fresh `time` read; returns `status`, `severity`, `timestamp`, `posixseconds`, `nanoseconds` |

The string rendering (`char_value`, `get(as_string=True)`): an enum shows
its label when the labels are known; a `DBR_CHAR` array is decoded as UTF-8
up to the first NUL; another array of at most 20 elements shows as a list
and a larger one as `<array size=N, type=T>`; a float uses the channel's
precision when known; anything else is `str(value)`.

### Metadata properties

`units`, `precision`, `enum_strs`, `upper_disp_limit`, `lower_disp_limit`,
`upper_alarm_limit`, `lower_alarm_limit`, `upper_warning_limit`,
`lower_warning_limit`, `upper_ctrl_limit`, `lower_ctrl_limit` come from the
control fields. If they have not been fetched yet and the channel is
connected, the first access performs `get_ctrlvars()`.

`status`, `severity`, `timestamp`, `posixseconds`, `nanoseconds`, `count`,
`ftype` (the wire type code) and `type` (its name) come from the last
value stored. `nelm` is the native element count. `host`, `read_access`,
`write_access` come from the channel; `access` is one of `"no access"`,
`"read-only"`, `"write-only"`, `"read/write"`.

`info` is a multi-line text block (see the example).

### Callbacks

| Member | Meaning |
| --- | --- |
| `add_callback(callback, index=None, run_now=False, with_ctrlvars=True, **kw)` | register; returns the index (`1 + max` of the existing ones when `None`). `with_ctrlvars` fetches the control fields (one `caget`) after registering, when they are not cached yet and the channel is connected; a monitor update already queued can reach the callback before that fetch, without the control keys. `run_now` calls it once now |
| `remove_callback(index)`, `clear_callbacks()` | unregister |
| `run_callbacks()`, `run_callback(index)` | call with the current cached state |
| `callbacks` | the dict `index: (callback, kw)` |
| `connection_callbacks`, `access_callbacks` | the lists behind the constructor arguments |

A value callback is called with keyword arguments only: `pvname`, `value`,
`char_value`, `status`, `severity`, `timestamp`, `posixseconds`,
`nanoseconds`, `count`, `ftype`, `type`, every control key the channel
has reported, the `**kw` given to `add_callback`, and
`cb_info=(index, pv)`. Callbacks run on the monitor thread.

`repr()` is `<PV 'name': not connected>` or
`<PV 'name', count=N, type=T, access=A>`; `str()` is the name.

## get_pv

```python
def get_pv(pvname, form='time', connect=False, timeout=5.0, **kw)
```

One cached `PV` per `(pvname, form)`, created with `**kw` on first use.
`connect=True` waits up to `timeout` for it. A `PV` built directly is not
in this cache.

## Example

```python
import time
from repics.pv import PV, get_pv

p = PV("demo:ai")
print(p.wait_for_connection(), p, p.connected)
print(p.get(), p.value, p.char_value, p.units, p.precision, p.count, p.type, p.ftype)
print(p.access, p.host.startswith("127.0.0.1:"), p.read_access, p.write_access)
print(p.get_ctrlvars())
print(p.get_timevars())
print(p.info)

q = PV("demo:long", auto_monitor=True)
q.wait_for_connection()
q.get_ctrlvars()                # fetch the control fields before registering
time.sleep(0.2)                 # let the first monitor update land
def on_change(pvname=None, value=None, char_value=None, cb_info=None, **kw):
    print("callback", pvname, value, char_value, cb_info[0], sorted(kw)[:4])
idx = q.add_callback(on_change)
q.put(11, wait=True)
time.sleep(0.2)
q.remove_callback(idx)
q.value = 12                    # a put
time.sleep(0.2)
print(q.get(), q.get(use_monitor=False))

done = []
q.put(13, callback=lambda pvname=None, data=None: done.append((pvname, data)), callback_data="tag")
time.sleep(0.3)
print(done, q.put_complete)

m = PV("demo:mbbo"); m.wait_for_connection()
print(m.get(), m.get(as_string=True), m.enum_strs)

w = PV("demo:wf"); w.wait_for_connection()
w.put([1.0, 2.0, 3.0], wait=True)
time.sleep(0.2)                 # the monitor update behind get() arrives asynchronously
print(w.get(), w.get(as_numpy=False), w.get(as_string=True), w.count, w.nelm)

ro = PV("demo:ro"); ro.wait_for_connection()
print(ro.access, ro.write_access)
print(get_pv("demo:ai") is get_pv("demo:ai"), get_pv("demo:ai") is p)
print(repr(PV("demo:nope")))
```

```
True demo:ai True
1.5 1.5 1.500 mm 3 1 double 6
read/write True True True
{'units': 'mm', 'precision': 3, 'upper_disp_limit': 10.0, 'lower_disp_limit': -10.0, 'upper_alarm_limit': 9.0, 'lower_alarm_limit': -9.0, 'upper_warning_limit': nan, 'lower_warning_limit': nan, 'upper_ctrl_limit': 10.0, 'lower_ctrl_limit': -10.0}
{'status': 0, 'severity': 0, 'timestamp': 1788945266.9073532, 'posixseconds': 1788945266, 'nanoseconds': 907353259}
== demo:ai  (double) ==
   value      = 1.5 <name='demo:ai', severity=0, status=0>
   char_value = '1.500'
   count      = 1
   nelm       = 1
   type       = double
   units      = mm
   precision  = 3
   host       = 127.0.0.1:44913
   access     = read/write
   status     = 0
   severity   = 0
   timestamp  = 1788945266.9073532
callback demo:long 11 11 1 ['count', 'ftype', 'lower_alarm_limit', 'lower_ctrl_limit']
12 12
[('demo:long', 'tag')] True
1 One ['Zero', 'One', 'Two']
[1. 2. 3.] [1.0, 2.0, 3.0] [1.0, 2.0, 3.0] 8 8
read/write True
True False
<PV 'demo:nope': not connected>
```

The first `callback` line is the monitor's initial value (42), delivered
when the callback was registered after connection; the second is the put.
`demo:ro` has `DISP=1`, which refuses writes at the record but does not
change the channel's access rights, so `write_access` is still `True`.

Connection and access callbacks:

```python
import time
from repics.pv import PV

def on_conn(pvname=None, conn=None, pv=None):
    print("connection", pvname, conn, pv.connected)
def on_access(read, write, pv=None):
    print("access", read, write)
p = PV("demo:long", connection_callback=on_conn, access_callback=on_access)
print(p.wait_for_connection(timeout=5.0))
time.sleep(0.1)
p.disconnect()
print(p.connected, repr(p))
print(PV("demo:missing", connection_timeout=0.5).wait_for_connection())
```

```
True
connection demo:long True True
access True True
True <PV 'demo:long', count=1, type=long, access=read/write>
False
```

`disconnect()` only stops the PV's subscription: the shared channel stays
connected, so `connected` is still `True` afterwards.
