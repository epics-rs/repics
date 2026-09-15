# repics.ca.asyncio

The `repics.ca` functions as coroutines for asyncio. Signatures, arguments,
return values, errors and constants are the same as in [ca.md](ca.md); only
the differences are listed here.

```python
from repics.ca.asyncio import caget, caput, camonitor, cainfo, connect
```

| Name | Difference from `repics.ca` |
| --- | --- |
| `await connect(pv, wait=True, timeout=5.0, throw=True)` | coroutine |
| `await caget(pv, form='time', datatype=None, count=0, timeout=5.0, throw=True)` | coroutine |
| `await caput(pv, value, wait=False, datatype=None, timeout=5.0, repeat_value=False, throw=True)` | coroutine |
| `await cainfo(pv, wait=True, timeout=5.0, throw=True)` | coroutine |
| `camonitor(pv, callback, form='time', datatype=None, count=0, mask=None, all_updates=False, notify_disconnect=False, connect_timeout=None)` | a plain function, not awaited. It must be called from inside a coroutine, because the subscription binds to the running loop |

The callback may be a plain function or a coroutine function. A coroutine
callback is awaited before the next update is taken from the queue, so a
slow callback applies back-pressure; with `all_updates=False` the updates
that pile up behind it collapse to the newest and `dropped_callbacks`
counts the rest. Callbacks run as a task on the loop that called
`camonitor`, one dispatcher task per loop.

`Subscription` has the same `close()`, `pause()`, `resume()`,
`dropped_callbacks`, context manager and `repr()` as the blocking flavour.

The same shared `CaContext` and channel cache serve `repics.ca`,
`repics.ca.asyncio` and `repics.pv`, so mixing them in one process is fine.

## Example

```python
import asyncio
from repics.ca.asyncio import caget, caput, camonitor, cainfo, connect, CaNothing

async def main():
    v = await caget("demo:ai")
    print(repr(v), v.units is None)
    await caput("demo:long", 5, wait=True)
    print(await caget(["demo:long", "demo:str"]))
    print(await connect("demo:ai"))
    print(repr(await caget("demo:missing", timeout=0.5, throw=False)))

    got = []
    async def on_update(value):
        got.append(int(value))
        await asyncio.sleep(0.05)      # awaited before the next delivery
    sub = camonitor("demo:cnt", on_update)     # not awaited
    await asyncio.sleep(0.5)
    sub.close()
    print(sub, len(got) >= 3)

    print(await cainfo("demo:long"))

asyncio.run(main())
```

```
1.5 <name='demo:ai', severity=0, status=0> True
[5 <name='demo:long', severity=0, status=0>, 'hello' <name='demo:str', severity=0, status=0>]
demo:ai: Normal successful completion
CaNothing('demo:missing', 80)
Subscription('demo:cnt', closed) True
demo:long:
    State: connected
    Host: 127.0.0.1:36885
    Access: True, True
    Data type: long
    Count: 1
```
