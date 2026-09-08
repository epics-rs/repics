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
