# Normative types

`repics.pva.nt` builds and interprets the standard pvAccess structures.
Each helper has a `type` (a `Type`), `wrap(python) -> Value`,
`unwrap(Value) -> python` and `assign(Value, python)`. A `Context` uses
`unwrap` on every read and `assign` on `put`; a `SharedPV(nt=...)` uses
`wrap` on `open`/`post` and `unwrap` for `current()` and `op.value()`.

```python
from repics.pva.nt import NTScalar, NTEnum, NTTable, NTNDArray, NTURI, NTBase, ntenum, alarm, timeStamp, defaultNT
```

| Name | Structure id |
| --- | --- |
| `timeStamp` | `time_t`: `secondsPastEpoch` (l), `nanoseconds` (i), `userTag` (i) |
| `alarm` | `alarm_t`: `severity` (i), `status` (i), `message` (s) |
| `NTScalar(valtype)` | `epics:nt/NTScalar:1.0`, or `epics:nt/NTScalarArray:1.0` when `valtype` starts with `a` |
| `NTEnum()` | `epics:nt/NTEnum:1.0` |
| `NTTable(columns)` | `epics:nt/NTTable:1.0` |
| `NTNDArray()` | `epics:nt/NTNDArray:1.0` |
| `NTURI(args)` | `epics:nt/NTURI:1.0` |

`defaultNT()` returns the id-to-class table a `Context` starts from: the
five `epics:nt/...` ids above, `NTScalar` serving both scalar ids.

## Common behaviour

`wrap(value, **kws)` accepts `timestamp`, `severity` and `message` keyword
arguments on every helper. `timestamp` may be a `datetime`, a float of
seconds, or a `(seconds, nanoseconds)` tuple. A `Value` passed to `wrap`
is annotated and returned as is.

`unwrap` returns the augmented types of [ca.md](ca.md) with these
attributes: `name` (empty), `ok` (`True`), `severity`, `status`,
`timestamp` (float seconds), `raw_stamp` (`(seconds, nanoseconds)`),
`datatype`, `raw` (the `Value` it came from), `enums` for `NTEnum`, plus
`units`, `precision`, `lower_disp_limit` and the other control fields
when the structure carries `display`, `control` or `valueAlarm`.

## NTScalar

```python
NTScalar(valtype='d', extra=[], display=False, control=False, valueAlarm=False, form=False)
NTScalar.buildType(valtype='d', extra=[], display=False, control=False, valueAlarm=False, form=False) -> Type
```

`valtype` is a type code from [values.md](values.md). The members are
`value`, `alarm` and `timeStamp`, then the optional sub-structures, then
`extra` (a list of `(name, spec)`). For a numeric code (anything but
`?`, `s` and `u`):

| Flag | Members added |
| --- | --- |
| `display=True` | `display`: `limitLow`, `limitHigh` (the value's scalar code), `description`, `format`, `units` |
| `display=True, form=True` | `display`: `limitLow`, `limitHigh`, `description`, `precision` (i), `form` (`enum_t`), `units` |
| `control=True` | `control`: `limitLow`, `limitHigh`, `minStep` |
| `valueAlarm=True` | `valueAlarm`: `active` (?), `lowAlarmLimit`, `lowWarningLimit`, `highWarningLimit`, `highAlarmLimit`, the four `...Severity` ints, `hysteresis` (d) |

For `?`, `s` and `u`, `display=True` adds only `description` and `units`
and the other flags add nothing.

| Method | Meaning |
| --- | --- |
| `wrap(value, **kws)` | a dict initialises fields by name; an augmented value from a read supplies `.raw` and its timestamp; anything else goes to `value` |
| `unwrap(V)` | the augmented `value` field; arrays get `element_count` and an empty payload becomes an empty numpy array (or `[]` for `as`) |
| `assign(V, py)` | a dict assigns each key; anything else assigns `value` |

## NTEnum

```python
NTEnum(extra=[], display=False, control=False, valueAlarm=False)
```

`value` is `enum_t`: `index` (i) and `choices` (as).

| Method | Meaning |
| --- | --- |
| `wrap(value, choices=None, **kws)` | `value` may be an int index, a choice label (resolved against `choices` or the choices cached by the last `unwrap`), a dict with `index`/`choices`, or an `ntenum` |
| `unwrap(V)` | an `ntenum`: an `AugmentedInt` whose `str()` is the label and whose `choice` attribute is the label or `None` when the index is out of range. The choices are cached on the helper instance |
| `assign(V, py)` | a label is looked up in the value's choices, else the cached ones; a numeric string is parsed with base 0 |

## NTTable

```python
NTTable(columns=[], extra=[])
```

`columns` is a list of `(name, code)` with scalar codes (`ValueError` for
an array code); each column is stored as an array of that code under
`value`, and `labels` holds the column names.

| Method | Meaning |
| --- | --- |
| `wrap(values, **kws)` | `values` is an iterable of row dicts |
| `unwrap(V)` | an `AugmentedList` of `OrderedDict` rows with a `labels` attribute and `datatype` `"table"` |

## NTNDArray

```python
NTNDArray(extra=[])
```

`value` is a union over `booleanValue` (a?), `byteValue` (ab),
`shortValue` (ah), `intValue` (ai), `longValue` (al), `ubyteValue` (aB),
`ushortValue` (aH), `uintValue` (aI), `ulongValue` (aL), `floatValue`
(af) and `doubleValue` (ad). `dimension` is a structure array of
`size`, `offset`, `fullSize`, `binning`, `reverse`; `attribute` a
structure array of `name`, `value` (variant), `descriptor`, `alarm`,
`timeStamp`, `sourceType`, `source`; plus `codec`, `compressedSize`,
`uncompressedSize`, `uniqueId`, `dataTimeStamp`, `alarm`, `timeStamp`.

| Method | Meaning |
| --- | --- |
| `wrap(value, attrib=None, **kws)` | `value` is anything `numpy.asarray` accepts. The union member follows the dtype, `dimension` lists the shape in reverse (fastest axis first), and each `attrib` entry becomes an attribute. `ColorMode` is added when absent: 0 for a 2-d array; for a 3-d array the axis of length 3 sets it (`ValueError` when none has length 3) |
| `unwrap(V)` | the pixel array reshaped to the dimensions (a view of the wire buffer) with `attrib` (a dict of the attributes) and the usual alarm/timestamp attributes |

## NTURI

```python
NTURI(args)
```

`args` is a list of `(name, code)` for the query arguments.

`wrap(path, args=(), kws=None, scheme='', authority='')` builds a request
`Value` for `Context.rpc`. Positional `args` are matched to the declared
names in order; `kws` by name; a `None` in `kws` is dropped. Only the
arguments supplied appear in `query`. A value the declared type cannot
take raises `ValueError("Unable to initialize NTURI ...")`.

## Client-side unwrapping

`buildNT(nt=None, unwrap=None)` is what `Context(nt=..., unwrap=...)`
calls. `nt` is merged over `defaultNT()`; `unwrap` (a legacy dict
`id -> callable`) replaces the table with unwrap-only entries; `False`
for either turns unwrapping off so reads return `Value`. The resulting
`ClientUnwrapper` picks the helper by `Value.getID()` and, for an unknown
id, returns the `Value` and assigns `put` values either key by key (a
dict) or to a `value` field (`TypeError` when there is none).

## Examples

```python
import numpy as np
from repics.pva import Value
from repics.pva.nt import NTScalar, NTEnum, NTTable, NTNDArray, NTURI, defaultNT, timeStamp, alarm

print(NTScalar.buildType("d"))
V = NTScalar("d", display=True).wrap(1.5, timestamp=1700000000.5, severity=1, message="warn")
print(V)
u = NTScalar.unwrap(V)
print(repr(u), u.timestamp, u.severity, u.status, u.raw is V, u.raw_stamp)
A = NTScalar("ai").wrap([1, 2, 3])
a = NTScalar.unwrap(A)
print(repr(a), a.element_count, a.dtype)
S = NTScalar("s").wrap("text")
print(repr(NTScalar.unwrap(S)))
print(sorted(defaultNT()))
print(timeStamp.getID(), alarm.getID())
```

```
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
struct "epics:nt/NTScalar:1.0" {
    double value = 1.5
    struct "alarm_t" {
        int32_t severity = 1
        int32_t status = 0
        string message = "warn"
    } alarm
    struct "time_t" {
        int64_t secondsPastEpoch = 1700000000
        int32_t nanoseconds = 500000000
        int32_t userTag = 0
    } timeStamp
    struct {
        double limitLow = 0
        double limitHigh = 0
        string description = ""
        string format = ""
        string units = ""
    } display
}

1.5 <name='', severity=1, status=0> 1700000000.5 1 0 True (1700000000, 500000000)
AugmentedArray([1, 2, 3], dtype=int32) <name='', severity=0, status=0> 3 int32
'text' <name='', severity=0, status=0>
['epics:nt/NTEnum:1.0', 'epics:nt/NTNDArray:1.0', 'epics:nt/NTScalar:1.0', 'epics:nt/NTScalarArray:1.0', 'epics:nt/NTTable:1.0']
time_t alarm_t
```

```python
import numpy as np
from repics.pva.nt import NTEnum, NTTable, NTNDArray, NTURI, ntenum

E = NTEnum()
V = E.wrap(1, choices=["Off", "On"])
print(V)
e = E.unwrap(V)
print(type(e).__name__, repr(e), str(e), int(e), e.choice)
V2 = E.wrap("Off")                 # choices cached by the last unwrap
print(V2.value.index)

T = NTTable(columns=[("a", "i"), ("b", "s")])
V = T.wrap([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}])
print(V.labels, V.value.a, V.value.b)
rows = T.unwrap(V)
print(rows, rows.labels, rows.datatype)

N = NTNDArray()
img = np.arange(6, dtype=np.uint8).reshape(2, 3)
V = N.wrap(img, attrib={"ColorMode": 0, "gain": 2.5})
print(V.type()["value"], V.value, [(d.size, d.offset) for d in V.dimension])
print([(a.name, a.value) for a in V.attribute])
back = N.unwrap(V)
print(back.shape, back.dtype, back.attrib)

U = NTURI([("a", "d"), ("b", "s")])
print(U.wrap("svc", kws={"a": 1.0, "b": "q"}))
print(U.wrap("svc", args=(2.0,), scheme="pva"))
```

```
struct "epics:nt/NTEnum:1.0" {
    struct "enum_t" {
        int32_t index = 1
        string[] choices = {2}["Off", "On"]
    } value
    struct "alarm_t" {
        int32_t severity = 0
        int32_t status = 0
        string message = ""
    } alarm
    struct "time_t" {
        int64_t secondsPastEpoch = 0
        int32_t nanoseconds = 0
        int32_t userTag = 0
    } timeStamp
}

ntenum ntenum(1, On) On 1 On
0
['a', 'b'] [1 2] ['x', 'y']
[OrderedDict({'a': np.int32(1), 'b': 'x'}), OrderedDict({'a': np.int32(2), 'b': 'y'})] ['a', 'b'] table
('U', None, [('booleanValue', 'a?'), ('byteValue', 'ab'), ('shortValue', 'ah'), ('intValue', 'ai'), ('longValue', 'al'), ('ubyteValue', 'aB'), ('ushortValue', 'aH'), ('uintValue', 'aI'), ('ulongValue', 'aL'), ('floatValue', 'af'), ('doubleValue', 'ad')]) [0 1 2 3 4 5] [(3, 0), (2, 0)]
[('ColorMode', 0), ('gain', 2.5)]
(2, 3) uint8 {'ColorMode': 0, 'gain': 2.5}
struct "epics:nt/NTURI:1.0" {
    string scheme = ""
    string authority = ""
    string path = "svc"
    struct {
        double a = 1
        string b = "q"
    } query
}

struct "epics:nt/NTURI:1.0" {
    string scheme = "pva"
    string authority = ""
    string path = "svc"
    struct {
        double a = 2
    } query
}
```
