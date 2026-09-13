# Type and Value

`repics.pva.Type` describes a pvData structure; `repics.pva.Value` holds
one. Both are extension classes. A `Value` carries a change-mark bitset
alongside the data: a put sends the marked fields, a monitor update marks
the fields the server changed, and `SharedPV.post` posts the marked fields.

```python
from repics.pva import Type, Value
```

## Type specs

A spec is a list of `(name, field_spec)` pairs. A field spec is a code
string, a `(code, id, members)` tuple for a structure or union, or a
`Type`.

| Code | Field type |
| --- | --- |
| `?` | boolean |
| `b`, `B` | byte, ubyte |
| `h`, `H` | short, ushort |
| `i`, `I` | int, uint |
| `l`, `L` | long, ulong |
| `f`, `d` | float, double |
| `s` | string |
| `a` + scalar code | array of that scalar, for example `ad` or `as` |
| `v` | variant (any) |
| `av` | variant array |
| `('S', id, members)` | structure with the given id (`None` for none) |
| `('aS', id, members)` | structure array |
| `('U', id, members)` | union |
| `('aU', id, members)` | union array |

Anything else raises `ValueError`; a member that is not a `(name, spec)`
pair raises `TypeError`.

## Type

```python
Type(spec, id=None)
```

`spec` is a member list as above, or another `Type` (copied; `id` then
replaces its id). `Type(('S', id, members))` and `Type('d')` are not
accepted: the top level is always a structure built from a member list,
so `TypeError: each member must be a (name, spec) tuple` is raised. The
tuple that `aspy()` returns therefore round-trips as
`Type(t[2], id=t[1])`, or as a member spec inside another `Type`.

| Member | Meaning |
| --- | --- |
| `keys()`, `items()`, `iter()`, `len()`, `in`, `has(key)` | the top-level members |
| `T[name]` | the member's `Type` for a structure, the code string for a scalar or array, the `(code, id, members)` tuple for a union, `'v'` for a variant |
| `getID()` | the structure id, `""` when none |
| `aspy()` | the `('S', id, members)` spec this type was (or could have been) built from |
| `T(initial=None)` | a new `Value` of this type |
| `==` | structural equality |
| `repr()`, `str()` | the multi-line rendering shown below |

```python
import numpy as np
from repics.pva import Type, Value

T = Type([
    ("value", "d"),
    ("alarm", ("S", "alarm_t", [("severity", "i"), ("status", "i"), ("message", "s")])),
    ("arr", "ad"),
    ("names", "as"),
    ("any", "v"),
    ("choice", ("U", None, [("i", "i"), ("s", "s")])),
], id="demo_t")
print(T)
print(T.keys(), T.getID(), len(T), "value" in T, T.has("arr"))
print(T["value"], T["alarm"], T["choice"])
print(T.aspy())

V = Value(T, {"value": 1.5, "alarm": {"severity": 1}})
print(sorted(V.changedSet()))
print(V.value, V["alarm.severity"], repr(V.alarm.message), repr(V.arr), V.names, V.any, V.choice)
```

```
Type(structure demo_t
    value: double
    alarm: structure alarm_t
        severity: int
        status: int
        message: string

    arr: double[]
    names: string[]
    any: any
    choice: union
        i: int
        s: string

)
['value', 'alarm', 'arr', 'names', 'any', 'choice'] demo_t 6 True True
d Type(structure alarm_t
    severity: int
    status: int
    message: string
) ('U', None, [('i', 'i'), ('s', 's')])
('S', 'demo_t', [('value', 'd'), ('alarm', ('S', 'alarm_t', [('severity', 'i'), ('status', 'i'), ('message', 's')])), ('arr', 'ad'), ('names', 'as'), ('any', 'v'), ('choice', ('U', None, [('i', 'i'), ('s', 's')]))])
['alarm.severity', 'value']
1.5 1 '' array([], dtype=float64) [] None None
```

Fields not given in `initial` hold their type's zero value: `0`, `""`, an
empty array, an unselected union, an empty variant.

## Value

```python
Value(type, initial=None)
```

`type` is a `Type` or a member list. `initial` is a dict of fields (nested
dicts for sub-structures), another `Value` of a compatible type, or
`None`. Fields given in `initial` are marked; the rest start unmarked.

### Reading

| Access | Returns |
| --- | --- |
| `V.name`, `V["name"]`, `V["a.b.c"]` | the field. Dotted paths reach nested fields |
| scalar field | `int`, `float`, `str` or `bool` |
| scalar array | a read-only `numpy.ndarray` view over the wire buffer (no copy). Copy it before modifying |
| string array | a `list` of `str` |
| sub-structure | a `Value` view sharing the root's data and marks |
| structure array | a `list` of detached `Value`s, `None` for a null element |
| union | the selected member's value, `None` when nothing is selected |
| variant | the stored value, `None` when empty |
| `get(key, default=None)` | like `dict.get` |
| `has(key)`, `in`, `keys()`, `items()`, `iter()`, `len()` | the members of this view |
| `getID()` | the structure id |
| `type(field=None)` | the `Type` of this view, or the spec of member `field` |
| `todict(fields=None)` | a `dict`; nested structures become dicts |
| `tolist()` | a list of `(name, value)` pairs; nested structures become lists |
| `tostr()`, `str()` | the pvData-style text rendering |
| `repr()` | `Value("id", <rendering>)` |

### Assigning

`V.name = x`, `V["name"] = x` and `V["a.b"] = x` assign and mark the
field. Names beginning with `_` cannot be assigned as attributes. An
unknown name raises `KeyError`.

| Field type | Accepted values |
| --- | --- |
| numeric scalar | `int`, `float`, `bool`, a numpy scalar, or a `str` that parses as the field type. Integer fields truncate floats |
| string | `str` |
| boolean | `bool` |
| scalar array | anything `numpy.asarray(x, dtype).ravel()` accepts, including a bare scalar (a one-element array) |
| string array | an iterable of `str`, or one `str` (a one-element array) |
| structure | a `dict` of fields, or a `Value` |
| structure array | a list of dicts or `Value`s, with `None` for a null element |
| union | `(member_name, value)` selects that member; a bare value picks the first member that accepts its Python type (string for `str`, bool for `bool`, any numeric for `int`, float or double for `float`, an array member for an array, a structure member for a `dict`); `None` unselects. No match raises `ValueError` |
| variant | any scalar, string, array or dict; the stored type is inferred from the Python type (`int` becomes long, `float` double) |

`select(field, selector=None)` selects union member `selector` of `field`
(or unselects with `None`) without assigning a value.

### Change marks

| Method | Meaning |
| --- | --- |
| `changed(*fields)` | `True` if any of `fields` is marked, or an ancestor or descendant of it is. With no argument, `True` if anything under this view is marked |
| `changedSet(expand=False, parents=False)` | the marked field paths relative to this view. `expand=True` replaces a marked structure by all of its leaves; `parents=True` adds the ancestors of every marked leaf |
| `mark(field=None, val=True)` | mark `field`; with no field, mark this whole view. `val=False` unmarks |
| `unmark(field=None)` | `mark(field, False)` |

Marking a structure covers all its members: `changedSet()` lists the
structure's own path, `changedSet(expand=True)` its leaves, and unmarking
one leaf does not undo the structure mark. Marking the root (a bare
`mark()`) sets the root's own bit, whose path is the empty string, so
`changedSet()` is then empty while `changed()` is `True` and
`changedSet(expand=True)` lists every leaf.

```python
import numpy as np
from repics.pva import Type, Value

T = Type([("value", "d"), ("alarm", ("S", "alarm_t", [("severity", "i"), ("message", "s")])),
          ("arr", "ad"), ("names", "as"), ("any", "v"), ("choice", ("U", None, [("i", "i"), ("s", "s")]))])
V = Value(T)
print(sorted(V.changedSet()), V.changed())
V.value = "2.5"               # strings are parsed to the field type
V.arr = [1, 2, 3]
V["names"] = "one two"        # one string becomes a one-element array
V.any = 5
V.choice = ("s", "picked")
print(sorted(V.changedSet()), V.changed("value"), V.changed("alarm"))
print(sorted(V.changedSet(expand=True, parents=True)))
arr = V.arr
print(type(arr).__name__, arr.dtype, arr.flags.writeable, arr)
print(V.names, V.any, V.choice, V.type("any"), V.type("choice"))
V.choice = 7                  # picks the first member that accepts an int
print(V.choice, sorted(V.changedSet()))
V.select("choice", None); print(V.choice)

sub = V.alarm                 # a view sharing the root and its marks
sub.message = "hi"
print(V.alarm.message, sorted(V.changedSet()))
V.unmark(); print(sorted(V.changedSet()))
V.mark("alarm"); print(sorted(V.changedSet(expand=True)))
V.unmark("alarm.message"); print(sorted(V.changedSet(expand=True)))

print(V.todict())
print(V.todict(["value", "alarm"]))
print(V.tolist())
print(V.get("missing", 0), V.has("value"), sorted(V.keys()), len(V))
print(repr(V))
print(V.tostr())
try:
    V.nope = 1
except Exception as e:
    print(type(e).__name__, e)
```

```
[] False
['any', 'arr', 'choice', 'names', 'value'] True False
['any', 'arr', 'choice', 'names', 'value']
ndarray float64 False [1. 2. 3.]
['one two'] 5 picked v ('U', None, [('i', 'i'), ('s', 's')])
7 ['any', 'arr', 'choice', 'names', 'value']
None
hi ['alarm.message', 'any', 'arr', 'choice', 'names', 'value']
[]
['alarm.message', 'alarm.severity']
['alarm.message', 'alarm.severity']
{'value': 2.5, 'alarm': {'severity': 0, 'message': 'hi'}, 'arr': array([1., 2., 3.]), 'names': ['one two'], 'any': 5, 'choice': None}
{'value': 2.5, 'alarm': {'severity': 0, 'message': 'hi'}}
[('value', 2.5), ('alarm', [('severity', 0), ('message', 'hi')]), ('arr', array([1., 2., 3.])), ('names', ['one two']), ('any', 5), ('choice', None)]
0 True ['alarm', 'any', 'arr', 'choice', 'names', 'value'] 6
Value("", struct {
    double value = 2.5
    struct "alarm_t" {
        int32_t severity = 0
        string message = "hi"
    } alarm
    double[] arr = {3}[1, 2, 3]
    string[] names = {1}["one two"]
    any any int64_t = 5
    union choice null
})
struct {
    double value = 2.5
    struct "alarm_t" {
        int32_t severity = 0
        string message = "hi"
    } alarm
    double[] arr = {3}[1, 2, 3]
    string[] names = {1}["one two"]
    any any int64_t = 5
    union choice null
}

KeyError 'no such member field "nope"'
```

### Arrays

```python
import numpy as np
from repics.pva import Type, Value

T = Type([("f", "af"), ("b", "a?"), ("strs", "as"), ("structs", ("aS", "row", [("x", "i")])), ("vs", "av")])
V = Value(T)
V.f = np.arange(4)              # cast to float32
V.b = [True, False]
V.strs = ["a", "b"]
V.structs = [{"x": 1}, None, {"x": 3}]
V.vs = [1, "two", 3.0]
print(V.f, V.f.dtype, V.b, V.strs, V.structs, V.vs)
print(V.structs[0], type(V.structs[0]).__name__)
V2 = Value(T, V)                # copy-construct from another Value
print(V2.f)
V.f = np.float32(7)             # numpy scalar
print(V.f)
V.f = 1.5                       # a scalar into an array field
print(V.f)
```

```
[0. 1. 2. 3.] float32 [ True False] ['a', 'b'] [Value("row", struct "row" {
    int32_t x = 1
}), None, Value("row", struct "row" {
    int32_t x = 3
})] [1, 'two', 3.0]
struct "row" {
    int32_t x = 1
}
 Value
[0. 1. 2. 3.]
[7.]
[1.5]
```

Values nested inside unions, variants and structure arrays are copies
when read: assigning into `V.structs[0]` does not change `V`. Assign the
whole field instead.
