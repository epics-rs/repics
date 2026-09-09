"""`python/epicsrs/_epicsrs.pyi` must describe the built extension exactly.

No type checker is assumed: the stub is parsed with `ast` and compared
with the live module, name by name and parameter by parameter.
"""

from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path

import pytest

import epicsrs  # noqa: F401  # applies the `CaError.status` property
from epicsrs import _epicsrs as ext

STUB = Path(epicsrs.__file__).with_name("_epicsrs.pyi")

# Dunders the stub must spell out when the class defines them itself.
DUNDERS = {
    "__call__",
    "__contains__",
    "__enter__",
    "__eq__",
    "__exit__",
    "__getattr__",
    "__getitem__",
    "__hash__",
    "__iter__",
    "__len__",
    "__repr__",
    "__setattr__",
    "__setitem__",
    "__str__",
}


def _stub_classes() -> tuple[dict[str, ast.ClassDef], set[str]]:
    tree = ast.parse(STUB.read_text())
    classes: dict[str, ast.ClassDef] = {}
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            classes[node.name] = node
            names.add(node.name)
        elif isinstance(node, ast.FunctionDef):
            names.add(node.name)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
    return classes, names


def _stub_members(cls: ast.ClassDef) -> dict[str, ast.FunctionDef | None]:
    """name -> FunctionDef (None for a bare annotation)."""
    out: dict[str, ast.FunctionDef | None] = {}
    for node in cls.body:
        if isinstance(node, ast.FunctionDef):
            out[node.name] = node
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            out[node.target.id] = None
    return out


def _live_members(cls: type) -> set[str]:
    own = set(vars(cls))
    plain = {k for k in own if not (k.startswith("__") and k.endswith("__"))}
    dunders = own & DUNDERS
    if "__new__" in own or "__init__" in own and cls.__init__ is not object.__init__:
        dunders.add("__init__")
    if "__getattribute__" in own:
        # pyo3 installs a `__getattr__` as the `tp_getattro` slot.
        dunders.add("__getattr__")
    return plain | dunders


def _text_params(sig: str) -> list[tuple[str, bool]]:
    """`($self, a, b=1)` -> [("a", False), ("b", True)]."""
    inner = sig.strip()[1:-1]
    out = []
    for part in [p.strip() for p in inner.split(",") if p.strip()]:
        if part in ("$self", "/", "*"):
            continue
        name, has_default = (part.split("=", 1)[0], "=" in part)
        out.append((name.lstrip("*"), has_default))
    return out


def _stub_params(fn: ast.FunctionDef) -> list[tuple[str, bool]]:
    a = fn.args
    params = [p.arg for p in a.args if p.arg not in ("self", "cls")]
    n_defaults = len(a.defaults)
    out = [(name, i >= len(params) - n_defaults) for i, name in enumerate(params)]
    if a.vararg:
        out.append((a.vararg.arg, False))
    for p, d in zip(a.kwonlyargs, a.kw_defaults):
        out.append((p.arg, d is not None))
    return out


CLASSES, NAMES = _stub_classes()
LIVE_NAMES = {n for n in dir(ext) if not n.startswith("_")} | {"__version__"}


def test_module_names_match():
    assert NAMES == LIVE_NAMES


@pytest.mark.parametrize("name", sorted(CLASSES))
def test_class_members_match(name: str):
    cls = getattr(ext, name)
    assert inspect.isclass(cls)
    stub = _stub_members(CLASSES[name])
    assert set(stub) == _live_members(cls), name


@pytest.mark.parametrize("name", sorted(CLASSES))
def test_class_bases_match(name: str):
    cls = getattr(ext, name)
    bases = [b.id for b in CLASSES[name].bases if isinstance(b, ast.Name)]
    live = [b.__name__ for b in cls.__bases__ if b is not object]
    assert bases == live, name


@pytest.mark.parametrize("name", sorted(CLASSES))
def test_signatures_match(name: str):
    cls = getattr(ext, name)
    stub = _stub_members(CLASSES[name])
    for member, fn in stub.items():
        if fn is None:
            continue
        if member == "__init__":
            sig = cls.__text_signature__
            if sig is None:
                continue  # exception classes: inherited __init__
        else:
            live = "__getattribute__" if member == "__getattr__" else member
            sig = getattr(getattr(cls, live), "__text_signature__", None)
        if sig is None or not re.match(r"^\(.*\)$", sig) or "*args" in sig:
            continue  # getset descriptors, and slot wrappers carrying no real signature
        is_property = any(isinstance(d, ast.Name) and d.id == "property" for d in fn.decorator_list)
        if is_property:
            continue
        assert _stub_params(fn) == _text_params(sig), f"{name}.{member}: stub {ast.unparse(fn.args)} vs {sig}"


@pytest.mark.parametrize("name", sorted(CLASSES))
def test_properties_are_properties(name: str):
    cls = getattr(ext, name)
    for member, fn in _stub_members(CLASSES[name]).items():
        if fn is None:
            continue
        is_property = any(isinstance(d, ast.Name) and d.id == "property" for d in fn.decorator_list)
        live = vars(cls).get(member)
        live_is_data = isinstance(live, property) or type(live).__name__ in ("getset_descriptor", "member_descriptor")
        assert is_property == live_is_data, f"{name}.{member}"
