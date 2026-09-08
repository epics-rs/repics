"""A pyepics-shaped ``PV``: one object per channel with a cached value,
keyword-argument callbacks and the familiar property names.

This is a thin shell over ``epicsrs.ca``: reads raise on failure (pyepics
returns ``None``); ``get(use_monitor=True)`` returns the last monitored
value when a monitor is running; callbacks run on the ``epicsrs.ca``
monitor dispatcher thread with pyepics' keyword arguments (``pvname``,
``value``, ``char_value``, ``timestamp``, ``severity``, ``status``,
``units``, ``enum_strs``, ...). Every ``PV`` is one hub subscription: it
follows the channel's connection and access rights for its whole life and
monitors values when ``auto_monitor`` says so.
"""

from __future__ import annotations

import threading
from typing import Any, Callable

import numpy

from . import ca
from ._context import channel
from ._dbr import DBR_CHAR, DBR_ENUM, _DATATYPE_STRINGS
from ._value import CaNothing

__all__ = ["PV", "get_pv"]

_CTRL_KEYS = (
    "units",
    "precision",
    "enum_strs",
    "upper_disp_limit",
    "lower_disp_limit",
    "upper_alarm_limit",
    "lower_alarm_limit",
    "upper_warning_limit",
    "lower_warning_limit",
    "upper_ctrl_limit",
    "lower_ctrl_limit",
)
_TIME_KEYS = ("status", "severity", "timestamp", "posixseconds", "nanoseconds")


def _meta(value: Any) -> dict[str, Any]:
    """pyepics' argument dictionary for an augmented value."""
    args: dict[str, Any] = {
        "value": value,
        "status": value.status,
        "severity": value.severity,
        "timestamp": value.timestamp,
        "posixseconds": value.raw_stamp[0],
        "nanoseconds": value.raw_stamp[1],
        "count": value.element_count,
        "ftype": value.dbr,
        "type": value.datatype,
    }
    for k in _CTRL_KEYS:
        src = "enums" if k == "enum_strs" else k
        v = getattr(value, src, None)
        if v is not None:
            args[k] = v
    return args


class _Subscription(ca.Subscription):
    """The PV's one subscription: values to ``_on_update``, lifecycle to
    the PV's connection and access callbacks."""

    def __init__(self, pv: "PV", **kw: Any):
        self._pv = pv
        super().__init__(pv.pvname, pv._on_update, **kw)

    def on_connection(self, connected: bool) -> None:
        self._pv._on_connection(connected)

    def on_access(self, read: bool, write: bool) -> None:
        self._pv._on_access(read, write)


class PV:
    """A channel with a cached value and pyepics-style callbacks.

    ``form`` is the metadata a read or monitor carries (``time`` or
    ``ctrl``); ``auto_monitor`` starts a monitor on connection (the default
    does so for channels of at most ``AUTOMONITOR_MAXLENGTH`` elements; an
    ``int`` is taken as the DBE mask to monitor with).
    """

    AUTOMONITOR_MAXLENGTH = 65536

    def __init__(
        self,
        pvname: str,
        callback: Callable[..., Any] | None = None,
        form: str = "time",
        auto_monitor: bool | int | None = None,
        connection_callback: Callable[..., Any] | None = None,
        connection_timeout: float | None = None,
        access_callback: Callable[..., Any] | None = None,
        count: int | None = None,
    ):
        self.pvname = pvname
        self.form = form
        self.auto_monitor = auto_monitor
        self.connection_timeout = connection_timeout
        self._count = count
        self._args: dict[str, Any] = {"pvname": pvname, "value": None, "char_value": None}
        self._ctrl: dict[str, Any] | None = None
        self._lock = threading.RLock()
        self.callbacks: dict[int, tuple[Callable[..., Any], dict[str, Any]]] = {}
        self.connection_callbacks: list[Callable[..., Any]] = []
        self.access_callbacks: list[Callable[..., Any]] = []
        self._ch = channel(pvname)
        self._monitored = False
        if callback is not None:
            self.add_callback(callback)
        if connection_callback is not None:
            self.connection_callbacks.append(connection_callback)
        if access_callback is not None:
            self.access_callbacks.append(access_callback)
        # pyepics lets `auto_monitor` be a DBE mask.
        mask = auto_monitor if isinstance(auto_monitor, int) and not isinstance(auto_monitor, bool) else None
        self._sub: _Subscription | None = _Subscription(
            self,
            form=form,
            datatype=None,
            count=count if count is not None else 0,
            mask=mask,
            all_updates=True,
            notify_disconnect=False,
            connect_timeout=None,
            values=auto_monitor is None or bool(auto_monitor),
            values_max_count=self.AUTOMONITOR_MAXLENGTH if auto_monitor is None else None,
        )

    # -- connection

    def _on_connection(self, connected: bool) -> None:
        for cb in list(self.connection_callbacks):
            cb(pvname=self.pvname, conn=connected, pv=self)

    def _on_access(self, read: bool, write: bool) -> None:
        for cb in list(self.access_callbacks):
            cb(read, write, pv=self)

    @property
    def connected(self) -> bool:
        return self._ch.connected

    def connect(self, timeout: float | None = None) -> bool:
        return self.wait_for_connection(timeout)

    def wait_for_connection(self, timeout: float | None = None) -> bool:
        """True once connected; False if ``timeout`` (or ``connection_timeout``) passes."""
        if timeout is None:
            timeout = self.connection_timeout
        r = ca.connect(self.pvname, timeout=timeout if timeout is not None else 5.0, throw=False)
        return not (isinstance(r, CaNothing) and not r.ok)

    def disconnect(self) -> None:
        """Stop following the channel; it stays cached in the context."""
        with self._lock:
            sub, self._sub = self._sub, None
        if sub is not None:
            sub.close()

    # -- values

    def _on_update(self, value: Any) -> None:
        self._monitored = True
        self._store(value)
        self.run_callbacks()

    def _store(self, value: Any, ctrl: bool = False) -> None:
        with self._lock:
            self._args.update(_meta(value))
            # The control fields are cached before the string rendering
            # asks for precision / enum strings, or that ask would fetch.
            if ctrl or value.units is not None or value.enums is not None:
                self._ctrl = {k: self._args[k] for k in _CTRL_KEYS if k in self._args}
            self._args["char_value"] = self._as_string(value)

    def get(
        self,
        count: int | None = None,
        as_string: bool = False,
        as_numpy: bool = True,
        timeout: float | None = None,
        use_monitor: bool = True,
    ) -> Any:
        """The value: the last monitored one when ``use_monitor`` and a
        monitor is running, else a fresh read. Raises on failure."""
        if use_monitor and count is None and self._monitored and self._args["value"] is not None:
            value = self._args["value"]
        else:
            value = ca.caget(
                self.pvname,
                form=self.form,
                count=count if count is not None else (self._count or 0),
                timeout=timeout if timeout is not None else 5.0,
            )
            self._store(value)
        if as_string:
            return self._as_string(value)
        if isinstance(value, numpy.ndarray) and not as_numpy:
            return value.tolist()
        return value

    def put(
        self,
        value: Any,
        wait: bool = False,
        timeout: float = 30.0,
        use_complete: bool = False,
        callback: Callable[..., Any] | None = None,
        callback_data: Any = None,
    ) -> None:
        """Write. ``wait=True`` returns after processing; ``callback`` runs
        after processing on a helper thread (``callback(pvname=, data=)``)."""
        if callback is None and not use_complete:
            ca.caput(self.pvname, value, wait=wait, timeout=timeout)
            return
        self.put_complete = False

        def done() -> None:
            ca.caput(self.pvname, value, wait=True, timeout=timeout)
            self.put_complete = True
            if callback is not None:
                callback(pvname=self.pvname, data=callback_data)

        if wait:
            done()
        else:
            threading.Thread(target=done, name=f"caput {self.pvname}", daemon=True).start()

    @property
    def value(self) -> Any:
        return self.get()

    @value.setter
    def value(self, v: Any) -> None:
        self.put(v)

    @property
    def char_value(self) -> str:
        if self._args["char_value"] is None:
            self.get()
        return self._args["char_value"]

    def _as_string(self, value: Any) -> str:
        """pyepics' string rendering: enum label, decoded char array,
        precision-formatted float, else ``str``."""
        if isinstance(value, CaNothing):
            return str(value)
        if value.dbr == DBR_ENUM:
            strs = self.enum_strs
            if strs is not None and 0 <= int(value) < len(strs):
                return strs[int(value)]
            return str(int(value))
        if isinstance(value, numpy.ndarray):
            if value.dbr == DBR_CHAR:
                raw = value.astype(numpy.uint8).tobytes()
                nul = raw.find(b"\0")
                return (raw if nul < 0 else raw[:nul]).decode("utf-8", errors="replace")
            return str(value.tolist()) if value.size <= 20 else f"<array size={value.size}, type={value.datatype}>"
        if isinstance(value, float):
            prec = self.precision
            if prec is not None and prec >= 0:
                return f"{value:.{prec}f}"
        return str(value)

    # -- metadata

    def get_ctrlvars(self, timeout: float = 5.0) -> dict[str, Any]:
        """Units, precision, limits and enum strings, read fresh."""
        value = ca.caget(self.pvname, form="ctrl", count=self._count or 0, timeout=timeout)
        self._store(value, ctrl=True)
        return dict(self._ctrl or {})

    def get_timevars(self, timeout: float = 5.0) -> dict[str, Any]:
        value = ca.caget(self.pvname, form="time", count=self._count or 0, timeout=timeout)
        self._store(value)
        return {k: self._args[k] for k in _TIME_KEYS}

    def _ctrlvar(self, key: str) -> Any:
        if self._ctrl is None and self._ch.connected:
            self.get_ctrlvars()
        return self._args.get(key)

    @property
    def units(self) -> str | None:
        return self._ctrlvar("units")

    @property
    def precision(self) -> int | None:
        return self._ctrlvar("precision")

    @property
    def enum_strs(self) -> list[str] | None:
        return self._ctrlvar("enum_strs")

    @property
    def upper_disp_limit(self) -> float | None:
        return self._ctrlvar("upper_disp_limit")

    @property
    def lower_disp_limit(self) -> float | None:
        return self._ctrlvar("lower_disp_limit")

    @property
    def upper_alarm_limit(self) -> float | None:
        return self._ctrlvar("upper_alarm_limit")

    @property
    def lower_alarm_limit(self) -> float | None:
        return self._ctrlvar("lower_alarm_limit")

    @property
    def upper_warning_limit(self) -> float | None:
        return self._ctrlvar("upper_warning_limit")

    @property
    def lower_warning_limit(self) -> float | None:
        return self._ctrlvar("lower_warning_limit")

    @property
    def upper_ctrl_limit(self) -> float | None:
        return self._ctrlvar("upper_ctrl_limit")

    @property
    def lower_ctrl_limit(self) -> float | None:
        return self._ctrlvar("lower_ctrl_limit")

    @property
    def status(self) -> int | None:
        return self._args.get("status")

    @property
    def severity(self) -> int | None:
        return self._args.get("severity")

    @property
    def timestamp(self) -> float | None:
        return self._args.get("timestamp")

    @property
    def posixseconds(self) -> int | None:
        return self._args.get("posixseconds")

    @property
    def nanoseconds(self) -> int | None:
        return self._args.get("nanoseconds")

    @property
    def count(self) -> int | None:
        return self._ch.element_count

    nelm = count

    @property
    def ftype(self) -> int | None:
        return self._ch.dbr

    @property
    def type(self) -> str | None:
        t = self._ch.dbr
        return None if t is None else _DATATYPE_STRINGS[t]

    @property
    def host(self) -> str | None:
        return self._ch.info().host if self._ch.connected else None

    @property
    def read_access(self) -> bool:
        return self._ch.connected and self._ch.info().read_access

    @property
    def write_access(self) -> bool:
        return self._ch.connected and self._ch.info().write_access

    @property
    def access(self) -> str:
        return {
            (False, False): "no access",
            (True, False): "read-only",
            (False, True): "write-only",
            (True, True): "read/write",
        }[(self.read_access, self.write_access)]

    # -- callbacks

    def add_callback(
        self,
        callback: Callable[..., Any],
        index: int | None = None,
        run_now: bool = False,
        with_ctrlvars: bool = True,
        **kw: Any,
    ) -> int:
        """Register ``callback(**kw)``; returns its index. ``run_now`` calls
        it at once with the current value."""
        if index is None:
            index = 1 + max(self.callbacks, default=0)
        self.callbacks[index] = (callback, kw)
        if with_ctrlvars and self._ctrl is None and self._ch.connected:
            self.get_ctrlvars()
        if run_now:
            if self._args["value"] is None and self._ch.connected:
                self.get()
            self.run_callback(index)
        return index

    def remove_callback(self, index: int) -> None:
        self.callbacks.pop(index, None)

    def clear_callbacks(self) -> None:
        self.callbacks.clear()

    def run_callbacks(self) -> None:
        for index in list(self.callbacks):
            self.run_callback(index)

    def run_callback(self, index: int) -> None:
        entry = self.callbacks.get(index)
        if entry is None:
            return
        fcn, kw = entry
        args = dict(self._args)
        args.update(kw)
        args["cb_info"] = (index, self)
        fcn(**args)

    # -- text

    @property
    def info(self) -> str:
        v = self.get()
        lines = [
            f"== {self.pvname}  ({self.type}) ==",
            f"   value      = {v!r}",
            f"   char_value = {self.char_value!r}",
            f"   count      = {self.count}",
            f"   nelm       = {self.nelm}",
            f"   type       = {self.type}",
            f"   units      = {self.units}",
            f"   precision  = {self.precision}",
            f"   host       = {self.host}",
            f"   access     = {self.access}",
            f"   status     = {self.status}",
            f"   severity   = {self.severity}",
            f"   timestamp  = {self.timestamp}",
        ]
        if self.enum_strs:
            lines.append(f"   enum strings: {self.enum_strs}")
        return "\n".join(lines)

    def __repr__(self) -> str:
        if not self.connected:
            return f"<PV {self.pvname!r}: not connected>"
        return f"<PV {self.pvname!r}, count={self.count}, type={self.type}, access={self.access}>"

    def __str__(self) -> str:
        return self.pvname


_pvs: dict[tuple[str, str], PV] = {}
_pvs_lock = threading.Lock()


def get_pv(pvname: str, form: str = "time", connect: bool = False, timeout: float = 5.0, **kw: Any) -> PV:
    """A cached ``PV`` per (name, form); ``connect=True`` waits for it."""
    with _pvs_lock:
        pv = _pvs.get((pvname, form))
        if pv is None:
            pv = PV(pvname, form=form, **kw)
            _pvs[(pvname, form)] = pv
    if connect:
        pv.wait_for_connection(timeout)
    return pv
