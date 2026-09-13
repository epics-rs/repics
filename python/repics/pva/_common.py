"""Shared by the blocking and asyncio pvAccess contexts."""

from __future__ import annotations

import logging
import os
from functools import partial
from typing import Any, Callable

from .._repics import PvaDisconnected, PvaError, PvaRemoteError, PvaTimeout, Value
from .._value import Augmented
from .nt import ClientUnwrapper, buildNT

log = logging.getLogger("repics.pva")

Disconnected = PvaDisconnected
RemoteError = PvaRemoteError
TimeoutError = PvaTimeout  # noqa: A001 - p4p spells it this way


class Finished(PvaError):
    """The server ended the subscription."""


class Cancelled(PvaError):
    """The operation was cancelled locally."""


# What ``useenv=False`` means: pvxs defaults for every key the context reads.
_NOENV = {
    "EPICS_PVA_ADDR_LIST": "",
    "EPICS_PVA_AUTO_ADDR_LIST": "YES",
    "EPICS_PVA_BROADCAST_PORT": "5076",
    "EPICS_PVA_SERVER_PORT": "5075",
    "EPICS_PVA_NAME_SERVERS": "",
}


def effective_conf(conf: dict | None, useenv: bool) -> dict[str, str]:
    """The ``EPICS_PVA_*`` table the Rust builder is configured from."""
    out = dict(_NOENV)
    if useenv:
        out.update((k, v) for k, v in os.environ.items() if k.startswith("EPICS_PVA"))
    if conf:
        out.update((k, str(v)) for k, v in conf.items())
    return out


def put_request(request: str | None, process: Any, wait: bool | None) -> str | None:
    """p4p's ``record[block=,process=]`` request for ``put(process=, wait=)``."""
    if process is None and wait is None:
        return request
    if request is not None:
        raise ValueError("request= cannot be combined with process=/wait=")
    proc = {None: "passive", True: "true", False: "false"}[process]
    block = "true" if wait else "false"
    return f"field()record[block={block},process={proc}]"


def named(out: Any, name: str) -> Any:
    if isinstance(out, Augmented):
        out._snap.name = name
    return out


class Wrapping:
    """The NT policy of one context: unwrap reads, assign puts."""

    def __init__(self, nt: Any = None, unwrap: Any = None):
        self.unwrapper: ClientUnwrapper = buildNT(nt, unwrap)

    def unwrap(self, V: Value, name: str) -> Any:
        return named(self.unwrapper.unwrap(V), name)

    def assign(self, V: Value, values: Any) -> Value:
        """Fill the (unmarked) current value ``V`` from ``values``."""
        if isinstance(values, Value):
            return values
        V.unmark()
        self.unwrapper.assign(V, values)
        return V


def dispatch(cb: Callable[[Any], Any], item: Any, queue: Any) -> None:
    """Hand ``item`` to ``cb`` directly or through a p4p-style work queue."""
    if queue is None:
        try:
            cb(item)
        except Exception:  # noqa: BLE001 - a callback must not kill the drain loop
            log.exception("pva monitor callback failed")
        return
    push = getattr(queue, "push", None) or queue.put
    push(partial(cb, item))
