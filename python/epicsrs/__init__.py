"""epicsrs — EPICS Channel Access and pvAccess for Python, built on epics-rs.

Front ends:

* ``epicsrs.ca``  — blocking ``caget`` / ``caput`` / ``camonitor`` / ``cainfo`` / ``connect``
* ``epicsrs.aio`` — the same, as coroutines for asyncio
* ``epicsrs.pva`` — pvAccess client (``Context``), ``pva.asyncio``, ``pva.nt``, ``pva.server``

Both return augmented values (see ``epicsrs.Augmented``) and share one
default context built from the ``EPICS_CA_*`` environment on first use.
"""

from . import aio, ca, pva
from ._context import context
from ._epicsrs import (
    CaChannel,
    CaContext,
    CaDisconnected,
    CaError,
    CaSubscription,
    CaTimeout,
    ChannelInfo,
    PvaDisconnected,
    PvaError,
    PvaRemoteError,
    PvaTimeout,
    Snapshot,
    __version__,
)
from ._value import (
    Augmented,
    AugmentedArray,
    AugmentedFloat,
    AugmentedInt,
    AugmentedList,
    AugmentedStr,
)

__all__ = [
    "__version__",
    "aio",
    "ca",
    "pva",
    "context",
    "CaChannel",
    "CaContext",
    "CaDisconnected",
    "CaError",
    "CaSubscription",
    "CaTimeout",
    "ChannelInfo",
    "PvaDisconnected",
    "PvaError",
    "PvaRemoteError",
    "PvaTimeout",
    "Snapshot",
    "Augmented",
    "AugmentedArray",
    "AugmentedFloat",
    "AugmentedInt",
    "AugmentedList",
    "AugmentedStr",
]
