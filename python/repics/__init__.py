"""repics — EPICS Channel Access and pvAccess for Python, built on epics-rs.

Front ends:

* ``repics.ca``  — blocking ``caget`` / ``caput`` / ``camonitor`` / ``cainfo`` / ``connect``
* ``repics.ca.asyncio`` — the same, as coroutines for asyncio
* ``repics.ca.server`` — a CA server (``SharedPV`` / ``Server``), ``ca.server.asyncio``
* ``repics.pv``  — a pyepics-shaped ``PV`` object over the blocking front end
* ``repics.pva`` — pvAccess client (``Context``), ``pva.asyncio``, ``pva.nt``, ``pva.server``

Both return augmented values (see ``repics.Augmented``) and share one
default context built from the ``EPICS_CA_*`` environment on first use.
"""

from . import ca, pv, pva
from ._context import ChannelStatus, context, get_channel_infos, purge_channel_caches
from ._dbr import *  # noqa: F401,F403 - DBR_* / DBE_* / ECA_* are the public vocabulary
from ._dbr import __all__ as _dbr_all
from ._repics import (
    CaChannel,
    CaContext,
    CaDisconnected,
    CaError,
    CaEvents,
    CaSubscription,
    CaTimeout,
    ChannelInfo,
    ConnectionEvent,
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
    AugmentedBytes,
    AugmentedFloat,
    AugmentedInt,
    AugmentedList,
    AugmentedStr,
    CAInfo,
    CaNothing,
)

__all__ = [
    "__version__",
    "ca",
    "pv",
    "pva",
    "context",
    "get_channel_infos",
    "purge_channel_caches",
    "ChannelStatus",
    "CaChannel",
    "CaContext",
    "CaDisconnected",
    "CaError",
    "CaEvents",
    "CaNothing",
    "CaSubscription",
    "CaTimeout",
    "CAInfo",
    "ChannelInfo",
    "ConnectionEvent",
    "PvaDisconnected",
    "PvaError",
    "PvaRemoteError",
    "PvaTimeout",
    "Snapshot",
    "Augmented",
    "AugmentedArray",
    "AugmentedBytes",
    "AugmentedFloat",
    "AugmentedInt",
    "AugmentedList",
    "AugmentedStr",
    *_dbr_all,
]
