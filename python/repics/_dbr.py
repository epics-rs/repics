"""libca vocabulary: DBR type codes, event masks, ECA status codes.

Values are the ``caerr.h`` / ``db_access.h`` numbers, so they interoperate
with code written against ``epicscorelibs`` or pyepics.
"""

from __future__ import annotations

from typing import Any

import numpy

from ._repics import CaError, ca_message

__all__ = [
    "FORMS",
    "DBR_STRING",
    "DBR_SHORT",
    "DBR_INT",
    "DBR_FLOAT",
    "DBR_ENUM",
    "DBR_CHAR",
    "DBR_LONG",
    "DBR_DOUBLE",
    "DBR_CHAR_STR",
    "DBR_CHAR_BYTES",
    "DBR_CHAR_UNICODE",
    "DBR_ENUM_STR",
    "DBR_NO_ACCESS",
    "DBE_VALUE",
    "DBE_LOG",
    "DBE_ALARM",
    "DBE_PROPERTY",
    "ECA_NORMAL",
    "ECA_TIMEOUT",
    "ECA_BADTYPE",
    "ECA_BADCOUNT",
    "ECA_DISCONN",
    "ECA_GETFAIL",
    "ECA_PUTFAIL",
    "ECA_NORDACCESS",
    "ECA_NOWTACCESS",
    "ECA_INTERNAL",
    "ca_message",
    "errorcode",
    "request",
    "form_offset",
]

# --- forms: the DBR class a read asks for, as its offset from the base type

_FORM_OFFSET = {"plain": 0, "sts": 7, "time": 14, "gr": 21, "ctrl": 28}
FORMS = tuple(_FORM_OFFSET)

# --- DBR base types (db_access.h)

DBR_STRING = 0
DBR_SHORT = 1
DBR_INT = 1
DBR_FLOAT = 2
DBR_ENUM = 3
DBR_CHAR = 4
DBR_LONG = 5
DBR_DOUBLE = 6
DBR_NO_ACCESS = 7

# Request markers with no wire type of their own: read a CHAR array and
# hand it back as text / bytes, or read an ENUM as its state label.
DBR_CHAR_STR = 999
DBR_CHAR_BYTES = 998
DBR_CHAR_UNICODE = 997
DBR_ENUM_STR = 996

_DATATYPE_STRINGS = ("string", "short", "float", "enum", "char", "long", "double", "no access")

# --- event masks

DBE_VALUE = 1
DBE_LOG = 2
DBE_ALARM = 4
DBE_PROPERTY = 8

# --- ECA status (caerr.h DEFMSG)

CA_K_WARNING = 0
CA_K_SUCCESS = 1
CA_K_ERROR = 2
CA_K_INFO = 3
CA_K_SEVERE = 4


def _defmsg(sev: int, num: int) -> int:
    return ((num << 3) & 0x0000FFF8) | (sev & 0x00000007)


ECA_NORMAL = _defmsg(CA_K_SUCCESS, 0)
ECA_TIMEOUT = _defmsg(CA_K_WARNING, 10)
ECA_BADTYPE = _defmsg(CA_K_ERROR, 14)
ECA_INTERNAL = _defmsg(CA_K_ERROR | CA_K_SEVERE, 17)
ECA_GETFAIL = _defmsg(CA_K_WARNING, 19)
ECA_PUTFAIL = _defmsg(CA_K_WARNING, 20)
ECA_BADCOUNT = _defmsg(CA_K_WARNING, 22)
ECA_DISCONN = _defmsg(CA_K_WARNING, 24)
ECA_NORDACCESS = _defmsg(CA_K_WARNING, 46)
ECA_NOWTACCESS = _defmsg(CA_K_WARNING, 47)


def errorcode(exc: BaseException) -> int:
    """The ECA status a raised ``CaError`` carries (``args[1]``)."""
    if isinstance(exc, CaError) and len(exc.args) > 1 and isinstance(exc.args[1], int):
        return exc.args[1]
    return ECA_INTERNAL


def _status(self: CaError) -> int:
    return errorcode(self)


CaError.status = property(_status)  # type: ignore[attr-defined]

# --- datatype requests


_PY_BASE: dict[Any, int] = {
    str: DBR_STRING,
    int: DBR_LONG,
    float: DBR_DOUBLE,
    bool: DBR_LONG,
}
_NUMPY_BASE: dict[Any, int] = {
    numpy.dtype(numpy.float64): DBR_DOUBLE,
    numpy.dtype(numpy.float32): DBR_FLOAT,
    numpy.dtype(numpy.int32): DBR_LONG,
    numpy.dtype(numpy.int16): DBR_SHORT,
    numpy.dtype(numpy.uint8): DBR_CHAR,
    numpy.dtype(numpy.str_): DBR_STRING,
}


def form_offset(form: str) -> int:
    try:
        return _FORM_OFFSET[form]
    except (KeyError, TypeError):
        raise ValueError(f"unknown form {form!r}; expected one of {', '.join(FORMS)}") from None


def request(datatype: Any, form: str) -> tuple[int | None, int, bool, int | None]:
    """What a read asks for: ``(base, offset, enum_as_string, marker)``.

    ``base`` is the DBR base code or ``None`` for the channel's native
    type; ``offset`` the class offset of ``form``; ``enum_as_string`` asks
    for an ENUM's label; ``marker`` is ``None`` or the ``DBR_CHAR_*``
    request that asked for text. ``datatype`` is ``None`` (native), a
    Python type (``str``/``int``/``float``), a numpy dtype, a ``DBR_*``
    base code or one of the request markers.
    """
    offset = form_offset(form)
    if datatype is None:
        return None, offset, False, None
    if datatype in (DBR_CHAR_STR, DBR_CHAR_BYTES, DBR_CHAR_UNICODE):
        return DBR_CHAR, offset, False, datatype
    if datatype == DBR_ENUM_STR:
        return None, offset, True, None
    if isinstance(datatype, type) and datatype in _PY_BASE:
        return _PY_BASE[datatype], offset, False, None
    if isinstance(datatype, int) and 0 <= datatype <= DBR_DOUBLE:
        return datatype, offset, False, None
    try:
        return _NUMPY_BASE[numpy.dtype(datatype)], offset, False, None
    except (TypeError, KeyError):
        raise TypeError(f"unsupported datatype {datatype!r}") from None
