"""Python <-> Java conversion helpers for Datalevin interop."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal
import uuid as py_uuid

import jpype
from jpype.types import JByte, JLong

from ._java import classes, is_java_object

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1


def to_java(value):
    """Recursively convert Python values into Java/JVM-friendly values."""

    if value is None or isinstance(value, (bool, float, str)):
        return value
    if is_java_object(value):
        return value

    raw_handle = getattr(value, "raw_handle", None)
    if callable(raw_handle):
        return raw_handle()

    if isinstance(value, int):
        if INT64_MIN <= value <= INT64_MAX:
            return JLong(value)
        return classes().big_integer(str(value))

    if isinstance(value, Decimal):
        return classes().big_decimal(str(value))

    if isinstance(value, py_uuid.UUID):
        return classes().uuid.fromString(str(value))

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        iso = value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        return classes().instant.parse(iso)

    if isinstance(value, (bytes, bytearray, memoryview)):
        return jpype.JArray(JByte)(bytes(value))

    if isinstance(value, Mapping):
        jmap = classes().linked_hash_map()
        for key, item in value.items():
            jmap.put(to_java(key), to_java(item))
        return jmap

    if isinstance(value, (set, frozenset)):
        jset = classes().linked_hash_set()
        for item in value:
            jset.add(to_java(item))
        return jset

    if isinstance(value, (list, tuple)):
        jlist = classes().array_list()
        for item in value:
            jlist.add(to_java(item))
        return jlist

    return value


def to_python(value):
    """Recursively convert Java results into Python-native values."""

    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value

    if not is_java_object(value):
        return value

    cls = classes()
    byte_array_type = jpype.JArray(JByte)

    if isinstance(value, byte_array_type):
        return bytes(value)

    if isinstance(value, cls.uuid):
        return py_uuid.UUID(str(value))

    if isinstance(value, cls.instant):
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))

    if isinstance(value, cls.date):
        return datetime.fromtimestamp(value.getTime() / 1000.0, tz=timezone.utc)

    if isinstance(value, cls.big_integer):
        return int(str(value))

    if isinstance(value, cls.big_decimal):
        return Decimal(str(value))

    if isinstance(value, (cls.keyword_type, cls.symbol_type)):
        return str(value)

    if isinstance(value, cls.map_type) or hasattr(value, "entrySet"):
        result = {}
        for entry in value.entrySet():
            result[to_python(entry.getKey())] = to_python(entry.getValue())
        return result

    if isinstance(value, cls.set_type):
        return {to_python(item) for item in value}

    if isinstance(value, (cls.list_type, cls.collection_type)) or hasattr(value, "iterator"):
        return [to_python(item) for item in value]

    return value
