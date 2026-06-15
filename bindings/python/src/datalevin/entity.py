"""Lazy Datalevin entity wrapper."""

from __future__ import annotations

from collections.abc import Mapping

import jpype
from jpype.types import JByte

from ._convert import to_python
from ._interop import _BINDINGS
from ._java import call_java, classes, is_java_object

_MISSING = object()


def _entity_value(value):
    if value is None or isinstance(value, (bool, int, float, str, bytes)):
        return value

    if not is_java_object(value):
        if isinstance(value, Mapping):
            return {_entity_value(key): _entity_value(item) for key, item in value.items()}
        if isinstance(value, (set, frozenset)):
            return {_entity_value(item) for item in value}
        if isinstance(value, (list, tuple)):
            return [_entity_value(item) for item in value]
        return value

    if _BINDINGS.entity_is(value):
        return Entity(value)

    cls = classes()
    byte_array_type = jpype.JArray(JByte)

    if isinstance(value, byte_array_type):
        return bytes(value)

    if isinstance(value, cls.map_type) or hasattr(value, "entrySet"):
        result = {}
        for entry in value.entrySet():
            result[to_python(entry.getKey())] = _entity_value(entry.getValue())
        return result

    if isinstance(value, cls.set_type):
        return {_entity_value(item) for item in value}

    if isinstance(value, (cls.list_type, cls.collection_type)) or hasattr(value, "iterator"):
        return [_entity_value(item) for item in value]

    return to_python(value)


class Entity(Mapping):
    """Lazy entity returned by `Connection.entity()`."""

    def __init__(self, handle) -> None:
        self._handle = handle

    def raw_handle(self):
        return self._handle

    @property
    def id(self):
        """Return the Datalevin entity id."""

        return to_python(_BINDINGS.entity_id(self._handle))

    def get(self, attr, default=None):
        if not self.__contains__(attr):
            return default
        return _entity_value(_BINDINGS.entity_get(self._handle, attr))

    def touch(self):
        """Materialize all current entity attributes into a Python dict."""

        return to_python(_BINDINGS.entity_touch(self._handle))

    def __getitem__(self, attr):
        value = self.get(attr, _MISSING)
        if value is _MISSING:
            raise KeyError(attr)
        return value

    def __contains__(self, attr):
        return _BINDINGS.entity_contains(self._handle, attr)

    def __iter__(self):
        return iter(self.touch())

    def __len__(self):
        return len(self.touch())

    def __eq__(self, other):
        if not isinstance(other, Entity):
            return NotImplemented
        return bool(call_java(self._handle.equals, other.raw_handle()))

    def __hash__(self):
        return int(call_java(self._handle.hashCode))

    def __repr__(self) -> str:
        return "<Entity lazy>"
