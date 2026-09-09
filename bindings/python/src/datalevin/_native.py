"""Runtime-only native value adapters; storage still uses registered payload bytes."""

from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Mapping
from contextvars import ContextVar
from functools import wraps
from inspect import isfunction
from uuid import uuid4
from weakref import WeakValueDictionary

import jpype
from jpype.types import JByte

from ._udf_value import UdfDescriptor

_REGISTRY = ContextVar("datalevin_native_registry", default=None)
_CODECS = WeakValueDictionary()


def current_registry():
    return _REGISTRY.get()


@contextmanager
def native_scope(registry):
    token = _REGISTRY.set(registry)
    try:
        yield
    finally:
        _REGISTRY.reset(token)


def native_methods(cls):
    """Scope argument conversion to the owning handle, including nested calls."""
    def scoped(fn):
        @wraps(fn)
        def call(self, *args, **kwargs):
            registry = self._native_registry
            if registry is current_registry():
                return fn(self, *args, **kwargs)
            with native_scope(registry):
                return fn(self, *args, **kwargs)
        return call

    for name, fn in vars(cls).copy().items():
        if not name.startswith("_") and isfunction(fn):
            setattr(cls, name, scoped(fn))
    return cls


def registry_from_opts(opts, dir=None):
    opts = opts or {}
    runtime = opts.get(":runtime-opts", opts.get("runtime-opts", {})) or {}
    registry = runtime.get(":udf-registry", runtime.get("udf-registry"))
    if not hasattr(registry, "_native_types"):
        return None
    if registry._native_types and str(dir).startswith("dtlv://"):
        raise ValueError("Native Python value transport currently requires a local database")
    return registry


def _field(mapping, key):
    return mapping.get(key, mapping.get(":" + key))


class NativeCodec:
    def __init__(self, registry, type_name, native_type, definition):
        if not isinstance(native_type, type):
            raise TypeError("native_type must be a Python class")
        name = str(type_name).removeprefix(":")
        if "/" not in name or not all(name.split("/", 1)):
            raise ValueError("Native type name must be a namespaced keyword")
        payload = _field(definition, "payload")
        if not isinstance(payload, Mapping):
            raise ValueError("Native types require serialize and deserialize UDF descriptors")
        self.serialize = UdfDescriptor.from_value(_field(payload, "serialize"), default_lang="python")
        self.deserialize = UdfDescriptor.from_value(_field(payload, "deserialize"), default_lang="python")
        if self.serialize.kind != ":serializer" or self.deserialize.kind != ":deserializer":
            raise ValueError("Native types require serializer and deserializer UDF kinds")
        # Callbacks retain only Python binding state, not a Java registry handle.
        # Otherwise Java proxy -> Python registry -> Java registry forms a cycle
        # that neither runtime's collector can break after the database closes.
        self.registry = registry._native_bindings
        self.native_type = native_type
        self.type_name = ":" + name
        self.codec_id = str(uuid4())
        _CODECS[self.codec_id] = self

    def _function(self, descriptor):
        fn = self.registry._functions.get(descriptor)
        if fn is None:
            raise ValueError(f"Missing native type UDF binding: {descriptor}")
        return fn

    def decode(self, payload):
        value = self._function(self.deserialize)(bytes(payload))
        self.check_value(value)
        return value

    def check_value(self, value):
        if type(value) is not self.native_type:
            raise TypeError(f"Deserializer for {self.type_name} must return {self.native_type.__name__}")

    def encode(self, value):
        payload = self._function(self.serialize)(value)
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise TypeError("Custom serializer must return a byte array (bytes-like value)")
        return bytes(payload)

    def test(self, left, right):
        try:
            # Each side may belong to a different database/codec. Decode with
            # its own binding before applying Python's ordinary equality.
            return bool(native_to_python(left) == native_to_python(right))
        except jpype.JException:
            raise
        except Exception as exc:
            raise jpype.JClass("java.lang.IllegalArgumentException")(
                f"Native Python equality failed: {type(exc).__name__}: {exc}"
            ) from exc

    def wrap(self, value):
        return self.wrap_payload(self.encode(value))

    def wrap_payload(self, payload):
        equality = jpype.JProxy("java.util.function.BiPredicate", inst=self)
        return jpype.JClass("datalevin.NativeValue")(
            self.codec_id, self.type_name, jpype.JArray(JByte)(payload), equality
        )


def native_to_java(value):
    registry = current_registry()
    codec = registry._native_types.get(type(value)) if registry is not None else None
    return codec.wrap(value) if codec is not None else None


def native_to_python(value):
    codec = _CODECS.get(str(value.codecId()))
    if codec is None:
        raise ValueError(f"Missing native type binding for {value.typeName()}")
    return codec.decode(value.payload())
