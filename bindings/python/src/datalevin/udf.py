"""Python UDF registration over the Datalevin JVM bridge."""

from __future__ import annotations

import jpype

from ._convert import to_java, to_python
from ._interop import _BINDINGS
from ._java import classes, is_java_object
from ._native import NativeCodec, native_scope
from ._udf_value import UdfDescriptor, descriptor_data


class _NativeBindings:
    """Host callables and codecs with no JVM registry/proxy ownership."""

    def __init__(self):
        self._functions = {}
        self._native_types = {}


class _PythonUdfFunction:
    def __init__(self, fn, registry, descriptor):
        self._fn = fn
        self._registry = registry
        self._descriptor = descriptor

    def invoke(self, args):
        try:
            with native_scope(self._registry):
                python_args = [_udf_arg_to_python(arg, index) for index, arg in enumerate(args)]
                result = self._fn(*python_args)
                if self._descriptor.kind == ":deserializer":
                    expected = None
                    for codec in self._registry._native_types.values():
                        if codec.deserialize == self._descriptor:
                            expected = expected or codec
                            # Retain the supplied snapshot; a read needs only
                            # the deserializer, and payloads need not be canonical.
                            if type(result) is codec.native_type:
                                return codec.wrap_payload(python_args[0])
                    if expected is not None:
                        expected.check_value(result)
                return to_java(result)
        except jpype.JException:
            raise
        except Exception as exc:
            # JPype's PyExceptionProxy has no useful message once Clojure wraps
            # it. Supply a JVM exception so storage retains the Python failure.
            raise jpype.JClass("java.lang.IllegalArgumentException")(
                f"Python UDF failed: {type(exc).__name__}: {exc}"
            ) from exc


def _java_class_name(value):
    if not is_java_object(value):
        return None
    try:
        return str(value.getClass().getName())
    except Exception:
        return None


def _udf_arg_to_python(arg, _index: int):
    if _java_class_name(arg) in {
        "datalevin.db.DB",
        "datalevin.DatabaseValue",
    }:
        from .database import Database

        return Database(arg)
    return to_python(arg)


class UdfRegistry:
    """Wrapper around a raw Datalevin UDF registry handle."""

    def __init__(self, handle=None) -> None:
        self._handle = _BINDINGS.create_udf_registry() if handle is None else handle
        self._proxies = {}
        self._native_bindings = _NativeBindings()
        self._functions = self._native_bindings._functions
        self._native_types = self._native_bindings._native_types

    def raw_handle(self):
        return self._handle

    def register(self, descriptor, fn=None):
        """Register ``fn``; bare descriptors default to the Python language."""

        if fn is None:
            def decorator(decorated):
                self.register(descriptor, decorated)
                return decorated

            return decorator
        if not callable(fn):
            raise TypeError("fn must be callable")
        normalized = UdfDescriptor.from_value(descriptor, default_lang="python")
        proxy = jpype.JProxy(classes().udf_function,
                            inst=_PythonUdfFunction(fn, self._native_bindings, normalized))
        _BINDINGS.register_udf(self._handle, normalized, proxy)
        self._proxies[normalized] = proxy
        self._functions[normalized] = fn
        return fn

    def unregister(self, descriptor):
        normalized = UdfDescriptor.from_value(descriptor, default_lang="python")
        _BINDINGS.unregister_udf(self._handle, normalized)
        self._proxies.pop(normalized, None)
        self._functions.pop(normalized, None)

    def bind_native_type(self, type_name, native_type, definition):
        """Bind a Python class to a registered type's payload UDFs.

        Call before opening a database with this registry in runtime
        options. This runtime-only binding is separate from ``register_type``;
        recreate it on reopen. Ordinary KV/query calls then accept instances
        of the class and return reconstructed instances automatically.
        """
        codec = NativeCodec(self, type_name, native_type, definition)
        existing = self._native_types.get(native_type)
        if existing is not None:
            if (existing.type_name, existing.serialize, existing.deserialize) != (
                    codec.type_name, codec.serialize, codec.deserialize):
                raise ValueError("Python class already has a different native type binding")
            return self
        if any(c.type_name == codec.type_name for c in self._native_types.values()):
            raise ValueError("Custom type already has a different Python class binding")
        if native_type.__module__ == "builtins":
            raise ValueError("Bind a custom Python class, not a built-in type")
        _BINDINGS.bind_native_type(self._handle, codec.type_name, codec.deserialize)
        self._native_types[native_type] = codec
        return self

    def registered(self, descriptor) -> bool:
        normalized = UdfDescriptor.from_value(descriptor, default_lang="python")
        return _BINDINGS.registered_udf(self._handle, normalized)

    def query_udf(self, udf_id: str, *, lang="python", version=None):
        def decorator(fn):
            self.register(
                UdfDescriptor.query_fn(udf_id, lang=lang, version=version), fn
            )
            return fn

        return decorator

    def predicate_udf(self, udf_id: str, *, lang="python", version=None):
        def decorator(fn):
            self.register(
                UdfDescriptor.predicate(udf_id, lang=lang, version=version), fn
            )
            return fn

        return decorator

    def tx_udf(self, udf_id: str, *, lang="python", version=None):
        def decorator(fn):
            self.register(UdfDescriptor.tx_fn(udf_id, lang=lang, version=version), fn)
            return fn

        return decorator

    def analyzer_udf(self, udf_id: str, *, lang="python", version=None):
        def decorator(fn):
            self.register(
                UdfDescriptor.analyzer(udf_id, lang=lang, version=version), fn
            )
            return fn

        return decorator

    def query_analyzer_udf(self, udf_id: str, *, lang="python", version=None):
        def decorator(fn):
            self.register(
                UdfDescriptor.query_analyzer(
                    udf_id, lang=lang, version=version
                ),
                fn,
            )
            return fn

        return decorator

    def order_udf(self, udf_id: str, *, lang="python", version=None):
        """Register a function producing a custom type's ordered backing value."""
        def decorator(fn):
            self.register(UdfDescriptor.order_fn(udf_id, lang=lang, version=version), fn)
            return fn

        return decorator

    def serializer_udf(self, udf_id: str, *, lang="python", version=None):
        """Register a custom payload function returning bytes."""
        def decorator(fn):
            self.register(UdfDescriptor.serializer(udf_id, lang=lang, version=version), fn)
            return fn

        return decorator

    def deserializer_udf(self, udf_id: str, *, lang="python", version=None):
        """Register a custom payload function accepting bytes."""
        def decorator(fn):
            self.register(UdfDescriptor.deserializer(udf_id, lang=lang, version=version), fn)
            return fn

        return decorator


def udf_descriptor(udf_id=None, *, kind=":query-fn", lang=":java", version=None):
    """Create the legacy colon-string descriptor dictionary.

    Use :class:`UdfDescriptor` when composing with the typed ``q`` and ``tx``
    APIs.  This function retains its existing dictionary shape for callers of
    the EDN/list compatibility API.
    """

    return descriptor_data(udf_id, kind=kind, lang=lang, version=version)


def create_udf_registry() -> UdfRegistry:
    """Create a new UDF registry wrapper."""

    return UdfRegistry()


__all__ = ["UdfDescriptor", "UdfRegistry", "create_udf_registry", "udf_descriptor"]
