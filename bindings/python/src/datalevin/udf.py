"""Python UDF registration over the Datalevin JVM bridge."""

from __future__ import annotations

import jpype

from ._convert import to_java, to_python
from ._interop import _BINDINGS
from ._java import classes, is_java_object
from ._udf_value import UdfDescriptor, descriptor_data


class _PythonUdfFunction:
    def __init__(self, fn):
        self._fn = fn

    def invoke(self, args):
        python_args = [_udf_arg_to_python(arg, index) for index, arg in enumerate(args)]
        return to_java(self._fn(*python_args))


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
        proxy = jpype.JProxy(classes().udf_function, inst=_PythonUdfFunction(fn))
        _BINDINGS.register_udf(self._handle, normalized, proxy)
        self._proxies[normalized] = proxy
        return fn

    def unregister(self, descriptor):
        normalized = UdfDescriptor.from_value(descriptor, default_lang="python")
        _BINDINGS.unregister_udf(self._handle, normalized)
        self._proxies.pop(normalized, None)

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
