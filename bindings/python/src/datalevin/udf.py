"""Python UDF registration over the Datalevin JVM bridge."""

from __future__ import annotations

import jpype

from ._convert import to_java, to_python
from ._interop import _BINDINGS
from ._java import classes


class _PythonUdfFunction:
    def __init__(self, fn):
        self._fn = fn

    def invoke(self, args):
        python_args = [to_python(arg) for arg in args]
        return to_java(self._fn(*python_args))


class UdfRegistry:
    """Wrapper around a raw Datalevin UDF registry handle."""

    def __init__(self, handle=None) -> None:
        self._handle = _BINDINGS.create_udf_registry() if handle is None else handle

    def raw_handle(self):
        return self._handle

    def register(self, descriptor, fn):
        proxy = jpype.JProxy(classes().udf_function, inst=_PythonUdfFunction(fn))
        _BINDINGS.register_udf(self._handle, descriptor, proxy)
        return fn

    def unregister(self, descriptor):
        _BINDINGS.unregister_udf(self._handle, descriptor)

    def registered(self, descriptor) -> bool:
        return _BINDINGS.registered_udf(self._handle, descriptor)

    def query_udf(self, udf_id: str):
        def decorator(fn):
            self.register(udf_descriptor(udf_id, kind=":query-fn"), fn)
            return fn

        return decorator

    def predicate_udf(self, udf_id: str):
        def decorator(fn):
            self.register(udf_descriptor(udf_id, kind=":predicate"), fn)
            return fn

        return decorator

    def tx_udf(self, udf_id: str):
        def decorator(fn):
            self.register(udf_descriptor(udf_id, kind=":tx-fn"), fn)
            return fn

        return decorator

    def analyzer_udf(self, udf_id: str):
        def decorator(fn):
            self.register(udf_descriptor(udf_id, kind=":analyzer"), fn)
            return fn

        return decorator

    def query_analyzer_udf(self, udf_id: str):
        def decorator(fn):
            self.register(udf_descriptor(udf_id, kind=":query-analyzer"), fn)
            return fn

        return decorator


def _keyword_string(value) -> str:
    text = str(value)
    return text if text.startswith(":") else f":{text}"


def udf_descriptor(udf_id=None, *, kind=":query-fn", lang=":java", version=None):
    """Create a Datalevin UDF descriptor dictionary."""

    if isinstance(udf_id, dict):
        descriptor = dict(udf_id)
        udf_id = descriptor.get(":udf/id", descriptor.get("id"))
        kind = descriptor.get(":udf/kind", descriptor.get("kind", kind))
        lang = descriptor.get(":udf/lang", descriptor.get("lang", lang))
        version = descriptor.get(":udf/version", descriptor.get("version", version))

    if udf_id is None:
        raise TypeError("udf_id is required")

    descriptor = {
        ":udf/lang": _keyword_string(lang),
        ":udf/kind": _keyword_string(kind),
        ":udf/id": _keyword_string(udf_id),
    }
    if version is not None:
        descriptor[":udf/version"] = version
    return descriptor


def create_udf_registry() -> UdfRegistry:
    """Create a new UDF registry wrapper."""

    return UdfRegistry()
