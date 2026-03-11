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
            descriptor = {":udf/lang": ":java", ":udf/kind": ":query-fn", ":udf/id": udf_id}
            self.register(descriptor, fn)
            return fn

        return decorator

    def tx_udf(self, udf_id: str):
        def decorator(fn):
            descriptor = {":udf/lang": ":java", ":udf/kind": ":tx-fn", ":udf/id": udf_id}
            self.register(descriptor, fn)
            return fn

        return decorator


def create_udf_registry() -> UdfRegistry:
    """Create a new UDF registry wrapper."""

    return UdfRegistry()
