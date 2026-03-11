"""JPype class loading and Java call helpers."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import jpype

from ._jvm import start_jvm
from .errors import DatalevinJavaError


@dataclass(frozen=True)
class JavaClasses:
    """Cached Java classes used by the Python wrapper."""

    datalevin: object
    interop: object
    json_api: object
    udf_function: object
    linked_hash_map: object
    linked_hash_set: object
    array_list: object
    uuid: object
    instant: object
    date: object
    big_integer: object
    big_decimal: object
    map_type: object
    list_type: object
    set_type: object
    collection_type: object
    keyword_type: object
    symbol_type: object


@lru_cache(maxsize=1)
def classes() -> JavaClasses:
    """Load and cache the Java classes used by the binding."""

    start_jvm()
    return JavaClasses(
        datalevin=jpype.JClass("datalevin.Datalevin"),
        interop=jpype.JClass("datalevin.DatalevinInterop"),
        json_api=jpype.JClass("datalevin.JSONApi"),
        udf_function=jpype.JClass("datalevin.UdfFunction"),
        linked_hash_map=jpype.JClass("java.util.LinkedHashMap"),
        linked_hash_set=jpype.JClass("java.util.LinkedHashSet"),
        array_list=jpype.JClass("java.util.ArrayList"),
        uuid=jpype.JClass("java.util.UUID"),
        instant=jpype.JClass("java.time.Instant"),
        date=jpype.JClass("java.util.Date"),
        big_integer=jpype.JClass("java.math.BigInteger"),
        big_decimal=jpype.JClass("java.math.BigDecimal"),
        map_type=jpype.JClass("java.util.Map"),
        list_type=jpype.JClass("java.util.List"),
        set_type=jpype.JClass("java.util.Set"),
        collection_type=jpype.JClass("java.util.Collection"),
        keyword_type=jpype.JClass("clojure.lang.Keyword"),
        symbol_type=jpype.JClass("clojure.lang.Symbol"),
    )


def call_java(target, *args):
    """Call a Java method and wrap JPype Java exceptions."""

    try:
        return target(*args)
    except jpype.JException as exc:
        raise DatalevinJavaError.from_java(exc) from exc


def is_java_object(value) -> bool:
    """Best-effort detection for live Java objects exposed through JPype."""

    if value is None or not jpype.isJVMStarted():
        return False
    module = type(value).__module__
    return (
        module.startswith("jpype")
        or module.startswith("_jpype")
        or str(type(value)).startswith("<java ")
    )
