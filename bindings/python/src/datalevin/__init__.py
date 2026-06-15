"""Datalevin Python bindings over the JVM interop bridge."""

from ._interop import (
    api_info,
    connect,
    datalog_kv,
    datom,
    exec_json,
    fill_db,
    init_db,
    keyword,
    new_client,
    open_kv,
    read_edn,
    schema_attr,
    symbol,
    transact_async,
    tx_add,
    tx_entity,
    tx_retract,
    tx_retract_entity,
    write_edn,
)
from ._jvm import jvm_started, start_jvm
from ._raw import interop
from .client import Client
from .connection import Connection
from .errors import (
    DatalevinConfigurationError,
    DatalevinError,
    DatalevinJavaError,
    DatalevinJvmError,
)
from .kv import KV
from .udf import UdfRegistry, create_udf_registry

__all__ = [
    "Client",
    "Connection",
    "DatalevinConfigurationError",
    "DatalevinError",
    "DatalevinJavaError",
    "DatalevinJvmError",
    "KV",
    "UdfRegistry",
    "api_info",
    "connect",
    "create_udf_registry",
    "datom",
    "datalog_kv",
    "exec_json",
    "fill_db",
    "init_db",
    "interop",
    "jvm_started",
    "keyword",
    "new_client",
    "open_kv",
    "read_edn",
    "schema_attr",
    "start_jvm",
    "symbol",
    "transact_async",
    "tx_add",
    "tx_entity",
    "tx_retract",
    "tx_retract_entity",
    "write_edn",
]

__version__ = "0.10.18"
