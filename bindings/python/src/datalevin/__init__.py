"""Datalevin Python bindings over the JVM interop bridge."""

from ._interop import (
    api_info,
    connect,
    datalog_kv,
    datom,
    embedding_attr,
    embedding_options,
    exec_json,
    fill_db,
    fulltext_attr,
    idoc_attr,
    idoc_options,
    init_db,
    keyword,
    new_client,
    open_kv,
    read_edn,
    schema_attr,
    search_domain,
    search_options,
    symbol,
    transact_async,
    tx_add,
    tx_entity,
    tx_retract,
    tx_retract_entity,
    vector_attr,
    vector_options,
    write_edn,
)
from ._jvm import jvm_started, start_jvm
from ._raw import interop
from .client import Client
from .connection import Connection
from .entity import Entity
from .errors import (
    DatalevinConfigurationError,
    DatalevinError,
    DatalevinJavaError,
    DatalevinJvmError,
)
from .kv import KV
from .udf import UdfRegistry, create_udf_registry, udf_descriptor

__all__ = [
    "Client",
    "Connection",
    "DatalevinConfigurationError",
    "Entity",
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
    "embedding_attr",
    "embedding_options",
    "exec_json",
    "fill_db",
    "fulltext_attr",
    "idoc_attr",
    "idoc_options",
    "init_db",
    "interop",
    "jvm_started",
    "keyword",
    "new_client",
    "open_kv",
    "read_edn",
    "schema_attr",
    "search_domain",
    "search_options",
    "start_jvm",
    "symbol",
    "transact_async",
    "tx_add",
    "tx_entity",
    "tx_retract",
    "tx_retract_entity",
    "udf_descriptor",
    "vector_attr",
    "vector_options",
    "write_edn",
]

__version__ = "0.10.18"
