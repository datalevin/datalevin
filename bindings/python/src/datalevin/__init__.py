"""Datalevin Python bindings over the JVM interop bridge."""

from ._interop import api_info, connect, exec_json, new_client, open_kv
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
    "exec_json",
    "interop",
    "jvm_started",
    "new_client",
    "open_kv",
    "start_jvm",
]

__version__ = "0.10.11"
