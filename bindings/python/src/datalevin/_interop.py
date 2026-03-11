"""Internal interop bindings and public constructors."""

from __future__ import annotations

import json

from ._convert import to_java, to_python
from ._java import call_java, classes
from ._jvm import jvm_started, start_jvm
from .errors import DatalevinError


class InteropBindings:
    """Thin wrapper around the Datalevin JVM bridge."""

    def api_info_raw(self):
        return call_java(classes().datalevin.apiInfo)

    def exec_json_raw(self, request_json: str) -> str:
        return call_java(classes().json_api.exec, request_json)

    def core_invoke(self, function: str, args=None):
        return call_java(classes().interop.coreInvoke, function, to_java(list(args or ())))

    def client_invoke(self, function: str, args=None):
        return call_java(classes().interop.clientInvoke, function, to_java(list(args or ())))

    def create_connection(self, dir=None, schema=None, opts=None, *, shared: bool = False):
        target = classes().interop.getConnection if shared else classes().interop.createConnection
        return call_java(target, dir, to_java(schema), to_java(opts))

    def close_connection(self, handle) -> None:
        call_java(classes().interop.closeConnection, handle)

    def connection_closed(self, handle) -> bool:
        return bool(call_java(classes().interop.connectionClosed, handle))

    def connection_db(self, handle):
        return call_java(classes().interop.connectionDb, handle)

    def open_key_value(self, dir, opts=None):
        return call_java(classes().interop.openKeyValue, dir, to_java(opts))

    def close_key_value(self, handle) -> None:
        call_java(classes().interop.closeKeyValue, handle)

    def key_value_closed(self, handle) -> bool:
        return bool(call_java(classes().interop.keyValueClosed, handle))

    def new_client(self, uri, opts=None):
        return call_java(classes().interop.newClient, uri, to_java(opts))

    def close_client(self, handle) -> None:
        call_java(classes().interop.closeClient, handle)

    def client_disconnected(self, handle) -> bool:
        return bool(call_java(classes().interop.clientDisconnected, handle))

    def read_edn(self, edn: str):
        return call_java(classes().interop.readEdn, edn)

    def keyword(self, value: str):
        return call_java(classes().interop.keyword, value)

    def symbol(self, value: str):
        return call_java(classes().interop.symbol, value)

    def schema(self, schema):
        if schema is None:
            return None
        return call_java(classes().interop.schema, to_java(schema))

    def options(self, opts):
        if opts is None:
            return None
        return call_java(classes().interop.options, to_java(opts))

    def udf_descriptor(self, descriptor):
        if descriptor is None:
            return None
        return call_java(classes().interop.udfDescriptor, to_java(descriptor))

    def create_udf_registry(self):
        return call_java(classes().interop.createUdfRegistry)

    def register_udf(self, registry, descriptor, fn):
        return call_java(classes().interop.registerUdf, registry, to_java(descriptor), fn)

    def unregister_udf(self, registry, descriptor):
        return call_java(classes().interop.unregisterUdf, registry, to_java(descriptor))

    def registered_udf(self, registry, descriptor) -> bool:
        return bool(call_java(classes().interop.registeredUdf, registry, to_java(descriptor)))

    def rename_map(self, rename_map):
        if rename_map is None:
            return None
        return call_java(classes().interop.renameMap, to_java(rename_map))

    def delete_attrs(self, attrs):
        if attrs is None:
            return None
        return call_java(classes().interop.deleteAttrs, to_java(list(attrs or ())))

    def lookup_ref(self, value):
        if value is None:
            return None
        return call_java(classes().interop.lookupRef, to_java(value))

    def tx_data(self, tx_data):
        if tx_data is None:
            return None
        return call_java(classes().interop.txData, to_java(tx_data))

    def kv_txs(self, txs):
        if txs is None:
            return None
        return call_java(classes().interop.kvTxs, to_java(txs))

    def kv_type(self, value):
        if value is None:
            return None
        return call_java(classes().interop.kvType, to_java(value))

    def database_type(self, value: str):
        return call_java(classes().interop.databaseType, value)

    def role(self, role: str):
        return call_java(classes().interop.role, role)

    def permission_keyword(self, value: str):
        return call_java(classes().interop.permissionKeyword, value)

    def permission_target(self, object_type: str, target):
        return call_java(classes().interop.permissionTarget, object_type, to_java(target))


_BINDINGS = InteropBindings()


def api_info():
    """Return Datalevin JSON/API metadata as a Python dictionary."""

    return to_python(_BINDINGS.api_info_raw())


def exec_json(op: str, args=None):
    """Execute a raw JSON API operation."""

    request = json.dumps({"op": op, "args": args or {}})
    envelope = json.loads(_BINDINGS.exec_json_raw(request))
    if envelope.get("ok"):
        return envelope.get("result")
    raise DatalevinError(
        envelope.get("error") or "Datalevin JSON API request failed.",
        type_name=envelope.get("type"),
        data=envelope.get("data"),
    )


def connect(dir=None, schema=None, opts=None, *, shared: bool = False) -> Connection:
    """Create or open a Datalevin Datalog connection."""

    from .connection import Connection

    return Connection(_BINDINGS.create_connection(dir, schema, opts, shared=shared))


def open_kv(dir, opts=None) -> KV:
    """Open a Datalevin KV store."""

    from .kv import KV

    return KV(_BINDINGS.open_key_value(dir, opts))


def new_client(uri, opts=None) -> Client:
    """Open a remote Datalevin admin client."""

    from .client import Client

    return Client(_BINDINGS.new_client(uri, opts))


__all__ = [
    "_BINDINGS",
    "api_info",
    "connect",
    "exec_json",
    "jvm_started",
    "new_client",
    "open_kv",
    "start_jvm",
]
