"""Advanced raw access to the Datalevin interop bridge."""

from __future__ import annotations

from ._interop import _BINDINGS


class RawInterop:
    """Expose raw `DatalevinInterop` operations for advanced callers."""

    def api_info(self):
        return _BINDINGS.api_info_raw()

    def exec_json(self, request_json: str):
        return _BINDINGS.exec_json_raw(request_json)

    def core_invoke(self, function: str, args=None):
        return _BINDINGS.core_invoke(function, args)

    def client_invoke(self, function: str, args=None):
        return _BINDINGS.client_invoke(function, args)

    def create_connection(self, dir=None, schema=None, opts=None, *, shared: bool = False):
        return _BINDINGS.create_connection(dir, schema, opts, shared=shared)

    def close_connection(self, handle):
        _BINDINGS.close_connection(handle)

    def connection_closed(self, handle):
        return _BINDINGS.connection_closed(handle)

    def connection_db(self, handle):
        return _BINDINGS.connection_db(handle)

    def open_key_value(self, dir, opts=None):
        return _BINDINGS.open_key_value(dir, opts)

    def close_key_value(self, handle):
        _BINDINGS.close_key_value(handle)

    def key_value_closed(self, handle):
        return _BINDINGS.key_value_closed(handle)

    def new_client(self, uri, opts=None):
        return _BINDINGS.new_client(uri, opts)

    def close_client(self, handle):
        _BINDINGS.close_client(handle)

    def client_disconnected(self, handle):
        return _BINDINGS.client_disconnected(handle)

    def read_edn(self, edn: str):
        return _BINDINGS.read_edn(edn)

    def keyword(self, value: str):
        return _BINDINGS.keyword(value)

    def symbol(self, value: str):
        return _BINDINGS.symbol(value)

    def schema(self, schema):
        return _BINDINGS.schema(schema)

    def options(self, opts):
        return _BINDINGS.options(opts)

    def udf_descriptor(self, descriptor):
        return _BINDINGS.udf_descriptor(descriptor)

    def create_udf_registry(self):
        return _BINDINGS.create_udf_registry()

    def register_udf(self, registry, descriptor, fn):
        return _BINDINGS.register_udf(registry, descriptor, fn)

    def unregister_udf(self, registry, descriptor):
        return _BINDINGS.unregister_udf(registry, descriptor)

    def registered_udf(self, registry, descriptor):
        return _BINDINGS.registered_udf(registry, descriptor)

    def rename_map(self, rename_map):
        return _BINDINGS.rename_map(rename_map)

    def delete_attrs(self, attrs):
        return _BINDINGS.delete_attrs(attrs)

    def lookup_ref(self, value):
        return _BINDINGS.lookup_ref(value)

    def tx_data(self, tx_data):
        return _BINDINGS.tx_data(tx_data)

    def kv_txs(self, txs):
        return _BINDINGS.kv_txs(txs)

    def kv_type(self, value):
        return _BINDINGS.kv_type(value)

    def database_type(self, value: str):
        return _BINDINGS.database_type(value)

    def role(self, role: str):
        return _BINDINGS.role(role)

    def permission_keyword(self, value: str):
        return _BINDINGS.permission_keyword(value)

    def permission_target(self, object_type: str, target):
        return _BINDINGS.permission_target(object_type, target)


_RAW_INTEROP = RawInterop()


def interop() -> RawInterop:
    """Return the singleton raw interop facade."""

    return _RAW_INTEROP
