"""High-level Python wrapper for Datalevin Datalog connections."""

from __future__ import annotations

from ._convert import to_java, to_python
from ._interop import _BINDINGS
from ._resource import ResourceWrapper


def _edn_form(value):
    if isinstance(value, str):
        return _BINDINGS.read_edn(value)
    return to_java(value)


class Connection(ResourceWrapper):
    """Thin Python wrapper over a raw Datalevin connection handle."""

    def __init__(self, handle) -> None:
        super().__init__(handle, _BINDINGS.close_connection, _BINDINGS.connection_closed, "connection")

    def schema(self):
        return to_python(_BINDINGS.core_invoke("schema", [self.raw_handle()]))

    def opts(self):
        return to_python(_BINDINGS.core_invoke("opts", [self.raw_handle()]))

    def update_schema(self, schema_update, del_attrs=None, rename_map=None):
        args = [self.raw_handle(), _BINDINGS.schema(schema_update) if schema_update is not None else None]
        if rename_map is not None:
            args.append(_BINDINGS.delete_attrs(del_attrs))
            args.append(_BINDINGS.rename_map(rename_map))
        elif del_attrs is not None:
            args.append(_BINDINGS.delete_attrs(del_attrs))
        return to_python(_BINDINGS.core_invoke("update-schema", args))

    def clear(self) -> None:
        _BINDINGS.core_invoke("clear", [self.raw_handle()])

    def entid(self, eid):
        db = _BINDINGS.connection_db(self.raw_handle())
        return to_python(_BINDINGS.core_invoke("entid", [db, _BINDINGS.lookup_ref(eid)]))

    def entity(self, eid):
        db = _BINDINGS.connection_db(self.raw_handle())
        entity = _BINDINGS.core_invoke("entity", [db, _BINDINGS.lookup_ref(eid)])
        if entity is None:
            return None
        return to_python(_BINDINGS.core_invoke("touch", [entity]))

    def pull(self, selector, eid):
        db = _BINDINGS.connection_db(self.raw_handle())
        return to_python(
            _BINDINGS.core_invoke("pull", [db, _edn_form(selector), _BINDINGS.lookup_ref(eid)])
        )

    def pull_many(self, selector, eids):
        db = _BINDINGS.connection_db(self.raw_handle())
        refs = [_BINDINGS.lookup_ref(eid) for eid in eids]
        return to_python(_BINDINGS.core_invoke("pull-many", [db, _edn_form(selector), refs]))

    def query(self, query, *inputs):
        db = _BINDINGS.connection_db(self.raw_handle())
        args = [_edn_form(query), db, *inputs]
        return to_python(_BINDINGS.core_invoke("q", args))

    def explain(self, query, *inputs, opts_edn=None):
        db = _BINDINGS.connection_db(self.raw_handle())
        opts = None if opts_edn is None else _edn_form(opts_edn)
        args = [opts, _edn_form(query), db, *inputs]
        return to_python(_BINDINGS.core_invoke("explain", args))

    def transact(self, tx_data, tx_meta=None):
        args = [self.raw_handle(), _BINDINGS.tx_data(tx_data)]
        if tx_meta is not None:
            args.append(to_java(tx_meta))
        return to_python(_BINDINGS.core_invoke("transact!", args))
