"""High-level Python wrapper for Datalevin Datalog connections."""

from __future__ import annotations

from concurrent.futures import Future
from threading import Thread

import jpype

from ._convert import to_edn_form, to_java, to_python, to_query_input
from ._interop import _BINDINGS
from ._java import call_java, classes
from ._resource import ResourceWrapper


def _edn_form(value):
    if isinstance(value, str):
        return _BINDINGS.read_edn(value)
    return to_edn_form(value)


def _slice_rows(rows, limit=None, offset=0):
    start = max(offset or 0, 0)
    if limit is None:
        return rows[start:] if start else rows
    return rows[start : start + max(limit, 0)]


def _fetch_limit(limit, offset=0):
    if limit is None:
        return None
    return max(limit, 0) + max(offset or 0, 0)


def _python_future(java_future):
    future = Future()

    def complete():
        try:
            future.set_result(to_python(call_java(java_future.get)))
        except BaseException as exc:
            future.set_exception(exc)

    Thread(target=complete, daemon=True).start()
    return future


class _PythonConsumer:
    def __init__(self, fn) -> None:
        self._fn = fn

    def accept(self, value):
        self._fn(to_python(value))


def _consumer_proxy(fn):
    if not callable(fn):
        raise TypeError("callback must be callable.")
    return jpype.JProxy(classes().consumer_type, inst=_PythonConsumer(fn))


class Connection(ResourceWrapper):
    """Thin Python wrapper over a raw Datalevin connection handle."""

    def __init__(self, handle, *, owned: bool = True) -> None:
        close_fn = _BINDINGS.close_connection if owned else lambda _handle: None
        super().__init__(handle, close_fn, _BINDINGS.connection_closed, "connection")
        self._listeners = {}

    def close(self) -> None:
        self._listeners.clear()
        super().close()

    @staticmethod
    def _query_input(value):
        if isinstance(value, Connection):
            return _BINDINGS.connection_db(value.raw_handle())
        return to_query_input(value)

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

    def with_transaction(self, fn):
        return to_python(_BINDINGS.connection_with_transaction(self.raw_handle(), fn))

    def entid(self, eid):
        db = _BINDINGS.connection_db(self.raw_handle())
        return to_python(_BINDINGS.core_invoke("entid", [db, _BINDINGS.lookup_ref(eid)]))

    def entity(self, eid):
        from .entity import Entity

        entity = _BINDINGS.connection_entity(self.raw_handle(), eid)
        if entity is None:
            return None
        return Entity(entity)

    def entity_map(self, eid):
        entity = _BINDINGS.connection_entity(self.raw_handle(), eid)
        if entity is None:
            return None
        return to_python(_BINDINGS.entity_touch(entity))

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
        args = [_edn_form(query), db, *(self._query_input(value) for value in inputs)]
        return to_python(_BINDINGS.core_invoke("q", args))

    def explain(self, query, *inputs, opts_edn=None):
        db = _BINDINGS.connection_db(self.raw_handle())
        opts = None if opts_edn is None else _edn_form(opts_edn)
        args = [opts, _edn_form(query), db, *(self._query_input(value) for value in inputs)]
        return to_python(_BINDINGS.core_invoke("explain", args))

    def transact(self, tx_data, tx_meta=None):
        args = [self.raw_handle(), _BINDINGS.tx_data(tx_data)]
        if tx_meta is not None:
            args.append(to_java(tx_meta))
        return to_python(_BINDINGS.core_invoke("transact!", args))

    def transact_async(self, tx_data, tx_meta=None):
        """Start an async transaction and return a concurrent.futures.Future."""

        return _python_future(
            _BINDINGS.connection_transact_async(self.raw_handle(), tx_data, tx_meta)
        )

    def listen(self, callback, key=None):
        proxy = _consumer_proxy(callback)
        if key is None:
            registered_key = to_python(_BINDINGS.connection_listen(self.raw_handle(), proxy))
        else:
            registered_key = to_python(_BINDINGS.connection_listen(self.raw_handle(), key, proxy))
        self._listeners[registered_key] = proxy
        return registered_key

    def unlisten(self, key) -> None:
        _BINDINGS.connection_unlisten(self.raw_handle(), key)
        self._listeners.pop(key, None)

    def fill_db(self, datoms):
        _BINDINGS.fill_db(self.raw_handle(), datoms)
        return self

    def datalog_kv(self):
        from .kv import KV

        return KV(_BINDINGS.connection_datalog_kv(self.raw_handle()), owned=False)

    def datoms(self, index, c1=None, c2=None, c3=None, limit=None, offset=0):
        rows = to_python(
            _BINDINGS.connection_datoms(
                self.raw_handle(),
                index,
                c1,
                c2,
                c3,
                _fetch_limit(limit, offset),
            )
        )
        return _slice_rows(rows, limit, offset)

    def search_datoms(self, e=None, attr=None, value=None, limit=None, offset=0):
        rows = to_python(_BINDINGS.connection_search_datoms(self.raw_handle(), e, attr, value))
        return _slice_rows(rows, limit, offset)

    def count_datoms(self, e=None, attr=None, value=None):
        return to_python(_BINDINGS.connection_count_datoms(self.raw_handle(), e, attr, value))

    def seek_datoms(self, index, c1=None, c2=None, c3=None, limit=None, offset=0):
        rows = to_python(
            _BINDINGS.connection_seek_datoms(
                self.raw_handle(),
                index,
                c1,
                c2,
                c3,
                _fetch_limit(limit, offset),
            )
        )
        return _slice_rows(rows, limit, offset)

    def rseek_datoms(self, index, c1=None, c2=None, c3=None, limit=None, offset=0):
        rows = to_python(
            _BINDINGS.connection_rseek_datoms(
                self.raw_handle(),
                index,
                c1,
                c2,
                c3,
                _fetch_limit(limit, offset),
            )
        )
        return _slice_rows(rows, limit, offset)

    def index_range(self, attr, start, end, limit=None, offset=0):
        rows = to_python(_BINDINGS.connection_index_range(self.raw_handle(), attr, start, end))
        return _slice_rows(rows, limit, offset)

    def fulltext_datoms(self, query, opts=None, limit=None, offset=0):
        rows = to_python(_BINDINGS.connection_fulltext_datoms(self.raw_handle(), query, opts))
        return _slice_rows(rows, limit, offset)

    def copy(self, dest, compact=None) -> None:
        _BINDINGS.connection_copy(self.raw_handle(), dest, compact)

    def tx_log_watermarks(self):
        return to_python(_BINDINGS.connection_tx_log_watermarks(self.raw_handle()))

    def open_tx_log(self, from_lsn, upto_lsn=None, limit=None):
        rows = to_python(_BINDINGS.connection_open_tx_log(self.raw_handle(), from_lsn, upto_lsn))
        if limit is None:
            return rows
        return rows[: max(limit, 0)]

    def create_snapshot(self):
        return to_python(_BINDINGS.connection_create_snapshot(self.raw_handle()))

    def list_snapshots(self):
        return to_python(_BINDINGS.connection_list_snapshots(self.raw_handle()))

    def gc_tx_log_segments(self, retain_floor_lsn=None):
        return to_python(_BINDINGS.connection_gc_tx_log_segments(self.raw_handle(), retain_floor_lsn))

    def re_index(self, opts=None, *, schema=None):
        self._handle = _BINDINGS.connection_re_index(self.raw_handle(), schema, opts)
        return self
