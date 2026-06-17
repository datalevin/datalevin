"""High-level Python wrapper for Datalevin KV handles."""

from __future__ import annotations

import jpype

from ._convert import to_edn_form, to_java, to_python
from ._interop import _BINDINGS
from ._java import classes
from ._resource import ResourceWrapper


def _slice_page(items, limit=None, offset=None):
    start = 0 if offset is None else max(offset, 0)
    if limit is None:
        return items[start:]
    return items[start : start + max(limit, 0)]


def _require_k_type(k_type, op):
    if k_type is None:
        raise ValueError(f"k_type is required for KV {op}().")


def _require_v_type(v_type, op):
    if v_type is None:
        raise ValueError(f"v_type is required for KV {op}().")


def _require_callable(fn, op):
    if not callable(fn):
        raise TypeError(f"callback is required for KV {op}().")


def _reject_v_type_without_k_type(k_type, v_type, op):
    if v_type is not None and k_type is None:
        raise ValueError(f"v_type requires k_type for KV {op}().")


def _append_typed_args(args, *, k_type=None, v_type=None, ignore_key=None, op):
    _reject_v_type_without_k_type(k_type, v_type, op)
    if ignore_key is not None and (k_type is None or v_type is None):
        raise ValueError(f"ignore_key requires k_type and v_type for KV {op}().")
    if k_type is not None:
        args.append(_BINDINGS.kv_type(k_type))
    if v_type is not None:
        args.append(_BINDINGS.kv_type(v_type))
    if ignore_key is not None:
        args.append(bool(ignore_key))


class _PythonConsumer:
    def __init__(self, fn) -> None:
        self._fn = fn

    def accept(self, value):
        self._fn(to_python(value))


class _PythonBiConsumer:
    def __init__(self, fn) -> None:
        self._fn = fn

    def accept(self, key, value):
        self._fn(to_python(key), to_python(value))


class _PythonBiPredicate:
    def __init__(self, fn) -> None:
        self._fn = fn

    def test(self, key, value):
        return bool(self._fn(to_python(key), to_python(value)))


class _PythonBiFunction:
    def __init__(self, fn) -> None:
        self._fn = fn

    def apply(self, key, value):
        return to_java(self._fn(to_python(key), to_python(value)))


def _consumer_proxy(fn):
    return jpype.JProxy(classes().consumer_type, inst=_PythonConsumer(fn))


def _bi_consumer_proxy(fn):
    return jpype.JProxy(classes().bi_consumer_type, inst=_PythonBiConsumer(fn))


def _bi_predicate_proxy(fn):
    return jpype.JProxy(classes().bi_predicate_type, inst=_PythonBiPredicate(fn))


def _bi_function_proxy(fn):
    return jpype.JProxy(classes().bi_function_type, inst=_PythonBiFunction(fn))


class KV(ResourceWrapper):
    """Thin Python wrapper over a raw Datalevin KV handle."""

    def __init__(self, handle, *, owned: bool = True) -> None:
        close_fn = _BINDINGS.close_key_value if owned else lambda _handle: None
        super().__init__(handle, close_fn, _BINDINGS.key_value_closed, "kv")

    def dir(self):
        return to_python(_BINDINGS.core_invoke("dir", [self.raw_handle()]))

    def open_dbi(self, name, opts=None) -> None:
        args = [self.raw_handle(), name]
        if opts is not None:
            args.append(_BINDINGS.options(opts))
        _BINDINGS.core_invoke("open-dbi", args)

    def open_list_dbi(self, name, opts=None) -> None:
        args = [self.raw_handle(), name]
        if opts is not None:
            args.append(_BINDINGS.options(opts))
        _BINDINGS.core_invoke("open-list-dbi", args)

    def begin_transaction(self):
        return KVTransaction(_BINDINGS.key_value_begin_transaction(self.raw_handle()))

    def transaction(self):
        return self.begin_transaction()

    def with_transaction(self, fn):
        return to_python(_BINDINGS.key_value_with_transaction(self.raw_handle(), fn))

    def search_index_writer(self, opts=None):
        from .search import SearchIndexWriter

        return SearchIndexWriter(_BINDINGS.search_index_writer(self.raw_handle(), opts))

    def list_dbis(self):
        return to_python(_BINDINGS.core_invoke("list-dbis", [self.raw_handle()]))

    def entries(self, dbi_name):
        return to_python(_BINDINGS.core_invoke("entries", [self.raw_handle(), dbi_name]))

    def stat(self, dbi_name=None):
        args = [self.raw_handle()]
        if dbi_name is not None:
            args.append(dbi_name)
        return to_python(_BINDINGS.core_invoke("stat", args))

    def copy(self, dest, compact=None):
        args = [self.raw_handle(), dest]
        if compact is not None:
            args.append(bool(compact))
        return to_python(_BINDINGS.core_invoke("copy", args))

    def sync(self, force=None):
        args = [self.raw_handle()]
        if force is not None:
            args.append(force)
        return to_python(_BINDINGS.core_invoke("sync", args))

    def tx_log_watermarks(self):
        return to_python(_BINDINGS.core_invoke("txlog-watermarks", [self.raw_handle()]))

    def open_tx_log(self, from_lsn, upto_lsn=None, limit=None):
        args = [self.raw_handle(), from_lsn]
        if upto_lsn is not None:
            args.append(upto_lsn)
        rows = to_python(_BINDINGS.core_invoke("open-tx-log", args))
        if limit is None:
            return rows
        return rows[: max(limit, 0)]

    def create_snapshot(self):
        return to_python(_BINDINGS.core_invoke("create-snapshot!", [self.raw_handle()]))

    def list_snapshots(self):
        return to_python(_BINDINGS.core_invoke("list-snapshots", [self.raw_handle()]))

    def gc_tx_log_segments(self, retain_floor_lsn=None):
        args = [self.raw_handle()]
        if retain_floor_lsn is not None:
            args.append(retain_floor_lsn)
        return to_python(_BINDINGS.core_invoke("gc-txlog-segments!", args))

    def re_index(self, opts=None):
        self._handle = _BINDINGS.key_value_re_index(self.raw_handle(), opts)
        return self

    def new_search_engine(self, opts=None):
        from .search import SearchEngine

        return SearchEngine(_BINDINGS.new_search_engine(self.raw_handle(), opts))

    def new_vector_index(self, opts):
        from .vector import VectorIndex

        return VectorIndex(_BINDINGS.new_vector_index(self.raw_handle(), opts))

    def transact(self, txs, dbi_name=None, k_type=None, v_type=None):
        if dbi_name is None and (k_type is not None or v_type is not None):
            raise ValueError("k_type and v_type require dbi_name for KV transact().")
        if v_type is not None and k_type is None:
            raise ValueError("v_type requires k_type for KV transact().")

        args = [self.raw_handle()]
        if dbi_name is None:
            args.append(_BINDINGS.kv_txs(txs))
        else:
            args.extend([dbi_name, _BINDINGS.kv_txs(txs)])
            if k_type is not None:
                args.append(_BINDINGS.kv_type(k_type))
            if v_type is not None:
                args.append(_BINDINGS.kv_type(v_type))
        return to_python(_BINDINGS.core_invoke("transact-kv", args))

    def get_value(self, dbi_name, key, k_type=None, v_type=None, ignore_key=False):
        args = [self.raw_handle(), dbi_name, to_java(key)]
        if (k_type is None) != (v_type is None):
            raise ValueError("k_type and v_type must be provided together for KV get_value().")
        if k_type is not None:
            args.append(_BINDINGS.kv_type(k_type))
            args.append(_BINDINGS.kv_type(v_type))
            args.append(bool(ignore_key))
        return to_python(_BINDINGS.core_invoke("get-value", args))

    def get_rank(self, dbi_name, key, k_type=None):
        args = [self.raw_handle(), dbi_name, to_java(key)]
        if k_type is not None:
            args.append(_BINDINGS.kv_type(k_type))
        return to_python(_BINDINGS.core_invoke("get-rank", args))

    def get_by_rank(self, dbi_name, rank, k_type=None, v_type=None, ignore_key=None):
        args = [self.raw_handle(), dbi_name, rank]
        _append_typed_args(args, k_type=k_type, v_type=v_type, ignore_key=ignore_key, op="get_by_rank")
        return to_python(_BINDINGS.core_invoke("get-by-rank", args))

    def sample_kv(self, dbi_name, n, k_type=None, v_type=None, ignore_key=None):
        args = [self.raw_handle(), dbi_name, n]
        _append_typed_args(args, k_type=k_type, v_type=v_type, ignore_key=ignore_key, op="sample_kv")
        return to_python(_BINDINGS.core_invoke("sample-kv", args))

    def get_first(self, dbi_name, key_range, k_type=None, v_type=None, ignore_key=None):
        if key_range is None:
            raise ValueError("key_range is required for KV get_first().")
        args = [self.raw_handle(), dbi_name, to_edn_form(key_range)]
        _append_typed_args(args, k_type=k_type, v_type=v_type, ignore_key=ignore_key, op="get_first")
        return to_python(_BINDINGS.core_invoke("get-first", args))

    def get_first_n(self, dbi_name, n, key_range, k_type=None, v_type=None, ignore_key=None):
        if key_range is None:
            raise ValueError("key_range is required for KV get_first_n().")
        args = [self.raw_handle(), dbi_name, n, to_edn_form(key_range)]
        _append_typed_args(args, k_type=k_type, v_type=v_type, ignore_key=ignore_key, op="get_first_n")
        return to_python(_BINDINGS.core_invoke("get-first-n", args))

    def get_range(self, dbi_name, key_range, k_type=None, v_type=None, limit=None, offset=None):
        if key_range is None:
            raise ValueError("key_range is required for KV get_range().")
        if v_type is not None and k_type is None:
            raise ValueError("v_type requires k_type for KV get_range().")
        args = [self.raw_handle(), dbi_name, to_edn_form(key_range)]
        if k_type is not None:
            args.append(_BINDINGS.kv_type(k_type))
            if v_type is not None:
                args.append(_BINDINGS.kv_type(v_type))
        return _slice_page(to_python(_BINDINGS.core_invoke("get-range", args)), limit, offset)

    def key_range(self, dbi_name, key_range, k_type=None, limit=None, offset=None):
        if key_range is None:
            raise ValueError("key_range is required for KV key_range().")
        args = [self.raw_handle(), dbi_name, to_edn_form(key_range)]
        if k_type is not None:
            args.append(_BINDINGS.kv_type(k_type))
        return _slice_page(to_python(_BINDINGS.core_invoke("key-range", args)), limit, offset)

    def key_range_count(self, dbi_name, key_range, k_type=None):
        if key_range is None:
            raise ValueError("key_range is required for KV key_range_count().")
        args = [self.raw_handle(), dbi_name, to_edn_form(key_range)]
        if k_type is not None:
            args.append(_BINDINGS.kv_type(k_type))
        return to_python(_BINDINGS.core_invoke("key-range-count", args))

    def range_count(self, dbi_name, key_range, k_type=None):
        if key_range is None:
            raise ValueError("key_range is required for KV range_count().")
        args = [self.raw_handle(), dbi_name, to_edn_form(key_range)]
        if k_type is not None:
            args.append(_BINDINGS.kv_type(k_type))
        return to_python(_BINDINGS.core_invoke("range-count", args))

    def put_list_items(self, list_name, key, values, k_type, v_type):
        _require_k_type(k_type, "put_list_items")
        _require_v_type(v_type, "put_list_items")
        _BINDINGS.core_invoke(
            "put-list-items",
            [
                self.raw_handle(),
                list_name,
                to_java(key),
                to_java(values),
                _BINDINGS.kv_type(k_type),
                _BINDINGS.kv_type(v_type),
            ],
        )

    def del_list_items(self, list_name, key, k_type, *, values=None, v_type=None):
        _require_k_type(k_type, "del_list_items")
        args = [self.raw_handle(), list_name, to_java(key)]
        if values is None:
            if v_type is not None:
                raise ValueError("v_type requires values for KV del_list_items().")
            args.append(_BINDINGS.kv_type(k_type))
        else:
            _require_v_type(v_type, "del_list_items")
            args.extend([to_java(values), _BINDINGS.kv_type(k_type), _BINDINGS.kv_type(v_type)])
        _BINDINGS.core_invoke("del-list-items", args)

    def get_list(self, list_name, key, k_type, v_type, limit=None, offset=None):
        _require_k_type(k_type, "get_list")
        _require_v_type(v_type, "get_list")
        return _slice_page(
            to_python(
                _BINDINGS.core_invoke(
                    "get-list",
                    [
                        self.raw_handle(),
                        list_name,
                        to_java(key),
                        _BINDINGS.kv_type(k_type),
                        _BINDINGS.kv_type(v_type),
                    ],
                )
            ),
            limit,
            offset,
        )

    def list_count(self, list_name, key, k_type):
        _require_k_type(k_type, "list_count")
        return to_python(
            _BINDINGS.core_invoke(
                "list-count",
                [self.raw_handle(), list_name, to_java(key), _BINDINGS.kv_type(k_type)],
            )
        )

    def in_list(self, list_name, key, value, k_type, v_type):
        _require_k_type(k_type, "in_list")
        _require_v_type(v_type, "in_list")
        return bool(
            _BINDINGS.core_invoke(
                "in-list?",
                [
                    self.raw_handle(),
                    list_name,
                    to_java(key),
                    to_java(value),
                    _BINDINGS.kv_type(k_type),
                    _BINDINGS.kv_type(v_type),
                ],
            )
        )

    def visit_list(self, list_name, visitor, key, k_type, v_type):
        _require_callable(visitor, "visit_list")
        _require_k_type(k_type, "visit_list")
        _require_v_type(v_type, "visit_list")
        _BINDINGS.kv_visit_list(
            self.raw_handle(),
            list_name,
            _consumer_proxy(visitor),
            to_java(key),
            k_type,
            v_type,
        )

    def visit_list_range(self, list_name, visitor, k_range, k_type, v_range, v_type):
        if k_range is None:
            raise ValueError("k_range is required for KV visit_list_range().")
        if v_range is None:
            raise ValueError("v_range is required for KV visit_list_range().")
        _require_callable(visitor, "visit_list_range")
        _require_k_type(k_type, "visit_list_range")
        _require_v_type(v_type, "visit_list_range")
        _BINDINGS.kv_visit_list_range(
            self.raw_handle(),
            list_name,
            _bi_consumer_proxy(visitor),
            to_edn_form(k_range),
            k_type,
            to_edn_form(v_range),
            v_type,
        )

    def list_range_filter(self, list_name, predicate, k_range, k_type, v_range, v_type, limit=None, offset=None):
        if k_range is None:
            raise ValueError("k_range is required for KV list_range_filter().")
        if v_range is None:
            raise ValueError("v_range is required for KV list_range_filter().")
        _require_callable(predicate, "list_range_filter")
        _require_k_type(k_type, "list_range_filter")
        _require_v_type(v_type, "list_range_filter")
        return _slice_page(
            to_python(
                _BINDINGS.kv_list_range_filter(
                    self.raw_handle(),
                    list_name,
                    _bi_predicate_proxy(predicate),
                    to_edn_form(k_range),
                    k_type,
                    to_edn_form(v_range),
                    v_type,
                )
            ),
            limit,
            offset,
        )

    def list_range_filter_count(self, list_name, predicate, k_range, k_type, v_range, v_type):
        if k_range is None:
            raise ValueError("k_range is required for KV list_range_filter_count().")
        if v_range is None:
            raise ValueError("v_range is required for KV list_range_filter_count().")
        _require_callable(predicate, "list_range_filter_count")
        _require_k_type(k_type, "list_range_filter_count")
        _require_v_type(v_type, "list_range_filter_count")
        return to_python(
            _BINDINGS.kv_list_range_filter_count(
                self.raw_handle(),
                list_name,
                _bi_predicate_proxy(predicate),
                to_edn_form(k_range),
                k_type,
                to_edn_form(v_range),
                v_type,
            )
        )

    def list_range_keep(self, list_name, fn, k_range, k_type, v_range, v_type, limit=None, offset=None):
        if k_range is None:
            raise ValueError("k_range is required for KV list_range_keep().")
        if v_range is None:
            raise ValueError("v_range is required for KV list_range_keep().")
        _require_callable(fn, "list_range_keep")
        _require_k_type(k_type, "list_range_keep")
        _require_v_type(v_type, "list_range_keep")
        return _slice_page(
            to_python(
                _BINDINGS.kv_list_range_keep(
                    self.raw_handle(),
                    list_name,
                    _bi_function_proxy(fn),
                    to_edn_form(k_range),
                    k_type,
                    to_edn_form(v_range),
                    v_type,
                )
            ),
            limit,
            offset,
        )

    def list_range_some(self, list_name, fn, k_range, k_type, v_range, v_type):
        if k_range is None:
            raise ValueError("k_range is required for KV list_range_some().")
        if v_range is None:
            raise ValueError("v_range is required for KV list_range_some().")
        _require_callable(fn, "list_range_some")
        _require_k_type(k_type, "list_range_some")
        _require_v_type(v_type, "list_range_some")
        return to_python(
            _BINDINGS.kv_list_range_some(
                self.raw_handle(),
                list_name,
                _bi_function_proxy(fn),
                to_edn_form(k_range),
                k_type,
                to_edn_form(v_range),
                v_type,
            )
        )

    def list_range(self, list_name, k_range, k_type, v_range, v_type, limit=None, offset=None):
        if k_range is None:
            raise ValueError("k_range is required for KV list_range().")
        if v_range is None:
            raise ValueError("v_range is required for KV list_range().")
        _require_k_type(k_type, "list_range")
        _require_v_type(v_type, "list_range")
        return _slice_page(
            to_python(
                _BINDINGS.core_invoke(
                    "list-range",
                    [
                        self.raw_handle(),
                        list_name,
                        to_edn_form(k_range),
                        _BINDINGS.kv_type(k_type),
                        to_edn_form(v_range),
                        _BINDINGS.kv_type(v_type),
                    ],
                )
            ),
            limit,
            offset,
        )

    def list_range_count(self, list_name, k_range, k_type):
        if k_range is None:
            raise ValueError("k_range is required for KV list_range_count().")
        _require_k_type(k_type, "list_range_count")
        return to_python(
            _BINDINGS.core_invoke(
                "list-range-count",
                [self.raw_handle(), list_name, to_edn_form(k_range), _BINDINGS.kv_type(k_type)],
            )
        )

    def list_range_first(self, list_name, k_range, k_type, v_range, v_type):
        if k_range is None:
            raise ValueError("k_range is required for KV list_range_first().")
        if v_range is None:
            raise ValueError("v_range is required for KV list_range_first().")
        _require_k_type(k_type, "list_range_first")
        _require_v_type(v_type, "list_range_first")
        return to_python(
            _BINDINGS.core_invoke(
                "list-range-first",
                [
                    self.raw_handle(),
                    list_name,
                    to_edn_form(k_range),
                    _BINDINGS.kv_type(k_type),
                    to_edn_form(v_range),
                    _BINDINGS.kv_type(v_type),
                ],
            )
        )

    def list_range_first_n(self, list_name, n, k_range, k_type, v_range, v_type):
        if k_range is None:
            raise ValueError("k_range is required for KV list_range_first_n().")
        if v_range is None:
            raise ValueError("v_range is required for KV list_range_first_n().")
        _require_k_type(k_type, "list_range_first_n")
        _require_v_type(v_type, "list_range_first_n")
        return to_python(
            _BINDINGS.core_invoke(
                "list-range-first-n",
                [
                    self.raw_handle(),
                    list_name,
                    n,
                    to_edn_form(k_range),
                    _BINDINGS.kv_type(k_type),
                    to_edn_form(v_range),
                    _BINDINGS.kv_type(v_type),
                ],
            )
        )

    def key_range_list_count(self, list_name, k_range, k_type):
        if k_range is None:
            raise ValueError("k_range is required for KV key_range_list_count().")
        _require_k_type(k_type, "key_range_list_count")
        return to_python(
            _BINDINGS.core_invoke(
                "key-range-list-count",
                [self.raw_handle(), list_name, to_edn_form(k_range), _BINDINGS.kv_type(k_type)],
            )
        )

    def clear_dbi(self, dbi_name) -> None:
        _BINDINGS.core_invoke("clear-dbi", [self.raw_handle(), dbi_name])

    def drop_dbi(self, dbi_name) -> None:
        _BINDINGS.core_invoke("drop-dbi", [self.raw_handle(), dbi_name])


class KVTransaction(KV):
    """Explicit KV write transaction."""

    def __init__(self, handle) -> None:
        super().__init__(handle, owned=False)
        self._finished = False

    def active(self) -> bool:
        return self._handle is not None and not self._finished

    def commit(self):
        handle = self.raw_handle()
        try:
            return to_python(_BINDINGS.key_value_commit_transaction(handle))
        finally:
            self._finished = True
            self._handle = None

    def abort(self):
        handle = self.raw_handle()
        try:
            return to_python(_BINDINGS.key_value_abort_transaction(handle))
        finally:
            self._finished = True
            self._handle = None

    def close(self) -> None:
        if self.active():
            self.abort()
