"""High-level Python wrapper for Datalevin KV handles."""

from __future__ import annotations

from ._convert import to_java, to_python
from ._interop import _BINDINGS
from ._resource import ResourceWrapper


def _slice_page(items, limit=None, offset=None):
    start = 0 if offset is None else max(offset, 0)
    if limit is None:
        return items[start:]
    return items[start : start + max(limit, 0)]


class KV(ResourceWrapper):
    """Thin Python wrapper over a raw Datalevin KV handle."""

    def __init__(self, handle) -> None:
        super().__init__(handle, _BINDINGS.close_key_value, _BINDINGS.key_value_closed, "kv")

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

    def list_dbis(self):
        return to_python(_BINDINGS.core_invoke("list-dbis", [self.raw_handle()]))

    def entries(self, dbi_name):
        return to_python(_BINDINGS.core_invoke("entries", [self.raw_handle(), dbi_name]))

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

    def get_range(self, dbi_name, key_range, k_type=None, v_type=None, limit=None, offset=None):
        if key_range is None:
            raise ValueError("key_range is required for KV get_range().")
        if v_type is not None and k_type is None:
            raise ValueError("v_type requires k_type for KV get_range().")
        args = [self.raw_handle(), dbi_name, to_java(key_range)]
        if k_type is not None:
            args.append(_BINDINGS.kv_type(k_type))
            if v_type is not None:
                args.append(_BINDINGS.kv_type(v_type))
        return _slice_page(to_python(_BINDINGS.core_invoke("get-range", args)), limit, offset)

    def clear_dbi(self, dbi_name) -> None:
        _BINDINGS.core_invoke("clear-dbi", [self.raw_handle(), dbi_name])

    def drop_dbi(self, dbi_name) -> None:
        _BINDINGS.core_invoke("drop-dbi", [self.raw_handle(), dbi_name])
