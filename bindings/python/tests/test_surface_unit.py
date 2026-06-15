from __future__ import annotations

import json

import pytest

import datalevin._jvm as jvm_module
import datalevin.client as client_module
import datalevin.connection as connection_module
import datalevin.kv as kv_module
import datalevin._interop as interop_module
import datalevin.udf as udf_module
from datalevin.errors import DatalevinError


class FakeClientBindings:
    def __init__(self) -> None:
        self.client_calls = []
        self.closed_handles = set()

    def client_invoke(self, function: str, args=None):
        args = list(args or ())
        self.client_calls.append((function, args))
        if function == "get-id":
            return f"id:{args[0]}"
        return {"function": function, "args": args}

    def close_client(self, handle) -> None:
        self.closed_handles.add(handle)

    def client_disconnected(self, handle) -> bool:
        return handle in self.closed_handles

    def database_type(self, value):
        return f"dbtype:{value}"

    def role(self, value):
        return f"role:{value}"

    def permission_keyword(self, value):
        return f"perm:{value}"

    def permission_target(self, object_type, target):
        return (object_type, target)

    def schema(self, value):
        return ("schema", value)

    def options(self, value):
        return ("options", value)

    def read_edn(self, value):
        return ("edn", value)


class FakeInteropBindings:
    def __init__(self) -> None:
        self.exec_response = json.dumps({"ok": True, "result": {"status": "ok"}})
        self.conn_closed = set()
        self.kv_closed = set()
        self.client_closed = set()

    def exec_json_raw(self, request_json: str):
        self.last_request = json.loads(request_json)
        return self.exec_response

    def create_connection(self, dir=None, schema=None, opts=None, *, shared: bool = False):
        self.last_connection = (dir, schema, opts, shared)
        return "CONN"

    def init_db(self, datoms, dir=None, schema=None, opts=None):
        self.last_init_db = (datoms, dir, schema, opts)
        return "INIT_CONN"

    def fill_db(self, conn, datoms):
        self.last_fill_db = (conn, datoms)
        return conn

    def connection_datalog_kv(self, conn):
        self.last_datalog_kv = conn
        return "DATALOG_KV"

    def connection_transact_async(self, conn, tx_data, tx_meta=None):
        self.last_transact_async = (conn, tx_data, tx_meta)
        return "TX_FUTURE"

    def datom(self, e, attr, value, tx=None, added=None):
        return ("datom", e, attr, value, tx, added)

    def close_connection(self, handle):
        self.conn_closed.add(handle)

    def connection_closed(self, handle):
        return handle in self.conn_closed

    def open_key_value(self, dir, opts=None):
        self.last_kv = (dir, opts)
        return "KV"

    def close_key_value(self, handle):
        self.kv_closed.add(handle)

    def key_value_closed(self, handle):
        return handle in self.kv_closed

    def new_client(self, uri, opts=None):
        self.last_client = (uri, opts)
        return "CLIENT"

    def close_client(self, handle):
        self.client_closed.add(handle)

    def client_disconnected(self, handle):
        return handle in self.client_closed


def test_client_wrapper_delegates_to_bindings(monkeypatch) -> None:
    fake = FakeClientBindings()
    monkeypatch.setattr(client_module, "_BINDINGS", fake)
    monkeypatch.setattr(client_module, "to_python", lambda value: value)
    monkeypatch.setattr(client_module, "to_query_input", lambda value: ("query-input", value))

    client = client_module.Client("HANDLE")
    other = client_module.Client("OTHER")

    assert client.client_id() == "id:HANDLE"

    assert client.open_database("main", "datalog") is None
    assert fake.client_calls[-1] == ("open-database", ["HANDLE", "main", "datalog"])

    assert client.open_database("main", "datalog", schema={":name": {}}, opts={":x": 1}, info=True) == {
        "function": "open-database",
        "args": [
            "HANDLE",
            "main",
            "datalog",
            ("schema", {":name": {}}),
            ("options", {":x": 1}),
            True,
        ],
    }

    client.create_database("main", "kv")
    assert fake.client_calls[-1] == ("create-database", ["HANDLE", "main", "dbtype:kv"])

    client.grant_permission("admins", ":read", ":datalevin.server/role", ":admins")
    assert fake.client_calls[-1] == (
        "grant-permission",
        [
            "HANDLE",
            "role:admins",
            "perm::read",
            "perm::datalevin.server/role",
            (":datalevin.server/role", ":admins"),
        ],
    )

    assert client.query_system("[:find ?e :where [?e :db/ident _]]", ":db/ident") == {
        "function": "query-system",
        "args": ["HANDLE", ("edn", "[:find ?e :where [?e :db/ident _]]"), ("query-input", ":db/ident")],
    }

    assert client.disconnected() is False
    client.disconnect_client("id:OTHER")
    assert other.disconnected() is True
    client.disconnect()
    assert client.disconnected() is True
    assert repr(client) == "<Client closed>"


def test_exec_json_and_public_factories(monkeypatch) -> None:
    fake = FakeInteropBindings()
    monkeypatch.setattr(interop_module, "_BINDINGS", fake)
    monkeypatch.setattr(connection_module, "_BINDINGS", fake)
    monkeypatch.setattr(kv_module, "_BINDINGS", fake)
    monkeypatch.setattr(client_module, "_BINDINGS", fake)

    assert interop_module.exec_json("ping", {"count": 1}) == {"status": "ok"}
    assert fake.last_request == {"op": "ping", "args": {"count": 1}}
    assert interop_module.schema_attr(value_type=":db.type/string", unique=":db.unique/identity") == {
        ":db/valueType": ":db.type/string",
        ":db/unique": ":db.unique/identity",
    }
    assert interop_module.tx_entity(-1, name="Ada") == {":db/id": -1, ":name": "Ada"}
    assert interop_module.tx_add(1, "name", "Ada") == [":db/add", 1, ":name", "Ada"]
    assert interop_module.tx_retract(1, ":name", "Ada") == [":db/retract", 1, ":name", "Ada"]
    assert interop_module.tx_retract_entity(1) == [":db/retractEntity", 1]

    conn_opts = {
        ":embedding-opts": {
            ":provider": ":openai-compatible",
            ":model": "text-embedding-3-small",
            ":api-key-env": "OPENAI_API_KEY",
        },
        ":client-opts": {
            ":pool-size": 1,
            ":ha-write-retry-timeout-ms": 5000,
        },
    }
    conn = interop_module.connect("/tmp/db", schema={":name": {}}, opts=conn_opts, shared=True)
    assert conn.raw_handle() == "CONN"
    assert fake.last_connection == ("/tmp/db", {":name": {}}, conn_opts, True)

    init_conn = interop_module.init_db([(1, ":name", "Ada")], dir="/tmp/init", schema={":name": {}})
    assert init_conn.raw_handle() == "INIT_CONN"
    assert fake.last_init_db == ([(1, ":name", "Ada")], "/tmp/init", {":name": {}}, None)

    assert interop_module.datom(1, ":name", "Ada") == (1, ":name", "Ada")
    for helper in ["keyword", "read_edn", "symbol", "write_edn"]:
        assert callable(getattr(interop_module, helper))
    assert init_conn.fill_db([(2, ":name", "Bob")]) is init_conn
    assert fake.last_fill_db == ("INIT_CONN", [(2, ":name", "Bob")])
    assert interop_module.fill_db(init_conn, [(3, ":name", "Cara")]) is init_conn
    assert fake.last_fill_db == (init_conn, [(3, ":name", "Cara")])
    assert callable(interop_module.transact_async)
    backing_kv = interop_module.datalog_kv(init_conn)
    assert backing_kv.raw_handle() == "DATALOG_KV"
    assert fake.last_datalog_kv == "INIT_CONN"

    kv = interop_module.open_kv("/tmp/kv", opts={":mapsize": 1})
    assert kv.raw_handle() == "KV"
    assert fake.last_kv == ("/tmp/kv", {":mapsize": 1})

    client_opts = {
        ":pool-size": 1,
        ":ha-write-retry-timeout-ms": 5000,
        ":ha-write-retry-delay-ms": 100,
    }
    client = interop_module.new_client("dtlv://user:pass@host", opts=client_opts)
    assert client.raw_handle() == "CLIENT"
    assert fake.last_client == ("dtlv://user:pass@host", client_opts)


def test_udf_descriptor_helper() -> None:
    assert udf_module.udf_descriptor("math/inc") == {
        ":udf/lang": ":java",
        ":udf/kind": ":query-fn",
        ":udf/id": ":math/inc",
    }
    assert udf_module.udf_descriptor(
        {"id": "math/positive?", "kind": "predicate", "version": "v1"}
    ) == {
        ":udf/lang": ":java",
        ":udf/kind": ":predicate",
        ":udf/id": ":math/positive?",
        ":udf/version": "v1",
    }
    assert callable(getattr(udf_module.UdfRegistry, "predicate_udf"))


def test_kv_public_surface_includes_richer_operations() -> None:
    for method in [
        "copy",
        "create_snapshot",
        "del_list_items",
        "gc_tx_log_segments",
        "get_by_rank",
        "get_first",
        "get_first_n",
        "get_list",
        "get_rank",
        "in_list",
        "key_range",
        "key_range_count",
        "list_count",
        "list_snapshots",
        "open_tx_log",
        "put_list_items",
        "range_count",
        "sample_kv",
        "stat",
        "sync",
        "tx_log_watermarks",
    ]:
        assert callable(getattr(kv_module.KV, method))


def test_connection_public_surface_includes_bulk_load_operations() -> None:
    assert callable(getattr(connection_module.Connection, "fill_db"))
    for method in [
        "count_datoms",
        "copy",
        "create_snapshot",
        "datalog_kv",
        "datoms",
        "fulltext_datoms",
        "gc_tx_log_segments",
        "index_range",
        "list_snapshots",
        "open_tx_log",
        "rseek_datoms",
        "search_datoms",
        "seek_datoms",
        "tx_log_watermarks",
        "transact_async",
    ]:
        assert callable(getattr(connection_module.Connection, method))


def test_exec_json_raises_datalevin_error(monkeypatch) -> None:
    fake = FakeInteropBindings()
    fake.exec_response = json.dumps(
        {"ok": False, "error": "boom", "type": "datalevin.test/error", "data": {"code": 42}}
    )
    monkeypatch.setattr(interop_module, "_BINDINGS", fake)

    with pytest.raises(DatalevinError) as exc_info:
        interop_module.exec_json("explode")

    assert str(exc_info.value) == "boom"
    assert exc_info.value.type_name == "datalevin.test/error"
    assert exc_info.value.data == {"code": 42}


def test_preferred_runtime_jar_prefers_shared_runtime_and_latest_version(tmp_path) -> None:
    legacy = tmp_path / "datalevin-java-0.10.6.jar"
    shared_old = tmp_path / "datalevin-runtime-0.10.6.jar"
    shared_new = tmp_path / "datalevin-runtime-0.10.15.jar"
    for path in (legacy, shared_old, shared_new):
        path.write_text("", encoding="utf-8")

    assert jvm_module._preferred_runtime_jar(tmp_path) == shared_new


def test_default_jvm_args_are_injected(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(jvm_module.DATALEVIN_JAVACPP_CACHEDIR_ENV, str(tmp_path))
    args = ["-Xmx1g"]

    jvm_module._ensure_default_jvm_args(args)
    jvm_module._ensure_javacpp_cachedir_arg(args)

    assert args[0] == "-Xmx1g"
    assert "--enable-native-access=ALL-UNNAMED" in args
    assert "--add-opens=java.base/java.lang=ALL-UNNAMED" in args
    assert "--add-opens=java.base/java.util=ALL-UNNAMED" in args
    assert "--add-opens=java.base/java.nio=ALL-UNNAMED" in args
    assert "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED" in args
    assert f"-Dorg.bytedeco.javacpp.cachedir={tmp_path}" in args
