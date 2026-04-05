from __future__ import annotations

import json

import pytest

import datalevin._jvm as jvm_module
import datalevin.client as client_module
import datalevin.connection as connection_module
import datalevin.kv as kv_module
import datalevin._interop as interop_module
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

    conn = interop_module.connect("/tmp/db", schema={":name": {}}, opts={":x": 1}, shared=True)
    assert conn.raw_handle() == "CONN"
    assert fake.last_connection == ("/tmp/db", {":name": {}}, {":x": 1}, True)

    kv = interop_module.open_kv("/tmp/kv", opts={":mapsize": 1})
    assert kv.raw_handle() == "KV"
    assert fake.last_kv == ("/tmp/kv", {":mapsize": 1})

    client = interop_module.new_client("dtlv://user:pass@host", opts={":pool-size": 1})
    assert client.raw_handle() == "CLIENT"
    assert fake.last_client == ("dtlv://user:pass@host", {":pool-size": 1})


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
