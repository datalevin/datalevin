from __future__ import annotations

import json

import pytest

import datalevin
import datalevin._jvm as jvm_module
import datalevin.client as client_module
import datalevin.connection as connection_module
import datalevin.kv as kv_module
import datalevin._interop as interop_module
import datalevin.search as search_module
import datalevin.udf as udf_module
import datalevin.vector as vector_module
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

    def connection_with_transaction(self, conn, fn):
        self.last_connection_with_transaction = conn
        return fn(connection_module.Connection("TX_CONN", owned=False))

    def datom(self, e, attr, value, tx=None, added=None):
        return ("datom", e, attr, value, tx, added)

    def close_connection(self, handle):
        self.conn_closed.add(handle)

    def connection_closed(self, handle):
        return handle in self.conn_closed

    def connection_re_index(self, conn, schema=None, opts=None):
        self.last_connection_re_index = (conn, schema, opts)
        return conn

    def open_key_value(self, dir, opts=None):
        self.last_kv = (dir, opts)
        return "KV"

    def close_key_value(self, handle):
        self.kv_closed.add(handle)

    def key_value_closed(self, handle):
        return handle in self.kv_closed

    def key_value_begin_transaction(self, kv):
        self.last_kv_begin_transaction = kv
        return "KV_TX"

    def key_value_commit_transaction(self, tx):
        self.last_kv_commit_transaction = tx
        return ":committed"

    def key_value_abort_transaction(self, tx):
        self.last_kv_abort_transaction = tx
        return ":aborted"

    def key_value_with_transaction(self, kv, fn):
        self.last_kv_with_transaction = kv
        return fn(kv_module.KV("KV_TX", owned=False))

    def key_value_re_index(self, kv, opts=None):
        self.last_kv_re_index = (kv, opts)
        return kv

    def new_search_engine(self, kv, opts=None):
        self.last_search_engine = (kv, opts)
        return "SEARCH_ENGINE"

    def search_add_doc(self, search, doc_ref, doc_text, check_exist=None):
        self.last_search_add_doc = (search, doc_ref, doc_text, check_exist)

    def search_remove_doc(self, search, doc_ref):
        self.last_search_remove_doc = (search, doc_ref)

    def search_clear_docs(self, search):
        self.last_search_clear_docs = search

    def search_doc_indexed(self, search, doc_ref):
        self.last_search_doc_indexed = (search, doc_ref)
        return True

    def search_doc_count(self, search):
        self.last_search_doc_count = search
        return 2

    def search(self, search, query, opts=None):
        self.last_search = (search, query, opts)
        return ["doc-1"]

    def search_re_index(self, search, opts=None):
        self.last_search_re_index = (search, opts)
        return search

    def search_index_writer(self, kv, opts=None):
        self.last_search_writer = (kv, opts)
        return "SEARCH_WRITER"

    def search_write(self, writer, doc_ref, doc_text):
        self.last_search_write = (writer, doc_ref, doc_text)

    def search_commit(self, writer):
        self.last_search_commit = writer
        return ":transacted"

    def new_vector_index(self, kv, opts=None):
        self.last_vector_index = (kv, opts)
        return "VECTOR_INDEX"

    def close_vector_index(self, index):
        self.last_close_vector_index = index

    def vector_index_closed(self, index):
        self.last_vector_index_closed = index
        return False

    def vector_add_vec(self, index, vec_ref, vec_data):
        self.last_vector_add_vec = (index, vec_ref, vec_data)

    def vector_remove_vec(self, index, vec_ref):
        self.last_vector_remove_vec = (index, vec_ref)

    def vector_indexed(self, index, vec_ref):
        self.last_vector_indexed = (index, vec_ref)
        return True

    def vector_search(self, index, query_vec, opts=None):
        self.last_vector_search = (index, query_vec, opts)
        return [["vec-1", 0.0]]

    def vector_re_index(self, index, opts=None):
        self.last_vector_re_index = (index, opts)
        return index

    def vector_clear(self, index):
        self.last_vector_clear = index
        return True

    def vector_force_checkpoint(self, index):
        self.last_vector_force_checkpoint = index
        return True

    def vector_info(self, index):
        self.last_vector_info = index
        return {":size": 1}

    def vector_checkpoint_state(self, index):
        self.last_vector_checkpoint_state = index
        return {":snapshot-lsn": 1}

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
    monkeypatch.setattr(search_module, "_BINDINGS", fake)
    monkeypatch.setattr(vector_module, "_BINDINGS", fake)

    assert interop_module.exec_json("ping", {"count": 1}) == {"status": "ok"}
    assert fake.last_request == {"op": "ping", "args": {"count": 1}}
    assert interop_module.schema_attr(value_type=":db.type/string", unique=":db.unique/identity") == {
        ":db/valueType": ":db.type/string",
        ":db/unique": ":db.unique/identity",
    }
    assert interop_module.fulltext_attr(domains=["docs"], auto_domain=True) == {
        ":db/valueType": ":db.type/string",
        ":db/fulltext": True,
        ":db.fulltext/domains": ["docs"],
        ":db.fulltext/autoDomain": True,
    }
    assert interop_module.embedding_attr(domains=["docs"], auto_domain=True) == {
        ":db/valueType": ":db.type/string",
        ":db/embedding": True,
        ":db.embedding/domains": ["docs"],
        ":db.embedding/autoDomain": True,
    }
    assert interop_module.vector_attr(domains=["vectors"]) == {
        ":db/valueType": ":db.type/vec",
        ":db.vec/domains": ["vectors"],
    }
    assert interop_module.idoc_attr(format="json", domain="profiles") == {
        ":db/valueType": ":db.type/idoc",
        ":db/idocFormat": ":json",
        ":db/domain": "profiles",
    }
    assert interop_module.search_options(top=5, display="refs+scores", domains=["docs"]) == {
        ":top": 5,
        ":display": ":refs+scores",
        ":domains": ["docs"],
    }
    assert interop_module.search_domain(
        domain="docs",
        index_position=True,
        include_text=True,
        indexing_mode="async",
    ) == {
        ":domain": "docs",
        ":index-position?": True,
        ":include-text?": True,
        ":indexing-mode": ":async",
    }
    assert interop_module.vector_options(dimensions=384, metric_type="cosine") == {
        ":dimensions": 384,
        ":metric-type": ":cosine",
    }
    assert interop_module.embedding_options(
        provider="openai-compatible",
        model="text-embedding-3-small",
        api_key_env="OPENAI_API_KEY",
        request_dimensions=1536,
        metric_type="cosine",
    ) == {
        ":provider": ":openai-compatible",
        ":model": "text-embedding-3-small",
        ":api-key-env": "OPENAI_API_KEY",
        ":request-dimensions": 1536,
        ":metric-type": ":cosine",
    }
    assert interop_module.idoc_options(domains=["profiles"]) == {
        ":domains": ["profiles"],
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
    for helper in [
        "keyword",
        "new_search_engine",
        "new_vector_index",
        "read_edn",
        "re_index",
        "search_index_writer",
        "symbol",
        "write_edn",
    ]:
        assert callable(getattr(interop_module, helper))
    assert init_conn.fill_db([(2, ":name", "Bob")]) is init_conn
    assert fake.last_fill_db == ("INIT_CONN", [(2, ":name", "Bob")])
    assert interop_module.fill_db(init_conn, [(3, ":name", "Cara")]) is init_conn
    assert fake.last_fill_db == (init_conn, [(3, ":name", "Cara")])
    assert interop_module.re_index(init_conn, opts={":backup?": False}) is init_conn
    assert fake.last_connection_re_index == ("INIT_CONN", None, {":backup?": False})
    assert init_conn.with_transaction(lambda tx: tx.raw_handle()) == "TX_CONN"
    assert fake.last_connection_with_transaction == "INIT_CONN"
    assert interop_module.with_transaction(init_conn, lambda tx: tx.raw_handle()) == "TX_CONN"
    assert callable(interop_module.transact_async)
    backing_kv = interop_module.datalog_kv(init_conn)
    assert backing_kv.raw_handle() == "DATALOG_KV"
    assert fake.last_datalog_kv == "INIT_CONN"

    kv = interop_module.open_kv("/tmp/kv", opts={":mapsize": 1})
    assert kv.raw_handle() == "KV"
    assert fake.last_kv == ("/tmp/kv", {":mapsize": 1})
    tx = kv.begin_transaction()
    assert tx.raw_handle() == "KV_TX"
    assert fake.last_kv_begin_transaction == "KV"
    assert tx.commit() == ":committed"
    assert fake.last_kv_commit_transaction == "KV_TX"
    tx = kv.transaction()
    assert tx.abort() == ":aborted"
    assert fake.last_kv_abort_transaction == "KV_TX"
    assert kv.with_transaction(lambda tx: tx.raw_handle()) == "KV_TX"
    assert fake.last_kv_with_transaction == "KV"
    assert kv.re_index(opts={":backup?": False}) is kv
    assert fake.last_kv_re_index == ("KV", {":backup?": False})
    engine = interop_module.new_search_engine(kv, opts={":include-text?": True})
    assert engine.raw_handle() == "SEARCH_ENGINE"
    assert fake.last_search_engine == (kv, {":include-text?": True})
    assert engine.add_doc("doc-1", "pizza and pasta") is engine
    assert fake.last_search_add_doc == ("SEARCH_ENGINE", "doc-1", "pizza and pasta", None)
    assert engine.add_doc("doc-2", "just pie", check_exist=False) is engine
    assert fake.last_search_add_doc == ("SEARCH_ENGINE", "doc-2", "just pie", False)
    assert engine.doc_indexed("doc-1") is True
    assert fake.last_search_doc_indexed == ("SEARCH_ENGINE", "doc-1")
    assert engine.doc_count() == 2
    assert fake.last_search_doc_count == "SEARCH_ENGINE"
    assert engine.search("pizza", opts={":top": 1}) == ["doc-1"]
    assert fake.last_search == ("SEARCH_ENGINE", "pizza", {":top": 1})
    assert engine.re_index(opts={":include-text?": True}) is engine
    assert fake.last_search_re_index == ("SEARCH_ENGINE", {":include-text?": True})
    assert engine.remove_doc("doc-2") is engine
    assert fake.last_search_remove_doc == ("SEARCH_ENGINE", "doc-2")
    assert engine.clear_docs() is engine
    assert fake.last_search_clear_docs == "SEARCH_ENGINE"
    writer = interop_module.search_index_writer(kv, opts={":include-text?": True})
    assert writer.raw_handle() == "SEARCH_WRITER"
    assert fake.last_search_writer == (kv, {":include-text?": True})
    assert writer.write("doc-1", "pizza and pasta") is writer
    assert fake.last_search_write == ("SEARCH_WRITER", "doc-1", "pizza and pasta")
    assert writer.commit() == ":transacted"
    assert fake.last_search_commit == "SEARCH_WRITER"
    assert writer.closed() is True
    index = interop_module.new_vector_index(kv, opts={":dimensions": 2})
    assert index.raw_handle() == "VECTOR_INDEX"
    assert fake.last_vector_index == (kv, {":dimensions": 2})
    assert index.add_vec("vec-1", [1.0, 0.0]) is index
    assert fake.last_vector_add_vec == ("VECTOR_INDEX", "vec-1", [1.0, 0.0])
    assert index.vec_indexed("vec-1") is True
    assert fake.last_vector_indexed == ("VECTOR_INDEX", "vec-1")
    assert index.search_vec([1.0, 0.0], opts={":display": ":refs+dists"}) == [["vec-1", 0.0]]
    assert fake.last_vector_search == ("VECTOR_INDEX", [1.0, 0.0], {":display": ":refs+dists"})
    assert index.info() == {":size": 1}
    assert fake.last_vector_info == "VECTOR_INDEX"
    assert index.force_checkpoint() is index
    assert fake.last_vector_force_checkpoint == "VECTOR_INDEX"
    assert index.checkpoint_state() == {":snapshot-lsn": 1}
    assert fake.last_vector_checkpoint_state == "VECTOR_INDEX"
    assert index.re_index(opts={":dimensions": 2}) is index
    assert fake.last_vector_re_index == ("VECTOR_INDEX", {":dimensions": 2})
    assert index.remove_vec("vec-1") is index
    assert fake.last_vector_remove_vec == ("VECTOR_INDEX", "vec-1")
    assert index.clear() is index
    assert fake.last_vector_clear == "VECTOR_INDEX"
    assert index.closed() is True

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
        "key_range_list_count",
        "key_range",
        "key_range_count",
        "list_count",
        "list_range",
        "list_range_count",
        "list_range_filter",
        "list_range_filter_count",
        "list_range_first",
        "list_range_first_n",
        "list_range_keep",
        "list_range_some",
        "list_snapshots",
        "new_search_engine",
        "new_vector_index",
        "open_tx_log",
        "put_list_items",
        "range_count",
        "begin_transaction",
        "re_index",
        "sample_kv",
        "search_index_writer",
        "stat",
        "sync",
        "transaction",
        "tx_log_watermarks",
        "visit_list",
        "visit_list_range",
        "with_transaction",
    ]:
        assert callable(getattr(kv_module.KV, method))
    assert callable(getattr(datalevin.KVTransaction, "abort"))
    assert callable(getattr(datalevin.KVTransaction, "active"))
    assert callable(getattr(datalevin.KVTransaction, "commit"))


def test_connection_public_surface_includes_bulk_load_operations() -> None:
    assert callable(getattr(connection_module.Connection, "fill_db"))
    assert callable(getattr(datalevin.Entity, "touch"))
    assert callable(getattr(datalevin.SearchEngine, "add_doc"))
    assert callable(getattr(datalevin.SearchEngine, "clear_docs"))
    assert callable(getattr(datalevin.SearchEngine, "doc_count"))
    assert callable(getattr(datalevin.SearchEngine, "doc_indexed"))
    assert callable(getattr(datalevin.SearchEngine, "re_index"))
    assert callable(getattr(datalevin.SearchEngine, "remove_doc"))
    assert callable(getattr(datalevin.SearchEngine, "search"))
    assert callable(getattr(datalevin.SearchIndexWriter, "commit"))
    assert callable(getattr(datalevin.SearchIndexWriter, "write"))
    assert callable(getattr(datalevin.VectorIndex, "add_vec"))
    assert callable(getattr(datalevin.VectorIndex, "checkpoint_state"))
    assert callable(getattr(datalevin.VectorIndex, "clear"))
    assert callable(getattr(datalevin.VectorIndex, "force_checkpoint"))
    assert callable(getattr(datalevin.VectorIndex, "info"))
    assert callable(getattr(datalevin.VectorIndex, "re_index"))
    assert callable(getattr(datalevin.VectorIndex, "remove_vec"))
    assert callable(getattr(datalevin.VectorIndex, "search_vec"))
    assert callable(getattr(datalevin.VectorIndex, "vec_indexed"))
    for method in [
        "count_datoms",
        "copy",
        "create_snapshot",
        "datalog_kv",
        "datoms",
        "entity_map",
        "fulltext_datoms",
        "gc_tx_log_segments",
        "index_range",
        "list_snapshots",
        "open_tx_log",
        "re_index",
        "rseek_datoms",
        "search_datoms",
        "seek_datoms",
        "tx_log_watermarks",
        "transact_async",
        "with_transaction",
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
