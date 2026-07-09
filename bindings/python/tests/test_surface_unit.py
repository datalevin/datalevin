from __future__ import annotations

from collections.abc import Mapping
import json

import pytest

import datalevin
import datalevin._jvm as jvm_module
import datalevin.client as client_module
import datalevin.connection as connection_module
import datalevin.kv as kv_module
import datalevin._interop as interop_module
import datalevin.llm as llm_module
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

    def connection_transact(self, conn, tx_data, tx_meta=None):
        self.last_transact = (conn, tx_data, tx_meta)
        return {":tx-data": [("datom", 1, ":name", "Ada", 1, True)], ":tx-meta": tx_meta}

    def connection_db(self, conn):
        self.last_connection_db = conn
        return "DB"

    def core_invoke(self, function: str, args=None):
        self.core_calls = getattr(self, "core_calls", [])
        self.core_calls.append((function, list(args or ())))
        self.last_core_invoke = (function, list(args or ()))
        if function == "datalog-index-cache-limit":
            if len(args or ()) > 1:
                self.cache_limit = int(args[1])
            return getattr(self, "cache_limit", 512)
        if function == "max-eid":
            return 42
        if function == "explicit-transaction-timeout":
            if args:
                self.explicit_transaction_timeout = args[0]
            return getattr(self, "explicit_transaction_timeout", None)
        if function == "set-explicit-transaction-timeout!":
            self.explicit_transaction_timeout = args[0]
            return self.explicit_transaction_timeout
        return {"function": function, "args": list(args or ())}

    def connection_transact_async(self, conn, tx_data, tx_meta=None):
        self.last_transact_async = (conn, tx_data, tx_meta)
        return "TX_FUTURE"

    def connection_tx_data_to_simulated_report(self, conn, tx_data):
        self.last_simulated_report = (conn, tx_data)
        return {":tx-data": [("datom", 1, ":name", "Ada", 1, True)], ":tempids": {}}

    def connection_listen(self, conn, key_or_listener, listener=None):
        self.last_connection_listen = (conn, key_or_listener, listener)
        return "LISTENER_KEY" if listener is None else key_or_listener

    def connection_unlisten(self, conn, key):
        self.last_connection_unlisten = (conn, key)

    def connection_with_transaction(self, conn, fn, timeout_ms=None):
        self.last_connection_with_transaction = conn
        self.last_connection_with_transaction_timeout = timeout_ms
        return fn(connection_module.Connection("TX_CONN", owned=False))

    def connection_abort_transact(self, conn):
        self.last_connection_abort_transact = conn
        return None

    def datom(self, e, attr, value, tx=None, added=None):
        return ("datom", e, attr, value, tx, added)

    def datom_is(self, value):
        return isinstance(value, (tuple, list)) and 3 <= len(value) <= 5

    def datom_e(self, value):
        return value[0]

    def datom_a(self, value):
        return value[1]

    def datom_v(self, value):
        return value[2]

    def datom_tx(self, value):
        return value[3] if len(value) > 3 else None

    def datom_added(self, value):
        return value[4] if len(value) > 4 else None

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

    def key_value_with_transaction(self, kv, fn, timeout_ms=None):
        self.last_kv_with_transaction = kv
        self.last_kv_with_transaction_timeout = timeout_ms
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

    def search_utils_create_analyzer(self, opts=None):
        self.last_search_utils_create_analyzer = opts
        return "ANALYZER"

    def search_utils_lower_case_token_filter(self):
        return "LOWER_FILTER"

    def search_utils_unaccent_token_filter(self):
        return "UNACCENT_FILTER"

    def search_utils_create_stop_words_token_filter(self, stop_words_or_predicate):
        self.last_search_utils_stop_words = stop_words_or_predicate
        return "STOP_FILTER"

    def search_utils_en_stop_words_token_filter(self):
        return "EN_STOP_FILTER"

    def search_utils_prefix_token_filter(self):
        return "PREFIX_FILTER"

    def search_utils_create_ngram_token_filter(self, min_gram_size, max_gram_size=None):
        self.last_search_utils_ngram = (min_gram_size, max_gram_size)
        return "NGRAM_FILTER"

    def search_utils_create_min_length_token_filter(self, min_length):
        self.last_search_utils_min_length = min_length
        return "MIN_FILTER"

    def search_utils_create_max_length_token_filter(self, max_length):
        self.last_search_utils_max_length = max_length
        return "MAX_FILTER"

    def search_utils_create_stemming_token_filter(self, language):
        self.last_search_utils_stemming = language
        return "STEM_FILTER"

    def search_utils_create_regexp_tokenizer(self, pattern):
        self.last_search_utils_regexp = pattern
        return "REGEXP_TOKENIZER"

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

    def new_llama_embedder(self, model_path, gpu_layers=0, ctx_size=0, batch_size=0, threads=0):
        self.last_llama_embedder = (model_path, gpu_layers, ctx_size, batch_size, threads)
        return "LLAMA_EMBEDDER"

    def close_llama_embedder(self, embedder):
        self.last_close_llama_embedder = embedder

    def llama_embedder_closed(self, embedder):
        self.last_llama_embedder_closed = embedder
        return False

    def llama_embedder_model_path(self, embedder):
        self.last_llama_embedder_model_path = embedder
        return "/models/embed.gguf"

    def llama_embedder_gpu_layers(self, embedder):
        return 1

    def llama_embedder_ctx_size(self, embedder):
        return 2048

    def llama_embedder_context_size(self, embedder):
        return 2048

    def llama_embedder_batch_size(self, embedder):
        return 128

    def llama_embedder_threads(self, embedder):
        return 4

    def llama_embedder_dimensions(self, embedder):
        return 2

    def llama_embedder_embed(self, embedder, text):
        self.last_llama_embedder_embed = (embedder, text)
        return [0.25, 0.75]

    def llama_embedder_embed_all(self, embedder, texts):
        self.last_llama_embedder_embed_all = (embedder, texts)
        return [[0.25, 0.75] for _text in texts]

    def llama_embedder_token_count(self, embedder, text):
        self.last_llama_embedder_token_count = (embedder, text)
        return 2

    def llama_embedder_tokenize(self, embedder, text):
        self.last_llama_embedder_tokenize = (embedder, text)
        return [1, 2]

    def llama_embedder_detokenize(self, embedder, tokens):
        self.last_llama_embedder_detokenize = (embedder, tokens)
        return "hi"

    def llama_embedder_truncate_text(self, embedder, text, max_tokens):
        self.last_llama_embedder_truncate_text = (embedder, text, max_tokens)
        return text

    def new_llama_generator(self, model_path, gpu_layers=0, ctx_size=0, threads=0):
        self.last_llama_generator = (model_path, gpu_layers, ctx_size, threads)
        return "LLAMA_GENERATOR"

    def close_llama_generator(self, generator):
        self.last_close_llama_generator = generator

    def llama_generator_closed(self, generator):
        self.last_llama_generator_closed = generator
        return False

    def llama_generator_model_path(self, generator):
        self.last_llama_generator_model_path = generator
        return "/models/generate.gguf"

    def llama_generator_gpu_layers(self, generator):
        return 2

    def llama_generator_ctx_size(self, generator):
        return 4096

    def llama_generator_context_size(self, generator):
        return 4096

    def llama_generator_threads(self, generator):
        return 6

    def llama_generator_token_count(self, generator, text):
        self.last_llama_generator_token_count = (generator, text)
        return 3

    def llama_generator_generate(self, generator, prompt, max_tokens):
        self.last_llama_generator_generate = (generator, prompt, max_tokens)
        return "generated"

    def llama_generator_summarize(self, generator, text, max_tokens):
        self.last_llama_generator_summarize = (generator, text, max_tokens)
        return "summary"

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

    assert client.replica_status("main") == {
        "function": "replica-status",
        "args": ["HANDLE", "main"],
    }
    spec = {
        ":ha-members": [{":node-id": 1, ":endpoint": "dtlv://node-a:8898/main"}],
        ":clear-leases?": True,
    }
    assert client.ha_update_membership("main", spec) == {
        "function": "ha-update-membership!",
        "args": ["HANDLE", "main", spec],
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
    monkeypatch.setattr(llm_module, "_BINDINGS", fake)
    monkeypatch.setattr(search_module, "_BINDINGS", fake)
    monkeypatch.setattr(vector_module, "_BINDINGS", fake)

    assert interop_module.exec_json("ping", {"count": 1}) == {"status": "ok"}
    assert fake.last_request == {"op": "ping", "args": {"count": 1}}
    attr_pred = {":udf/lang": ":python", ":udf/kind": ":predicate", ":udf/id": ":score/high?"}
    analyzer = {":udf/lang": ":python", ":udf/kind": ":analyzer", ":udf/id": ":text/hashtags"}
    query_analyzer = {
        ":udf/lang": ":python",
        ":udf/kind": ":query-analyzer",
        ":udf/id": ":text/plain-query",
    }
    assert interop_module.schema_attr(
        value_type=":db.type/string",
        unique=":db.unique/identity",
        attr_preds=attr_pred,
        fulltext_domains=["docs"],
        fulltext_auto_domain=True,
        embedding=True,
        embedding_domains=["embed"],
        embedding_auto_domain=True,
        vector_domains=["vectors"],
        idoc_format="json",
        idoc_domain="profiles",
        idoc_indexed_paths=[":status"],
        idoc_excluded_paths=[[":profile", ":raw"]],
    ) == {
        ":db/valueType": ":db.type/string",
        ":db/unique": ":db.unique/identity",
        ":db.attr/preds": attr_pred,
        ":db.fulltext/domains": ["docs"],
        ":db.fulltext/autoDomain": True,
        ":db/embedding": True,
        ":db.embedding/domains": ["embed"],
        ":db.embedding/autoDomain": True,
        ":db.vec/domains": ["vectors"],
        ":db/idocFormat": ":json",
        ":db/domain": "profiles",
        ":db.idoc/indexedPaths": [":status"],
        ":db.idoc/excludedPaths": [[":profile", ":raw"]],
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
    assert interop_module.idoc_attr(
        format="json",
        domain="profiles",
        indexed_paths=[":status", [":profile", ":age"]],
        excluded_paths=[":raw"],
    ) == {
        ":db/valueType": ":db.type/idoc",
        ":db/idocFormat": ":json",
        ":db/domain": "profiles",
        ":db.idoc/indexedPaths": [":status", [":profile", ":age"]],
        ":db.idoc/excludedPaths": [":raw"],
    }
    assert interop_module.idoc_domain(
        indexed_paths=[":status"],
        excluded_paths=[[":profile", ":raw"]],
    ) == {
        ":indexed-paths": [":status"],
        ":excluded-paths": [[":profile", ":raw"]],
    }
    assert interop_module.search_options(
        top=5,
        limit=2,
        offset=4,
        paging_cache_pages=3,
        display="refs+scores",
        domains=["docs"],
        analyzer=analyzer,
        query_analyzer=query_analyzer,
    ) == {
        ":top": 5,
        ":limit": 2,
        ":offset": 4,
        ":paging-cache-pages": 3,
        ":display": ":refs+scores",
        ":domains": ["docs"],
        ":analyzer": analyzer,
        ":query-analyzer": query_analyzer,
    }
    assert interop_module.search_domain(
        domain="docs",
        index_position=True,
        include_text=True,
        indexing_mode="async",
        analyzer=analyzer,
        query_analyzer=query_analyzer,
    ) == {
        ":domain": "docs",
        ":index-position?": True,
        ":include-text?": True,
        ":indexing-mode": ":async",
        ":analyzer": analyzer,
        ":query-analyzer": query_analyzer,
    }
    assert interop_module.vector_options(dimensions=384, metric_type="cosine") == {
        ":dimensions": 384,
        ":metric-type": ":cosine",
    }
    assert interop_module.embedding_options(
        provider="openai-compatible",
        model="text-embedding-3-small",
        base_url="https://api.openai.com/v1",
        endpoint="https://embed.internal/v1/embeddings",
        api_key="secret-key",
        api_key_env="OPENAI_API_KEY",
        headers={"X-Trace": "1"},
        timeout_ms=3210,
        query_prefix="query: ",
        document_prefix="passage: ",
        request_dimensions=1536,
        embedding_metadata={":source": "surface-test"},
        dimensions=1536,
        metric_type="cosine",
        indexing_mode="async",
    ) == {
        ":provider": ":openai-compatible",
        ":model": "text-embedding-3-small",
        ":base-url": "https://api.openai.com/v1",
        ":endpoint": "https://embed.internal/v1/embeddings",
        ":api-key": "secret-key",
        ":api-key-env": "OPENAI_API_KEY",
        ":headers": {"X-Trace": "1"},
        ":timeout-ms": 3210,
        ":query-prefix": "query: ",
        ":document-prefix": "passage: ",
        ":request-dimensions": 1536,
        ":embedding-metadata": {":source": "surface-test"},
        ":dimensions": 1536,
        ":metric-type": ":cosine",
        ":indexing-mode": ":async",
    }
    assert interop_module.idoc_options(domains=["profiles"]) == {
        ":domains": ["profiles"],
    }
    assert interop_module.tx_entity(-1, name="Ada") == {":db/id": -1, ":name": "Ada"}
    assert interop_module.tx_add(1, "name", "Ada") == [":db/add", 1, ":name", "Ada"]
    assert interop_module.tx_retract(1, ":name", "Ada") == [":db/retract", 1, ":name", "Ada"]
    assert interop_module.tx_retract_entity(1) == [":db/retractEntity", 1]
    assert interop_module.tx_ensure(":score/high?", -1, 10) == [":db/ensure", ":score/high?", -1, 10]

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

    datom_value = interop_module.datom(1, ":name", "Ada", 2, True)
    assert datom_value == (1, ":name", "Ada", 2, True)
    assert interop_module.datom_is(datom_value) is True
    assert interop_module.datom_e(datom_value) == 1
    assert interop_module.datom_a(datom_value) == ":name"
    assert interop_module.datom_v(datom_value) == "Ada"
    assert interop_module.datom_tx(datom_value) == 2
    assert interop_module.datom_added(datom_value) is True
    for helper in [
        "keyword",
        "max_eid",
        "abort_transact",
        "new_search_engine",
        "new_vector_index",
        "read_edn",
        "re_index",
        "search_index_writer",
        "symbol",
        "tx_data_to_simulated_report",
        "write_edn",
    ]:
        assert callable(getattr(interop_module, helper))
    assert init_conn.fill_db([(2, ":name", "Bob")]) is init_conn
    assert fake.last_fill_db == ("INIT_CONN", [(2, ":name", "Bob")])
    tx_data = [{":db/id": -1, ":name": "Ada"}]
    tx_meta = {":source": "surface"}
    assert init_conn.transact(tx_data, tx_meta)[":tx-data"]
    assert fake.last_transact == ("INIT_CONN", tx_data, tx_meta)
    assert interop_module.transact(init_conn, tx_data)[":tx-data"]
    assert fake.last_transact == ("INIT_CONN", tx_data, None)
    assert init_conn.datalog_index_cache_limit() == 512
    assert fake.last_connection_db == "INIT_CONN"
    assert fake.last_core_invoke == ("datalog-index-cache-limit", ["DB"])
    assert init_conn.max_eid() == 42
    assert fake.last_core_invoke == ("max-eid", ["DB"])
    assert interop_module.max_eid(init_conn) == 42
    assert fake.last_core_invoke == ("max-eid", ["DB"])
    assert interop_module.explicit_transaction_timeout() is None
    assert fake.last_core_invoke == ("explicit-transaction-timeout", [])
    assert interop_module.explicit_transaction_timeout(400) == 400
    assert fake.last_core_invoke == ("explicit-transaction-timeout", [400])
    assert interop_module.set_explicit_transaction_timeout(None) is None
    assert fake.last_core_invoke == ("set-explicit-transaction-timeout!", [None])
    assert init_conn.datalog_index_cache_limit(16) == 16
    assert fake.core_calls[-2:] == [
        ("datalog-index-cache-limit", ["DB", 16]),
        ("datalog-index-cache-limit", ["DB"]),
    ]
    assert interop_module.fill_db(init_conn, [(3, ":name", "Cara")]) is init_conn
    assert fake.last_fill_db == (init_conn, [(3, ":name", "Cara")])
    assert interop_module.re_index(init_conn, opts={":backup?": False}) is init_conn
    assert fake.last_connection_re_index == ("INIT_CONN", None, {":backup?": False})
    assert init_conn.with_transaction(lambda tx: tx.raw_handle()) == "TX_CONN"
    assert fake.last_connection_with_transaction == "INIT_CONN"
    assert fake.last_connection_with_transaction_timeout is None
    assert init_conn.with_transaction(lambda tx: tx.raw_handle(), timeout_ms=250) == "TX_CONN"
    assert fake.last_connection_with_transaction_timeout == 250
    assert interop_module.with_transaction(init_conn, lambda tx: tx.raw_handle()) == "TX_CONN"
    assert interop_module.with_transaction(
        init_conn, lambda tx: tx.raw_handle(), timeout_ms=300
    ) == "TX_CONN"
    assert fake.last_connection_with_transaction_timeout == 300
    assert init_conn.abort_transact() is None
    assert fake.last_connection_abort_transact == "INIT_CONN"
    assert interop_module.abort_transact(init_conn) is None
    assert fake.last_connection_abort_transact == "INIT_CONN"
    assert init_conn.tx_data_to_simulated_report([{":db/id": -1, ":name": "Ada"}])[":tx-data"]
    assert fake.last_simulated_report == ("INIT_CONN", [{":db/id": -1, ":name": "Ada"}])
    assert interop_module.tx_data_to_simulated_report(init_conn, [{":db/id": -2, ":name": "Bob"}])[":tx-data"]
    assert fake.last_simulated_report == ("INIT_CONN", [{":db/id": -2, ":name": "Bob"}])
    assert init_conn.listen(lambda _report: None) == "LISTENER_KEY"
    assert fake.last_connection_listen[0] == "INIT_CONN"
    init_conn.unlisten("LISTENER_KEY")
    assert fake.last_connection_unlisten == ("INIT_CONN", "LISTENER_KEY")
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
    assert fake.last_kv_with_transaction_timeout is None
    assert kv.with_transaction(lambda tx: tx.raw_handle(), timeout_ms=125) == "KV_TX"
    assert fake.last_kv_with_transaction_timeout == 125
    assert interop_module.with_transaction(kv, lambda tx: tx.raw_handle(), timeout_ms=175) == "KV_TX"
    assert fake.last_kv_with_transaction_timeout == 175
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
    tokenizer = search_module.create_regexp_tokenizer(r"\s+")
    lower_filter = search_module.lower_case_token_filter()
    stop_filter = search_module.create_stop_words_token_filter(["and", "the"])
    prefix_filter = search_module.prefix_token_filter()
    assert tokenizer == "REGEXP_TOKENIZER"
    assert lower_filter == "LOWER_FILTER"
    assert stop_filter == "STOP_FILTER"
    assert search_module.unaccent_token_filter() == "UNACCENT_FILTER"
    assert search_module.en_stop_words_token_filter() == "EN_STOP_FILTER"
    assert prefix_filter == "PREFIX_FILTER"
    assert search_module.create_ngram_token_filter(2) == "NGRAM_FILTER"
    assert fake.last_search_utils_ngram == (2, None)
    assert search_module.create_ngram_token_filter(2, 4) == "NGRAM_FILTER"
    assert fake.last_search_utils_ngram == (2, 4)
    assert search_module.create_min_length_token_filter(3) == "MIN_FILTER"
    assert fake.last_search_utils_min_length == 3
    assert search_module.create_max_length_token_filter(8) == "MAX_FILTER"
    assert fake.last_search_utils_max_length == 8
    assert search_module.create_stemming_token_filter("english") == "STEM_FILTER"
    assert fake.last_search_utils_stemming == "english"
    assert search_module.create_analyzer(tokenizer=tokenizer, token_filters=[lower_filter, stop_filter]) == "ANALYZER"
    assert fake.last_search_utils_create_analyzer == {
        ":tokenizer": "REGEXP_TOKENIZER",
        ":token-filters": ["LOWER_FILTER", "STOP_FILTER"],
    }
    assert callable(datalevin.create_analyzer)
    assert callable(datalevin.create_regexp_tokenizer)
    assert callable(datalevin.lower_case_token_filter)
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
    embedder = datalevin.new_llama_embedder(
        "/models/embed.gguf",
        gpu_layers=1,
        ctx_size=2048,
        batch_size=128,
        threads=4,
    )
    assert embedder.raw_handle() == "LLAMA_EMBEDDER"
    assert fake.last_llama_embedder == ("/models/embed.gguf", 1, 2048, 128, 4)
    assert embedder.model_path() == "/models/embed.gguf"
    assert embedder.gpu_layers() == 1
    assert embedder.ctx_size() == 2048
    assert embedder.context_size() == 2048
    assert embedder.batch_size() == 128
    assert embedder.threads() == 4
    assert embedder.dimensions() == 2
    assert embedder.embed("hi") == [0.25, 0.75]
    assert embedder.embed_all(["hi", "there"]) == [[0.25, 0.75], [0.25, 0.75]]
    assert embedder.token_count("hi") == 2
    assert embedder.tokenize("hi") == [1, 2]
    assert embedder.detokenize([1, 2]) == "hi"
    assert embedder.truncate_text("hi there", 2) == "hi there"
    embedder.close()
    assert fake.last_close_llama_embedder == "LLAMA_EMBEDDER"
    generator = datalevin.new_llama_generator(
        "/models/generate.gguf",
        gpu_layers=2,
        ctx_size=4096,
        threads=6,
    )
    assert generator.raw_handle() == "LLAMA_GENERATOR"
    assert fake.last_llama_generator == ("/models/generate.gguf", 2, 4096, 6)
    assert generator.model_path() == "/models/generate.gguf"
    assert generator.gpu_layers() == 2
    assert generator.ctx_size() == 4096
    assert generator.context_size() == 4096
    assert generator.threads() == 6
    assert generator.token_count("prompt") == 3
    assert generator.generate("prompt", 8) == "generated"
    assert generator.summarize("long text", 8) == "summary"
    generator.close()
    assert fake.last_close_llama_generator == "LLAMA_GENERATOR"

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
    assert callable(getattr(udf_module.UdfRegistry, "analyzer_udf"))
    assert callable(getattr(udf_module.UdfRegistry, "query_analyzer_udf"))


def test_kv_public_surface_includes_richer_operations() -> None:
    for method in [
        "copy",
        "create_snapshot",
        "del_list_items",
        "gc_tx_log_segments",
        "get_by_rank",
        "get_entry_by_rank",
        "get_env_flags",
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
        "list_range_filter_raw",
        "list_range_filter_count",
        "list_range_filter_count_raw",
        "list_range_first",
        "list_range_first_n",
        "list_range_keep",
        "list_range_keep_raw",
        "list_range_some",
        "list_range_some_raw",
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
        "set_env_flags",
        "stat",
        "sync",
        "transaction",
        "tx_log_watermarks",
        "visit",
        "visit_key_range",
        "visit_key_range_raw",
        "visit_list",
        "visit_list_raw",
        "visit_list_range",
        "visit_list_range_raw",
        "visit_raw",
        "with_transaction",
    ]:
        assert callable(getattr(kv_module.KV, method))
    assert callable(getattr(datalevin.RawBuffer, "read"))
    assert callable(getattr(datalevin.RawBuffer, "bytes"))
    assert callable(getattr(datalevin.RawKV, "read_key"))
    assert callable(getattr(datalevin.RawKV, "read_value"))
    assert callable(getattr(datalevin.RawKV, "key_bytes"))
    assert callable(getattr(datalevin.RawKV, "value_bytes"))
    assert callable(getattr(datalevin.KVTransaction, "abort"))
    assert callable(getattr(datalevin.KVTransaction, "active"))
    assert callable(getattr(datalevin.KVTransaction, "commit"))


def test_connection_public_surface_includes_bulk_load_operations() -> None:
    assert callable(getattr(datalevin, "abort_transact"))
    assert callable(getattr(datalevin, "analyze"))
    assert callable(getattr(datalevin, "cardinality"))
    assert callable(getattr(datalevin, "explicit_transaction_timeout"))
    assert callable(getattr(datalevin, "set_explicit_transaction_timeout"))
    assert callable(getattr(datalevin, "transact"))
    assert issubclass(datalevin.Entity, Mapping)
    assert callable(getattr(connection_module.Connection, "fill_db"))
    assert callable(getattr(datalevin.Database, "analyze"))
    assert callable(getattr(datalevin.Database, "cardinality"))
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
    assert callable(getattr(datalevin, "new_llama_embedder"))
    assert callable(getattr(datalevin, "new_llama_generator"))
    for method in [
        "batch_size",
        "context_size",
        "ctx_size",
        "detokenize",
        "dimensions",
        "embed",
        "embed_all",
        "gpu_layers",
        "model_path",
        "threads",
        "token_count",
        "tokenize",
        "truncate_text",
    ]:
        assert callable(getattr(datalevin.LlamaEmbedder, method))
    for method in [
        "context_size",
        "ctx_size",
        "generate",
        "gpu_layers",
        "model_path",
        "summarize",
        "threads",
        "token_count",
    ]:
        assert callable(getattr(datalevin.LlamaGenerator, method))
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
        "abort_transact",
        "analyze",
        "cardinality",
        "count_datoms",
        "copy",
        "create_snapshot",
        "datalog_kv",
        "datoms",
        "entity_map",
        "fulltext_datoms",
        "gc_tx_log_segments",
        "index_range",
        "listen",
        "list_snapshots",
        "max_eid",
        "open_tx_log",
        "re_index",
        "rseek_datoms",
        "search_datoms",
        "seek_datoms",
        "tx_log_watermarks",
        "transact",
        "transact_async",
        "unlisten",
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
