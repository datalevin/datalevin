from __future__ import annotations

from pathlib import Path

import pytest

from datalevin import (
    api_info,
    connect,
    create_analyzer,
    create_regexp_tokenizer,
    create_stop_words_token_filter,
    lower_case_token_filter,
    new_search_engine,
    new_vector_index,
    open_kv,
    re_index,
    search_domain,
    search_index_writer,
    unaccent_token_filter,
    with_transaction,
)
pytestmark = pytest.mark.usefixtures("require_runtime")


def test_anonymous_connection_forwards_schema_and_options() -> None:
    with connect(
        None,
        schema={":name": {":db/valueType": ":db.type/string"}},
        opts={":kv-opts": {":inmemory?": True}},
    ) as conn:
        assert conn.schema()[":name"][":db/valueType"] == ":db.type/string"
        assert conn.opts()[":kv-opts"][":inmemory?"] is True


def test_local_datalog_smoke(tmp_path: Path) -> None:
    info = api_info()
    assert isinstance(info, dict)
    assert "datalevin-version" in info

    db_dir = tmp_path / "db"
    with connect(
        str(db_dir),
        schema={":name": {":db/valueType": ":db.type/string"}},
    ) as conn:
        conn.transact([{":db/id": -1, ":name": "Ada"}])
        assert conn.with_transaction(
            lambda tx: tx.transact([{":db/id": -2, ":name": "Bob"}]) and "ok"
        ) == "ok"
        assert with_transaction(
            conn,
            lambda tx: tx.transact([{":db/id": -3, ":name": "Cara"}]) and "top",
        ) == "top"
        assert conn.re_index() is conn
        names = conn.query("[:find [?name ...] :where [?e :name ?name]]")
        assert sorted(names) == ["Ada", "Bob", "Cara"]


def test_structured_query_forms_and_inputs(tmp_path: Path) -> None:
    db_dir = tmp_path / "db"
    with connect(
        str(db_dir),
        schema={":name": {":db/valueType": ":db.type/string"}},
    ) as conn:
        conn.transact([{":db/id": -1, ":name": "Ada"}])

        entity_id = conn.query(
            [":find", "?e", ".", ":in", "$", "?attr", "?value", ":where", ["?e", "?attr", "?value"]],
            ":name",
            "Ada",
        )
        assert entity_id == 1
        assert conn.pull([":name"], entity_id) == {":name": "Ada"}


def test_kv_range_specs_accept_python_forms(tmp_path: Path) -> None:
    kv_dir = tmp_path / "kv"
    with open_kv(str(kv_dir)) as kv:
        kv.open_dbi("items")
        kv.transact(
            [(":put", 1, "a"), (":put", 2, "b"), (":put", 3, "c")],
            dbi_name="items",
            k_type=":long",
            v_type=":string",
        )

        assert kv.get_range("items", [":all"], k_type=":long", v_type=":string") == [
            [1, "a"],
            [2, "b"],
            [3, "c"],
        ]
        assert kv.get_range("items", [":closed", 2, 3], k_type=":long", v_type=":string") == [
            [2, "b"],
            [3, "c"],
        ]
        assert kv.with_transaction(
            lambda tx: tx.transact([(":put", 4, "d")], "items", ":long", ":string")
            and tx.get_value("items", 4, ":long", ":string", ignore_key=True)
        ) == "d"
        with kv.transaction() as tx:
            tx.transact([(":put", 5, "e")], "items", ":long", ":string")
            assert tx.commit() == ":committed"
        with kv.transaction() as tx:
            tx.transact([(":put", 6, "f")], "items", ":long", ":string")
            assert tx.abort() == ":aborted"
        assert kv.get_value("items", 5, ":long", ":string", ignore_key=True) == "e"
        assert kv.get_value("items", 6, ":long", ":string", ignore_key=True) is None
        assert re_index(kv) is kv
        assert kv.entries("items") == 5


def test_search_index_writer_commits_to_kv_index(tmp_path: Path) -> None:
    kv_dir = tmp_path / "search-kv"
    with open_kv(str(kv_dir)) as kv:
        writer = search_index_writer(kv, search_domain(include_text=True))
        assert writer.write("doc-1", "pizza and pasta") is writer
        writer.write("doc-2", "just pie")
        assert writer.commit() == ":transacted"
        assert writer.closed() is True

        assert kv.entries("datalevin/docs") == 2
        assert kv.entries("datalevin/rawtext") == 2
        assert kv.entries("datalevin/terms") > 0

        writer = kv.search_index_writer(search_domain(domain="notes", include_text=True))
        writer.write("note-1", "searchable local note")
        writer.commit()
        assert kv.entries("notes/docs") == 1

        engine = new_search_engine(kv, search_domain(include_text=True))
        assert repr(engine) == "<SearchEngine open>"
        assert engine.add_doc("doc-3", "pizza search engine") is engine
        assert engine.add_doc("doc-4", "engine indexing", check_exist=False) is engine
        assert engine.doc_count() == 4
        assert engine.doc_indexed("doc-3") is True
        assert engine.search("pizza") == ["doc-1", "doc-3"]
        assert engine.re_index(search_domain(include_text=True)) is engine
        assert engine.search("pizza") == ["doc-1", "doc-3"]
        assert re_index(engine, search_domain(include_text=True)) is engine
        assert engine.remove_doc("doc-3") is engine
        assert re_index(engine) is engine
        assert engine.doc_indexed("doc-3") is False
        assert engine.clear_docs() is engine
        assert engine.doc_count() == 0

        analyzer = create_analyzer(
            tokenizer=create_regexp_tokenizer(r"\s+"),
            token_filters=[
                lower_case_token_filter(),
                unaccent_token_filter(),
                create_stop_words_token_filter(["pizza"]),
            ],
        )
        custom = new_search_engine(
            kv,
            search_domain(
                domain="custom",
                include_text=True,
                analyzer=analyzer,
                query_analyzer=analyzer,
            ),
        )
        custom.add_doc("accent", "Café pizza")
        custom.add_doc("plain", "Cafe pasta")
        assert set(custom.search("cafe")) == {"accent", "plain"}
        assert custom.search("pizza") is None
        custom.close()


def test_standalone_vector_index(tmp_path: Path) -> None:
    kv_dir = tmp_path / "vector-kv"
    opts = {":dimensions": 2}
    with open_kv(str(kv_dir)) as kv:
        index = new_vector_index(kv, opts)
        assert repr(index) == "<VectorIndex open>"
        assert index.info()[":dimensions"] == 2

        assert index.add_vec("vec-1", [1.0, 0.0]) is index
        assert index.add_vec("vec-2", [0.0, 1.0]) is index
        assert index.vec_indexed("vec-1") is True
        assert index.search_vec([1.0, 0.0], opts={":top": 1}) == ["vec-1"]
        assert index.search_vec([1.0, 0.0], opts={":top": 1, ":display": ":refs+dists"}) == [["vec-1", 0.0]]

        assert index.force_checkpoint() is index
        assert isinstance(index.checkpoint_state(), dict)
        assert re_index(index) is index
        assert index.search_vec([0.0, 1.0], opts={":top": 1}) == ["vec-2"]
        assert index.remove_vec("vec-1") is index
        assert index.vec_indexed("vec-1") is False

        assert index.clear() is index
        assert index.closed() is True

        index = kv.new_vector_index(opts)
        try:
            assert index.info()[":size"] == 0
        finally:
            index.close()
