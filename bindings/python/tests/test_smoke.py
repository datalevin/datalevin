from __future__ import annotations

from pathlib import Path

import pytest

from datalevin import api_info, connect, new_search_engine, open_kv, re_index, search_domain, search_index_writer
pytestmark = pytest.mark.usefixtures("require_runtime")


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
        assert conn.re_index() is conn
        names = conn.query("[:find [?name ...] :where [?e :name ?name]]")
        assert names == ["Ada"]


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
        assert re_index(kv) is kv
        assert kv.entries("items") == 3


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
