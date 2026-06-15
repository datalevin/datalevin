from __future__ import annotations

import pytest

from datalevin import connect, datalog_kv, datom, fill_db, init_db
from datalevin.errors import DatalevinJavaError


pytestmark = pytest.mark.usefixtures("require_runtime")


def test_connection_methods_cover_common_local_flow(tmp_path) -> None:
    db_dir = tmp_path / "db"
    with connect(
        str(db_dir),
        schema={
            ":name": {
                ":db/valueType": ":db.type/string",
                ":db/unique": ":db.unique/identity",
            },
            ":bio": {
                ":db/valueType": ":db.type/string",
                ":db/fulltext": True,
                ":db.fulltext/autoDomain": True,
            }
        },
    ) as conn:
        assert repr(conn) == "<Connection open>"
        assert conn.closed() is False

        conn.transact(
            [
                {":db/id": -1, ":name": "Ada", ":bio": "Ada builds database systems"},
                {":db/id": -2, ":name": "Bob", ":bio": "Bob writes migration tools"},
            ]
        )

        assert ":name" in conn.schema()
        assert isinstance(conn.opts(), dict)
        assert conn.entid([":name", "Ada"]) == 1
        assert conn.pull([":name"], 1) == {":name": "Ada"}
        assert conn.pull_many([":name"], [1, [":name", "Bob"]]) == [
            {":name": "Ada"},
            {":name": "Bob"},
        ]
        assert conn.datoms(":eav", 1, ":name", limit=1)[0][":v"] == "Ada"
        assert conn.seek_datoms(":eav", 1, ":name", limit=1)[0][":v"] == "Ada"
        assert conn.rseek_datoms(":ave", ":name", "Bob", limit=1)[0][":v"] == "Bob"
        assert conn.search_datoms(attr=":name", value="Ada")[0][":e"] == 1
        assert conn.count_datoms(None, ":name", "Ada") == 1
        assert [row[":v"] for row in conn.index_range(":name", "A", "C")] == ["Ada", "Bob"]
        assert conn.fulltext_datoms("database", opts={":top": 5})[0][2] == "Ada builds database systems"
        kv = conn.datalog_kv()
        kv.open_dbi("app-state")
        kv.transact([(":put", "k", "v")], "app-state", ":string", ":string")
        assert kv.get_value("app-state", "k", ":string", ":string", True) == "v"
        assert datalog_kv(conn).dir() == str(db_dir)
        assert conn.query(
            [":find", "?e", ".", ":in", "$", "?attr", "?value", ":where", ["?e", "?attr", "?value"]],
            ":name",
            "Ada",
        ) == 1

        explain = conn.explain("[:find ?e :where [?e :name _]]")
        assert ":plan" in explain

        conn.update_schema({":age": {":db/valueType": ":db.type/long"}})
        assert ":age" in conn.schema()

        conn.update_schema(None, del_attrs=[":age"])
        assert ":age" not in conn.schema()

    assert conn.closed() is True
    assert repr(conn) == "<Connection closed>"


def test_clear_closes_underlying_connection(tmp_path) -> None:
    db_dir = tmp_path / "db"
    with connect(
        str(db_dir),
        schema={
            ":name": {
                ":db/valueType": ":db.type/string",
                ":db/unique": ":db.unique/identity",
            }
        },
    ) as conn:
        conn.transact([{":db/id": -1, ":name": "Ada"}])
        conn.clear()

        assert conn.closed() is True
        with pytest.raises(DatalevinJavaError):
            conn.query("[:find [?name ...] :where [?e :name ?name]]")


def test_bulk_load_init_db_and_fill_db(tmp_path) -> None:
    db_dir = tmp_path / "bulk"
    schema = {
        ":name": {
            ":db/valueType": ":db.type/string",
            ":db/unique": ":db.unique/identity",
        }
    }

    with init_db([(1, ":name", "Ada")], dir=str(db_dir), schema=schema) as conn:
        assert conn.query("[:find [?name ...] :where [?e :name ?name]]") == ["Ada"]

        assert fill_db(conn, [(2, ":name", "Bob")]) is conn
        assert conn.fill_db([datom(3, ":name", "Cara")]) is conn

        assert sorted(conn.query("[:find [?name ...] :where [?e :name ?name]]")) == [
            "Ada",
            "Bob",
            "Cara",
        ]
