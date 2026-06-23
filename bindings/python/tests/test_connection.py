from __future__ import annotations

from collections.abc import Mapping

import pytest

from datalevin import (
    Entity,
    connect,
    datalog_kv,
    datom,
    fill_db,
    init_db,
    keyword,
    max_eid,
    schema_attr,
    transact_async,
    tx_entity,
)
from datalevin.errors import DatalevinJavaError


pytestmark = pytest.mark.usefixtures("require_runtime")


def test_connection_methods_cover_common_local_flow(tmp_path) -> None:
    db_dir = tmp_path / "db"
    other_dir = tmp_path / "other-db"
    with connect(
        str(db_dir),
        schema={
            ":name": schema_attr(
                value_type=":db.type/string",
                unique=":db.unique/identity",
            ),
            ":bio": schema_attr(
                value_type=":db.type/string",
                fulltext=True,
                extra={":db.fulltext/autoDomain": True},
            ),
            ":status": schema_attr(value_type=":db.type/keyword"),
            ":friend": schema_attr(value_type=":db.type/ref"),
        },
    ) as conn, connect(
        str(other_dir),
        schema={":name": schema_attr(value_type=":db.type/string")},
    ) as other_conn:
        assert repr(conn) == "<Connection open>"
        assert conn.closed() is False

        reports = []
        listener_key = conn.listen(reports.append, key="test-listener")
        assert listener_key == "test-listener"
        conn.transact(
            [
                tx_entity(
                    -1,
                    {
                        ":name": "Ada",
                        ":bio": "Ada builds database systems",
                        ":status": keyword(":active"),
                        ":friend": -2,
                    },
                ),
                tx_entity(
                    -2,
                    {
                        ":name": "Bob",
                        ":bio": "Bob writes migration tools",
                        ":status": keyword(":draft"),
                    },
                ),
            ]
        )
        other_conn.transact([tx_entity(-1, {":name": "Cara"})])
        conn.unlisten(listener_key)
        async_report = conn.transact_async(
            [{":db/id": -3, ":bio": "Async transactions help ingestion"}]
        ).result(timeout=10)
        top_async_report = transact_async(
            conn,
            [{":db/id": -4, ":bio": "Top-level async helper"}],
        ).result(timeout=10)

        assert len(reports) == 1
        assert reports[0][":tx-data"]
        assert reports[0][":tx-meta"] is None
        assert ":name" in conn.schema()
        assert isinstance(conn.opts(), dict)
        assert conn.max_eid() == 4
        assert max_eid(conn) == 4
        assert conn.datalog_index_cache_limit() == 512
        assert conn.datalog_index_cache_limit(16) == 16
        assert conn.datalog_index_cache_limit() == 16
        assert conn.entid([":name", "Ada"]) == 1
        entity = conn.entity(1)
        assert isinstance(entity, Mapping)
        assert entity.id == 1
        assert entity[":db/id"] == 1
        assert entity.get(":name") == "Ada"
        assert entity[":name"] == "Ada"
        assert ":name" in entity
        assert {":name", ":bio", ":status", ":friend"}.issubset(entity.keys())
        assert "Ada" in entity.values()
        entity_items = dict(entity.items())
        assert entity_items[":name"] == "Ada"
        assert isinstance(entity_items[":friend"], Entity)
        assert dict(entity)[":name"] == "Ada"
        assert entity.get(":missing", "fallback") == "fallback"
        friend = entity.get(":friend")
        assert isinstance(friend, Entity)
        assert friend.get(":name") == "Bob"
        assert entity.touch()[":name"] == "Ada"
        assert conn.entity_map([":name", "Ada"])[":name"] == "Ada"
        assert conn.pull([":name"], 1) == {":name": "Ada"}
        assert conn.pull([":status"], 1) == {":status": ":active"}
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
        assert ":tx-data" in async_report
        assert ":tx-data" in top_async_report
        assert "Async transactions help ingestion" in [
            row[2] for row in conn.fulltext_datoms("async", opts={":top": 5})
        ]
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
        assert conn.query(
            "[:find [?name ...] :in $ $other :where "
            "[$ ?e :name \"Ada\"] [$other ?x :name ?name]]",
            other_conn,
        ) == ["Cara"]
        simulated = conn.tx_data_to_simulated_report([
            tx_entity(-100, {":name": "Sim", ":bio": "Simulated only"})
        ])
        assert simulated[":tx-data"]
        assert conn.query('[:find ?e . :where [?e :name "Sim"]]') is None

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
