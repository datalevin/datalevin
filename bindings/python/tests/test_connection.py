from __future__ import annotations

import pytest

from datalevin import connect
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
            }
        },
    ) as conn:
        assert repr(conn) == "<Connection open>"
        assert conn.closed() is False

        conn.transact([{":db/id": -1, ":name": "Ada"}, {":db/id": -2, ":name": "Bob"}])

        assert ":name" in conn.schema()
        assert isinstance(conn.opts(), dict)
        assert conn.entid([":name", "Ada"]) == 1
        assert conn.pull([":name"], 1) == {":name": "Ada"}
        assert conn.pull_many([":name"], [1, [":name", "Bob"]]) == [
            {":name": "Ada"},
            {":name": "Bob"},
        ]
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
