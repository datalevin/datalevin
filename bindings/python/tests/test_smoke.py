from __future__ import annotations

from pathlib import Path

import pytest

from datalevin import api_info, connect, open_kv
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
