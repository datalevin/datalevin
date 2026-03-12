from __future__ import annotations

import pytest

from datalevin import api_info, connect, create_udf_registry, exec_json, interop
from datalevin._convert import to_python


pytestmark = pytest.mark.usefixtures("require_runtime")


def test_api_info_matches_json_api() -> None:
    info = api_info()
    assert info["datalevin-version"] == exec_json("api-info")["datalevin-version"]


def test_raw_interop_exposes_normalizers_and_kv_calls(tmp_path) -> None:
    raw = interop()
    assert interop() is raw

    assert str(raw.keyword(":name")) == ":name"
    assert str(raw.symbol("?e")) == "?e"
    assert str(raw.database_type("kv")) == ":key-value"
    assert str(raw.permission_target(":datalevin.server/role", ":admins")) == ":admins"
    assert to_python(
        raw.udf_descriptor(
            {":udf/lang": ":java", ":udf/kind": ":query-fn", ":udf/id": ":math/inc"}
        )
    ) == {
        ":udf/lang": ":java",
        ":udf/kind": ":query-fn",
        ":udf/id": ":math/inc",
    }

    kv = raw.open_key_value(str(tmp_path / "kv"))
    try:
        raw.core_invoke("open-dbi", [kv, "items"])
        raw.core_invoke(
            "transact-kv",
            [
                kv,
                "items",
                raw.kv_txs([(":put", "a", "alpha"), (":put", "b", "beta")]),
                raw.kv_type(":string"),
                raw.kv_type(":string"),
            ],
        )
        assert to_python(raw.core_invoke("list-dbis", [kv])) == ["items"]
        assert to_python(raw.core_invoke("entries", [kv, "items"])) == 2
        assert to_python(
            raw.core_invoke(
                "get-range",
                [kv, "items", raw.read_edn("[:all]"), raw.kv_type(":string"), raw.kv_type(":string")],
            )
        ) == [["a", "alpha"], ["b", "beta"]]
    finally:
        raw.close_key_value(kv)

    assert raw.key_value_closed(kv) is True


def test_udf_registry_supports_inline_query_and_tx_functions(tmp_path) -> None:
    registry = create_udf_registry()

    @registry.query_udf(":math/inc")
    def inc(value):
        return value + 1

    @registry.tx_udf(":person/bootstrap")
    def bootstrap(db, name):
        return [{":db/id": -1, ":name": name, ":score": 10}]

    query_descriptor = {":udf/lang": ":java", ":udf/kind": ":query-fn", ":udf/id": ":math/inc"}
    tx_descriptor = {":udf/lang": ":java", ":udf/kind": ":tx-fn", ":udf/id": ":person/bootstrap"}

    with connect(
        str(tmp_path / "db"),
        schema={
            ":name": {
                ":db/valueType": ":db.type/string",
                ":db/unique": ":db.unique/identity",
            },
            ":score": {":db/valueType": ":db.type/long"},
        },
        opts={":runtime-opts": {":udf-registry": registry}},
    ) as conn:
        conn.transact([[":db.fn/call", tx_descriptor, "Ada"]])

        assert conn.query(
            "[:find ?v . :in $ ?desc ?n :where [(udf ?desc ?n) ?v]]",
            query_descriptor,
            9,
        ) == 10
        assert conn.query("[:find [?name ...] :where [?e :name ?name]]") == ["Ada"]
        assert registry.registered(query_descriptor) is True
        assert registry.registered(tx_descriptor) is True

        registry.unregister(query_descriptor)
        registry.unregister(tx_descriptor)

        assert registry.registered(query_descriptor) is False
        assert registry.registered(tx_descriptor) is False
