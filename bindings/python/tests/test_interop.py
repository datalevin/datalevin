from __future__ import annotations

import re

import pytest

from datalevin import (
    api_info,
    connect,
    create_udf_registry,
    exec_json,
    interop,
    schema_attr,
    search_domain,
    udf_descriptor,
)
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
        raw.core_invoke("open-dbi", [kv, "blob-keys"])
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
        raw.core_invoke(
            "transact-kv",
            [
                kv,
                "blob-keys",
                raw.kv_txs([(":put", b"\x01", b"\x09"), (":put", b"\x02", b"\x08")]),
                raw.kv_type(":bytes"),
                raw.kv_type(":bytes"),
            ],
        )
        assert sorted(to_python(raw.core_invoke("list-dbis", [kv]))) == ["blob-keys", "items"]
        assert to_python(raw.core_invoke("entries", [kv, "items"])) == 2
        assert to_python(
            raw.core_invoke(
                "get-range",
                [kv, "items", raw.read_edn("[:all]"), raw.kv_type(":string"), raw.kv_type(":string")],
            )
        ) == [["a", "alpha"], ["b", "beta"]]
        assert to_python(
            raw.core_invoke(
                "get-value",
                [kv, "blob-keys", b"\x02", raw.kv_type(":bytes"), raw.kv_type(":bytes"), True],
            )
        ) == b"\x08"
        assert to_python(
            raw.core_invoke(
                "get-range",
                [
                    kv,
                    "blob-keys",
                    [raw.keyword(":closed"), b"\x01", b"\x02"],
                    raw.kv_type(":bytes"),
                    raw.kv_type(":bytes"),
                ],
            )
        ) == [[b"\x01", b"\x09"], [b"\x02", b"\x08"]]
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

    query_descriptor = udf_descriptor(":math/inc")
    tx_descriptor = udf_descriptor(":person/bootstrap", kind=":tx-fn")

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


def test_udf_registry_supports_fulltext_analyzers(tmp_path) -> None:
    registry = create_udf_registry()
    analyzer_descriptor = udf_descriptor(":text/hashtags", kind=":analyzer")
    query_descriptor = udf_descriptor(":text/plain-query", kind=":query-analyzer")

    @registry.analyzer_udf(":text/hashtags")
    def hashtag_analyzer(text):
        return [
            [match.group(0)[1:], pos, match.start()]
            for pos, match in enumerate(re.finditer(r"#\w+", text))
        ]

    @registry.query_analyzer_udf(":text/plain-query")
    def plain_query_analyzer(text):
        return [[token, pos, pos] for pos, token in enumerate(text.split())]

    with connect(
        str(tmp_path / "fulltext-udf"),
        schema={
            ":text": schema_attr(
                value_type=":db.type/string",
                fulltext=True,
                extra={":db.fulltext/autoDomain": True},
            )
        },
        opts={
            ":runtime-opts": {":udf-registry": registry},
            ":search-domains": {
                "text": search_domain(
                    index_position=True,
                    extra={
                        ":analyzer": analyzer_descriptor,
                        ":query-analyzer": query_descriptor,
                    },
                )
            },
        },
    ) as conn:
        conn.transact(
            [
                {":db/id": 1, ":text": "alpha #needle"},
                {":db/id": 2, ":text": "needle without hash"},
            ]
        )

        assert conn.query(
            "[:find [?e ...] :in $ ?q "
            ":where [(fulltext $ :text ?q) [[?e ?a ?v]]]]",
            "needle",
        ) == [1]
        assert registry.registered(analyzer_descriptor) is True
        assert registry.registered(query_descriptor) is True
