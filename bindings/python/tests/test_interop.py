from __future__ import annotations

import re

import pytest

from datalevin import (
    Database,
    UdfDescriptor,
    api_info,
    connect,
    create_udf_registry,
    exec_json,
    interop,
    keyword,
    q,
    schema_attr,
    search_domain,
    tx,
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


def test_udf_registry_supports_inline_query_and_tx_functions() -> None:
    registry = create_udf_registry()
    query_descriptor = UdfDescriptor.query_fn(":math/inc")
    predicate_descriptor = UdfDescriptor.predicate(":score/high?")
    attr_predicate_descriptor = UdfDescriptor.predicate(":score/guarded?")
    ensure_descriptor = UdfDescriptor.predicate(":score/ensure-guarded?")
    tx_descriptor = UdfDescriptor.tx_fn(":person/bootstrap")
    bare_id_descriptor = UdfDescriptor.query_fn(":math/double")
    convenience_descriptor = UdfDescriptor.query_fn(":math/triple")

    @registry.register(query_descriptor)
    def inc(value):
        return value + 1

    @registry.register(":math/double")
    def double(value):
        return value * 2

    @registry.query_udf(":math/triple")
    def triple(value):
        return value * 3

    @registry.register(predicate_descriptor)
    def high_score(score):
        return score >= 10

    def guarded_score(score):
        return score >= 10

    def ensure_guarded_score(db, eid):
        return db.entity_map(eid)[":guarded-score"] == 11

    registry.register(attr_predicate_descriptor, guarded_score)
    registry.register(ensure_descriptor, ensure_guarded_score)

    @registry.register(tx_descriptor)
    def bootstrap(db, name):
        assert isinstance(db, Database)
        return tx.data(tx.entity(-1, {"name": name, "score": 10}))

    with connect(
        None,
        schema={
            ":name": {
                ":db/valueType": ":db.type/string",
                ":db/unique": ":db.unique/identity",
            },
            ":score": {":db/valueType": ":db.type/long"},
            ":guarded-score": schema_attr(
                value_type=":db.type/long",
                attr_preds=attr_predicate_descriptor,
            ),
        },
        opts={
            ":kv-opts": {":inmemory?": True},
            ":runtime-opts": {":udf-registry": registry},
        },
    ) as conn:
        conn.transact(tx.data(tx.call_udf(tx_descriptor, "Ada")))
        conn.transact(tx.data(tx.install_udf(tx_descriptor)))
        conn.transact(tx.data(tx.invoke("person/bootstrap", "Ada")))
        conn.transact([{":db/id": -1, ":name": "Bob", ":score": 3}])
        conn.transact([{":db/id": -2, ":guarded-score": 11}])
        with pytest.raises(Exception, match="failed pred"):
            conn.transact([{":db/id": -3, ":guarded-score": 3}])
        conn.transact(
            tx.data(
                tx.entity(-4, {"guarded-score": 11}),
                tx.ensure(ensure_descriptor, -4),
            )
        )
        with pytest.raises(Exception, match=":db/ensure failed"):
            conn.transact(
                tx.data(
                    tx.entity(-5, {"guarded-score": 12}),
                    tx.ensure(ensure_descriptor, -5),
                )
            )

        descriptor = q.var("descriptor")
        number = q.var("number")
        value = q.var("value")
        typed_query = q.query(
            find=q.scalar(value),
            inputs=[q.DB, descriptor, number],
            where=[q.bind_udf(descriptor, value, number)],
        )
        assert conn.query(
            typed_query,
            query_descriptor,
            9,
        ) == 10
        assert conn.query(typed_query, bare_id_descriptor, 9) == 18
        assert conn.query(typed_query, convenience_descriptor, 9) == 27
        inline_query = q.query(
            find=q.scalar(value),
            inputs=[q.DB, number],
            where=[q.bind_udf(query_descriptor, value, number)],
        )
        assert conn.query(inline_query, 19) == 20
        entity = q.var("entity")
        name = q.var("name")
        score = q.var("score")
        pred = q.var("pred")
        typed_predicate_query = q.query(
            find=q.collection(name),
            inputs=[q.DB, pred],
            where=[
                q.datom(entity, "name", name),
                q.datom(entity, "score", score),
                q.udf_predicate(pred, score),
            ],
        )
        assert conn.query(
            typed_predicate_query,
            predicate_descriptor,
        ) == ["Ada"]
        assert conn.query(
            "[:find ?v . :in $ ?n :where [(udf :math/inc ?n) ?v]]",
            41,
        ) == 42
        assert sorted(conn.query("[:find [?name ...] :where [?e :name ?name]]")) == ["Ada", "Bob"]
        assert registry.registered(query_descriptor) is True
        assert registry.registered(predicate_descriptor) is True
        assert registry.registered(attr_predicate_descriptor) is True
        assert registry.registered(ensure_descriptor) is True
        assert registry.registered(tx_descriptor) is True
        assert registry.registered(bare_id_descriptor) is True
        assert registry.registered(convenience_descriptor) is True

        registry.unregister(query_descriptor)
        registry.unregister(predicate_descriptor)
        registry.unregister(attr_predicate_descriptor)
        registry.unregister(ensure_descriptor)
        registry.unregister(tx_descriptor)
        registry.unregister(":math/double")
        registry.unregister(":math/triple")

        assert registry.registered(query_descriptor) is False
        assert registry.registered(predicate_descriptor) is False
        assert registry.registered(attr_predicate_descriptor) is False
        assert registry.registered(ensure_descriptor) is False
        assert registry.registered(tx_descriptor) is False
        assert registry.registered(bare_id_descriptor) is False
        assert registry.registered(convenience_descriptor) is False


def test_udf_registry_supports_fulltext_analyzers() -> None:
    registry = create_udf_registry()
    analyzer_descriptor = UdfDescriptor.analyzer(":text/hashtags")
    query_descriptor = UdfDescriptor.query_analyzer(":text/plain-query")

    @registry.register(analyzer_descriptor)
    def hashtag_analyzer(text):
        return [
            [match.group(0)[1:], pos, match.start()]
            for pos, match in enumerate(re.finditer(r"#\w+", text))
        ]

    @registry.register(query_descriptor)
    def plain_query_analyzer(text):
        return [[token, pos, pos] for pos, token in enumerate(text.split())]

    with connect(
        None,
        schema={
            ":text": schema_attr(
                value_type=":db.type/string",
                fulltext=True,
                fulltext_auto_domain=True,
            )
        },
        opts={
            ":kv-opts": {":inmemory?": True},
            ":runtime-opts": {":udf-registry": registry},
            ":search-domains": {
                "text": search_domain(
                    index_position=True,
                    analyzer=analyzer_descriptor,
                    query_analyzer=query_descriptor,
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
