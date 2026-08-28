from __future__ import annotations

import pytest

from datalevin import connect, idoc_attr, q, schema_attr, tx


pytestmark = pytest.mark.usefixtures("require_runtime")


def test_composed_queries_and_transactions_use_the_current_jvm_bridge(tmp_path) -> None:
    schema = {
        ":person/name": schema_attr(
            value_type=":db.type/string",
            fulltext=True,
            fulltext_auto_domain=True,
        ),
        ":person/age": schema_attr(value_type=":db.type/long"),
        ":person/label": schema_attr(value_type=":db.type/string"),
        ":person/nickname": schema_attr(value_type=":db.type/string"),
        ":person/status": schema_attr(value_type=":db.type/keyword"),
        ":person/friend": schema_attr(value_type=":db.type/ref"),
        ":person/embedding": schema_attr(value_type=":db.type/vec"),
        ":user/handle": schema_attr(
            value_type=":db.type/string", unique=":db.unique/identity"
        ),
        ":user/name": schema_attr(value_type=":db.type/string"),
        ":user/friend": schema_attr(value_type=":db.type/ref"),
        ":user/profile": idoc_attr(format="edn", domain="profiles"),
    }
    with connect(
        str(tmp_path / "composition"),
        schema=schema,
        opts={
            ":vector-opts": {
                ":dimensions": 2,
                ":metric-type": ":cosine",
            }
        },
    ) as conn:
        conn.transact(
            tx.data(
                tx.entity(
                    -1,
                    {
                        "person/name": "Ada",
                        "person/age": 42,
                        "person/label": "?literal",
                        "person/nickname": "Ace",
                        "person/status": q.kw("active"),
                        "person/friend": -2,
                        "person/embedding": [1.0, 0.0],
                    },
                ),
                tx.entity(
                    -2,
                    {
                        "person/name": "Bob",
                        "person/age": 21,
                        "person/label": ":literal",
                        "person/status": q.kw("draft"),
                        "person/embedding": [0.0, 1.0],
                    },
                ),
            )
        )

        entity = q.var("entity")
        name = q.var("name")
        age = q.var("age")
        minimum = q.var("minimum")
        adults = q.query(
            find=q.collection(name),
            inputs=[q.DB, minimum],
            where=[
                q.datom(entity, "person/name", name),
                q.datom(entity, "person/age", age),
                q.predicate(">=", age, minimum),
            ],
        )
        assert conn.query(adults, 30) == ["Ada"]

        search_term = q.var("search_term")
        fulltext_names = q.query(
            find=q.collection(name),
            inputs=[q.DB, search_term],
            where=[
                q.fulltext(
                    search_term,
                    [entity, q.IGNORE, q.IGNORE],
                    attribute="person/name",
                    options=q.fulltext_options(top=1),
                ),
                q.datom(entity, "person/name", name),
            ],
        )
        assert conn.query(fulltext_names, "Ada") == ["Ada"]

        query_vector = q.var("query_vector")
        distance = q.var("distance")
        vector_names = q.query(
            find=q.collection(name),
            inputs=[q.DB, query_vector],
            where=[
                q.vec_neighbors(
                    query_vector,
                    [entity, q.IGNORE, q.IGNORE, distance],
                    attribute="person/embedding",
                    options=q.vector_search_options(
                        top=1,
                        display="refs+dists",
                    ),
                ),
                q.datom(entity, "person/name", name),
            ],
        )
        assert conn.query(vector_names, [1.0, 0.0]) == ["Ada"]

        inner_age = q.var("inner_age")
        youngest = q.var("youngest")
        youngest_entity = q.var("youngest_entity")
        youngest_age_query = q.query(
            find=q.relation(q.aggregate("min", inner_age)),
            where=[q.datom(q.IGNORE, "person/age", inner_age)],
        )
        nested_query = q.query(
            find=q.relation(youngest_entity, youngest),
            where=[
                q.bind(
                    "q",
                    q.relation_binding(youngest),
                    q.quote(youngest_age_query),
                    q.DB,
                ),
                q.datom(youngest_entity, "person/age", youngest),
            ],
        )
        assert conn.query(nested_query) == [[2, 21]]

        ordered = q.query(
            find=q.relation(name, age),
            where=[
                q.datom(entity, "person/name", name),
                q.datom(entity, "person/age", age),
            ],
            order_by=[q.desc(age)],
        )
        assert conn.query(ordered) == [["Ada", 42], ["Bob", 21]]

        unordered = q.query(
            find=q.relation(name, age),
            where=[
                q.datom(entity, "person/name", name),
                q.datom(entity, "person/age", age),
            ],
        )
        unordered_rows = conn.query(unordered)
        assert isinstance(unordered_rows, list)
        assert {tuple(row) for row in unordered_rows} == {
            ("Ada", 42),
            ("Bob", 21),
        }

        leading_question = q.query(
            find=q.collection(entity),
            where=[q.datom(entity, "person/label", "?literal")],
        )
        leading_colon = q.query(
            find=q.collection(entity),
            where=[q.datom(entity, "person/label", ":literal")],
        )
        assert conn.query(leading_question) == [1]
        assert conn.query(leading_colon) == [2]

        label = q.var("label")
        by_label = q.query(
            find=q.collection(entity),
            inputs=[q.DB, label],
            where=[q.datom(entity, "person/label", label)],
        )
        assert conn.query(by_label, ":literal") == [2]
        assert ":plan" in conn.explain(by_label, ":literal")

        by_labels = q.query(
            find=q.collection(entity),
            inputs=[q.DB, q.collection_binding(label)],
            where=[q.datom(entity, "person/label", label)],
        )
        assert conn.query(by_labels, [":literal"]) == [2]

        active = q.query(
            find=q.collection(name),
            where=[
                q.datom(entity, "person/name", name),
                q.datom(entity, "person/status", q.kw("active")),
            ],
        )
        assert conn.query(active) == ["Ada"]

        status = q.var("status")
        by_status = q.query(
            find=q.collection(name),
            inputs=[q.DB, status],
            where=[
                q.datom(entity, "person/name", name),
                q.datom(entity, "person/status", status),
            ],
        )
        assert conn.query(by_status, q.kw("active")) == ["Ada"]

        fallback_selector = q.selector(
            q.pull_attr(
                "person/nickname",
                default="none",
                as_="nickname",
            )
        )
        assert conn.pull(fallback_selector, 2) == {"nickname": "none"}

        structured_fallback = q.selector(
            q.pull_attr(
                "person/nickname",
                default={"text": ":none"},
                as_="fallback",
            )
        )
        assert conn.pull(structured_fallback, 2) == {
            "fallback": {"text": ":none"}
        }

        tuple_alias = q.selector(
            q.pull_attr(
                "person/nickname",
                default="none",
                as_=("label", ":literal"),
            )
        )
        assert conn.pull(tuple_alias, 2) == {
            ("label", ":literal"): "none"
        }

        legacy_fallback = q.selector(
            [["person/nickname", ":default", "none", ":as", "nickname"]]
        )
        assert conn.pull(legacy_fallback, 2) == {"nickname": "none"}

        pulled_bob = q.query(
            find=q.scalar(q.pull(entity, fallback_selector)),
            where=[q.datom(entity, "person/name", "Bob")],
        )
        assert conn.query(pulled_bob) == {"nickname": "none"}

        age_text = q.selector(
            q.pull_attr("person/age", as_="age-text", xform="str")
        )
        assert conn.pull(age_text, 1) == {"age-text": "42"}

        friend_name = q.selector(
            q.pull_nested("person/friend", q.selector("person/name"))
        )
        assert conn.pull(friend_name, 1) == {
            ":person/friend": {":person/name": "Bob"}
        }

        recursive_friend = q.selector(
            "db/id",
            q.pull_recursive(
                q.pull_attr("person/friend", as_="friend"),
                depth=1,
            )
        )
        assert conn.pull(recursive_friend, 1) == {
            ":db/id": 1,
            "friend": {":db/id": 2}
        }

        adult_rule = q.rule_branch(
            "adult",
            [entity, name, minimum],
            q.datom(entity, "person/name", name),
            q.datom(entity, "person/age", age),
            q.predicate(">=", age, minimum),
        )
        by_rule = q.query(
            find=q.collection(name),
            inputs=[q.DB, q.RULES, minimum],
            where=[q.rule("adult", entity, name, minimum)],
        )
        assert conn.query(by_rule, q.rules(adult_rule), 30) == ["Ada"]

        eve = tx.lookup_ref("user/handle", "eve")
        conn.transact(
            tx.data(
                tx.entity(
                    -10,
                    {
                        "user/handle": "eve",
                        "user/name": "Eve",
                        "user/profile": {
                            "status": "active",
                            "profile": {"age": 30},
                            "tags": ["a"],
                            "obsolete": True,
                        },
                    },
                )
            )
        )
        conn.transact(
            tx.data(
                tx.entity(
                    -11,
                    {"user/handle": "alice", "user/friend": eve},
                ),
                tx.entity(
                    -12,
                    {
                        "user/handle": "parent",
                        "user/friend": tx.entity(
                            -13,
                            {"user/handle": "child", "user/name": "Child"},
                        ),
                    },
                ),
                tx.add(eve, "user/name", "Evelyn"),
            )
        )
        conn.transact(
            tx.data(
                tx.patch_idoc(
                    eve,
                    "user/profile",
                    [
                        tx.patch_set(["status"], ":literal"),
                        tx.patch_update(["profile"], "assoc", "role", "admin"),
                        tx.patch_update(["profile", "age"], "inc"),
                        tx.patch_update(["tags"], "conj", ":literal"),
                        tx.patch_unset(["obsolete"]),
                    ],
                )
            )
        )

        user = q.var("user")
        handle = q.var("handle")
        friend = q.var("friend")
        friend_handle = q.var("friend_handle")
        owner = q.var("owner")
        friend_by_owner = q.query(
            find=q.scalar(friend_handle),
            inputs=[q.DB, owner],
            where=[
                q.datom(user, "user/handle", owner),
                q.datom(user, "user/friend", friend),
                q.datom(friend, "user/handle", friend_handle),
            ],
        )
        assert conn.query(friend_by_owner, "alice") == "eve"
        assert conn.query(friend_by_owner, "parent") == "child"

        user_name = q.var("user_name")
        name_by_handle = q.query(
            find=q.scalar(user_name),
            inputs=[q.DB, handle],
            where=[
                q.datom(user, "user/handle", handle),
                q.datom(user, "user/name", user_name),
            ],
        )
        assert conn.query(name_by_handle, "eve") == "Evelyn"

        profile = q.var("profile")
        profile_by_handle = q.query(
            find=q.scalar(profile),
            inputs=[q.DB, handle],
            where=[
                q.datom(user, "user/handle", handle),
                q.datom(user, "user/profile", profile),
            ],
        )
        assert conn.query(profile_by_handle, "eve") == {
            "status": ":literal",
            "profile": {"age": 31, "role": "admin"},
            "tags": ["a", ":literal"],
        }

        idoc_entity = q.var("idoc_entity")
        idoc_attribute = q.var("idoc_attribute")
        idoc_value = q.var("idoc_value")
        idoc_predicate = q.var("idoc_predicate")
        matching_handles = q.query(
            find=q.collection(handle),
            inputs=[q.DB, idoc_predicate],
            where=[
                q.idoc_match(
                    idoc_predicate,
                    [idoc_entity, idoc_attribute, idoc_value],
                    options=q.idoc_match_options(domains=["profiles"]),
                ),
                q.datom(idoc_entity, "user/handle", handle),
            ],
        )
        assert conn.query(
            matching_handles,
            {"profile": {"age": q.edn_list(q.sym(">="), 31)}},
        ) == ["eve"]
