from __future__ import annotations

import pytest

from datalevin import connect, idoc_attr, q, schema_attr, tx


pytestmark = pytest.mark.usefixtures("require_runtime")


def test_composed_queries_and_transactions_use_the_current_jvm_bridge(tmp_path) -> None:
    schema = {
        ":person/name": schema_attr(value_type=":db.type/string"),
        ":person/age": schema_attr(value_type=":db.type/long"),
        ":person/label": schema_attr(value_type=":db.type/string"),
        ":person/nickname": schema_attr(value_type=":db.type/string"),
        ":person/status": schema_attr(value_type=":db.type/keyword"),
        ":person/friend": schema_attr(value_type=":db.type/ref"),
        ":user/handle": schema_attr(
            value_type=":db.type/string", unique=":db.unique/identity"
        ),
        ":user/name": schema_attr(value_type=":db.type/string"),
        ":user/friend": schema_attr(value_type=":db.type/ref"),
        ":user/profile": idoc_attr(format="edn"),
    }
    with connect(str(tmp_path / "composition"), schema=schema) as conn:
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
                    },
                ),
                tx.entity(
                    -2,
                    {
                        "person/name": "Bob",
                        "person/age": 21,
                        "person/label": ":literal",
                        "person/status": q.kw("draft"),
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

        ordered = q.query(
            find=q.relation(name, age),
            where=[
                q.datom(entity, "person/name", name),
                q.datom(entity, "person/age", age),
            ],
            order_by=[q.desc(age)],
        )
        assert conn.query(ordered) == [["Ada", 42], ["Bob", 21]]

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
