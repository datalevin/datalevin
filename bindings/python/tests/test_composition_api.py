from __future__ import annotations

import pytest

from datalevin import (
    Keyword,
    LookupRef,
    PatchOp,
    PullAttr,
    PullNested,
    PullSelector,
    Query,
    Symbol,
    TxData,
    q,
    tx,
)


def test_query_builder_is_pure_typed_and_composable() -> None:
    entity = q.var("e")
    name = q.var("name")
    age = q.var("age")
    clauses = [
        q.datom(entity, "person/name", name),
        q.datom(entity, "person/age", age),
    ]
    minimum_age = 30
    if minimum_age is not None:
        clauses.append(q.predicate(">=", age, minimum_age))

    adults = q.query(
        find=q.relation(name),
        where=clauses,
        order_by=[q.asc(name)],
        limit=10,
    )

    assert isinstance(adults, Query)
    assert adults.as_data() == [
        ":find",
        "?name",
        ":where",
        ["?e", ":person/name", "?name"],
        ["?e", ":person/age", "?age"],
        [[">=", "?age", 30]],
        ":order-by",
        ["?name", ":asc"],
        ":limit",
        10,
    ]


def test_query_strings_are_literals_unless_explicitly_typed() -> None:
    entity = q.var("e")
    presence = q.pattern(entity, "user/id")
    assert presence.to_form() == (entity, q.kw("user/id"))

    clause = q.datom(entity, "label", "?literal")
    form = clause.to_form()

    assert isinstance(form[0], Symbol)
    assert isinstance(form[1], Keyword)
    assert form[2] == "?literal"

    keyword_literal = q.datom(entity, q.var("attribute"), ":literal")
    keyword_form = keyword_literal.to_form()
    assert isinstance(keyword_form[1], Symbol)
    assert keyword_form[2] == ":literal"


def test_pull_selectors_type_grammar_tokens_without_changing_values() -> None:
    attribute = q.pull_attr(
        "person/nickname",
        as_="display",
        limit=None,
        default={"text": ":none"},
        xform="str",
    )
    form = attribute.to_form()

    assert isinstance(attribute, PullAttr)
    assert isinstance(form[0], Keyword)
    assert form[2] == "display"
    assert form[4] is None
    assert form[6] == {"text": ":none"}
    assert isinstance(form[8], Symbol)
    assert str(form[8]) == "str"

    nested = q.pull_nested("person/friend", q.selector("person/name"))
    recursive = q.pull_recursive("person/manager")
    bounded = q.pull_recursive(q.pull_attr("person/reports", limit=2), depth=3)
    selector = q.selector(attribute, nested, recursive, bounded)

    assert isinstance(nested, PullNested)
    assert isinstance(selector, PullSelector)
    assert selector.as_data() == [
        [
            ":person/nickname",
            ":as",
            "display",
            ":limit",
            None,
            ":default",
            {"text": ":none"},
            ":xform",
            "str",
        ],
        {":person/friend": [":person/name"]},
        {":person/manager": "..."},
        {(":person/reports", ":limit", 2): 3},
    ]

    legacy_expression = q.selector(
        [["person/nickname", ":default", "none", ":as", "display"]]
    )
    assert legacy_expression.as_data() == [
        [":person/nickname", ":default", "none", ":as", "display"]
    ]


def test_query_supports_inputs_bindings_joins_rules_and_return_maps() -> None:
    entity = q.var("e")
    name = q.var("name")
    age = q.var("age")
    minimum = q.var("minimum")
    adult = q.rule_branch(
        "adult",
        [entity, name, minimum],
        q.datom(entity, "person/name", name),
        q.datom(entity, "person/age", age),
        q.predicate(">=", age, minimum),
    )
    query = q.query(
        find=q.relation(entity, name),
        keys=["id", "name"],
        inputs=[q.DB, q.RULES, minimum],
        where=[q.rule("adult", entity, name, minimum)],
    )

    assert q.rules(adult).as_data() == [
        [
            ["adult", "?e", "?name", "?minimum"],
            ["?e", ":person/name", "?name"],
            ["?e", ":person/age", "?age"],
            [[">=", "?age", "?minimum"]],
        ]
    ]
    assert query.as_data() == [
        ":find",
        "?e",
        "?name",
        ":keys",
        "id",
        "name",
        ":in",
        "$",
        "%",
        "?minimum",
        ":where",
        ["adult", "?e", "?name", "?minimum"],
    ]

    relation_binding = q.relation_binding(entity, name)
    assert relation_binding.to_form() == ((entity, name),)


def test_query_builder_rejects_invalid_structural_combinations() -> None:
    entity = q.var("entity")
    name = q.var("name")
    other = q.var("other")
    clause = q.datom(entity, "person/name", name)

    with pytest.raises(ValueError, match="scalar find"):
        q.query(find=q.scalar(entity), keys=["id"])
    with pytest.raises(ValueError, match="collection find"):
        q.query(find=q.collection(entity), keys=["id"])
    with pytest.raises(ValueError, match="field count"):
        q.query(find=q.relation(entity, name), keys=["id"])
    with pytest.raises(TypeError, match="field names"):
        q.query(find=q.relation(entity), keys=[q.kw("id")])

    with pytest.raises(ValueError, match="must not be empty"):
        q.join_vars()
    with pytest.raises(ValueError, match="distinct"):
        q.join_vars(entity, required=[entity])
    with pytest.raises(TypeError, match="q.var"):
        q.join_vars("entity")

    with pytest.raises(TypeError, match="order term"):
        q.asc("entity")
    with pytest.raises(TypeError, match="order term"):
        q.desc(-1)
    with pytest.raises(ValueError, match="occur in the find"):
        q.query(find=q.relation(entity), order_by=[q.asc(other)])
    with pytest.raises(ValueError, match="outside the find"):
        q.query(find=q.relation(entity), order_by=[q.asc(1)])
    with pytest.raises(ValueError, match="distinct"):
        q.query(
            find=q.relation(entity),
            order_by=[q.asc(entity), q.desc(entity)],
        )
    with pytest.raises(ValueError, match="direction"):
        q.Order(entity, q.kw("sideways"))

    branch_group = q.and_(clause, q.predicate("some?", name))
    with pytest.raises(ValueError, match="only valid as a branch"):
        q.query(find=q.relation(entity), where=[branch_group])
    with pytest.raises(ValueError, match="only valid as a branch"):
        q.not_(branch_group)

    valid = q.query(
        find=q.relation(entity, name),
        keys=["id", "name"],
        order_by=[q.asc(entity), q.desc(1)],
        where=[q.or_(branch_group, clause)],
    )
    assert isinstance(valid, Query)
    assert isinstance(
        q.query(find=q.tuple_find(entity, name), keys=["id", "name"]),
        Query,
    )
    assert isinstance(
        q.query(
            find=q.relation(entity),
            inputs=[q.RULES],
            where=[q.rule("and", entity)],
        ),
        Query,
    )


def test_rule_sets_validate_required_and_free_branch_arity() -> None:
    entity = q.var("entity")
    child = q.var("child")
    clause = q.datom(entity, "person/child", child)
    required_branch = q.rule_branch(
        "ancestor",
        q.join_vars(child, required=[entity]),
        clause,
    )
    free_branch = q.rule_branch("ancestor", [entity, child], clause)

    with pytest.raises(ValueError, match="matching required/free arity"):
        q.rules(required_branch, free_branch)
    with pytest.raises(ValueError, match="top level of a rule branch"):
        q.rule_branch("ancestor", [entity], q.and_(clause))

    assert q.rules(required_branch).as_data() == [
        [
            ["ancestor", ["?entity"], "?child"],
            ["?entity", ":person/child", "?child"],
        ]
    ]


def test_transaction_builder_produces_typed_immutable_forms() -> None:
    transaction = tx.data(
        tx.entity(-1, {"person/name": "?Ada", "person/status": q.kw("active")}),
        tx.add(-1, "person/age", 42),
        tx.compare_and_swap(-1, "person/age", 42, 43),
        tx.retract_attribute(-1, "person/nickname"),
        tx.ensure("person/valid?", -1),
    )

    assert isinstance(transaction, TxData)
    entity_form = transaction[0].to_form()
    assert all(isinstance(key, Keyword) for key in entity_form)
    assert entity_form[q.kw("person/name")] == "?Ada"
    assert isinstance(entity_form[q.kw("person/status")], Keyword)
    assert transaction.as_data() == [
        {":person/name": "?Ada", ":person/status": ":active", ":db/id": -1},
        [":db/add", -1, ":person/age", 42],
        [":db.fn/cas", -1, ":person/age", 42, 43],
        [":db.fn/retractAttribute", -1, ":person/nickname"],
        [":db/ensure", ":person/valid?", -1],
    ]


def test_builder_forms_take_recursive_immutable_snapshots() -> None:
    entity = q.var("entity")
    literal = {"labels": ["before"]}
    clause = q.datom(entity, "user/data", literal)
    attrs = {"user/data": {"tags": ["before"]}}
    item = tx.entity(-1, attrs)
    transaction = tx.data(item)
    source_bytes = bytearray([1, 2])
    raw_value = q.raw({"bytes": source_bytes})

    literal["labels"].append("after")
    attrs["user/data"]["tags"].append("after")
    attrs["user/data"] = {"tags": ["replacement"]}
    source_bytes[0] = 9

    clause_form = clause.to_form()
    entity_form = item.to_form()
    tx_form = transaction.to_form()
    stored_data = entity_form[q.kw("user/data")]

    assert clause_form[2] == {"labels": ("before",)}
    assert stored_data == {"tags": ("before",)}
    assert tx_form[0] is entity_form
    assert raw_value.to_form()["bytes"] == b"\x01\x02"

    with pytest.raises(TypeError):
        clause_form[2]["labels"] = ("changed",)
    with pytest.raises(TypeError):
        entity_form[q.kw("user/data")] = {"tags": ()}
    with pytest.raises(AttributeError):
        stored_data["tags"].append("changed")


def test_python_tokens_and_forms_have_value_equality() -> None:
    first = q.kw("user/id")
    second = q.kw(":user/id")

    assert first == second
    assert len({first: 1, second: 2}) == 1
    assert q.datom(q.var("e"), "user/id", 1) == q.datom(
        q.var("e"), "user/id", 1
    )


def test_transaction_context_specific_forms_are_explicit_and_composable() -> None:
    eve = tx.lookup_ref("user/handle", "eve")
    patches = [
        tx.patch_set(["profile", "status"], ":literal"),
        tx.patch_unset(q.kw("obsolete")),
        tx.patch_update(["tags"], "conj", ":literal"),
        tx.patch_update(q.kw("profile"), q.kw("assoc"), "role", "admin"),
    ]
    transaction = tx.data(
        tx.entity(
            -1,
            {
                "user/handle": "alice",
                "user/friend": eve,
                "user/child": tx.entity(-2, {"user/handle": "child"}),
                "user/data": {"set": ":literal"},
            },
        ),
        tx.patch_idoc(eve, "user/profile", patches),
        tx.invoke("people/inc-age", eve),
    )

    assert isinstance(eve, LookupRef)
    assert eve.as_data() == [":user/handle", "eve"]
    assert all(isinstance(patch, PatchOp) for patch in patches)
    assert patches[0].to_form()[2] == ":literal"
    assert patches[2].to_form()[3] == ":literal"
    assert transaction.as_data() == [
        {
            ":user/handle": "alice",
            ":user/friend": [":user/handle", "eve"],
            ":user/child": {":user/handle": "child", ":db/id": -2},
            ":user/data": {"set": ":literal"},
            ":db/id": -1,
        },
        [
            ":db.fn/patchIdoc",
            [":user/handle", "eve"],
            ":user/profile",
            [
                [":set", ["profile", "status"], ":literal"],
                [":unset", ":obsolete"],
                [":update", ["tags"], ":conj", ":literal"],
                [":update", ":profile", ":assoc", "role", "admin"],
            ],
        ],
        [":people/inc-age", [":user/handle", "eve"]],
    ]
