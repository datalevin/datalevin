from __future__ import annotations

from datalevin import (
    Keyword,
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
