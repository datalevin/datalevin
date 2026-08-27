from __future__ import annotations

from collections.abc import Mapping
import os
from pathlib import Path
import re

import pytest

from datalevin import Keyword, Symbol, connect, q, read_edn, tx
from datalevin._convert import to_python
from datalevin._java import classes


pytestmark = pytest.mark.usefixtures("require_runtime")

_MISSING = object()
_QUERY_SECTIONS = {
    "find",
    "with",
    "in",
    "where",
    "having",
    "order-by",
    "offset",
    "limit",
    "timeout",
    "keys",
    "strs",
    "syms",
}
_ERROR_PATTERNS = (
    (":dl/invalid-value-type", re.compile(r"invalid data|value type|:db/valueType", re.I)),
    (":dl/unique-constraint", re.compile(r"unique", re.I)),
    (
        ":dl/conflicting-upsert",
        re.compile(r"conflicting upsert|upsert.*conflict|conflict.*upsert", re.I),
    ),
)


def _dtlvtest_root() -> Path:
    configured = os.environ.get("DTLVTEST_ROOT")
    root = (
        Path(configured).expanduser().resolve()
        if configured
        else Path(__file__).resolve().parents[4] / "dtlvtest"
    )
    if (root / "manifest.edn").is_file():
        root = root.parent
    if not (root / "spec" / "manifest.edn").is_file():
        pytest.skip(
            "dtlvtest golden specs not found; set DTLVTEST_ROOT to the sibling checkout"
        )
    return root


def _decode_edn(value):
    cls = classes()
    if isinstance(value, cls.keyword_type):
        return Keyword(str(value))
    if isinstance(value, cls.symbol_type):
        return Symbol(str(value))
    if isinstance(value, cls.map_type) or hasattr(value, "entrySet"):
        return {
            _decode_edn(entry.getKey()): _decode_edn(entry.getValue())
            for entry in value.entrySet()
        }
    if isinstance(value, cls.set_type):
        return {_decode_edn(item) for item in value}
    if isinstance(value, (cls.list_type, cls.collection_type)) or hasattr(
        value, "iterator"
    ):
        return [_decode_edn(item) for item in value]
    return to_python(value)


def _read_edn_file(path: Path):
    return _decode_edn(read_edn(path.read_text(encoding="utf-8")))


def _field(mapping: Mapping, name: str, default=_MISSING):
    key = Keyword(name)
    if key in mapping:
        return mapping[key]
    legacy_key = f":{key.name}"
    if legacy_key in mapping:
        return mapping[legacy_key]
    if default is not _MISSING:
        return default
    raise KeyError(legacy_key)


def _token_name(value) -> str | None:
    if isinstance(value, (Keyword, Symbol)):
        return value.name
    return None


def _term(value):
    if isinstance(value, Symbol):
        if value.name.startswith("?"):
            return q.var(value)
        if value.name.startswith("$"):
            return q.source(value)
        if value.name == "%":
            return q.RULES
        if value.name == "_":
            return q.IGNORE
    return value


def _binding(value):
    if not isinstance(value, list):
        return _term(value)
    if len(value) == 2 and _token_name(value[1]) == "...":
        return q.collection_binding(_term(value[0]))
    if len(value) == 1 and isinstance(value[0], list):
        return q.relation_binding(*(_term(item) for item in value[0]))
    return q.tuple_binding(*(_term(item) for item in value))


def _find_term(value):
    if isinstance(value, list):
        if not value or not isinstance(value[0], Symbol):
            raise ValueError(f"Unsupported golden find expression: {value!r}")
        return q.aggregate(value[0], *(_term(item) for item in value[1:]))
    return _term(value)


def _find_spec(values):
    if not values:
        raise ValueError("A golden query must contain at least one :find value.")
    if _token_name(values[-1]) == ".":
        terms = [_find_term(value) for value in values[:-1]]
        return q.scalar(terms[0]) if len(terms) == 1 else q.tuple_scalar(*terms)
    if len(values) == 1 and isinstance(values[0], list):
        nested = values[0]
        if len(nested) == 2 and _token_name(nested[1]) == "...":
            return q.collection(_find_term(nested[0]))
        return q.tuple_find(*(_find_term(value) for value in nested))
    return q.relation(*(_find_term(value) for value in values))


def _clause(value):
    if not isinstance(value, list) or not value:
        raise ValueError(f"Unsupported golden where clause: {value!r}")
    if isinstance(value[0], list):
        call = value[0]
        if not call or not isinstance(call[0], Symbol):
            raise ValueError(f"Unsupported golden call clause: {value!r}")
        args = [_term(item) for item in call[1:]]
        if len(value) == 1:
            return q.predicate(call[0], *args)
        if len(value) == 2:
            return q.bind(call[0], _binding(value[1]), *args)
        raise ValueError(f"Unsupported golden call clause: {value!r}")

    terms = list(value)
    source_value = None
    if isinstance(terms[0], Symbol) and terms[0].name.startswith("$"):
        source_value = q.source(terms.pop(0))
    if isinstance(terms[0], Symbol) and not terms[0].name.startswith(("?", "$")):
        return q.rule(terms[0], *(_term(item) for item in terms[1:]), from_source=source_value)
    return q.pattern(*(_term(item) for item in terms), from_source=source_value)


def _orders(values):
    if len(values) == 1 and isinstance(values[0], list):
        values = values[0]
    result = []
    index = 0
    while index < len(values):
        term = _term(values[index])
        direction = _token_name(values[index + 1]) if index + 1 < len(values) else None
        if direction in {"asc", "desc"}:
            result.append(q.desc(term) if direction == "desc" else q.asc(term))
            index += 2
        else:
            result.append(q.asc(term))
            index += 1
    return result


def _query(form):
    sections: dict[str, list] = {}
    current = None
    for item in form:
        name = _token_name(item)
        if isinstance(item, Keyword) and name in _QUERY_SECTIONS:
            current = name
            sections.setdefault(name, [])
        elif current is None:
            raise ValueError(f"Golden query value appears before a section: {item!r}")
        else:
            sections[current].append(item)

    options = {
        "find": _find_spec(sections.get("find", [])),
        "where": [_clause(value) for value in sections.get("where", [])],
        "inputs": [_binding(value) for value in sections.get("in", [])],
        "with_": [_term(value) for value in sections.get("with", [])],
        "having": [_clause(value) for value in sections.get("having", [])],
        "order_by": _orders(sections.get("order-by", [])),
    }
    for query_key, builder_key in (
        ("offset", "offset"),
        ("limit", "limit"),
        ("timeout", "timeout"),
    ):
        if sections.get(query_key):
            options[builder_key] = sections[query_key][0]
    for return_key in ("keys", "strs", "syms"):
        if sections.get(return_key):
            options[return_key] = sections[return_key]
    return q.query(**options)


def _tx_item(value):
    if isinstance(value, Mapping):
        return tx.entity(attrs=value)
    if not isinstance(value, list) or not value or not isinstance(value[0], Keyword):
        raise ValueError(f"Unsupported golden transaction item: {value!r}")
    op = value[0].name
    args = value[1:]
    if op == "db/add":
        return tx.add(*args)
    if op == "db/retract":
        return tx.retract(*args)
    if op in {"db/retractAttribute", "db.fn/retractAttribute"}:
        return tx.retract_attribute(*args)
    if op in {"db/retractEntity", "db.fn/retractEntity"}:
        return tx.retract_entity(*args)
    if op in {"db/cas", "db.fn/cas"}:
        return tx.cas(*args)
    if op == "db.fn/call":
        return tx.call(*args)
    if op == "db/ensure":
        return tx.ensure(*args)
    return tx.invoke(value[0], *args)


def _tx_data(values):
    return tx.data(*(_tx_item(value) for value in values))


def _canonical(value):
    if isinstance(value, (Keyword, Symbol)):
        return str(value)
    if isinstance(value, Mapping):
        return {_canonical(key): _canonical(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return {_canonical(item) for item in value}
    return value


def _normalize(value, result_kind):
    kind = _token_name(result_kind)
    value = _canonical(value)
    if kind == "scalar":
        return value
    if kind == "tuple":
        return list(value)
    if kind == "set-of-scalars":
        return set(value)
    if kind == "set-of-tuples":
        return {tuple(item) for item in value}
    if kind == "seq-of-scalars":
        return list(value)
    if kind == "seq-of-tuples":
        return [list(item) for item in value]
    raise ValueError(f"Unsupported golden result kind: {result_kind}")


def _error_id(error: BaseException) -> str | None:
    data = getattr(error, "data", None)
    if isinstance(data, Mapping):
        for key in (Keyword("error/id"), ":error/id", Keyword("dl/error-id")):
            if key in data:
                return str(data[key])
    message = str(error)
    for error_id, pattern in _ERROR_PATTERNS:
        if pattern.search(message):
            return error_id
    return None


def _active_cases(root: Path):
    spec_root = root / "spec"
    manifest = _read_edn_file(spec_root / "manifest.edn")
    active_release = _field(manifest, "active-release")
    release = _read_edn_file(spec_root / "releases" / f"{active_release}.edn")
    suites = _field(manifest, "suites")
    for suite_id in _field(release, "required-suites"):
        entry = suites[suite_id]
        suite = _read_edn_file(root / _field(entry, "path"))
        dataset = _read_edn_file(root / _field(suite, "dataset"))
        for case_def in _field(suite, "cases"):
            yield active_release, suite_id, dataset, case_def


def _run_case(db_dir: Path, dataset, case_def):
    schema = _field(dataset, "schema")
    opts = _field(dataset, "conn-opts", None)
    with connect(str(db_dir), schema=schema, opts=opts) as conn:
        seed_tx = _field(dataset, "seed-tx", None)
        if seed_tx:
            conn.transact(_tx_data(seed_tx))

        operation = _token_name(_field(case_def, "op"))
        matcher = _token_name(_field(case_def, "matcher"))
        case_tx = _field(case_def, "tx-data", None)
        if operation == "query-after-tx" and case_tx:
            conn.transact(_tx_data(case_tx))

        if matcher == "equals":
            query = _query(_field(case_def, "query"))
            actual = conn.query(query, *_field(case_def, "args", []))
            result_kind = _field(case_def, "result-kind")
            assert _normalize(actual, result_kind) == _normalize(
                _field(case_def, "expect"), result_kind
            )
            return

        if matcher == "error-match":
            expected_error = _field(case_def, "expect-error")
            try:
                conn.transact(_tx_data(case_tx))
            except BaseException as error:
                assert _error_id(error) == str(_field(expected_error, "id"))
                message_pattern = _field(expected_error, "message-pattern", None)
                if message_pattern:
                    assert re.search(message_pattern, str(error))
                return
            raise AssertionError("Expected golden transaction to fail")

        raise ValueError(f"Unsupported golden matcher: {matcher}")


def test_python_binding_executes_active_dtlvtest_golden_release(tmp_path) -> None:
    root = _dtlvtest_root()
    cases = list(_active_cases(root))
    assert cases, "The active dtlvtest release must select at least one golden case."
    for release, suite_id, dataset, case_def in cases:
        case_id = _field(case_def, "id")
        label = f"{release}/{suite_id}/{case_id}"
        db_dir = tmp_path / f"{suite_id.name}-{case_id.name}"
        try:
            _run_case(db_dir, dataset, case_def)
        except BaseException as error:
            raise AssertionError(f"Python golden conformance failed at {label}") from error
