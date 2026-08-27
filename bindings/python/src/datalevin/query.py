"""Pure, composable Datalog query forms.

The objects in this module do not start the JVM.  A connection lowers them to
the active backend only when a query is executed.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from ._forms import (
    Form,
    FrozenMap,
    Keyword,
    RawForm,
    Symbol,
    form_data,
    immutable_snapshot,
)


def kw(name: str) -> Keyword:
    """Create a backend-neutral keyword."""

    return name if isinstance(name, Keyword) else Keyword(name)


def sym(name: str) -> Symbol:
    """Create a backend-neutral symbol."""

    return name if isinstance(name, Symbol) else Symbol(name)


def var(name: str) -> Symbol:
    """Create a Datalog variable, adding ``?`` when omitted."""

    if isinstance(name, Symbol):
        if not name.name.startswith("?"):
            raise ValueError(f"Expected a Datalog variable, got {name}.")
        return name
    text = str(name)
    return Symbol(text if text.startswith("?") else f"?{text}")


v = var


def source(name: str = "$", /) -> Symbol:
    """Create a Datalog source symbol, adding ``$`` when omitted."""

    if isinstance(name, Symbol):
        if not name.name.startswith("$"):
            raise ValueError(f"Expected a Datalog source, got {name}.")
        return name
    text = str(name)
    return Symbol(text if text.startswith("$") else f"${text}")


DB = Symbol("$")
RULES = Symbol("%")
IGNORE = Symbol("_")
_ELLIPSIS = Symbol("...")
_DOT = Symbol(".")
_MISSING = object()


def _as_form(value):
    return value.to_form() if isinstance(value, Form) else value


def _attribute(value):
    return kw(value) if isinstance(value, str) else value


def _callable(value):
    return sym(value) if isinstance(value, str) else value


def _is_variable_symbol(value) -> bool:
    return isinstance(value, Symbol) and value.name.startswith("?")


def _is_order_index(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


@dataclass(frozen=True, slots=True)
class Expression(Form):
    """A query call or find expression."""

    form: tuple[object, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "form", immutable_snapshot(self.form))

    def to_form(self):
        return self.form


@dataclass(frozen=True, slots=True)
class Clause(Form):
    """A Datalog ``:where`` or ``:having`` clause."""

    form: tuple[object, ...]
    kind: str = "clause"

    def __post_init__(self) -> None:
        object.__setattr__(self, "form", immutable_snapshot(self.form))

    def to_form(self):
        return self.form


def _clause_operator(value):
    return value.kind if isinstance(value, Clause) else None


def _reject_and_clauses(clauses, context: str) -> None:
    if any(_clause_operator(clause) == "and" for clause in clauses):
        raise ValueError(
            f"q.and_ is only valid as a branch of q.or_ or q.or_join, not {context}."
        )


@dataclass(frozen=True, slots=True)
class Binding(Form):
    """A tuple, collection, relation, or ignored function/input binding."""

    form: object

    def __post_init__(self) -> None:
        object.__setattr__(self, "form", immutable_snapshot(self.form))

    def to_form(self):
        return self.form


@dataclass(frozen=True, slots=True)
class FindSpec(Form):
    """The complete value following a query's ``:find`` marker."""

    form: tuple[object, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "form", immutable_snapshot(self.form))

    def to_form(self):
        return self.form


def _find_spec_details(find: FindSpec):
    form = find.form
    if len(form) == 2 and form[-1] == _DOT:
        return "scalar", (form[0],)
    if len(form) > 2 and form[-1] == _DOT:
        return "tuple", form[:-1]
    if len(form) == 1 and isinstance(form[0], tuple):
        nested = form[0]
        if len(nested) == 2 and nested[-1] == _ELLIPSIS:
            return "collection", (nested[0],)
        return "tuple", nested
    return "relation", form


def _find_element_variables(value):
    if _is_variable_symbol(value):
        return (value,)
    if not isinstance(value, Expression) or not value.form:
        return ()

    operator = value.form[0]
    name = operator.name if isinstance(operator, Symbol) else None
    if name == "pull":
        variable_index = 2 if len(value.form) == 4 else 1
        if variable_index < len(value.form):
            variable = value.form[variable_index]
            return (variable,) if _is_variable_symbol(variable) else ()
        return ()
    if name in {"+", "-", "*", "/", "mod", "rem", "quot"}:
        return tuple(
            variable
            for argument in value.form[1:]
            for variable in _find_element_variables(argument)
        )

    arguments = value.form[2:] if name == "aggregate" else value.form[1:]
    if arguments and _is_variable_symbol(arguments[-1]):
        return (arguments[-1],)
    return ()


def _find_variables(find: FindSpec):
    _, elements = _find_spec_details(find)
    return {
        variable
        for element in elements
        for variable in _find_element_variables(element)
    }


@dataclass(frozen=True, slots=True)
class Order:
    term: object
    direction: Keyword

    def __post_init__(self) -> None:
        term = immutable_snapshot(self.term)
        if not (_is_variable_symbol(term) or _is_order_index(term)):
            raise TypeError(
                "An order term must be a query variable or non-negative index."
            )
        if not isinstance(self.direction, Keyword) or self.direction.name not in {
            "asc",
            "desc",
        }:
            raise ValueError("An order direction must be :asc or :desc.")
        object.__setattr__(self, "term", term)


@dataclass(frozen=True, slots=True, init=False)
class PullAttr(Form):
    """An attribute expression whose option values retain their native types."""

    attribute: object
    options: tuple[tuple[Keyword, object], ...]

    def __init__(
        self,
        attribute,
        *,
        as_=_MISSING,
        limit=_MISSING,
        default=_MISSING,
        xform=_MISSING,
    ) -> None:
        options = []
        if as_ is not _MISSING:
            options.append((kw("as"), as_))
        if limit is not _MISSING:
            options.append((kw("limit"), _pull_limit(limit)))
        if default is not _MISSING:
            options.append((kw("default"), default))
        if xform is not _MISSING:
            options.append((kw("xform"), _pull_xform(xform)))
        object.__setattr__(self, "attribute", immutable_snapshot(_attribute(attribute)))
        object.__setattr__(self, "options", immutable_snapshot(options))

    def __hash__(self) -> int:
        # Pull attributes are valid map keys even when an arbitrary option
        # value is itself an unhashable Python container.
        try:
            return hash((self.attribute, self.options))
        except TypeError:
            return hash(PullAttr)

    def to_form(self):
        result = [self.attribute]
        for key, value in self.options:
            result.extend((key, value))
        return immutable_snapshot(tuple(result))

    def as_data(self):
        return form_data(self)


@dataclass(frozen=True, slots=True, init=False)
class PullNested(Form):
    """A nested or recursive pull map specification."""

    attribute: object
    pattern: object

    def __init__(self, attribute, pattern) -> None:
        object.__setattr__(
            self,
            "attribute",
            immutable_snapshot(_pull_attr_spec(attribute)),
        )
        object.__setattr__(
            self,
            "pattern",
            immutable_snapshot(_pull_nested_pattern(pattern)),
        )

    def to_form(self):
        return FrozenMap(((self.attribute, self.pattern),))

    def as_data(self):
        return form_data(self)


@dataclass(frozen=True, slots=True, init=False)
class PullSelector(Form):
    """An immutable, backend-neutral pull selector."""

    items: tuple[object, ...]

    def __init__(self, items=()) -> None:
        object.__setattr__(
            self,
            "items",
            immutable_snapshot(tuple(_pull_selector_item(item) for item in items)),
        )

    def __iter__(self):
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, index):
        return self.items[index]

    def to_form(self):
        return self.items

    def as_data(self):
        return form_data(self)


@dataclass(frozen=True, slots=True)
class JoinVars(Form):
    required: tuple[object, ...]
    free: tuple[object, ...]

    def __post_init__(self) -> None:
        required = immutable_snapshot(self.required)
        free = immutable_snapshot(self.free)
        if not isinstance(required, tuple) or not isinstance(free, tuple):
            raise TypeError("Required and free join variables must be sequences.")
        variables = (*required, *free)
        if not variables:
            raise ValueError("Join variables must not be empty.")
        if not all(_is_variable_symbol(variable) for variable in variables):
            raise TypeError("Join variables must be values created by q.var().")
        if len(set(variables)) != len(variables):
            raise ValueError("Join variables must be distinct.")
        object.__setattr__(self, "required", required)
        object.__setattr__(self, "free", free)

    def to_form(self):
        if self.required:
            return (self.required, *self.free)
        return self.free


@dataclass(frozen=True, slots=True)
class RuleBranch(Form):
    name: Symbol
    variables: JoinVars
    clauses: tuple[Form, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.name, Symbol) or self.name.name.startswith(("?", "$")):
            raise TypeError("A rule name must be a plain Datalog symbol.")
        if self.name.name in {"%", "_"}:
            raise TypeError("A rule name must be a plain Datalog symbol.")
        if not isinstance(self.variables, JoinVars):
            raise TypeError("Rule variables must be a q.join_vars() value.")
        clauses = immutable_snapshot(self.clauses)
        if not clauses:
            raise ValueError("A rule branch requires at least one clause.")
        _reject_and_clauses(clauses, "at the top level of a rule branch")
        object.__setattr__(self, "clauses", clauses)

    def to_form(self):
        head = (self.name, *self.variables.to_form())
        return (head, *(_as_form(clause) for clause in self.clauses))


@dataclass(frozen=True, slots=True)
class RuleSet(Form):
    branches: tuple[RuleBranch, ...]

    def __post_init__(self) -> None:
        branches = immutable_snapshot(self.branches)
        if not branches:
            raise ValueError("A rule set requires at least one branch.")
        if not all(isinstance(branch, RuleBranch) for branch in branches):
            raise TypeError("A rule set accepts only q.rule_branch() values.")

        arities = {}
        for branch in branches:
            arity = (
                len(branch.variables.required),
                len(branch.variables.free),
            )
            previous = arities.setdefault(branch.name, arity)
            if previous != arity:
                raise ValueError(
                    f"Rule branches named {branch.name} must have matching "
                    "required/free arity."
                )
        object.__setattr__(self, "branches", branches)

    def to_form(self):
        return tuple(branch.to_form() for branch in self.branches)

    def as_data(self):
        return form_data(self)


def _normalize_return_map(find: FindSpec, mode: str, values):
    if isinstance(values, (str, bytes)):
        raise TypeError(f"{mode} must be a sequence of field names.")
    try:
        names = tuple(sym(name) if isinstance(name, str) else name for name in values)
    except TypeError as error:
        raise TypeError(f"{mode} must be a sequence of field names.") from error
    if not names:
        raise ValueError(f"{mode} requires at least one field name.")
    if not all(isinstance(name, Symbol) for name in names):
        raise TypeError(f"{mode} field names must be symbols or strings.")

    shape, elements = _find_spec_details(find)
    if shape in {"scalar", "collection"}:
        raise ValueError(f"{mode} does not work with a {shape} find specification.")
    if len(names) != len(elements):
        raise ValueError(
            f"{mode} field count must match the {len(elements)} find elements."
        )
    return (kw(mode), names)


def _validate_query_order(find: FindSpec, ordering) -> None:
    find_variables = _find_variables(find)
    _, elements = _find_spec_details(find)
    seen = set()
    for item in ordering:
        term = item.term
        key = ("index", term) if _is_order_index(term) else ("variable", term)
        if key in seen:
            raise ValueError("Order terms must be distinct.")
        seen.add(key)
        if _is_order_index(term):
            if term >= len(elements):
                raise ValueError(
                    "Order column index is outside the find specification."
                )
        elif term not in find_variables:
            raise ValueError("An order variable must occur in the find specification.")


@dataclass(frozen=True, slots=True, init=False)
class Query(Form):
    """An immutable, backend-neutral Datalevin query."""

    find: FindSpec
    where: tuple[Form, ...]
    inputs: tuple[object, ...]
    with_vars: tuple[object, ...]
    having: tuple[Form, ...]
    order_by: tuple[Order, ...]
    limit: int | None
    offset: int | None
    timeout: int | None
    return_map: tuple[Keyword, tuple[Symbol, ...]] | None

    def __init__(
        self,
        *,
        find,
        where=(),
        inputs=(),
        with_=(),
        having=(),
        order_by=(),
        limit=None,
        offset=None,
        timeout=None,
        keys=None,
        strs=None,
        syms=None,
    ) -> None:
        find_spec = find if isinstance(find, FindSpec) else relation(*_find_values(find))
        return_maps = [("keys", keys), ("strs", strs), ("syms", syms)]
        selected = [(mode, values) for mode, values in return_maps if values is not None]
        if len(selected) > 1:
            raise ValueError("Only one of keys, strs, or syms may be supplied.")
        return_map = None
        if selected:
            mode, names = selected[0]
            return_map = _normalize_return_map(find_spec, mode, names)

        normalized_order = []
        for item in order_by:
            normalized_order.append(item if isinstance(item, Order) else asc(item))
        _validate_query_order(find_spec, normalized_order)

        where_snapshot = immutable_snapshot(tuple(where))
        having_snapshot = immutable_snapshot(tuple(having))
        _reject_and_clauses(where_snapshot, "at the top level of :where")
        _reject_and_clauses(having_snapshot, "at the top level of :having")

        object.__setattr__(self, "find", find_spec)
        object.__setattr__(self, "where", where_snapshot)
        object.__setattr__(self, "inputs", immutable_snapshot(tuple(inputs)))
        object.__setattr__(self, "with_vars", immutable_snapshot(tuple(with_)))
        object.__setattr__(self, "having", having_snapshot)
        object.__setattr__(self, "order_by", immutable_snapshot(tuple(normalized_order)))
        object.__setattr__(self, "limit", limit)
        object.__setattr__(self, "offset", offset)
        object.__setattr__(self, "timeout", timeout)
        object.__setattr__(self, "return_map", immutable_snapshot(return_map))

    def to_form(self):
        result = [kw("find"), *self.find.to_form()]
        if self.return_map is not None:
            mode, names = self.return_map
            result.extend((mode, *names))
        if self.with_vars:
            result.extend((kw("with"), *self.with_vars))
        if self.inputs:
            result.extend((kw("in"), *self.inputs))
        if self.where:
            result.append(kw("where"))
            result.extend(_as_form(clause) for clause in self.where)
        if self.having:
            result.append(kw("having"))
            result.extend(_as_form(clause) for clause in self.having)
        if self.timeout is not None:
            result.extend((kw("timeout"), self.timeout))
        if self.order_by:
            ordering = []
            for item in self.order_by:
                ordering.extend((item.term, item.direction))
            result.extend((kw("order-by"), tuple(ordering)))
        if self.offset is not None:
            result.extend((kw("offset"), self.offset))
        if self.limit is not None:
            result.extend((kw("limit"), self.limit))
        return immutable_snapshot(tuple(result))

    def as_data(self):
        """Return a debug-friendly nested form without starting a backend."""

        return form_data(self)


def _find_values(value) -> tuple[object, ...]:
    if isinstance(value, (str, bytes, Keyword, Symbol, Expression, RawForm)):
        return (value,)
    try:
        return tuple(value)
    except TypeError:
        return (value,)


def query(**options) -> Query:
    """Build an immutable query from keyword arguments."""

    return Query(**options)


def relation(*expressions) -> FindSpec:
    return FindSpec(tuple(expressions))


def collection(expression) -> FindSpec:
    return FindSpec(((expression, _ELLIPSIS),))


def tuple_find(*expressions) -> FindSpec:
    return FindSpec((tuple(expressions),))


def scalar(expression) -> FindSpec:
    return FindSpec((expression, _DOT))


def tuple_scalar(*expressions) -> FindSpec:
    return FindSpec((*expressions, _DOT))


def call(function, *args) -> Expression:
    return Expression((_callable(function), *args))


def aggregate(function, *args) -> Expression:
    return call(function, *args)


def custom_aggregate(function_var, *args) -> Expression:
    return Expression((sym("aggregate"), function_var, *args))


def expression(operator, *args) -> Expression:
    return call(operator, *args)


def _pull_limit(value):
    if value is not None and (
        isinstance(value, bool) or not isinstance(value, int) or value <= 0
    ):
        raise ValueError("A pull limit must be a positive integer or None.")
    return value


def _pull_xform(value):
    if isinstance(value, str):
        return sym(value)
    if not isinstance(value, Symbol):
        raise TypeError("A pull xform must be a symbol or symbol name.")
    return value


def _pull_token_name(value):
    if isinstance(value, (Keyword, Symbol)):
        return value.name
    if isinstance(value, str):
        return value[1:] if value.startswith(":") else value
    return None


def _pull_attr_expression(values):
    values = tuple(values)
    if not values:
        raise ValueError("A pull attribute expression must not be empty.")

    legacy_op = _pull_token_name(values[0])
    if legacy_op in {"default", "limit"} and len(values) == 3:
        value = _pull_limit(values[2]) if legacy_op == "limit" else values[2]
        return immutable_snapshot((sym(legacy_op), _pull_attr_spec(values[1]), value))

    if len(values) % 2 == 0:
        raise ValueError("A pull attribute expression requires option/value pairs.")

    result = [_pull_attr_spec(values[0])]
    for index in range(1, len(values), 2):
        option_name = _pull_token_name(values[index])
        if option_name not in {"as", "limit", "default", "xform"}:
            raise ValueError(f"Unknown pull attribute option: {values[index]!r}.")
        option = kw(option_name)
        value = values[index + 1]
        if option_name == "limit":
            value = _pull_limit(value)
        elif option_name == "xform":
            value = _pull_xform(value)
        result.extend((option, value))
    return immutable_snapshot(tuple(result))


def _pull_attr_spec(value):
    if isinstance(value, str):
        return kw(value)
    if isinstance(value, (list, tuple)):
        return _pull_attr_expression(value)
    return value


def _pull_nested_pattern(value):
    if isinstance(value, str):
        return _ELLIPSIS if value == "..." else PullSelector((value,))
    if isinstance(value, Symbol) and value.name == "...":
        return value
    if isinstance(value, bool):
        raise ValueError("A pull recursion depth must be a positive integer.")
    if isinstance(value, int):
        if value <= 0:
            raise ValueError("A pull recursion depth must be a positive integer.")
        return value
    if isinstance(value, (PullSelector, RawForm)):
        return value
    if isinstance(value, (list, tuple)):
        return PullSelector(value)
    if isinstance(value, Mapping):
        return PullSelector((value,))
    return value


def _pull_selector_item(item):
    if isinstance(item, str):
        return sym("*") if item == "*" else kw(item)
    if isinstance(item, Mapping):
        return {
            _pull_attr_spec(key): _pull_nested_pattern(value)
            for key, value in item.items()
        }
    if isinstance(item, (list, tuple)):
        return _pull_attr_expression(item)
    return item


def _pull_pattern(pattern):
    if isinstance(pattern, (PullSelector, RawForm, Symbol)):
        return pattern
    if isinstance(pattern, str):
        return PullSelector((pattern,))
    if isinstance(pattern, (list, tuple)):
        return PullSelector(pattern)
    if isinstance(pattern, Mapping):
        return PullSelector((pattern,))
    return pattern


def selector(*attributes) -> PullSelector:
    """Build a typed pull selector."""

    if len(attributes) == 1 and isinstance(attributes[0], PullSelector):
        return attributes[0]

    if len(attributes) == 1 and isinstance(attributes[0], (list, tuple)):
        attributes = tuple(attributes[0])
    return PullSelector(attributes)


def pull_attr(
    attribute,
    *,
    as_=_MISSING,
    limit=_MISSING,
    default=_MISSING,
    xform=_MISSING,
) -> PullAttr:
    """Build a pull attribute expression with position-aware option values."""

    return PullAttr(
        attribute,
        as_=as_,
        limit=limit,
        default=default,
        xform=xform,
    )


def pull_nested(attribute, pattern) -> PullNested:
    """Build a nested pull selector for a reference attribute."""

    return PullNested(attribute, pattern)


def pull_recursive(attribute, depth=None) -> PullNested:
    """Build an unbounded or positive-depth recursive pull selector."""

    if depth is None:
        pattern = _ELLIPSIS
    elif isinstance(depth, bool) or not isinstance(depth, int) or depth <= 0:
        raise ValueError("A pull recursion depth must be a positive integer or None.")
    else:
        pattern = depth
    return PullNested(attribute, pattern)


def pull(variable, pattern, *, from_source=None) -> Expression:
    items = [sym("pull")]
    if from_source is not None:
        items.append(from_source)
    items.extend((variable, _pull_pattern(pattern)))
    return Expression(tuple(items))


def pattern(*terms, from_source=None) -> Clause:
    """Build a variable-arity database pattern with a typed attribute term."""

    if not terms:
        raise ValueError("A database pattern requires at least one term.")
    items = [] if from_source is None else [from_source]
    normalized = list(terms)
    if len(normalized) >= 2:
        normalized[1] = _attribute(normalized[1])
    items.extend(normalized)
    return Clause(tuple(items))


def datom(entity, attribute, value, *, from_source=None) -> Clause:
    return pattern(entity, attribute, value, from_source=from_source)


def predicate(function, *args) -> Clause:
    return Clause(((_callable(function), *args),))


def bind(function, binding, *args) -> Clause:
    return Clause(((_callable(function), *args), _as_form(binding)))


def rule(name, *args, from_source=None) -> Clause:
    items = [] if from_source is None else [from_source]
    items.extend((_callable(name), *args))
    return Clause(tuple(items))


def _clauses(operator, clauses, source_value=None) -> Clause:
    if not clauses:
        raise ValueError(f"{operator} requires at least one clause.")
    if operator in {"and", "not"}:
        _reject_and_clauses(clauses, f"inside q.{operator}_")
    items = [] if source_value is None else [source_value]
    items.append(sym(operator))
    items.extend(_as_form(clause) for clause in clauses)
    return Clause(tuple(items), kind=operator)


def and_(*clauses) -> Clause:
    return _clauses("and", clauses)


def or_(*clauses, from_source=None) -> Clause:
    return _clauses("or", clauses, from_source)


def not_(*clauses, from_source=None) -> Clause:
    return _clauses("not", clauses, from_source)


def join_vars(*free, required=()) -> JoinVars:
    return JoinVars(tuple(required), tuple(free))


def _join_clause(operator, variables, clauses, source_value=None) -> Clause:
    if not clauses:
        raise ValueError(f"{operator} requires at least one clause.")
    if not isinstance(variables, JoinVars):
        variables = join_vars(*variables)
    if operator == "not-join":
        _reject_and_clauses(clauses, "inside q.not_join")
    items = [] if source_value is None else [source_value]
    items.extend((sym(operator), variables.to_form()))
    items.extend(_as_form(clause) for clause in clauses)
    return Clause(tuple(items))


def or_join(variables, *clauses, from_source=None) -> Clause:
    return _join_clause("or-join", variables, clauses, from_source)


def not_join(variables, *clauses, from_source=None) -> Clause:
    if isinstance(variables, JoinVars) and variables.required:
        raise ValueError("not-join does not support required join variables.")
    return _join_clause("not-join", variables, clauses, from_source)


def tuple_binding(*bindings) -> Binding:
    if not bindings:
        raise ValueError("A tuple binding requires at least one item.")
    return Binding(tuple(_as_form(binding) for binding in bindings))


def collection_binding(binding) -> Binding:
    return Binding((_as_form(binding), _ELLIPSIS))


def relation_binding(*bindings) -> Binding:
    return Binding((tuple(_as_form(binding) for binding in bindings),))


def ignore_binding() -> Binding:
    return Binding(IGNORE)


def asc(term) -> Order:
    return Order(term, kw("asc"))


def desc(term) -> Order:
    return Order(term, kw("desc"))


def rule_branch(name, variables, *clauses) -> RuleBranch:
    if not clauses:
        raise ValueError("A rule branch requires at least one clause.")
    if not isinstance(variables, JoinVars):
        variables = join_vars(*variables)
    return RuleBranch(sym(name), variables, tuple(clauses))


def rules(*branches) -> RuleSet:
    if not branches:
        raise ValueError("A rule set requires at least one branch.")
    return RuleSet(tuple(branches))


def raw(form) -> RawForm:
    """Wrap a structured form without applying string token heuristics."""

    return RawForm(form)


__all__ = [
    "Binding",
    "Clause",
    "DB",
    "Expression",
    "FindSpec",
    "IGNORE",
    "JoinVars",
    "Keyword",
    "Order",
    "PullAttr",
    "PullNested",
    "PullSelector",
    "Query",
    "RULES",
    "RawForm",
    "RuleBranch",
    "RuleSet",
    "Symbol",
    "aggregate",
    "and_",
    "asc",
    "bind",
    "call",
    "collection",
    "collection_binding",
    "custom_aggregate",
    "datom",
    "desc",
    "expression",
    "ignore_binding",
    "join_vars",
    "kw",
    "not_",
    "not_join",
    "or_",
    "or_join",
    "pattern",
    "predicate",
    "pull",
    "pull_attr",
    "pull_nested",
    "pull_recursive",
    "query",
    "raw",
    "relation",
    "relation_binding",
    "rule",
    "rule_branch",
    "rules",
    "scalar",
    "selector",
    "source",
    "sym",
    "tuple_binding",
    "tuple_find",
    "tuple_scalar",
    "v",
    "var",
]
