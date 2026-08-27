"""Pure, composable Datalevin transaction forms."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from ._forms import Form, Keyword, RawForm, form_data, immutable_snapshot
from .query import kw

_MISSING = object()
_PATCH_UPDATE_OPERATIONS = frozenset({"conj", "merge", "assoc", "dissoc", "inc", "dec"})


def _attribute(value):
    return kw(value) if isinstance(value, str) else value


def _callable(value):
    return kw(value) if isinstance(value, str) else value


def _patch_path(path):
    """Validate a backend-neutral idoc path without changing literal strings."""

    if isinstance(path, (str, Keyword)):
        segments = (path,)
        result = path
    elif isinstance(path, (list, tuple)):
        if not path:
            raise ValueError("An idoc patch path must not be empty.")
        segments = tuple(path)
        result = segments
    else:
        raise TypeError("An idoc patch path must be a string, keyword, list, or tuple.")

    for segment in segments:
        if isinstance(segment, bool):
            raise TypeError("An idoc patch path index must be an integer, not bool.")
        if isinstance(segment, int):
            if segment < 0:
                raise ValueError("An idoc patch path index must be non-negative.")
            continue
        if isinstance(segment, Keyword):
            if segment.name in {"?", "*"}:
                raise ValueError("An idoc patch path does not allow keyword wildcards.")
            continue
        if not isinstance(segment, str):
            raise TypeError("Idoc patch path segments must be strings, keywords, or integers.")
    return result


def _patch_update_operation(operation):
    operation = kw(operation) if isinstance(operation, str) else operation
    if not isinstance(operation, Keyword):
        raise TypeError("An idoc patch update operation must be a keyword or keyword name.")
    if operation.name not in _PATCH_UPDATE_OPERATIONS:
        supported = ", ".join(sorted(_PATCH_UPDATE_OPERATIONS))
        raise ValueError(
            f"Unknown idoc patch update operation {operation}; expected one of {supported}."
        )
    return operation


@dataclass(frozen=True, slots=True)
class TxItem(Form):
    """One entity map or operation in transaction data."""

    form: object

    def __post_init__(self) -> None:
        object.__setattr__(self, "form", immutable_snapshot(self.form))

    def to_form(self):
        return self.form


@dataclass(frozen=True, slots=True, init=False)
class LookupRef(Form):
    """An explicit Datalevin lookup ref whose attribute is a keyword."""

    attribute: object
    value: object

    def __init__(self, attribute, value):
        object.__setattr__(self, "attribute", immutable_snapshot(_attribute(attribute)))
        object.__setattr__(self, "value", immutable_snapshot(value))

    def to_form(self):
        return (self.attribute, self.value)

    def as_data(self):
        return form_data(self)


@dataclass(frozen=True, slots=True)
class PatchOp(Form):
    """One typed idoc patch operation."""

    form: tuple[object, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "form", immutable_snapshot(self.form))

    def to_form(self):
        return self.form

    def as_data(self):
        return form_data(self)


@dataclass(frozen=True, slots=True)
class TxData(Form, Sequence):
    """Immutable transaction data accepted directly by ``Connection``."""

    items: tuple[Form, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "items", immutable_snapshot(self.items))

    def to_form(self):
        return immutable_snapshot(
            tuple(item.to_form() if isinstance(item, Form) else item for item in self.items)
        )

    def __getitem__(self, index):
        return self.items[index]

    def __len__(self) -> int:
        return len(self.items)

    def as_data(self):
        return form_data(self)


def data(*items) -> TxData:
    """Compose transaction items into immutable transaction data."""

    if len(items) == 1 and not isinstance(items[0], Form):
        candidate = items[0]
        if isinstance(candidate, Iterable) and not isinstance(candidate, (str, bytes, Mapping)):
            items = tuple(candidate)
    return TxData(tuple(items))


def entity(db_id=None, attrs=None, **values) -> TxItem:
    """Build an entity-map item, treating attribute-name strings as keywords."""

    result = {}
    if attrs:
        result.update({_attribute(key): value for key, value in attrs.items()})
    result.update({_attribute(key): value for key, value in values.items()})
    if db_id is not None:
        result[kw("db/id")] = db_id
    return TxItem(result)


def lookup_ref(attribute, value) -> LookupRef:
    """Build a lookup ref for an entity id or ref-valued attribute."""

    return LookupRef(attribute, value)


def add(entity_id, attribute, value) -> TxItem:
    return TxItem((kw("db/add"), entity_id, _attribute(attribute), value))


def retract(entity_id, attribute, value) -> TxItem:
    return TxItem((kw("db/retract"), entity_id, _attribute(attribute), value))


def retract_attribute(entity_id, attribute) -> TxItem:
    return TxItem((kw("db.fn/retractAttribute"), entity_id, _attribute(attribute)))


def retract_entity(entity_id) -> TxItem:
    return TxItem((kw("db/retractEntity"), entity_id))


def compare_and_swap(entity_id, attribute, old_value, new_value) -> TxItem:
    return TxItem((kw("db.fn/cas"), entity_id, _attribute(attribute), old_value, new_value))


cas = compare_and_swap


def call(function, *args) -> TxItem:
    return TxItem((kw("db.fn/call"), _callable(function), *args))


def invoke(function, *args) -> TxItem:
    """Invoke a transaction function installed under a database ident."""

    return TxItem((_callable(function), *args))


def ensure(predicate, *args) -> TxItem:
    return TxItem((kw("db/ensure"), _callable(predicate), *args))


def patch_set(path, value) -> PatchOp:
    """Set an idoc value; ``value`` remains ordinary literal data."""

    return PatchOp((kw("set"), _patch_path(path), value))


def patch_unset(path) -> PatchOp:
    """Remove an idoc value at ``path``."""

    return PatchOp((kw("unset"), _patch_path(path)))


def patch_update(path, operation, *args) -> PatchOp:
    """Apply one of Datalevin's supported idoc update operations."""

    return PatchOp(
        (kw("update"), _patch_path(path), _patch_update_operation(operation), *args)
    )


def patch_idoc(entity_id, attribute, patch, *, old_value=_MISSING) -> TxItem:
    if isinstance(patch, PatchOp):
        patch = (patch,)
    items = [kw("db.fn/patchIdoc"), entity_id, _attribute(attribute)]
    if old_value is not _MISSING:
        items.append(old_value)
    items.append(patch)
    return TxItem(tuple(items))


def raw(form) -> RawForm:
    """Wrap a structured transaction item without string token heuristics."""

    return RawForm(form)


__all__ = [
    "LookupRef",
    "PatchOp",
    "TxData",
    "TxItem",
    "add",
    "call",
    "cas",
    "compare_and_swap",
    "data",
    "ensure",
    "entity",
    "invoke",
    "lookup_ref",
    "patch_idoc",
    "patch_set",
    "patch_unset",
    "patch_update",
    "raw",
    "retract",
    "retract_attribute",
    "retract_entity",
]
