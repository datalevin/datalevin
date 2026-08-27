"""Pure, composable Datalevin transaction forms."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from ._forms import Form, RawForm, form_data
from .query import kw

_MISSING = object()


def _attribute(value):
    return kw(value) if isinstance(value, str) else value


def _callable(value):
    return kw(value) if isinstance(value, str) else value


@dataclass(frozen=True, slots=True)
class TxItem(Form):
    """One entity map or operation in transaction data."""

    form: object

    def to_form(self):
        return self.form


@dataclass(frozen=True, slots=True)
class TxData(Form, Sequence):
    """Immutable transaction data accepted directly by ``Connection``."""

    items: tuple[Form, ...]

    def to_form(self):
        return tuple(item.to_form() if isinstance(item, Form) else item for item in self.items)

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


def ensure(predicate, *args) -> TxItem:
    return TxItem((kw("db/ensure"), _callable(predicate), *args))


def patch_idoc(entity_id, attribute, patch, *, old_value=_MISSING) -> TxItem:
    items = [kw("db.fn/patchIdoc"), entity_id, _attribute(attribute)]
    if old_value is not _MISSING:
        items.append(old_value)
    items.append(patch)
    return TxItem(tuple(items))


def raw(form) -> RawForm:
    """Wrap a structured transaction item without string token heuristics."""

    return RawForm(form)


__all__ = [
    "TxData",
    "TxItem",
    "add",
    "call",
    "cas",
    "compare_and_swap",
    "data",
    "ensure",
    "entity",
    "patch_idoc",
    "raw",
    "retract",
    "retract_attribute",
    "retract_entity",
]
