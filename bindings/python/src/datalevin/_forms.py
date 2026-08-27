"""Backend-neutral value and form nodes used by the public builders."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass


class FrozenMap(Mapping):
    """A small insertion-ordered, immutable mapping used inside forms."""

    __slots__ = ("_items",)

    def __init__(self, items=()) -> None:
        # Build through a temporary dict so normal mapping last-value and
        # insertion-order behavior is preserved without retaining mutable
        # storage on the resulting value.
        object.__setattr__(self, "_items", tuple(dict(items).items()))

    def __setattr__(self, name, value) -> None:
        raise AttributeError("FrozenMap values are immutable.")

    def __delattr__(self, name) -> None:
        raise AttributeError("FrozenMap values are immutable.")

    def __getitem__(self, key):
        for current_key, value in self._items:
            if current_key == key:
                return value
        raise KeyError(key)

    def __iter__(self):
        return (key for key, _ in self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __hash__(self) -> int:
        return hash(frozenset(self._items))

    def __repr__(self) -> str:
        return f"FrozenMap({dict(self._items)!r})"


def immutable_snapshot(value, _active=None):
    """Recursively detach and freeze host containers used in typed forms.

    Builder forms deliberately recognize Python's ordinary structural value
    containers. Other objects (for example backend handles) are treated as
    atomic values and retained by reference.
    """

    if isinstance(value, (Form, Keyword, Symbol, FrozenMap)):
        return value
    if isinstance(value, (str, bytes, int, float, complex, bool, type(None))):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)

    structural = isinstance(value, (Mapping, list, tuple, set, frozenset))
    if not structural:
        return value

    active = set() if _active is None else _active
    identity = id(value)
    if identity in active:
        raise ValueError("Datalevin forms cannot contain cyclic containers.")
    active.add(identity)
    try:
        if isinstance(value, Mapping):
            return FrozenMap(
                (immutable_snapshot(key, active), immutable_snapshot(item, active))
                for key, item in value.items()
            )
        if isinstance(value, (list, tuple)):
            return tuple(immutable_snapshot(item, active) for item in value)
        return frozenset(immutable_snapshot(item, active) for item in value)
    finally:
        active.remove(identity)


def _token_text(value: str, prefix: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"Datalevin token names must be strings, got {type(value).__name__}.")
    text = value[len(prefix) :] if prefix and value.startswith(prefix) else value
    if not text:
        raise ValueError("Datalevin token names must not be empty.")
    if any(char.isspace() for char in text):
        raise ValueError(f"Datalevin token names must not contain whitespace: {value!r}.")
    return text


@dataclass(frozen=True, slots=True)
class Keyword:
    """A backend-neutral EDN keyword value."""

    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _token_text(self.name, ":"))

    def __str__(self) -> str:
        return f":{self.name}"


@dataclass(frozen=True, slots=True)
class Symbol:
    """A backend-neutral EDN symbol value."""

    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", _token_text(self.name, ""))

    def __str__(self) -> str:
        return self.name


class Form(ABC):
    """A pure form that a JVM or native backend can lower at execution time."""

    @abstractmethod
    def to_form(self):
        """Return the form in backend-neutral Python containers and tokens."""


@dataclass(frozen=True, slots=True)
class RawForm(Form):
    """A structured escape hatch; strings remain literal values."""

    value: object

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", immutable_snapshot(self.value))

    def to_form(self):
        return self.value


def form_data(value):
    """Return a debug-friendly form using strings for keywords and symbols."""

    if isinstance(value, Form):
        return form_data(value.to_form())
    if isinstance(value, (Keyword, Symbol)):
        return str(value)
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            converted_key = form_data(key)
            try:
                hash(converted_key)
            except TypeError:
                converted_key = _hashable_form_data(converted_key)
            result[converted_key] = form_data(item)
        return result
    if isinstance(value, (list, tuple)):
        return [form_data(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return {form_data(item) for item in value}
    return value


def _hashable_form_data(value):
    if isinstance(value, Mapping):
        return tuple(
            (_hashable_form_data(key), _hashable_form_data(item))
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return tuple(_hashable_form_data(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(_hashable_form_data(item) for item in value)
    return value
