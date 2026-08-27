"""Backend-neutral value and form nodes used by the public builders."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass


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
