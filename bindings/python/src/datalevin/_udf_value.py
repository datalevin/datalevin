"""Pure UDF descriptor values shared by the typed builders and registry."""

from __future__ import annotations

from collections.abc import Mapping

from ._forms import Form, FrozenMap, Keyword, Symbol, form_data, immutable_snapshot


UDF_KINDS = frozenset(
    {"query-fn", "predicate", "tx-fn", "analyzer", "query-analyzer"}
)

_ALIASES = {
    ":udf/lang": "lang",
    "lang": "lang",
    ":udf/kind": "kind",
    "kind": "kind",
    ":udf/id": "id",
    "id": "id",
    "udf_id": "id",
    ":udf/version": "version",
    "version": "version",
}
_MISSING = object()


def _keyword_text(value, field: str) -> str:
    if isinstance(value, Keyword):
        text = value.name
    elif isinstance(value, str):
        text = value[1:] if value.startswith(":") else value
    else:
        raise TypeError(f"UDF descriptor {field} must be a keyword or string.")
    if not text:
        raise ValueError(f"UDF descriptor {field} must not be empty.")
    if any(char.isspace() for char in text):
        raise ValueError(f"UDF descriptor {field} must not contain whitespace.")
    return f":{text}"


def _descriptor_fields(value) -> dict[str, object]:
    if isinstance(value, UdfDescriptor):
        return {
            "lang": value.lang,
            "kind": value.kind,
            "id": value.udf_id,
            "version": value.version,
        }
    if not isinstance(value, Mapping):
        return {"id": value}

    fields: dict[str, object] = {}
    unknown = []
    for key, item in value.items():
        key_text = str(key) if isinstance(key, Keyword) else key
        canonical = _ALIASES.get(key_text)
        if canonical is None:
            unknown.append(key)
            continue
        if canonical in fields and fields[canonical] != item:
            raise ValueError(f"Conflicting UDF descriptor values for {canonical}.")
        fields[canonical] = item
    if unknown:
        raise ValueError(f"Unsupported UDF descriptor key(s): {unknown!r}.")
    return fields


def descriptor_data(
    value=None,
    *,
    kind="query-fn",
    lang="java",
    version=_MISSING,
) -> dict[str, object]:
    """Normalize descriptor aliases while retaining the legacy string map shape."""

    fields = _descriptor_fields(value)
    udf_id = fields.get("id")
    if udf_id is None:
        raise TypeError("udf id is required")

    normalized_lang = _keyword_text(fields.get("lang", lang), ":udf/lang")
    normalized_kind = _keyword_text(fields.get("kind", kind), ":udf/kind")
    if normalized_kind[1:] not in UDF_KINDS:
        supported = ", ".join(sorted(UDF_KINDS))
        raise ValueError(
            f"Unsupported UDF kind {normalized_kind}; expected one of {supported}."
        )
    normalized_id = _keyword_text(udf_id, ":udf/id")

    normalized_version = fields.get("version", version)
    if normalized_version is not _MISSING and normalized_version is not None:
        if isinstance(normalized_version, bool) or not isinstance(
            normalized_version, (Keyword, str, int)
        ):
            raise TypeError(
                "UDF descriptor :udf/version must be a keyword, string, integer, or None."
            )
        normalized_version = immutable_snapshot(normalized_version)

    result: dict[str, object] = {
        ":udf/lang": normalized_lang,
        ":udf/kind": normalized_kind,
        ":udf/id": normalized_id,
    }
    if normalized_version is not _MISSING and normalized_version is not None:
        result[":udf/version"] = normalized_version
    return result


class UdfDescriptor(Form, Mapping):
    """Immutable, backend-neutral UDF descriptor for typed APIs.

    Its mapping view retains the familiar colon-string representation.  Its
    form view carries explicit keyword nodes, so it remains typed when nested
    in a query, transaction, schema, search option, or runtime input.
    """

    __slots__ = ("_data", "_form")

    def __init__(
        self,
        udf_id=None,
        *,
        kind="query-fn",
        lang="python",
        version=_MISSING,
    ) -> None:
        data = descriptor_data(udf_id, kind=kind, lang=lang, version=version)
        form_items = []
        for key, item in data.items():
            if key == ":udf/version":
                form_item = (
                    Keyword(item)
                    if isinstance(item, str) and item.startswith(":")
                    else item
                )
            else:
                form_item = Keyword(item)
            form_items.append((Keyword(key), form_item))
        object.__setattr__(self, "_data", FrozenMap(data.items()))
        object.__setattr__(self, "_form", FrozenMap(form_items))

    def __setattr__(self, name, value) -> None:
        raise AttributeError("UdfDescriptor values are immutable.")

    def __delattr__(self, name) -> None:
        raise AttributeError("UdfDescriptor values are immutable.")

    @property
    def lang(self):
        return self._data[":udf/lang"]

    @property
    def kind(self):
        return self._data[":udf/kind"]

    @property
    def udf_id(self):
        return self._data[":udf/id"]

    @property
    def version(self):
        return self._data.get(":udf/version")

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __eq__(self, other):
        if not isinstance(other, Mapping):
            return NotImplemented
        return dict(self.items()) == dict(other.items())

    def __hash__(self) -> int:
        return hash(self._form)

    def __repr__(self) -> str:
        return f"UdfDescriptor({dict(self._data)!r})"

    def to_form(self):
        return self._form

    def as_data(self):
        return form_data(self)

    @classmethod
    def of(cls, kind, udf_id, *, lang="python", version=_MISSING):
        return cls(udf_id, kind=kind, lang=lang, version=version)

    @classmethod
    def query_fn(cls, udf_id, *, lang="python", version=_MISSING):
        return cls.of("query-fn", udf_id, lang=lang, version=version)

    @classmethod
    def predicate(cls, udf_id, *, lang="python", version=_MISSING):
        return cls.of("predicate", udf_id, lang=lang, version=version)

    @classmethod
    def tx_fn(cls, udf_id, *, lang="python", version=_MISSING):
        return cls.of("tx-fn", udf_id, lang=lang, version=version)

    @classmethod
    def analyzer(cls, udf_id, *, lang="python", version=_MISSING):
        return cls.of("analyzer", udf_id, lang=lang, version=version)

    @classmethod
    def query_analyzer(cls, udf_id, *, lang="python", version=_MISSING):
        return cls.of("query-analyzer", udf_id, lang=lang, version=version)

    @classmethod
    def from_value(cls, value, *, default_lang="python"):
        if isinstance(value, cls):
            return value
        return cls(value, lang=default_lang)


def udf_reference(value):
    """Normalize a concrete descriptor/id while retaining query variables."""

    if isinstance(value, str):
        return Keyword(value)
    if isinstance(value, Mapping) and not isinstance(value, UdfDescriptor):
        return UdfDescriptor.from_value(value, default_lang="java")
    if isinstance(value, (Keyword, Symbol, Form)):
        return value
    raise TypeError(
        "A UDF reference must be a descriptor, keyword id, or query variable."
    )


__all__ = ["UDF_KINDS", "UdfDescriptor", "descriptor_data", "udf_reference"]
