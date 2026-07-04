"""Opaque Datalevin database value wrapper."""

from __future__ import annotations

from ._convert import to_edn_form, to_python
from ._interop import _BINDINGS


def _edn_form(value):
    if isinstance(value, str):
        return _BINDINGS.read_edn(value)
    return to_edn_form(value)


class Database:
    """Database value returned by simulated transaction reports."""

    def __init__(self, handle) -> None:
        self._handle = handle

    def __repr__(self) -> str:
        return "<Database>"

    def raw_handle(self):
        """Return the underlying JVM database value."""

        return self._handle

    def entid(self, eid):
        """Resolve an entity id or lookup ref to an entity id."""

        return to_python(_BINDINGS.database_entid(self.raw_handle(), eid))

    def entity(self, eid):
        """Return a lazy entity for the given entity id or lookup ref."""

        from .entity import Entity

        entity = _BINDINGS.database_entity(self.raw_handle(), eid)
        if entity is None:
            return None
        return Entity(entity)

    def entity_map(self, eid):
        """Return a touched entity map for the given entity id or lookup ref."""

        return to_python(_BINDINGS.database_entity_map(self.raw_handle(), eid))

    def pull(self, selector, eid):
        """Pull one entity using a raw selector value."""

        return to_python(_BINDINGS.database_pull(self.raw_handle(), _edn_form(selector), eid))

    def pull_many(self, selector, eids):
        """Pull many entities using a raw selector value."""

        return to_python(_BINDINGS.database_pull_many(self.raw_handle(), _edn_form(selector), eids))

    def cardinality(self, attr):
        """Return the number of distinct values for an attribute."""

        return to_python(_BINDINGS.database_cardinality(self.raw_handle(), attr))

    def analyze(self, attr=None):
        """Collect query-planner statistics for all attributes or one attribute."""

        return to_python(_BINDINGS.database_analyze(self.raw_handle(), attr))
