"""High-level Python wrapper for standalone Datalevin vector indexes."""

from __future__ import annotations

from ._convert import to_python
from ._interop import _BINDINGS
from ._resource import ResourceWrapper


class VectorIndex(ResourceWrapper):
    """KV-backed standalone vector index."""

    def __init__(self, handle) -> None:
        super().__init__(handle, _BINDINGS.close_vector_index, _BINDINGS.vector_index_closed, "vector index")

    def add_vec(self, vec_ref, vec_data) -> "VectorIndex":
        """Add one vector to the index."""

        _BINDINGS.vector_add_vec(self.raw_handle(), vec_ref, vec_data)
        return self

    def remove_vec(self, vec_ref) -> "VectorIndex":
        """Remove all vectors associated with a reference."""

        _BINDINGS.vector_remove_vec(self.raw_handle(), vec_ref)
        return self

    def vec_indexed(self, vec_ref) -> bool:
        """Return whether a vector reference is indexed."""

        return bool(_BINDINGS.vector_indexed(self.raw_handle(), vec_ref))

    def search_vec(self, query_vec, opts=None):
        """Search the vector index."""

        return to_python(_BINDINGS.vector_search(self.raw_handle(), query_vec, opts))

    def re_index(self, opts=None) -> "VectorIndex":
        """Rebuild the vector index."""

        self._handle = _BINDINGS.vector_re_index(self.raw_handle(), opts)
        return self

    def clear(self) -> "VectorIndex":
        """Clear this vector index from memory and disk."""

        handle = self.raw_handle()
        _BINDINGS.vector_clear(handle)
        self._handle = None
        return self

    def force_checkpoint(self) -> "VectorIndex":
        """Force vector checkpoint persistence to the backing KV store."""

        _BINDINGS.vector_force_checkpoint(self.raw_handle())
        return self

    def info(self):
        """Return vector index metadata."""

        return to_python(_BINDINGS.vector_info(self.raw_handle()))

    def checkpoint_state(self):
        """Return checkpoint metadata for this vector index."""

        return to_python(_BINDINGS.vector_checkpoint_state(self.raw_handle()))
