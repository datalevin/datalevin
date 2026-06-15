"""High-level Python wrapper for KV search index writers."""

from __future__ import annotations

from ._convert import to_python
from ._interop import _BINDINGS


class SearchIndexWriter:
    """Batched writer for a Datalevin KV full-text search index."""

    def __init__(self, handle) -> None:
        self._handle = handle

    def __enter__(self):
        self._require_open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self._handle is None else "open"
        return f"<{type(self).__name__} {state}>"

    def raw_handle(self):
        """Return the underlying JVM writer handle."""

        return self._require_open()

    def closed(self) -> bool:
        """Return whether this writer has been committed or closed."""

        return self._handle is None

    def close(self) -> None:
        """Close this wrapper without committing pending documents."""

        self._handle = None

    def write(self, doc_ref, doc_text: str) -> "SearchIndexWriter":
        """Add one document to the pending search index batch."""

        _BINDINGS.search_write(self.raw_handle(), doc_ref, doc_text)
        return self

    def commit(self):
        """Flush all pending documents and close this writer."""

        handle = self.raw_handle()
        result = to_python(_BINDINGS.search_commit(handle))
        self._handle = None
        return result

    def _require_open(self):
        if self._handle is None:
            raise RuntimeError("search index writer is closed.")
        return self._handle
