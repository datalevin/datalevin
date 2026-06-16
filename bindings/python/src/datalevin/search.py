"""High-level Python wrappers for KV full-text search."""

from __future__ import annotations

from ._convert import to_python
from ._interop import _BINDINGS


class SearchEngine:
    """KV-backed full-text search engine."""

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
        """Return the underlying JVM search engine handle."""

        return self._require_open()

    def closed(self) -> bool:
        """Return whether this wrapper has been closed."""

        return self._handle is None

    def close(self) -> None:
        """Close this wrapper."""

        self._handle = None

    def add_doc(self, doc_ref, doc_text: str, check_exist=None) -> "SearchEngine":
        """Add or update one document in the search index."""

        _BINDINGS.search_add_doc(self.raw_handle(), doc_ref, doc_text, check_exist)
        return self

    def remove_doc(self, doc_ref) -> "SearchEngine":
        """Remove one document from the search index."""

        _BINDINGS.search_remove_doc(self.raw_handle(), doc_ref)
        return self

    def clear_docs(self) -> "SearchEngine":
        """Remove all documents from the search index."""

        _BINDINGS.search_clear_docs(self.raw_handle())
        return self

    def doc_indexed(self, doc_ref) -> bool:
        """Return whether a document reference is indexed."""

        return bool(_BINDINGS.search_doc_indexed(self.raw_handle(), doc_ref))

    def doc_count(self) -> int:
        """Return the number of indexed documents."""

        return int(_BINDINGS.search_doc_count(self.raw_handle()))

    def search(self, query: str, opts=None):
        """Search indexed documents."""

        return to_python(_BINDINGS.search(self.raw_handle(), query, opts))

    def re_index(self, opts=None) -> "SearchEngine":
        """Rebuild the index from stored raw text."""

        self._handle = _BINDINGS.search_re_index(self.raw_handle(), opts)
        return self

    def _require_open(self):
        if self._handle is None:
            raise RuntimeError("search engine is closed.")
        return self._handle


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
