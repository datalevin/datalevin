"""High-level Python wrappers for KV full-text search."""

from __future__ import annotations

from ._convert import to_python
from ._interop import _BINDINGS


def create_analyzer(opts=None, *, tokenizer=None, token_filters=None):
    """Create a Datalevin search analyzer from JVM search-utils helpers."""

    merged = dict(opts or {})
    if tokenizer is not None:
        merged[":tokenizer"] = tokenizer
    if token_filters is not None:
        merged[":token-filters"] = list(token_filters)
    return _BINDINGS.search_utils_create_analyzer(merged)


def lower_case_token_filter():
    """Return the Datalevin lower-case token filter."""

    return _BINDINGS.search_utils_lower_case_token_filter()


def unaccent_token_filter():
    """Return the Datalevin unaccent token filter."""

    return _BINDINGS.search_utils_unaccent_token_filter()


def create_stop_words_token_filter(stop_words_or_predicate):
    """Create a Datalevin stop-words token filter."""

    return _BINDINGS.search_utils_create_stop_words_token_filter(stop_words_or_predicate)


def en_stop_words_token_filter():
    """Return the Datalevin English stop-words token filter."""

    return _BINDINGS.search_utils_en_stop_words_token_filter()


def prefix_token_filter():
    """Return the Datalevin prefix token filter."""

    return _BINDINGS.search_utils_prefix_token_filter()


def create_ngram_token_filter(min_gram_size, max_gram_size=None):
    """Create a Datalevin ngram token filter."""

    return _BINDINGS.search_utils_create_ngram_token_filter(min_gram_size, max_gram_size)


def create_min_length_token_filter(min_length):
    """Create a Datalevin minimum-length token filter."""

    return _BINDINGS.search_utils_create_min_length_token_filter(min_length)


def create_max_length_token_filter(max_length):
    """Create a Datalevin maximum-length token filter."""

    return _BINDINGS.search_utils_create_max_length_token_filter(max_length)


def create_stemming_token_filter(language: str):
    """Create a Datalevin Snowball stemming token filter."""

    return _BINDINGS.search_utils_create_stemming_token_filter(language)


def create_regexp_tokenizer(pattern: str):
    """Create a Datalevin regexp tokenizer from a Java regular-expression string."""

    return _BINDINGS.search_utils_create_regexp_tokenizer(pattern)


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
