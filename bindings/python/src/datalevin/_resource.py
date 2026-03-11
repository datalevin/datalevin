"""Shared resource wrapper behavior."""

from __future__ import annotations


class ResourceWrapper:
    """Context-manager wrapper for a live Datalevin handle."""

    def __init__(self, handle, close_fn, closed_fn, kind: str) -> None:
        self._handle = handle
        self._close_fn = close_fn
        self._closed_fn = closed_fn
        self._kind = kind

    def __enter__(self):
        self._require_open()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def __repr__(self) -> str:
        state = "closed" if self._handle is None else "open"
        return f"<{type(self).__name__} {state}>"

    def raw_handle(self):
        """Return the underlying JVM handle."""

        return self._require_open()

    def close(self) -> None:
        """Close the wrapped handle."""

        handle = self._handle
        if handle is None:
            return
        self._handle = None
        self._close_fn(handle)

    def closed(self) -> bool:
        """Return whether the wrapped handle has been closed."""

        handle = self._handle
        if handle is None:
            return True
        return bool(self._closed_fn(handle))

    def _require_open(self):
        if self._handle is None:
            raise RuntimeError(f"{self._kind} is closed.")
        return self._handle
