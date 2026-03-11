"""Error types for the Datalevin Python bindings."""

from __future__ import annotations

from typing import Any


class DatalevinError(RuntimeError):
    """Base class for Datalevin binding errors."""

    def __init__(
        self,
        message: str,
        *,
        type_name: str | None = None,
        data: Any = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.type_name = type_name
        self.data = data
        self.cause = cause


class DatalevinConfigurationError(DatalevinError):
    """Raised when the binding runtime is not configured correctly."""


class DatalevinJvmError(DatalevinError):
    """Raised when the JVM bridge fails before Datalevin code runs."""


class DatalevinJavaError(DatalevinError):
    """Raised when a Java-side Datalevin call fails."""

    def __init__(
        self,
        message: str,
        *,
        java_class: str | None = None,
        type_name: str | None = None,
        data: Any = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message, type_name=type_name, data=data, cause=cause)
        self.java_class = java_class

    @classmethod
    def from_java(cls, exc: BaseException) -> "DatalevinJavaError":
        java_class = None
        try:
            java_class = exc.getClass().getName()
        except Exception:
            java_class = type(exc).__name__
        return cls(str(exc), java_class=java_class, cause=exc)
