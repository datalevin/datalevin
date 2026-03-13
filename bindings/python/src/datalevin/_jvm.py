"""JVM bootstrap and runtime jar resolution."""

from __future__ import annotations

import importlib.resources as resources
import os
import re
import shlex
from pathlib import Path

import jpype

from .errors import DatalevinConfigurationError, DatalevinJvmError

DATALEVIN_JAR_ENV = "DATALEVIN_JAR"
DATALEVIN_CLASSPATH_ENV = "DATALEVIN_CLASSPATH"
DATALEVIN_JVM_ARGS_ENV = "DATALEVIN_JVM_ARGS"
DATALEVIN_JAVACPP_CACHEDIR_ENV = "DATALEVIN_JAVACPP_CACHEDIR"
PACKAGE_NAME = "datalevin"
TARGET_JAR_PATTERNS = (
    "datalevin-runtime-*.jar",
    "datalevin-python-runtime-*.jar",
    "datalevin-java-*.jar",
)
DEFAULT_JAVACPP_CACHEDIR = Path("/tmp/datalevin-javacpp-cache")


def jvm_started() -> bool:
    """Return whether the current Python process already has a JVM."""

    return jpype.isJVMStarted()


def resolve_classpath(classpath: list[str] | tuple[str, ...] | None = None) -> list[str]:
    """Resolve the JVM classpath for the Datalevin runtime."""

    if classpath:
        return [str(Path(entry).expanduser()) for entry in classpath]

    env_classpath = os.environ.get(DATALEVIN_CLASSPATH_ENV)
    if env_classpath:
        return [entry for entry in env_classpath.split(os.pathsep) if entry]

    env_jar = os.environ.get(DATALEVIN_JAR_ENV)
    if env_jar:
        return [str(Path(env_jar).expanduser())]

    vendored = _vendored_jars()
    if vendored:
        return vendored

    repo_local = _repo_local_jars()
    if repo_local:
        return repo_local

    raise DatalevinConfigurationError(
        "Unable to resolve a Datalevin runtime jar. "
        f"Set {DATALEVIN_JAR_ENV}, set {DATALEVIN_CLASSPATH_ENV}, "
        "or vendor a jar under datalevin/jars/."
    )


def start_jvm(
    *,
    classpath: list[str] | tuple[str, ...] | None = None,
    jvm_args: list[str] | tuple[str, ...] | None = None,
    convert_strings: bool = False,
) -> None:
    """Start the JVM if it is not already running."""

    if jpype.isJVMStarted():
        return

    resolved_classpath = resolve_classpath(classpath)
    resolved_jvm_args = list(jvm_args or ())
    if not resolved_jvm_args:
        env_args = os.environ.get(DATALEVIN_JVM_ARGS_ENV)
        if env_args:
            resolved_jvm_args = shlex.split(env_args)
    _ensure_javacpp_cachedir_arg(resolved_jvm_args)

    try:
        jpype.startJVM(
            *resolved_jvm_args,
            classpath=resolved_classpath,
            convertStrings=convert_strings,
        )
    except OSError as exc:
        raise DatalevinJvmError(
            "Failed to start the JVM for Datalevin. "
            "Check that Java 21+ is installed and that the runtime jar is valid.",
            cause=exc,
        ) from exc


def _vendored_jars() -> list[str]:
    try:
        jar_root = resources.files(PACKAGE_NAME).joinpath("jars")
    except (FileNotFoundError, ModuleNotFoundError):
        return []

    if not jar_root.is_dir():
        return []

    jar = _preferred_runtime_jar(Path(str(jar_root)))
    return [] if jar is None else [str(jar)]


def _repo_local_jars() -> list[str]:
    repo_root = Path(__file__).resolve().parents[4]
    target_dir = repo_root / "target"
    if not target_dir.exists():
        return []
    jar = _preferred_runtime_jar(target_dir)
    return [] if jar is None else [str(jar)]


def _preferred_runtime_jar(dir_path: Path) -> Path | None:
    for pattern in TARGET_JAR_PATTERNS:
        matches = sorted(dir_path.glob(pattern), key=_jar_sort_key)
        if matches:
            return matches[-1]
    return None


def _jar_sort_key(path: Path):
    stem = path.stem
    version = stem.rsplit("-", 1)[-1]
    parts = re.split(r"([0-9]+)", version)
    return [int(part) if part.isdigit() else part for part in parts]


def _ensure_javacpp_cachedir_arg(jvm_args: list[str]) -> None:
    for arg in jvm_args:
        if (
            arg.startswith("-Dorg.bytedeco.javacpp.cachedir=")
            or arg.startswith("-Dorg.bytedeco.javacpp.cacheDir=")
        ):
            return

    cache_dir = Path(os.environ.get(DATALEVIN_JAVACPP_CACHEDIR_ENV, DEFAULT_JAVACPP_CACHEDIR))
    cache_dir.mkdir(parents=True, exist_ok=True)
    jvm_args.append(f"-Dorg.bytedeco.javacpp.cachedir={cache_dir}")
