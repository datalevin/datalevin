from __future__ import annotations

import os
import subprocess
import sysconfig
from pathlib import Path

from distutils.errors import DistutilsSetupError
from setuptools import setup
from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
from setuptools.command.sdist import sdist as _sdist


PACKAGE_ROOT = Path(__file__).resolve().parent
REPO_ROOT = PACKAGE_ROOT.parent.parent
BUILD_FILE = REPO_ROOT / "build.clj"
NATIVE_PLATFORM_ENV = "DATALEVIN_NATIVE_PLATFORM"
WHEEL_PLATFORM_TAGS = {
    "linux-x86_64": "linux_x86_64",
    "linux-arm64": "linux_aarch64",
    "macosx-arm64": "macosx_11_0_arm64",
    "windows-x86_64": "win_amd64",
}


def _normalize_native_platform(value: str | None) -> str:
    if value is None:
        raise DistutilsSetupError("Missing native platform for Datalevin wheel build.")

    normalized = value.strip().lower().replace(".", "_")
    if not normalized:
        raise DistutilsSetupError("Empty native platform for Datalevin wheel build.")
    if normalized == "all":
        return "all"

    if any(token in normalized for token in ("linux", "manylinux", "musllinux")):
        if any(token in normalized for token in ("x86_64", "amd64")):
            return "linux-x86_64"
        if any(token in normalized for token in ("aarch64", "arm64")):
            return "linux-arm64"

    if any(token in normalized for token in ("macosx", "macos", "darwin")):
        if any(token in normalized for token in ("aarch64", "arm64")):
            return "macosx-arm64"

    if "freebsd" in normalized and any(token in normalized for token in ("x86_64", "amd64")):
        return "freebsd-x86_64"

    if "win" in normalized and any(token in normalized for token in ("x86_64", "amd64")):
        return "windows-x86_64"

    if normalized in {
        "linux_x86_64",
        "linux_arm64",
        "macosx_arm64",
        "freebsd_x86_64",
        "freebsd_amd64",
        "windows_x86_64",
    }:
        return normalized.replace("_", "-")

    raise DistutilsSetupError(
        f"Unsupported wheel platform '{value}'. "
        "Set DATALEVIN_NATIVE_PLATFORM to one of: "
        "linux-x86_64, linux-arm64, macosx-arm64, freebsd-x86_64, "
        "windows-x86_64, all."
    )


def _build_native_platform(plat_name: str | None) -> str:
    override = os.environ.get(NATIVE_PLATFORM_ENV)
    if override:
        return _normalize_native_platform(override)
    return _normalize_native_platform(plat_name or sysconfig.get_platform())


def _wheel_platform_tag(native_platform: str, fallback_tag: str | None) -> str:
    if fallback_tag:
        try:
            if _normalize_native_platform(fallback_tag) == native_platform:
                return fallback_tag
        except DistutilsSetupError:
            pass
    if native_platform == "freebsd-x86_64":
        raise DistutilsSetupError(
            "FreeBSD wheel tagging requires a native FreeBSD build or an explicit "
            "--plat-name freebsd_*_amd64."
        )
    return WHEEL_PLATFORM_TAGS[native_platform]


def _vendor_runtime_jar(native_platform: str) -> None:
    if not BUILD_FILE.exists():
        raise DistutilsSetupError(
            "Datalevin wheel builds must run from a full repository checkout so "
            "the runtime jar can be regenerated."
        )

    build_platform = "all" if native_platform == "freebsd-x86_64" else native_platform
    command = [
        "clojure",
        "-T:build",
        "vendor-jar",
        ":native-platform",
        build_platform,
    ]
    try:
        subprocess.run(command, cwd=REPO_ROOT, check=True)
    except FileNotFoundError as exc:
        raise DistutilsSetupError("Clojure CLI is required to build Datalevin wheels.") from exc
    except subprocess.CalledProcessError as exc:
        raise DistutilsSetupError(
            f"Datalevin runtime vendoring failed for platform {native_platform}."
        ) from exc


class bdist_wheel(_bdist_wheel):
    def finalize_options(self) -> None:
        super().finalize_options()
        self.root_is_pure = False
        self._datalevin_native_platform = None

    def get_tag(self) -> tuple[str, str, str]:
        _, _, plat = super().get_tag()
        native_platform = self._datalevin_native_platform or _build_native_platform(self.plat_name)
        return "py3", "none", _wheel_platform_tag(native_platform, plat)

    def run(self) -> None:
        native_platform = _build_native_platform(self.plat_name)
        self._datalevin_native_platform = native_platform
        self.announce(
            f"Vendoring Datalevin runtime jar for native platform {native_platform}",
            level=2,
        )
        _vendor_runtime_jar(native_platform)
        super().run()


class sdist(_sdist):
    def run(self) -> None:
        raise DistutilsSetupError(
            "Datalevin Python releases are wheel-only because the bundled JVM "
            "runtime is platform-specific. Build with `python -m build --wheel` "
            "or `python -m pip wheel --no-build-isolation bindings/python`."
        )


setup(cmdclass={"bdist_wheel": bdist_wheel, "sdist": sdist})
