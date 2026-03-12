from __future__ import annotations

import importlib.util
import os
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SETUP_PY = ROOT / "setup.py"


def _load_setup_module():
    spec = importlib.util.spec_from_file_location("datalevin_python_setup", SETUP_PY)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    with patch("setuptools.setup"):
        spec.loader.exec_module(module)
    return module


def test_build_native_platform_prefers_override() -> None:
    module = _load_setup_module()
    with patch.dict(os.environ, {module.NATIVE_PLATFORM_ENV: "windows-x86_64"}):
        assert module._build_native_platform("linux_x86_64") == "windows-x86_64"


def test_wheel_platform_tag_tracks_target_platform() -> None:
    module = _load_setup_module()
    assert module._wheel_platform_tag("linux-x86_64", "linux_x86_64") == "linux_x86_64"
    assert module._wheel_platform_tag("windows-x86_64", "linux_x86_64") == "win_amd64"
