from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import subprocess
import time

import pytest

from datalevin._jvm import resolve_classpath

REPO_ROOT = Path(__file__).resolve().parents[3]
SERVER_START_TIMEOUT_SECONDS = 60.0
SERVER_STOP_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class LiveServer:
    port: int
    root: Path

    def admin_uri(self, username: str = "datalevin", password: str = "datalevin") -> str:
        return f"dtlv://{username}:{password}@127.0.0.1:{self.port}"

    def database_uri(self, name: str, username: str = "datalevin", password: str = "datalevin") -> str:
        return f"{self.admin_uri(username, password)}/{name}"


def runtime_available() -> bool:
    try:
        return bool(resolve_classpath())
    except Exception:
        return False


@pytest.fixture(scope="session")
def require_runtime() -> None:
    pytest.importorskip("jpype")
    if not runtime_available():
        pytest.skip("Datalevin runtime jar not configured")


@pytest.fixture(scope="session")
def require_clojure_cli() -> str:
    clojure = shutil.which("clojure")
    if clojure is None:
        pytest.skip("clojure CLI not installed")
    return clojure


def _server_expr(root: Path, port_file: Path) -> str:
    return f"""
    (require '[datalevin.server :as srv]
             '[taoensso.timbre :as log])
    (log/set-min-level! :report)
    (let [server (srv/create {{:port 0
                               :root {json.dumps(str(root))}}})]
      (.addShutdownHook
       (Runtime/getRuntime)
       (Thread. (fn []
                  (try
                    (srv/stop server)
                    (catch Throwable _)))))
      (srv/start server)
      (let [port (.getLocalPort (.socket ^java.nio.channels.ServerSocketChannel
                                         (.-server-socket ^datalevin.server.Server server)))]
        (spit {json.dumps(str(port_file))} (str port "\\n")))
      @(promise))
    """.strip()


def _wait_for_server(proc: subprocess.Popen[str], port_file: Path, log_path: Path) -> int:
    deadline = time.monotonic() + SERVER_START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            pytest.fail(
                f"Datalevin server exited before becoming ready (code {proc.returncode}).\n"
                f"{log_path.read_text(encoding='utf-8', errors='replace')}"
            )
        if port_file.exists():
            port = port_file.read_bytes()
            if port.endswith(b"\n"):
                return int(port)
        time.sleep(0.1)

    pytest.fail(
        f"Timed out waiting for Datalevin server after {SERVER_START_TIMEOUT_SECONDS:g} seconds.\n"
        f"{log_path.read_text(encoding='utf-8', errors='replace')}"
    )


@pytest.fixture
def live_server(require_runtime, require_clojure_cli, tmp_path_factory, request) -> LiveServer:
    root = tmp_path_factory.mktemp("datalevin-server")
    port_file = root / "server.port"
    log_path = root / "server.log"
    expr = _server_expr(root, port_file)
    if getattr(request, "param", None) == "native-values":
        expr = '(load-file "test/data/native_wire_server.clj")\n' + expr

    with log_path.open("w", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            [require_clojure_cli, "-M", "-e", expr],
            cwd=REPO_ROOT,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            port = _wait_for_server(proc, port_file, log_path)
            yield LiveServer(port=port, root=root)
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=SERVER_STOP_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=SERVER_STOP_TIMEOUT_SECONDS)
