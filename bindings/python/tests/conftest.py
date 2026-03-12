from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import shutil
import socket
import subprocess
import time

import pytest

from datalevin._jvm import resolve_classpath

REPO_ROOT = Path(__file__).resolve().parents[3]
SERVER_START_TIMEOUT_SECONDS = 30.0
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


def _find_free_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _server_expr(port: int, root: Path) -> str:
    return f"""
    (require '[datalevin.server :as srv]
             '[taoensso.timbre :as log])
    (log/set-min-level! :report)
    (let [server (srv/create {{:port {port}
                               :root {json.dumps(str(root))}}})]
      (.addShutdownHook
       (Runtime/getRuntime)
       (Thread. (fn []
                  (try
                    (srv/stop server)
                    (catch Throwable _)))))
      (srv/start server)
      @(promise))
    """.strip()


def _read_log(log_file) -> str:
    log_file.flush()
    log_file.seek(0)
    return log_file.read()


def _wait_for_server(proc: subprocess.Popen[str], port: int, log_file) -> None:
    deadline = time.monotonic() + SERVER_START_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            pytest.fail(
                f"Datalevin server exited before becoming ready (code {proc.returncode}).\n{_read_log(log_file)}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.1)

    proc.terminate()
    try:
        proc.wait(timeout=SERVER_STOP_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=SERVER_STOP_TIMEOUT_SECONDS)
    pytest.fail(f"Timed out waiting for Datalevin server on port {port}.\n{_read_log(log_file)}")


@pytest.fixture
def live_server(require_runtime, require_clojure_cli, tmp_path_factory) -> LiveServer:
    root = tmp_path_factory.mktemp("datalevin-server")
    port = _find_free_local_port()
    expr = _server_expr(port, root)

    with (root / "server.log").open("w+", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            [require_clojure_cli, "-M", "-e", expr],
            cwd=REPO_ROOT,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            _wait_for_server(proc, port, log_file)
            yield LiveServer(port=port, root=root)
        finally:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=SERVER_STOP_TIMEOUT_SECONDS)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=SERVER_STOP_TIMEOUT_SECONDS)
