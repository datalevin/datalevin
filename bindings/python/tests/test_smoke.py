from __future__ import annotations

from pathlib import Path

import pytest

jpype = pytest.importorskip("jpype")

from datalevin import api_info, connect
from datalevin._jvm import resolve_classpath


def _runtime_available() -> bool:
    try:
        return bool(resolve_classpath())
    except Exception:
        return False


@pytest.mark.skipif(not _runtime_available(), reason="Datalevin runtime jar not configured")
def test_local_datalog_smoke(tmp_path: Path) -> None:
    info = api_info()
    assert isinstance(info, dict)
    assert "datalevin-version" in info

    db_dir = tmp_path / "db"
    with connect(
        str(db_dir),
        schema={":name": {":db/valueType": ":db.type/string"}},
    ) as conn:
        conn.transact([{":db/id": -1, ":name": "Ada"}])
        names = conn.query("[:find [?name ...] :where [?e :name ?name]]")
        assert names == ["Ada"]
