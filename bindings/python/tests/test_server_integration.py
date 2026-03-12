from __future__ import annotations

from contextlib import suppress
import uuid

import pytest

from datalevin import connect, new_client


pytestmark = pytest.mark.usefixtures("require_runtime")


def test_remote_python_surface_works_against_live_server(live_server) -> None:
    db_name = f"py-remote-{uuid.uuid4().hex}"
    schema = {
        ":name": {
            ":db/valueType": ":db.type/string",
            ":db/unique": ":db.unique/identity",
        }
    }
    client_opts = {":pool-size": 1, ":time-out": 5000}
    client = new_client(live_server.admin_uri(), opts=client_opts)

    try:
        assert client.disconnected() is False

        client.create_database(db_name, "datalog")
        assert db_name in client.list_databases()

        open_info = client.open_database(db_name, "datalog", schema=schema, info=True)
        assert isinstance(open_info, dict)
        assert db_name in client.list_databases_in_use()

        with connect(
            live_server.database_uri(db_name),
            schema=schema,
            opts={":client-opts": client_opts},
        ) as conn:
            conn.transact([{":db/id": -1, ":name": "Ada"}])

            assert conn.query("[:find ?e . :in $ ?attr ?value :where [?e ?attr ?value]]", ":name", "Ada") == 1
            assert conn.pull([":name"], 1) == {":name": "Ada"}
    finally:
        with suppress(Exception):
            client.close_database(db_name)
        with suppress(Exception):
            client.drop_database(db_name)
        client.disconnect()

    assert client.disconnected() is True
