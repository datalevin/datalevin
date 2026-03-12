from __future__ import annotations

from contextlib import suppress
import time
import uuid

import pytest

from datalevin import connect, new_client
from datalevin.errors import DatalevinJavaError


pytestmark = pytest.mark.usefixtures("require_runtime")

CLIENT_OPTS = {":pool-size": 1, ":time-out": 5000}
SCHEMA = {
    ":name": {
        ":db/valueType": ":db.type/string",
        ":db/unique": ":db.unique/identity",
    }
}


def _matches_permission(permission: dict, act: str, obj: str) -> bool:
    return permission.get(":permission/act") == act and permission.get(":permission/obj") == obj


def _wait_for(predicate, timeout: float = 5.0, interval: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_remote_python_surface_works_against_live_server(live_server) -> None:
    db_name = f"py-remote-{uuid.uuid4().hex}"
    client = new_client(live_server.admin_uri(), opts=CLIENT_OPTS)

    try:
        assert client.disconnected() is False

        client.create_database(db_name, "datalog")
        assert db_name in client.list_databases()

        open_info = client.open_database(db_name, "datalog", schema=SCHEMA, info=True)
        assert isinstance(open_info, dict)
        assert db_name in client.list_databases_in_use()

        with connect(
            live_server.database_uri(db_name),
            schema=SCHEMA,
            opts={":client-opts": CLIENT_OPTS},
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


def test_remote_admin_user_role_and_permission_flow(live_server) -> None:
    suffix = uuid.uuid4().hex
    db_name = f"py-admin-db-{suffix}"
    username = f"py-admin-user-{suffix}"
    password = "secret-one"
    new_password = "secret-two"
    role = f"py-admin-role-{suffix}"
    role_key = f":{role}"
    permission_act = ":datalevin.server/alter"
    permission_obj = ":datalevin.server/database"
    admin_client = new_client(live_server.admin_uri(), opts=CLIENT_OPTS)

    try:
        admin_client.create_database(db_name, "datalog")
        open_info = admin_client.open_database(db_name, "datalog", schema=SCHEMA, info=True)
        assert isinstance(open_info, dict)
        assert db_name in admin_client.list_databases()
        assert db_name in admin_client.list_databases_in_use()

        admin_client.create_user(username, password)
        admin_client.reset_password(username, new_password)
        admin_client.create_role(role)
        admin_client.assign_role(role, username)
        admin_client.grant_permission(role, permission_act, permission_obj, db_name)

        assert username in admin_client.list_users()
        assert role_key in set(admin_client.list_roles())
        assert role_key in set(admin_client.list_user_roles(username))
        assert any(
            _matches_permission(permission, permission_act, permission_obj)
            for permission in admin_client.list_role_permissions(role)
        )
        assert any(
            _matches_permission(permission, permission_act, permission_obj)
            for permission in admin_client.list_user_permissions(username)
        )
        assert username in set(
            admin_client.query_system(
                "[:find [?u ...] :where [?e :user/name ?u]]",
            )
        )
        assert db_name in set(
            admin_client.query_system(
                "[:find [?name ...] :where [?e :database/name ?name]]",
            )
        )

        user_client = new_client(live_server.admin_uri(username, new_password), opts=CLIENT_OPTS)
        try:
            assert user_client.disconnected() is False
            with pytest.raises(DatalevinJavaError):
                user_client.list_databases()

            with connect(
                live_server.database_uri(db_name, username, new_password),
                opts={":client-opts": CLIENT_OPTS},
            ) as conn:
                conn.transact([{":db/id": -1, ":name": "Ada"}])
                assert conn.query("[:find ?name . :where [?e :name ?name]]") == "Ada"
        finally:
            user_client.disconnect()

        admin_client.revoke_permission(role, permission_act, permission_obj, db_name)
        admin_client.withdraw_role(role, username)

        assert role_key not in set(admin_client.list_user_roles(username))
        assert not any(
            _matches_permission(permission, permission_act, permission_obj)
            for permission in admin_client.list_role_permissions(role)
        )
    finally:
        with suppress(Exception):
            admin_client.close_database(db_name)
        with suppress(Exception):
            admin_client.drop_role(role)
        with suppress(Exception):
            admin_client.drop_user(username)
        with suppress(Exception):
            admin_client.drop_database(db_name)
        admin_client.disconnect()


def test_remote_admin_can_disconnect_other_clients(live_server) -> None:
    db_name = f"py-clients-db-{uuid.uuid4().hex}"
    admin_client = new_client(live_server.admin_uri(), opts=CLIENT_OPTS)
    other_client = new_client(live_server.admin_uri(), opts=CLIENT_OPTS)

    try:
        admin_id = admin_client.client_id()
        other_id = other_client.client_id()

        admin_client.create_database(db_name, "datalog")
        admin_client.open_database(db_name, "datalog", schema=SCHEMA)

        clients = admin_client.show_clients()
        assert admin_id in clients
        assert other_id in clients
        assert isinstance(clients[admin_id], dict)
        assert isinstance(clients[other_id], dict)

        admin_client.disconnect_client(other_id)

        assert _wait_for(lambda: other_id not in admin_client.show_clients())
    finally:
        with suppress(Exception):
            admin_client.close_database(db_name)
        with suppress(Exception):
            admin_client.drop_database(db_name)
        with suppress(Exception):
            other_client.disconnect()
        admin_client.disconnect()
