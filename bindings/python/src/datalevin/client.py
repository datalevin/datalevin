"""High-level Python wrapper for Datalevin remote admin clients."""

from __future__ import annotations

from ._convert import to_python
from ._interop import _BINDINGS
from ._resource import ResourceWrapper


def _query_form(value):
    if isinstance(value, str):
        return _BINDINGS.read_edn(value)
    return value


class Client(ResourceWrapper):
    """Thin Python wrapper over a raw Datalevin remote client handle."""

    def __init__(self, handle) -> None:
        super().__init__(handle, _BINDINGS.close_client, _BINDINGS.client_disconnected, "client")

    def disconnect(self) -> None:
        self.close()

    def disconnected(self) -> bool:
        return self.closed()

    def client_id(self):
        return to_python(_BINDINGS.client_invoke("get-id", [self.raw_handle()]))

    def open_database(self, name, db_type, schema=None, opts=None, info=False):
        if info or schema is not None or opts is not None:
            args = [
                self.raw_handle(),
                name,
                db_type,
                None if schema is None else _BINDINGS.schema(schema),
                None if opts is None else _BINDINGS.options(opts),
                bool(info),
            ]
            return to_python(_BINDINGS.client_invoke("open-database", args))
        _BINDINGS.client_invoke("open-database", [self.raw_handle(), name, db_type])
        return None

    def close_database(self, name) -> None:
        _BINDINGS.client_invoke("close-database", [self.raw_handle(), name])

    def create_database(self, name, db_type) -> None:
        _BINDINGS.client_invoke(
            "create-database",
            [self.raw_handle(), name, _BINDINGS.database_type(db_type)],
        )

    def drop_database(self, name) -> None:
        _BINDINGS.client_invoke("drop-database", [self.raw_handle(), name])

    def list_databases(self):
        return to_python(_BINDINGS.client_invoke("list-databases", [self.raw_handle()]))

    def list_databases_in_use(self):
        return to_python(_BINDINGS.client_invoke("list-databases-in-use", [self.raw_handle()]))

    def create_user(self, username, password) -> None:
        _BINDINGS.client_invoke("create-user", [self.raw_handle(), username, password])

    def drop_user(self, username) -> None:
        _BINDINGS.client_invoke("drop-user", [self.raw_handle(), username])

    def reset_password(self, username, password) -> None:
        _BINDINGS.client_invoke("reset-password", [self.raw_handle(), username, password])

    def list_users(self):
        return to_python(_BINDINGS.client_invoke("list-users", [self.raw_handle()]))

    def create_role(self, role) -> None:
        _BINDINGS.client_invoke("create-role", [self.raw_handle(), _BINDINGS.role(role)])

    def drop_role(self, role) -> None:
        _BINDINGS.client_invoke("drop-role", [self.raw_handle(), _BINDINGS.role(role)])

    def list_roles(self):
        return to_python(_BINDINGS.client_invoke("list-roles", [self.raw_handle()]))

    def assign_role(self, role, username) -> None:
        _BINDINGS.client_invoke(
            "assign-role",
            [self.raw_handle(), _BINDINGS.role(role), username],
        )

    def withdraw_role(self, role, username) -> None:
        _BINDINGS.client_invoke(
            "withdraw-role",
            [self.raw_handle(), _BINDINGS.role(role), username],
        )

    def list_user_roles(self, username):
        return to_python(_BINDINGS.client_invoke("list-user-roles", [self.raw_handle(), username]))

    def grant_permission(self, role, act, obj, tgt=None) -> None:
        _BINDINGS.client_invoke(
            "grant-permission",
            [
                self.raw_handle(),
                _BINDINGS.role(role),
                _BINDINGS.permission_keyword(act),
                _BINDINGS.permission_keyword(obj),
                _BINDINGS.permission_target(obj, tgt),
            ],
        )

    def revoke_permission(self, role, act, obj, tgt=None) -> None:
        _BINDINGS.client_invoke(
            "revoke-permission",
            [
                self.raw_handle(),
                _BINDINGS.role(role),
                _BINDINGS.permission_keyword(act),
                _BINDINGS.permission_keyword(obj),
                _BINDINGS.permission_target(obj, tgt),
            ],
        )

    def list_role_permissions(self, role):
        return to_python(
            _BINDINGS.client_invoke("list-role-permissions", [self.raw_handle(), _BINDINGS.role(role)])
        )

    def list_user_permissions(self, username):
        return to_python(_BINDINGS.client_invoke("list-user-permissions", [self.raw_handle(), username]))

    def query_system(self, query, *args):
        return to_python(
            _BINDINGS.client_invoke(
                "query-system",
                [self.raw_handle(), _query_form(query), *args],
            )
        )

    def show_clients(self):
        return to_python(_BINDINGS.client_invoke("show-clients", [self.raw_handle()]))

    def disconnect_client(self, client_id) -> None:
        _BINDINGS.client_invoke("disconnect-client", [self.raw_handle(), client_id])
