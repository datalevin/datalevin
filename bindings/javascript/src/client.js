import { toEdnForm, toQueryInput } from "./convert.js";
import { _BINDINGS } from "./interop.js";
import { classes } from "./jvm.js";
import { ResourceWrapper } from "./resource.js";
import { toJsResult } from "./result.js";

async function queryForm(value) {
  if (typeof value === "string") {
    return _BINDINGS.readEdn(value);
  }
  return toEdnForm(value);
}

async function clientIdInput(value) {
  if (typeof value === "string") {
    const cls = await classes();
    return cls.uuid.fromStringSync(value);
  }
  return value;
}

export class Client extends ResourceWrapper {
  constructor(handle) {
    super(
      handle,
      (rawHandle) => _BINDINGS.closeClient(rawHandle),
      (rawHandle) => _BINDINGS.clientDisconnected(rawHandle),
      "client"
    );
  }

  async disconnect() {
    await this.close();
  }

  async disconnected() {
    return this.closed();
  }

  async clientId() {
    return toJsResult(await _BINDINGS.clientInvoke("get-id", [this.rawHandle()]));
  }

  async openDatabase(name, dbType, { schema = null, opts = null, info = false } = {}) {
    if (info || schema !== null || opts !== null) {
      return toJsResult(await _BINDINGS.clientInvoke("open-database", [
        this.rawHandle(),
        name,
        dbType,
        schema === null ? null : await _BINDINGS.schema(schema),
        opts === null ? null : await _BINDINGS.options(opts),
        Boolean(info)
      ]));
    }

    await _BINDINGS.clientInvoke("open-database", [this.rawHandle(), name, dbType]);
    return null;
  }

  async closeDatabase(name) {
    await _BINDINGS.clientInvoke("close-database", [this.rawHandle(), name]);
  }

  async createDatabase(name, dbType) {
    await _BINDINGS.clientInvoke("create-database", [
      this.rawHandle(),
      name,
      await _BINDINGS.databaseType(dbType)
    ]);
  }

  async dropDatabase(name) {
    await _BINDINGS.clientInvoke("drop-database", [this.rawHandle(), name]);
  }

  async listDatabases() {
    return toJsResult(await _BINDINGS.clientInvoke("list-databases", [this.rawHandle()]));
  }

  async listDatabasesInUse() {
    return toJsResult(await _BINDINGS.clientInvoke("list-databases-in-use", [this.rawHandle()]));
  }

  async createUser(username, password) {
    await _BINDINGS.clientInvoke("create-user", [this.rawHandle(), username, password]);
  }

  async dropUser(username) {
    await _BINDINGS.clientInvoke("drop-user", [this.rawHandle(), username]);
  }

  async resetPassword(username, password) {
    await _BINDINGS.clientInvoke("reset-password", [this.rawHandle(), username, password]);
  }

  async listUsers() {
    return toJsResult(await _BINDINGS.clientInvoke("list-users", [this.rawHandle()]));
  }

  async createRole(role) {
    await _BINDINGS.clientInvoke("create-role", [this.rawHandle(), await _BINDINGS.role(role)]);
  }

  async dropRole(role) {
    await _BINDINGS.clientInvoke("drop-role", [this.rawHandle(), await _BINDINGS.role(role)]);
  }

  async listRoles() {
    return toJsResult(await _BINDINGS.clientInvoke("list-roles", [this.rawHandle()]));
  }

  async assignRole(role, username) {
    await _BINDINGS.clientInvoke("assign-role", [this.rawHandle(), await _BINDINGS.role(role), username]);
  }

  async withdrawRole(role, username) {
    await _BINDINGS.clientInvoke("withdraw-role", [this.rawHandle(), await _BINDINGS.role(role), username]);
  }

  async listUserRoles(username) {
    return toJsResult(await _BINDINGS.clientInvoke("list-user-roles", [this.rawHandle(), username]));
  }

  async grantPermission(role, act, obj, tgt = null) {
    await _BINDINGS.clientInvoke("grant-permission", [
      this.rawHandle(),
      await _BINDINGS.role(role),
      await _BINDINGS.permissionKeyword(act),
      await _BINDINGS.permissionKeyword(obj),
      await _BINDINGS.permissionTarget(obj, tgt)
    ]);
  }

  async revokePermission(role, act, obj, tgt = null) {
    await _BINDINGS.clientInvoke("revoke-permission", [
      this.rawHandle(),
      await _BINDINGS.role(role),
      await _BINDINGS.permissionKeyword(act),
      await _BINDINGS.permissionKeyword(obj),
      await _BINDINGS.permissionTarget(obj, tgt)
    ]);
  }

  async listRolePermissions(role) {
    return toJsResult(
      await _BINDINGS.clientInvoke("list-role-permissions", [this.rawHandle(), await _BINDINGS.role(role)])
    );
  }

  async listUserPermissions(username) {
    return toJsResult(await _BINDINGS.clientInvoke("list-user-permissions", [this.rawHandle(), username]));
  }

  async querySystem(query, ...args) {
    const normalizedInputs = [];
    for (const value of args) {
      normalizedInputs.push(await toQueryInput(value));
    }
    return toJsResult(
      await _BINDINGS.clientInvoke("query-system", [
        this.rawHandle(),
        await queryForm(query),
        ...normalizedInputs
      ])
    );
  }

  async showClients() {
    return toJsResult(await _BINDINGS.clientInvoke("show-clients", [this.rawHandle()]));
  }

  async disconnectClient(clientId) {
    await _BINDINGS.clientInvoke("disconnect-client", [this.rawHandle(), await clientIdInput(clientId)]);
  }
}
