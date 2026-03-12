import { toEdnForm, toJava, toQueryInput } from "./convert.js";
import { _BINDINGS } from "./interop.js";
import { callJavaMethod } from "./jvm.js";
import { ResourceWrapper } from "./resource.js";
import { toJsResult } from "./result.js";

async function queryForm(value) {
  if (typeof value === "string") {
    return value;
  }
  return toEdnForm(value);
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
    return toJsResult(await callJavaMethod(this.rawHandle(), "clientId"));
  }

  async openDatabase(name, dbType, { schema = null, opts = null, info = false } = {}) {
    if (info || schema !== null || opts !== null) {
      return toJsResult(
        await callJavaMethod(
          this.rawHandle(),
          "openDatabase",
          name,
          dbType,
          schema === null ? null : await toJava(schema),
          opts === null ? null : await toJava(opts),
          Boolean(info)
        )
      );
    }

    await callJavaMethod(this.rawHandle(), "openDatabase", name, dbType);
    return null;
  }

  async closeDatabase(name) {
    await callJavaMethod(this.rawHandle(), "closeDatabase", name);
  }

  async createDatabase(name, dbType) {
    await callJavaMethod(this.rawHandle(), "createDatabase", name, dbType);
  }

  async dropDatabase(name) {
    await callJavaMethod(this.rawHandle(), "dropDatabase", name);
  }

  async listDatabases() {
    return toJsResult(await callJavaMethod(this.rawHandle(), "listDatabases"));
  }

  async listDatabasesInUse() {
    return toJsResult(await callJavaMethod(this.rawHandle(), "listDatabasesInUse"));
  }

  async createUser(username, password) {
    await callJavaMethod(this.rawHandle(), "createUser", username, password);
  }

  async dropUser(username) {
    await callJavaMethod(this.rawHandle(), "dropUser", username);
  }

  async resetPassword(username, password) {
    await callJavaMethod(this.rawHandle(), "resetPassword", username, password);
  }

  async listUsers() {
    return toJsResult(await callJavaMethod(this.rawHandle(), "listUsers"));
  }

  async createRole(role) {
    await callJavaMethod(this.rawHandle(), "createRole", role);
  }

  async dropRole(role) {
    await callJavaMethod(this.rawHandle(), "dropRole", role);
  }

  async listRoles() {
    return toJsResult(await callJavaMethod(this.rawHandle(), "listRoles"));
  }

  async assignRole(role, username) {
    await callJavaMethod(this.rawHandle(), "assignRole", role, username);
  }

  async withdrawRole(role, username) {
    await callJavaMethod(this.rawHandle(), "withdrawRole", role, username);
  }

  async listUserRoles(username) {
    return toJsResult(await callJavaMethod(this.rawHandle(), "listUserRoles", username));
  }

  async grantPermission(role, act, obj, tgt = null) {
    await callJavaMethod(
      this.rawHandle(),
      "grantPermission",
      role,
      act,
      obj,
      tgt === null ? null : await toJava(tgt)
    );
  }

  async revokePermission(role, act, obj, tgt = null) {
    await callJavaMethod(
      this.rawHandle(),
      "revokePermission",
      role,
      act,
      obj,
      tgt === null ? null : await toJava(tgt)
    );
  }

  async listRolePermissions(role) {
    return toJsResult(await callJavaMethod(this.rawHandle(), "listRolePermissions", role));
  }

  async listUserPermissions(username) {
    return toJsResult(await callJavaMethod(this.rawHandle(), "listUserPermissions", username));
  }

  async querySystem(query, ...args) {
    const normalizedInputs = [];
    for (const value of args) {
      normalizedInputs.push(await toQueryInput(value));
    }

    if (typeof query === "string") {
      return toJsResult(await callJavaMethod(this.rawHandle(), "querySystem", query, await toJava(normalizedInputs)));
    }
    return toJsResult(
      await callJavaMethod(this.rawHandle(), "querySystemForm", await queryForm(query), await toJava(normalizedInputs))
    );
  }

  async showClients() {
    return toJsResult(await callJavaMethod(this.rawHandle(), "showClients"));
  }

  async disconnectClient(clientId) {
    if (typeof clientId === "string") {
      await callJavaMethod(this.rawHandle(), "disconnectClient", clientId);
      return;
    }
    await callJavaMethod(this.rawHandle(), "disconnectClient", await toJava(clientId));
  }
}
