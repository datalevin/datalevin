package datalevin;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Handle for remote Datalevin server administration.
 *
 * <p>Use instances with try-with-resources when you own the handle lifecycle.
 * This surface covers database, user, role, and permission management.
 */
public final class Client extends HandleResource {

    Client(String handle) {
        super(handle, "close-client", "client");
    }

    /**
     * Closes the client connection.
     */
    public void disconnect() {
        close();
    }

    /**
     * Returns whether this client has been disconnected.
     */
    public boolean disconnected() {
        return isReleased() || JsonBridge.asBoolean(call("disconnected?"));
    }

    /**
     * Returns the server-assigned client id.
     */
    public UUID clientId() {
        return JsonBridge.asUuid(call("client-id"));
    }

    /**
     * Opens a database on the remote server.
     */
    public void openDatabase(String dbName, String dbType) {
        call("open-database", Datalevin.mapOf("db-name", dbName, "db-type", dbType));
    }

    /**
     * Opens a database and returns the resulting database info map.
     */
    public Map<String, Object> openDatabaseInfo(String dbName,
                                                String dbType,
                                                Map<String, ?> schema,
                                                Map<String, ?> opts) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf(
                "db-name", dbName,
                "db-type", dbType,
                "return-db-info?", true
        );
        if (schema != null) {
            args.put("schema", schema);
        }
        if (opts != null) {
            args.put("opts", opts);
        }
        return JsonBridge.asMapOrNull(call("open-database", args));
    }

    /**
     * Closes a remote database handle.
     */
    public void closeDatabase(String dbName) {
        call("close-database", Datalevin.mapOf("db-name", dbName));
    }

    /**
     * Creates a remote database.
     */
    public void createDatabase(String dbName, String dbType) {
        call("create-database", Datalevin.mapOf("db-name", dbName, "db-type", dbType));
    }

    /**
     * Drops a remote database.
     */
    public void dropDatabase(String dbName) {
        call("drop-database", Datalevin.mapOf("db-name", dbName));
    }

    /**
     * Lists all databases visible to the client.
     */
    public List<Object> listDatabases() {
        return JsonBridge.asList(call("list-databases"));
    }

    /**
     * Lists databases currently in use on the server.
     */
    public List<Object> listDatabasesInUse() {
        return JsonBridge.asList(call("list-databases-in-use"));
    }

    /**
     * Creates a user account.
     */
    public void createUser(String username, String password) {
        call("create-user", Datalevin.mapOf("username", username, "password", password));
    }

    /**
     * Drops a user account.
     */
    public void dropUser(String username) {
        call("drop-user", Datalevin.mapOf("username", username));
    }

    /**
     * Resets a user's password.
     */
    public void resetPassword(String username, String password) {
        call("reset-password", Datalevin.mapOf("username", username, "password", password));
    }

    /**
     * Lists users visible to the client.
     */
    public List<Object> listUsers() {
        return JsonBridge.asList(call("list-users"));
    }

    /**
     * Creates a role.
     */
    public void createRole(String role) {
        call("create-role", Datalevin.mapOf("role", role));
    }

    /**
     * Drops a role.
     */
    public void dropRole(String role) {
        call("drop-role", Datalevin.mapOf("role", role));
    }

    /**
     * Lists all roles.
     */
    public List<Object> listRoles() {
        return JsonBridge.asList(call("list-roles"));
    }

    /**
     * Assigns a role to a user.
     */
    public void assignRole(String role, String username) {
        call("assign-role", Datalevin.mapOf("role", role, "username", username));
    }

    /**
     * Withdraws a role from a user.
     */
    public void withdrawRole(String role, String username) {
        call("withdraw-role", Datalevin.mapOf("role", role, "username", username));
    }

    /**
     * Lists roles assigned to a user.
     */
    public List<Object> listUserRoles(String username) {
        return JsonBridge.asList(call("list-user-roles", Datalevin.mapOf("username", username)));
    }

    /**
     * Grants a permission to a role.
     */
    public void grantPermission(String role, String act, String obj, Object tgt) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("role", role, "act", act, "obj", obj);
        args.put("tgt", tgt);
        call("grant-permission", args);
    }

    /**
     * Revokes a permission from a role.
     */
    public void revokePermission(String role, String act, String obj, Object tgt) {
        LinkedHashMap<String, Object> args = Datalevin.mapOf("role", role, "act", act, "obj", obj);
        args.put("tgt", tgt);
        call("revoke-permission", args);
    }

    /**
     * Lists permissions granted to a role.
     */
    public List<Object> listRolePermissions(String role) {
        return JsonBridge.asList(call("list-role-permissions", Datalevin.mapOf("role", role)));
    }

    /**
     * Lists effective permissions for a user.
     */
    public List<Object> listUserPermissions(String username) {
        return JsonBridge.asList(call("list-user-permissions", Datalevin.mapOf("username", username)));
    }

    /**
     * Runs a system query expressed as EDN text with positional arguments.
     */
    public Object querySystem(String query, Object... args) {
        LinkedHashMap<String, Object> request = Datalevin.mapOf("query", query);
        if (args.length > 0) {
            request.put("args", Datalevin.listOf(args));
        }
        return call("query-system", request);
    }

    /**
     * Runs a system query expressed as EDN text with positional arguments.
     */
    public Object querySystem(String query, List<?> args) {
        LinkedHashMap<String, Object> request = Datalevin.mapOf("query", query);
        if (args != null && !args.isEmpty()) {
            request.put("args", args);
        }
        return call("query-system", request);
    }

    /**
     * Shows connected clients on the remote server.
     */
    public List<Object> showClients() {
        return JsonBridge.asList(call("show-clients"));
    }

    /**
     * Disconnects another client by id.
     */
    public void disconnectClient(UUID clientId) {
        call("disconnect-client", Datalevin.mapOf("client-id", clientId));
    }

    /**
     * Escape hatch for calling a client-scoped JSON API operation directly.
     */
    public Object exec(String op, Map<String, ?> args) {
        return super.call(op, args);
    }
}
