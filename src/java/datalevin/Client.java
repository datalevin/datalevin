package datalevin;

import java.util.ArrayList;
import java.util.Arrays;
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

    Client(Object client) {
        super(client,
              resource -> ClojureBridge.client("disconnect", resource),
              "client",
              "client");
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
        return isReleased() || ClojureBridge.javaBoolean(ClojureBridge.client("disconnected?", resource()));
    }

    /**
     * Returns the server-assigned client id.
     */
    public UUID clientId() {
        return ClojureBridge.javaUuid(ClojureBridge.client("get-id", resource()));
    }

    /**
     * Opens a database on the remote server.
     */
    public void openDatabase(String dbName, String dbType) {
        ClojureBridge.client("open-database", resource(), dbName, dbType);
    }

    /**
     * Opens a database and returns the resulting database info map.
     */
    public Map<String, Object> openDatabaseInfo(String dbName,
                                                String dbType,
                                                Map<String, ?> schema,
                                                Map<String, ?> opts) {
        return ClojureBridge.javaMapOrNull(
                ClojureBridge.client("open-database",
                                     resource(),
                                     dbName,
                                     dbType,
                                     ClojureBridge.schemaInput(schema),
                                     ClojureBridge.optionsInput(opts),
                                     true)
        );
    }

    /**
     * Closes a remote database handle.
     */
    public void closeDatabase(String dbName) {
        ClojureBridge.client("close-database", resource(), dbName);
    }

    /**
     * Creates a remote database.
     */
    public void createDatabase(String dbName, String dbType) {
        ClojureBridge.client("create-database",
                             resource(),
                             dbName,
                             ClojureBridge.createDatabaseType(dbType));
    }

    /**
     * Drops a remote database.
     */
    public void dropDatabase(String dbName) {
        ClojureBridge.client("drop-database", resource(), dbName);
    }

    /**
     * Lists all databases visible to the client.
     */
    public List<Object> listDatabases() {
        return ClojureBridge.javaList(ClojureBridge.client("list-databases", resource()));
    }

    /**
     * Lists databases currently in use on the server.
     */
    public List<Object> listDatabasesInUse() {
        return ClojureBridge.javaList(ClojureBridge.client("list-databases-in-use", resource()));
    }

    /**
     * Creates a user account.
     */
    public void createUser(String username, String password) {
        ClojureBridge.client("create-user", resource(), username, password);
    }

    /**
     * Drops a user account.
     */
    public void dropUser(String username) {
        ClojureBridge.client("drop-user", resource(), username);
    }

    /**
     * Resets a user's password.
     */
    public void resetPassword(String username, String password) {
        ClojureBridge.client("reset-password", resource(), username, password);
    }

    /**
     * Lists users visible to the client.
     */
    public List<Object> listUsers() {
        return ClojureBridge.javaList(ClojureBridge.client("list-users", resource()));
    }

    /**
     * Creates a role.
     */
    public void createRole(String role) {
        ClojureBridge.client("create-role", resource(), ClojureBridge.roleInput(role));
    }

    /**
     * Drops a role.
     */
    public void dropRole(String role) {
        ClojureBridge.client("drop-role", resource(), ClojureBridge.roleInput(role));
    }

    /**
     * Lists all roles.
     */
    public List<Object> listRoles() {
        return ClojureBridge.javaList(ClojureBridge.client("list-roles", resource()));
    }

    /**
     * Assigns a role to a user.
     */
    public void assignRole(String role, String username) {
        ClojureBridge.client("assign-role",
                             resource(),
                             ClojureBridge.roleInput(role),
                             username);
    }

    /**
     * Withdraws a role from a user.
     */
    public void withdrawRole(String role, String username) {
        ClojureBridge.client("withdraw-role",
                             resource(),
                             ClojureBridge.roleInput(role),
                             username);
    }

    /**
     * Lists roles assigned to a user.
     */
    public List<Object> listUserRoles(String username) {
        return ClojureBridge.javaList(ClojureBridge.client("list-user-roles", resource(), username));
    }

    /**
     * Grants a permission to a role.
     */
    public void grantPermission(String role, String act, String obj, Object tgt) {
        ClojureBridge.client("grant-permission",
                             resource(),
                             ClojureBridge.roleInput(role),
                             ClojureBridge.permissionKeyword(act),
                             ClojureBridge.permissionKeyword(obj),
                             ClojureBridge.permissionTarget(obj, tgt));
    }

    /**
     * Revokes a permission from a role.
     */
    public void revokePermission(String role, String act, String obj, Object tgt) {
        ClojureBridge.client("revoke-permission",
                             resource(),
                             ClojureBridge.roleInput(role),
                             ClojureBridge.permissionKeyword(act),
                             ClojureBridge.permissionKeyword(obj),
                             ClojureBridge.permissionTarget(obj, tgt));
    }

    /**
     * Lists permissions granted to a role.
     */
    public List<Object> listRolePermissions(String role) {
        return ClojureBridge.javaList(
                ClojureBridge.client("list-role-permissions",
                                     resource(),
                                     ClojureBridge.roleInput(role))
        );
    }

    /**
     * Lists effective permissions for a user.
     */
    public List<Object> listUserPermissions(String username) {
        return ClojureBridge.javaList(ClojureBridge.client("list-user-permissions", resource(), username));
    }

    /**
     * Runs a system query expressed as EDN text with positional arguments.
     */
    public Object querySystem(String query, Object... args) {
        return querySystem(query, Arrays.asList(args));
    }

    /**
     * Runs a system query expressed as EDN text with positional arguments.
     */
    public Object querySystem(String query, List<?> args) {
        ArrayList<Object> inputs = new ArrayList<>();
        if (args != null) {
            for (Object arg : args) {
                inputs.add(ClojureBridge.genericInput(arg));
            }
        }
        ArrayList<Object> request = new ArrayList<>();
        request.add(resource());
        request.add(ClojureBridge.queryForm(query));
        request.addAll(inputs);
        return ClojureBridge.toJava(ClojureBridge.client("query-system", request.toArray()));
    }

    /**
     * Shows connected clients on the remote server.
     */
    public Map<Object, Object> showClients() {
        return ClojureBridge.javaAnyMap(ClojureBridge.client("show-clients", resource()));
    }

    /**
     * Disconnects another client by id.
     */
    public void disconnectClient(UUID clientId) {
        ClojureBridge.client("disconnect-client", resource(), clientId);
    }

    /**
     * Escape hatch for calling a client-scoped JSON API operation directly.
     */
    public Object exec(String op, Map<String, ?> args) {
        return execJson(op, args);
    }
}
