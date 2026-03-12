package datalevin;

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
              resource -> ClojureRuntime.client("disconnect", resource),
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
        return isReleased() || ClojureCodec.javaBoolean(ClojureRuntime.client("disconnected?", resource()));
    }

    /**
     * Returns the server-assigned client id.
     */
    public UUID clientId() {
        return ClojureCodec.javaUuid(ClojureRuntime.client("get-id", resource()));
    }

    /**
     * Opens a database on the remote server.
     */
    public void openDatabase(String dbName, String dbType) {
        ClojureRuntime.client("open-database", resource(), dbName, dbType);
    }

    /**
     * Opens a database with optional schema/options and optional info output.
     */
    public Map<?, ?> openDatabase(String dbName,
                                  String dbType,
                                  Map<?, ?> schema,
                                  Map<?, ?> opts,
                                  boolean info) {
        return (Map<?, ?>) ClojureRuntime.client("open-database",
                                                resource(),
                                                dbName,
                                                dbType,
                                                DatalevinForms.schemaInput(schema),
                                                DatalevinForms.optionsInput(opts),
                                                info);
    }

    /**
     * Opens a database on the remote server.
     */
    public void openDatabase(String dbName, DatabaseType dbType) {
        ClojureRuntime.client("open-database", resource(), dbName, dbType.wireName());
    }

    /**
     * Opens a database and returns the resulting database info map.
     */
    public Map<?, ?> openDatabaseInfo(String dbName,
                                      String dbType,
                                      Map<?, ?> schema,
                                      Map<?, ?> opts) {
        return (Map<?, ?>) ClojureRuntime.client("open-database",
                                                resource(),
                                                dbName,
                                                dbType,
                                                DatalevinForms.schemaInput(schema),
                                                DatalevinForms.optionsInput(opts),
                                                true);
    }

    /**
     * Opens a database and returns the resulting database info map.
     */
    public Map<?, ?> openDatabaseInfo(String dbName,
                                      DatabaseType dbType,
                                      Map<?, ?> schema,
                                      Map<?, ?> opts) {
        return (Map<?, ?>) ClojureRuntime.client("open-database",
                                                resource(),
                                                dbName,
                                                dbType.wireName(),
                                                DatalevinForms.schemaInput(schema),
                                                DatalevinForms.optionsInput(opts),
                                                true);
    }

    /**
     * Closes a remote database handle.
     */
    public void closeDatabase(String dbName) {
        ClojureRuntime.client("close-database", resource(), dbName);
    }

    /**
     * Creates a remote database.
     */
    public void createDatabase(String dbName, String dbType) {
        ClojureRuntime.client("create-database",
                             resource(),
                             dbName,
                             DatalevinForms.createDatabaseType(dbType));
    }

    /**
     * Creates a remote database.
     */
    public void createDatabase(String dbName, DatabaseType dbType) {
        ClojureRuntime.client("create-database",
                             resource(),
                             dbName,
                             DatalevinForms.createDatabaseType(dbType));
    }

    /**
     * Drops a remote database.
     */
    public void dropDatabase(String dbName) {
        ClojureRuntime.client("drop-database", resource(), dbName);
    }

    /**
     * Lists all databases visible to the client.
     */
    public List<?> listDatabases() {
        return ResultSupport.sequence(ClojureRuntime.client("list-databases", resource()));
    }

    /**
     * Lists databases currently in use on the server.
     */
    public List<?> listDatabasesInUse() {
        return ResultSupport.sequence(ClojureRuntime.client("list-databases-in-use", resource()));
    }

    /**
     * Creates a user account.
     */
    public void createUser(String username, String password) {
        ClojureRuntime.client("create-user", resource(), username, password);
    }

    /**
     * Drops a user account.
     */
    public void dropUser(String username) {
        ClojureRuntime.client("drop-user", resource(), username);
    }

    /**
     * Resets a user's password.
     */
    public void resetPassword(String username, String password) {
        ClojureRuntime.client("reset-password", resource(), username, password);
    }

    /**
     * Lists users visible to the client.
     */
    public List<?> listUsers() {
        return ResultSupport.sequence(ClojureRuntime.client("list-users", resource()));
    }

    /**
     * Creates a role.
     */
    public void createRole(String role) {
        ClojureRuntime.client("create-role", resource(), DatalevinForms.roleInput(role));
    }

    /**
     * Drops a role.
     */
    public void dropRole(String role) {
        ClojureRuntime.client("drop-role", resource(), DatalevinForms.roleInput(role));
    }

    /**
     * Lists all roles.
     */
    public List<?> listRoles() {
        return ResultSupport.sequence(ClojureRuntime.client("list-roles", resource()));
    }

    /**
     * Assigns a role to a user.
     */
    public void assignRole(String role, String username) {
        ClojureRuntime.client("assign-role",
                             resource(),
                             DatalevinForms.roleInput(role),
                             username);
    }

    /**
     * Withdraws a role from a user.
     */
    public void withdrawRole(String role, String username) {
        ClojureRuntime.client("withdraw-role",
                             resource(),
                             DatalevinForms.roleInput(role),
                             username);
    }

    /**
     * Lists roles assigned to a user.
     */
    public List<?> listUserRoles(String username) {
        return ResultSupport.sequence(ClojureRuntime.client("list-user-roles", resource(), username));
    }

    /**
     * Grants a permission to a role.
     */
    public void grantPermission(String role, String act, String obj, Object tgt) {
        ClojureRuntime.client("grant-permission",
                             resource(),
                             DatalevinForms.roleInput(role),
                             DatalevinForms.permissionKeyword(act),
                             DatalevinForms.permissionKeyword(obj),
                             DatalevinForms.permissionTarget(obj, tgt));
    }

    /**
     * Grants a built-in permission to a role.
     */
    public void grantPermission(String role,
                                PermissionAction act,
                                PermissionObject obj,
                                Object tgt) {
        ClojureRuntime.client("grant-permission",
                             resource(),
                             DatalevinForms.roleInput(role),
                             DatalevinForms.permissionKeyword(act.keyword()),
                             DatalevinForms.permissionKeyword(obj.keyword()),
                             DatalevinForms.permissionTarget(obj.keyword(), tgt));
    }

    /**
     * Revokes a permission from a role.
     */
    public void revokePermission(String role, String act, String obj, Object tgt) {
        ClojureRuntime.client("revoke-permission",
                             resource(),
                             DatalevinForms.roleInput(role),
                             DatalevinForms.permissionKeyword(act),
                             DatalevinForms.permissionKeyword(obj),
                             DatalevinForms.permissionTarget(obj, tgt));
    }

    /**
     * Revokes a built-in permission from a role.
     */
    public void revokePermission(String role,
                                 PermissionAction act,
                                 PermissionObject obj,
                                 Object tgt) {
        ClojureRuntime.client("revoke-permission",
                             resource(),
                             DatalevinForms.roleInput(role),
                             DatalevinForms.permissionKeyword(act.keyword()),
                             DatalevinForms.permissionKeyword(obj.keyword()),
                             DatalevinForms.permissionTarget(obj.keyword(), tgt));
    }

    /**
     * Lists permissions granted to a role.
     */
    public List<?> listRolePermissions(String role) {
        return ResultSupport.sequence(ClojureRuntime.client("list-role-permissions",
                                                           resource(),
                                                           DatalevinForms.roleInput(role)));
    }

    /**
     * Lists effective permissions for a user.
     */
    public List<?> listUserPermissions(String username) {
        return ResultSupport.sequence(ClojureRuntime.client("list-user-permissions", resource(), username));
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
        int argCount = args == null ? 0 : args.size();
        Object[] request = new Object[argCount + 2];
        request[0] = resource();
        request[1] = DatalevinForms.queryForm(query);
        if (args != null) {
            for (int i = 0; i < args.size(); i++) {
                request[i + 2] = ClojureCodec.runtimeInput(args.get(i));
            }
        }
        return ClojureRuntime.client("query-system", request);
    }

    /**
     * Runs a system query expressed as a raw EDN-like form with positional
     * arguments.
     */
    public Object querySystemForm(Object queryForm, List<?> args) {
        int argCount = args == null ? 0 : args.size();
        Object[] request = new Object[argCount + 2];
        request[0] = resource();
        request[1] = DatalevinForms.queryFormInput(queryForm);
        if (args != null) {
            for (int i = 0; i < args.size(); i++) {
                request[i + 2] = ClojureCodec.runtimeInput(args.get(i));
            }
        }
        return ClojureRuntime.client("query-system", request);
    }

    /**
     * Shows connected clients on the remote server.
     */
    public Map<?, ?> showClients() {
        return (Map<?, ?>) ClojureRuntime.client("show-clients", resource());
    }

    /**
     * Disconnects another client by id.
     */
    public void disconnectClient(UUID clientId) {
        ClojureRuntime.client("disconnect-client", resource(), clientId);
    }

    /**
     * Disconnects another client by id.
     */
    public void disconnectClient(String clientId) {
        disconnectClient(UUID.fromString(clientId));
    }

    /**
     * Escape hatch for calling a client-scoped JSON API operation directly.
     */
    public Object exec(String op, Map<String, ?> args) {
        return execJson(op, args);
    }
}
