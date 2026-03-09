package datalevin;

/**
 * Supported remote database kinds for the Java API.
 */
public enum DatabaseType {
    DATALOG("datalog", ":datalog"),
    KV("kv", ":key-value"),
    ENGINE("engine", null);

    private final String wireName;
    private final String createKeyword;

    DatabaseType(String wireName, String createKeyword) {
        this.wireName = wireName;
        this.createKeyword = createKeyword;
    }

    /**
     * Returns the wire representation used by remote open-database operations.
     */
    public String wireName() {
        return wireName;
    }

    /**
     * Returns whether this type can be created with {@code createDatabase}.
     */
    public boolean creatable() {
        return createKeyword != null;
    }

    static Object createArg(DatabaseType type) {
        if (type == null) {
            return null;
        }
        if (!type.creatable()) {
            throw new IllegalArgumentException("Unsupported database type for createDatabase: " + type);
        }
        return ClojureCodec.keyword(type.createKeyword);
    }
}
