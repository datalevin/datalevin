package datalevin;

/**
 * Built-in server permission object kinds.
 */
public enum PermissionObject {
    DATABASE(":datalevin.server/database"),
    USER(":datalevin.server/user"),
    ROLE(":datalevin.server/role"),
    SERVER(":datalevin.server/server");

    private final String keyword;

    PermissionObject(String keyword) {
        this.keyword = keyword;
    }

    /**
     * Returns the underlying Datalevin keyword string.
     */
    public String keyword() {
        return keyword;
    }

    static PermissionObject fromKeywordOrNull(String value) {
        if (value == null) {
            return null;
        }
        for (PermissionObject object : values()) {
            if (object.keyword.equals(value)
                    || object.keyword.substring(1).equals(value)) {
                return object;
            }
        }
        return null;
    }
}
