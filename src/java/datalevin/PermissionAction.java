package datalevin;

/**
 * Built-in server permission actions.
 */
public enum PermissionAction {
    VIEW(":datalevin.server/view"),
    ALTER(":datalevin.server/alter"),
    CREATE(":datalevin.server/create"),
    CONTROL(":datalevin.server/control");

    private final String keyword;

    PermissionAction(String keyword) {
        this.keyword = keyword;
    }

    /**
     * Returns the underlying Datalevin keyword string.
     */
    public String keyword() {
        return keyword;
    }

    static PermissionAction fromKeywordOrNull(String value) {
        if (value == null) {
            return null;
        }
        for (PermissionAction action : values()) {
            if (action.keyword.equals(value)
                    || action.keyword.substring(1).equals(value)) {
                return action;
            }
        }
        return null;
    }
}
