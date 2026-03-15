package datalevin;

/**
 * Lowest-level Java bridge to the Datalevin JSON API.
 *
 * <p>Most callers should prefer the typed wrappers in this package instead of
 * interacting with raw JSON strings directly.
 */
public final class JSONApi {
    private JSONApi() {
    }

    /**
     * Executes a raw JSON request string and returns the raw JSON response.
     */
    public static String exec(String json) {
        return (String) ClojureRuntime.jsonApi("exec", json);
    }
}
