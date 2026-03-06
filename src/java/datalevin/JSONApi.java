package datalevin;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

/**
 * Lowest-level Java bridge to the Datalevin JSON API.
 *
 * <p>Most callers should prefer the typed wrappers in this package instead of
 * interacting with raw JSON strings directly.
 */
public final class JSONApi {

    private static final IFn EXEC_FN;

    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("datalevin.json-api"));
        EXEC_FN = Clojure.var("datalevin.json-api", "exec");
    }

    private JSONApi() {
    }

    /**
     * Executes a raw JSON request string and returns the raw JSON response.
     */
    public static String exec(String json) {
        return (String) EXEC_FN.invoke(json);
    }
}
