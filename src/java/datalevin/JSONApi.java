package datalevin;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public final class JSONApi {

    private static final IFn EXEC_FN;

    static {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("datalevin.json-api"));
        EXEC_FN = Clojure.var("datalevin.json-api", "exec");
    }

    private JSONApi() {
    }

    public static String exec(String json) {
        return (String) EXEC_FN.invoke(json);
    }
}
