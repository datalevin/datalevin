package datalevin;

import clojure.java.api.Clojure;
import clojure.lang.ArraySeq;
import clojure.lang.IFn;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Shared JVM interop runtime for Datalevin language bindings.
 *
 * <p>This layer owns namespace loading, cached var lookup, invocation, and
 * exception translation. It does not know about Java wrapper ergonomics or
 * Datalevin-specific data shaping.
 */
final class ClojureRuntime {

    private static final int READ_EDN_CACHE_SIZE = 256;
    private static final int READ_EDN_CACHE_MAX_LENGTH = 4096;
    private static final Object NULL_SENTINEL = new Object();
    private static final ConcurrentHashMap<String, IFn> VARS = new ConcurrentHashMap<>();
    private static final Map<String, Object> READ_EDN_CACHE =
            Collections.synchronizedMap(new LinkedHashMap<String, Object>(READ_EDN_CACHE_SIZE,
                                                                          0.75f,
                                                                          true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Object> eldest) {
                    return size() > READ_EDN_CACHE_SIZE;
                }
            });
    private static final IFn REQUIRE = Clojure.var("clojure.core", "require");

    static {
        require("clojure.edn");
        require("datalevin.core");
        require("datalevin.client");
        require("datalevin.datom");
        require("datalevin.json-api");
        require("datalevin.udf");
    }

    private ClojureRuntime() {
    }

    static Object core(String name) {
        return invoke("datalevin.core", name);
    }

    static Object core(String name, Object arg1) {
        return invoke("datalevin.core", name, arg1);
    }

    static Object core(String name, Object arg1, Object arg2) {
        return invoke("datalevin.core", name, arg1, arg2);
    }

    static Object core(String name, Object arg1, Object arg2, Object arg3) {
        return invoke("datalevin.core", name, arg1, arg2, arg3);
    }

    static Object core(String name, Object arg1, Object arg2, Object arg3, Object arg4) {
        return invoke("datalevin.core", name, arg1, arg2, arg3, arg4);
    }

    static Object core(String name, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        return invoke("datalevin.core", name, arg1, arg2, arg3, arg4, arg5);
    }

    static Object core(String name, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        return invoke("datalevin.core", name, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    static Object core(String name,
                       Object arg1,
                       Object arg2,
                       Object arg3,
                       Object arg4,
                       Object arg5,
                       Object arg6,
                       Object arg7) {
        return invoke("datalevin.core", name, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
    }

    static Object core(String name,
                       Object arg1,
                       Object arg2,
                       Object arg3,
                       Object arg4,
                       Object arg5,
                       Object arg6,
                       Object arg7,
                       Object arg8) {
        return invoke("datalevin.core", name, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
    }

    static Object core(String name, Object... args) {
        return invoke("datalevin.core", name, args);
    }

    static Object client(String name) {
        return invoke("datalevin.client", name);
    }

    static Object client(String name, Object arg1) {
        return invoke("datalevin.client", name, arg1);
    }

    static Object client(String name, Object arg1, Object arg2) {
        return invoke("datalevin.client", name, arg1, arg2);
    }

    static Object client(String name, Object arg1, Object arg2, Object arg3) {
        return invoke("datalevin.client", name, arg1, arg2, arg3);
    }

    static Object client(String name, Object arg1, Object arg2, Object arg3, Object arg4) {
        return invoke("datalevin.client", name, arg1, arg2, arg3, arg4);
    }

    static Object client(String name, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        return invoke("datalevin.client", name, arg1, arg2, arg3, arg4, arg5);
    }

    static Object client(String name,
                         Object arg1,
                         Object arg2,
                         Object arg3,
                         Object arg4,
                         Object arg5,
                         Object arg6) {
        return invoke("datalevin.client", name, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    static Object client(String name, Object... args) {
        return invoke("datalevin.client", name, args);
    }

    static Object jsonApi(String name) {
        return invoke("datalevin.json-api", name);
    }

    static Object jsonApi(String name, Object arg1) {
        return invoke("datalevin.json-api", name, arg1);
    }

    static Object jsonApi(String name, Object arg1, Object arg2) {
        return invoke("datalevin.json-api", name, arg1, arg2);
    }

    static Object jsonApi(String name, Object arg1, Object arg2, Object arg3) {
        return invoke("datalevin.json-api", name, arg1, arg2, arg3);
    }

    static Object jsonApi(String name, Object arg1, Object arg2, Object arg3, Object arg4) {
        return invoke("datalevin.json-api", name, arg1, arg2, arg3, arg4);
    }

    static Object jsonApi(String name, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        return invoke("datalevin.json-api", name, arg1, arg2, arg3, arg4, arg5);
    }

    static Object jsonApi(String name,
                          Object arg1,
                          Object arg2,
                          Object arg3,
                          Object arg4,
                          Object arg5,
                          Object arg6) {
        return invoke("datalevin.json-api", name, arg1, arg2, arg3, arg4, arg5, arg6);
    }

    static Object jsonApi(String name, Object... args) {
        return invoke("datalevin.json-api", name, args);
    }

    static Object datom(String name) {
        return invoke("datalevin.datom", name);
    }

    static Object datom(String name, Object arg1) {
        return invoke("datalevin.datom", name, arg1);
    }

    static Object datom(String name, Object arg1, Object arg2) {
        return invoke("datalevin.datom", name, arg1, arg2);
    }

    static Object datom(String name, Object arg1, Object arg2, Object arg3) {
        return invoke("datalevin.datom", name, arg1, arg2, arg3);
    }

    static Object datom(String name, Object arg1, Object arg2, Object arg3, Object arg4) {
        return invoke("datalevin.datom", name, arg1, arg2, arg3, arg4);
    }

    static Object datom(String name, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        return invoke("datalevin.datom", name, arg1, arg2, arg3, arg4, arg5);
    }

    static Object datom(String name, Object... args) {
        return invoke("datalevin.datom", name, args);
    }

    static Object readEdn(String edn) {
        if (edn.length() > READ_EDN_CACHE_MAX_LENGTH) {
            return invoke("clojure.edn", "read-string", edn);
        }
        Object cached = READ_EDN_CACHE.get(edn);
        if (cached != null) {
            return cached == NULL_SENTINEL ? null : cached;
        }
        Object parsed = invoke("clojure.edn", "read-string", edn);
        READ_EDN_CACHE.put(edn, parsed == null ? NULL_SENTINEL : parsed);
        return parsed;
    }

    static Object invoke(String ns, String name, Object... args) {
        try {
            return invoke(lookup(ns, name), args);
        } catch (RuntimeException e) {
            throw translateException(e);
        }
    }

    static Object invoke(String ns, String name) {
        try {
            return lookup(ns, name).invoke();
        } catch (RuntimeException e) {
            throw translateException(e);
        }
    }

    static Object invoke(String ns, String name, Object arg1) {
        try {
            return lookup(ns, name).invoke(arg1);
        } catch (RuntimeException e) {
            throw translateException(e);
        }
    }

    static Object invoke(String ns, String name, Object arg1, Object arg2) {
        try {
            return lookup(ns, name).invoke(arg1, arg2);
        } catch (RuntimeException e) {
            throw translateException(e);
        }
    }

    static Object invoke(String ns, String name, Object arg1, Object arg2, Object arg3) {
        try {
            return lookup(ns, name).invoke(arg1, arg2, arg3);
        } catch (RuntimeException e) {
            throw translateException(e);
        }
    }

    static Object invoke(String ns, String name, Object arg1, Object arg2, Object arg3, Object arg4) {
        try {
            return lookup(ns, name).invoke(arg1, arg2, arg3, arg4);
        } catch (RuntimeException e) {
            throw translateException(e);
        }
    }

    static Object invoke(String ns, String name, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5) {
        try {
            return lookup(ns, name).invoke(arg1, arg2, arg3, arg4, arg5);
        } catch (RuntimeException e) {
            throw translateException(e);
        }
    }

    static Object invoke(String ns, String name, Object arg1, Object arg2, Object arg3, Object arg4, Object arg5, Object arg6) {
        try {
            return lookup(ns, name).invoke(arg1, arg2, arg3, arg4, arg5, arg6);
        } catch (RuntimeException e) {
            throw translateException(e);
        }
    }

    static Object invoke(String ns,
                         String name,
                         Object arg1,
                         Object arg2,
                         Object arg3,
                         Object arg4,
                         Object arg5,
                         Object arg6,
                         Object arg7) {
        try {
            return lookup(ns, name).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
        } catch (RuntimeException e) {
            throw translateException(e);
        }
    }

    static Object invoke(String ns,
                         String name,
                         Object arg1,
                         Object arg2,
                         Object arg3,
                         Object arg4,
                         Object arg5,
                         Object arg6,
                         Object arg7,
                         Object arg8) {
        try {
            return lookup(ns, name).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
        } catch (RuntimeException e) {
            throw translateException(e);
        }
    }

    private static Object invoke(IFn fn, Object[] args) {
        if (args == null || args.length == 0) {
            return fn.invoke();
        }
        return switch (args.length) {
            case 1 -> fn.invoke(args[0]);
            case 2 -> fn.invoke(args[0], args[1]);
            case 3 -> fn.invoke(args[0], args[1], args[2]);
            case 4 -> fn.invoke(args[0], args[1], args[2], args[3]);
            case 5 -> fn.invoke(args[0], args[1], args[2], args[3], args[4]);
            case 6 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5]);
            case 7 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6]);
            case 8 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
            case 9 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8]);
            case 10 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9]);
            case 11 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10]);
            case 12 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11]);
            case 13 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11], args[12]);
            case 14 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11], args[12], args[13]);
            case 15 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11], args[12], args[13], args[14]);
            case 16 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11], args[12], args[13], args[14], args[15]);
            case 17 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11], args[12], args[13], args[14], args[15], args[16]);
            case 18 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11], args[12], args[13], args[14], args[15], args[16], args[17]);
            case 19 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11], args[12], args[13], args[14], args[15], args[16], args[17], args[18]);
            case 20 -> fn.invoke(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7], args[8], args[9], args[10], args[11], args[12], args[13], args[14], args[15], args[16], args[17], args[18], args[19]);
            default -> fn.applyTo(ArraySeq.create(args));
        };
    }

    private static void require(String ns) {
        REQUIRE.invoke(Clojure.read(ns));
    }

    private static IFn lookup(String ns, String name) {
        return VARS.computeIfAbsent(ns + "/" + name, ignored -> Clojure.var(ns, name));
    }

    private static RuntimeException translateException(RuntimeException e) {
        if (e instanceof DatalevinException de) {
            return de;
        }
        if (e instanceof clojure.lang.ExceptionInfo info) {
            Object data = info.getData();
            String errorType = null;
            if (info.getData() != null) {
                Object raw = info.getData().valAt(ClojureCodec.keyword(":error"));
                if (raw != null) {
                    errorType = String.valueOf(raw);
                }
            }
            return new DatalevinException(info.getMessage(), errorType, data);
        }
        return e;
    }
}
