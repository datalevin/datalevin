package datalevin;

import clojure.lang.Keyword;
import clojure.lang.PersistentList;
import clojure.lang.PersistentVector;
import clojure.lang.Symbol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

/**
 * Static factory for reusable query clauses.
 *
 * <p>Use this class to build nested clauses such as {@code or}, {@code not},
 * and rule invocations without writing raw EDN strings.
 */
public final class QueryClause {

    private final Object form;
    private final boolean requiresDb;

    private QueryClause(Object form, boolean requiresDb) {
        this.form = form;
        this.requiresDb = requiresDb;
    }

    /**
     * Wraps a raw EDN clause.
     */
    public static QueryClause raw(String clauseEdn) {
        Objects.requireNonNull(clauseEdn, "clauseEdn");
        return new QueryClause(ClojureRuntime.readEdn(clauseEdn), true);
    }

    /**
     * Creates a datom clause using the default source.
     */
    public static QueryClause datom(Object e, Object a, Object v) {
        return new QueryClause(vectorForm(queryValue(e), attributeValue(a), queryValue(v)), true);
    }

    /**
     * Creates a datom clause with an explicit source variable.
     */
    public static QueryClause datom(Object source, Object e, Object a, Object v) {
        return new QueryClause(vectorForm(patternValue(source),
                                          queryValue(e),
                                          attributeValue(a),
                                          queryValue(v)),
                               true);
    }

    /**
     * Creates a predicate clause such as {@code [(> ?x 10)]}.
     */
    public static QueryClause predicate(String fn, Object... args) {
        return bracketedCall(fn, args);
    }

    /**
     * Creates a binding clause such as {@code [(ground 42) ?x]}.
     */
    public static QueryClause bind(String fn, Object binding, Object... args) {
        return new QueryClause(vectorForm(callForm(fn, args), patternValue(binding)), false);
    }

    /**
     * Creates a rule invocation clause.
     */
    public static QueryClause rule(String name, Object... args) {
        return new QueryClause(callForm(name, args), true);
    }

    /**
     * Creates an {@code and} clause.
     */
    public static QueryClause and(QueryClause... clauses) {
        return nested("and", clauses);
    }

    /**
     * Creates an {@code or} clause.
     */
    public static QueryClause or(QueryClause... clauses) {
        return nested("or", clauses);
    }

    /**
     * Creates a {@code not} clause.
     */
    public static QueryClause not(QueryClause... clauses) {
        return nested("not", clauses);
    }

    /**
     * Creates an {@code or-join} clause.
     */
    public static QueryClause orJoin(Object joinVars, QueryClause... clauses) {
        return joined("or-join", joinVars, clauses);
    }

    /**
     * Creates a {@code not-join} clause.
     */
    public static QueryClause notJoin(Object joinVars, QueryClause... clauses) {
        return joined("not-join", joinVars, clauses);
    }

    /**
     * Helper for creating join-variable vectors.
     */
    public static Object vars(Object... vars) {
        return Arrays.asList(vars);
    }

    Object form() {
        return form;
    }

    boolean requiresDb() {
        return requiresDb;
    }

    String toEdn() {
        return Edn.render(form);
    }

    @Override
    public String toString() {
        return toEdn();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof QueryClause that)) {
            return false;
        }
        return requiresDb == that.requiresDb && Objects.equals(form, that.form);
    }

    @Override
    public int hashCode() {
        return Objects.hash(form, requiresDb);
    }

    private static QueryClause bracketedCall(String fn, Object... args) {
        return new QueryClause(vectorForm(callForm(fn, args)), false);
    }

    private static QueryClause nested(String op, QueryClause... clauses) {
        requireClauses(op, clauses);
        ArrayList<Object> items = new ArrayList<>(clauses.length + 1);
        items.add(ClojureCodec.symbol(op));
        for (QueryClause clause : clauses) {
            items.add(clause.form());
        }
        return new QueryClause(listForm(items),
                               Arrays.stream(clauses).anyMatch(QueryClause::requiresDb));
    }

    private static QueryClause joined(String op, Object joinVars, QueryClause... clauses) {
        requireClauses(op, clauses);
        ArrayList<Object> items = new ArrayList<>(clauses.length + 2);
        items.add(ClojureCodec.symbol(op));
        items.add(patternValue(joinVars));
        for (QueryClause clause : clauses) {
            items.add(clause.form());
        }
        return new QueryClause(listForm(items), true);
    }

    private static void requireClauses(String op, QueryClause[] clauses) {
        Objects.requireNonNull(clauses, "clauses");
        if (clauses.length == 0) {
            throw new IllegalArgumentException(op + " requires at least one clause.");
        }
    }

    private static Object callForm(String fn, Object... args) {
        Objects.requireNonNull(fn, "fn");
        ArrayList<Object> items = new ArrayList<>(args.length + 1);
        items.add(ClojureCodec.symbol(fn));
        for (Object arg : args) {
            items.add(queryValue(arg));
        }
        return listForm(items);
    }

    static Object queryValue(Object value) {
        if (value instanceof Keyword
                || value instanceof Symbol) {
            return value;
        }

        if (value instanceof EdnLiteral literal) {
            return ClojureRuntime.readEdn(literal.value());
        }

        if (value instanceof String s) {
            return new LiteralString(s);
        }

        if (value instanceof Collection<?> collection) {
            ArrayList<Object> items = new ArrayList<>(collection.size());
            for (Object item : collection) {
                items.add(queryValue(item));
            }
            return vectorForm(items);
        }

        if (value instanceof Object[] array) {
            ArrayList<Object> items = new ArrayList<>(array.length);
            for (Object item : array) {
                items.add(queryValue(item));
            }
            return vectorForm(items);
        }

        return value;
    }

    static Object patternValue(Object value) {
        if (value instanceof String s && isPatternToken(s)) {
            return ClojureCodec.symbol(s);
        }
        if (value instanceof Collection<?> collection) {
            ArrayList<Object> items = new ArrayList<>(collection.size());
            for (Object item : collection) {
                items.add(patternValue(item));
            }
            return vectorForm(items);
        }
        if (value instanceof Object[] array) {
            ArrayList<Object> items = new ArrayList<>(array.length);
            for (Object item : array) {
                items.add(patternValue(item));
            }
            return vectorForm(items);
        }
        return queryValue(value);
    }

    static Object attributeValue(Object value) {
        if (value instanceof String s) {
            return ClojureCodec.keyword(s);
        }
        return queryValue(value);
    }

    private static Object vectorForm(Object... items) {
        return PersistentVector.create(Arrays.asList(items));
    }

    private static Object vectorForm(Collection<?> items) {
        return PersistentVector.create(items);
    }

    private static Object listForm(Collection<?> items) {
        return PersistentList.create(new ArrayList<>(items));
    }

    private static boolean isPatternToken(String value) {
        return value.startsWith("?")
                || value.startsWith("$")
                || value.startsWith("%")
                || "_".equals(value)
                || ".".equals(value)
                || "...".equals(value)
                || "*".equals(value);
    }
}
