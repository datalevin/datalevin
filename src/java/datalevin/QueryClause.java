package datalevin;

import java.util.Arrays;
import java.util.Objects;

/**
 * Static factory for reusable query clauses.
 *
 * <p>Use this class to build nested clauses such as {@code or}, {@code not},
 * and rule invocations without writing raw EDN strings.
 */
public final class QueryClause {

    private final String edn;

    private QueryClause(String edn) {
        this.edn = edn;
    }

    /**
     * Wraps a raw EDN clause.
     */
    public static QueryClause raw(String clauseEdn) {
        Objects.requireNonNull(clauseEdn, "clauseEdn");
        return new QueryClause(clauseEdn);
    }

    /**
     * Creates a datom clause using the default source.
     */
    public static QueryClause datom(Object e, Object a, Object v) {
        return new QueryClause("[" + Edn.renderQuery(e) + " "
                + Edn.renderAttribute(a) + " "
                + Edn.renderQuery(v) + "]");
    }

    /**
     * Creates a datom clause with an explicit source variable.
     */
    public static QueryClause datom(Object source, Object e, Object a, Object v) {
        return new QueryClause("[" + Edn.renderPattern(source) + " "
                + Edn.renderQuery(e) + " "
                + Edn.renderAttribute(a) + " "
                + Edn.renderQuery(v) + "]");
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
        return new QueryClause("[" + renderCall(fn, args) + " " + Edn.renderPattern(binding) + "]");
    }

    /**
     * Creates a rule invocation clause.
     */
    public static QueryClause rule(String name, Object... args) {
        return new QueryClause(renderCall(name, args));
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

    String toEdn() {
        return edn;
    }

    @Override
    public String toString() {
        return edn;
    }

    private static QueryClause bracketedCall(String fn, Object... args) {
        return new QueryClause("[" + renderCall(fn, args) + "]");
    }

    private static QueryClause nested(String op, QueryClause... clauses) {
        requireClauses(op, clauses);
        StringBuilder builder = new StringBuilder("(").append(op);
        for (QueryClause clause : clauses) {
            builder.append(" ").append(clause.toEdn());
        }
        return new QueryClause(builder.append(")").toString());
    }

    private static QueryClause joined(String op, Object joinVars, QueryClause... clauses) {
        requireClauses(op, clauses);
        return new QueryClause("(" + op + " " + Edn.renderPattern(joinVars) + " "
                + joinClauses(clauses) + ")");
    }

    private static void requireClauses(String op, QueryClause[] clauses) {
        Objects.requireNonNull(clauses, "clauses");
        if (clauses.length == 0) {
            throw new IllegalArgumentException(op + " requires at least one clause.");
        }
    }

    private static String renderCall(String fn, Object... args) {
        Objects.requireNonNull(fn, "fn");
        StringBuilder builder = new StringBuilder("(").append(fn);
        for (Object arg : args) {
            builder.append(" ").append(Edn.renderQuery(arg));
        }
        return builder.append(")").toString();
    }

    private static String joinClauses(QueryClause[] clauses) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < clauses.length; i++) {
            if (i > 0) {
                builder.append(" ");
            }
            builder.append(clauses[i].toEdn());
        }
        return builder.toString();
    }
}
