package datalevin;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Builder for Datalog rules.
 */
public final class Rules {

    private final List<String> rules = new ArrayList<>();

    Rules() {
    }

    /**
     * Starts a new rule definition.
     */
    public RuleBuilder rule(String name, String... headArgs) {
        return new RuleBuilder(this, name, headArgs);
    }

    /**
     * Renders the full rules value to EDN text.
     */
    public String toEdn() {
        StringBuilder builder = new StringBuilder("[");
        for (int i = 0; i < rules.size(); i++) {
            if (i > 0) {
                builder.append(" ");
            }
            builder.append(rules.get(i));
        }
        return builder.append("]").toString();
    }

    @Override
    public String toString() {
        return toEdn();
    }

    EdnLiteral asInput() {
        return new EdnLiteral(toEdn());
    }

    private Rules addRule(String edn) {
        rules.add(edn);
        return this;
    }

    /**
     * Builder for a single rule branch.
     */
    public static final class RuleBuilder {

        private final Rules parent;
        private final String head;
        private final List<String> body = new ArrayList<>();

        private RuleBuilder(Rules parent, String name, String... headArgs) {
            this.parent = parent;
            Objects.requireNonNull(name, "name");
            StringBuilder builder = new StringBuilder("(").append(name);
            for (String arg : headArgs) {
                builder.append(" ").append(arg);
            }
            this.head = builder.append(")").toString();
        }

        /**
         * Adds a prebuilt clause to the rule body.
         */
        public RuleBuilder where(QueryClause clause) {
            body.add(clause.toEdn());
            return this;
        }

        /**
         * Adds a raw EDN clause to the rule body.
         */
        public RuleBuilder whereClause(String clauseEdn) {
            return where(QueryClause.raw(clauseEdn));
        }

        /**
         * Adds a datom clause using the default source.
         */
        public RuleBuilder whereDatom(Object e, Object a, Object v) {
            return where(QueryClause.datom(e, a, v));
        }

        /**
         * Adds a datom clause with an explicit source variable.
         */
        public RuleBuilder whereDatom(Object source, Object e, Object a, Object v) {
            return where(QueryClause.datom(source, e, a, v));
        }

        /**
         * Adds a rule invocation clause.
         */
        public RuleBuilder whereRule(String name, Object... args) {
            return where(QueryClause.rule(name, args));
        }

        /**
         * Adds a predicate clause.
         */
        public RuleBuilder wherePredicate(String fn, Object... args) {
            return where(QueryClause.predicate(fn, args));
        }

        /**
         * Adds a binding clause.
         */
        public RuleBuilder whereBind(String fn, Object binding, Object... args) {
            return where(QueryClause.bind(fn, binding, args));
        }

        /**
         * Adds an {@code or} clause.
         */
        public RuleBuilder whereOr(QueryClause... clauses) {
            return where(QueryClause.or(clauses));
        }

        /**
         * Adds an {@code and} clause.
         */
        public RuleBuilder whereAnd(QueryClause... clauses) {
            return where(QueryClause.and(clauses));
        }

        /**
         * Adds a {@code not} clause.
         */
        public RuleBuilder whereNot(QueryClause... clauses) {
            return where(QueryClause.not(clauses));
        }

        /**
         * Adds an {@code or-join} clause.
         */
        public RuleBuilder whereOrJoin(Object joinVars, QueryClause... clauses) {
            return where(QueryClause.orJoin(joinVars, clauses));
        }

        /**
         * Adds a {@code not-join} clause.
         */
        public RuleBuilder whereNotJoin(Object joinVars, QueryClause... clauses) {
            return where(QueryClause.notJoin(joinVars, clauses));
        }

        /**
         * Finishes the current rule branch and returns the parent rules builder.
         */
        public Rules end() {
            if (body.isEmpty()) {
                throw new IllegalStateException("Rule requires at least one body clause.");
            }
            return parent.addRule("[" + head + " " + String.join(" ", body) + "]");
        }
    }
}
