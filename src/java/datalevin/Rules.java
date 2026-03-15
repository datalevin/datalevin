package datalevin;

import clojure.lang.PersistentVector;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Builder for Datalog rules.
 */
public final class Rules {

    private final List<Object> rules = new ArrayList<>();
    private Object cachedForm;

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
        return Edn.render(buildForm());
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
        if (!(other instanceof Rules that)) {
            return false;
        }
        return Objects.equals(buildForm(), that.buildForm());
    }

    @Override
    public int hashCode() {
        return Objects.hash(buildForm());
    }

    Object asInput() {
        return buildForm();
    }

    private Rules addRule(Object rule) {
        cachedForm = null;
        rules.add(rule);
        return this;
    }

    Object buildForm() {
        if (cachedForm == null) {
            cachedForm = ClojureCodec.toClojure(rules);
        }
        return cachedForm;
    }

    /**
     * Builder for a single rule branch.
     */
    public static final class RuleBuilder {

        private final Rules parent;
        private final Object head;
        private final List<QueryClause> body = new ArrayList<>();

        private RuleBuilder(Rules parent, String name, String... headArgs) {
            this.parent = parent;
            Objects.requireNonNull(name, "name");
            ArrayList<Object> headItems = new ArrayList<>(headArgs.length + 1);
            headItems.add(ClojureCodec.symbol(name));
            for (String arg : headArgs) {
                headItems.add(QueryClause.patternValue(arg));
            }
            this.head = clojure.lang.PersistentList.create(headItems);
        }

        /**
         * Adds a prebuilt clause to the rule body.
         */
        public RuleBuilder where(QueryClause clause) {
            body.add(clause);
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
            ArrayList<Object> rule = new ArrayList<>(body.size() + 1);
            rule.add(head);
            for (QueryClause clause : body) {
                rule.add(clause.form());
            }
            return parent.addRule(PersistentVector.create(rule));
        }
    }
}
