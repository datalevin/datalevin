package datalevin;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for Datalevin Datalog queries.
 *
 * <p>This builder emits EDN query text and can attach a {@link Rules} value as
 * the implicit {@code %} input when used through {@link Connection}.
 */
public final class DatalogQuery {

    private final List<String> find = new ArrayList<>();
    private final List<String> with = new ArrayList<>();
    private final List<String> in = new ArrayList<>();
    private final List<String> where = new ArrayList<>();
    private Rules rules;

    DatalogQuery() {
    }

    /**
     * Adds raw {@code :find} terms.
     */
    public DatalogQuery find(String... terms) {
        for (String term : terms) {
            find.add(term);
        }
        return this;
    }

    /**
     * Declares a scalar find result using the {@code .} find spec.
     */
    public DatalogQuery findScalar(String term) {
        find.add(term);
        find.add(".");
        return this;
    }

    /**
     * Declares a collection find result using the {@code [?x ...]} find spec.
     */
    public DatalogQuery findAll(String variable) {
        find.add("[" + variable + " ...]");
        return this;
    }

    /**
     * Declares a tuple find result using the {@code [?x ?y]} find spec.
     */
    public DatalogQuery findTuple(String... terms) {
        find.add("[" + String.join(" ", terms) + "]");
        return this;
    }

    /**
     * Adds raw {@code :in} inputs.
     */
    public DatalogQuery in(String... inputs) {
        for (String input : inputs) {
            in.add(input);
        }
        return this;
    }

    /**
     * Adds raw {@code :with} variables.
     */
    public DatalogQuery with(String... vars) {
        for (String var : vars) {
            with.add(var);
        }
        return this;
    }

    /**
     * Attaches rules to this query.
     */
    public DatalogQuery rules(Rules rules) {
        this.rules = rules;
        return this;
    }

    /**
     * Adds a prebuilt clause.
     */
    public DatalogQuery where(QueryClause clause) {
        where.add(clause.toEdn());
        return this;
    }

    /**
     * Adds a raw EDN clause.
     */
    public DatalogQuery whereClause(String clauseEdn) {
        return where(QueryClause.raw(clauseEdn));
    }

    /**
     * Adds a datom clause using the default source.
     */
    public DatalogQuery whereDatom(Object e, Object a, Object v) {
        return where(QueryClause.datom(e, a, v));
    }

    /**
     * Adds a datom clause with an explicit source variable.
     */
    public DatalogQuery whereDatom(Object source, Object e, Object a, Object v) {
        return where(QueryClause.datom(source, e, a, v));
    }

    /**
     * Adds a rule invocation clause.
     */
    public DatalogQuery whereRule(String name, Object... args) {
        return where(QueryClause.rule(name, args));
    }

    /**
     * Adds a predicate clause such as {@code [(>= ?age 18)]}.
     */
    public DatalogQuery wherePredicate(String fn, Object... args) {
        return where(QueryClause.predicate(fn, args));
    }

    /**
     * Adds a binding clause such as {@code [(ground 42) ?x]}.
     */
    public DatalogQuery whereBind(String fn, Object binding, Object... args) {
        return where(QueryClause.bind(fn, binding, args));
    }

    /**
     * Adds an {@code and} clause.
     */
    public DatalogQuery whereAnd(QueryClause... clauses) {
        return where(QueryClause.and(clauses));
    }

    /**
     * Adds an {@code or} clause.
     */
    public DatalogQuery whereOr(QueryClause... clauses) {
        return where(QueryClause.or(clauses));
    }

    /**
     * Adds a {@code not} clause.
     */
    public DatalogQuery whereNot(QueryClause... clauses) {
        return where(QueryClause.not(clauses));
    }

    /**
     * Adds an {@code or-join} clause.
     */
    public DatalogQuery whereOrJoin(Object joinVars, QueryClause... clauses) {
        return where(QueryClause.orJoin(joinVars, clauses));
    }

    /**
     * Adds a {@code not-join} clause.
     */
    public DatalogQuery whereNotJoin(Object joinVars, QueryClause... clauses) {
        return where(QueryClause.notJoin(joinVars, clauses));
    }

    /**
     * Renders this query to EDN text.
     */
    public String toEdn() {
        if (find.isEmpty()) {
            throw new IllegalStateException("DatalogQuery requires at least one :find term.");
        }
        if (where.isEmpty()) {
            throw new IllegalStateException("DatalogQuery requires at least one :where clause.");
        }

        StringBuilder builder = new StringBuilder("[:find ");
        builder.append(String.join(" ", find));
        if (!with.isEmpty()) {
            builder.append(" :with ").append(String.join(" ", with));
        }
        List<String> inputs = renderedInputs();
        if (!inputs.isEmpty()) {
            builder.append(" :in ").append(String.join(" ", inputs));
        }
        builder.append(" :where ").append(String.join(" ", where)).append("]");
        return builder.toString();
    }

    List<Object> prepareInputs(Object... inputs) {
        ArrayList<Object> prepared = new ArrayList<>();
        if (inputs != null) {
            for (Object input : inputs) {
                prepared.add(input);
            }
        }
        injectRulesInput(prepared);
        return prepared;
    }

    List<Object> prepareInputs(List<?> inputs) {
        ArrayList<Object> prepared = new ArrayList<>();
        if (inputs != null) {
            prepared.addAll(inputs);
        }
        injectRulesInput(prepared);
        return prepared;
    }

    @Override
    public String toString() {
        return toEdn();
    }

    private List<String> renderedInputs() {
        ArrayList<String> rendered = new ArrayList<>();
        if ((!in.isEmpty() || rules != null) && in.stream().noneMatch("$"::equals)) {
            rendered.add("$");
        }
        rendered.addAll(in);
        if (rules != null && rendered.stream().noneMatch("%"::equals)) {
            rendered.add("%");
        }
        return rendered;
    }

    private void injectRulesInput(List<Object> prepared) {
        if (rules == null) {
            return;
        }
        int index = in.indexOf("%");
        if (index < 0 || index > prepared.size()) {
            prepared.add(rules.asInput());
        } else {
            prepared.add(index, rules.asInput());
        }
    }
}
