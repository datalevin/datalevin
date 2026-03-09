package datalevin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builder for Datalevin Datalog queries.
 *
 * <p>This builder emits EDN query text and can attach a {@link Rules} value as
 * the implicit {@code %} input when used through {@link Connection}.
 */
public final class DatalogQuery {

    enum ResultShape {
        RELATION,
        COLLECTION,
        TUPLE,
        SCALAR,
        KEYED
    }

    private enum ReturnMapMode {
        KEYS(":keys"),
        STRS(":strs"),
        SYMS(":syms");

        private final String keyword;

        ReturnMapMode(String keyword) {
            this.keyword = keyword;
        }

        Object keyword() {
            return ClojureCodec.keyword(keyword);
        }
    }

    private final List<Object> find = new ArrayList<>();
    private final List<Object> with = new ArrayList<>();
    private final List<Object> in = new ArrayList<>();
    private final List<QueryClause> where = new ArrayList<>();
    private final List<String> returnMapFields = new ArrayList<>();
    private Rules rules;
    private ResultShape resultShape = ResultShape.RELATION;
    private ReturnMapMode returnMapMode;
    private boolean requiresDb;
    private Object cachedForm;

    DatalogQuery() {
    }

    /**
     * Adds raw {@code :find} terms.
     */
    public DatalogQuery find(String... terms) {
        invalidateForm();
        resultShape = returnMapMode == null ? ResultShape.RELATION : ResultShape.KEYED;
        for (String term : terms) {
            find.add(rawOrSymbol(term));
        }
        return this;
    }

    /**
     * Declares a scalar find result using the {@code .} find spec.
     */
    public DatalogQuery findScalar(String term) {
        invalidateForm();
        ensureKeyedResultAbsent("findScalar");
        resultShape = ResultShape.SCALAR;
        find.add(rawOrSymbol(term));
        find.add(ClojureCodec.symbol("."));
        return this;
    }

    /**
     * Declares a collection find result using the {@code [?x ...]} find spec.
     */
    public DatalogQuery findAll(String variable) {
        invalidateForm();
        ensureKeyedResultAbsent("findAll");
        resultShape = ResultShape.COLLECTION;
        find.add(List.of(rawOrSymbol(variable), ClojureCodec.symbol("...")));
        return this;
    }

    /**
     * Declares a tuple find result using the {@code [?x ?y]} find spec.
     */
    public DatalogQuery findTuple(String... terms) {
        invalidateForm();
        ensureKeyedResultAbsent("findTuple");
        resultShape = ResultShape.TUPLE;
        ArrayList<Object> values = new ArrayList<>(terms.length);
        for (String term : terms) {
            values.add(rawOrSymbol(term));
        }
        find.add(values);
        return this;
    }

    /**
     * Declares a {@code :keys} return map.
     */
    public DatalogQuery keys(String... names) {
        return returnMap(ReturnMapMode.KEYS, names);
    }

    /**
     * Declares a {@code :strs} return map.
     */
    public DatalogQuery strs(String... names) {
        return returnMap(ReturnMapMode.STRS, names);
    }

    /**
     * Declares a {@code :syms} return map.
     */
    public DatalogQuery syms(String... names) {
        return returnMap(ReturnMapMode.SYMS, names);
    }

    /**
     * Adds raw {@code :in} inputs.
     */
    public DatalogQuery in(String... inputs) {
        invalidateForm();
        for (String input : inputs) {
            in.add(rawOrInputSymbol(input));
        }
        return this;
    }

    /**
     * Adds raw {@code :with} variables.
     */
    public DatalogQuery with(String... vars) {
        invalidateForm();
        for (String var : vars) {
            with.add(rawOrSymbol(var));
        }
        return this;
    }

    /**
     * Attaches rules to this query.
     */
    public DatalogQuery rules(Rules rules) {
        invalidateForm();
        this.rules = rules;
        return this;
    }

    /**
     * Adds a prebuilt clause.
     */
    public DatalogQuery where(QueryClause clause) {
        invalidateForm();
        where.add(clause);
        requiresDb = requiresDb || clause.requiresDb();
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
        return Edn.render(renderForm());
    }

    List<Object> prepareInputs(Object... inputs) {
        if (rules == null) {
            return inputs == null || inputs.length == 0 ? List.of() : Arrays.asList(inputs);
        }
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
        if (rules == null) {
            if (inputs == null || inputs.isEmpty()) {
                return List.of();
            }
            @SuppressWarnings("unchecked")
            List<Object> existing = (List<Object>) inputs;
            return existing;
        }
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

    private void injectRulesInput(List<Object> prepared) {
        if (rules == null) {
            return;
        }
        int index = indexOfToken("%");
        if (index < 0 || index > prepared.size()) {
            prepared.add(rules.asInput());
        } else {
            prepared.add(index, rules.asInput());
        }
    }

    Object buildForm() {
        if (cachedForm == null) {
            cachedForm = ClojureCodec.toClojure(renderForm());
        }
        return cachedForm;
    }

    private List<Object> renderForm() {
        if (find.isEmpty()) {
            throw new IllegalStateException("DatalogQuery requires at least one :find term.");
        }
        if (where.isEmpty()) {
            throw new IllegalStateException("DatalogQuery requires at least one :where clause.");
        }

        ArrayList<Object> form = new ArrayList<>();
        form.add(ClojureCodec.keyword(":find"));
        form.addAll(find);
        if (returnMapMode != null) {
            validateReturnMap();
            form.add(returnMapMode.keyword());
            for (String field : returnMapFields) {
                form.add(ClojureCodec.symbol(field));
            }
        }
        if (!with.isEmpty()) {
            form.add(ClojureCodec.keyword(":with"));
            form.addAll(with);
        }
        List<Object> inputs = renderedInputs();
        if (!inputs.isEmpty()) {
            form.add(ClojureCodec.keyword(":in"));
            form.addAll(inputs);
        }
        form.add(ClojureCodec.keyword(":where"));
        for (QueryClause clause : where) {
            form.add(clause.form());
        }
        return form;
    }

    ResultShape resultShape() {
        return resultShape;
    }

    List<String> returnMapFields() {
        return ResultSupport.copyList(returnMapFields);
    }

    boolean requiresDb() {
        return requiresDb || (!in.isEmpty() && indexOfToken("$") >= 0);
    }

    private List<Object> renderedInputs() {
        ArrayList<Object> rendered = new ArrayList<>();
        if ((!in.isEmpty() || rules != null) && requiresDb() && indexOfToken("$") < 0) {
            rendered.add(ClojureCodec.symbol("$"));
        }
        rendered.addAll(in);
        if (rules != null && rendered.stream().noneMatch(input -> tokenEquals(input, "%"))) {
            rendered.add(ClojureCodec.symbol("%"));
        }
        return rendered;
    }

    private int indexOfToken(String token) {
        for (int i = 0; i < in.size(); i++) {
            if (tokenEquals(in.get(i), token)) {
                return i;
            }
        }
        return -1;
    }

    private static Object rawOrSymbol(String term) {
        return isSimpleSymbol(term) ? ClojureCodec.symbol(term) : ClojureRuntime.readEdn(term);
    }

    private static Object rawOrInputSymbol(String term) {
        return isSimpleInput(term) ? ClojureCodec.symbol(term) : ClojureRuntime.readEdn(term);
    }

    private DatalogQuery returnMap(ReturnMapMode mode, String... names) {
        invalidateForm();
        if (resultShape != ResultShape.RELATION && resultShape != ResultShape.KEYED) {
            throw new IllegalStateException(mode.keyword + " requires a relation-style :find clause.");
        }
        returnMapMode = mode;
        returnMapFields.clear();
        for (String name : names) {
            returnMapFields.add(normalizeReturnMapField(name));
        }
        resultShape = ResultShape.KEYED;
        return this;
    }

    private void invalidateForm() {
        cachedForm = null;
    }

    private void ensureKeyedResultAbsent(String method) {
        if (returnMapMode != null) {
            throw new IllegalStateException(method + " cannot be used with a keyed query result.");
        }
    }

    private void validateReturnMap() {
        if (returnMapFields.isEmpty()) {
            throw new IllegalStateException("Keyed query results require at least one field name.");
        }
        if (returnMapFields.size() != find.size()) {
            throw new IllegalStateException("Keyed query result names must match the number of :find terms.");
        }
    }

    private static String normalizeReturnMapField(String field) {
        if (field == null || field.isBlank()) {
            throw new IllegalArgumentException("Return map field names must be non-empty.");
        }
        return field.startsWith(":") ? field.substring(1) : field;
    }

    private static boolean tokenEquals(Object token, String expected) {
        if (token instanceof EdnLiteral literal) {
            return expected.equals(literal.value());
        }
        return expected.equals(String.valueOf(token));
    }

    private static boolean isSimpleSymbol(String term) {
        return term.startsWith("?")
                || term.startsWith("$")
                || term.startsWith("%")
                || term.startsWith("_")
                || ".".equals(term)
                || "...".equals(term);
    }

    private static boolean isSimpleInput(String term) {
        return "$".equals(term)
                || "%".equals(term)
                || term.startsWith("?")
                || term.startsWith("$")
                || term.startsWith("%");
    }
}
