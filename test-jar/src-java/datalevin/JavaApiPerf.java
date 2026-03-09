package datalevin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Consumer;

public final class JavaApiPerf {

    private static final int DEFAULT_WARMUP_MS = 1000;
    private static final int DEFAULT_BENCH_MS = 2000;
    private static final int DEFAULT_REPEATS = 5;
    private static final int DEFAULT_STEP = 10;

    private static final int ENTITY_COUNT = 4096;
    private static final int BUCKET_COUNT = 16;
    private static final long QUERY_BUCKET = 3L;
    private static final int PULL_COUNT = 128;
    private static final int KV_VALUE_COUNT = 64;
    private static final int PAGED_LIMIT = 8;
    private static final int PAGED_OFFSET = 8;

    private static final String QUERY_EDN =
            "[:find [?name ...] :where [?e :bucket 3] [?e :name ?name]]";

    private static volatile Object sink;
    private static final Consumer<Object> VISIT_LIST_CONSUMER = JavaApiPerf::consume;
    private static final BiPredicate<Object, Object> FILTER_PREDICATE =
            (key, value) -> "bucket-0".equals(key) && ((Long) value) >= 32L;
    private static final BiFunction<Object, Object, Object> KEEP_FN =
            (key, value) -> ((Long) value) >= 32L ? value : null;
    private static final BiFunction<Object, Object, Object> SOME_FN =
            (key, value) -> ((Long) value) == (KV_VALUE_COUNT - 1L) ? value : null;
    private static final BiPredicate<Object, Object> FILTER_COUNT_PREDICATE =
            (key, value) -> ((Long) value) >= 32L;
    private static final BiConsumer<Object, Object> VISIT_RANGE_CONSUMER =
            (key, value) -> consume(value);

    private JavaApiPerf() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.parse(args);
        Path dir = Files.createTempDirectory("datalevin-java-perf-");
        try {
            run(config, dir);
        } finally {
            deleteRecursively(dir);
        }
    }

    private static void run(Config config, Path dir) {
        Schema schema = Datalevin.schema()
                .attr("name", Schema.attribute()
                        .valueType(Schema.ValueType.STRING)
                        .unique(Schema.Unique.IDENTITY))
                .attr("age", Schema.attribute().valueType(Schema.ValueType.LONG))
                .attr("bucket", Schema.attribute().valueType(Schema.ValueType.LONG));

        try (Connection conn = Datalevin.createConn(dir.resolve("conn").toString(), schema);
             KV kv = Datalevin.openKV(dir.resolve("kv").toString())) {
            seedConnection(conn);
            seedKv(kv);

            DatalogQuery typedQuery = Datalevin.query()
                    .findAll("?name")
                    .whereDatom(Datalevin.var("e"), "bucket", QUERY_BUCKET)
                    .whereDatom(Datalevin.var("e"), "name", Datalevin.var("name"));
            PullSelector selector = Datalevin.pull().attr("name").attr("age").attr("bucket");

            Object db = ClojureRuntime.core("db", conn.handle());
            Object typedQueryForm = typedQuery.buildForm();
            Object stringQueryForm = DatalevinForms.queryForm(QUERY_EDN);
            Object selectorForm = selector.buildForm();

            List<Object> rawEntityIds = buildPullEntityIds(conn);
            Object normalizedEntityIds = DatalevinForms.entityIdsInput(rawEntityIds);

            Object kvHandle = kv.handle();
            Object keyType = DatalevinForms.typeInput(KVType.STRING);
            Object valueType = DatalevinForms.typeInput(KVType.LONG);
            RangeSpec allSpec = RangeSpec.all();
            Object allRange = DatalevinForms.rangeInput(allSpec.build());
            Object visitListFn = ClojureFns.consumer(VISIT_LIST_CONSUMER);
            Object filterFn = ClojureFns.biPredicate(FILTER_PREDICATE);
            Object keepFn = ClojureFns.biFunction(KEEP_FN);
            Object someFn = ClojureFns.biFunction(SOME_FN);
            Object filterCountFn = ClojureFns.biPredicate(FILTER_COUNT_PREDICATE);
            Object visitRangeFn = ClojureFns.biConsumer(VISIT_RANGE_CONSUMER);

            assertEqual(ClojureRuntime.core("q", typedQueryForm, db),
                        conn.query(typedQuery),
                        "typed raw query");
            assertEqual(ClojureRuntime.core("q", stringQueryForm, db),
                        conn.query(QUERY_EDN),
                        "string query");
            assertEqual(ClojureRuntime.core("q", typedQueryForm, db),
                        conn.queryCollection(typedQuery, String.class),
                        "typed collection query");
            assertEqual(ClojureRuntime.core("pull-many", db, selectorForm, normalizedEntityIds),
                        conn.pullMany(selector, rawEntityIds),
                        "pullMany");
            assertEqual(ClojureRuntime.core("get-list",
                                            kvHandle,
                                            "scores",
                                            "bucket-0",
                                            keyType,
                                            valueType),
                        kv.getList("scores", "bucket-0", KVType.STRING, KVType.LONG),
                        "kv getList");
            assertEqual(ClojureRuntime.core("get-range",
                                            kvHandle,
                                            "scores",
                                            allRange,
                                            keyType,
                                            valueType),
                        kv.getRange("scores", allSpec, KVType.STRING, KVType.LONG, null, null),
                        "kv getRange");
            assertEqual(collectVisitedListDirect(kvHandle, keyType, valueType),
                        collectVisitedListWrapper(kv),
                        "kv visitList");
            assertEqual(ClojureRuntime.core("list-range-filter",
                                            kvHandle,
                                            "scores",
                                            filterFn,
                                            allRange,
                                            keyType,
                                            allRange,
                                            valueType,
                                            false),
                        kv.listRangeFilter("scores",
                                           FILTER_PREDICATE,
                                           allSpec,
                                           KVType.STRING,
                                           allSpec,
                                           KVType.LONG,
                                           null,
                                           null),
                        "kv listRangeFilter");
            assertEqual(ClojureRuntime.core("list-range-keep",
                                            kvHandle,
                                            "scores",
                                            keepFn,
                                            allRange,
                                            keyType,
                                            allRange,
                                            valueType,
                                            false),
                        kv.listRangeKeep("scores",
                                         KEEP_FN,
                                         allSpec,
                                         KVType.STRING,
                                         allSpec,
                                         KVType.LONG,
                                         null,
                                         null),
                        "kv listRangeKeep");
            assertEqual(ClojureRuntime.core("list-range-some",
                                            kvHandle,
                                            "scores",
                                            someFn,
                                            allRange,
                                            keyType,
                                            allRange,
                                            valueType,
                                            false),
                        kv.listRangeSome("scores",
                                         SOME_FN,
                                         allSpec,
                                         KVType.STRING,
                                         allSpec,
                                         KVType.LONG),
                        "kv listRangeSome");
            assertEqual(ClojureRuntime.core("list-range-filter-count",
                                            kvHandle,
                                            "scores",
                                            filterCountFn,
                                            allRange,
                                            keyType,
                                            allRange,
                                            valueType,
                                            false),
                        kv.listRangeFilterCount("scores",
                                                FILTER_COUNT_PREDICATE,
                                                allSpec,
                                                KVType.STRING,
                                                allSpec,
                                                KVType.LONG),
                        "kv listRangeFilterCount");
            assertEqual(collectVisitedRangeDirect(kvHandle,
                                                  keyType,
                                                  valueType,
                                                  allRange),
                        collectVisitedRangeWrapper(kv),
                        "kv visitListRange");
            assertEqual(pageList(ResultSupport.sequence(ClojureRuntime.core("list-range-filter",
                                                                            kvHandle,
                                                                            "scores",
                                                                            filterFn,
                                                                            allRange,
                                                                            keyType,
                                                                            allRange,
                                                                            valueType,
                                                                            false)),
                                 PAGED_LIMIT,
                                 PAGED_OFFSET),
                        kv.listRangeFilter("scores",
                                           FILTER_PREDICATE,
                                           allSpec,
                                           KVType.STRING,
                                           allSpec,
                                           KVType.LONG,
                                           PAGED_LIMIT,
                                           PAGED_OFFSET),
                        "kv paged listRangeFilter");
            assertEqual(pageList(ResultSupport.sequence(ClojureRuntime.core("list-range-keep",
                                                                            kvHandle,
                                                                            "scores",
                                                                            keepFn,
                                                                            allRange,
                                                                            keyType,
                                                                            allRange,
                                                                            valueType,
                                                                            false)),
                                 PAGED_LIMIT,
                                 PAGED_OFFSET),
                        kv.listRangeKeep("scores",
                                         KEEP_FN,
                                         allSpec,
                                         KVType.STRING,
                                         allSpec,
                                         KVType.LONG,
                                         PAGED_LIMIT,
                                         PAGED_OFFSET),
                        "kv paged listRangeKeep");

            Result directTypedQuery = benchmark(config,
                                                "direct typed query",
                                                () -> consume(ClojureRuntime.core("q",
                                                                                  typedQueryForm,
                                                                                  db)));
            Result wrapperTypedQuery = benchmark(config,
                                                 "wrapper typed query",
                                                 () -> consume(conn.query(typedQuery)));

            Result directStringQuery = benchmark(config,
                                                 "direct string query",
                                                 () -> consume(ClojureRuntime.core("q",
                                                                                   stringQueryForm,
                                                                                   db)));
            Result wrapperStringQuery = benchmark(config,
                                                  "wrapper string query",
                                                  () -> consume(conn.query(QUERY_EDN)));

            Result wrapperTypedCollection = benchmark(config,
                                                      "wrapper typed collection",
                                                      () -> consume(conn.queryCollection(typedQuery,
                                                                                        String.class)));

            Result directPullMany = benchmark(config,
                                              "direct pullMany",
                                              () -> consume(ClojureRuntime.core("pull-many",
                                                                                db,
                                                                                selectorForm,
                                                                                normalizedEntityIds)));
            Result wrapperPullMany = benchmark(config,
                                               "wrapper pullMany",
                                               () -> consume(conn.pullMany(selector, rawEntityIds)));

            Result directGetList = benchmark(config,
                                             "direct kv getList",
                                             () -> consume(ClojureRuntime.core("get-list",
                                                                               kvHandle,
                                                                               "scores",
                                                                               "bucket-0",
                                                                               keyType,
                                                                               valueType)));
            Result wrapperGetList = benchmark(config,
                                              "wrapper kv getList",
                                              () -> consume(kv.getList("scores",
                                                                       "bucket-0",
                                                                       KVType.STRING,
                                                                       KVType.LONG)));

            Result directGetRange = benchmark(config,
                                              "direct kv getRange",
                                              () -> consume(ClojureRuntime.core("get-range",
                                                                                kvHandle,
                                                                                "scores",
                                                                                allRange,
                                                                                keyType,
                                                                                valueType)));
            Result wrapperGetRange = benchmark(config,
                                               "wrapper kv getRange",
                                               () -> consume(kv.getRange("scores",
                                                                         allSpec,
                                                                         KVType.STRING,
                                                                         KVType.LONG,
                                                                         null,
                                                                         null)));
            Result directVisitList = benchmark(config,
                                               "direct kv visitList",
                                               () -> ClojureRuntime.core("visit-list",
                                                                         kvHandle,
                                                                         "scores",
                                                                         visitListFn,
                                                                         "bucket-0",
                                                                         keyType,
                                                                         valueType,
                                                                         false));
            Result wrapperVisitList = benchmark(config,
                                                "wrapper kv visitList",
                                                () -> kv.visitList("scores",
                                                                   VISIT_LIST_CONSUMER,
                                                                   "bucket-0",
                                                                   KVType.STRING,
                                                                   KVType.LONG));
            Result directListRangeFilter = benchmark(config,
                                                     "direct kv listRangeFilter",
                                                     () -> consume(ClojureRuntime.core("list-range-filter",
                                                                                       kvHandle,
                                                                                       "scores",
                                                                                       filterFn,
                                                                                       allRange,
                                                                                       keyType,
                                                                                       allRange,
                                                                                       valueType,
                                                                                       false)));
            Result wrapperListRangeFilter = benchmark(config,
                                                      "wrapper kv listRangeFilter",
                                                      () -> consume(kv.listRangeFilter("scores",
                                                                                       FILTER_PREDICATE,
                                                                                       allSpec,
                                                                                       KVType.STRING,
                                                                                       allSpec,
                                                                                       KVType.LONG,
                                                                                       null,
                                                                                       null)));
            Result directListRangeKeep = benchmark(config,
                                                   "direct kv listRangeKeep",
                                                   () -> consume(ClojureRuntime.core("list-range-keep",
                                                                                     kvHandle,
                                                                                     "scores",
                                                                                     keepFn,
                                                                                     allRange,
                                                                                     keyType,
                                                                                     allRange,
                                                                                     valueType,
                                                                                     false)));
            Result wrapperListRangeKeep = benchmark(config,
                                                    "wrapper kv listRangeKeep",
                                                    () -> consume(kv.listRangeKeep("scores",
                                                                                   KEEP_FN,
                                                                                   allSpec,
                                                                                   KVType.STRING,
                                                                                   allSpec,
                                                                                   KVType.LONG,
                                                                                   null,
                                                                                   null)));
            Result directListRangeSome = benchmark(config,
                                                   "direct kv listRangeSome",
                                                   () -> consume(ClojureRuntime.core("list-range-some",
                                                                                     kvHandle,
                                                                                     "scores",
                                                                                     someFn,
                                                                                     allRange,
                                                                                     keyType,
                                                                                     allRange,
                                                                                     valueType,
                                                                                     false)));
            Result wrapperListRangeSome = benchmark(config,
                                                    "wrapper kv listRangeSome",
                                                    () -> consume(kv.listRangeSome("scores",
                                                                                   SOME_FN,
                                                                                   allSpec,
                                                                                   KVType.STRING,
                                                                                   allSpec,
                                                                                   KVType.LONG)));
            Result directListRangeFilterCount = benchmark(config,
                                                          "direct kv listRangeFilterCount",
                                                          () -> consume(ClojureRuntime.core("list-range-filter-count",
                                                                                            kvHandle,
                                                                                            "scores",
                                                                                            filterCountFn,
                                                                                            allRange,
                                                                                            keyType,
                                                                                            allRange,
                                                                                            valueType,
                                                                                            false)));
            Result wrapperListRangeFilterCount = benchmark(config,
                                                           "wrapper kv listRangeFilterCount",
                                                           () -> consume(kv.listRangeFilterCount("scores",
                                                                                                FILTER_COUNT_PREDICATE,
                                                                                                allSpec,
                                                                                                KVType.STRING,
                                                                                                allSpec,
                                                                                                KVType.LONG)));
            Result directVisitListRange = benchmark(config,
                                                    "direct kv visitListRange",
                                                    () -> ClojureRuntime.core("visit-list-range",
                                                                              kvHandle,
                                                                              "scores",
                                                                              visitRangeFn,
                                                                              allRange,
                                                                              keyType,
                                                                              allRange,
                                                                              valueType,
                                                                              false));
            Result wrapperVisitListRange = benchmark(config,
                                                     "wrapper kv visitListRange",
                                                     () -> kv.visitListRange("scores",
                                                                             VISIT_RANGE_CONSUMER,
                                                                             allSpec,
                                                                             KVType.STRING,
                                                                             allSpec,
                                                                             KVType.LONG));
            Result directPagedListRangeFilter = benchmark(config,
                                                          "direct kv listRangeFilter paged",
                                                          () -> consume(pageList(ResultSupport.sequence(ClojureRuntime.core("list-range-filter",
                                                                                                                            kvHandle,
                                                                                                                            "scores",
                                                                                                                            filterFn,
                                                                                                                            allRange,
                                                                                                                            keyType,
                                                                                                                            allRange,
                                                                                                                            valueType,
                                                                                                                            false)),
                                                                                   PAGED_LIMIT,
                                                                                   PAGED_OFFSET)));
            Result wrapperPagedListRangeFilter = benchmark(config,
                                                           "wrapper kv listRangeFilter paged",
                                                           () -> consume(kv.listRangeFilter("scores",
                                                                                            FILTER_PREDICATE,
                                                                                            allSpec,
                                                                                            KVType.STRING,
                                                                                            allSpec,
                                                                                            KVType.LONG,
                                                                                            PAGED_LIMIT,
                                                                                            PAGED_OFFSET)));
            Result directPagedListRangeKeep = benchmark(config,
                                                        "direct kv listRangeKeep paged",
                                                        () -> consume(pageList(ResultSupport.sequence(ClojureRuntime.core("list-range-keep",
                                                                                                                          kvHandle,
                                                                                                                          "scores",
                                                                                                                          keepFn,
                                                                                                                          allRange,
                                                                                                                          keyType,
                                                                                                                          allRange,
                                                                                                                          valueType,
                                                                                                                          false)),
                                                                                 PAGED_LIMIT,
                                                                                 PAGED_OFFSET)));
            Result wrapperPagedListRangeKeep = benchmark(config,
                                                         "wrapper kv listRangeKeep paged",
                                                         () -> consume(kv.listRangeKeep("scores",
                                                                                        KEEP_FN,
                                                                                        allSpec,
                                                                                        KVType.STRING,
                                                                                        allSpec,
                                                                                        KVType.LONG,
                                                                                        PAGED_LIMIT,
                                                                                        PAGED_OFFSET)));

            printHeader(config);
            printComparison("Query raw typed result", directTypedQuery, wrapperTypedQuery);
            printComparison("Query raw string result", directStringQuery, wrapperStringQuery);
            printComparison("Query typed collection result", directTypedQuery, wrapperTypedCollection);
            printComparison("Pull many (" + PULL_COUNT + " lookup refs)", directPullMany, wrapperPullMany);
            printComparison("KV getList (" + KV_VALUE_COUNT + " longs)", directGetList, wrapperGetList);
            printComparison("KV getRange (" + KV_VALUE_COUNT + " entries)", directGetRange, wrapperGetRange);
            printComparison("KV visitList (" + KV_VALUE_COUNT + " longs)", directVisitList, wrapperVisitList);
            printComparison("KV listRangeFilter (" + KV_VALUE_COUNT + " matches)", directListRangeFilter, wrapperListRangeFilter);
            printComparison("KV listRangeKeep (" + KV_VALUE_COUNT + " matches)", directListRangeKeep, wrapperListRangeKeep);
            printComparison("KV listRangeSome", directListRangeSome, wrapperListRangeSome);
            printComparison("KV listRangeFilterCount", directListRangeFilterCount, wrapperListRangeFilterCount);
            printComparison("KV visitListRange (" + KV_VALUE_COUNT + " entries)", directVisitListRange, wrapperVisitListRange);
            printComparison("KV listRangeFilter paged (limit=" + PAGED_LIMIT + ", offset=" + PAGED_OFFSET + ")",
                            directPagedListRangeFilter,
                            wrapperPagedListRangeFilter);
            printComparison("KV listRangeKeep paged (limit=" + PAGED_LIMIT + ", offset=" + PAGED_OFFSET + ")",
                            directPagedListRangeKeep,
                            wrapperPagedListRangeKeep);
        }
    }

    private static void seedConnection(Connection conn) {
        TxData tx = Datalevin.tx();
        for (int i = 0; i < ENTITY_COUNT; i++) {
            tx.entity(Tx.entity(-(i + 1L))
                    .put("name", "user-" + i)
                    .put("age", 20L + (i % 50))
                    .put("bucket", (long) (i % BUCKET_COUNT)));
        }
        conn.transact(tx);
    }

    private static void seedKv(KV kv) {
        kv.openListDbi("scores");
        ArrayList<Long> values = new ArrayList<>(KV_VALUE_COUNT);
        for (long i = 0; i < KV_VALUE_COUNT; i++) {
            values.add(i);
        }
        kv.putListItems("scores", "bucket-0", values, KVType.STRING, KVType.LONG);
    }

    private static List<Object> buildPullEntityIds(Connection conn) {
        ArrayList<Object> entityIds = new ArrayList<>(PULL_COUNT);
        for (int i = 0; i < PULL_COUNT; i++) {
            int entity = (int) QUERY_BUCKET + (i * BUCKET_COUNT);
            entityIds.add(conn.entid(List.of(Datalevin.kw("name"), "user-" + entity)));
        }
        return List.copyOf(entityIds);
    }

    private static List<Object> collectVisitedListDirect(Object kvHandle,
                                                         Object keyType,
                                                         Object valueType) {
        ArrayList<Object> visited = new ArrayList<>(KV_VALUE_COUNT);
        ClojureRuntime.core("visit-list",
                            kvHandle,
                            "scores",
                            ClojureFns.consumer(visited::add),
                            "bucket-0",
                            keyType,
                            valueType,
                            false);
        return visited;
    }

    private static List<Object> collectVisitedListWrapper(KV kv) {
        ArrayList<Object> visited = new ArrayList<>(KV_VALUE_COUNT);
        kv.visitList("scores", visited::add, "bucket-0", KVType.STRING, KVType.LONG);
        return visited;
    }

    private static List<Object> collectVisitedRangeDirect(Object kvHandle,
                                                          Object keyType,
                                                          Object valueType,
                                                          Object allRange) {
        ArrayList<Object> visited = new ArrayList<>(KV_VALUE_COUNT);
        ClojureRuntime.core("visit-list-range",
                            kvHandle,
                            "scores",
                            ClojureFns.biConsumer((key, value) -> visited.add(List.of(key, value))),
                            allRange,
                            keyType,
                            allRange,
                            valueType,
                            false);
        return visited;
    }

    private static List<Object> collectVisitedRangeWrapper(KV kv) {
        ArrayList<Object> visited = new ArrayList<>(KV_VALUE_COUNT);
        kv.visitListRange("scores",
                          (key, value) -> visited.add(List.of(key, value)),
                          RangeSpec.all(),
                          KVType.STRING,
                          RangeSpec.all(),
                          KVType.LONG);
        return visited;
    }

    private static Result benchmark(Config config, String name, Task task) {
        forDuration(config.warmupMs(), config.step(), task);
        double[] samples = new double[config.repeats()];
        for (int i = 0; i < samples.length; i++) {
            samples[i] = forDuration(config.benchMs(), config.step(), task);
        }
        Arrays.sort(samples);
        return new Result(name, samples[samples.length / 2]);
    }

    private static double forDuration(int durationMs, int step, Task task) {
        long start = System.nanoTime();
        long end = start + (durationMs * 1_000_000L);
        long iterations = 0L;
        while (true) {
            for (int i = 0; i < step; i++) {
                task.run();
            }
            iterations += step;
            long now = System.nanoTime();
            if (now >= end) {
                return (now - start) / (double) iterations / 1_000.0;
            }
        }
    }

    private static void consume(Object value) {
        sink = fingerprint(value);
    }

    private static void printHeader(Config config) {
        System.out.printf(Locale.ROOT,
                          "Java API overhead benchmark%n"
                                  + "dataset: entities=%d buckets=%d query-bucket=%d pullMany=%d kv-values=%d%n"
                                  + "timing: warmup=%dms bench=%dms repeats=%d%n%n",
                          ENTITY_COUNT,
                          BUCKET_COUNT,
                          QUERY_BUCKET,
                          PULL_COUNT,
                          KV_VALUE_COUNT,
                          config.warmupMs(),
                          config.benchMs(),
                          config.repeats());
    }

    private static void printComparison(String label, Result direct, Result wrapper) {
        double overhead = ((wrapper.microsPerOp() / direct.microsPerOp()) - 1.0) * 100.0;
        System.out.printf(Locale.ROOT,
                          "%s%n  direct   %8s us/op%n  wrapper  %8s us/op  (%+.1f%%)%n%n",
                          label,
                          formatMicros(direct.microsPerOp()),
                          formatMicros(wrapper.microsPerOp()),
                          overhead);
    }

    private static String formatMicros(double value) {
        if (value >= 100.0) {
            return String.format(Locale.ROOT, "%.1f", value);
        }
        if (value >= 10.0) {
            return String.format(Locale.ROOT, "%.2f", value);
        }
        return String.format(Locale.ROOT, "%.3f", value);
    }

    private static void assertEqual(Object expected, Object actual, String label) {
        if (!equivalent(expected, actual)) {
            throw new IllegalStateException(label + " mismatch.\nexpected: " + expected + "\nactual:   " + actual);
        }
    }

    private static Object fingerprint(Object value) {
        if (value instanceof Map<?, ?> map) {
            return List.of(map.size(), map.hashCode());
        }
        if (value instanceof List<?> list) {
            int size = list.size();
            return List.of(size,
                           size == 0 ? null : list.get(0),
                           size == 0 ? null : list.get(size - 1));
        }
        if (value instanceof Iterable<?> iterable) {
            int count = 0;
            Object first = null;
            Object last = null;
            for (Object item : iterable) {
                if (count == 0) {
                    first = item;
                }
                last = item;
                count++;
            }
            return List.of(count, first, last);
        }
        return value;
    }

    private static boolean equivalent(Object expected, Object actual) {
        if (Objects.equals(expected, actual)) {
            return true;
        }
        if (expected instanceof Map<?, ?> expectedMap && actual instanceof Map<?, ?> actualMap) {
            return expectedMap.equals(actualMap);
        }
        if (expected instanceof Iterable<?> expectedIterable && actual instanceof Iterable<?> actualIterable) {
            return iterableValues(expectedIterable).equals(iterableValues(actualIterable));
        }
        return false;
    }

    private static List<Object> iterableValues(Iterable<?> value) {
        ArrayList<Object> items = new ArrayList<>();
        for (Object item : value) {
            items.add(item);
        }
        return items;
    }

    private static List<?> pageList(List<?> items, int limit, int offset) {
        int start = Math.max(offset, 0);
        if (start >= items.size() || limit <= 0) {
            return List.of();
        }
        int end = Math.min(items.size(), start + limit);
        if (start == 0 && end == items.size()) {
            return items;
        }
        ArrayList<Object> page = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            page.add(items.get(i));
        }
        return page;
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || Files.notExists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted((a, b) -> b.getNameCount() - a.getNameCount())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw e;
        }
    }

    @FunctionalInterface
    private interface Task {
        void run();
    }

    private record Result(String name, double microsPerOp) {
    }

    private record Config(int warmupMs, int benchMs, int repeats, int step) {

        static Config parse(String[] args) {
            if (args.length == 0) {
                return new Config(DEFAULT_WARMUP_MS, DEFAULT_BENCH_MS, DEFAULT_REPEATS, DEFAULT_STEP);
            }
            if (args.length != 3) {
                throw new IllegalArgumentException("Usage: JavaApiPerf [warmup-ms bench-ms repeats]");
            }
            return new Config(Integer.parseInt(args[0]),
                              Integer.parseInt(args[1]),
                              Integer.parseInt(args[2]),
                              DEFAULT_STEP);
        }
    }
}
