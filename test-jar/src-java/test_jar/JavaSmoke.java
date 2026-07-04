package test_jar;

import datalevin.Connection;
import datalevin.Datalevin;
import datalevin.DatalevinInterop;
import datalevin.DatalogQuery;
import datalevin.KV;
import datalevin.KVType;
import datalevin.PullSelector;
import datalevin.RangeSpec;
import datalevin.Schema;
import datalevin.Tx;
import datalevin.UdfDescriptor;
import datalevin.UdfRegistry;
import datalevin.VectorIndex;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class JavaSmoke {

    private JavaSmoke() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("datalevin-java-smoke-");
        try {
            try (Connection conn = Datalevin.createConn(
                    dir.resolve("conn").toString(),
                    Datalevin.schema()
                            .attr("name", Schema.attribute()
                                    .valueType(Schema.ValueType.STRING)
                                    .unique(Schema.Unique.IDENTITY))
                            .attr("age", Schema.attribute().valueType(Schema.ValueType.LONG)))) {

                conn.transact(Datalevin.tx()
                        .entity(Tx.entity(-1).put("name", "Alice").put("age", 30))
                        .entity(Tx.entity(-2).put("name", "Bob").put("age", 25)));

                List<Map<?, ?>> reports = new ArrayList<>();
                Object listenerKey = conn.listen("test-listener", reports::add);
                if (!"test-listener".equals(listenerKey)) {
                    throw new IllegalStateException("Unexpected listener key: " + listenerKey);
                }
                conn.transact(List.of(Map.of(":db/id", -3L, "name", "Cara", "age", 22L)));
                conn.unlisten(listenerKey);
                conn.transact(List.of(Map.of(":db/id", -4L, "name", "Drew", "age", 28L)));
                if (reports.size() != 1 || !reports.get(0).containsKey(Datalevin.kw("tx-data"))) {
                    throw new IllegalStateException("Unexpected listener reports: " + reports);
                }

                DatalogQuery adultsQuery = Datalevin.query()
                        .findAll("?name")
                        .whereDatom(Datalevin.var("e"), "name", Datalevin.var("name"))
                        .whereDatom(Datalevin.var("e"), "age", Datalevin.var("age"))
                        .wherePredicate(">=", Datalevin.var("age"), 30);

                DatalogQuery keyedQuery = Datalevin.query()
                        .find("?name", "?age")
                        .keys("name", "age")
                        .whereDatom(Datalevin.var("e"), "name", Datalevin.var("name"))
                        .whereDatom(Datalevin.var("e"), "age", Datalevin.var("age"));

                List<String> adults = conn.queryCollection(adultsQuery, String.class);
                if (!List.of("Alice").equals(adults)) {
                    throw new IllegalStateException("Unexpected adult query result: " + adults);
                }

                try (Connection other = Datalevin.createConn(
                        dir.resolve("other").toString(),
                        Datalevin.schema().attr("name", Schema.attribute().valueType(Schema.ValueType.STRING)))) {
                    other.transact(List.of(Map.of(":db/id", -1L, "name", "Eve")));
                    Object sourceResult = conn.query(
                            "[:find [?name ...] :in $ $other :where "
                                    + "[$ ?e :name \"Alice\"] [$other ?x :name ?name]]",
                            other);
                    if (!List.of("Eve").equals(sourceResult)) {
                        throw new IllegalStateException("Unexpected multi-source query result: " + sourceResult);
                    }
                }

                List<?> keyed = conn.queryKeyed(keyedQuery);
                if (keyed.size() != 4) {
                    throw new IllegalStateException("Unexpected keyed query fields: " + keyed);
                }
                @SuppressWarnings("unchecked")
                Map<Object, Object> firstRow = (Map<Object, Object>) keyed.get(0);
                if (!firstRow.containsKey(Datalevin.kw("name"))) {
                    throw new IllegalStateException("Unexpected keyed query result: " + keyed);
                }

                PullSelector selector = Datalevin.pull().attr("name").attr("age");
                Map<?, ?> simulated = Datalevin.txDataToSimulatedReport(
                        conn,
                        List.of(Map.of(":db/id", -5L, "name", "Sim", "age", 99L)));
                if (!simulated.containsKey(Datalevin.kw("tx-data"))
                        || ((List<?>) simulated.get(Datalevin.kw("tx-data"))).isEmpty()) {
                    throw new IllegalStateException("Unexpected simulated report: " + simulated);
                }
                Object simulatedDb = simulated.get(Datalevin.kw("db-after"));
                Map<?, ?> simulatedPull = Datalevin.pull(
                        simulatedDb,
                        selector,
                        Datalevin.listOf(":name", "Sim"));
                if (!"Sim".equals(simulatedPull.get(Datalevin.kw("name")))
                        || !Long.valueOf(99L).equals(simulatedPull.get(Datalevin.kw("age")))) {
                    throw new IllegalStateException("Unexpected simulated pull result: " + simulatedPull);
                }
                Object simulatedEntity = conn.query("[:find ?e . :where [?e :name \"Sim\"]]");
                if (simulatedEntity != null) {
                    throw new IllegalStateException("Simulated transaction was committed: " + simulatedEntity);
                }

                Map<?, ?> alice = conn.pull(selector, Datalevin.listOf(":name", "Alice"));
                if (!"Alice".equals(alice.get(Datalevin.kw("name")))) {
                    throw new IllegalStateException("Unexpected pull result: " + alice);
                }
            }

            UdfRegistry registry = Datalevin.udfRegistry();
            UdfDescriptor analyzerDescriptor = Datalevin.analyzerUdf("text/hashtags");
            UdfDescriptor queryDescriptor = Datalevin.queryAnalyzerUdf("text/plain-query");
            registry.analyzer("text/hashtags", udfArgs -> {
                String text = (String) udfArgs.get(0);
                List<List<Object>> tokens = new ArrayList<>();
                int searchFrom = 0;
                while (searchFrom < text.length()) {
                    int start = text.indexOf('#', searchFrom);
                    if (start < 0) {
                        break;
                    }
                    int end = start + 1;
                    while (end < text.length() && Character.isLetterOrDigit(text.charAt(end))) {
                        end++;
                    }
                    if (end > start + 1) {
                        tokens.add(List.of(text.substring(start + 1, end), tokens.size(), start));
                    }
                    searchFrom = Math.max(end, start + 1);
                }
                return tokens;
            });
            registry.queryAnalyzer("text/plain-query", udfArgs -> {
                String text = (String) udfArgs.get(0);
                List<List<Object>> tokens = new ArrayList<>();
                for (String token : text.split("\\s+")) {
                    if (!token.isBlank()) {
                        tokens.add(List.of(token, tokens.size(), tokens.size()));
                    }
                }
                return tokens;
            });
            try (Connection conn = Datalevin.createConn(
                    dir.resolve("fulltext-udf").toString(),
                    Datalevin.schema().attr("text", Schema.attribute()
                            .valueType(Schema.ValueType.STRING)
                            .fulltext(true)
                            .fulltextAutoDomain(true)),
                    Map.of(
                            ":runtime-opts", Map.of(":udf-registry", registry),
                            ":search-domains", Map.of(
                                    "text",
                                    Datalevin.searchDomain()
                                            .indexPosition(true)
                                            .prop("analyzer", analyzerDescriptor)
                                            .prop("query-analyzer", queryDescriptor)
                                            .build())))) {
                conn.transact(List.of(
                        Map.of(":db/id", 1L, "text", "alpha #needle"),
                        Map.of(":db/id", 2L, "text", "needle without hash")));
                Object matches = conn.query(
                        "[:find [?e ...] :in $ ?q :where [(fulltext $ :text ?q) [[?e ?a ?v]]]]",
                        "needle");
                if (!List.of(1L).equals(matches)) {
                    throw new IllegalStateException("Unexpected analyzer UDF result: " + matches);
                }
            }

            Object rawConn = DatalevinInterop.createConnection(
                    dir.resolve("interop").toString(),
                    Map.of("name", Map.of(":db/valueType", ":db.type/string",
                                          ":db/unique", ":db.unique/identity")),
                    null);
            try {
                Object tx = DatalevinInterop.txData(List.of(
                        Map.of(":db/id", -1L, "name", "Ivy")));
                DatalevinInterop.coreInvoke("transact!", List.of(rawConn, tx));
                @SuppressWarnings("unchecked")
                Map<Object, Object> simulated = (Map<Object, Object>)
                        DatalevinInterop.connectionTxDataToSimulatedReport(
                                rawConn,
                                List.of(Map.of(":db/id", -2L, "name", "Sim")));
                if (!simulated.containsKey(Datalevin.kw("tx-data"))
                        || ((List<?>) simulated.get(Datalevin.kw("tx-data"))).isEmpty()) {
                    throw new IllegalStateException("Unexpected interop simulated report: " + simulated);
                }
                Object simulatedDb = simulated.get(Datalevin.kw("db-after"));
                @SuppressWarnings("unchecked")
                Map<Object, Object> simulatedPull = (Map<Object, Object>)
                        DatalevinInterop.databasePull(
                                simulatedDb,
                                DatalevinInterop.readEdn("[:name]"),
                                List.of(DatalevinInterop.keyword("name"), "Sim"));
                if (!"Sim".equals(simulatedPull.get(Datalevin.kw("name")))) {
                    throw new IllegalStateException(
                            "Unexpected interop simulated pull result: " + simulatedPull);
                }
                Object db = DatalevinInterop.connectionDb(rawConn);
                @SuppressWarnings("unchecked")
                List<Object> names = (List<Object>) DatalevinInterop.coreInvoke(
                        "q",
                        List.of(DatalevinInterop.readEdn("[:find [?name ...] :where [?e :name ?name]]"),
                                db));
                if (!List.of("Ivy").equals(names)) {
                    throw new IllegalStateException("Unexpected interop query result: " + names);
                }
            } finally {
                DatalevinInterop.closeConnection(rawConn);
            }

            try (KV kv = Datalevin.openKV(dir.resolve("kv").toString())) {
                kv.setEnvFlags(List.of("nosync"), true);
                if (!kv.getEnvFlags().contains(Datalevin.kw("nosync"))) {
                    throw new IllegalStateException("Expected :nosync env flag after set: " + kv.getEnvFlags());
                }
                Datalevin.setEnvFlags(kv, List.of(":nosync"), false);
                if (Datalevin.getEnvFlags(kv).contains(Datalevin.kw("nosync"))) {
                    throw new IllegalStateException("Unexpected :nosync env flag after clear: " + kv.getEnvFlags());
                }

                kv.openListDbi("list");
                kv.putListItems("list", "a", List.of(1L, 2L, 3L), KVType.STRING, KVType.LONG);
                kv.putListItems("list", "b", List.of(4L, 5L), KVType.STRING, KVType.LONG);

                List<?> list = kv.getList("list", "a", KVType.STRING, KVType.LONG);
                if (!List.of(1L, 2L, 3L).equals(list)) {
                    throw new IllegalStateException("Unexpected list result: " + list);
                }

                List<Object> visited = new ArrayList<>();
                kv.visitList("list", visited::add, "a", KVType.STRING, KVType.LONG);
                if (!List.of(1L, 2L, 3L).equals(visited)) {
                    throw new IllegalStateException("Unexpected visit-list result: " + visited);
                }

                List<?> filtered = kv.listRangeFilter(
                        "list",
                        (key, value) -> "b".equals(key) && ((Long) value) >= 4L,
                        RangeSpec.all(),
                        KVType.STRING,
                        RangeSpec.all(),
                        KVType.LONG,
                        null,
                        null);
                @SuppressWarnings("unchecked")
                List<Object> firstFiltered = (List<Object>) filtered.get(0);
                @SuppressWarnings("unchecked")
                List<Object> secondFiltered = (List<Object>) filtered.get(1);
                if (filtered.size() != 2
                        || !"b".equals(firstFiltered.get(0))
                        || !Long.valueOf(4L).equals(firstFiltered.get(1))
                        || !Long.valueOf(5L).equals(secondFiltered.get(1))) {
                    throw new IllegalStateException("Unexpected list-range-filter result: " + filtered);
                }
                List<?> pagedFiltered = kv.listRangeFilter(
                        "list",
                        (key, value) -> ((Long) value) >= 2L,
                        RangeSpec.all(),
                        KVType.STRING,
                        RangeSpec.all(),
                        KVType.LONG,
                        Integer.valueOf(2),
                        Integer.valueOf(1));
                if (!List.of(List.of("a", 3L), List.of("b", 4L)).equals(pagedFiltered)) {
                    throw new IllegalStateException("Unexpected paged list-range-filter result: " + pagedFiltered);
                }

                List<?> kept = kv.listRangeKeep(
                        "list",
                        (key, value) -> ((Long) value) >= 3L ? key + ":" + value : null,
                        RangeSpec.all(),
                        KVType.STRING,
                        RangeSpec.all(),
                        KVType.LONG,
                        null,
                        null);
                if (!List.of("a:3", "b:4", "b:5").equals(kept)) {
                    throw new IllegalStateException("Unexpected list-range-keep result: " + kept);
                }
                List<?> pagedKept = kv.listRangeKeep(
                        "list",
                        (key, value) -> ((Long) value) >= 2L ? key + ":" + value : null,
                        RangeSpec.all(),
                        KVType.STRING,
                        RangeSpec.all(),
                        KVType.LONG,
                        Integer.valueOf(1),
                        Integer.valueOf(2));
                if (!List.of("b:4").equals(pagedKept)) {
                    throw new IllegalStateException("Unexpected paged list-range-keep result: " + pagedKept);
                }

                Object some = kv.listRangeSome(
                        "list",
                        (key, value) -> ((Long) value) == 5L ? key + ":" + value : null,
                        RangeSpec.all(),
                        KVType.STRING,
                        RangeSpec.all(),
                        KVType.LONG);
                if (!"b:5".equals(some)) {
                    throw new IllegalStateException("Unexpected list-range-some result: " + some);
                }

                long filterCount = kv.listRangeFilterCount(
                        "list",
                        (key, value) -> "a".equals(key) && ((Long) value) >= 2L,
                        RangeSpec.all(),
                        KVType.STRING,
                        RangeSpec.all(),
                        KVType.LONG);
                if (filterCount != 2L) {
                    throw new IllegalStateException("Unexpected list-range-filter-count result: " + filterCount);
                }

                List<List<Object>> ranged = new ArrayList<>();
                kv.visitListRange(
                        "list",
                        (key, value) -> ranged.add(List.of(key, value)),
                        RangeSpec.closed("a", "b"),
                        KVType.STRING,
                        RangeSpec.closed(2L, 4L),
                        KVType.LONG);
                if (!List.of(List.of("a", 2L), List.of("a", 3L), List.of("b", 4L)).equals(ranged)) {
                    throw new IllegalStateException("Unexpected visit-list-range result: " + ranged);
                }

                List<Object> rawVisited = new ArrayList<>();
                kv.visitListRaw("list", raw -> rawVisited.add(raw.read(KVType.LONG)), "a", KVType.STRING);
                if (!List.of(1L, 2L, 3L).equals(rawVisited)) {
                    throw new IllegalStateException("Unexpected raw visit-list result: " + rawVisited);
                }

                List<List<Object>> rawRanged = new ArrayList<>();
                kv.visitListRangeRaw(
                        "list",
                        raw -> rawRanged.add(List.of(raw.readKey(KVType.STRING), raw.readValue(KVType.LONG))),
                        RangeSpec.closed("a", "b"),
                        KVType.STRING,
                        RangeSpec.closed(2L, 4L),
                        KVType.LONG);
                if (!List.of(List.of("a", 2L), List.of("a", 3L), List.of("b", 4L)).equals(rawRanged)) {
                    throw new IllegalStateException("Unexpected raw visit-list-range result: " + rawRanged);
                }

                List<?> rawFiltered = kv.listRangeFilterRaw(
                        "list",
                        raw -> "b".equals(raw.readKey(KVType.STRING))
                                && ((Long) raw.readValue(KVType.LONG)) >= 4L,
                        RangeSpec.all(),
                        KVType.STRING,
                        RangeSpec.all(),
                        KVType.LONG,
                        null,
                        null);
                if (!List.of(List.of("b", 4L), List.of("b", 5L)).equals(rawFiltered)) {
                    throw new IllegalStateException("Unexpected raw list-range-filter result: " + rawFiltered);
                }

                List<?> rawKept = kv.listRangeKeepRaw(
                        "list",
                        raw -> ((Long) raw.readValue(KVType.LONG)) >= 3L
                                ? raw.readKey(KVType.STRING) + ":" + raw.readValue(KVType.LONG)
                                : null,
                        RangeSpec.all(),
                        KVType.STRING,
                        RangeSpec.all(),
                        KVType.LONG,
                        null,
                        null);
                if (!List.of("a:3", "b:4", "b:5").equals(rawKept)) {
                    throw new IllegalStateException("Unexpected raw list-range-keep result: " + rawKept);
                }

                Object rawSome = kv.listRangeSomeRaw(
                        "list",
                        raw -> ((Long) raw.readValue(KVType.LONG)) == 5L
                                ? raw.readKey(KVType.STRING) + ":" + raw.readValue(KVType.LONG)
                                : null,
                        RangeSpec.all(),
                        KVType.STRING,
                        RangeSpec.all(),
                        KVType.LONG);
                if (!"b:5".equals(rawSome)) {
                    throw new IllegalStateException("Unexpected raw list-range-some result: " + rawSome);
                }

                long rawFilterCount = kv.listRangeFilterCountRaw(
                        "list",
                        raw -> "a".equals(raw.readKey(KVType.STRING))
                                && ((Long) raw.readValue(KVType.LONG)) >= 2L,
                        RangeSpec.all(),
                        KVType.STRING,
                        RangeSpec.all(),
                        KVType.LONG);
                if (rawFilterCount != 2L) {
                    throw new IllegalStateException("Unexpected raw list-range-filter-count result: " + rawFilterCount);
                }

                try (VectorIndex index = Datalevin.newVectorIndex(kv, Datalevin.vectorOptions(2))) {
                    index.addVec("vec-1", List.of(1.0, 0.0));
                    index.addVec("vec-2", List.of(0.0, 1.0));
                    if (!index.vecIndexed("vec-1")) {
                        throw new IllegalStateException("Vector ref was not indexed");
                    }
                    List<?> vectorResult = index.searchVec(List.of(1.0, 0.0), Map.of(":top", 1L));
                    if (!List.of("vec-1").equals(vectorResult)) {
                        throw new IllegalStateException("Unexpected vector search result: " + vectorResult);
                    }
                    if (!Long.valueOf(2L).equals(index.info().get(Datalevin.kw("dimensions")))) {
                        throw new IllegalStateException("Unexpected vector index info: " + index.info());
                    }
                    index.forceCheckpoint();
                    if (index.checkpointState() == null) {
                        throw new IllegalStateException("Missing vector checkpoint state");
                    }
                    Datalevin.reIndex(index);
                    index.removeVec("vec-1");
                    if (index.vecIndexed("vec-1")) {
                        throw new IllegalStateException("Vector ref was not removed");
                    }
                    index.clear();
                    if (!index.closed()) {
                        throw new IllegalStateException("Vector index was not closed after clear");
                    }
                }
            }

            System.out.println("Java jar test succeeded!");
        } finally {
            deleteRecursively(dir);
        }
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
}
