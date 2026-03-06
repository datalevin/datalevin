package test_jar;

import datalevin.Connection;
import datalevin.Datalevin;
import datalevin.DatalogQuery;
import datalevin.PullSelector;
import datalevin.Schema;
import datalevin.Tx;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public final class JavaSmoke {

    private JavaSmoke() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("datalevin-java-smoke-");
        try {
            try (Connection conn = Datalevin.createConn(
                    dir.toString(),
                    Datalevin.schema()
                            .attr("name", Schema.attribute()
                                    .valueType("db.type/string")
                                    .unique("db.unique/identity"))
                            .attr("age", Schema.attribute().valueType("db.type/long")))) {

                conn.transact(Datalevin.tx()
                        .entity(Tx.entity(-1).put("name", "Alice").put("age", 30))
                        .entity(Tx.entity(-2).put("name", "Bob").put("age", 25)));

                DatalogQuery adultsQuery = Datalevin.query()
                        .findAll("?name")
                        .whereDatom(Datalevin.var("e"), "name", Datalevin.var("name"))
                        .whereDatom(Datalevin.var("e"), "age", Datalevin.var("age"))
                        .wherePredicate(">=", Datalevin.var("age"), 30);

                List<Object> adults = Datalevin.listResult(conn.query(adultsQuery));
                if (!List.of("Alice").equals(adults)) {
                    throw new IllegalStateException("Unexpected adult query result: " + adults);
                }

                PullSelector selector = Datalevin.pull().attr("name").attr("age");
                Map<String, Object> alice = conn.pull(selector, Datalevin.listOf(":name", "Alice"));
                if (!"Alice".equals(alice.get(":name"))) {
                    throw new IllegalStateException("Unexpected pull result: " + alice);
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
