import datalevin.Connection;
import datalevin.DatalogQuery;
import datalevin.Datalevin;
import datalevin.PullSelector;
import datalevin.Schema;
import datalevin.Tx;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public final class DatalogQuickStart {

    private DatalogQuickStart() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("datalevin-java-quickstart");

        try (Connection conn = Datalevin.createConn(
                dir.toString(),
                Datalevin.schema()
                        .attr("person/name",
                                Schema.attribute()
                                        .valueType("db.type/string")
                                        .unique("db.unique/identity"))
                        .attr("person/age",
                                Schema.attribute()
                                        .valueType("db.type/long")))) {

            conn.transact(Datalevin.tx()
                    .entity(Tx.entity(-1)
                            .put("person/name", "Alice")
                            .put("person/age", 30))
                    .entity(Tx.entity(-2)
                            .put("person/name", "Bob")
                            .put("person/age", 25)));

            DatalogQuery adultsQuery = Datalevin.query()
                    .findAll("?name")
                    .whereDatom(Datalevin.var("e"), "person/name", Datalevin.var("name"))
                    .whereDatom(Datalevin.var("e"), "person/age", Datalevin.var("age"))
                    .wherePredicate(">=", Datalevin.var("age"), 30);

            PullSelector personSelector = Datalevin.pull()
                    .attr("person/name")
                    .attr("person/age");

            List<Object> adults = Datalevin.listResult(conn.query(adultsQuery));
            Map<String, Object> alice = conn.pull(
                    personSelector,
                    Datalevin.listOf(":person/name", "Alice"));

            System.out.println("Adults: " + adults);
            System.out.println("Alice: " + alice);
        } finally {
            deleteTree(dir);
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (root == null || Files.notExists(root)) {
            return;
        }

        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
