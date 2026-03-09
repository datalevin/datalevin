import datalevin.Connection;
import datalevin.DatalogQuery;
import datalevin.Datalevin;
import datalevin.PullSelector;
import datalevin.Schema;
import datalevin.Tx;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

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
                                        .valueType(Schema.ValueType.STRING)
                                        .unique(Schema.Unique.IDENTITY))
                        .attr("person/age",
                                Schema.attribute()
                                        .valueType(Schema.ValueType.LONG)))) {

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

            List<String> adults = conn.queryCollection(adultsQuery, String.class);
            Map<?, ?> alice = conn.pull(
                    personSelector,
                    Datalevin.listOf(Datalevin.kw("person/name"), "Alice"));

            System.out.println("Adults: " + adults);
            System.out.println("Alice: " + alice);
        } finally {
            ExampleSupport.deleteTree(dir);
        }
    }
}
