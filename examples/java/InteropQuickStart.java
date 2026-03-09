import datalevin.DatalevinInterop;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public final class InteropQuickStart {

    private InteropQuickStart() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("datalevin-java-interop-quickstart");

        Map<Object, Object> schema = Map.of(
                DatalevinInterop.keyword("person/name"),
                Map.of(DatalevinInterop.keyword(":db/valueType"), DatalevinInterop.keyword(":db.type/string"),
                       DatalevinInterop.keyword(":db/unique"), DatalevinInterop.keyword(":db.unique/identity")),
                DatalevinInterop.keyword("person/age"),
                Map.of(DatalevinInterop.keyword(":db/valueType"), DatalevinInterop.keyword(":db.type/long")));

        Object conn = DatalevinInterop.createConnection(dir.toString(), schema, null);
        try {
            Object tx = DatalevinInterop.txData(List.of(
                    Map.of(DatalevinInterop.keyword(":db/id"), -1L,
                           DatalevinInterop.keyword("person/name"), "Ivy",
                           DatalevinInterop.keyword("person/age"), 41L),
                    Map.of(DatalevinInterop.keyword(":db/id"), -2L,
                           DatalevinInterop.keyword("person/name"), "Noah",
                           DatalevinInterop.keyword("person/age"), 29L)));

            DatalevinInterop.coreInvoke("transact!", List.of(conn, tx));

            Object db = DatalevinInterop.connectionDb(conn);
            Object ivy = DatalevinInterop.coreInvoke(
                    "pull",
                    List.of(db,
                            DatalevinInterop.readEdn("[:person/name :person/age]"),
                            DatalevinInterop.lookupRef(
                                    List.of(DatalevinInterop.keyword("person/name"), "Ivy"))));
            Object names = DatalevinInterop.coreInvoke(
                    "q",
                    List.of(DatalevinInterop.readEdn(
                                    "[:find [?name ...] :where [?e :person/name ?name]]"),
                            db));

            System.out.println("Names class: " + names.getClass().getName());
            System.out.println("Names: " + names);
            System.out.println("Pulled entity class: " + ivy.getClass().getName());
            System.out.println("Pulled entity: " + ivy);
        } finally {
            DatalevinInterop.closeConnection(conn);
            System.out.println("Connection closed: " + DatalevinInterop.connectionClosed(conn));
            ExampleSupport.deleteTree(dir);
        }
    }
}
