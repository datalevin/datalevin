import datalevin.Datalevin;
import datalevin.KV;
import datalevin.KVType;
import datalevin.RangeSpec;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public final class KVQuickStart {

    private KVQuickStart() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("datalevin-java-kv-quickstart");

        try (KV kv = Datalevin.openKV(dir.toString())) {
            kv.openDbi("people");
            kv.openListDbi("tags");

            kv.transact("people",
                        List.of(
                                List.of(":put", 1001L, "Alice"),
                                List.of(":put", 1002L, "Bob"),
                                List.of(":put", 1003L, "Cara")),
                        KVType.LONG,
                        KVType.STRING);

            kv.putListItems("tags", "alice", List.of("admin", "editor"), KVType.STRING, KVType.STRING);
            kv.putListItems("tags", "bob", List.of("reader"), KVType.STRING, KVType.STRING);

            Object bob = kv.getValue("people", 1002L, KVType.LONG, KVType.STRING, true);
            List<?> people = kv.getRange("people",
                                         RangeSpec.closed(1001L, 1003L),
                                         KVType.LONG,
                                         KVType.STRING,
                                         null,
                                         null);
            List<?> aliceTags = kv.getList("tags", "alice", KVType.STRING, KVType.STRING);

            System.out.println("Bob: " + bob);
            System.out.println("People: " + people);
            System.out.println("Alice tags: " + aliceTags);
        } finally {
            ExampleSupport.deleteTree(dir);
        }
    }
}
