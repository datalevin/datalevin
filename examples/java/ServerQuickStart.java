import datalevin.DatalevinServer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class ServerQuickStart {

    private ServerQuickStart() {
    }

    public static void main(String[] args) throws Exception {
        Path dir = Files.createTempDirectory("datalevin-java-server-quickstart");

        try (DatalevinServer server = DatalevinServer.create(Map.of(
                "host", "127.0.0.1",
                "port", 0,
                "root", dir.toString(),
                "verbose", true))) {
            server.start();
            System.out.println("Started in-process Datalevin server: " + server.isOpen());
        } finally {
            ExampleSupport.deleteTree(dir);
        }
    }
}
