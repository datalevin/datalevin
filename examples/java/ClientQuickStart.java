import datalevin.Client;
import datalevin.DatabaseType;
import datalevin.Datalevin;

import java.util.Map;
import java.util.UUID;

public final class ClientQuickStart {

    private static final String DEFAULT_URI = "dtlv://datalevin:datalevin@localhost";

    private ClientQuickStart() {
    }

    public static void main(String[] args) {
        String uri = System.getenv().getOrDefault("DATALEVIN_URI", DEFAULT_URI);
        String dbName = "java-quickstart-" + UUID.randomUUID();

        try (Client client = Datalevin.newClient(uri)) {
            boolean created = false;
            boolean opened = false;

            try {
                client.createDatabase(dbName, DatabaseType.DATALOG);
                created = true;

                Map<?, ?> openInfo = client.openDatabaseInfo(dbName, DatabaseType.DATALOG, null, null);
                opened = true;

                Object systemDb = client.querySystem(
                        "[:find ?db . :in $ ?db :where [?e :database/name ?db]]",
                        dbName);

                System.out.println("Client id: " + client.clientId());
                System.out.println("Databases: " + client.listDatabases());
                System.out.println("Open info: " + openInfo);
                System.out.println("System query result: " + systemDb);
                System.out.println("Connected clients: " + client.showClients().keySet());
            } finally {
                if (opened) {
                    client.closeDatabase(dbName);
                }
                if (created) {
                    client.dropDatabase(dbName);
                }
            }
        }
    }
}
