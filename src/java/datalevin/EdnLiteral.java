package datalevin;

final class EdnLiteral {

    private final String edn;

    EdnLiteral(String edn) {
        this.edn = edn;
    }

    String value() {
        return edn;
    }
}
