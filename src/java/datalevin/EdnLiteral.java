package datalevin;

import java.util.Objects;

final class EdnLiteral {

    private final String edn;

    EdnLiteral(String edn) {
        this.edn = edn;
    }

    String value() {
        return edn;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof EdnLiteral that)) {
            return false;
        }
        return Objects.equals(edn, that.edn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edn);
    }
}
