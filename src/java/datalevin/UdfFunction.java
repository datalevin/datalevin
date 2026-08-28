package datalevin;

import java.util.List;

/**
 * Functional interface for Java-backed Datalevin UDFs.
 *
 * <p>The provided argument list contains Datalevin call arguments. Raw
 * database values are represented by the bridge-safe {@link DatabaseValue};
 * for transaction functions it is the first argument.
 */
@FunctionalInterface
public interface UdfFunction {

    Object invoke(List<?> args);
}
