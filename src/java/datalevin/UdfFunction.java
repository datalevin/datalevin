package datalevin;

import java.util.List;

/**
 * Functional interface for Java-backed Datalevin UDFs.
 *
 * <p>The provided argument list contains the raw Datalevin call arguments. For
 * transaction functions the first value is the immutable database value.
 */
@FunctionalInterface
public interface UdfFunction {

    Object invoke(List<?> args);
}
