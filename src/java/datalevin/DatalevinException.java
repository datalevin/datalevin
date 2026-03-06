package datalevin;

/**
 * Exception thrown when a Datalevin JSON API request fails.
 */
public final class DatalevinException extends RuntimeException {

    private final String errorType;
    private final Object data;

    DatalevinException(String message, String errorType, Object data) {
        super(message);
        this.errorType = errorType;
        this.data = data;
    }

    /**
     * Returns the error type reported by the server, when available.
     */
    public String getErrorType() {
        return errorType;
    }

    /**
     * Returns structured error data reported by the server, when available.
     */
    public Object getData() {
        return data;
    }
}
