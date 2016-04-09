package io.druid.embedded.exception;

/**
 * Created by srira on 4/7/2016.
 */
public class UnhandledQueryException extends Exception {
    private static final long serialVersionUID = 98523789562397L;

    public UnhandledQueryException() {
    }

    public UnhandledQueryException(String message) {
        super(message);
    }

    public UnhandledQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnhandledQueryException(Throwable cause) {
        super(cause);
    }

    public UnhandledQueryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
