package me.bechberger.jfr.extended.plan.exception;

/**
 * Runtime exception thrown when sort operation fails.
 * 
 * This runtime exception is used for failures during ORDER BY processing
 * in contexts where checked exceptions cannot be thrown (like Comparator lambdas).
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class SortRuntimeException extends RuntimeException {
    
    public SortRuntimeException(String message) {
        super(message);
    }
    
    public SortRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
