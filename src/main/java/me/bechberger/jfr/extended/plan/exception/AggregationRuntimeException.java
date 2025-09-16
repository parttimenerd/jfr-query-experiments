package me.bechberger.jfr.extended.plan.exception;

/**
 * Runtime exception thrown when aggregation processing fails.
 * 
 * This runtime exception is used for failures during aggregate function evaluation
 * in contexts where checked exceptions cannot be thrown (like lambda expressions).
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class AggregationRuntimeException extends RuntimeException {
    
    public AggregationRuntimeException(String message) {
        super(message);
    }
    
    public AggregationRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
