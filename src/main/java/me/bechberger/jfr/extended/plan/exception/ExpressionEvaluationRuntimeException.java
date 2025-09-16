package me.bechberger.jfr.extended.plan.exception;

/**
 * Runtime exception thrown when expression evaluation fails.
 * 
 * This runtime exception is used for failures during expression evaluation
 * in contexts where checked exceptions cannot be thrown.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class ExpressionEvaluationRuntimeException extends RuntimeException {
    
    public ExpressionEvaluationRuntimeException(String message) {
        super(message);
    }
    
    public ExpressionEvaluationRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
