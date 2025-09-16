package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.JFRErrorContext;

/**
 * Exception thrown when expression evaluation fails.
 * 
 * This exception is used for failures during expression processing,
 * including binary operations, function calls, and complex expressions.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class ExpressionEvaluationException extends PlanExecutionException {
    
    public ExpressionEvaluationException(String message) {
        super(message);
    }
    
    public ExpressionEvaluationException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public ExpressionEvaluationException(String message, JFRErrorContext errorContext, Throwable cause) {
        super(message, errorContext, cause);
    }
}
