package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.JFRErrorContext;

/**
 * Exception thrown when statement execution fails.
 * 
 * This exception is used for failures during statement processing,
 * including query statements, assignments, and view definitions.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class StatementExecutionException extends PlanExecutionException {
    
    public StatementExecutionException(String message) {
        super(message);
    }
    
    public StatementExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public StatementExecutionException(String message, JFRErrorContext errorContext, Throwable cause) {
        super(message, errorContext, cause);
    }
}
