package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.JFRErrorContext;

/**
 * Exception thrown when query execution fails.
 * 
 * This exception is used for failures during query processing,
 * including parsing, plan execution, and result handling.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class QueryExecutionException extends PlanExecutionException {
    
    public QueryExecutionException(String message) {
        super(message);
    }
    
    public QueryExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public QueryExecutionException(String message, JFRErrorContext errorContext, Throwable cause) {
        super(message, errorContext, cause);
    }
}
