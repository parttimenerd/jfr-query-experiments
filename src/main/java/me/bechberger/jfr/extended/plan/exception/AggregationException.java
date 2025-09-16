package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.JFRErrorContext;

/**
 * Exception thrown when aggregation processing fails.
 * 
 * This exception is used for failures during aggregate function evaluation,
 * grouping operations, and HAVING clause processing.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class AggregationException extends PlanExecutionException {
    
    public AggregationException(String message) {
        super(message);
    }
    
    public AggregationException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public AggregationException(String message, JFRErrorContext errorContext, Throwable cause) {
        super(message, errorContext, cause);
    }
}
