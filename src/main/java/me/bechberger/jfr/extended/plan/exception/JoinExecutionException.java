package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.JFRErrorContext;

/**
 * Exception thrown when JOIN operation fails.
 * 
 * This is the base class for all JOIN-related exceptions.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class JoinExecutionException extends PlanExecutionException {
    
    public JoinExecutionException(String message) {
        super(message);
    }
    
    public JoinExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public JoinExecutionException(String message, JFRErrorContext errorContext, Throwable cause) {
        super(message, errorContext, cause);
    }
}
