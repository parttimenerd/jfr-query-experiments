package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.plan.JFRErrorContext;

/**
 * Exception thrown when JOIN operation processing fails.
 * 
 * This exception is used for failures during the actual join processing,
 * such as type mismatches, memory issues, or algorithmic failures.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class JoinProcessingException extends JoinExecutionException {
    
    public JoinProcessingException(String message, Throwable cause) {
        super("JOIN processing failed: " + message, cause);
    }
    
    public JoinProcessingException(String message, JFRErrorContext errorContext, Throwable cause) {
        super("JOIN processing failed: " + message, errorContext, cause);
    }
}
