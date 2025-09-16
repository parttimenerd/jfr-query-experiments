package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.plan.JFRErrorContext;

/**
 * Exception thrown when the left side of a JOIN operation fails.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class LeftJoinSideException extends JoinExecutionException {
    
    public LeftJoinSideException(String message, Throwable cause) {
        super("Left side of JOIN failed: " + message, cause);
    }
    
    public LeftJoinSideException(String message, JFRErrorContext errorContext, Throwable cause) {
        super("Left side of JOIN failed: " + message, errorContext, cause);
    }
}
