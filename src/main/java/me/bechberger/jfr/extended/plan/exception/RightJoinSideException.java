package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.plan.JFRErrorContext;

/**
 * Exception thrown when the right side of a JOIN operation fails.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class RightJoinSideException extends JoinExecutionException {
    
    public RightJoinSideException(String message, Throwable cause) {
        super("Right side of JOIN failed: " + message, cause);
    }
    
    public RightJoinSideException(String message, JFRErrorContext errorContext, Throwable cause) {
        super("Right side of JOIN failed: " + message, errorContext, cause);
    }
}
