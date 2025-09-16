package me.bechberger.jfr.extended.plan;

/**
 * Exception thrown during query plan execution.
 * 
 * This exception wraps errors that occur during the execution
 * of streaming query plans, providing enhanced error context
 * and debugging information.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class PlanExecutionException extends Exception {
    private final JFRErrorContext errorContext;
    
    public PlanExecutionException(String message, JFRErrorContext errorContext) {
        super(message);
        this.errorContext = errorContext;
    }
    
    public PlanExecutionException(String message, JFRErrorContext errorContext, Throwable cause) {
        super(message, cause);
        this.errorContext = errorContext;
    }
    
    public PlanExecutionException(String message, Throwable cause) {
        super(message, cause);
        this.errorContext = null;
    }
    
    public PlanExecutionException(String message) {
        super(message);
        this.errorContext = null;
    }
    
    /**
     * Get the enhanced error context.
     */
    public JFRErrorContext getErrorContext() {
        return errorContext;
    }
    
    /**
     * Get a detailed error message including context.
     */
    public String getDetailedMessage() {
        if (errorContext != null) {
            return getMessage() + " - " + errorContext.createContextualErrorMessage();
        }
        return getMessage();
    }
    
    @Override
    public String toString() {
        return getDetailedMessage();
    }
}
