package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.plan.PlanExecutionException;
import me.bechberger.jfr.extended.plan.JFRErrorContext;

/**
 * Exception thrown when variable evaluation fails.
 * 
 * This exception is used for failures during variable assignment,
 * variable lookup, and expression evaluation.
 * 
 * @author JFR Query Plan Architecture
 * @since 3.0
 */
public class VariableEvaluationException extends PlanExecutionException {
    
    public VariableEvaluationException(String message) {
        super(message);
    }
    
    public VariableEvaluationException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public VariableEvaluationException(String message, JFRErrorContext errorContext, Throwable cause) {
        super(message, errorContext, cause);
    }
}
