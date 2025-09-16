package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.plan.JFRErrorContext;
import me.bechberger.jfr.extended.plan.plans.StreamingQueryPlan;
import me.bechberger.jfr.extended.engine.exception.QueryEvaluationException;
import me.bechberger.jfr.extended.engine.exception.QueryExecutionException;

/**
 * Exception wrapper that associates streaming query plans with underlying execution errors.
 * 
 * This exception wraps existing QueryEvaluationException and QueryExecutionException instances
 * and adds plan-specific context, making it easier to debug issues in the streaming architecture
 * by knowing exactly which plan caused the problem.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class PlanException extends Exception {
    
    /** The plan that was executing when the error occurred */
    private final StreamingQueryPlan plan;
    
    /** The AST node associated with this plan */
    private final ASTNode astNode;
    
    /** Enhanced error context with JFR-specific information */
    private final JFRErrorContext errorContext;
    
    /** The phase of plan execution where the error occurred */
    private final ExecutionPhase executionPhase;
    
    /** The original exception that was wrapped */
    private final Exception originalException;
    
    /**
     * Execution phases for better error categorization.
     */
    public enum ExecutionPhase {
        INITIALIZATION("Plan initialization"),
        INPUT_VALIDATION("Input validation"),
        DATA_PROCESSING("Data processing"),
        OUTPUT_GENERATION("Output generation"),
        CLEANUP("Cleanup");
        
        private final String description;
        
        ExecutionPhase(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Wrap a QueryEvaluationException with plan context.
     */
    public static PlanException wrap(QueryEvaluationException queryException, StreamingQueryPlan plan, 
                                    ASTNode astNode, ExecutionPhase phase) {
        return new PlanException(queryException.getMessage(), plan, astNode, null, phase, queryException);
    }
    
    /**
     * Wrap a QueryExecutionException with plan context.
     */
    public static PlanException wrap(QueryExecutionException queryException, StreamingQueryPlan plan, 
                                    ASTNode astNode, ExecutionPhase phase) {
        return new PlanException(queryException.getMessage(), plan, astNode, null, phase, queryException);
    }
    
    /**
     * Wrap any exception with plan context.
     */
    public static PlanException wrap(Exception exception, StreamingQueryPlan plan, ExecutionPhase phase) {
        return new PlanException(exception.getMessage(), plan, null, null, phase, exception);
    }
    
    /**
     * Create a new plan exception with full context.
     */
    public static PlanException create(String message, StreamingQueryPlan plan, ASTNode astNode, 
                                      JFRErrorContext errorContext, ExecutionPhase phase) {
        return new PlanException(message, plan, astNode, errorContext, phase, null);
    }
    
    /**
     * Private constructor - use static factory methods instead.
     */
    private PlanException(String message, StreamingQueryPlan plan, ASTNode astNode, 
                         JFRErrorContext errorContext, ExecutionPhase executionPhase, Exception originalException) {
        super(message, originalException);
        this.plan = plan;
        this.astNode = astNode;
        this.errorContext = errorContext;
        this.executionPhase = executionPhase;
        this.originalException = originalException;
    }
    
    /**
     * Get the plan that was executing when the error occurred.
     */
    public StreamingQueryPlan getPlan() {
        return plan;
    }
    
    /**
     * Get the AST node associated with this plan.
     */
    public ASTNode getAstNode() {
        return astNode;
    }
    
    /**
     * Get the enhanced error context.
     */
    public JFRErrorContext getErrorContext() {
        return errorContext;
    }
    
    /**
     * Get the execution phase where the error occurred.
     */
    public ExecutionPhase getExecutionPhase() {
        return executionPhase;
    }
    
    /**
     * Get the original exception that was wrapped (if any).
     */
    public Exception getOriginalException() {
        return originalException;
    }
    
    /**
     * Check if this exception wraps a QueryEvaluationException.
     */
    public boolean isQueryEvaluationError() {
        return originalException instanceof QueryEvaluationException;
    }
    
    /**
     * Get the wrapped QueryEvaluationException if present.
     */
    public QueryEvaluationException getQueryEvaluationException() {
        return isQueryEvaluationError() ? (QueryEvaluationException) originalException : null;
    }
    
    /**
     * Check if this exception wraps a QueryExecutionException.
     */
    public boolean isQueryExecutionError() {
        return originalException instanceof QueryExecutionException;
    }
    
    /**
     * Get the wrapped QueryExecutionException if present.
     */
    public QueryExecutionException getQueryExecutionException() {
        return isQueryExecutionError() ? (QueryExecutionException) originalException : null;
    }
    
    /**
     * Get a detailed error message including all available context.
     */
    public String getDetailedMessage() {
        StringBuilder sb = new StringBuilder();
        
        // Basic error message
        sb.append(getMessage());
        
        // Add execution phase context
        if (executionPhase != null) {
            sb.append(" (during ").append(executionPhase.getDescription()).append(")");
        }
        
        // Add plan context
        if (plan != null) {
            sb.append(" in ").append(plan.getClass().getSimpleName());
        }
        
        // Add AST context
        if (astNode != null) {
            sb.append(" at AST node: ").append(astNode.getClass().getSimpleName());
        }
        
        // Add JFR error context
        if (errorContext != null) {
            sb.append(" - ").append(errorContext.createContextualErrorMessage());
        }
        
        // Add original exception details
        if (originalException != null) {
            sb.append(" - Original: ").append(originalException.getClass().getSimpleName());
        }
        
        return sb.toString();
    }
    
    /**
     * Get the plan class name for error categorization.
     */
    public String getPlanType() {
        return plan != null ? plan.getClass().getSimpleName() : "UnknownPlan";
    }
    
    /**
     * Check if this exception has enhanced context.
     */
    public boolean hasEnhancedContext() {
        return errorContext != null || astNode != null || originalException != null;
    }
    
    /**
     * Create a formatted error report with all available information.
     */
    public String createErrorReport() {
        StringBuilder report = new StringBuilder();
        
        report.append("=== PLAN EXECUTION ERROR ===\n");
        report.append("Message: ").append(getMessage()).append("\n");
        
        if (plan != null) {
            report.append("Plan Type: ").append(plan.getClass().getSimpleName()).append("\n");
        }
        
        if (executionPhase != null) {
            report.append("Execution Phase: ").append(executionPhase.getDescription()).append("\n");
        }
        
        if (astNode != null) {
            report.append("AST Node: ").append(astNode.getClass().getSimpleName()).append("\n");
        }
        
        if (errorContext != null) {
            report.append("Error Context: ").append(errorContext.createContextualErrorMessage()).append("\n");
        }
        
        if (originalException != null) {
            report.append("Original Exception: ").append(originalException.getClass().getSimpleName()).append("\n");
            report.append("Original Message: ").append(originalException.getMessage()).append("\n");
            
            // Add specific details for QueryEvaluationException
            if (isQueryEvaluationError()) {
                QueryEvaluationException qee = getQueryEvaluationException();
                report.append("Evaluation Error Type: ").append(qee.getErrorType()).append("\n");
                if (qee.getEvaluationContext() != null) {
                    report.append("Evaluation Context: ").append(qee.getEvaluationContext()).append("\n");
                }
            }
        }
        
        if (getCause() != null && getCause() != originalException) {
            report.append("Root Cause: ").append(getCause().getClass().getSimpleName())
                  .append(" - ").append(getCause().getMessage()).append("\n");
        }
        
        report.append("=============================");
        
        return report.toString();
    }
}
