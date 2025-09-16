package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.plan.JFRErrorContext;
import me.bechberger.jfr.extended.plan.plans.StreamingQueryPlan;
import me.bechberger.jfr.extended.engine.exception.QueryEvaluationException;
import me.bechberger.jfr.extended.engine.exception.QueryExecutionException;
import me.bechberger.jfr.extended.plan.exception.PlanException.ExecutionPhase;

/**
 * Factory for creating and wrapping plan exceptions with proper context.
 * 
 * This factory provides convenient methods for wrapping existing exceptions
 * and creating new plan-specific exceptions with appropriate context.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class PlanExceptionFactory {
    
    /**
     * Wrap a caught exception in plan execution context.
     * This is the main method to use in plan execute() methods when catching exceptions.
     */
    public static PlanException wrapException(Exception exception, StreamingQueryPlan plan, 
                                            ExecutionPhase phase, String context) {
        if (exception instanceof QueryEvaluationException qee) {
            return PlanException.wrap(qee, plan, plan.getSourceNode(), phase);
        } else if (exception instanceof QueryExecutionException qex) {
            return PlanException.wrap(qex, plan, plan.getSourceNode(), phase);
        } else if (exception instanceof PlanException pe) {
            // Already a PlanException, just return it (avoid double wrapping)
            return pe;
        } else {
            // Wrap any other exception
            return PlanException.wrap(exception, plan, phase);
        }
    }
    
    /**
     * Create a new plan exception for data access errors.
     */
    public static DataAccessException createDataAccessError(String resource, 
                                                           DataAccessException.AccessType accessType,
                                                           StreamingQueryPlan plan, 
                                                           ExecutionPhase phase) {
        return new DataAccessException("Cannot access " + accessType.getDescription() + ": " + resource, 
                                     resource, accessType, (String[]) null);
    }
    
    /**
     * Create a new plan exception for data access errors with suggestions.
     */
    public static DataAccessException createDataAccessError(String resource, 
                                                           DataAccessException.AccessType accessType,
                                                           String[] alternatives,
                                                           StreamingQueryPlan plan, 
                                                           ExecutionPhase phase) {
        return new DataAccessException("Cannot access " + accessType.getDescription() + ": " + resource, 
                                     resource, accessType, alternatives);
    }
    
    /**
     * Create a new plan exception for type errors.
     */
    public static TypeException createTypeError(String message, 
                                              me.bechberger.jfr.extended.table.CellType expectedType,
                                              me.bechberger.jfr.extended.table.CellType actualType,
                                              StreamingQueryPlan plan, 
                                              ExecutionPhase phase) {
        return new TypeException(message, expectedType, actualType, (Object) null);
    }
    
    /**
     * Create a table not found exception.
     */
    public static DataAccessException createTableNotFoundError(String tableName, 
                                                              String[] availableTables,
                                                              StreamingQueryPlan plan) {
        return createDataAccessError(tableName, DataAccessException.AccessType.TABLE_LOOKUP, 
                                   availableTables, plan, ExecutionPhase.INPUT_VALIDATION);
    }
    
    /**
     * Create a column not found exception.
     */
    public static DataAccessException createColumnNotFoundError(String columnName, 
                                                               String[] availableColumns,
                                                               StreamingQueryPlan plan) {
        return createDataAccessError(columnName, DataAccessException.AccessType.COLUMN_ACCESS, 
                                   availableColumns, plan, ExecutionPhase.DATA_PROCESSING);
    }
    
    /**
     * Create a filter evaluation error.
     */
    public static PlanException createFilterError(String filterCondition, Exception cause, 
                                                 StreamingQueryPlan plan) {
        String message = "Filter evaluation failed: " + filterCondition;
        return wrapException(cause, plan, ExecutionPhase.DATA_PROCESSING, message);
    }
    
    /**
     * Create a projection evaluation error.
     */
    public static PlanException createProjectionError(String expression, Exception cause, 
                                                     StreamingQueryPlan plan) {
        String message = "Projection evaluation failed: " + expression;
        return wrapException(cause, plan, ExecutionPhase.DATA_PROCESSING, message);
    }
    
    /**
     * Create a scan operation error.
     */
    public static PlanException createScanError(String source, Exception cause, 
                                               StreamingQueryPlan plan) {
        String message = "Scan operation failed on: " + source;
        return wrapException(cause, plan, ExecutionPhase.INPUT_VALIDATION, message);
    }
    
    /**
     * Create a raw query execution error.
     */
    public static PlanException createRawQueryError(String query, Exception cause, 
                                                   StreamingQueryPlan plan) {
        String message = "Raw query execution failed: " + query;
        return wrapException(cause, plan, ExecutionPhase.DATA_PROCESSING, message);
    }
    
    /**
     * Create a plan initialization error.
     */
    public static PlanException createInitializationError(String planType, Exception cause, 
                                                         StreamingQueryPlan plan) {
        String message = planType + " initialization failed";
        return wrapException(cause, plan, ExecutionPhase.INITIALIZATION, message);
    }
    
    /**
     * Create a plan cleanup error.
     */
    public static PlanException createCleanupError(String planType, Exception cause, 
                                                  StreamingQueryPlan plan) {
        String message = planType + " cleanup failed";
        return wrapException(cause, plan, ExecutionPhase.CLEANUP, message);
    }
    
    /**
     * Create a generic plan execution error.
     */
    public static PlanException createExecutionError(String message, StreamingQueryPlan plan, 
                                                    ExecutionPhase phase) {
        return PlanException.create(message, plan, plan.getSourceNode(), null, phase);
    }
    
    /**
     * Create a plan execution error with context.
     */
    public static PlanException createExecutionError(String message, StreamingQueryPlan plan, 
                                                    ExecutionPhase phase, JFRErrorContext context) {
        return PlanException.create(message, plan, plan.getSourceNode(), context, phase);
    }
}
