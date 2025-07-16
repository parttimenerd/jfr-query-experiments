package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Exception thrown when there are issues with query execution infrastructure.
 * 
 * This exception is used for problems related to query executors, missing dependencies,
 * configuration issues, or infrastructure failures that prevent query execution.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class QueryExecutorException extends QueryExecutionException {
    
    private final String executorType;
    private final ExecutorErrorType errorType;
    
    /**
     * Enum defining different types of executor errors
     */
    public enum ExecutorErrorType {
        MISSING_EXECUTOR("Required executor not available"),
        CONFIGURATION_ERROR("Executor configuration error"),
        EXECUTION_FAILURE("Executor execution failure"),
        DEPENDENCY_ERROR("Missing executor dependency"),
        INITIALIZATION_ERROR("Executor initialization failure");
        
        private final String description;
        
        ExecutorErrorType(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Creates a new QueryExecutorException with detailed information.
     * 
     * @param executorType The type of executor that failed
     * @param errorType The type of executor error
     * @param message The error message
     * @param errorNode The AST node where the error occurred (can be null)
     * @param cause The underlying cause (can be null)
     */
    public QueryExecutorException(String executorType, ExecutorErrorType errorType, 
                                String message, ASTNode errorNode, Throwable cause) {
        super(
            message,
            errorNode,
            buildContext(executorType, errorType),
            buildUserHint(executorType, errorType),
            cause
        );
        this.executorType = executorType;
        this.errorType = errorType;
    }
    
    /**
     * Factory method for missing executor errors.
     */
    public static QueryExecutorException forMissingExecutor(String executorType, ASTNode errorNode) {
        return new QueryExecutorException(
            executorType,
            ExecutorErrorType.MISSING_EXECUTOR,
            String.format("%s executor not available - required for query execution", executorType),
            errorNode,
            null
        );
    }
    
    /**
     * Factory method for executor configuration errors.
     */
    public static QueryExecutorException forConfigurationError(String executorType, 
                                                             String configurationIssue, 
                                                             ASTNode errorNode, 
                                                             Throwable cause) {
        return new QueryExecutorException(
            executorType,
            ExecutorErrorType.CONFIGURATION_ERROR,
            String.format("%s executor configuration error: %s", executorType, configurationIssue),
            errorNode,
            cause
        );
    }
    
    /**
     * Factory method for execution failures.
     */
    public static QueryExecutorException forExecutionFailure(String executorType, 
                                                           String operationDescription,
                                                           ASTNode errorNode, 
                                                           Throwable cause) {
        return new QueryExecutorException(
            executorType,
            ExecutorErrorType.EXECUTION_FAILURE,
            String.format("%s executor failed during %s", executorType, operationDescription),
            errorNode,
            cause
        );
    }
    
    private static String buildContext(String executorType, ExecutorErrorType errorType) {
        return String.format("Query executor error: %s - %s", 
            errorType.getDescription(), executorType);
    }
    
    private static String buildUserHint(String executorType, ExecutorErrorType errorType) {
        return switch (errorType) {
            case MISSING_EXECUTOR -> String.format(
                "Ensure %s executor is properly initialized and provided to the query evaluator", 
                executorType
            );
            case CONFIGURATION_ERROR -> String.format(
                "Check %s executor configuration and ensure all required settings are provided", 
                executorType
            );
            case EXECUTION_FAILURE -> String.format(
                "Verify %s executor is functioning correctly and has access to required resources", 
                executorType
            );
            case DEPENDENCY_ERROR -> String.format(
                "Ensure all dependencies for %s executor are available and properly configured", 
                executorType
            );
            case INITIALIZATION_ERROR -> String.format(
                "Check %s executor initialization process and resolve any startup issues", 
                executorType
            );
        };
    }
    
    public String getExecutorType() {
        return executorType;
    }
    
    public ExecutorErrorType getErrorType() {
        return errorType;
    }
}
