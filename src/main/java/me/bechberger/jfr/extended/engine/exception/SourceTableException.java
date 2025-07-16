package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Exception thrown when source table or data access fails during query execution.
 * 
 * This exception provides specific information about table access failures,
 * including source information and context about the failed operation.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class SourceTableException extends QueryExecutionException {
    
    private final String tableName;
    private final String operation;
    private final String sourceContext;
    
    /**
     * Creates a new SourceTableException for table access failures.
     * 
     * @param tableName The name of the table that couldn't be accessed
     * @param operation The operation that failed (e.g., "read", "access", "load")
     * @param sourceContext Additional context about the source
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public SourceTableException(String tableName, String operation, String sourceContext, ASTNode errorNode) {
        super(
            String.format("Cannot %s table '%s': %s", operation, tableName, sourceContext),
            errorNode,
            buildContext(tableName, operation, sourceContext),
            buildHint(tableName, operation),
            null
        );
        this.tableName = tableName;
        this.operation = operation;
        this.sourceContext = sourceContext;
    }
    
    /**
     * Creates a new SourceTableException with a custom message.
     */
    public SourceTableException(String message, String tableName, ASTNode errorNode) {
        super(
            message,
            errorNode,
            String.format("Table access failed for '%s'", tableName),
            "Verify that the table exists and is accessible in the current query context.",
            null
        );
        this.tableName = tableName;
        this.operation = "access";
        this.sourceContext = "unknown";
    }
    
    /**
     * Factory method for data source access failures.
     */
    public static SourceTableException dataSourceFailure(String tableName, String reason, ASTNode errorNode) {
        return new SourceTableException(
            tableName,
            "access data source",
            reason,
            errorNode
        );
    }
    
    /**
     * Factory method for table initialization failures.
     */
    public static SourceTableException initializationFailure(String tableName, String reason, ASTNode errorNode) {
        return new SourceTableException(
            tableName,
            "initialize",
            reason,
            errorNode
        );
    }
    
    /**
     * Factory method for source validation failures.
     */
    public static SourceTableException validationFailure(String tableName, String reason, ASTNode errorNode) {
        return new SourceTableException(
            tableName,
            "validate",
            reason,
            errorNode
        );
    }
    
    private static String buildContext(String tableName, String operation, String sourceContext) {
        return String.format("Source table operation failed: attempting to %s table '%s' - %s", 
            operation, tableName, sourceContext);
    }
    
    private static String buildHint(String tableName, String operation) {
        switch (operation.toLowerCase()) {
            case "read":
            case "access":
                return String.format("Ensure table '%s' exists and is accessible. Check your data source configuration and permissions.", tableName);
            case "load":
                return String.format("Table '%s' could not be loaded. Verify the data source is available and contains valid data.", tableName);
            case "initialize":
                return String.format("Table '%s' initialization failed. Check data source configuration and connection settings.", tableName);
            case "validate":
                return String.format("Table '%s' validation failed. Verify the table structure matches the query expectations.", tableName);
            default:
                return String.format("Operation '%s' failed for table '%s'. Check the table configuration and try again.", operation, tableName);
        }
    }
    
    // Getters
    public String getTableName() { return tableName; }
    public String getOperation() { return operation; }
    public String getSourceContext() { return sourceContext; }
}
