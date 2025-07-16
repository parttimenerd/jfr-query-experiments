package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Exception thrown when ORDER BY clause evaluation fails during query execution.
 * 
 * This exception provides specific information about sorting failures,
 * including the sort expression and context about the failed operation.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class OrderByEvaluationException extends QueryExecutionException {
    
    private final String sortExpression;
    private final String sortDirection;
    private final String failureReason;
    
    /**
     * Creates a new OrderByEvaluationException for sorting failures.
     * 
     * @param sortExpression The expression that failed to evaluate for sorting
     * @param sortDirection The sort direction (ASC/DESC)
     * @param failureReason The reason for the failure
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public OrderByEvaluationException(String sortExpression, String sortDirection, String failureReason, ASTNode errorNode) {
        super(
            String.format("ORDER BY evaluation failed for expression '%s' %s: %s", 
                sortExpression, sortDirection, failureReason),
            errorNode,
            buildContext(sortExpression, sortDirection, failureReason),
            buildHint(sortExpression, failureReason),
            null
        );
        this.sortExpression = sortExpression;
        this.sortDirection = sortDirection;
        this.failureReason = failureReason;
    }
    
    /**
     * Creates a new OrderByEvaluationException with a simple message.
     */
    public OrderByEvaluationException(String message, String sortExpression, ASTNode errorNode) {
        super(
            message,
            errorNode,
            String.format("ORDER BY clause evaluation failed for '%s'", sortExpression),
            "Ensure the ORDER BY expression returns comparable values and check for null values.",
            null
        );
        this.sortExpression = sortExpression;
        this.sortDirection = "ASC";
        this.failureReason = "evaluation failed";
    }
    
    /**
     * Factory method for type comparison failures in ORDER BY.
     */
    public static OrderByEvaluationException typeComparison(String sortExpression, String type1, String type2, ASTNode errorNode) {
        return new OrderByEvaluationException(
            sortExpression,
            "ASC",
            String.format("cannot compare %s with %s", type1, type2),
            errorNode
        );
    }
    
    /**
     * Factory method for null value handling in ORDER BY.
     */
    public static OrderByEvaluationException nullValue(String sortExpression, ASTNode errorNode) {
        return new OrderByEvaluationException(
            sortExpression,
            "ASC",
            "null values cannot be ordered without explicit handling",
            errorNode
        );
    }
    
    /**
     * Factory method for expression evaluation failures in ORDER BY.
     */
    public static OrderByEvaluationException expressionFailure(String sortExpression, String reason, ASTNode errorNode) {
        return new OrderByEvaluationException(
            sortExpression,
            "ASC",
            reason,
            errorNode
        );
    }
    
    private static String buildContext(String sortExpression, String sortDirection, String failureReason) {
        return String.format("ORDER BY clause processing failed: expression '%s' %s could not be evaluated - %s", 
            sortExpression, sortDirection, failureReason);
    }
    
    private static String buildHint(String sortExpression, String failureReason) {
        if (failureReason.contains("null")) {
            return "Use COALESCE or CASE expressions to handle null values in ORDER BY clauses.";
        } else if (failureReason.contains("compare") || failureReason.contains("type")) {
            return "Ensure all values in the ORDER BY expression are of comparable types. Use CAST to convert to a common type.";
        } else if (failureReason.contains("column") || failureReason.contains("unknown")) {
            return "Verify that all columns referenced in the ORDER BY clause exist in the SELECT result set.";
        } else {
            return "Check that the ORDER BY expression produces sortable values and handles edge cases properly.";
        }
    }
    
    // Getters
    public String getSortExpression() { return sortExpression; }
    public String getSortDirection() { return sortDirection; }
    public String getFailureReason() { return failureReason; }
}
