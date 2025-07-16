package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Exception thrown when join operations fail during query execution.
 * 
 * This exception provides specific information about join failures,
 * including join types, field information, and performance constraints.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class JoinException extends QueryExecutionException {
    
    private final String joinType;
    private final String leftField;
    private final String rightField;
    private final JoinErrorType errorType;
    
    /**
     * Types of join errors
     */
    public enum JoinErrorType {
        VALIDATION_ERROR("Join validation failed"),
        FIELD_ERROR("Join field error"), 
        TYPE_ERROR("Join type mismatch"),
        PERFORMANCE_ERROR("Join performance constraint violated"),
        EXECUTION_ERROR("Join execution failed"),
        FUZZY_JOIN_ERROR("Fuzzy join operation failed");
        
        private final String description;
        
        JoinErrorType(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Creates a new JoinException with detailed information.
     */
    public JoinException(String joinType, String leftField, String rightField, 
                        JoinErrorType errorType, String message, ASTNode errorNode) {
        super(
            buildJoinContext(joinType, leftField, rightField, errorType),
            errorNode,
            buildDetailedContext(joinType, leftField, rightField, message),
            buildJoinHint(errorType, joinType),
            null
        );
        this.joinType = joinType;
        this.leftField = leftField;
        this.rightField = rightField;
        this.errorType = errorType;
    }
    
    /**
     * Factory method for validation errors
     */
    public static JoinException forValidationError(String message, ASTNode errorNode) {
        return new JoinException(
            "unknown", null, null, 
            JoinErrorType.VALIDATION_ERROR, 
            message, errorNode
        );
    }
    
    /**
     * Factory method for field errors
     */
    public static JoinException forFieldError(String message, String fieldName, 
                                            String[] availableFields, ASTNode errorNode) {
        String detailedMessage = message + ". Available fields: " + String.join(", ", availableFields);
        return new JoinException(
            "unknown", fieldName, null, 
            JoinErrorType.FIELD_ERROR, 
            detailedMessage, errorNode
        );
    }
    
    /**
     * Factory method for type errors
     */
    public static JoinException forTypeError(String message, ASTNode errorNode) {
        return new JoinException(
            "unknown", null, null, 
            JoinErrorType.TYPE_ERROR, 
            message, errorNode
        );
    }
    
    /**
     * Factory method for performance errors
     */
    public static JoinException forPerformanceError(String message, ASTNode errorNode) {
        return new JoinException(
            "unknown", null, null, 
            JoinErrorType.PERFORMANCE_ERROR, 
            message, errorNode
        );
    }
    
    /**
     * Factory method for execution errors
     */
    public static JoinException forExecutionError(String joinType, String leftField, 
                                                String rightField, String message, ASTNode errorNode) {
        return new JoinException(
            joinType, leftField, rightField, 
            JoinErrorType.EXECUTION_ERROR, 
            message, errorNode
        );
    }
    
    /**
     * Factory method for fuzzy join errors
     */
    public static JoinException forFuzzyJoinError(String joinType, String leftField, 
                                                String rightField, String message, ASTNode errorNode) {
        return new JoinException(
            joinType, leftField, rightField, 
            JoinErrorType.FUZZY_JOIN_ERROR, 
            message, errorNode
        );
    }
    
    /**
     * Build join context string
     */
    private static String buildJoinContext(String joinType, String leftField, String rightField, 
                                         JoinErrorType errorType) {
        StringBuilder context = new StringBuilder();
        context.append(errorType.getDescription());
        
        if (joinType != null && !joinType.equals("unknown")) {
            context.append(" for ").append(joinType).append(" join");
        }
        
        if (leftField != null || rightField != null) {
            context.append(" on fields ");
            if (leftField != null) {
                context.append("left.").append(leftField);
            }
            if (rightField != null) {
                if (leftField != null) {
                    context.append(" = ");
                }
                context.append("right.").append(rightField);
            }
        }
        
        return context.toString();
    }
    
    /**
     * Build detailed context information
     */
    private static String buildDetailedContext(String joinType, String leftField, String rightField, 
                                             String message) {
        StringBuilder context = new StringBuilder("Join operation failed: ");
        context.append(message);
        
        if (joinType != null && !joinType.equals("unknown")) {
            context.append(" (Join type: ").append(joinType).append(")");
        }
        
        return context.toString();
    }
    
    /**
     * Build helpful hints based on error type
     */
    private static String buildJoinHint(JoinErrorType errorType, String joinType) {
        return switch (errorType) {
            case VALIDATION_ERROR -> 
                "Ensure both tables exist and join fields are specified correctly.";
            case FIELD_ERROR -> 
                "Verify that the join fields exist in their respective tables and have compatible types.";
            case TYPE_ERROR -> 
                "Join fields must have compatible data types. Consider type conversion if necessary.";
            case PERFORMANCE_ERROR -> 
                "Reduce table sizes using WHERE clauses or increase join size limits in configuration.";
            case EXECUTION_ERROR -> 
                "Check for data consistency issues and ensure sufficient memory for join operation.";
            case FUZZY_JOIN_ERROR -> 
                "Verify timestamp fields exist and contain valid temporal data for fuzzy join operations.";
        };
    }
    
    // Getters for specific error information
    
    public String getJoinType() {
        return joinType;
    }
    
    public String getLeftField() {
        return leftField;
    }
    
    public String getRightField() {
        return rightField;
    }
    
    public JoinErrorType getErrorType() {
        return errorType;
    }
    
    /**
     * Returns true if this is a field-related error
     */
    public boolean isFieldError() {
        return errorType == JoinErrorType.FIELD_ERROR;
    }
    
    /**
     * Returns true if this is a performance-related error
     */
    public boolean isPerformanceError() {
        return errorType == JoinErrorType.PERFORMANCE_ERROR;
    }
    
    /**
     * Returns true if this is a fuzzy join error
     */
    public boolean isFuzzyJoinError() {
        return errorType == JoinErrorType.FUZZY_JOIN_ERROR;
    }
}
