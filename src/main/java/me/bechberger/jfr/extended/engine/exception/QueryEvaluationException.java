package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Exception thrown when there's an error during query evaluation or execution.
 * 
 * This exception provides specific information about evaluation errors such as
 * division by zero, null pointer access, invalid aggregation, and other runtime issues.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class QueryEvaluationException extends QueryExecutionException {
    
    private final String evaluationContext;
    private final Object problematicValue;
    private final EvaluationErrorType errorType;
    
    /**
     * Enumeration of different types of evaluation errors.
     */
    public enum EvaluationErrorType {
        DIVISION_BY_ZERO("Division by zero"),
        NULL_POINTER("Null value access"),
        INVALID_AGGREGATION("Invalid aggregation operation"),
        ARITHMETIC_OVERFLOW("Arithmetic overflow"),
        INVALID_CONVERSION("Invalid type conversion"),
        OUT_OF_BOUNDS("Array or collection index out of bounds"),
        UNSUPPORTED_OPERATION("Unsupported operation"),
        EVALUATION_TIMEOUT("Evaluation timeout"),
        RESOURCE_EXHAUSTED("Resource exhausted"),
        INVALID_STATE("Invalid evaluation state");
        
        private final String description;
        
        EvaluationErrorType(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Creates a new QueryEvaluationException with detailed information.
     * 
     * @param evaluationContext Description of what was being evaluated
     * @param problematicValue The value that caused the error (can be null)
     * @param errorType The type of evaluation error
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public QueryEvaluationException(String evaluationContext, Object problematicValue, 
                                   EvaluationErrorType errorType, ASTNode errorNode) {
        super(
            String.format("%s during %s", errorType.getDescription(), evaluationContext),
            errorNode,
            buildContext(evaluationContext, problematicValue, errorType),
            buildUserHint(errorType, problematicValue),
            null
        );
        this.evaluationContext = evaluationContext;
        this.problematicValue = problematicValue;
        this.errorType = errorType;
    }
    
    /**
     * Creates a new QueryEvaluationException with cause.
     */
    public QueryEvaluationException(String evaluationContext, Object problematicValue, 
                                   EvaluationErrorType errorType, ASTNode errorNode, Throwable cause) {
        super(
            String.format("%s during %s", errorType.getDescription(), evaluationContext),
            errorNode,
            buildContext(evaluationContext, problematicValue, errorType),
            buildUserHint(errorType, problematicValue),
            cause
        );
        this.evaluationContext = evaluationContext;
        this.problematicValue = problematicValue;
        this.errorType = errorType;
    }
    
    /**
     * Creates a QueryEvaluationException for division by zero errors.
     */
    public static QueryEvaluationException forDivisionByZero(String expression, ASTNode errorNode) {
        return new QueryEvaluationException(
            "arithmetic expression: " + expression, 
            0, 
            EvaluationErrorType.DIVISION_BY_ZERO, 
            errorNode
        );
    }
    
    /**
     * Creates a QueryEvaluationException for null pointer errors.
     */
    public static QueryEvaluationException forNullPointer(String operation, ASTNode errorNode) {
        return new QueryEvaluationException(
            operation, 
            null, 
            EvaluationErrorType.NULL_POINTER, 
            errorNode
        );
    }
    
    /**
     * Creates a QueryEvaluationException for invalid aggregation errors.
     */
    public static QueryEvaluationException forInvalidAggregation(String functionName, String reason, ASTNode errorNode) {
        String context = String.format("aggregate function '%s': %s", functionName, reason);
        return new QueryEvaluationException(
            context, 
            functionName, 
            EvaluationErrorType.INVALID_AGGREGATION, 
            errorNode
        );
    }
    
    /**
     * Creates a QueryEvaluationException for type conversion errors.
     */
    public static QueryEvaluationException forInvalidConversion(String fromType, String toType, 
                                                               Object value, ASTNode errorNode) {
        String context = String.format("type conversion from %s to %s", fromType, toType);
        return new QueryEvaluationException(
            context, 
            value, 
            EvaluationErrorType.INVALID_CONVERSION, 
            errorNode
        );
    }
    
    /**
     * Creates a QueryEvaluationException for array/collection access errors.
     */
    public static QueryEvaluationException forOutOfBounds(String collectionType, int index, 
                                                          int size, ASTNode errorNode) {
        String context = String.format("%s access: index %d, size %d", collectionType, index, size);
        return new QueryEvaluationException(
            context, 
            index, 
            EvaluationErrorType.OUT_OF_BOUNDS, 
            errorNode
        );
    }
    
    /**
     * Builds the context string for this exception.
     */
    private static String buildContext(String evaluationContext, Object problematicValue, EvaluationErrorType errorType) {
        StringBuilder context = new StringBuilder();
        context.append("Query evaluation failed: ").append(errorType.getDescription());
        context.append("\nOperation: ").append(evaluationContext);
        
        if (problematicValue != null) {
            String valueStr = problematicValue.toString();
            if (valueStr.length() > 100) {
                valueStr = valueStr.substring(0, 97) + "...";
            }
            context.append("\nProblematic value: ").append(valueStr);
        }
        
        return context.toString();
    }
    
    /**
     * Builds a helpful user hint for resolving the evaluation error.
     */
    private static String buildUserHint(EvaluationErrorType errorType, Object problematicValue) {
        return switch (errorType) {
            case DIVISION_BY_ZERO -> "Check for zero values in denominators. Add conditional logic to handle division by zero cases.";
            case NULL_POINTER -> "Check for null values before accessing properties or methods. Use NULL checks or COALESCE functions.";
            case INVALID_AGGREGATION -> "Ensure aggregate functions are used correctly with proper GROUP BY clauses and compatible data types.";
            case ARITHMETIC_OVERFLOW -> "Use smaller numbers or different data types to avoid overflow. Consider using LIMIT clauses for large datasets.";
            case INVALID_CONVERSION -> "Ensure data types are compatible for conversion. Use explicit conversion functions where needed.";
            case OUT_OF_BOUNDS -> "Check array or collection sizes before accessing elements. Use bounds checking or LENGTH functions.";
            case UNSUPPORTED_OPERATION -> "This operation is not supported in the current context. Check function documentation for usage requirements.";
            case EVALUATION_TIMEOUT -> "Query evaluation took too long. Simplify the query or add more specific WHERE clauses to reduce data.";
            case RESOURCE_EXHAUSTED -> "Query requires too many resources. Add LIMIT clauses or filter data more aggressively.";
            case INVALID_STATE -> "The evaluation state is invalid. This may indicate a bug in the query engine.";
        };
    }
    
    // Getters for accessing specific error information
    
    public String getEvaluationContext() {
        return evaluationContext;
    }
    
    public Object getProblematicValue() {
        return problematicValue;
    }
    
    public EvaluationErrorType getErrorType() {
        return errorType;
    }
    
    /**
     * Returns true if this exception has information about the problematic value.
     */
    public boolean hasProblematicValue() {
        return problematicValue != null;
    }
}
