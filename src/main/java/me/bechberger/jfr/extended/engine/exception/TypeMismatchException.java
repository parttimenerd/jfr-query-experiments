package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Exception thrown when there's a type mismatch during query execution.
 * 
 * This exception provides specific information about the expected and actual types,
 * context about where the mismatch occurred, and suggestions for resolving the issue.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class TypeMismatchException extends QueryExecutionException {
    
    private final String expectedType;
    private final String actualType;
    private final String operationContext;
    private final Object actualValue;
    
    /**
     * Creates a new TypeMismatchException with detailed information.
     * 
     * @param expectedType The expected type description
     * @param actualType The actual type description
     * @param actualValue The actual value that caused the mismatch (can be null)
     * @param operationContext Description of the operation being performed
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public TypeMismatchException(String expectedType, String actualType, Object actualValue,
                               String operationContext, ASTNode errorNode) {
        super(
            String.format("Type mismatch: expected %s but got %s", expectedType, actualType),
            errorNode,
            buildContext(expectedType, actualType, operationContext),
            buildUserHint(expectedType, actualType, actualValue),
            null
        );
        this.expectedType = expectedType;
        this.actualType = actualType;
        this.operationContext = operationContext;
        this.actualValue = actualValue;
    }
    
    /**
     * Creates a new TypeMismatchException without operation context.
     */
    public TypeMismatchException(String expectedType, String actualType, Object actualValue, ASTNode errorNode) {
        this(expectedType, actualType, actualValue, "type conversion", errorNode);
    }
    
    /**
     * Creates a TypeMismatchException for function argument type errors.
     */
    public static TypeMismatchException forFunctionArgument(String functionName, int argumentIndex, 
                                                            String expectedType, String actualType, 
                                                            Object actualValue, ASTNode errorNode) {
        String context = String.format("function '%s' argument %d", functionName, argumentIndex + 1);
        return new TypeMismatchException(expectedType, actualType, actualValue, context, errorNode);
    }
    
    /**
     * Creates a TypeMismatchException for binary operation type errors.
     */
    public static TypeMismatchException forBinaryOperation(String operator, String leftType, String rightType,
                                                           Object leftValue, Object rightValue, ASTNode errorNode) {
        String expectedType = String.format("compatible types for '%s' operation", operator);
        String actualType = String.format("%s and %s", leftType, rightType);
        String context = String.format("binary operation '%s'", operator);
        return new TypeMismatchException(expectedType, actualType, 
            String.format("left=%s, right=%s", leftValue, rightValue), context, errorNode);
    }
    
    /**
     * Creates a TypeMismatchException for assignment type errors.
     */
    public static TypeMismatchException forAssignment(String variableName, String expectedType, 
                                                     String actualType, Object actualValue, ASTNode errorNode) {
        String context = String.format("assignment to variable '%s'", variableName);
        return new TypeMismatchException(expectedType, actualType, actualValue, context, errorNode);
    }
    
    /**
     * Builds the context string for this exception.
     */
    private static String buildContext(String expectedType, String actualType, String operationContext) {
        return String.format("Performing %s: expected %s but received %s", 
            operationContext, expectedType, actualType);
    }
    
    /**
     * Builds a helpful user hint for resolving the type mismatch error.
     */
    private static String buildUserHint(String expectedType, String actualType, Object actualValue) {
        StringBuilder hint = new StringBuilder();
        
        // Provide specific conversion suggestions based on common type mismatches
        if (isNumericMismatch(expectedType, actualType)) {
            hint.append("Consider using explicit type conversion functions or checking numeric formats");
        } else if (isStringConversionNeeded(expectedType, actualType)) {
            hint.append("Consider converting to string using appropriate formatting functions");
        } else if (isBooleanMismatch(expectedType, actualType)) {
            hint.append("Use boolean expressions (true/false) or comparison operators that return boolean values");
        } else if (isNullValueIssue(actualValue)) {
            hint.append("Check for null values and handle them appropriately (use NULL checks or default values)");
        } else {
            hint.append("Ensure the data types match the expected operation requirements");
        }
        
        // Add value information if available
        if (actualValue != null) {
            String valueStr = actualValue.toString();
            if (valueStr.length() > 50) {
                valueStr = valueStr.substring(0, 47) + "...";
            }
            hint.append(". Current value: ").append(valueStr);
        }
        
        return hint.toString();
    }
    
    /**
     * Checks if this is a numeric type mismatch.
     */
    private static boolean isNumericMismatch(String expectedType, String actualType) {
        String[] numericTypes = {"number", "integer", "float", "long", "double", "numeric"};
        boolean expectedIsNumeric = containsAny(expectedType.toLowerCase(), numericTypes);
        boolean actualIsNumeric = containsAny(actualType.toLowerCase(), numericTypes);
        return expectedIsNumeric && !actualIsNumeric;
    }
    
    /**
     * Checks if string conversion is needed.
     */
    private static boolean isStringConversionNeeded(String expectedType, String actualType) {
        return expectedType.toLowerCase().contains("string") && !actualType.toLowerCase().contains("string");
    }
    
    /**
     * Checks if this is a boolean type mismatch.
     */
    private static boolean isBooleanMismatch(String expectedType, String actualType) {
        return expectedType.toLowerCase().contains("boolean") && !actualType.toLowerCase().contains("boolean");
    }
    
    /**
     * Checks if the issue is related to null values.
     */
    private static boolean isNullValueIssue(Object actualValue) {
        return actualValue == null || "null".equals(String.valueOf(actualValue).toLowerCase());
    }
    
    /**
     * Helper method to check if a string contains any of the given substrings.
     */
    private static boolean containsAny(String str, String[] substrings) {
        for (String substring : substrings) {
            if (str.contains(substring)) {
                return true;
            }
        }
        return false;
    }
    
    // Getters for accessing specific error information
    
    public String getExpectedType() {
        return expectedType;
    }
    
    public String getActualType() {
        return actualType;
    }
    
    public String getOperationContext() {
        return operationContext;
    }
    
    public Object getActualValue() {
        return actualValue;
    }
    
    /**
     * Returns true if this exception has information about the actual value.
     */
    public boolean hasActualValue() {
        return actualValue != null;
    }
}
