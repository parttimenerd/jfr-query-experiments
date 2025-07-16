package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Exception thrown when a function is called with incorrect arguments.
 * 
 * This exception provides detailed information about function argument errors,
 * including wrong argument count, invalid argument types, and parameter validation failures.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class FunctionArgumentException extends QueryExecutionException {
    
    private final String functionName;
    private final int argumentIndex;
    private final int expectedArgumentCount;
    private final int actualArgumentCount;
    private final String expectedType;
    private final String actualType;
    private final Object actualValue;
    private final ArgumentErrorType errorType;
    
    /**
     * Enum defining different types of function argument errors
     */
    public enum ArgumentErrorType {
        WRONG_ARGUMENT_COUNT("Wrong number of arguments"),
        INVALID_ARGUMENT_TYPE("Invalid argument type"),
        INVALID_ARGUMENT_VALUE("Invalid argument value"),
        NULL_ARGUMENT("Null argument not allowed"),
        OUT_OF_RANGE("Argument value out of range");
        
        private final String description;
        
        ArgumentErrorType(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
    
    /**
     * Constructor for function argument exceptions
     */
    public FunctionArgumentException(String functionName, int argumentIndex, 
                                   String expectedType, String actualType, Object actualValue,
                                   ArgumentErrorType errorType, ASTNode errorNode) {
        super(buildMessage(functionName, argumentIndex, expectedType, actualType, actualValue, errorType),
              errorNode, 
              buildContext(functionName, errorType),
              buildUserHint(functionName, errorType, expectedType),
              null);
        
        this.functionName = functionName;
        this.argumentIndex = argumentIndex;
        this.expectedArgumentCount = -1;
        this.actualArgumentCount = -1;
        this.expectedType = expectedType;
        this.actualType = actualType;
        this.actualValue = actualValue;
        this.errorType = errorType;
    }
    
    /**
     * Constructor for wrong argument count errors
     */
    public FunctionArgumentException(String functionName, int expectedCount, int actualCount, 
                                   ArgumentErrorType errorType, ASTNode errorNode) {
        super(buildCountMessage(functionName, expectedCount, actualCount),
              errorNode,
              "Function call validation",
              buildCountHint(functionName, expectedCount, actualCount),
              null);
        
        this.functionName = functionName;
        this.argumentIndex = -1;
        this.expectedArgumentCount = expectedCount;
        this.actualArgumentCount = actualCount;
        this.expectedType = null;
        this.actualType = null;
        this.actualValue = null;
        this.errorType = errorType;
    }
    
    /**
     * Constructor for custom message errors (like range-based argument count)
     */
    public FunctionArgumentException(String customMessage, ASTNode errorNode, 
                                   String context, String userHint,
                                   String functionName, int expectedCount, int actualCount, 
                                   ArgumentErrorType errorType) {
        super(customMessage, errorNode, context, userHint, null);
        
        this.functionName = functionName;
        this.argumentIndex = -1;
        this.expectedArgumentCount = expectedCount;
        this.actualArgumentCount = actualCount;
        this.expectedType = null;
        this.actualType = null;
        this.actualValue = null;
        this.errorType = errorType;
    }
    
    /**
     * Build error message for type/value errors
     */
    private static String buildMessage(String functionName, int argumentIndex, 
                                     String expectedType, String actualType, Object actualValue,
                                     ArgumentErrorType errorType) {
        StringBuilder message = new StringBuilder();
        message.append("Function '").append(functionName).append("' ");
        message.append("argument ").append(argumentIndex + 1).append(": ");
        message.append(errorType.getDescription().toLowerCase());
        
        if (expectedType != null && actualType != null) {
            message.append(" - expected ").append(expectedType);
            message.append(" but got ").append(actualType);
        }
        
        if (actualValue != null) {
            message.append(" (value: ").append(actualValue).append(")");
        }
        
        return message.toString();
    }
    
    /**
     * Build error message for argument count errors
     */
    private static String buildCountMessage(String functionName, int expectedCount, int actualCount) {
        StringBuilder message = new StringBuilder();
        message.append("Function '").append(functionName).append("' ");
        
        if (expectedCount == 1) {
            message.append("requires exactly 1 argument");
        } else {
            message.append("requires exactly ").append(expectedCount).append(" arguments");
        }
        
        message.append(" but got ").append(actualCount);
        
        return message.toString();
    }
    
    /**
     * Build context information
     */
    private static String buildContext(String functionName, ArgumentErrorType errorType) {
        return "Validating arguments for function '" + functionName + "' (" + errorType.getDescription() + ")";
    }
    
    /**
     * Build user hint for type/value errors
     */
    private static String buildUserHint(String functionName, ArgumentErrorType errorType, String expectedType) {
        switch (errorType) {
            case WRONG_ARGUMENT_COUNT:
                return "Check the function documentation for correct argument count.";
            case INVALID_ARGUMENT_TYPE:
                if (expectedType != null) {
                    return "Ensure the argument is of type " + expectedType + 
                           ". Use type conversion functions if needed.";
                }
                break;
            case NULL_ARGUMENT:
                return "This function does not accept null values. Check your data or use COALESCE() to handle nulls.";
            case OUT_OF_RANGE:
                return "Check that numeric arguments are within valid ranges for this function.";
            case INVALID_ARGUMENT_VALUE:
                return "Verify that the argument value is valid for function '" + functionName + "'.";
        }
        return "Check the function documentation for correct argument types and values.";
    }
    
    /**
     * Build user hint for argument count errors
     */
    private static String buildCountHint(String functionName, int expectedCount, int actualCount) {
        if (actualCount < expectedCount) {
            return "Add " + (expectedCount - actualCount) + " more argument(s) to the function call.";
        } else {
            return "Remove " + (actualCount - expectedCount) + " argument(s) from the function call.";
        }
    }
    
    // ===== FACTORY METHODS =====
    
    /**
     * Create exception for wrong argument count
     */
    public static FunctionArgumentException forWrongArgumentCount(String functionName, 
                                                                int expectedCount, int actualCount, 
                                                                ASTNode errorNode) {
        return new FunctionArgumentException(functionName, expectedCount, actualCount, 
                                           ArgumentErrorType.WRONG_ARGUMENT_COUNT, errorNode);
    }
    
    /**
     * Create exception for wrong argument count with range
     */
    public static FunctionArgumentException forWrongArgumentCountRange(String functionName, 
                                                                      String expectedRange, int actualCount, 
                                                                      ASTNode errorNode) {
        // Create custom message for range-based argument count errors
        String message = "Function '" + functionName + "' requires " + expectedRange + " argument(s) but got " + actualCount;
        
        return new FunctionArgumentException(message, errorNode, 
                                           "Function call validation",
                                           "Check the function documentation for correct argument count.",
                                           functionName, -1, actualCount, ArgumentErrorType.WRONG_ARGUMENT_COUNT);
    }
    
    /**
     * Create exception for invalid argument type
     */
    public static FunctionArgumentException forInvalidArgumentType(String functionName, 
                                                                  int argumentIndex, String expectedType, 
                                                                  String actualType, Object actualValue, 
                                                                  ASTNode errorNode) {
        return new FunctionArgumentException(functionName, argumentIndex, expectedType, actualType, 
                                           actualValue, ArgumentErrorType.INVALID_ARGUMENT_TYPE, errorNode);
    }
    
    /**
     * Create exception for null argument
     */
    public static FunctionArgumentException forNullArgument(String functionName, int argumentIndex, 
                                                           ASTNode errorNode) {
        return new FunctionArgumentException(functionName, argumentIndex, "non-null value", "null", 
                                           null, ArgumentErrorType.NULL_ARGUMENT, errorNode);
    }
    
    /**
     * Create exception for out of range argument
     */
    public static FunctionArgumentException forOutOfRange(String functionName, int argumentIndex, 
                                                         Object actualValue, String validRange, 
                                                         ASTNode errorNode) {
        return new FunctionArgumentException(functionName, argumentIndex, validRange, 
                                           "out of range", actualValue, 
                                           ArgumentErrorType.OUT_OF_RANGE, errorNode);
    }
    
    /**
     * Create exception for invalid argument value
     */
    public static FunctionArgumentException forInvalidValue(String functionName, int argumentIndex, 
                                                           Object actualValue, String expectedDescription, 
                                                           ASTNode errorNode) {
        return new FunctionArgumentException(functionName, argumentIndex, expectedDescription, 
                                           "invalid", actualValue, 
                                           ArgumentErrorType.INVALID_ARGUMENT_VALUE, errorNode);
    }
    
    // ===== GETTERS =====
    
    public String getFunctionName() {
        return functionName;
    }
    
    public int getArgumentIndex() {
        return argumentIndex;
    }
    
    public int getExpectedArgumentCount() {
        return expectedArgumentCount;
    }
    
    public int getActualArgumentCount() {
        return actualArgumentCount;
    }
    
    public String getExpectedType() {
        return expectedType;
    }
    
    public String getActualType() {
        return actualType;
    }
    
    public Object getActualValue() {
        return actualValue;
    }
    
    public ArgumentErrorType getErrorType() {
        return errorType;
    }
    
    public boolean hasArgumentIndex() {
        return argumentIndex >= 0;
    }
    
    public boolean hasArgumentCount() {
        return expectedArgumentCount >= 0 && actualArgumentCount >= 0;
    }
    
    public boolean hasActualValue() {
        return actualValue != null;
    }
}
