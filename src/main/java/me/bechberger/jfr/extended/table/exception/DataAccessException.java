package me.bechberger.jfr.extended.table.exception;

import me.bechberger.jfr.extended.engine.exception.QueryExecutionException;
import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Base exception for data access errors in table operations.
 * 
 * This exception serves as the parent for all table data access errors,
 * including cell access violations, row access errors, and type mismatches
 * during data retrieval from JFR tables.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public abstract class DataAccessException extends QueryExecutionException {
    
    private final String accessLocation;
    private final String expectedType;
    private final String actualType;
    private final Object actualValue;
    
    /**
     * Creates a new DataAccessException with detailed information.
     * 
     * @param message The error message
     * @param accessLocation Description of where the access occurred (e.g., "row 0, column 'name'")
     * @param expectedType The expected data type
     * @param actualType The actual data type encountered
     * @param actualValue The actual value that caused the error
     * @param errorNode The AST node where the error occurred (can be null)
     */
    protected DataAccessException(String message, String accessLocation, String expectedType, 
                                String actualType, Object actualValue, ASTNode errorNode) {
        super(
            message,
            errorNode,
            buildContext(accessLocation, expectedType, actualType),
            buildUserHint(expectedType, actualType, actualValue),
            null
        );
        this.accessLocation = accessLocation;
        this.expectedType = expectedType;
        this.actualType = actualType;
        this.actualValue = actualValue;
    }
    
    /**
     * Creates a new DataAccessException with simplified parameters.
     */
    protected DataAccessException(String message, String accessLocation, String expectedType, 
                                String actualType, Object actualValue) {
        this(message, accessLocation, expectedType, actualType, actualValue, null);
    }
    
    private static String buildContext(String accessLocation, String expectedType, String actualType) {
        return String.format("Data access error at %s: expected %s but found %s", 
            accessLocation, expectedType, actualType);
    }
    
    private static String buildUserHint(String expectedType, String actualType, Object actualValue) {
        StringBuilder hint = new StringBuilder("Suggestions: ");
        
        if ("null".equals(actualType)) {
            hint.append("Check if the cell should contain a value or use isNull() to verify null status.");
        } else {
            switch (expectedType.toLowerCase()) {
                case "number", "long" -> {
                    if ("STRING".equals(actualType)) {
                        hint.append("use getString() if you want the string representation, ");
                    }
                    hint.append("ensure the data contains numeric values");
                }
                case "string" -> {
                    hint.append("use toString() for string representation of any data type");
                }
                case "boolean" -> {
                    hint.append("ensure the data contains boolean values (true/false)");
                }
                case "array" -> {
                    hint.append("ensure the data contains array values (use COLLECT or ARRAY functions)");
                }
                case "duration" -> {
                    hint.append("ensure the data contains duration values (e.g., from DURATION literals)");
                }
                case "timestamp" -> {
                    hint.append("ensure the data contains timestamp values (e.g., from event timestamps)");
                }
                case "memory size" -> {
                    hint.append("ensure the data contains memory size values (e.g., from allocation events)");
                }
                case "rate" -> {
                    hint.append("ensure the data contains rate values (e.g., events per second)");
                }
                case "float", "double" -> {
                    if ("NUMBER".equals(actualType)) {
                        hint.append("use getNumber() for integer values, ");
                    }
                    hint.append("ensure the data contains floating-point numeric values");
                }
                default -> hint.append("verify the data contains the expected type");
            }
        }
        
        return hint.toString();
    }
    
    // Getters
    public String getAccessLocation() { return accessLocation; }
    public String getExpectedType() { return expectedType; }
    public String getActualType() { return actualType; }
    public Object getActualValue() { return actualValue; }
}
