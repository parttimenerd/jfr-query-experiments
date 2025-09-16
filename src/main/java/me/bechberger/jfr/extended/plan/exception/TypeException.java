package me.bechberger.jfr.extended.plan.exception;

import me.bechberger.jfr.extended.table.CellType;

/**
 * Exception for type-related errors in plan execution.
 * 
 * This exception covers errors related to:
 * - Type mismatches in expressions
 * - Invalid type conversions
 * - Incompatible types in operations
 * - Type validation failures
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class TypeException extends Exception {
    
    /** The expected type */
    private final CellType expectedType;
    
    /** The actual type that was encountered */
    private final CellType actualType;
    
    /** The operation that failed due to type mismatch */
    private final String operation;
    
    /** The value that caused the type error */
    private final Object problematicValue;
    
    /**
     * Creates a type exception with value context.
     */
    public TypeException(String message, CellType expectedType, CellType actualType, Object problematicValue) {
        super(message);
        this.expectedType = expectedType;
        this.actualType = actualType;
        this.operation = null;
        this.problematicValue = problematicValue;
    }
    
    /**
     * Creates a type exception for type mismatch.
     */
    public TypeException(String message, CellType expectedType, CellType actualType, String operation, Throwable cause) {
        super(message, cause);
        this.expectedType = expectedType;
        this.actualType = actualType;
        this.operation = operation;
        this.problematicValue = null;
    }
    
    /**
     * Get the expected type.
     */
    public CellType getExpectedType() {
        return expectedType;
    }
    
    /**
     * Get the actual type that was encountered.
     */
    public CellType getActualType() {
        return actualType;
    }
    
    /**
     * Get the operation that failed.
     */
    public String getOperation() {
        return operation;
    }
    
    /**
     * Get the value that caused the type error.
     */
    public Object getProblematicValue() {
        return problematicValue;
    }
    
    /**
     * Check if conversion might be possible.
     */
    public boolean isConversionPossible() {
        if (expectedType == null || actualType == null) {
            return false;
        }
        
        // Define conversion rules
        return switch (expectedType) {
            case STRING -> true; // Most types can convert to string
            case NUMBER -> actualType == CellType.NUMBER || actualType == CellType.DURATION;
            case TIMESTAMP -> actualType == CellType.NUMBER; // Unix timestamp
            case DURATION -> actualType == CellType.NUMBER; // Milliseconds
            case MEMORY_SIZE -> actualType == CellType.NUMBER; // Bytes
            default -> false;
        };
    }
    
    /**
     * Get a conversion suggestion if possible.
     */
    public String getConversionSuggestion() {
        if (!isConversionPossible()) {
            return null;
        }
        
        return switch (expectedType) {
            case STRING -> "Consider using STRING() function to convert the value";
            case TIMESTAMP -> "Consider using TIMESTAMP() function or providing Unix timestamp";
            case DURATION -> "Consider using duration literal (e.g., '100ms') or milliseconds";
            case MEMORY_SIZE -> "Consider using memory size literal (e.g., '1MB') or bytes";
            default -> null;
        };
    }
    
    /**
     * Create a formatted error report with all available information.
     */
    public String createErrorReport() {
        StringBuilder report = new StringBuilder();
        
        report.append("=== TYPE ERROR DETAILS ===\n");
        report.append("Message: ").append(getMessage()).append("\n");
        
        if (expectedType != null) {
            report.append("Expected Type: ").append(expectedType).append("\n");
        }
        
        if (actualType != null) {
            report.append("Actual Type: ").append(actualType).append("\n");
        }
        
        if (operation != null) {
            report.append("Operation: ").append(operation).append("\n");
        }
        
        if (problematicValue != null) {
            report.append("Problematic Value: ").append(problematicValue).append("\n");
        }
        
        if (isConversionPossible()) {
            report.append("Conversion Possible: Yes\n");
            String suggestion = getConversionSuggestion();
            if (suggestion != null) {
                report.append("Suggestion: ").append(suggestion).append("\n");
            }
        } else {
            report.append("Conversion Possible: No\n");
        }
        
        report.append("=========================");
        
        return report.toString();
    }
}
