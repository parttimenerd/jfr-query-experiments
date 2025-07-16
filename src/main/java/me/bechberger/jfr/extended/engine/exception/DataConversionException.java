package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;

/**
 * Exception thrown when data conversion fails during query execution.
 * 
 * This exception provides specific information about the conversion that failed,
 * including the source value, target type, and context about the conversion operation.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class DataConversionException extends QueryExecutionException {
    
    private final Object sourceValue;
    private final String targetType;
    private final String conversionContext;
    
    /**
     * Creates a new DataConversionException with detailed information.
     * 
     * @param sourceValue The value that failed to convert (can be null)
     * @param targetType The target type for the conversion
     * @param conversionContext Description of the conversion operation
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public DataConversionException(Object sourceValue, String targetType, String conversionContext, ASTNode errorNode) {
        super(
            String.format("Cannot convert %s to %s", 
                sourceValue != null ? sourceValue.getClass().getSimpleName() : "null", 
                targetType),
            errorNode,
            buildContext(sourceValue, targetType, conversionContext),
            buildUserHint(sourceValue, targetType),
            null
        );
        this.sourceValue = sourceValue;
        this.targetType = targetType;
        this.conversionContext = conversionContext;
    }
    
    /**
     * Creates a new DataConversionException with a custom message.
     */
    public DataConversionException(String message, Object sourceValue, String targetType, ASTNode errorNode) {
        super(
            message,
            errorNode,
            buildContext(sourceValue, targetType, "data conversion"),
            buildUserHint(sourceValue, targetType),
            null
        );
        this.sourceValue = sourceValue;
        this.targetType = targetType;
        this.conversionContext = "data conversion";
    }
    
    private static String buildContext(Object sourceValue, String targetType, String conversionContext) {
        return String.format("Data conversion failed during %s: attempting to convert %s to %s", 
            conversionContext,
            sourceValue != null ? sourceValue.getClass().getSimpleName() : "null",
            targetType);
    }
    
    private static String buildUserHint(Object sourceValue, String targetType) {
        if (sourceValue == null) {
            return "Cannot convert null values. Check for missing data or use COALESCE to provide default values.";
        }
        
        String sourceType = sourceValue.getClass().getSimpleName();
        
        if ("Instant".equals(targetType)) {
            return "For timestamp conversion, ensure the value is a timestamp, ISO-8601 string, or epoch milliseconds.";
        } else if ("Number".equals(targetType)) {
            return "For numeric conversion, ensure the value is a number or a string containing a valid number.";
        } else if ("Boolean".equals(targetType)) {
            return "For boolean conversion, use true/false, 1/0, or yes/no values.";
        }
        
        return String.format("Review the data types in your query. Expected %s but found %s.", targetType, sourceType);
    }
    
    // Getters
    public Object getSourceValue() { return sourceValue; }
    public String getTargetType() { return targetType; }
    public String getConversionContext() { return conversionContext; }
}
