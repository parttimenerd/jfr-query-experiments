package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.table.CellValue;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * Utility functions shared across function evaluators.
 * Package-private utility methods for use within the evaluator package.
 */
class FunctionUtils {
    
    /**
     * Validates that an argument is numeric
     */
    static double validateNumericArgument(String functionName, Object argument, int argumentIndex) {
        if (argument instanceof Number) {
            return ((Number) argument).doubleValue();
        }
        
        if (argument instanceof CellValue cellValue) {
            return extractNumericValue(cellValue, functionName, argumentIndex);
        }
        
        throw new IllegalArgumentException(
            functionName + " function requires numeric argument at position " + 
            (argumentIndex + 1) + ", but got " + 
            (argument == null ? "null" : argument.getClass().getSimpleName())
        );
    }
    
    /**
     * Extracts numeric value from CellValue types
     */
    private static double extractNumericValue(CellValue cellValue, String functionName, int argumentIndex) {
        return switch (cellValue.getType()) {
            case NUMBER -> ((CellValue.NumberValue) cellValue).value();
            case FLOAT -> ((CellValue.FloatValue) cellValue).value();
            case DURATION -> ((CellValue.DurationValue) cellValue).value().toNanos() / 1_000_000.0;
            case TIMESTAMP -> ((CellValue.TimestampValue) cellValue).value().toEpochMilli();
            case MEMORY_SIZE -> ((CellValue.MemorySizeValue) cellValue).value();
            case RATE -> ((CellValue.RateValue) cellValue).count();
            default -> throw new IllegalArgumentException(
                functionName + " function cannot process " + cellValue.getType() + 
                " at position " + (argumentIndex + 1) + " - numeric type required"
            );
        };
    }
    
    /**
     * Helper method to compare two values, handling CellValue types
     */
    @SuppressWarnings("unchecked")
    static int compareValues(Object a, Object b) {
        if (a == null && b == null) return 0;
        if (a == null) return -1;
        if (b == null) return 1;
        
        // Handle CellValue comparisons
        if (a instanceof CellValue cellA && b instanceof CellValue cellB) {
            return CellValue.compare(cellA, cellB);
        }
        
        // Handle mixed CellValue and regular value comparisons
        if (a instanceof CellValue cellA) {
            Object valueA = cellA.getValue();
            return compareValues(valueA, b);
        }
        if (b instanceof CellValue cellB) {
            Object valueB = cellB.getValue();
            return compareValues(a, valueB);
        }
        
        if (a instanceof Comparable && b instanceof Comparable) {
            // Try to compare as the same type first
            if (a.getClass().equals(b.getClass())) {
                return ((Comparable<Object>) a).compareTo(b);
            }
            
            // If both are numbers, compare as doubles
            if (a instanceof Number && b instanceof Number) {
                double da = ((Number) a).doubleValue();
                double db = ((Number) b).doubleValue();
                return Double.compare(da, db);
            }
            
            // Fallback to string comparison
            return a.toString().compareTo(b.toString());
        }
        
        // Default case - compare string representations
        return a.toString().compareTo(b.toString());
    }
    
    /**
     * Converts a value to boolean
     */
    static boolean toBoolean(Object value) {
        if (value == null) {
            return false;
        }
        
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        
        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0;
        }
        
        if (value instanceof String) {
            String str = ((String) value).toLowerCase().trim();
            return "true".equals(str) || "1".equals(str) || "yes".equals(str);
        }
        
        if (value instanceof CellValue) {
            return toBoolean((CellValue) value);
        }
        
        // Default to true for non-null objects
        return true;
    }
    
    /**
     * Converts a CellValue to boolean
     */
    static boolean toBoolean(CellValue value) {
        if (value == null || value instanceof CellValue.NullValue) {
            return false;
        }
        
        return switch (value.getType()) {
            case BOOLEAN -> ((CellValue.BooleanValue) value).value();
            case NUMBER -> ((CellValue.NumberValue) value).value() != 0;
            case FLOAT -> ((CellValue.FloatValue) value).value() != 0;
            case STRING -> {
                String str = ((CellValue.StringValue) value).value().toLowerCase().trim();
                yield "true".equals(str) || "1".equals(str) || "yes".equals(str);
            }
            default -> true; // Default to true for other non-null types
        };
    }
    
    /**
     * Checks if two values are equal, handling different types appropriately
     */
    static boolean areEqual(Object a, Object b) {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        
        // Handle CellValue comparisons
        if (a instanceof CellValue && b instanceof CellValue) {
            return compareValues(a, b) == 0;
        }
        
        if (a instanceof CellValue cellA) {
            return areEqual(cellA.getValue(), b);
        }
        
        if (b instanceof CellValue cellB) {
            return areEqual(a, cellB.getValue());
        }
        
        // Handle numeric comparisons
        if (a instanceof Number && b instanceof Number) {
            return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue()) == 0;
        }
        
        return Objects.equals(a, b);
    }
    
    /**
     * Converts various date/time types to Instant
     */
    static Instant toInstant(Object value) {
        if (value instanceof Instant) {
            return (Instant) value;
        }
        
        if (value instanceof Long) {
            return Instant.ofEpochMilli((Long) value);
        }
        
        if (value instanceof Number) {
            return Instant.ofEpochMilli(((Number) value).longValue());
        }
        
        if (value instanceof CellValue.TimestampValue) {
            return ((CellValue.TimestampValue) value).value();
        }
        
        throw new IllegalArgumentException("Cannot convert " + value + " to Instant");
    }
    
    /**
     * Converts CellValue to Instant
     */
    static Instant toInstant(CellValue value) {
        if (value instanceof CellValue.TimestampValue) {
            return ((CellValue.TimestampValue) value).value();
        } else if (value instanceof CellValue.NumberValue) {
            return Instant.ofEpochMilli((long) ((CellValue.NumberValue) value).value());
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to Instant");
        }
    }
    
    /**
     * Converts Object to CellValue
     */
    static CellValue toCellValue(Object obj) {
        if (obj == null) {
            return new CellValue.NullValue();
        }
        
        if (obj instanceof CellValue) {
            return (CellValue) obj;
        }
        
        if (obj instanceof String) {
            return new CellValue.StringValue((String) obj);
        }
        
        if (obj instanceof Number) {
            double value = ((Number) obj).doubleValue();
            if (obj instanceof Float || obj instanceof Double) {
                return new CellValue.FloatValue(value);
            } else {
                return new CellValue.NumberValue(value);
            }
        }
        
        if (obj instanceof Boolean) {
            return new CellValue.BooleanValue((Boolean) obj);
        }
        
        if (obj instanceof Instant) {
            return new CellValue.TimestampValue((Instant) obj);
        }
        
        // Fallback: convert to string
        return new CellValue.StringValue(obj.toString());
    }
}
