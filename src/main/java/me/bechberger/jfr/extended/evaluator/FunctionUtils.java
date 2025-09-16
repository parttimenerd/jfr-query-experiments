package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.engine.exception.DataConversionException;
import me.bechberger.jfr.extended.engine.exception.FunctionArgumentException;
import me.bechberger.jfr.extended.engine.exception.TypeMismatchException;
import me.bechberger.jfr.extended.table.CellValue;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Utility functions shared across function evaluators.
 * Package-private utility methods for use within the evaluator package.
 */
public class FunctionUtils {
    
    // ===== ARGUMENT VALIDATION UTILITIES =====
    
    /**
     * Assert that a function has exactly the specified number of arguments
     */
    static void assertArgumentCount(String functionName, List<CellValue> arguments, int expectedCount) {
        if (arguments.size() != expectedCount) {
            throw FunctionArgumentException.forWrongArgumentCount(functionName, expectedCount, arguments.size(), null);
        }
    }
    
    /**
     * Assert that a function has at least the specified number of arguments
     */
    static void assertAtLeastArguments(String functionName, List<CellValue> arguments, int minCount) {
        if (arguments.size() < minCount) {
            String expectedRange = minCount == 1 ? "at least 1" : "at least " + minCount;
            throw FunctionArgumentException.forWrongArgumentCountRange(functionName, expectedRange, arguments.size(), null);
        }
    }
    
    /**
     * Assert that a function has at most the specified number of arguments
     */
    static void assertAtMostArguments(String functionName, List<CellValue> arguments, int maxCount) {
        if (arguments.size() > maxCount) {
            String expectedRange = maxCount == 1 ? "at most 1" : "at most " + maxCount;
            throw FunctionArgumentException.forWrongArgumentCountRange(functionName, expectedRange, arguments.size(), null);
        }
    }
    
    /**
     * Assert that a function has arguments within the specified range
     */
    static void assertArgumentRange(String functionName, List<CellValue> arguments, int minCount, int maxCount) {
        if (arguments.size() < minCount || arguments.size() > maxCount) {
            String expectedRange;
            if (minCount == maxCount) {
                expectedRange = "exactly " + minCount;
            } else {
                expectedRange = minCount + " to " + maxCount;
            }
            throw FunctionArgumentException.forWrongArgumentCountRange(functionName, expectedRange, arguments.size(), null);
        }
    }
    
    /**
     * Assert that a specific argument is of the expected type
     */
    static void assertArgumentType(String functionName, List<CellValue> arguments, int argumentIndex, 
                                 Class<? extends CellValue> expectedType, String typeName) {
        if (argumentIndex >= arguments.size()) {
            throw FunctionArgumentException.forWrongArgumentCount(functionName, argumentIndex + 1, arguments.size(), null);
        }
        
        CellValue argument = arguments.get(argumentIndex);
        if (!expectedType.isInstance(argument)) {
            throw FunctionArgumentException.forInvalidArgumentType(functionName, argumentIndex, typeName,
                argument.getClass().getSimpleName(), argument, null);
        }
    }
    
    /**
     * Assert that a specific argument is an array
     */
    static void assertArrayArgument(String functionName, List<CellValue> arguments, int argumentIndex) {
        assertArgumentType(functionName, arguments, argumentIndex, CellValue.ArrayValue.class, "array");
    }
    
    /**
     * Assert that a specific argument is a string
     */
    static void assertStringArgument(String functionName, List<CellValue> arguments, int argumentIndex) {
        assertArgumentType(functionName, arguments, argumentIndex, CellValue.StringValue.class, "string");
    }
    
    /**
     * Assert that a specific argument is a number
     */
    static void assertNumberArgument(String functionName, List<CellValue> arguments, int argumentIndex) {
        assertArgumentType(functionName, arguments, argumentIndex, CellValue.NumberValue.class, "number");
    }
    
    /**
     * Assert that a specific argument is a boolean
     */
    static void assertBooleanArgument(String functionName, List<CellValue> arguments, int argumentIndex) {
        assertArgumentType(functionName, arguments, argumentIndex, CellValue.BooleanValue.class, "boolean");
    }
    
    /**
     * Assert that a specific argument is not null
     */
    static void assertNotNullArgument(String functionName, List<CellValue> arguments, int argumentIndex) {
        if (argumentIndex >= arguments.size()) {
            throw FunctionArgumentException.forWrongArgumentCount(functionName, argumentIndex + 1, arguments.size(), null);
        }
        
        CellValue argument = arguments.get(argumentIndex);
        if (argument instanceof CellValue.NullValue) {
            throw FunctionArgumentException.forNullArgument(functionName, argumentIndex, null);
        }
    }
    
    /**
     * Get an array argument safely with validation
     */
    static CellValue.ArrayValue getArrayArgument(String functionName, List<CellValue> arguments, int argumentIndex) {
        assertArrayArgument(functionName, arguments, argumentIndex);
        return (CellValue.ArrayValue) arguments.get(argumentIndex);
    }
    
    /**
     * Get a string argument safely with validation
     */
    static CellValue.StringValue getStringArgument(String functionName, List<CellValue> arguments, int argumentIndex) {
        assertStringArgument(functionName, arguments, argumentIndex);
        return (CellValue.StringValue) arguments.get(argumentIndex);
    }
    
    /**
     * Get a number argument safely with validation
     */
    static CellValue.NumberValue getNumberArgument(String functionName, List<CellValue> arguments, int argumentIndex) {
        try {
            assertNumberArgument(functionName, arguments, argumentIndex);
            return (CellValue.NumberValue) arguments.get(argumentIndex);
        } catch (FunctionArgumentException e) {
            // Handle special case for NumberValue
            if (arguments.get(argumentIndex) instanceof CellValue.NumberValue floatValue) {
                return new CellValue.NumberValue(floatValue.value());
            }
            throw e; // Re-throw if not a NumberValue
        }
    }
    
    /**
     * Get a boolean argument safely with validation
     */
    static CellValue.BooleanValue getBooleanArgument(String functionName, List<CellValue> arguments, int argumentIndex) {
        assertBooleanArgument(functionName, arguments, argumentIndex);
        return (CellValue.BooleanValue) arguments.get(argumentIndex);
    }
    
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
        
        throw new FunctionArgumentException(
            functionName,
            argumentIndex,
            "numeric", 
            argument == null ? "null" : argument.getClass().getSimpleName(),
            argument,
            FunctionArgumentException.ArgumentErrorType.INVALID_ARGUMENT_TYPE,
            null
        );
    }
    
    /**
     * Extracts numeric value from CellValue types
     */
    private static double extractNumericValue(CellValue cellValue, String functionName, int argumentIndex) {
        return switch (cellValue.getType()) {
            case NUMBER -> ((CellValue.NumberValue) cellValue).value();
            case DURATION -> ((CellValue.DurationValue) cellValue).value().toNanos() / 1_000_000.0;
            case TIMESTAMP -> ((CellValue.TimestampValue) cellValue).value().toEpochMilli();
            case MEMORY_SIZE -> ((CellValue.MemorySizeValue) cellValue).value();
            case RATE -> ((CellValue.RateValue) cellValue).count();
            default -> throw new FunctionArgumentException(
                functionName,
                argumentIndex,
                "numeric",
                cellValue.getType().toString(),
                cellValue,
                FunctionArgumentException.ArgumentErrorType.INVALID_ARGUMENT_TYPE,
                null
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
        
        throw new DataConversionException(value, "Instant", "timestamp conversion", null);
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
            throw new DataConversionException(value, "Instant", "CellValue timestamp conversion", null);
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
                return new CellValue.NumberValue(value);
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
    
    /**
     * Converts a value to a Number, throwing TypeMismatchException on failure
     */
    public static Number convertToNumber(Object value) {
        if (value instanceof Number) {
            return (Number) value;
        }
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                throw new TypeMismatchException("number", "string", value, 
                    "numeric conversion", null);
            }
        }
        if (value instanceof CellValue cellValue) {
            if (cellValue instanceof CellValue.NumberValue numberValue) {
                return numberValue.value();
            } else if (cellValue instanceof CellValue.StringValue stringValue) {
                return convertToNumber(stringValue.value());
            }
        }
        throw new TypeMismatchException("number", value.getClass().getSimpleName(), value,
            "numeric conversion", null);
    }
    
    /**
     * Converts a value to a boolean
     */
    public static boolean convertToBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            String str = ((String) value).toLowerCase();
            return str.equals("true") || str.equals("1") || str.equals("yes");
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0.0;
        }
        if (value instanceof CellValue cellValue) {
            return toBoolean(cellValue);
        }
        return false; // null or other types default to false
    }
}
