package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.table.CellValue;
import java.util.List;

/**
 * Static methods for evaluating conditional functions.
 * Package-private methods for use within the evaluator package.
 */
public class ConditionalFunctions {
    
    /**
     * Evaluates an IF function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return CellValue result of the IF operation
     */
    public static CellValue evaluateIf(CellValue[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("IF function requires exactly 3 arguments");
        }
        
        boolean condition = FunctionUtils.toBoolean(args[0]);
        return condition ? args[1] : args[2];
    }
    
    /**
     * Evaluates an IF function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return CellValue result of the IF operation
     */
    public static CellValue evaluateIf(List<CellValue> arguments) {
        return evaluateIf(arguments.toArray(new CellValue[0]));
    }
    
    /**
     * Evaluates a CASE function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return CellValue result of the CASE operation
     */
    public static CellValue evaluateCase(CellValue[] args) {
        if (args.length < 3 || args.length % 2 == 0) {
            throw new IllegalArgumentException("CASE function requires odd number of arguments (at least 3)");
        }
        
        for (int i = 0; i < args.length - 1; i += 2) {
            boolean condition = FunctionUtils.toBoolean(args[i]);
            if (condition) {
                return args[i + 1];
            }
        }
        
        // Return default value (last argument)
        return args[args.length - 1];
    }
    
    /**
     * Evaluates a CASE function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return CellValue result of the CASE operation
     */
    public static CellValue evaluateCase(List<CellValue> arguments) {
        return evaluateCase(arguments.toArray(new CellValue[0]));
    }
    
    /**
     * Evaluates a COALESCE function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return First non-null CellValue or null CellValue if all are null
     */
    public static CellValue evaluateCoalesce(CellValue[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("COALESCE function requires at least 1 argument");
        }
        
        for (CellValue arg : args) {
            if (arg != null && !(arg instanceof CellValue.NullValue)) {
                return arg;
            }
        }
        
        return new CellValue.NullValue();
    }
    
    /**
     * Evaluates a COALESCE function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return First non-null CellValue or null CellValue if all are null
     */
    public static CellValue evaluateCoalesce(List<CellValue> arguments) {
        return evaluateCoalesce(arguments.toArray(new CellValue[0]));
    }
    
    /**
     * Evaluates a NULLIF function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return Null if both arguments are equal, otherwise the first argument
     */
    public static CellValue evaluateNullIf(CellValue[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("NULLIF function requires exactly 2 arguments");
        }
        
        CellValue value1 = args[0];
        CellValue value2 = args[1];
        
        return FunctionUtils.areEqual(value1, value2) ? new CellValue.NullValue() : value1;
    }
    
    /**
     * Evaluates a NULLIF function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return Null if both arguments are equal, otherwise the first argument
     */
    public static CellValue evaluateNullIf(List<CellValue> arguments) {
        return evaluateNullIf(arguments.toArray(new CellValue[0]));
    }
    
    /**
     * Evaluates an ISNULL function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return Boolean CellValue indicating if the value is null
     */
    public static CellValue evaluateIsNull(CellValue[] args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("ISNULL function requires exactly 1 argument");
        }
        
        boolean isNull = args[0] == null || args[0] instanceof CellValue.NullValue;
        return new CellValue.BooleanValue(isNull);
    }
    
    /**
     * Evaluates an ISNULL function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return Boolean CellValue indicating if the value is null
     */
    public static CellValue evaluateIsNull(List<CellValue> arguments) {
        return evaluateIsNull(arguments.toArray(new CellValue[0]));
    }
    
    /**
     * Evaluates an ISNOTNULL function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return Boolean CellValue indicating if the value is not null
     */
    public static CellValue evaluateIsNotNull(CellValue[] args) {
        if (args.length != 1) {
            throw new IllegalArgumentException("ISNOTNULL function requires exactly 1 argument");
        }
        
        boolean isNotNull = !(args[0] == null || args[0] instanceof CellValue.NullValue);
        return new CellValue.BooleanValue(isNotNull);
    }
    
    /**
     * Evaluates an ISNOTNULL function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return Boolean CellValue indicating if the value is not null
     */
    public static CellValue evaluateIsNotNull(List<CellValue> arguments) {
        return evaluateIsNotNull(arguments.toArray(new CellValue[0]));
    }
    
    /**
     * Evaluates a GREATEST function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return CellValue with the greatest value
     */
    public static CellValue evaluateGreatest(CellValue[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("GREATEST function requires at least 1 argument");
        }
        
        CellValue greatest = args[0];
        for (int i = 1; i < args.length; i++) {
            if (FunctionUtils.compareValues(args[i], greatest) > 0) {
                greatest = args[i];
            }
        }
        
        return greatest;
    }
    
    /**
     * Evaluates a GREATEST function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return CellValue with the greatest value
     */
    public static CellValue evaluateGreatest(List<CellValue> arguments) {
        return evaluateGreatest(arguments.toArray(new CellValue[0]));
    }
    
    /**
     * Evaluates a LEAST function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return CellValue with the smallest value
     */
    public static CellValue evaluateLeast(CellValue[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("LEAST function requires at least 1 argument");
        }
        
        CellValue least = args[0];
        for (int i = 1; i < args.length; i++) {
            if (FunctionUtils.compareValues(args[i], least) < 0) {
                least = args[i];
            }
        }
        
        return least;
    }
    
    /**
     * Evaluates a LEAST function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return CellValue with the smallest value
     */
    public static CellValue evaluateLeast(List<CellValue> arguments) {
        return evaluateLeast(arguments.toArray(new CellValue[0]));
    }
}
