package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.engine.exception.FunctionArgumentException;
import java.util.List;

/**
 * Static methods for evaluating conditional functions.
 * Package-private methods for use within the evaluator package.
 */
public class ConditionalFunctions {
    
    /**
     * Evaluates an IF function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return CellValue result of the IF operation
     */
    public static CellValue evaluateIf(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("IF", arguments, 3);
        
        boolean condition = FunctionUtils.toBoolean(arguments.get(0));
        return condition ? arguments.get(1) : arguments.get(2);
    }
    
    /**
     * Evaluates an IF function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return CellValue result of the IF operation
     */
    public static CellValue evaluateIf(CellValue[] args) {
        return evaluateIf(List.of(args));
    }
    
    /**
     * Evaluates a CASE function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return CellValue result of the CASE operation
     */
    public static CellValue evaluateCase(List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("CASE", arguments, 3);
        if (arguments.size() % 2 == 0) {
            throw FunctionArgumentException.forWrongArgumentCountRange("CASE", "odd number (3, 5, 7, ...)", arguments.size(), null);
        }
        
        for (int i = 0; i < arguments.size() - 1; i += 2) {
            boolean condition = FunctionUtils.toBoolean(arguments.get(i));
            if (condition) {
                return arguments.get(i + 1);
            }
        }
        
        // Return default value (last argument)
        return arguments.get(arguments.size() - 1);
    }
    
    /**
     * Evaluates a CASE function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return CellValue result of the CASE operation
     */
    public static CellValue evaluateCase(CellValue[] args) {
        return evaluateCase(List.of(args));
    }
    
    /**
     * Evaluates a COALESCE function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return First non-null CellValue or null CellValue if all are null
     */
    public static CellValue evaluateCoalesce(List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("COALESCE", arguments, 1);
        
        for (CellValue arg : arguments) {
            if (arg != null && !(arg instanceof CellValue.NullValue)) {
                return arg;
            }
        }
        
        return new CellValue.NullValue();
    }
    
    /**
     * Evaluates a COALESCE function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return First non-null CellValue or null CellValue if all are null
     */
    public static CellValue evaluateCoalesce(CellValue[] args) {
        return evaluateCoalesce(List.of(args));
    }
    
    /**
     * Evaluates a NULLIF function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return Null if both arguments are equal, otherwise the first argument
     */
    public static CellValue evaluateNullIf(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("NULLIF", arguments, 2);
        
        CellValue value1 = arguments.get(0);
        CellValue value2 = arguments.get(1);
        
        return FunctionUtils.areEqual(value1, value2) ? new CellValue.NullValue() : value1;
    }
    
    /**
     * Evaluates a NULLIF function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return Null if both arguments are equal, otherwise the first argument
     */
    public static CellValue evaluateNullIf(CellValue[] args) {
        return evaluateNullIf(List.of(args));
    }
    
    /**
     * Evaluates an ISNULL function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return Boolean CellValue indicating if the value is null
     */
    public static CellValue evaluateIsNull(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("ISNULL", arguments, 1);
        
        boolean isNull = arguments.get(0) == null || arguments.get(0) instanceof CellValue.NullValue;
        return new CellValue.BooleanValue(isNull);
    }
    
    /**
     * Evaluates an ISNULL function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return Boolean CellValue indicating if the value is null
     */
    public static CellValue evaluateIsNull(CellValue[] args) {
        return evaluateIsNull(List.of(args));
    }
    
    /**
     * Evaluates an ISNOTNULL function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return Boolean CellValue indicating if the value is not null
     */
    public static CellValue evaluateIsNotNull(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("ISNOTNULL", arguments, 1);
        
        boolean isNotNull = !(arguments.get(0) == null || arguments.get(0) instanceof CellValue.NullValue);
        return new CellValue.BooleanValue(isNotNull);
    }
    
    /**
     * Evaluates an ISNOTNULL function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return Boolean CellValue indicating if the value is not null
     */
    public static CellValue evaluateIsNotNull(CellValue[] args) {
        return evaluateIsNotNull(List.of(args));
    }
    
    /**
     * Evaluates a GREATEST function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return CellValue with the greatest value
     */
    public static CellValue evaluateGreatest(List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("GREATEST", arguments, 1);
        
        CellValue greatest = arguments.get(0);
        for (int i = 1; i < arguments.size(); i++) {
            if (FunctionUtils.compareValues(arguments.get(i), greatest) > 0) {
                greatest = arguments.get(i);
            }
        }
        
        return greatest;
    }
    
    /**
     * Evaluates a GREATEST function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return CellValue with the greatest value
     */
    public static CellValue evaluateGreatest(CellValue[] args) {
        return evaluateGreatest(List.of(args));
    }
    
    /**
     * Evaluates a LEAST function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return CellValue with the smallest value
     */
    public static CellValue evaluateLeast(List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("LEAST", arguments, 1);
        
        CellValue least = arguments.get(0);
        for (int i = 1; i < arguments.size(); i++) {
            if (FunctionUtils.compareValues(arguments.get(i), least) < 0) {
                least = arguments.get(i);
            }
        }
        
        return least;
    }
    
    /**
     * Evaluates a LEAST function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return CellValue with the smallest value
     */
    public static CellValue evaluateLeast(CellValue[] args) {
        return evaluateLeast(List.of(args));
    }
}
