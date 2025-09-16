package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.table.CellValue;
import java.util.List;

/**
 * Static methods for evaluating conversion functions.
 * Package-private methods for use within the evaluator package.
 */
public class ConversionFunctions {
    
    /**
     * Evaluates a BOOLEAN function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return CellValue result of the BOOLEAN conversion
     */
    public static CellValue evaluateBoolean(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("BOOLEAN", arguments, 1);
        
        boolean result = FunctionUtils.toBoolean(arguments.get(0));
        return new CellValue.BooleanValue(result);
    }
    
    /**
     * Evaluates a BOOLEAN function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return CellValue result of the BOOLEAN conversion
     */
    public static CellValue evaluateBoolean(CellValue[] args) {
        return evaluateBoolean(List.of(args));
    }
    
    /**
     * Evaluates a STRING function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return CellValue result of the STRING conversion
     */
    public static CellValue evaluateString(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("STRING", arguments, 1);
        
        CellValue value = arguments.get(0);
        if (value instanceof CellValue.NullValue) {
            return new CellValue.StringValue("");
        }
        
        String result = value.toString();
        return new CellValue.StringValue(result);
    }
    
    /**
     * Evaluates a STRING function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return CellValue result of the STRING conversion
     */
    public static CellValue evaluateString(CellValue[] args) {
        return evaluateString(List.of(args));
    }
    
    /**
     * Evaluates a NUMBER function with a List of CellValues
     * @param arguments List of CellValue arguments
     * @return CellValue result of the NUMBER conversion
     */
    public static CellValue evaluateNumber(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("NUMBER", arguments, 1);
        
        CellValue value = arguments.get(0);
        if (value instanceof CellValue.NullValue) {
            return new CellValue.NumberValue(0);
        }
        
        if (value instanceof CellValue.NumberValue numberValue) {
            return numberValue;
        }
        
        if (value instanceof CellValue.NumberValue floatValue) {
            return new CellValue.NumberValue((long) floatValue.value());
        }
        
        if (value instanceof CellValue.StringValue stringValue) {
            try {
                String str = stringValue.value().trim();
                if (str.contains(".")) {
                    return new CellValue.NumberValue((long) Double.parseDouble(str));
                } else {
                    return new CellValue.NumberValue(Long.parseLong(str));
                }
            } catch (NumberFormatException e) {
                return new CellValue.NumberValue(0);
            }
        }
        
        if (value instanceof CellValue.BooleanValue booleanValue) {
            return new CellValue.NumberValue(booleanValue.value() ? 1 : 0);
        }
        
        return new CellValue.NumberValue(0);
    }
    
    /**
     * Evaluates a NUMBER function with CellValue arguments
     * @param args Array of CellValue arguments
     * @return CellValue result of the NUMBER conversion
     */
    public static CellValue evaluateNumber(CellValue[] args) {
        return evaluateNumber(List.of(args));
    }
}
