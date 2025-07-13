package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.table.CellValue;
import java.util.List;

/**
 * Static methods for evaluating string functions.
 * Package-private methods for use within the evaluator package.
 */
public class StringFunctions {
    
    /**
     * Concatenates multiple string values
     */
    static CellValue evaluateConcat(List<CellValue> arguments) {
        if (arguments.size() < 2) {
            throw new IllegalArgumentException("CONCAT function requires at least 2 arguments");
        }
        
        StringBuilder result = new StringBuilder();
        for (CellValue arg : arguments) {
            if (arg != null && !(arg instanceof CellValue.NullValue)) {
                result.append(arg.toString());
            }
        }
        return new CellValue.StringValue(result.toString());
    }
    
    /**
     * Extracts a substring from a string
     */
    static CellValue evaluateSubstring(List<CellValue> arguments) {
        if (arguments.size() < 2 || arguments.size() > 3) {
            throw new IllegalArgumentException("SUBSTRING function requires 2 or 3 arguments");
        }
        
        if (!(arguments.get(0) instanceof CellValue.StringValue)) {
            throw new IllegalArgumentException("SUBSTRING first argument must be a string");
        }
        
        String str = ((CellValue.StringValue) arguments.get(0)).value();
        
        if (!(arguments.get(1) instanceof CellValue.NumberValue)) {
            throw new IllegalArgumentException("SUBSTRING second argument must be a number");
        }
        
        int start = (int) ((CellValue.NumberValue) arguments.get(1)).value();
        
        if (arguments.size() == 3) {
            if (!(arguments.get(2) instanceof CellValue.NumberValue)) {
                throw new IllegalArgumentException("SUBSTRING third argument must be a number");
            }
            
            int length = (int) ((CellValue.NumberValue) arguments.get(2)).value();
            int end = Math.min(start + length, str.length());
            return new CellValue.StringValue(str.substring(Math.max(0, start), Math.max(0, end)));
        } else {
            return new CellValue.StringValue(str.substring(Math.max(0, start)));
        }
    }
    
    /**
     * Converts a string to uppercase
     */
    static CellValue evaluateUpper(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("UPPER function requires exactly 1 argument");
        }
        
        if (!(arguments.get(0) instanceof CellValue.StringValue)) {
            throw new IllegalArgumentException("UPPER argument must be a string");
        }
        
        return new CellValue.StringValue(((CellValue.StringValue) arguments.get(0)).value().toUpperCase());
    }
    
    /**
     * Converts a string to lowercase
     */
    static CellValue evaluateLower(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("LOWER function requires exactly 1 argument");
        }
        
        if (!(arguments.get(0) instanceof CellValue.StringValue)) {
            throw new IllegalArgumentException("LOWER argument must be a string");
        }
        
        return new CellValue.StringValue(((CellValue.StringValue) arguments.get(0)).value().toLowerCase());
    }
    
    /**
     * Gets the length of a string
     */
    static CellValue evaluateLength(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("LENGTH function requires exactly 1 argument");
        }
        
        if (!(arguments.get(0) instanceof CellValue.StringValue)) {
            throw new IllegalArgumentException("LENGTH argument must be a string");
        }
        
        return new CellValue.NumberValue(((CellValue.StringValue) arguments.get(0)).value().length());
    }
    
    /**
     * Trims whitespace from a string
     */
    static CellValue evaluateTrim(List<CellValue> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("TRIM function requires exactly 1 argument");
        }
        
        if (!(arguments.get(0) instanceof CellValue.StringValue)) {
            throw new IllegalArgumentException("TRIM argument must be a string");
        }
        
        return new CellValue.StringValue(((CellValue.StringValue) arguments.get(0)).value().trim());
    }
    
    /**
     * Replaces all occurrences of a string with another
     */
    static CellValue evaluateReplace(List<CellValue> arguments) {
        if (arguments.size() != 3) {
            throw new IllegalArgumentException("REPLACE function requires exactly 3 arguments");
        }
        
        if (!(arguments.get(0) instanceof CellValue.StringValue)) {
            throw new IllegalArgumentException("REPLACE first argument must be a string");
        }
        
        if (!(arguments.get(1) instanceof CellValue.StringValue)) {
            throw new IllegalArgumentException("REPLACE second argument must be a string");
        }
        
        if (!(arguments.get(2) instanceof CellValue.StringValue)) {
            throw new IllegalArgumentException("REPLACE third argument must be a string");
        }
        
        String str = ((CellValue.StringValue) arguments.get(0)).value();
        String target = ((CellValue.StringValue) arguments.get(1)).value();
        String replacement = ((CellValue.StringValue) arguments.get(2)).value();
        
        return new CellValue.StringValue(str.replace(target, replacement));
    }
}
