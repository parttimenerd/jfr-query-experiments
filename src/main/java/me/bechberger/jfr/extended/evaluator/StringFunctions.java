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
        FunctionUtils.assertAtLeastArguments("CONCAT", arguments, 2);
        
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
        FunctionUtils.assertArgumentRange("SUBSTRING", arguments, 2, 3);
        
        CellValue.StringValue stringValue = FunctionUtils.getStringArgument("SUBSTRING", arguments, 0);
        CellValue.NumberValue startValue = FunctionUtils.getNumberArgument("SUBSTRING", arguments, 1);
        
        String str = stringValue.value();
        int start = (int) startValue.value();
        
        if (arguments.size() == 3) {
            CellValue.NumberValue lengthValue = FunctionUtils.getNumberArgument("SUBSTRING", arguments, 2);
            int length = (int) lengthValue.value();
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
        FunctionUtils.assertArgumentCount("UPPER", arguments, 1);
        CellValue.StringValue stringValue = FunctionUtils.getStringArgument("UPPER", arguments, 0);
        
        return new CellValue.StringValue(stringValue.value().toUpperCase());
    }
    
    /**
     * Converts a string to lowercase
     */
    static CellValue evaluateLower(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("LOWER", arguments, 1);
        CellValue.StringValue stringValue = FunctionUtils.getStringArgument("LOWER", arguments, 0);
        
        return new CellValue.StringValue(stringValue.value().toLowerCase());
    }
    
    /**
     * Gets the length of a string
     */
    static CellValue evaluateLength(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("LENGTH", arguments, 1);
        CellValue.StringValue stringValue = FunctionUtils.getStringArgument("LENGTH", arguments, 0);
        
        return new CellValue.NumberValue(stringValue.value().length());
    }
    
    /**
     * Trims whitespace from a string
     */
    static CellValue evaluateTrim(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("TRIM", arguments, 1);
        CellValue.StringValue stringValue = FunctionUtils.getStringArgument("TRIM", arguments, 0);
        
        return new CellValue.StringValue(stringValue.value().trim());
    }
    
    /**
     * Replaces all occurrences of a string with another
     */
    static CellValue evaluateReplace(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("REPLACE", arguments, 3);
        CellValue.StringValue stringValue = FunctionUtils.getStringArgument("REPLACE", arguments, 0);
        CellValue.StringValue targetValue = FunctionUtils.getStringArgument("REPLACE", arguments, 1);
        CellValue.StringValue replacementValue = FunctionUtils.getStringArgument("REPLACE", arguments, 2);
        
        return new CellValue.StringValue(stringValue.value().replace(targetValue.value(), replacementValue.value()));
    }
}
