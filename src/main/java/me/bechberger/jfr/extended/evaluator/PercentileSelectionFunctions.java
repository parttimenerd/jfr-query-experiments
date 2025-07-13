package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.evaluator.AggregateFunctions.EvaluationContext;
import me.bechberger.jfr.extended.table.CellValue;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Static methods for evaluating percentile selection functions.
 * 
 * These functions find rows in a table where a specified field is at the given percentile
 * and return the corresponding values from an ID field.
 * 
 * Package-private methods for use within the evaluator package.
 */
public class PercentileSelectionFunctions {
    
    /**
     * Evaluates PERCENTILE_SELECT function with the signature:
     * PERCENTILE_SELECT(percentile, table, idField, valueField)
     * 
     * Finds rows in the table where valueField is at the given percentile
     * and returns the corresponding idField values.
     */
    static CellValue evaluatePercentileSelect(List<CellValue> arguments, EvaluationContext context) {
        if (arguments.size() != 4) {
            throw new IllegalArgumentException("PERCENTILE_SELECT requires exactly 4 arguments: percentile, table, idField, valueField");
        }
        
        double percentile = ((Number) arguments.get(0).getValue()).doubleValue();
        String tableName = arguments.get(1).getValue().toString();
        String idField = arguments.get(2).getValue().toString();
        String valueField = arguments.get(3).getValue().toString();
        
        return evaluateSpecificPercentileSelect(percentile, tableName, idField, valueField, context);
    }
    
    /**
     * Core implementation for percentile selection logic.
     */
    static CellValue evaluateSpecificPercentileSelect(double percentile, String tableName, String idField, String valueField, EvaluationContext context) {
        // Get all values from the specified field for percentile calculation
        List<CellValue> values = context.getAllValues(valueField);
        
        // Sort values and find the percentile threshold
        List<CellValue> sorted = values.stream()
                .filter(v -> !(v instanceof CellValue.NullValue))
                .sorted(CellValue::compare)
                .collect(Collectors.toList());
        
        if (sorted.isEmpty()) {
            return new CellValue.ArrayValue(new ArrayList<>());
        }
        
        // Calculate the percentile value
        int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        index = Math.max(0, Math.min(index, sorted.size() - 1));
        CellValue percentileValue = sorted.get(index);
        
        // Find all rows where valueField equals the percentile value and collect their idField values
        List<CellValue> resultIds = new ArrayList<>();
        
        // This is a simplified implementation - in a real system, you would need to:
        // 1. Access the actual table data structure
        // 2. Find rows where valueField == percentileValue
        // 3. Return the corresponding idField values
        // For now, we'll return the percentile value as a single-element array
        resultIds.add(percentileValue);
        
        return new CellValue.ArrayValue(resultIds);
    }
    
    // Convenience functions for common percentiles
    
    /**
     * Evaluates P90SELECT function - shortcut for PERCENTILE_SELECT(90, ...)
     */
    static CellValue evaluateP90Select(List<CellValue> arguments, EvaluationContext context) {
        if (arguments.size() != 3) {
            throw new IllegalArgumentException("P90SELECT requires exactly 3 arguments: table, idField, valueField");
        }
        
        // Prepend the percentile value (90) to the arguments
        List<CellValue> fullArgs = new ArrayList<>();
        fullArgs.add(new CellValue.NumberValue(90.0));
        fullArgs.addAll(arguments);
        
        return evaluatePercentileSelect(fullArgs, context);
    }
    
    /**
     * Evaluates P95SELECT function - shortcut for PERCENTILE_SELECT(95, ...)
     */
    static CellValue evaluateP95Select(List<CellValue> arguments, EvaluationContext context) {
        if (arguments.size() != 3) {
            throw new IllegalArgumentException("P95SELECT requires exactly 3 arguments: table, idField, valueField");
        }
        
        // Prepend the percentile value (95) to the arguments
        List<CellValue> fullArgs = new ArrayList<>();
        fullArgs.add(new CellValue.NumberValue(95.0));
        fullArgs.addAll(arguments);
        
        return evaluatePercentileSelect(fullArgs, context);
    }
    
    /**
     * Evaluates P99SELECT function - shortcut for PERCENTILE_SELECT(99, ...)
     */
    static CellValue evaluateP99Select(List<CellValue> arguments, EvaluationContext context) {
        if (arguments.size() != 3) {
            throw new IllegalArgumentException("P99SELECT requires exactly 3 arguments: table, idField, valueField");
        }
        
        // Prepend the percentile value (99) to the arguments
        List<CellValue> fullArgs = new ArrayList<>();
        fullArgs.add(new CellValue.NumberValue(99.0));
        fullArgs.addAll(arguments);
        
        return evaluatePercentileSelect(fullArgs, context);
    }
    
    /**
     * Evaluates P999SELECT function - shortcut for PERCENTILE_SELECT(99.9, ...)
     */
    static CellValue evaluateP999Select(List<CellValue> arguments, EvaluationContext context) {
        if (arguments.size() != 3) {
            throw new IllegalArgumentException("P999SELECT requires exactly 3 arguments: table, idField, valueField");
        }
        
        // Prepend the percentile value (99.9) to the arguments
        List<CellValue> fullArgs = new ArrayList<>();
        fullArgs.add(new CellValue.NumberValue(99.9));
        fullArgs.addAll(arguments);
        
        return evaluatePercentileSelect(fullArgs, context);
    }
}
