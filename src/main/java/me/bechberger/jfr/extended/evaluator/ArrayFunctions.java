package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.engine.exception.TypeMismatchException;
import me.bechberger.jfr.extended.engine.exception.QueryEvaluationException;
import me.bechberger.jfr.extended.engine.exception.FunctionArgumentException;
import me.bechberger.jfr.extended.table.CellValue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Static methods for evaluating array functions that work on CellValue arrays.
 * These are non-aggregate functions that operate directly on array values.
 */
public class ArrayFunctions {
    
    /**
     * Get the first N elements from an array
     */
    static CellValue evaluateHead(List<CellValue> arguments) {
        CellValue[] args = arguments.toArray(new CellValue[0]);
        return evaluateHeadInternal(args);
    }
    
    /**
     * Internal implementation for HEAD function
     */
    private static CellValue evaluateHeadInternal(CellValue[] arguments) {
        List<CellValue> args = List.of(arguments);
        FunctionUtils.assertArgumentRange("HEAD", args, 1, 2);
        
        CellValue.ArrayValue arrayValue = FunctionUtils.getArrayArgument("HEAD", args, 0);
        List<CellValue> array = arrayValue.elements();
        
        if (arguments.length == 1) {
            // Return first element
            if (array.isEmpty()) {
                return new CellValue.NullValue();
            }
            return array.get(0);
        } else {
            // Return first N elements
            CellValue.NumberValue countValue = FunctionUtils.getNumberArgument("HEAD", args, 1);
            
            int count = (int) countValue.value();
            if (count <= 0) {
                return new CellValue.ArrayValue(new ArrayList<>());
            }
            
            int limit = Math.min(count, array.size());
            return new CellValue.ArrayValue(array.subList(0, limit));
        }
    }
    
    /**
     * Get the last N elements from an array
     */
    static CellValue evaluateTail(List<CellValue> arguments) {
        CellValue[] args = arguments.toArray(new CellValue[0]);
        return evaluateTailInternal(args);
    }
    
    /**
     * Internal implementation for TAIL function
     */
    private static CellValue evaluateTailInternal(CellValue[] arguments) {
        List<CellValue> args = List.of(arguments);
        FunctionUtils.assertArgumentRange("TAIL", args, 1, 2);
        
        CellValue.ArrayValue arrayValue = FunctionUtils.getArrayArgument("TAIL", args, 0);
        List<CellValue> array = arrayValue.elements();
        
        if (arguments.length == 1) {
            // Return last element (single element case)
            if (array.isEmpty()) {
                return new CellValue.NullValue();
            }
            return array.get(array.size() - 1);  // Returns single element, not array
        } else {
            // Return last N elements
            CellValue.NumberValue countValue = FunctionUtils.getNumberArgument("TAIL", args, 1);
            
            int count = (int) countValue.value();
            if (count <= 0) {
                return new CellValue.ArrayValue(new ArrayList<>());
            }
            
            int start = Math.max(0, array.size() - count);
            return new CellValue.ArrayValue(array.subList(start, array.size()));
        }
    }
    
    /**
     * Sort an array in ascending or descending order
     */
    static CellValue evaluateSort(List<CellValue> arguments) {
        CellValue[] args = arguments.toArray(new CellValue[0]);
        return evaluateSortInternal(args);
    }
    
    /**
     * Internal implementation for SORT function
     */
    private static CellValue evaluateSortInternal(CellValue[] arguments) {
        List<CellValue> args = List.of(arguments);
        FunctionUtils.assertArgumentRange("SORT", args, 1, 2);
        
        CellValue.ArrayValue arrayValue = FunctionUtils.getArrayArgument("SORT", args, 0);
        
        boolean ascending = true; // Default to ascending
        
        if (arguments.length == 2) {
            CellValue.StringValue directionValue = FunctionUtils.getStringArgument("SORT", args, 1);
            
            String direction = directionValue.value().toUpperCase();
            if ("DESC".equals(direction)) {
                ascending = false;
            } else if (!"ASC".equals(direction)) {
                throw FunctionArgumentException.forInvalidValue("SORT", 1, direction, "'ASC' or 'DESC'", null);
            }
        }
        
        List<CellValue> array = new ArrayList<>(arrayValue.elements());
        
        // Sort the array using CellValue.compare
        Comparator<CellValue> comparator = CellValue::compare;
        if (!ascending) {
            comparator = comparator.reversed();
        }
        
        array.sort(comparator);
        
        return new CellValue.ArrayValue(array);
    }
    
    /**
     * Alternative SORT implementation that takes a field name and sorts by that field
     */
    static CellValue evaluateSortByField(List<CellValue> arguments) {
        CellValue[] args = arguments.toArray(new CellValue[0]);
        return evaluateSortByFieldInternal(args);
    }
    
    /**
     * Internal implementation for SORT_BY function
     * This is for when we want to sort an array of objects by a specific field
     */
    private static CellValue evaluateSortByFieldInternal(CellValue[] arguments) {
        List<CellValue> args = List.of(arguments);
        FunctionUtils.assertArgumentCount("SORT_BY", args, 2);
        
        CellValue.ArrayValue arrayValue = FunctionUtils.getArrayArgument("SORT_BY", args, 0);
        CellValue.StringValue fieldNameValue = FunctionUtils.getStringArgument("SORT_BY", args, 1);
        
        // For now, just sort the values directly since we don't have object structure
        // In a more advanced implementation, this would sort by object fields
        // Field name: ((CellValue.StringValue) arguments[1]).value() (reserved for future use)
        List<CellValue> array = new ArrayList<>(arrayValue.elements());
        
        // For now, just sort the values directly since we don't have object structure
        // In a more advanced implementation, this would sort by object fields
        array.sort(CellValue::compare);
        
        return new CellValue.ArrayValue(array);
    }
    
    /**
     * Get the length of an array
     */
    static CellValue evaluateLength(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("LENGTH", arguments, 1);
        CellValue.ArrayValue arrayValue = FunctionUtils.getArrayArgument("LENGTH", arguments, 0);
        
        return new CellValue.NumberValue(arrayValue.elements().size());
    }
    
    /**
     * Get a slice of an array
     */
    static CellValue evaluateSlice(List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("SLICE", arguments, 3);
        
        CellValue.ArrayValue arrayValue = FunctionUtils.getArrayArgument("SLICE", arguments, 0);
        CellValue.NumberValue startValue = FunctionUtils.getNumberArgument("SLICE", arguments, 1);
        CellValue.NumberValue endValue = FunctionUtils.getNumberArgument("SLICE", arguments, 2);
        
        List<CellValue> array = arrayValue.elements();
        int start = (int) startValue.value();
        int end = (int) endValue.value();
        
        // Handle negative indices
        if (start < 0) start = array.size() + start;
        if (end < 0) end = array.size() + end;
        
        // Clamp to valid range
        start = Math.max(0, Math.min(start, array.size()));
        end = Math.max(start, Math.min(end, array.size()));
        
        return new CellValue.ArrayValue(array.subList(start, end));
    }
}
