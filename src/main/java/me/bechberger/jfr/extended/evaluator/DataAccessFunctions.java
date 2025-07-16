package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.engine.exception.FunctionArgumentException;
import me.bechberger.jfr.extended.engine.exception.QueryEvaluationException;
import me.bechberger.jfr.extended.table.CellValue;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Static methods for evaluating data access functions.
 * Package-private methods for use within the evaluator package.
 */
public class DataAccessFunctions {
    
    static CellValue evaluateFirst(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("FIRST", arguments, 1);
        
        String fieldName = arguments.get(0).getValue().toString();
        List<CellValue> values = context.getAllValues(fieldName);
        
        return values.isEmpty() ? new CellValue.NullValue() : values.get(0);
    }
    
    static CellValue evaluateLast(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("LAST", arguments, 1);
        
        String fieldName = arguments.get(0).getValue().toString();
        List<CellValue> values = context.getAllValues(fieldName);
        
        return values.isEmpty() ? new CellValue.NullValue() : values.get(values.size() - 1);
    }
    
    static CellValue evaluateLastBatch(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("LAST_BATCH", arguments, 2);
        
        String fieldName = arguments.get(0).getValue().toString();
        int batchSize = ((Number) arguments.get(1).getValue()).intValue();
        List<CellValue> values = context.getAllValues(fieldName);
        
        int start = Math.max(0, values.size() - batchSize);
        return new CellValue.ArrayValue(values.subList(start, values.size()));
    }
    
    static CellValue evaluateUnique(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("UNIQUE", arguments, 1);
        
        String fieldName = arguments.get(0).getValue().toString();
        List<CellValue> values = context.getAllValues(fieldName);
        
        return new CellValue.ArrayValue(values.stream().distinct().collect(Collectors.toList()));
    }
    
    static CellValue evaluateList(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("LIST", arguments, 1);
        
        String fieldName = arguments.get(0).getValue().toString();
        return new CellValue.ArrayValue(context.getAllValues(fieldName));
    }
    
    static CellValue evaluateDiff(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("DIFF", arguments, 1);
        
        String fieldName = arguments.get(0).getValue().toString();
        List<CellValue> values = context.getAllValues(fieldName);
        
        if (values.size() < 2) {
            return new CellValue.NullValue();
        }
        
        List<CellValue> differences = new ArrayList<>();
        for (int i = 1; i < values.size(); i++) {
            CellValue prev = values.get(i - 1);
            CellValue curr = values.get(i);
            
            if (prev.isNumeric() && curr.isNumeric()) {
                double diff = curr.extractNumericValue() - prev.extractNumericValue();
                // Preserve the type of the current value for the difference
                differences.add(curr.mapNumeric(x -> diff));
            } else {
                differences.add(new CellValue.NullValue());
            }
        }
        
        return new CellValue.ArrayValue(differences);
    }
    
    static CellValue evaluateHead(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("HEAD", arguments, 2);
        
        String fieldName = arguments.get(0).getValue().toString();
        int count = ((Number) arguments.get(1).getValue()).intValue();
        List<CellValue> values = context.getAllValues(fieldName);
        
        return new CellValue.ArrayValue(values.stream().limit(count).collect(Collectors.toList()));
    }
    
    static CellValue evaluateTail(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("TAIL", arguments, 2);
        
        String fieldName = arguments.get(0).getValue().toString();
        int count = ((Number) arguments.get(1).getValue()).intValue();
        List<CellValue> values = context.getAllValues(fieldName);
        
        int start = Math.max(0, values.size() - count);
        return new CellValue.ArrayValue(values.subList(start, values.size()));
    }
    
    static CellValue evaluateSlice(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("SLICE", arguments, 3);
        
        String fieldName = arguments.get(0).getValue().toString();
        int start = ((Number) arguments.get(1).getValue()).intValue();
        int end = ((Number) arguments.get(2).getValue()).intValue();
        List<CellValue> values = context.getAllValues(fieldName);
        
        start = Math.max(0, start);
        end = Math.min(values.size(), end);
        
        return start < end ? new CellValue.ArrayValue(values.subList(start, end)) : new CellValue.ArrayValue(new ArrayList<>());
    }
    
    static CellValue evaluateBeforeGc(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("BEFORE_GC", arguments, 1);
        
        Object timeArg = arguments.get(0).getValue();
        long timestamp = convertToTimestamp(timeArg);
        
        // Get cached GC events map
        TreeMap<Long, AggregateFunctions.GcEvent> gcEvents = context.getGcEventsMap();
        
        // Find the latest GC event before the given timestamp using TreeMap
        Map.Entry<Long, AggregateFunctions.GcEvent> beforeEntry = gcEvents.lowerEntry(timestamp);
        
        return beforeEntry != null ? 
            FunctionUtils.toCellValue(beforeEntry.getValue().id()) : 
            new CellValue.NullValue();
    }
    
    static CellValue evaluateAfterGc(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("AFTER_GC", arguments, 1);
        
        Object timeArg = arguments.get(0).getValue();
        long timestamp = convertToTimestamp(timeArg);
        
        // Get cached GC events map
        TreeMap<Long, AggregateFunctions.GcEvent> gcEvents = context.getGcEventsMap();
        
        // Find the earliest GC event after the given timestamp using TreeMap
        Map.Entry<Long, AggregateFunctions.GcEvent> afterEntry = gcEvents.higherEntry(timestamp);
        
        return afterEntry != null ? 
            FunctionUtils.toCellValue(afterEntry.getValue().id()) : 
            new CellValue.NullValue();
    }
    
    static CellValue evaluateClosestGc(AggregateFunctions.EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("CLOSEST_GC", arguments, 1);
        
        Object timeArg = arguments.get(0).getValue();
        long timestamp = convertToTimestamp(timeArg);
        
        // Get cached GC events map
        TreeMap<Long, AggregateFunctions.GcEvent> gcEvents = context.getGcEventsMap();
        
        if (gcEvents.isEmpty()) {
            return new CellValue.NullValue();
        }
        
        // Find the closest GC event using TreeMap floor/ceiling methods
        Map.Entry<Long, AggregateFunctions.GcEvent> floorEntry = gcEvents.floorEntry(timestamp);
        Map.Entry<Long, AggregateFunctions.GcEvent> ceilingEntry = gcEvents.ceilingEntry(timestamp);
        
        if (floorEntry == null) {
            return FunctionUtils.toCellValue(ceilingEntry.getValue().id());
        }
        if (ceilingEntry == null) {
            return FunctionUtils.toCellValue(floorEntry.getValue().id());
        }
        
        // Return the closer one
        long floorDistance = timestamp - floorEntry.getKey();
        long ceilingDistance = ceilingEntry.getKey() - timestamp;
        
        return FunctionUtils.toCellValue(floorDistance <= ceilingDistance ? 
            floorEntry.getValue().id() : ceilingEntry.getValue().id());
    }
    
    /**
     * Convert various time representations to timestamp (long milliseconds since epoch)
     */
    private static long convertToTimestamp(Object timeArg) {
        if (timeArg instanceof CellValue.TimestampValue timestampValue) {
            return timestampValue.value().toEpochMilli();
        } else if (timeArg instanceof java.time.Instant instant) {
            return instant.toEpochMilli();
        } else if (timeArg instanceof Long longValue) {
            return longValue;
        } else if (timeArg instanceof Number number) {
            return number.longValue();
        } else if (timeArg instanceof String stringValue) {
            try {
                // Try to parse as number (milliseconds since epoch)
                return Long.parseLong(stringValue);
            } catch (NumberFormatException e) {
                try {
                    // Try to parse as ISO timestamp
                    return java.time.Instant.parse(stringValue).toEpochMilli();
                } catch (Exception ex) {
                    throw QueryEvaluationException.forInvalidConversion("string", "timestamp", stringValue, null);
                }
            }
        } else {
            throw QueryEvaluationException.forInvalidConversion(
                timeArg != null ? timeArg.getClass().getSimpleName() : "null", 
                "timestamp", timeArg, null);
        }
    }
}
