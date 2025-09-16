package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.engine.exception.FunctionArgumentException;
import me.bechberger.jfr.extended.engine.exception.QueryEvaluationException;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.ast.ASTNodes;
import java.util.List;
import java.util.TreeMap;
import java.util.Map;
import java.util.HashMap;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * Static methods for evaluating aggregate functions.
 * Package-private methods for use within the evaluator package.
 */
public class AggregateFunctions {
    
    /**
     * Interface for executing raw JFR queries
     */
    @FunctionalInterface
    public interface JfrQueryExecutor {
        List<List<CellValue>> execute(ASTNodes.RawJfrQueryNode queryNode);
    }

    /**
     * Interface for executing extended queries  
     */
    @FunctionalInterface
    public interface ExtendedQueryExecutor {
        List<List<CellValue>> execute(ASTNodes.QueryNode queryNode, EvaluationContext context);
    }
    
    /**
     * Evaluation context class for aggregate functions.
     * Supports context stacking and variable management for nested query execution.
     */
    public static class EvaluationContext {
        // Cached GC events for efficient lookups
        private TreeMap<Long, GcEvent> gcEventsByTimestamp;
        private boolean gcEventsCacheBuilt = false;
        
        // Query execution callbacks
        private final JfrQueryExecutor jfrQueryExecutor;
        private final ExtendedQueryExecutor extendedQueryExecutor;
        
        // Context stacking for nested queries and variable scope
        private final Stack<Map<String, Object>> variableStack = new Stack<>();
        private final Map<String, Object> globalVariables = new HashMap<>();
        
        /**
         * Constructor with query executors
         */
        public EvaluationContext(JfrQueryExecutor jfrQueryExecutor, ExtendedQueryExecutor extendedQueryExecutor) {
            this.jfrQueryExecutor = jfrQueryExecutor;
            this.extendedQueryExecutor = extendedQueryExecutor;
            // Initialize with empty local scope
            variableStack.push(new HashMap<>());
        }
        
        /**
         * Default constructor (for backward compatibility)
         */
        public EvaluationContext() {
            this(null, null);
        }
        
        /**
         * Push a new variable scope onto the stack
         */
        public void pushScope() {
            variableStack.push(new HashMap<>());
        }
        
        /**
         * Pop the current variable scope from the stack
         */
        public void popScope() {
            if (variableStack.size() > 1) {
                variableStack.pop();
            }
        }
        
        /**
         * Set a variable in the current scope
         */
        public void setVariable(String name, Object value) {
            variableStack.peek().put(name, value);
        }
        
        /**
         * Get a variable, searching from current scope up to global scope
         */
        public Object getVariable(String name) {
            // Search from current scope up the stack
            for (int i = variableStack.size() - 1; i >= 0; i--) {
                Map<String, Object> scope = variableStack.get(i);
                if (scope.containsKey(name)) {
                    return scope.get(name);
                }
            }
            // Check global variables
            return globalVariables.get(name);
        }
        
        /**
         * Set a global variable
         */
        public void setGlobalVariable(String name, Object value) {
            globalVariables.put(name, value);
        }

        /**
         * Remove a variable from the current scope
         */
        public void removeVariable(String name) {
            // Remove from current scope first
            if (!variableStack.isEmpty()) {
                variableStack.peek().remove(name);
            }
            // Also remove from global scope if present
            globalVariables.remove(name);
        }
        
        public CellValue getFieldValue(String fieldName) {
            throw new UnsupportedOperationException("Subclasses must implement getFieldValue");
        }
        
        public List<CellValue> getAllValues(String fieldName) {
            throw new UnsupportedOperationException("Subclasses must implement getAllValues");
        }
        
        /**
         * Get GC events as a cached TreeMap for efficient lookups
         */
        public TreeMap<Long, GcEvent> getGcEventsMap() {
            if (!gcEventsCacheBuilt) {
                gcEventsByTimestamp = new TreeMap<>();
                buildGcEventsCache();
                gcEventsCacheBuilt = true;
            }
            return gcEventsByTimestamp;
        }
        
        /**
         * Build the GC events cache from the context data
         * Subclasses should override this to provide actual GC data
         */
        protected void buildGcEventsCache() {
            // Default implementation - to be overridden by subclasses
            // Sample data for testing:
            gcEventsByTimestamp.put(1000L, new GcEvent(1, 1000L));
            gcEventsByTimestamp.put(2000L, new GcEvent(2, 2000L));
            gcEventsByTimestamp.put(3000L, new GcEvent(3, 3000L));
        }
        
        /**
         * Clear the GC events cache
         */
        public void clearGcEventsCache() {
            gcEventsByTimestamp = null;
            gcEventsCacheBuilt = false;
        }
        
        /**
         * Execute a raw JFR query using the provided executor
         */
        public List<List<CellValue>> jfrQuery(me.bechberger.jfr.extended.ast.ASTNodes.RawJfrQueryNode rawQueryNode) {
            if (jfrQueryExecutor == null) {
                throw new UnsupportedOperationException("Raw JFR query execution not supported - no executor provided");
            }
            return jfrQueryExecutor.execute(rawQueryNode);
        }
        
        /**
         * Execute an extended query using the evaluation context
         */
        public List<List<CellValue>> extendedQuery(me.bechberger.jfr.extended.ast.ASTNodes.QueryNode queryNode) {
            if (extendedQueryExecutor == null) {
                throw new UnsupportedOperationException("Extended query execution not supported - no executor provided");
            }
            return extendedQueryExecutor.execute(queryNode, this);
        }
        
        /**
         * Parse and execute a query string (which might be multiline)
         * Routes to appropriate execution method based on query type
         */
        public List<List<CellValue>> query(String queryString) {
            throw new UnsupportedOperationException("Query parsing requires external parser - use QueryEvaluator.query() instead");
        }
        
        /**
         * Evaluate an expression argument to get all values for aggregate functions.
         * This method should handle qualified field references like "i.innerValue" properly.
         * 
         * @param argument The function argument (could be field reference, expression, etc.)
         * @return List of values to be aggregated
         */
        public List<CellValue> evaluateExpressionForAggregation(CellValue argument) {
            if (argument instanceof CellValue.ArrayValue arrayValue) {
                // Already processed by QueryEvaluator - contains all values from the group
                return arrayValue.elements();
            } else if (argument instanceof CellValue.StringValue stringValue) {
                // Legacy fallback: simple field name lookup
                String fieldName = stringValue.getValue().toString();
                return getAllValues(fieldName);
            } else {
                // Single value case
                return List.of(argument);
            }
        }
    }
    
    /**
     * Simple record to represent a GC event
     */
    public static record GcEvent(Object id, long timestamp) {
    }
    
    /**
     * Helper method to extract values from function arguments.
     * Delegates to EvaluationContext to properly handle qualified field references and table aliases.
     */
    private static List<CellValue> extractValuesFromArguments(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            return List.of();
        }
        
        // Delegate to the context to handle the complexity of expression evaluation
        return context.evaluateExpressionForAggregation(arguments.get(0));
    }
    
    static CellValue evaluateCount(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            // COUNT(*) - should not happen as QueryEvaluator passes row count
            return new CellValue.NumberValue(0L);
        } else if (arguments.get(0) instanceof CellValue.NumberValue numberValue) {
            // COUNT(*) - QueryEvaluator passes the row count directly
            return numberValue;
        } else if (arguments.get(0) instanceof CellValue.ArrayValue arrayValue) {
            // COUNT(field) - count non-null values in the array
            return new CellValue.NumberValue(arrayValue.elements().stream()
                    .filter(v -> !(v instanceof CellValue.NullValue))
                    .count());
        } else {
            // Single field value - treat as count of 1 if not null
            return new CellValue.NumberValue(arguments.get(0) instanceof CellValue.NullValue ? 0L : 1L);
        }
    }
    
    static CellValue evaluateAvg(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("AVG", arguments, 1);
        
        List<CellValue> values = extractValuesFromArguments(context, arguments);
        
        return CellValue.mapDouble(values, doubles -> 
            doubles.stream().mapToDouble(Double::doubleValue).average().orElse(0.0));
    }
    
    static CellValue evaluateSum(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("SUM", arguments, 1);
        
        List<CellValue> values = extractValuesFromArguments(context, arguments);
        
        return CellValue.mapDouble(values, doubles -> 
            doubles.stream().mapToDouble(Double::doubleValue).sum());
    }
    
    static CellValue evaluateMin(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("MIN", arguments, 1);
        
        List<CellValue> values = extractValuesFromArguments(context, arguments);
        
        CellValue min = null;
        
        for (CellValue value : values) {
            if (!(value instanceof CellValue.NullValue)) {
                if (min == null || CellValue.compare(value, min) < 0) {
                    min = value;
                }
            }
        }
        
        return min != null ? min : new CellValue.NullValue();
    }
    
    static CellValue evaluateMax(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("MAX", arguments, 1);
        
        List<CellValue> values = extractValuesFromArguments(context, arguments);
        
        CellValue max = null;
        
        for (CellValue value : values) {
            if (!(value instanceof CellValue.NullValue)) {
                if (max == null || CellValue.compare(value, max) > 0) {
                    max = value;
                }
            }
        }
        
        return max != null ? max : new CellValue.NullValue();
    }
    
    static CellValue evaluatePercentile(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertArgumentCount("PERCENTILE", arguments, 2);
        
        double percentile = arguments.get(0).extractNumericValue();
        List<CellValue> values = context.evaluateExpressionForAggregation(arguments.get(1));
        
        return calculatePercentile(values, percentile);
    }
    
    static CellValue evaluateP99(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("P99", arguments, 1);
        
        List<CellValue> values = context.evaluateExpressionForAggregation(arguments.get(0));
        return calculatePercentile(values, 99.0);
    }
    
    static CellValue evaluateP95(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("P95", arguments, 1);
        
        List<CellValue> values = context.evaluateExpressionForAggregation(arguments.get(0));
        return calculatePercentile(values, 95.0);
    }
    
    static CellValue evaluateP90(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("P90", arguments, 1);
        
        List<CellValue> values = context.evaluateExpressionForAggregation(arguments.get(0));
        return calculatePercentile(values, 90.0);
    }
    
    static CellValue evaluateP50(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("P50", arguments, 1);
        
        List<CellValue> values = context.evaluateExpressionForAggregation(arguments.get(0));
        return calculatePercentile(values, 50.0);
    }
    
    static CellValue evaluateP999(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("P999", arguments, 1);
        
        List<CellValue> values = context.evaluateExpressionForAggregation(arguments.get(0));
        return calculatePercentile(values, 99.9);
    }
    
    static CellValue evaluateStddev(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("STDDEV", arguments, 1);
        
        List<CellValue> values = context.evaluateExpressionForAggregation(arguments.get(0));
        
        // First calculate mean
        double sum = 0.0;
        int count = 0;
        
        for (CellValue value : values) {
            if (!(value instanceof CellValue.NullValue) && value.isNumeric()) {
                sum += value.extractNumericValue();
                count++;
            }
        }
        
        if (count == 0) return new CellValue.NullValue();
        
        double mean = sum / count;
        
        // Calculate variance
        double variance = 0.0;
        for (CellValue value : values) {
            if (!(value instanceof CellValue.NullValue) && value.isNumeric()) {
                double numericValue = value.extractNumericValue();
                variance += Math.pow(numericValue - mean, 2);
            }
        }
        
        return new CellValue.NumberValue(Math.sqrt(variance / count));
    }
    
    static CellValue evaluateVariance(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertAtLeastArguments("VARIANCE", arguments, 1);
        
        List<CellValue> values = context.evaluateExpressionForAggregation(arguments.get(0));
        
        return CellValue.mapDouble(values, doubles -> {
            if (doubles.isEmpty()) return 0.0;
            
            double mean = doubles.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
            return doubles.stream()
                    .mapToDouble(d -> Math.pow(d - mean, 2))
                    .average()
                    .orElse(0.0);
        });
    }
    
    private static CellValue calculatePercentile(List<CellValue> values, double percentile) {
        if (values.isEmpty()) {
            return new CellValue.NullValue();
        }
        
        // Filter out null values and sort
        List<CellValue> sortedValues = values.stream()
                .filter(v -> !(v instanceof CellValue.NullValue))
                .sorted(CellValue::compare)
                .collect(Collectors.toList());
        
        if (sortedValues.isEmpty()) {
            return new CellValue.NullValue();
        }
        
        if (sortedValues.size() == 1) {
            return sortedValues.get(0);
        }
        
        // Calculate percentile index
        double index = (percentile / 100.0) * (sortedValues.size() - 1);
        int lowerIndex = (int) Math.floor(index);
        int upperIndex = (int) Math.ceil(index);
        
        if (lowerIndex == upperIndex) {
            return sortedValues.get(lowerIndex);
        }
        
        // Interpolate between the two values
        CellValue lowerValue = sortedValues.get(lowerIndex);
        CellValue upperValue = sortedValues.get(upperIndex);
        
        if (lowerValue.isNumeric() && upperValue.isNumeric()) {
            double lower = lowerValue.extractNumericValue();
            double upper = upperValue.extractNumericValue();
            double weight = index - lowerIndex;
            double interpolatedValue = lower + weight * (upper - lower);
            
            // Return result with the same type as the input values (preserve type)
            return lowerValue.mapNumeric(x -> interpolatedValue);
        }
        
        // For non-numeric values, return the lower value
        return lowerValue;
    }
    
    /**
     * COLLECT aggregate function - collect all values of an expression in the group into an array
     * 
     * @param context The evaluation context containing grouped data
     * @param arguments List containing the field name to collect
     * @return CellValue.ArrayValue containing all values from the group
     */
    static CellValue evaluateCollect(EvaluationContext context, List<CellValue> arguments) {
        FunctionUtils.assertArgumentRange("COLLECT", arguments, 1, 2);
        
        List<CellValue> values = context.evaluateExpressionForAggregation(arguments.get(0));
        return new CellValue.ArrayValue(values);
    }
}
