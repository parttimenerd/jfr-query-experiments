package me.bechberger.jfr.extended.evaluator;

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
    }
    
    /**
     * Simple record to represent a GC event
     */
    public static record GcEvent(Object id, long timestamp) {
    }
    
    static CellValue evaluateCount(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            // COUNT(*) - count all rows
            return new CellValue.NumberValue(context.getAllValues("*").size());
        } else {
            // COUNT(field) - count non-null values
            String fieldName = arguments.get(0).getValue().toString();
            return new CellValue.NumberValue(context.getAllValues(fieldName).stream()
                    .filter(v -> !(v instanceof CellValue.NullValue))
                    .count());
        }
    }
    
    static CellValue evaluateAvg(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            throw new RuntimeException("AVG requires at least one argument");
        }
        
        String fieldName = arguments.get(0).getValue().toString();
        List<CellValue> values = context.getAllValues(fieldName);
        
        return CellValue.mapDouble(values, doubles -> 
            doubles.stream().mapToDouble(Double::doubleValue).average().orElse(0.0));
    }
    
    static CellValue evaluateSum(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            throw new RuntimeException("SUM requires at least one argument");
        }
        
        String fieldName = arguments.get(0).getValue().toString();
        List<CellValue> values = context.getAllValues(fieldName);
        
        return CellValue.mapDouble(values, doubles -> 
            doubles.stream().mapToDouble(Double::doubleValue).sum());
    }
    
    static CellValue evaluateMin(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            throw new RuntimeException("MIN requires at least one argument");
        }
        
        String fieldName = arguments.get(0).getValue().toString();
        List<CellValue> values = context.getAllValues(fieldName);
        
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
        if (arguments.isEmpty()) {
            throw new RuntimeException("MAX requires at least one argument");
        }
        
        String fieldName = arguments.get(0).getValue().toString();
        List<CellValue> values = context.getAllValues(fieldName);
        
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
        if (arguments.size() != 2) {
            throw new RuntimeException("PERCENTILE requires exactly 2 arguments");
        }
        
        double percentile = arguments.get(0).extractNumericValue();
        String fieldName = arguments.get(1).getValue().toString();
        
        return calculatePercentile(context.getAllValues(fieldName), percentile);
    }
    
    static CellValue evaluateP99(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            throw new RuntimeException("P99 requires at least one argument");
        }
        
        String fieldName = arguments.get(0).getValue().toString();
        return calculatePercentile(context.getAllValues(fieldName), 99.0);
    }
    
    static CellValue evaluateP95(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            throw new RuntimeException("P95 requires at least one argument");
        }
        
        String fieldName = arguments.get(0).getValue().toString();
        return calculatePercentile(context.getAllValues(fieldName), 95.0);
    }
    
    static CellValue evaluateP90(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            throw new RuntimeException("P90 requires at least one argument");
        }
        
        String fieldName = arguments.get(0).getValue().toString();
        return calculatePercentile(context.getAllValues(fieldName), 90.0);
    }
    
    static CellValue evaluateP50(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            throw new RuntimeException("P50 requires at least one argument");
        }
        
        String fieldName = arguments.get(0).getValue().toString();
        return calculatePercentile(context.getAllValues(fieldName), 50.0);
    }
    
    static CellValue evaluateP999(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            throw new RuntimeException("P999 requires at least one argument");
        }
        
        String fieldName = arguments.get(0).getValue().toString();
        return calculatePercentile(context.getAllValues(fieldName), 99.9);
    }
    
    static CellValue evaluateStddev(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            throw new RuntimeException("STDDEV requires at least one argument");
        }
        
        String fieldName = arguments.get(0).getValue().toString();
        List<CellValue> values = context.getAllValues(fieldName);
        
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
        
        return new CellValue.FloatValue(Math.sqrt(variance / count));
    }
    
    static CellValue evaluateVariance(EvaluationContext context, List<CellValue> arguments) {
        if (arguments.isEmpty()) {
            throw new RuntimeException("VARIANCE requires at least one argument");
        }
        
        String fieldName = arguments.get(0).getValue().toString();
        List<CellValue> values = context.getAllValues(fieldName);
        
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
}
