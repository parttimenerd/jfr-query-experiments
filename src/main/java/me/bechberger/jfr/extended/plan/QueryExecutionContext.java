package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTNodes.ExpressionNode;
import me.bechberger.jfr.extended.ast.ExpressionKey;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.evaluator.AggregateFunctions;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import me.bechberger.jfr.extended.engine.RawJfrQueryExecutor;

import java.util.*;

/**
 * Execution context with resource management for streaming query plans.
 * 
 * This class manages the execution environment for query plans,
 * including variable scoping, table aliases, memory tracking,
 * and metadata about available JFR events.
 * 
 * @author JFR Query Plan Architecture  
 * @since 2.0
 */
public class QueryExecutionContext {
    private final JFRFileMetadata metadata;
    private final RawJfrQueryExecutor rawJfrExecutor;
    private final MemoryUsageStats memoryStats;
    private final Stack<Map<String, JfrTable>> aliasScopes;
    private final Map<String, Object> variables;
    private final Map<String, LazyStreamingQuery> lazyVariables;
    private final Map<ExpressionKey, Object> computedAggregates; // Store aggregate values by expression key
    private final AggregateFunctions.EvaluationContext evaluationContext;
    private final FunctionRegistry functionRegistry;
    private final List<PlanExecutionTrace> executionTrace;
    private long rowsProcessed;
    private boolean memoryBoundedMode;
    private long maxMemoryThreshold;
    
    public QueryExecutionContext(JFRFileMetadata metadata, RawJfrQueryExecutor rawJfrExecutor) {
        this.metadata = metadata;
        this.rawJfrExecutor = rawJfrExecutor;
        this.memoryStats = new MemoryUsageStats();
        this.aliasScopes = new Stack<>();
        this.variables = new HashMap<>();
        this.lazyVariables = new HashMap<>();
        this.computedAggregates = new HashMap<>();
        this.functionRegistry = FunctionRegistry.getInstance();
        this.evaluationContext = new AggregateFunctions.EvaluationContext();
        this.executionTrace = new ArrayList<>();
        this.rowsProcessed = 0;
        this.memoryBoundedMode = false;
        this.maxMemoryThreshold = Runtime.getRuntime().maxMemory() / 2; // Default: 50% of max heap
        
        // Initialize with root scope
        this.aliasScopes.push(new HashMap<>());
    }

    // Legacy constructor for backward compatibility
    public QueryExecutionContext(JFRFileMetadata metadata) {
        this(metadata, null);
    }
    
    /**
     * Get JFR file metadata for optimization decisions.
     */
    public JFRFileMetadata getMetadata() {
        return metadata;
    }
    
    /**
     * Get the raw JFR query executor for nested query execution.
     */
    public RawJfrQueryExecutor getRawJfrExecutor() {
        return rawJfrExecutor;
    }
    
    /**
     * Get memory usage statistics.
     */
    public MemoryUsageStats getMemoryStats() {
        return memoryStats;
    }
    
    /**
     * Push a new alias scope (for subqueries, etc.).
     */
    public void pushScope() {
        aliasScopes.push(new HashMap<>());
    }
    
    /**
     * Pop the current alias scope.
     */
    public void popScope() {
        if (aliasScopes.size() > 1) {
            aliasScopes.pop();
        }
    }
    
    /**
     * Register a table alias in the current scope.
     */
    public void registerAlias(String alias, JfrTable table) {
        aliasScopes.peek().put(alias, table);
    }
    
    /**
     * Resolve a table alias from current and parent scopes.
     */
    public JfrTable resolveAlias(String alias, ASTNode contextNode) throws PlanExecutionException {
        // Search from current scope up to root
        for (int i = aliasScopes.size() - 1; i >= 0; i--) {
            Map<String, JfrTable> scope = aliasScopes.get(i);
            if (scope.containsKey(alias)) {
                return scope.get(alias);
            }
        }
        
        throw new PlanExecutionException(
            "Unknown table alias: " + alias,
            new JFRErrorContext(contextNode, "alias resolution", 
                "Alias '" + alias + "' not found in any scope")
        );
    }
    
    /**
     * Set a variable value.
     */
    public void setVariable(String name, Object value) {
        variables.put(name, value);
    }
    
    /**
     * Get a variable value.
     */
    public Object getVariable(String name) {
        return variables.get(name);
    }
    
    /**
     * Set a lazy variable that will be evaluated on demand.
     */
    public void setLazyVariable(String name, LazyStreamingQuery lazyQuery) {
        lazyVariables.put(name, lazyQuery);
    }
    
    /**
     * Get a lazy variable.
     */
    public LazyStreamingQuery getLazyVariable(String name) {
        return lazyVariables.get(name);
    }
    
    /**
     * Check if a lazy variable exists.
     */
    public boolean hasLazyVariable(String name) {
        return lazyVariables.containsKey(name);
    }
    
    /**
     * Get all lazy variables.
     */
    public Map<String, LazyStreamingQuery> getLazyVariables() {
        return Map.copyOf(lazyVariables);
    }
    
    /**
     * Get the evaluation context for function evaluation.
     */
    public AggregateFunctions.EvaluationContext getEvaluationContext() {
        return evaluationContext;
    }
    
    /**
     * Get the function registry.
     */
    public FunctionRegistry getFunctionRegistry() {
        return functionRegistry;
    }
    
    /**
     * Record that additional rows have been processed.
     */
    public void recordRowsProcessed(long additionalRows) {
        this.rowsProcessed += additionalRows;
        memoryStats.recordRowsProcessed(additionalRows);
    }
    
    /**
     * Get total rows processed in this execution context.
     */
    public long getRowsProcessed() {
        return rowsProcessed;
    }
    
    /**
     * Enable memory-bounded mode with specified threshold.
     */
    public void enableMemoryBoundedMode(long maxMemoryBytes) {
        this.memoryBoundedMode = true;
        this.maxMemoryThreshold = maxMemoryBytes;
    }
    
    /**
     * Check if memory-bounded mode is enabled.
     */
    public boolean isMemoryBoundedMode() {
        return memoryBoundedMode;
    }
    
    /**
     * Get the maximum memory threshold for bounded mode.
     */
    public long getMaxMemoryThreshold() {
        return maxMemoryThreshold;
    }
    
    /**
     * Check if memory usage has exceeded the threshold.
     */
    public boolean isMemoryThresholdExceeded() {
        if (!memoryBoundedMode) {
            return false;
        }
        
        memoryStats.updateMemoryUsage();
        return memoryStats.getHeapUsed() > maxMemoryThreshold;
    }
    
    /**
     * Record the start of a plan execution.
     */
    public void recordPlanStart(String planType, String planDescription) {
        long executionOrder = executionTrace.size() + 1;
        executionTrace.add(new PlanExecutionTrace(planType, planDescription, executionOrder, 
            "STARTED", null, rowsProcessed, memoryStats.getHeapUsed()));
    }
    
    /**
     * Record the successful completion of a plan execution.
     */
    public void recordPlanSuccess(String planType, String planDescription, long rowsProcessed) {
        long executionOrder = executionTrace.size() + 1;
        executionTrace.add(new PlanExecutionTrace(planType, planDescription, executionOrder, 
            "SUCCESS", null, rowsProcessed, memoryStats.getHeapUsed()));
    }
    
    /**
     * Record the failure of a plan execution.
     */
    public void recordPlanFailure(String planType, String planDescription, String errorMessage) {
        long executionOrder = executionTrace.size() + 1;
        executionTrace.add(new PlanExecutionTrace(planType, planDescription, executionOrder, 
            "FAILED", errorMessage, rowsProcessed, memoryStats.getHeapUsed()));
    }
    
    /**
     * Get the execution trace for debugging.
     */
    public List<PlanExecutionTrace> getExecutionTrace() {
        return new ArrayList<>(executionTrace);
    }
    
    /**
     * Get execution trace since a specific point. This allows getting execution traces
     * for individual statements in multi-statement queries.
     */
    public List<PlanExecutionTrace> getExecutionTraceSince(int startIndex) {
        if (startIndex >= executionTrace.size()) {
            return new ArrayList<>();
        }
        return new ArrayList<>(executionTrace.subList(startIndex, executionTrace.size()));
    }
    
    /**
     * Print the execution trace for debugging.
     */
    public void printExecutionTrace() {
        System.out.println("=== Query Execution Trace ===");
        for (PlanExecutionTrace trace : executionTrace) {
            System.out.println(trace);
        }
        System.out.println("=== End Execution Trace ===");
    }
    
    /**
     * Clear the execution trace.
     */
    public void clearExecutionTrace() {
        executionTrace.clear();
    }
    
    /**
     * Create a copy of this context for parallel execution.
     */
    public QueryExecutionContext copy() {
        QueryExecutionContext copy = new QueryExecutionContext(metadata, rawJfrExecutor);
        copy.variables.putAll(this.variables);
        copy.memoryBoundedMode = this.memoryBoundedMode;
        copy.maxMemoryThreshold = this.maxMemoryThreshold;
        
        // Copy alias scopes
        copy.aliasScopes.clear();
        for (Map<String, JfrTable> scope : this.aliasScopes) {
            copy.aliasScopes.push(new HashMap<>(scope));
        }
        
        // Copy execution trace
        copy.executionTrace.addAll(this.executionTrace);
        
        return copy;
    }
    
    /**
     * Record of a plan execution step for debugging.
     */
    public static class PlanExecutionTrace {
        private final String planType;
        private final String planDescription;
        private final long timestamp;
        private final long executionOrder;
        private final String status;
        private final String errorMessage;
        private final long rowsProcessed;
        private final long memoryUsed;
        
        public PlanExecutionTrace(String planType, String planDescription, long executionOrder, 
                                String status, String errorMessage, long rowsProcessed, long memoryUsed) {
            this.planType = planType;
            this.planDescription = planDescription;
            this.timestamp = System.currentTimeMillis();
            this.executionOrder = executionOrder;
            this.status = status;
            this.errorMessage = errorMessage;
            this.rowsProcessed = rowsProcessed;
            this.memoryUsed = memoryUsed;
        }
        
        public String getPlanType() { return planType; }
        public String getPlanDescription() { return planDescription; }
        public long getTimestamp() { return timestamp; }
        public long getExecutionOrder() { return executionOrder; }
        public String getStatus() { return status; }
        public String getErrorMessage() { return errorMessage; }
        public long getRowsProcessed() { return rowsProcessed; }
        public long getMemoryUsed() { return memoryUsed; }
        
        @Override
        public String toString() {
            return String.format("[%d] %s: %s (%s) - rows: %d, memory: %d KB%s",
                executionOrder, planType, planDescription, status, rowsProcessed, memoryUsed / 1024,
                errorMessage != null ? " ERROR: " + errorMessage : "");
        }
    }
    
    // ===== COMPUTED AGGREGATE MANAGEMENT =====
    
    /**
     * Store a computed aggregate value by its expression node.
     * This allows HAVING and ORDER BY plans to access aggregates computed during GROUP BY.
     */
    public void setComputedAggregate(ExpressionNode expressionNode, Object value) {
        computedAggregates.put(new ExpressionKey(expressionNode), value);
    }
    
    /**
     * Get a computed aggregate value by its expression node.
     * Returns null if the aggregate was not computed.
     * This method uses ExpressionKey for proper semantic equality comparison.
     */
    public Object getComputedAggregate(ExpressionNode expressionNode) {
        return computedAggregates.get(new ExpressionKey(expressionNode));
    }
    
    /**
     * Check if an aggregate value has been computed.
     * This method uses ExpressionKey for proper semantic equality comparison.
     */
    public boolean hasComputedAggregate(ExpressionNode expressionNode) {
        return computedAggregates.containsKey(new ExpressionKey(expressionNode));
    }
    
    /**
     * Clear all computed aggregate values.
     * This is typically called when entering a new query scope.
     */
    public void clearComputedAggregates() {
        computedAggregates.clear();
    }
    
}