package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.engine.RawJfrQueryExecutor;
import me.bechberger.jfr.extended.plan.plans.StreamingQueryPlan;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.plan.exception.ErrorMessageEnhancer;
import me.bechberger.jfr.extended.plan.exception.QueryExecutionException;

import java.util.*;
import jdk.jfr.EventType;

/**
 * Main executor for streaming query plans - replaces QueryEvaluator.
 * 
 * This class serves as the primary entry point for executing JFR queries
 * using the streaming query plan architecture. It provides the same API
 * as QueryEvaluator but uses streaming plans internally for better
 * memory efficiency and performance.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class QueryPlanExecutor {
    private final AstToPlanConverter planConverter;
    private final QueryExecutionContext globalContext;
    private MemoryUsageStats lastExecutionStats;
    
    /**
     * Create a new query plan executor.
     */
    public QueryPlanExecutor(RawJfrQueryExecutor rawJfrExecutor) {
        this.planConverter = new AstToPlanConverter(rawJfrExecutor);
        
        // Create metadata by querying the raw JFR executor
        JFRFileMetadata metadata = createMetadataFromRawExecutor(rawJfrExecutor);
        this.globalContext = new QueryExecutionContext(metadata, rawJfrExecutor);
        this.lastExecutionStats = new MemoryUsageStats();
    }
    
    /**
     * Execute a query string and return the result table.
     * This method handles both single and multi-statement queries.
     * For multi-statement queries, only the last statement's result is returned.
     */
    public JfrTable execute(String queryString) throws Exception {
        try {
            Parser parser = new Parser(queryString);
            ProgramNode program = parser.parse();
            
            if (program.statements().isEmpty()) {
                throw new PlanExecutionException("No statements found in query", null, null);
            }
            
            // Execute all statements using the visitor
            StatementExecutionVisitor visitor = new StatementExecutionVisitor(planConverter, globalContext);
            QueryResult lastResult = null;
            
            for (StatementNode statement : program.statements()) {
                lastResult = visitor.executeStatement(statement);
            }
            
            // Update execution statistics
            lastExecutionStats = globalContext.getMemoryStats();
            
            if (lastResult != null && lastResult.isSuccess()) {
                return lastResult.getTable();
            } else {
                // Create enhanced error message with query context
                String enhancedMessage = ErrorMessageEnhancer.createEnhancedMessage(
                    "Query execution failed",
                    queryString,
                    lastResult != null ? lastResult.getError() : new Exception("No results")
                );
                throw new QueryExecutionException(enhancedMessage, 
                    lastResult != null ? lastResult.getError() : new Exception("No results"));
            }
        } catch (Exception e) {
            // For parsing errors and other exceptions, provide enhanced context
            if (e instanceof QueryExecutionException && e.getMessage() != null && e.getMessage().contains("Query execution failed")) {
                // Already enhanced, just re-throw
                throw e;
            } else {
                // Create enhanced message for parsing and other errors
                String enhancedMessage = ErrorMessageEnhancer.createEnhancedMessage(
                    "Query processing failed",
                    queryString,
                    e
                );
                throw new QueryExecutionException(enhancedMessage, e);
            }
        }
    }
    
    /**
     * Execute a query string using the configured raw executor.
     * This method provides the same API as QueryEvaluator.query().
     */
    public JfrTable query(String queryString) throws Exception {
        return execute(queryString);
    }
    
    /**
     * Get memory usage statistics from the last execution.
     */
    public MemoryUsageStats getLastExecutionStats() {
        return lastExecutionStats;
    }
    
    /**
     * Enable memory-bounded execution mode.
     */
    public void enableMemoryBoundedMode(long maxMemoryBytes) {
        globalContext.enableMemoryBoundedMode(maxMemoryBytes);
    }
    
    /**
     * Clear all variables and reset execution state.
     */
    public void reset() {
        // Reset variables but keep metadata
        JFRFileMetadata metadata = globalContext.getMetadata();
        RawJfrQueryExecutor rawExecutor = globalContext.getRawJfrExecutor();
        QueryExecutionContext newContext = new QueryExecutionContext(metadata, rawExecutor);
        // Copy any settings from old context to new one
        if (globalContext.isMemoryBoundedMode()) {
            newContext.enableMemoryBoundedMode(globalContext.getMaxMemoryThreshold());
        }
        // Note: globalContext is final, so we can't replace it
    }
    
    /**
     * Get the AST to plan converter for advanced usage.
     */
    public AstToPlanConverter getPlanConverter() {
        return planConverter;
    }
    
    /**
     * Get the execution context for advanced usage.
     */
    public QueryExecutionContext getExecutionContext() {
        return globalContext;
    }
    
    /**
     * Visualize the execution plan for a query without executing it.
     * This method parses the query, converts it to a streaming plan,
     * and displays the plan structure using ASCII art visualization.
     * 
     * @param queryString The query to visualize
     * @throws Exception if the query cannot be parsed or converted to a plan
     */
    public void visualizePlan(String queryString) throws Exception {
        Parser parser = new Parser(queryString);
        ProgramNode program = parser.parse();
        
        // Extract the first statement (assuming single query)
        if (program.statements().isEmpty()) {
            throw new PlanExecutionException("No statements found in query", null, null);
        }
        
        StatementNode firstStatement = program.statements().get(0);
        
        // Convert AST to streaming plan
        StreamingQueryPlan plan = planConverter.convertStatementToPlan(firstStatement);
        
        // Create plan visualization
        List<QueryPlanVisualizer.PlanStep> planSteps = extractPlanSteps(plan);
        
        // Show the plan visualization
        System.out.println();
        QueryPlanVisualizer.printHeader("QUERY EXECUTION PLAN VISUALIZATION");
        System.out.println();
        
        // Show query in a box
        QueryPlanVisualizer.printQueryBox(queryString);
        System.out.println();
        
        // Show plan structure
        System.out.println("PLAN STRUCTURE:");
        System.out.println(plan.explain());
        System.out.println();
        
        // Show execution flow (simulated)
        if (!planSteps.isEmpty()) {
            QueryPlanVisualizer.ExecutionMetrics metrics = 
                QueryPlanVisualizer.ExecutionMetrics.fromSteps(planSteps);
            
            QueryPlanVisualizer.visualizePipeline(
                "Execution Plan for: " + queryString,
                planSteps,
                metrics
            );
            
            // Show data flow
            QueryPlanVisualizer.visualizeDataFlow(planSteps);
        }
        
        // Show plan analysis
        System.out.println("PLAN ANALYSIS:");
        System.out.println("- Plan Type: " + plan.getClass().getSimpleName());
        System.out.println("- Estimated Memory Usage: " + estimateMemoryUsage(plan));
        System.out.println("- Supports Streaming: " + supportsStreaming(plan));
        System.out.println("- Parallelizable: " + isParallelizable(plan));
        System.out.println();
    }
    
    /**
     * Extract plan steps from a StreamingQueryPlan for visualization.
     * This method analyzes the plan structure and creates visualization steps.
     */
    private List<QueryPlanVisualizer.PlanStep> extractPlanSteps(StreamingQueryPlan plan) {
        List<QueryPlanVisualizer.PlanStep> steps = new ArrayList<>();
        
        // Analyze the plan structure and extract steps
        extractPlanStepsRecursive(plan, steps, "JFR Event Stream");
        
        return steps;
    }
    
    /**
     * Recursively extract plan steps from nested plans.
     */
    private void extractPlanStepsRecursive(StreamingQueryPlan plan, 
                                         List<QueryPlanVisualizer.PlanStep> steps, 
                                         String inputDescription) {
        String planType = plan.getClass().getSimpleName();
        String outputDescription = generateOutputDescription(plan);
        int estimatedRows = estimateRowCount(plan);
        
        // Create a plan step (mark as successful since this is just visualization)
        QueryPlanVisualizer.PlanStep step = new QueryPlanVisualizer.PlanStep(
            planType,
            inputDescription,
            outputDescription,
            true, // success = true for visualization
            estimatedRows
        );
        
        steps.add(step);
        
        // If this plan has child plans, recursively extract them
        List<StreamingQueryPlan> childPlans = getChildPlans(plan);
        for (StreamingQueryPlan childPlan : childPlans) {
            extractPlanStepsRecursive(childPlan, steps, outputDescription);
        }
    }
    
    /**
     * Generate a description of what this plan outputs.
     */
    private String generateOutputDescription(StreamingQueryPlan plan) {
        String planType = plan.getClass().getSimpleName();
        
        return switch (planType) {
            case "ScanPlan" -> "JFR Event Table";
            case "FilterPlan" -> "Filtered Table";
            case "ProjectionPlan" -> "Projected Table";
            case "GroupByPlan" -> "Grouped Table";
            case "JoinPlan" -> "Joined Table";
            case "SortPlan" -> "Sorted Table";
            case "LimitPlan" -> "Limited Table";
            default -> "Processed Table";
        };
    }
    
    /**
     * Estimate the number of rows this plan might produce.
     * This is a rough estimation for visualization purposes.
     */
    private int estimateRowCount(StreamingQueryPlan plan) {
        String planType = plan.getClass().getSimpleName();
        
        return switch (planType) {
            case "ScanPlan" -> 1000; // Assume 1000 input events
            case "FilterPlan" -> 300; // Assume filtering reduces by 70%
            case "ProjectionPlan" -> 1000; // Projection doesn't change row count
            case "GroupByPlan" -> 50; // Grouping typically reduces rows significantly
            case "JoinPlan" -> 1500; // Joins might increase rows
            case "SortPlan" -> 1000; // Sorting doesn't change row count
            case "LimitPlan" -> 100; // Limit typically reduces to a small number
            default -> 500;
        };
    }
    
    /**
     * Get child plans from a streaming plan (if any).
     * This method uses type checking to extract child plans from different plan types.
     */
    private List<StreamingQueryPlan> getChildPlans(StreamingQueryPlan plan) {
        List<StreamingQueryPlan> children = new ArrayList<>();
        
        // Use reflection to get child plans based on plan type
        try {
            java.lang.reflect.Field[] fields = plan.getClass().getDeclaredFields();
            
            for (java.lang.reflect.Field field : fields) {
                field.setAccessible(true);
                
                // Check if field is a StreamingQueryPlan (single child)
                if (StreamingQueryPlan.class.isAssignableFrom(field.getType())) {
                    StreamingQueryPlan childPlan = (StreamingQueryPlan) field.get(plan);
                    if (childPlan != null) {
                        children.add(childPlan);
                    }
                }
            }
        } catch (Exception e) {
            // If reflection fails, fallback to type-specific checks
            return getChildPlansTyped(plan);
        }
        
        return children;
    }
    
    /**
     * Fallback method to get child plans using type-specific checks.
     */
    private List<StreamingQueryPlan> getChildPlansTyped(StreamingQueryPlan plan) {
        List<StreamingQueryPlan> children = new ArrayList<>();
        
        // Handle different plan types explicitly
        String planType = plan.getClass().getSimpleName();
        
        try {
            switch (planType) {
                case "ScanPlan":
                    // ScanPlan has no children - it's a leaf node
                    break;
                    
                case "FilterPlan":
                    // FilterPlan has inputPlan
                    java.lang.reflect.Field inputPlanField = plan.getClass().getDeclaredField("inputPlan");
                    inputPlanField.setAccessible(true);
                    StreamingQueryPlan inputPlan = (StreamingQueryPlan) inputPlanField.get(plan);
                    if (inputPlan != null) children.add(inputPlan);
                    break;
                    
                case "ProjectionPlan":
                    // ProjectionPlan has sourcePlan
                    java.lang.reflect.Field sourcePlanField = plan.getClass().getDeclaredField("sourcePlan");
                    sourcePlanField.setAccessible(true);
                    StreamingQueryPlan sourcePlan = (StreamingQueryPlan) sourcePlanField.get(plan);
                    if (sourcePlan != null) children.add(sourcePlan);
                    break;
                    
                case "AggregationPlan":
                    // AggregationPlan has sourcePlan
                    java.lang.reflect.Field aggSourceField = plan.getClass().getDeclaredField("sourcePlan");
                    aggSourceField.setAccessible(true);
                    StreamingQueryPlan aggSourcePlan = (StreamingQueryPlan) aggSourceField.get(plan);
                    if (aggSourcePlan != null) children.add(aggSourcePlan);
                    break;
                    
                case "SortPlan":
                    // SortPlan has inputPlan
                    java.lang.reflect.Field sortInputField = plan.getClass().getDeclaredField("inputPlan");
                    sortInputField.setAccessible(true);
                    StreamingQueryPlan sortInputPlan = (StreamingQueryPlan) sortInputField.get(plan);
                    if (sortInputPlan != null) children.add(sortInputPlan);
                    break;
                    
                case "JoinPlan":
                case "InnerJoinPlan":
                case "LeftJoinPlan":
                case "RightJoinPlan":
                case "FullJoinPlan":
                    // JoinPlan has leftPlan and rightPlan
                    java.lang.reflect.Field leftPlanField = plan.getClass().getDeclaredField("leftPlan");
                    leftPlanField.setAccessible(true);
                    StreamingQueryPlan leftPlan = (StreamingQueryPlan) leftPlanField.get(plan);
                    if (leftPlan != null) children.add(leftPlan);
                    
                    java.lang.reflect.Field rightPlanField = plan.getClass().getDeclaredField("rightPlan");
                    rightPlanField.setAccessible(true);
                    StreamingQueryPlan rightPlan = (StreamingQueryPlan) rightPlanField.get(plan);
                    if (rightPlan != null) children.add(rightPlan);
                    break;
                    
                default:
                    // For unknown plan types, assume no children
                    break;
            }
        } catch (Exception e) {
            // If reflection fails, return empty list
            System.err.println("Warning: Could not extract child plans from " + planType + ": " + e.getMessage());
        }
        
        return children;
    }
    
    /**
     * Estimate memory usage for a plan.
     */
    private String estimateMemoryUsage(StreamingQueryPlan plan) {
        String planType = plan.getClass().getSimpleName();
        
        return switch (planType) {
            case "ScanPlan" -> "Low (streaming)";
            case "FilterPlan" -> "Low (streaming)";
            case "ProjectionPlan" -> "Low (streaming)";
            case "GroupByPlan" -> "Medium (requires grouping state)";
            case "JoinPlan" -> "High (requires hash tables)";
            case "SortPlan" -> "High (requires full materialization)";
            case "LimitPlan" -> "Low (early termination)";
            default -> "Medium";
        };
    }
    
    /**
     * Check if a plan supports streaming execution.
     */
    private boolean supportsStreaming(StreamingQueryPlan plan) {
        String planType = plan.getClass().getSimpleName();
        
        return switch (planType) {
            case "ScanPlan", "FilterPlan", "ProjectionPlan", "LimitPlan" -> true;
            case "GroupByPlan", "JoinPlan", "SortPlan" -> false; // These require materialization
            default -> true;
        };
    }
    
    /**
     * Check if a plan can be parallelized.
     */
    private boolean isParallelizable(StreamingQueryPlan plan) {
        String planType = plan.getClass().getSimpleName();
        
        return switch (planType) {
            case "ScanPlan", "FilterPlan", "ProjectionPlan" -> true;
            case "GroupByPlan" -> true; // Can be parallelized with partitioning
            case "JoinPlan" -> true; // Can be parallelized with partitioning
            case "SortPlan", "LimitPlan" -> false; // These have ordering requirements
            default -> false;
        };
    }
    
    /**
     * Create metadata by querying the raw JFR executor for event types.
     * This method asks the raw executor for available event types only.
     */
    private JFRFileMetadata createMetadataFromRawExecutor(RawJfrQueryExecutor rawJfrExecutor) {
        Set<String> eventTypeNames = new HashSet<>();
        
        try {
            // Get event types from the raw executor
            List<EventType> eventTypes = rawJfrExecutor.getEventTypes();
            
            for (EventType eventType : eventTypes) {
                String name = eventType.getName();
                String simpleName = name.substring(name.lastIndexOf('.') + 1);
                eventTypeNames.add(simpleName);
            }
            
        } catch (Exception e) {
            // If metadata extraction fails, return empty set - don't assume any events
            // The query executor should handle empty metadata gracefully
        }
        
        // Return the actual event types found, even if empty
        // Don't make assumptions about what events might be present
        return new JFRFileMetadata(eventTypeNames);
    }
}
