package me.bechberger.jfr.extended.examples;

import me.bechberger.jfr.extended.engine.QueryEvaluator;
import me.bechberger.jfr.extended.engine.RawJfrQueryExecutorImpl;
import me.bechberger.jfr.extended.ast.ASTNodes;
import me.bechberger.jfr.extended.ast.Location;
import me.bechberger.jfr.extended.table.JfrTable;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Example demonstrating the new QueryEvaluator architecture with caching RawJfrQueryExecutor.
 * 
 * Key improvements:
 * - RawJfrQueryExecutorImpl caches all JFR events using RecordingFile for efficient re-execution
 * - QueryEvaluator operates standalone (QueryEngine is optional) and uses FunctionRegistry.getInstance()
 * - Proper function evaluation with context stacking and variable scoping for nested queries
 * - Type parsing from tabular output with correct CellType detection (similar to main.js)
 * - Support for percentile functions and aggregate functions through FunctionRegistry
 */
public class QueryEvaluatorExample {
    
    public static void main(String[] args) throws Exception {
        // Path to a JFR file
        Path jfrFile = Paths.get("sample.jfr");
        
        // Create the caching raw JFR query executor
        RawJfrQueryExecutorImpl rawExecutor = new RawJfrQueryExecutorImpl(jfrFile);
        
        // Create the QueryEvaluator with the caching executor (QueryEngine not needed)
        QueryEvaluator evaluator = new QueryEvaluator(rawExecutor);
        
        // Example 1: Execute a basic JFR query
        System.out.println("=== Basic JFR Query ===");
        try {
            JfrTable result1 = evaluator.query("SELECT eventType, COUNT(*) FROM @SELECT * FROM GarbageCollection GROUP BY eventType");
            System.out.println("Result: " + result1.getRowCount() + " rows");
            
            // Example 2: Execute a percentile selection query (uses function registry)
            System.out.println("\n=== Percentile Selection Query ===");
            JfrTable result2 = evaluator.query("P99SELECT(GarbageCollection, gcId, duration)");
            System.out.println("P99 Result: " + result2.getRowCount() + " rows");
            
            // Example 3: Use context variables and stacking
            System.out.println("\n=== Variable Assignment and Context ===");
            JfrTable result3 = evaluator.query("$gcEvents = @SELECT * FROM GarbageCollection; SELECT AVG(duration) FROM $gcEvents");
            System.out.println("Average duration result: " + result3.getRowCount() + " rows");
            
            // Example 4: Multiple query execution - should use cached events
            System.out.println("\n=== Re-executing query (should use cache) ===");
            JfrTable result4 = evaluator.query("@SELECT TOP 10 * FROM GarbageCollection ORDER BY duration DESC");
            System.out.println("Top 10 GC events: " + result4.getRowCount() + " rows");
            
        } catch (Exception e) {
            System.err.println("Query execution failed: " + e.getMessage());
            e.printStackTrace();
        }
        
        // Example 5: Raw JFR query execution 
        System.out.println("\n=== Direct Raw JFR Query ===");
        try {
            ASTNodes.RawJfrQueryNode rawQuery = new ASTNodes.RawJfrQueryNode(
                "SELECT * FROM GarbageCollection LIMIT 5", 
                new Location(1, 1));
            JfrTable rawResult = evaluator.jfrQuery(rawQuery);
            System.out.println("Raw query result: " + rawResult.getRowCount() + " rows");
            
            // Print column information
            if (!rawResult.getColumns().isEmpty()) {
                System.out.println("Columns: " + 
                    rawResult.getColumns().stream()
                        .map(col -> col.name() + "(" + col.type() + ")")
                        .reduce((a, b) -> a + ", " + b)
                        .orElse("none"));
            }
            
        } catch (Exception e) {
            System.err.println("Raw query execution failed: " + e.getMessage());
        }
        
        // Example 6: Show available event types (cached)
        System.out.println("\n=== Available Event Types ===");
        try {
            var eventTypes = rawExecutor.getEventTypes();
            System.out.println("Found " + eventTypes.size() + " event types:");
            eventTypes.stream()
                .limit(5)
                .forEach(et -> System.out.println("  - " + et.getName()));
            if (eventTypes.size() > 5) {
                System.out.println("  ... and " + (eventTypes.size() - 5) + " more");
            }
        } catch (Exception e) {
            System.err.println("Failed to get event types: " + e.getMessage());
        }
    }
}
