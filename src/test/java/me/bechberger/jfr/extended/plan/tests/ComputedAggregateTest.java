package me.bechberger.jfr.extended.plan.tests;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.engine.framework.QueryResult;

/**
 * Comprehensive test for the new computed aggregate system.
 * Tests ORDER BY and HAVING with various aggregate expressions.
 */
public class ComputedAggregateTest {
    public static void main(String[] args) {
        try {
            QueryTestFramework framework = new QueryTestFramework();
            
            // Create test data
            framework.createTable("Events", """
                category | value | count
                A | 100 | 5
                B | 200 | 3
                A | 150 | 2
                B | 300 | 4
                C | 50 | 1
                """);
            
            System.out.println("=== Testing Computed Aggregate System ===\n");
            
            // Test 1: ORDER BY with aggregate function
            testOrderByAggregate(framework);
            
            // Test 2: HAVING with aggregate function
            testHavingAggregate(framework);
            
            // Test 3: Complex expressions with aggregates
            testComplexAggregateExpressions(framework);
            
        } catch (Exception e) {
            System.out.println("❌ EXCEPTION: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void testOrderByAggregate(QueryTestFramework framework) {
        System.out.println("--- Test 1: ORDER BY with Aggregate ---");
        
        String query = """
            @SELECT category, SUM(value) as total_value
            FROM Events 
            GROUP BY category 
            ORDER BY SUM(value) DESC
            """;
        
        System.out.println("Query: " + query.replace("\n", " "));
        QueryResult result = framework.executeQuery(query);
        
        if (result.isSuccess()) {
            System.out.println("✅ SUCCESS!");
            System.out.println("Results:");
            for (int i = 0; i < result.getTable().getRowCount(); i++) {
                String category = result.getTable().getString(i, "category");
                long total = result.getTable().getNumber(i, "total_value");
                System.out.printf("  %s: %d%n", category, total);
            }
        } else {
            System.out.println("❌ FAILED: " + result.getError().getMessage());
        }
        System.out.println();
    }
    
    private static void testHavingAggregate(QueryTestFramework framework) {
        System.out.println("--- Test 2: HAVING with Aggregate ---");
        
        String query = """
            @SELECT category, SUM(value) as total_value
            FROM Events 
            GROUP BY category 
            HAVING SUM(value) > 200
            """;
        
        System.out.println("Query: " + query.replace("\n", " "));
        QueryResult result = framework.executeQuery(query);
        
        if (result.isSuccess()) {
            System.out.println("✅ SUCCESS!");
            System.out.println("Results (categories with SUM(value) > 200):");
            for (int i = 0; i < result.getTable().getRowCount(); i++) {
                String category = result.getTable().getString(i, "category");
                long total = result.getTable().getNumber(i, "total_value");
                System.out.printf("  %s: %d%n", category, total);
            }
        } else {
            System.out.println("❌ FAILED: " + result.getError().getMessage());
        }
        System.out.println();
    }
    
    private static void testComplexAggregateExpressions(QueryTestFramework framework) {
        System.out.println("--- Test 3: Complex Aggregate Expressions ---");
        
        String query = """
            @SELECT category, SUM(value) as total_value, AVG(count) as avg_count
            FROM Events 
            GROUP BY category 
            ORDER BY SUM(value) + AVG(count) DESC
            """;
        
        System.out.println("Query: " + query.replace("\n", " "));
        QueryResult result = framework.executeQuery(query);
        
        if (result.isSuccess()) {
            System.out.println("✅ SUCCESS!");
            System.out.println("Results (ordered by SUM(value) + AVG(count)):");
            for (int i = 0; i < result.getTable().getRowCount(); i++) {
                String category = result.getTable().getString(i, "category");
                long total = result.getTable().getNumber(i, "total_value");
                double avg = result.getTable().getNumber(i, "avg_count");
                System.out.printf("  %s: total=%d, avg=%.1f, sum=%.1f%n", 
                    category, total, avg, total + avg);
            }
        } else {
            System.out.println("❌ FAILED: " + result.getError().getMessage());
        }
        System.out.println();
    }
}
