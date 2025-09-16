package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Debug ORDER BY with aggregates to understand the matching logic
 */
public class OrderByAggregateDebugTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testSimpleOrderByAggregate() {
        framework.createTable("Sales", """
            region | amount
            North | 1000
            North | 1500
            South | 800
            East | 1200
            """);
        
        System.out.println("=== Testing ORDER BY SUM(amount) with exact aggregate name ===");
        
        // Test with column named exactly as aggregate function
        var result = framework.executeAndAssertSuccess("""
            @SELECT 
                region,
                SUM(amount) as SUM_amount
            FROM Sales 
            GROUP BY region 
            ORDER BY SUM(amount) DESC
            """);
        
        var table = result.getTable();
        for (int i = 0; i < table.getRowCount(); i++) {
            System.out.printf("Row %d: region=%s, SUM_amount=%s%n", 
                i, 
                table.getCell(i, "region"),
                table.getCell(i, "SUM_amount"));
        }
    }
    
    @Test  
    void testOrderByWithFunctionName() {
        framework.createTable("Sales", """
            region | amount
            North | 1000
            North | 1500
            South | 800
            East | 1200
            """);
        
        System.out.println("=== Testing ORDER BY SUM(amount) with function name as alias ===");
        
        // Test with column named as function name
        var result = framework.executeAndAssertSuccess("""
            @SELECT 
                region,
                SUM(amount) as SUM
            FROM Sales 
            GROUP BY region 
            ORDER BY SUM(amount) DESC
            """);
        
        var table = result.getTable();
        for (int i = 0; i < table.getRowCount(); i++) {
            System.out.printf("Row %d: region=%s, SUM=%s%n", 
                i, 
                table.getCell(i, "region"),
                table.getCell(i, "SUM"));
        }
    }
    
    public static void main(String[] args) {
        OrderByAggregateDebugTest test = new OrderByAggregateDebugTest();
        test.setUp();
        
        try {
            System.out.println("Running ORDER BY aggregate tests...\n");
            test.testSimpleOrderByAggregate();
            System.out.println();
            test.testOrderByWithFunctionName();
            
            // Now test the semantic matching case that was failing
            System.out.println("\n=== Testing ORDER BY SUM(amount) with semantic alias ===");
            test.framework.createTable("Sales", """
                region | amount
                North | 1000
                North | 1500
                South | 800
                East | 1200
                """);
            
            var result = test.framework.executeAndAssertSuccess("""
                @SELECT 
                    region,
                    SUM(amount) as total_sales
                FROM Sales 
                GROUP BY region 
                ORDER BY SUM(amount) DESC
                """);
            
            var table = result.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.printf("Row %d: region=%s, total_sales=%s%n", 
                    i, 
                    table.getCell(i, "region"),
                    table.getCell(i, "total_sales"));
            }
            
            System.out.println("\nAll tests completed successfully!");
            
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
