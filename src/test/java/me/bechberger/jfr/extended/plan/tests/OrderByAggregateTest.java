package me.bechberger.jfr.extended.plan.tests;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.engine.framework.QueryResult;

/**
 * Test to verify that ORDER BY with aggregate expressions works correctly
 * using the new computed aggregate system.
 */
public class OrderByAggregateTest {
    public static void main(String[] args) {
        try {
            QueryTestFramework framework = new QueryTestFramework();
            
            // Create test data
            framework.createTable("Sales", """
                region | amount
                North | 100
                South | 200
                North | 150
                South | 300
                """);
            
            // Test ORDER BY with SUM aggregate
            String query = """
                @SELECT region, SUM(amount) as total_sales 
                FROM Sales 
                GROUP BY region 
                ORDER BY SUM(amount) DESC
                """;
            
            System.out.println("Testing ORDER BY with aggregate: " + query);
            QueryResult result = framework.executeQuery(query);
            
            if (result.isSuccess()) {
                System.out.println("SUCCESS! Query executed successfully.");
                System.out.println("Result table:");
                System.out.println(result.getTable().toString());
                
                // Verify the results are correctly ordered (South=500, North=250)
                if (result.getTable().getRowCount() == 2) {
                    String firstRegion = result.getTable().getString(0, "region");
                    long firstTotal = result.getTable().getNumber(0, "total_sales");
                    String secondRegion = result.getTable().getString(1, "region");
                    long secondTotal = result.getTable().getNumber(1, "total_sales");
                    
                    System.out.printf("Row 1: %s = %d%n", firstRegion, firstTotal);
                    System.out.printf("Row 2: %s = %d%n", secondRegion, secondTotal);
                    
                    // Check that results are ordered by SUM(amount) DESC
                    if (firstTotal >= secondTotal) {
                        System.out.println("✅ ORDER BY working correctly - results are properly sorted!");
                    } else {
                        System.out.println("❌ ORDER BY not working - results are not sorted correctly");
                    }
                } else {
                    System.out.println("❌ Unexpected number of rows: " + result.getTable().getRowCount());
                }
            } else {
                System.out.println("❌ FAILED: " + result.getError().getMessage());
                result.getError().printStackTrace();
            }
            
        } catch (Exception e) {
            System.out.println("❌ EXCEPTION: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
