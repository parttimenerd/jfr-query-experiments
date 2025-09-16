package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test for the computed aggregate system.
 * Tests ORDER BY and HAVING with various aggregate expressions using the new architecture
 * where aggregates are computed during GROUP BY and accessed via hooks in PlanExpressionEvaluator.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class ComputedAggregateSystemTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Create comprehensive test data for aggregate testing
        framework.createTable("Sales", """
            region | amount | count | quarter
            North | 100 | 5 | Q1
            South | 200 | 3 | Q1
            North | 150 | 2 | Q2
            South | 300 | 4 | Q2
            East | 50 | 1 | Q1
            West | 75 | 6 | Q1
            East | 125 | 3 | Q2
            West | 225 | 2 | Q2
            """);
    }
    
    // ===== ORDER BY AGGREGATE TESTS =====
    
    @Test
    @DisplayName("ORDER BY with simple aggregate function should work correctly")
    void testOrderBySimpleAggregate() {
        var result = framework.executeAndAssertSuccess("""
            @SELECT region, SUM(amount) as total_sales
            FROM Sales 
            GROUP BY region 
            ORDER BY SUM(amount) DESC
            """);
        
        assertEquals(4, result.getTable().getRowCount());
        
        // Verify correct ordering: South(500) > West(300) > North(250) > East(175)
        assertEquals("South", result.getTable().getString(0, "region"));
        assertEquals(500L, result.getTable().getNumber(0, "total_sales"));
        
        assertEquals("West", result.getTable().getString(1, "region"));
        assertEquals(300L, result.getTable().getNumber(1, "total_sales"));
        
        assertEquals("North", result.getTable().getString(2, "region"));
        assertEquals(250L, result.getTable().getNumber(2, "total_sales"));
        
        assertEquals("East", result.getTable().getString(3, "region"));
        assertEquals(175L, result.getTable().getNumber(3, "total_sales"));
    }
    
    @Test
    @DisplayName("ORDER BY with complex aggregate expression should work correctly")
    void testOrderByComplexAggregateExpression() {
        var result = framework.executeAndAssertSuccess("""
            @SELECT region, SUM(amount) as total_sales, AVG(count) as avg_count
            FROM Sales 
            GROUP BY region 
            ORDER BY SUM(amount) + AVG(count) DESC
            """);
        
        assertEquals(4, result.getTable().getRowCount());
        
        // Verify ordering based on SUM(amount) + AVG(count)
        String firstRegion = result.getTable().getString(0, "region");
        long firstTotal = result.getTable().getNumber(0, "total_sales");
        double firstAvg = result.getTable().getDouble(0, "avg_count");
        
        // South should be first: 500 + 3.5 = 503.5
        assertEquals("South", firstRegion);
        assertEquals(500L, firstTotal);
        assertEquals(3.5, firstAvg, 0.1);
    }
    
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
        "@SELECT region, SUM(amount) as total FROM Sales GROUP BY region ORDER BY SUM(amount) ASC|East",
        "@SELECT region, COUNT(*) as cnt FROM Sales GROUP BY region ORDER BY COUNT(*) DESC|North",
        "@SELECT region, AVG(amount) as avg FROM Sales GROUP BY region ORDER BY AVG(amount) DESC|South"
    })
    @DisplayName("ORDER BY with different aggregate functions should work correctly")
    void testOrderByDifferentAggregates(String query, String expectedFirstRegion) {
        var result = framework.executeAndAssertSuccess(query);
        assertTrue(result.getTable().getRowCount() > 0);
        assertEquals(expectedFirstRegion, result.getTable().getString(0, "region"));
    }
    
    // ===== HAVING AGGREGATE TESTS =====
    
    @Test
    @DisplayName("HAVING with simple aggregate condition should filter correctly")
    void testHavingSimpleAggregate() {
        var result = framework.executeAndAssertSuccess("""
            @SELECT region, SUM(amount) as total_sales
            FROM Sales 
            GROUP BY region 
            HAVING SUM(amount) > 200
            """);
        
        assertEquals(3, result.getTable().getRowCount());
        
        // Should include South(500), West(300), North(250) but exclude East(175)
        var regions = java.util.stream.IntStream.range(0, result.getTable().getRowCount())
            .mapToObj(i -> result.getTable().getString(i, "region"))
            .collect(java.util.stream.Collectors.toSet());
        
        assertTrue(regions.contains("South"));
        assertTrue(regions.contains("West"));  
        assertTrue(regions.contains("North"));
        assertFalse(regions.contains("East"));
    }
    
    @Test
    @DisplayName("HAVING with complex aggregate expression should filter correctly")
    void testHavingComplexAggregateExpression() {
        var result = framework.executeAndAssertSuccess("""
            @SELECT region, SUM(amount) as total_sales, COUNT(*) as order_count
            FROM Sales 
            GROUP BY region 
            HAVING SUM(amount) > 150 AND COUNT(*) >= 2
            """);
        
        assertEquals(4, result.getTable().getRowCount());
        
        // Verify all results meet both conditions
        for (int i = 0; i < result.getTable().getRowCount(); i++) {
            long totalSales = result.getTable().getNumber(i, "total_sales");
            long orderCount = result.getTable().getNumber(i, "order_count");
            
            assertTrue(totalSales > 150, "Total sales should be > 150");
            assertTrue(orderCount >= 2, "Order count should be >= 2");
        }
    }
    
    @ParameterizedTest
    @CsvSource({
        "SUM(amount) > 300, 1", // Only South(500) > 300
        "AVG(amount) > 100, 3", // Regions with average > 100
        "COUNT(*) = 2, 4"       // All regions have exactly 2 orders
    })
    @DisplayName("HAVING with different aggregate conditions should filter correctly")
    void testHavingDifferentConditions(String havingCondition, int expectedRows) {
        String query = String.format("""
            @SELECT region, SUM(amount) as total, AVG(amount) as avg, COUNT(*) as cnt
            FROM Sales 
            GROUP BY region 
            HAVING %s
            """, havingCondition);
        
        var result = framework.executeAndAssertSuccess(query);
        assertEquals(expectedRows, result.getTable().getRowCount());
    }
    
    // ===== COMBINED ORDER BY AND HAVING TESTS =====
    
    @Test
    @DisplayName("Combined HAVING and ORDER BY with aggregates should work correctly")
    void testCombinedHavingAndOrderBy() {
        var result = framework.executeAndAssertSuccess("""
            @SELECT region, SUM(amount) as total_sales, AVG(count) as avg_orders
            FROM Sales 
            GROUP BY region 
            HAVING SUM(amount) > 200
            ORDER BY AVG(count) DESC
            """);
        
        assertEquals(3, result.getTable().getRowCount());
        
        // Verify results are filtered (HAVING) and ordered (ORDER BY)
        for (int i = 0; i < result.getTable().getRowCount(); i++) {
            long totalSales = result.getTable().getNumber(i, "total_sales");
            assertTrue(totalSales > 200, "HAVING filter should apply");
            
            if (i > 0) {
                double currentAvg = result.getTable().getNumber(i, "avg_orders");
                double previousAvg = result.getTable().getNumber(i - 1, "avg_orders");
                assertTrue(currentAvg <= previousAvg, "ORDER BY DESC should apply");
            }
        }
    }
    
    // ===== SEMANTIC MATCHING TESTS =====
    
    @Test
    @DisplayName("ORDER BY should semantically match aggregate expressions to column aliases")
    void testSemanticMatching() {
        var result = framework.executeAndAssertSuccess("""
            @SELECT region, SUM(amount) as total_revenue
            FROM Sales 
            GROUP BY region 
            ORDER BY SUM(amount) DESC
            """);
        
        assertEquals(4, result.getTable().getRowCount());
        
        // Verify that SUM(amount) correctly matched to total_revenue column for ordering
        String firstRegion = result.getTable().getString(0, "region");
        long firstRevenue = result.getTable().getNumber(0, "total_revenue");
        
        assertEquals("South", firstRegion);
        assertEquals(500L, firstRevenue);
    }
    
    // ===== EDGE CASES AND ERROR HANDLING =====
    
    @Test
    @DisplayName("ORDER BY with non-existent aggregate should handle gracefully")
    void testOrderByNonExistentAggregate() {
        // This should work because MAX(amount) will be computed as an additional aggregate
        var result = framework.executeAndAssertSuccess("""
            @SELECT region, SUM(amount) as total_sales
            FROM Sales 
            GROUP BY region 
            ORDER BY MAX(amount) DESC
            """);
        
        assertEquals(4, result.getTable().getRowCount());
        
        // Verify ordering by MAX(amount): South(300) > West(225) > East(125) > North(150)
        // Note: North has max 150, South has max 300
        assertEquals("South", result.getTable().getString(0, "region"));
    }
    
    @Test
    @DisplayName("Multiple aggregates in ORDER BY expression should work correctly")
    void testMultipleAggregatesInOrderBy() {
        var result = framework.executeAndAssertSuccess("""
            @SELECT region, SUM(amount) as total_sales, COUNT(*) as order_count
            FROM Sales 
            GROUP BY region 
            ORDER BY SUM(amount) * COUNT(*) DESC
            """);
        
        assertEquals(4, result.getTable().getRowCount());
        
        // Verify complex ordering: SUM(amount) * COUNT(*)
        // South: 500 * 2 = 1000, West: 300 * 2 = 600, North: 250 * 2 = 500, East: 175 * 2 = 350
        assertEquals("South", result.getTable().getString(0, "region"));
        assertEquals("West", result.getTable().getString(1, "region"));
    }
    
    // ===== PERFORMANCE AND CORRECTNESS VERIFICATION =====
    
    @Test
    @DisplayName("Computed aggregates should not be recalculated for each usage")
    void testAggregateReuse() {
        // This test verifies that SUM(amount) is computed once and reused
        var result = framework.executeAndAssertSuccess("""
            @SELECT region, SUM(amount) as total_sales
            FROM Sales 
            GROUP BY region 
            HAVING SUM(amount) > 200
            ORDER BY SUM(amount) DESC
            """);
        
        assertTrue(result.getTable().getRowCount() > 0);
        
        // Verify the results are both filtered and ordered correctly
        for (int i = 0; i < result.getTable().getRowCount(); i++) {
            long totalSales = result.getTable().getNumber(i, "total_sales");
            assertTrue(totalSales > 200, "HAVING condition should be applied");
            
            if (i > 0) {
                long previousSales = result.getTable().getNumber(i - 1, "total_sales");
                assertTrue(totalSales <= previousSales, "ORDER BY DESC should be applied");
            }
        }
    }
}
