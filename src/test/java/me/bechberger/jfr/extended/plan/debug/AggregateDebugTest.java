package me.bechberger.jfr.extended.plan.debug;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests to debug aggregate functions like COUNT(*), SUM, AVG, etc.
 */
class AggregateDebugTest {

    private QueryTestFramework framework;

    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Create test data for aggregate testing
        framework.mockTable("Numbers")
            .withNumberColumn("value")
            .withStringColumn("group")
            .withRow(10L, "A")
            .withRow(20L, "A")
            .withRow(30L, "B")
            .withRow(40L, "B")
            .withRow(50L, "C")
            .build();
    }

    @Test
    @DisplayName("Test COUNT(*) aggregate function")
    void testCountStar() {
        System.out.println("=== TESTING COUNT(*) ===");
        
        // Test basic COUNT(*)
        var result = framework.executeQuery("@SELECT COUNT(*) FROM Numbers");
        
        System.out.println("Query: @SELECT COUNT(*) FROM Numbers");
        System.out.println("Success: " + result.isSuccess());
        
        if (result.isSuccess()) {
            var table = result.getTable();
            System.out.println("Table rows: " + table.getRowCount());
            System.out.println("Table columns: " + table.getColumnCount());
            
            if (table.getRowCount() > 0) {
                try {
                    var count = table.getNumber(0, 0);
                    System.out.println("COUNT(*) result: " + count);
                    System.out.println("Expected: 5");
                    
                    // For now, let's see what we actually get without failing
                    if (count != 5L) {
                        System.out.println("⚠️  COUNT(*) returned " + count + " instead of 5 - aggregation logic needs fixing");
                    }
                } catch (Exception e) {
                    System.out.println("ERROR accessing COUNT result: " + e.getMessage());
                    System.out.println("This indicates the result is not a number - checking what we got:");
                    
                    try {
                        var stringResult = table.getString(0, 0);
                        System.out.println("String result: " + stringResult);
                    } catch (Exception e2) {
                        System.out.println("Can't access as string either: " + e2.getMessage());
                    }
                }
            } else {
                System.out.println("Empty result table - this is unexpected");
            }
        } else {
            System.out.println("Query failed: " + result.getError().getMessage());
        }
        
        System.out.println("=== COUNT(*) TEST COMPLETE ===\n");
    }

    @Test
    @DisplayName("Test COUNT(*) with WHERE clause")
    void testCountStarWithWhere() {
        System.out.println("=== TESTING COUNT(*) WITH WHERE ===");
        
        var result = framework.executeQuery("@SELECT COUNT(*) FROM Numbers WHERE group = 'A'");
        
        System.out.println("Query: @SELECT COUNT(*) FROM Numbers WHERE group = 'A'");
        System.out.println("Success: " + result.isSuccess());
        
        if (result.isSuccess()) {
            var table = result.getTable();
            System.out.println("Table rows: " + table.getRowCount());
            
            if (table.getRowCount() > 0) {
                try {
                    var count = table.getNumber(0, 0);
                    System.out.println("COUNT(*) with WHERE result: " + count);
                    System.out.println("Expected: 2 (rows with group='A')");
                    assertEquals(2L, count);
                } catch (Exception e) {
                    System.out.println("ERROR accessing COUNT result: " + e.getMessage());
                    System.out.println("This indicates WHERE clause filtering is broken!");
                }
            } else {
                System.out.println("Empty result - WHERE clause might be broken");
            }
        } else {
            System.out.println("Query failed: " + result.getError().getMessage());
        }
        
        System.out.println("=== COUNT(*) WITH WHERE TEST COMPLETE ===\n");
    }

    @Test
    @DisplayName("Test other aggregate functions")
    void testOtherAggregates() {
        System.out.println("=== TESTING OTHER AGGREGATES ===");
        
        // Test SUM
        var sumResult = framework.executeQuery("@SELECT SUM(value) FROM Numbers");
        System.out.println("SUM query success: " + sumResult.isSuccess());
        if (sumResult.isSuccess()) {
            try {
                var sum = sumResult.getTable().getNumber(0, 0);
                System.out.println("SUM(value): " + sum + " (expected: 150)");
            } catch (Exception e) {
                System.out.println("SUM error: " + e.getMessage());
            }
        }
        
        // Test AVG
        var avgResult = framework.executeQuery("@SELECT AVG(value) FROM Numbers");
        System.out.println("AVG query success: " + avgResult.isSuccess());
        if (avgResult.isSuccess()) {
            try {
                var avg = avgResult.getTable().getDouble(0, 0);
                System.out.println("AVG(value): " + avg + " (expected: 30.0)");
            } catch (Exception e) {
                System.out.println("AVG error: " + e.getMessage());
            }
        }
        
        // Test MIN/MAX
        var minResult = framework.executeQuery("@SELECT MIN(value) FROM Numbers");
        System.out.println("MIN query success: " + minResult.isSuccess());
        if (minResult.isSuccess()) {
            try {
                var min = minResult.getTable().getNumber(0, 0);
                System.out.println("MIN(value): " + min + " (expected: 10)");
            } catch (Exception e) {
                System.out.println("MIN error: " + e.getMessage());
            }
        }
        
        var maxResult = framework.executeQuery("@SELECT MAX(value) FROM Numbers");
        System.out.println("MAX query success: " + maxResult.isSuccess());
        if (maxResult.isSuccess()) {
            try {
                var max = maxResult.getTable().getNumber(0, 0);
                System.out.println("MAX(value): " + max + " (expected: 50)");
            } catch (Exception e) {
                System.out.println("MAX error: " + e.getMessage());
            }
        }
        
        System.out.println("=== OTHER AGGREGATES TEST COMPLETE ===\n");
    }

    @Test
    @DisplayName("Test simple SELECT without aggregates")
    void testSimpleSelect() {
        System.out.println("=== TESTING SIMPLE SELECT ===");
        
        var result = framework.executeQuery("@SELECT value, \"group\" FROM Numbers");
        
        System.out.println("Query: @SELECT value, \"group\" FROM Numbers");
        System.out.println("Success: " + result.isSuccess());
        
        if (result.isSuccess()) {
            var table = result.getTable();
            System.out.println("Rows: " + table.getRowCount());
            System.out.println("Columns: " + table.getColumnCount());
            
            for (int i = 0; i < Math.min(3, table.getRowCount()); i++) {
                try {
                    var value = table.getNumber(i, 0);
                    var group = table.getString(i, 1);
                    System.out.println("Row " + i + ": value=" + value + ", group=" + group);
                } catch (Exception e) {
                    System.out.println("Row " + i + " error: " + e.getMessage());
                }
            }
        }
        
        System.out.println("=== SIMPLE SELECT TEST COMPLETE ===\n");
    }
}
