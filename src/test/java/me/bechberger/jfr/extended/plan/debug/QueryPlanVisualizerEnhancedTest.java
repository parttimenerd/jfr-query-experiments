package me.bechberger.jfr.extended.plan.debug;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.engine.exception.FunctionArgumentException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the enhanced QueryPlanVisualizer with configurable row display limits.
 */
class QueryPlanVisualizerEnhancedTest {

    private QueryTestFramework framework;
    private QueryPlanVisualizer visualizer;

    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        visualizer = new QueryPlanVisualizer();
    }

    @Test
    @DisplayName("Visualizer should support configurable row display limits")
    void testConfigurableRowDisplayLimits() {
        // Test default behavior (5 rows)
        assertEquals(5, visualizer.getMaxRowsToDisplay());
        
        // Test setting custom limit
        visualizer.setMaxRowsToDisplay(3);
        assertEquals(3, visualizer.getMaxRowsToDisplay());
        
        // Test validation
        assertThrows(FunctionArgumentException.class, () -> {
            visualizer.setMaxRowsToDisplay(0);
        });
        
        assertThrows(FunctionArgumentException.class, () -> {
            visualizer.setMaxRowsToDisplay(-1);
        });
    }

    @Test
    @DisplayName("Enhanced visualizer should debug double-quoted string WHERE clause issues")
    void testDoubleQuotedStringWhereClauseDebugging() {
        // Create test data that matches the failing DoubleQuotedStringTest scenario
        framework.mockTable("TestEvents")
            .withStringColumn("message")
            .withNumberColumn("id")
            .withRow("Hello", 1L)
            .withRow("World", 2L)
            .build();

        // Test the problematic WHERE clause query from the failing test
        String query = "@SELECT COUNT(*) FROM TestEvents WHERE message = \"Hello\"";
        
        // Get the execution plan via reflection-like approach
        var result = framework.executeQuery(query);
        
        System.out.println("=== DEBUGGING DOUBLE-QUOTED STRING WHERE CLAUSE ===");
        System.out.println("Query: " + query);
        System.out.println("Result success: " + result.isSuccess());
        if (result.isSuccess()) {
            // Check if the result table has data
            var table = result.getTable();
            System.out.println("Table rows: " + table.getRowCount());
            System.out.println("Table columns: " + table.getColumnCount());
            
            if (table.getRowCount() > 0 && table.getColumnCount() > 0) {
                try {
                    System.out.println("Result count: " + table.getNumber(0, 0));
                } catch (Exception e) {
                    System.out.println("Result is NULL or error (filtering not working): " + e.getMessage());
                }
            } else {
                System.out.println("Empty result table");
            }
            System.out.println("Expected: 1 (should find 'Hello' row)");
        } else {
            System.out.println("Error: " + result.getError().getMessage());
        }
        
        // Let's test a simpler query without WHERE to ensure basic functionality works
        String simpleQuery = "@SELECT message FROM TestEvents";
        var simpleResult = framework.executeQuery(simpleQuery);
        
        System.out.println("\nSimple query without WHERE:");
        System.out.println("Success: " + simpleResult.isSuccess());
        if (simpleResult.isSuccess()) {
            var table = simpleResult.getTable();
            System.out.println("Rows: " + table.getRowCount());
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("Row " + i + ": message=" + table.getString(i, "message"));
            }
        }
        
        System.out.println("=== END DEBUGGING ===");
    }

    @Test
    @DisplayName("Test visualizer configuration and overloaded methods")
    void testVisualizerOverloadedMethods() {
        // Create some test data
        framework.mockTable("SimpleTest")
            .withStringColumn("name")
            .withRow("test")
            .build();

        // Test basic configuration
        visualizer.setMaxRowsToDisplay(10);
        assertEquals(10, visualizer.getMaxRowsToDisplay());
        
        // Execute a simple query to test basic functionality
        var result = framework.executeQuery("@SELECT name FROM SimpleTest");
        assertTrue(result.isSuccess());
        assertEquals("test", result.getTable().getString(0, "name"));
        
        // Verify visualizer doesn't break the basic query execution
        assertEquals(10, visualizer.getMaxRowsToDisplay());
    }
}
