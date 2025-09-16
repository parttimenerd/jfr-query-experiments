package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.engine.framework.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify that QueryTestFramework works correctly with QueryPlanExecutor.
 * This validates the migration from QueryEvaluator to QueryPlanExecutor.
 * 
 * @author Migration Test
 */
public class QueryTestFrameworkMigrationTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("QueryTestFramework should use QueryPlanExecutor successfully")
    void testFrameworkUsesQueryPlanExecutor() {
        // Create a simple test table
        framework.mockTable("Users")
            .withStringColumn("name")
            .withNumberColumn("age")
            .withRow("Alice", 25L)
            .withRow("Bob", 30L)
            .build();
        
        // Execute a simple query
        QueryResult result = framework.executeQuery("@SELECT name FROM Users");
        
        // Verify the result
        assertTrue(result.isSuccess(), "Query should execute successfully");
        assertNotNull(result.getTable(), "Result table should not be null");
        assertEquals(2, result.getTable().getRowCount(), "Should have 2 rows");
        assertEquals("Alice", result.getTable().getString(0, "name"), "First row should be Alice");
        assertEquals("Bob", result.getTable().getString(1, "name"), "Second row should be Bob");
    }
    
    @Test
    @DisplayName("QueryTestFramework should handle query parsing correctly")
    void testQueryParsing() {
        // Test that parsing still works
        assertDoesNotThrow(() -> {
            var queryNode = framework.parseQuery("@SELECT * FROM Users");
            assertNotNull(queryNode, "Parsed query should not be null");
        }, "Query parsing should work without errors");
    }
    
    @Test
    @DisplayName("QueryTestFramework should handle query execution errors gracefully")
    void testErrorHandling() {
        // Test error handling with invalid query
        QueryResult result = framework.executeQuery("@SELECT * FROM NonExistentTable");
        
        // Should return a failed result, not throw an exception
        assertFalse(result.isSuccess(), "Query with non-existent table should fail");
        assertNotNull(result.getError(), "Error should be provided");
        assertNull(result.getTable(), "Table should be null for failed query");
    }
}
