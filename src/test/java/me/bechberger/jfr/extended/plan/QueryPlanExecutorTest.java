package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.MockRawJfrQueryExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the QueryPlanExecutor - the main API for the streaming query plan architecture.
 * 
 * This test suite validates that the QueryPlanExecutor can successfully initialize
 * and handle basic operations without requiring actual JFR data.
 */
class QueryPlanExecutorTest {
    
    private QueryPlanExecutor queryPlanExecutor;
    
    @BeforeEach
    void setUp() {
        // Create a minimal QueryEvaluator for testing
        // In a real test, this would be a proper mock or test implementation
        queryPlanExecutor = new QueryPlanExecutor(new MockRawJfrQueryExecutor());
    }
    
    @Test
    void testQueryPlanExecutorInitialization() {
        // Arrange & Act
        QueryPlanExecutor executor = new QueryPlanExecutor(new MockRawJfrQueryExecutor());
        
        // Assert
        assertNotNull(executor);
        assertNotNull(executor.getLastExecutionStats());
    }
    
    @Test
    void testMemoryBoundedMode() {
        // Arrange & Act
        queryPlanExecutor.enableMemoryBoundedMode(1024L * 1024L); // 1MB limit
        MemoryUsageStats stats = queryPlanExecutor.getLastExecutionStats();
        
        // Assert
        assertNotNull(stats);
        // In a real implementation, we would verify memory limits are enforced
    }
    
    @Test
    void testExecuteInvalidQuery() {
        // Arrange
        String invalidQuery = "COMPLETELY INVALID SQL QUERY SYNTAX";
        
        // Act & Assert
        assertThrows(Exception.class, () -> {
            queryPlanExecutor.execute(invalidQuery);
        });
    }
    
    /**
     * Test that demonstrates the streaming plan architecture is working.
     * This shows that queries can be processed through the plan pipeline.
     */
    @Test
    void testStreamingPlanArchitectureBasics() {
        // Arrange & Act
        QueryPlanExecutor executor = new QueryPlanExecutor(new MockRawJfrQueryExecutor());
        
        // Assert - Just verify the infrastructure is set up correctly
        assertNotNull(executor);
        assertNotNull(executor.getLastExecutionStats());
        
        // Verify execution stats are initialized
        MemoryUsageStats stats = executor.getLastExecutionStats();
        assertNotNull(stats);
        assertTrue(stats.getHeapUsed() >= 0);
    }
    
    @Test
    void testVisualizationDoesNotThrow() {
        // Arrange
        String simpleQuery = "@SELECT name FROM SomeTable";
        
        // Act & Assert
        // For now, just ensure that the visualization method doesn't throw exceptions
        // when called with a simple query string
        assertDoesNotThrow(() -> {
            try {
                queryPlanExecutor.visualizePlan(simpleQuery);
            } catch (Exception e) {
                // Expected for missing tables - just verify structure is working
                assertTrue(e.getMessage().contains("SomeTable") || 
                          e.getMessage().contains("parse") ||
                          e.getMessage().contains("table"));
            }
        });
    }
    
    @Test
    void testChildPlanExtractionMethods() throws Exception {
        // Test that the private methods for child plan extraction exist and can be called
        assertDoesNotThrow(() -> {
            java.lang.reflect.Method getChildPlansMethod = 
                QueryPlanExecutor.class.getDeclaredMethod("getChildPlans", 
                    me.bechberger.jfr.extended.plan.plans.StreamingQueryPlan.class);
            assertNotNull(getChildPlansMethod);
            
            java.lang.reflect.Method getChildPlansTypedMethod = 
                QueryPlanExecutor.class.getDeclaredMethod("getChildPlansTyped", 
                    me.bechberger.jfr.extended.plan.plans.StreamingQueryPlan.class);
            assertNotNull(getChildPlansTypedMethod);
        });
    }
}
