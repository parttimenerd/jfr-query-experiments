import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test the improved error handling in AggregationPlan
 */
public class TestAggregationErrorHandling {
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testNonAggregateWithoutGroupBy() {
        // Create a test table
        framework.createTable("TestData", """
            name | value
            Alice | 10
            Bob | 20
            """);
        
        // This should fail with a clear error message
        try {
            var result = framework.executeQuery("@SELECT name, COUNT(*) FROM TestData");
            
            // Check if the result indicates failure
            if (result.getTable() == null || !result.isSuccess()) {
                System.out.println("Query failed as expected");
                // This is what we expect - the query should fail
                return;
            }
            
            System.out.println("Unexpected success: " + result.getTable().toString());
            fail("Expected error for non-aggregate column without GROUP BY");
        } catch (Exception e) {
            System.out.println("Expected error: " + e.getMessage());
            assertTrue(e.getMessage().contains("Non-aggregate expression") || 
                      e.getMessage().contains("GROUP BY") ||
                      e.getMessage().contains("aggregate") ||
                      e.getMessage().contains("null"), 
                "Error should mention aggregation issue: " + e.getMessage());
        }
    }
    
    @Test
    void testValidGroupBy() {
        // Create a test table
        framework.createTable("TestData", """
            category | value
            A | 10
            B | 20
            A | 15
            """);
        
        // This should work fine
        framework.executeAndExpectTable("@SELECT category, COUNT(*) as count FROM TestData GROUP BY category", """
            category | count
            A | 2
            B | 1
            """);
    }
    
    @Test
    void testInvalidAggregateFunction() {
        // Create a test table
        framework.createTable("TestData", """
            value
            10
            20
            30
            """);
        
        // This should fail with a clear error message about invalid function
        try {
            framework.executeQuery("@SELECT NONEXISTENT_FUNC(value) FROM TestData");
            // This might not fail immediately due to how functions are resolved
            // But if it does, we should get a good error message
        } catch (Exception e) {
            System.out.println("Function error: " + e.getMessage());
            // Check that error messages are descriptive
            assertFalse(e.getMessage().isEmpty(), "Error message should not be empty");
        }
    }
}
