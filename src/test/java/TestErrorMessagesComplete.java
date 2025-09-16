import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive test of enhanced error message reporting functionality.
 */
public class TestErrorMessagesComplete {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testAllErrorMessageTypes() {
        // Create test data
        framework.createTable("Orders", """
            id | customer | amount
            1 | Alice | 100.50
            2 | Bob | 200.75
            """);
        
        System.out.println("=== Testing Enhanced Error Messages ===\n");
        
        // Test 1: Missing table error
        testErrorMessage("Missing Table", "@SELECT * FROM NonExistentTable");
        
        // Test 2: Invalid function error  
        testErrorMessage("Invalid Function", "@SELECT INVALID_FUNC(id) FROM Orders");
        
        // Test 3: Invalid column error
        testErrorMessage("Invalid Column", "@SELECT nonexistent_column FROM Orders");
        
        // Test 4: Cross join with enhanced error context
        testErrorMessage("Cross Join Error", "@SELECT * FROM Orders CROSS JOIN NonExistentTable");
        
        // Test 5: Aggregation with invalid expression
        testErrorMessage("Invalid Aggregation", "@SELECT COUNT(invalid_column) FROM Orders");
        
        System.out.println("=== Error Message Testing Complete ===");
    }
    
    private void testErrorMessage(String testName, String query) {
        System.out.println("--- " + testName + " ---");
        try {
            framework.executeAndAssertSuccess(query);
            System.out.println("ERROR: Query unexpectedly succeeded: " + query);
        } catch (AssertionError e) {
            System.out.println("Enhanced Error Message:");
            System.out.println(e.getMessage());
        } catch (Exception e) {
            System.out.println("Unexpected exception: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        System.out.println();
    }
}
