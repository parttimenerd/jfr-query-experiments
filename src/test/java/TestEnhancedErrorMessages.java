import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test enhanced error message reporting functionality.
 */
public class TestEnhancedErrorMessages {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testErrorMessageForMissingTable() {
        // This should trigger an enhanced error message showing the query and error details
        try {
            framework.executeAndAssertSuccess("@SELECT * FROM NonExistentTable");
        } catch (AssertionError e) {
            System.out.println("Enhanced error message:");
            System.out.println(e.getMessage());
            // The error should now contain the query text and detailed error information
        }
    }
    
    @Test
    void testErrorMessageForInvalidFunction() {
        framework.createTable("Users", """
            name | age
            Alice | 25
            Bob | 30
            """);
        
        try {
            framework.executeAndAssertSuccess("@SELECT INVALID_FUNCTION(name) FROM Users");
        } catch (AssertionError e) {
            System.out.println("Enhanced error message for invalid function:");
            System.out.println(e.getMessage());
        }
    }
    
    @Test
    void testErrorMessageForTypeError() {
        // Create a table with string data 
        framework.createTable("Data", """
            name | value
            Alice | hello
            Bob | world
            """);
        
        try {
            // This should fail because SUM requires numeric data but we have strings
            var result = framework.executeQuery("@SELECT SUM(value) FROM Data");
            if (!result.isSuccess()) {
                System.out.println("Enhanced error message for type error:");
                System.out.println(result.getError().getMessage());
            } else {
                System.out.println("Query unexpectedly succeeded: " + result.getTable().getString(0, 0));
            }
        } catch (Exception e) {
            System.out.println("Enhanced error message for type error:");
            System.out.println(e.getMessage());
        }
    }
}
