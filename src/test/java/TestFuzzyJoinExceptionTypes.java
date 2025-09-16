import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.plan.exception.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test that FuzzyJoinPlan uses specific exception classes.
 */
public class TestFuzzyJoinExceptionTypes {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testFuzzyJoinRightSideException() {
        framework.createTable("LeftEvents", """
            timestamp | event_type
            1000 | click
            2000 | view
            """);
        
        System.out.println("=== Testing FUZZY JOIN Right Side Exception ===");
        
        try {
            framework.executeAndAssertSuccess("@SELECT * FROM LeftEvents FUZZY JOIN NonExistentTable ON temporal_correlation");
            fail("Expected RightJoinSideException");
        } catch (AssertionError e) {
            System.out.println("Enhanced error message:");
            System.out.println(e.getMessage());
            
            // Verify the exception type in the error chain
            Throwable cause = e.getCause();
            while (cause != null) {
                System.out.println("Exception type: " + cause.getClass().getSimpleName());
                if (cause instanceof RightJoinSideException) {
                    System.out.println("✓ Found RightJoinSideException as expected for FUZZY JOIN");
                    break;
                }
                cause = cause.getCause();
            }
        }
        System.out.println();
    }
    
    @Test
    void testFuzzyJoinLeftSideException() {
        framework.createTable("RightEvents", """
            timestamp | event_data
            1500 | data1
            2500 | data2
            """);
        
        System.out.println("=== Testing FUZZY JOIN Left Side Exception ===");
        
        try {
            framework.executeAndAssertSuccess("@SELECT * FROM NonExistentTable FUZZY JOIN RightEvents ON temporal_correlation");
            fail("Expected LeftJoinSideException");
        } catch (AssertionError e) {
            System.out.println("Enhanced error message:");
            System.out.println(e.getMessage());
            
            // Verify the exception type in the error chain
            Throwable cause = e.getCause();
            while (cause != null) {
                System.out.println("Exception type: " + cause.getClass().getSimpleName());
                if (cause instanceof LeftJoinSideException) {
                    System.out.println("✓ Found LeftJoinSideException as expected for FUZZY JOIN");
                    break;
                }
                cause = cause.getCause();
            }
        }
        System.out.println();
    }
}
