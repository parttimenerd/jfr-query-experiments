import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.plan.exception.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test specific JOIN exception classes for enhanced error categorization.
 */
public class TestJoinExceptionTypes {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testRightJoinSideException() {
        framework.createTable("LeftTable", """
            id | name
            1 | Alice
            2 | Bob
            """);
        
        System.out.println("=== Testing Right JOIN Side Exception ===");
        
        try {
            framework.executeAndAssertSuccess("@SELECT * FROM LeftTable CROSS JOIN NonExistentTable");
            fail("Expected RightJoinSideException");
        } catch (AssertionError e) {
            System.out.println("Enhanced error message:");
            System.out.println(e.getMessage());
            
            // Verify the exception type in the error chain
            Throwable cause = e.getCause();
            while (cause != null) {
                System.out.println("Exception type: " + cause.getClass().getSimpleName());
                if (cause instanceof RightJoinSideException) {
                    System.out.println("✓ Found RightJoinSideException as expected");
                    break;
                }
                cause = cause.getCause();
            }
        }
        System.out.println();
    }
    
    @Test
    void testLeftJoinSideException() {
        framework.createTable("RightTable", """
            id | value
            1 | 100
            2 | 200
            """);
        
        System.out.println("=== Testing Left JOIN Side Exception ===");
        
        try {
            framework.executeAndAssertSuccess("@SELECT * FROM NonExistentTable CROSS JOIN RightTable");
            fail("Expected LeftJoinSideException");
        } catch (AssertionError e) {
            System.out.println("Enhanced error message:");
            System.out.println(e.getMessage());
            
            // Verify the exception type in the error chain
            Throwable cause = e.getCause();
            while (cause != null) {
                System.out.println("Exception type: " + cause.getClass().getSimpleName());
                if (cause instanceof LeftJoinSideException) {
                    System.out.println("✓ Found LeftJoinSideException as expected");
                    break;
                }
                cause = cause.getCause();
            }
        }
        System.out.println();
    }
    
    @Test
    void testBothSidesFailure() {
        System.out.println("=== Testing Both Sides JOIN Failure ===");
        
        try {
            framework.executeAndAssertSuccess("@SELECT * FROM NonExistentLeft CROSS JOIN NonExistentRight");
            fail("Expected JoinExecutionException");
        } catch (AssertionError e) {
            System.out.println("Enhanced error message:");
            System.out.println(e.getMessage());
            
            // Verify the exception type in the error chain
            Throwable cause = e.getCause();
            while (cause != null) {
                System.out.println("Exception type: " + cause.getClass().getSimpleName());
                if (cause instanceof JoinExecutionException) {
                    System.out.println("✓ Found JoinExecutionException as expected");
                    break;
                }
                cause = cause.getCause();
            }
        }
        System.out.println();
    }
}
