package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class EqualsFixVerificationTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        framework.mockTable("Test", """
            name
            Alice
            Bob
            """);
    }
    
    @Test
    void testEqualsOperatorWorking() {
        // Test that EQUALS operator works for matching case
        String query1 = "@SELECT name FROM Test WHERE name = 'Alice'";
        var result1 = framework.executeQuery(query1);
        
        assertTrue(result1.isSuccess(), "Query should succeed");
        assertEquals(1, result1.getTable().getRowCount(), "Should return 1 row for Alice");
        
        // Test that EQUALS operator works for non-matching case
        String query2 = "@SELECT name FROM Test WHERE name = 'NonExistent'";
        var result2 = framework.executeQuery(query2);
        
        assertTrue(result2.isSuccess(), "Query should succeed");
        assertEquals(0, result2.getTable().getRowCount(), "Should return 0 rows for NonExistent");
        
        // Test the GROUP BY case that was failing
        String query3 = "@SELECT name, COUNT(*) as count FROM Test WHERE name = 'NonExistent' GROUP BY name";
        var result3 = framework.executeQuery(query3);
        
        assertTrue(result3.isSuccess(), "GROUP BY query should succeed");
        assertEquals(0, result3.getTable().getRowCount(), "GROUP BY with empty WHERE should return 0 rows");
    }
}
