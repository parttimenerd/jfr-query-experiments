package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test that exactly mirrors the failing DoubleQuotedStringTest
 */
class ExactMatchDebugTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("Debug the exact failing test scenario")
    void testExactMatch() {
        // Exact same setup as the failing test
        framework.mockTable("Users")
            .withStringColumn("name")
            .withStringColumn("role")
            .withRow("Alice", "admin")
            .withRow("Bob", "user")
            .build();
        
        // Test the exact same query that's failing
        System.out.println("=== Debug: Exact Failing Query ===");
        var result1 = framework.executeQuery(
            "@SELECT name FROM Users WHERE role = 'admin'"
        );
        
        System.out.println("Query success: " + result1.isSuccess());
        if (result1.isSuccess()) {
            System.out.println("Result rows: " + result1.getTable().getRowCount());
            for (int i = 0; i < result1.getTable().getRowCount(); i++) {
                System.out.println("Row " + i + ": name=" + result1.getTable().getString(i, "name"));
            }
        } else {
            System.out.println("Error: " + result1.getError().getMessage());
        }
        
        // Debug: Try a simple query without WHERE
        System.out.println("=== Debug: Query without WHERE ===");
        var allResult = framework.executeQuery("@SELECT name, role FROM Users");
        System.out.println("All data success: " + allResult.isSuccess());
        if (allResult.isSuccess()) {
            System.out.println("All data rows: " + allResult.getTable().getRowCount());
            for (int i = 0; i < allResult.getTable().getRowCount(); i++) {
                System.out.println("Row " + i + ": name=" + allResult.getTable().getString(i, "name") + 
                                 ", role=" + allResult.getTable().getString(i, "role"));
            }
        }
        
        // This will probably fail, but we'll see the debug output
        assertTrue(result1.isSuccess());
        assertEquals(1, result1.getTable().getRowCount());
    }
}
