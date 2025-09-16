package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test exact copy of failing scenario
 */
class ExactCopyTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("Exact copy of failing scenario")
    void testExactCopy() {
        framework.mockTable("Users")
            .withStringColumn("name")
            .withStringColumn("role")
            .withRow("Alice", "admin")
            .withRow("Bob", "user")
            .build();
        
        System.out.println("=== Testing exact failing query ===");
        var result1 = framework.executeQuery(
            "@SELECT name FROM Users WHERE role = 'admin'"
        );
        
        System.out.println("Success: " + result1.isSuccess());
        if (result1.isSuccess()) {
            System.out.println("Rows: " + result1.getTable().getRowCount());
            System.out.println("Table: " + result1.getTable().toString());
            if (result1.getTable().getRowCount() > 0) {
                System.out.println("First row name: " + result1.getTable().getString(0, "name"));
            } else {
                System.out.println("No rows returned");
            }
        } else {
            System.out.println("Error: " + result1.getError().getMessage());
        }
        
        assertTrue(result1.isSuccess());
        assertEquals(1, result1.getTable().getRowCount());
        assertEquals("Alice", result1.getTable().getString(0, "name"));
    }
}
