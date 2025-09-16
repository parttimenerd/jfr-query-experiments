package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test exact reproduction of failing case
 */
class ReproductionTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("Exact reproduction of failing test")
    void testExactRepro() {
        framework.mockTable("Users")
            .withStringColumn("name")
            .withStringColumn("role")
            .withRow("Alice", "admin")
            .withRow("Bob", "user")
            .build();
        
        // Test single quotes in WHERE
        System.out.println("=== Single quote test ===");
        var result1 = framework.executeQuery(
            "@SELECT name FROM Users WHERE role = 'admin'"
        );
        
        System.out.println("Single quote success: " + result1.isSuccess());
        if (result1.isSuccess()) {
            System.out.println("Single quote rows: " + result1.getTable().getRowCount());
            System.out.println("Single quote table: " + result1.getTable().toString());
            if (result1.getTable().getRowCount() > 0) {
                try {
                    String name = result1.getTable().getString(0, "name");
                    System.out.println("First row name: '" + name + "'");
                } catch (Exception e) {
                    System.out.println("Error getting name: " + e.getMessage());
                }
            }
        } else {
            System.out.println("Single quote error: " + result1.getError().getMessage());
        }
        
        // Test double quotes in WHERE
        System.out.println("=== Double quote test ===");
        var result2 = framework.executeQuery(
            "@SELECT name FROM Users WHERE role = \"admin\""
        );
        
        System.out.println("Double quote success: " + result2.isSuccess());
        if (result2.isSuccess()) {
            System.out.println("Double quote rows: " + result2.getTable().getRowCount());
            System.out.println("Double quote table: " + result2.getTable().toString());
            if (result2.getTable().getRowCount() > 0) {
                try {
                    String name = result2.getTable().getString(0, "name");
                    System.out.println("First row name: '" + name + "'");
                } catch (Exception e) {
                    System.out.println("Error getting name: " + e.getMessage());
                }
            }
        } else {
            System.out.println("Double quote error: " + result2.getError().getMessage());
        }
        
        // Test what both should return
        assertTrue(result1.isSuccess());
        assertTrue(result2.isSuccess());
        assertEquals(1, result1.getTable().getRowCount());
        assertEquals(1, result2.getTable().getRowCount());
    }
}
