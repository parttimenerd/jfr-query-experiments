package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple debug test for WHERE clause evaluation with double quotes
 */
class SimpleWhereDebugTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("Debug WHERE clause with double quotes")
    void testWhereDebug() {
        // Create simple test data
        framework.mockTable("Test")
            .withStringColumn("name")
            .withRow("admin")
            .build();
        
        // Test with single quotes (should work)
        System.out.println("=== Testing single quotes ===");
        var result1 = framework.executeQuery(
            "@SELECT name FROM Test WHERE name = 'admin'"
        );
        System.out.println("Single quote success: " + result1.isSuccess());
        if (result1.isSuccess()) {
            System.out.println("Single quote result rows: " + result1.getTable().getRowCount());
            if (result1.getTable().getRowCount() > 0) {
                System.out.println("First row name: " + result1.getTable().getString(0, "name"));
            }
        } else {
            System.out.println("Single quote error: " + result1.getError().getMessage());
        }
        
        // Test with double quotes (currently failing)
        System.out.println("=== Testing double quotes ===");
        var result2 = framework.executeQuery(
            "@SELECT name FROM Test WHERE name = \"admin\""
        );
        System.out.println("Double quote success: " + result2.isSuccess());
        if (result2.isSuccess()) {
            System.out.println("Double quote result rows: " + result2.getTable().getRowCount());
            if (result2.getTable().getRowCount() > 0) {
                System.out.println("First row name: " + result2.getTable().getString(0, "name"));
            }
        } else {
            System.out.println("Double quote error: " + result2.getError().getMessage());
        }
        
        // First verify single quotes work
        assertTrue(result1.isSuccess(), "Single quotes should work");
        assertEquals(1, result1.getTable().getRowCount(), "Single quotes should find 1 row");
        
        // Then verify double quotes work
        assertTrue(result2.isSuccess(), "Double quotes should work");
        assertEquals(1, result2.getTable().getRowCount(), "Double quotes should find 1 row");
    }
}
