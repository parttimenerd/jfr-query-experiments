package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test double quotes specifically in SELECT vs WHERE
 */
class SelectVsWhereQuotesTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("Test double quotes in SELECT clause")
    void testDoubleQuotesInSelect() {
        framework.mockTable("Test")
            .withStringColumn("name")
            .withRow("Alice")
            .build();
        
        // Test single quotes in SELECT (should work)
        System.out.println("=== Single quotes in SELECT ===");
        var result1 = framework.executeQuery(
            "@SELECT 'constant' AS literal, name FROM Test"
        );
        System.out.println("Single quote SELECT success: " + result1.isSuccess());
        if (result1.isSuccess()) {
            System.out.println("Rows: " + result1.getTable().getRowCount());
            if (result1.getTable().getRowCount() > 0) {
                System.out.println("Literal: " + result1.getTable().getString(0, "literal"));
                System.out.println("Name: " + result1.getTable().getString(0, "name"));
            }
        }
        
        // Test double quotes in SELECT (might be failing)
        System.out.println("=== Double quotes in SELECT ===");
        var result2 = framework.executeQuery(
            "@SELECT \"constant\" AS literal, name FROM Test"
        );
        System.out.println("Double quote SELECT success: " + result2.isSuccess());
        if (result2.isSuccess()) {
            System.out.println("Rows: " + result2.getTable().getRowCount());
            if (result2.getTable().getRowCount() > 0) {
                System.out.println("Literal: " + result2.getTable().getString(0, "literal"));
                System.out.println("Name: " + result2.getTable().getString(0, "name"));
            }
        } else {
            System.out.println("Error: " + result2.getError().getMessage());
        }
        
        assertTrue(result1.isSuccess());
        assertTrue(result2.isSuccess());
        assertEquals(1, result1.getTable().getRowCount());
        assertEquals(1, result2.getTable().getRowCount());
    }
}
