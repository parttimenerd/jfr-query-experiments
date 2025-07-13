package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test the newly implemented assertSortedByColumn functionality
 */
public class SortCheckingTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Create test data for sorting validation
        framework.mockTable("SortTestData", """
            name | age | score
            Alice | 25 | 95.5
            Bob | 30 | 87.2
            Charlie | 20 | 92.0
            David | 35 | 88.8
            """);
    }
    
    @Test
    void testSortCheckingAscending() {
        // This should pass - data is sorted by age ascending
        var result = framework.executeQuery("@SELECT * FROM SortTestData ORDER BY age ASC");
        // Test the sort checking - this should work
        assertDoesNotThrow(() -> {
            result.assertSortedByColumn("age", true);
        });
    }
    
    @Test 
    void testSortCheckingDescending() {
        // This should pass - data is sorted by score descending
        var result = framework.executeQuery("SELECT * FROM SortTestData ORDER BY score DESC");
                System.out.println("RESULT: " + result);

        
        // Test the sort checking - this should work
        assertDoesNotThrow(() -> {
            result.assertSortedByColumn("score", false);
        });
    }
    
    @Test
    void testSortCheckingFailure() {
        // This should fail - data is not sorted by name
        var result = framework.executeQuery("SELECT * FROM SortTestData");
        
        // Test that sort checking properly detects unsorted data
        assertThrows(AssertionError.class, () -> {
            result.assertSortedByColumn("name", true);
        });
    }
    
    @Test
    void testSortCheckingNonExistentColumn() {
        var result = framework.executeQuery("SELECT * FROM SortTestData");
        
        // Test that sort checking properly handles non-existent columns
        assertThrows(AssertionError.class, () -> {
            result.assertSortedByColumn("nonexistent", true);
        });
    }
}
