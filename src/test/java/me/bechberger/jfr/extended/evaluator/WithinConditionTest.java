package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for WITHIN condition functionality using real queries and the query engine.
 * 
 * Tests the implementation of temporal proximity queries using WITHIN operator.
 */
public class WithinConditionTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("WITHIN condition should correctly evaluate temporal proximity")
    void testWithinConditionWithTimestamps() {
        // Create a table with timestamp data
        framework.mockTable("Events")
            .withNumberColumn("timestamp")
            .withStringColumn("name")
            .withRow(1000000000000L, "event1")    // reference time
            .withRow(1000000003000L, "event2")    // 3000 nanoseconds later (within 5000)
            .withRow(1000000008000L, "event3")    // 8000 nanoseconds later (outside 5000)
            .build();
        
        // Test WITHIN query: select events within 5000 nanoseconds of reference time
        var result = framework.executeQuery(
            "@SELECT name FROM Events WHERE timestamp WITHIN 5000 OF 1000000000000"
        );
        
        System.out.println("WITHIN query result: " + result.isSuccess());
        if (!result.isSuccess()) {
            System.out.println("Error: " + result.getError().getMessage());
            System.out.println("Error type: " + result.getError().getClass().getSimpleName());
            result.getError().printStackTrace();
        } else {
            System.out.println("Row count: " + result.getTable().getRowCount());
            // Print the results for debugging
            for (int i = 0; i < result.getTable().getRowCount(); i++) {
                System.out.println("Row " + i + ": " + result.getTable().getString(i, "name"));
            }
        }
        
        assertTrue(result.isSuccess(), "WITHIN query should execute successfully");
        assertEquals(2, result.getTable().getRowCount(), "Should return event1 and event2 (within 5000ns)");
        
        // Verify the correct events are returned
        var resultNames = java.util.Set.of(
            result.getTable().getString(0, "name"),
            result.getTable().getString(1, "name")
        );
        assertTrue(resultNames.contains("event1"), "Should include event1 (reference time)");
        assertTrue(resultNames.contains("event2"), "Should include event2 (within window)");
    }
    
    @Test
    @DisplayName("WITHIN condition should return false when value is outside time window")
    void testWithinOutsideTimeWindow() {
        // Create a table with timestamp data
        framework.mockTable("Events")
            .withNumberColumn("timestamp")
            .withStringColumn("name")
            .withRow(1000000000000L, "reference")   // reference time
            .withRow(1000000005000L, "far_event")   // 5000 nanoseconds later (outside 1000ns window)
            .build();
        
        // Test WITHIN query with small window: only events within 1000 nanoseconds
        var result = framework.executeQuery(
            "@SELECT name FROM Events WHERE timestamp WITHIN 1000 OF 1000000000000"
        );
        
        System.out.println("Narrow WITHIN query result: " + result.isSuccess());
        if (!result.isSuccess()) {
            System.out.println("Error: " + result.getError().getMessage());
            result.getError().printStackTrace();
        } else {
            System.out.println("Row count: " + result.getTable().getRowCount());
        }
        
        assertTrue(result.isSuccess(), "WITHIN query should execute successfully");
        assertEquals(1, result.getTable().getRowCount(), "Should only return reference event (within 1000ns)");
        assertEquals("reference", result.getTable().getString(0, "name"), "Should only include reference event");
    }
    
    @Test
    @DisplayName("WITHIN condition should work with different time values")
    void testWithinDifferentTimeValues() {
        // Create a table with various timestamps
        framework.mockTable("TimeEvents")
            .withNumberColumn("timestamp")
            .withStringColumn("event")
            .withRow(100L, "early")
            .withRow(500L, "target")      // reference
            .withRow(750L, "close")       // 250 units later (within 300)
            .withRow(900L, "far")         // 400 units later (outside 300)
            .build();
        
        // Test WITHIN with window of 300 units
        var result = framework.executeQuery(
            "@SELECT event FROM TimeEvents WHERE timestamp WITHIN 300 OF 500"
        );
        
        assertTrue(result.isSuccess(), "WITHIN query should execute successfully");
        assertEquals(2, result.getTable().getRowCount(), "Should return target and close events");
        
        var eventNames = java.util.Set.of(
            result.getTable().getString(0, "event"),
            result.getTable().getString(1, "event")
        );
        assertTrue(eventNames.contains("target"), "Should include target event");
        assertTrue(eventNames.contains("close"), "Should include close event");
    }
}
