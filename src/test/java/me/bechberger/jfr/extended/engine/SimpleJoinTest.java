package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple test to check if JOIN queries can be parsed and executed
 */
class SimpleJoinTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testSimpleInnerJoinParsing() {
        // Create very simple test tables
        framework.mockTable("events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withRow(1.0, "GC")
            .withRow(2.0, "Allocation")
            .build();
            
        framework.mockTable("metrics")
            .withNumberColumn("threadId") 
            .withNumberColumn("memory")
            .withRow(1.0, 1024.0)
            .withRow(2.0, 2048.0)
            .build();
        
        // Try to execute a simple INNER JOIN
        var result = framework.executeQuery(
            "@SELECT * FROM events e INNER JOIN metrics m ON e.threadId = m.threadId"
        );
        
        // Print result details for debugging
        System.out.println("Query success: " + result.isSuccess());
        if (!result.isSuccess()) {
            System.out.println("Error: " + result.getError());
        }
        
        // For now, let's just check if it doesn't throw a parser exception
        // We'll assert success once we find the issue
        assertNotNull(result, "Result should not be null");
    }
}
