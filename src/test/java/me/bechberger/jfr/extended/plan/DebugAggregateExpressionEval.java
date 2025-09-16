package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Debug to understand what evaluateAggregateExpression returns for binary expressions
 */
public class DebugAggregateExpressionEval {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void debugBinaryExpressionInAggregate() {
        // Create test data with different counts per group
        framework.createTable("Events", """
            eventType | eventTime
            GarbageCollection | 2023-01-01T10:00:00Z
            GarbageCollection | 2023-01-01T10:00:01Z
            GarbageCollection | 2023-01-01T10:00:02Z
            GarbageCollection | 2023-01-01T10:00:03Z
            GarbageCollection | 2023-01-01T10:00:04Z
            ExecutionSample | 2023-01-01T10:00:05Z
            ExecutionSample | 2023-01-01T10:00:06Z
            ExecutionSample | 2023-01-01T10:00:07Z
            ThreadSample | 2023-01-01T10:00:08Z
            ThreadSample | 2023-01-01T10:00:09Z
            """);
        
        System.out.println("=== Testing binary expression evaluation in aggregate context ===");
        
        // Test what happens when we evaluate COUNT(*) > 4 as an aggregate expression
        var result = framework.executeAndAssertSuccess("""
            @SELECT 
                eventType,
                COUNT(*) as count,
                (COUNT(*) > 4) as is_high_count
            FROM Events 
            GROUP BY eventType 
            ORDER BY COUNT(*) DESC
            """);
        
        var table = result.getTable();
        for (int i = 0; i < table.getRowCount(); i++) {
            System.out.printf("Row %d: eventType=%s, count=%s, is_high_count=%s%n", 
                i, 
                table.getCell(i, "eventType"),
                table.getCell(i, "count"), 
                table.getCell(i, "is_high_count"));
        }
        
        System.out.println("\\nExpected:");
        System.out.println("GarbageCollection: count=5, is_high_count=true");
        System.out.println("ExecutionSample: count=3, is_high_count=false");
        System.out.println("ThreadSample: count=2, is_high_count=false");
    }
}
