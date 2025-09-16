package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugCaseTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Same test data as the original test
        framework.createTable("Events", """
            eventType | duration | threadName | startTime | priority
            GarbageCollection | 200 | GC-Thread-1 | 2023-01-01T10:00:00Z | HIGH
            GarbageCollection | 400 | GC-Thread-1 | 2023-01-01T10:00:10Z | MEDIUM
            GarbageCollection | 600 | GC-Thread-2 | 2023-01-01T10:00:20Z | HIGH
            GarbageCollection | 800 | GC-Thread-2 | 2023-01-01T10:00:30Z | HIGH
            GarbageCollection | 500 | GC-Thread-1 | 2023-01-01T10:00:40Z | MEDIUM
            ExecutionSample | 300 | App-Thread-1 | 2023-01-01T10:00:05Z | LOW
            ExecutionSample | 700 | App-Thread-2 | 2023-01-01T10:00:15Z | MEDIUM
            ExecutionSample | 900 | App-Thread-3 | 2023-01-01T10:00:25Z | HIGH
            ThreadSample | 100 | Background-1 | 2023-01-01T10:00:35Z | LOW
            ThreadSample | 150 | Background-2 | 2023-01-01T10:00:45Z | LOW
            """);
    }
    
    @Test
    void debugCaseExpression() {
        // First test just COUNT to verify it's working correctly
        var result = framework.executeQuery("""
            @SELECT eventType, COUNT(*) AS count
            FROM Events 
            GROUP BY eventType
            """);
        
        if (result.isSuccess()) {
            System.out.println("=== COUNT only ===");
            var table = result.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("Row " + i + ": eventType=" + table.getString(i, "eventType") 
                    + ", count=" + table.getNumber(i, "count"));
            }
        }
        
        // Now test with simple CASE without aggregates first
        var result2 = framework.executeQuery("""
            @SELECT eventType, 
                    COUNT(*) AS count,
                    CASE 
                        WHEN eventType = 'GarbageCollection' THEN 'GC_TYPE'
                        WHEN eventType = 'ExecutionSample' THEN 'EXEC_TYPE'
                        ELSE 'OTHER_TYPE'
                    END AS type_label
            FROM Events 
            GROUP BY eventType
            """);
        
        if (result2.isSuccess()) {
            System.out.println("=== CASE without aggregates ===");
            var table = result2.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("Row " + i + ": eventType=" + table.getString(i, "eventType") 
                    + ", count=" + table.getNumber(i, "count")
                    + ", type_label=" + table.getString(i, "type_label"));
            }
        }
        
        // Finally test with CASE that has aggregates
        var result3 = framework.executeQuery("""
            @SELECT eventType, 
                    COUNT(*) AS count,
                    CASE 
                        WHEN COUNT(*) > 4 THEN 'High'
                        WHEN COUNT(*) > 2 THEN 'Medium'
                        ELSE 'Low'
                    END AS frequency
            FROM Events 
            GROUP BY eventType
            """);
        
        if (result3.isSuccess()) {
            System.out.println("=== CASE with aggregates ===");
            var table = result3.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("Row " + i + ": eventType=" + table.getString(i, "eventType") 
                    + ", count=" + table.getNumber(i, "count") 
                    + ", frequency=" + table.getString(i, "frequency"));
            }
        } else {
            System.out.println("Query failed: " + result3.getError().getMessage());
        }
    }
}
