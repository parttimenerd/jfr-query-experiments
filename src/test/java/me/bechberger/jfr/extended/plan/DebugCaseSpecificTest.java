package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugCaseSpecificTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
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
    void testSpecificCaseIssue() {
        System.out.println("=== SPECIFIC CASE TEST ===");
        var result = framework.executeQuery("""
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
        
        if (result.isSuccess()) {
            var table = result.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                String eventType = table.getString(i, "eventType");
                long count = table.getNumber(i, "count");
                String frequency = table.getString(i, "frequency");
                
                System.out.println("Row " + i + ": eventType=" + eventType 
                    + ", count=" + count 
                    + ", frequency=" + frequency);
                
                // Manually verify the logic
                String expected;
                if (count > 4) {
                    expected = "High";
                } else if (count > 2) {
                    expected = "Medium";
                } else {
                    expected = "Low";
                }
                
                System.out.println("   Expected: " + expected + ", Got: " + frequency + 
                    (expected.equals(frequency) ? " ✓" : " ❌"));
            }
        } else {
            System.out.println("Query failed: " + result.getError().getMessage());
        }
    }
}
