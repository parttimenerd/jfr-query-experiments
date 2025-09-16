package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugCaseIsolatedTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Very simple data for debugging
        framework.createTable("Test", """
            type | value
            A | 100
            A | 200
            B | 300
            """);
    }
    
    @Test
    void testSimpleCaseWithLiterals() {
        System.out.println("=== Simple CASE with literals ===");
        var result = framework.executeQuery("""
            @SELECT type, 
                    COUNT(*) AS count,
                    CASE 
                        WHEN 2 > 1 THEN 'Two is greater'
                        ELSE 'One is greater'
                    END AS simple_case
            FROM Test 
            GROUP BY type
            """);
        
        if (result.isSuccess()) {
            var table = result.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("Row " + i + ": type=" + table.getString(i, "type") 
                    + ", count=" + table.getNumber(i, "count")
                    + ", simple_case=" + table.getString(i, "simple_case"));
            }
        } else {
            System.out.println("Query failed: " + result.getError().getMessage());
        }
    }
    
    @Test
    void testCaseWithOneCondition() {
        System.out.println("=== CASE with one aggregate condition ===");
        var result = framework.executeQuery("""
            @SELECT type, 
                    COUNT(*) AS count,
                    CASE 
                        WHEN COUNT(*) > 1 THEN 'Many'
                        ELSE 'Few'
                    END AS frequency
            FROM Test 
            GROUP BY type
            """);
        
        if (result.isSuccess()) {
            var table = result.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("Row " + i + ": type=" + table.getString(i, "type") 
                    + ", count=" + table.getNumber(i, "count")
                    + ", frequency=" + table.getString(i, "frequency"));
            }
        } else {
            System.out.println("Query failed: " + result.getError().getMessage());
        }
    }
}
