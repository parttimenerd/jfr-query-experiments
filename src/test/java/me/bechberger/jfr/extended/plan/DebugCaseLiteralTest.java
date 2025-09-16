package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugCaseLiteralTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        framework.createTable("Test", """
            type | value
            A | 100
            A | 200
            B | 300
            """);
    }
    
    @Test
    void testCaseWithLiterals() {
        System.out.println("=== Testing CASE with pure literals ===");
        var result = framework.executeQuery("""
            @SELECT type, 
                    COUNT(*) AS count,
                    CASE 
                        WHEN 5 > 4 THEN 'High'
                        WHEN 3 > 2 THEN 'Medium'
                        ELSE 'Low'
                    END AS literal_case
            FROM Test 
            GROUP BY type
            """);
        
        if (result.isSuccess()) {
            var table = result.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("Row " + i + ": type=" + table.getString(i, "type") 
                    + ", count=" + table.getNumber(i, "count")
                    + ", literal_case=" + table.getString(i, "literal_case"));
            }
        } else {
            System.out.println("Query failed: " + result.getError().getMessage());
        }
    }
    
    @Test
    void testCaseWithMixedLiterals() {
        System.out.println("=== Testing CASE with mixed literals ===");
        var result = framework.executeQuery("""
            @SELECT type, 
                    COUNT(*) AS count,
                    CASE 
                        WHEN false THEN 'Should not appear'
                        WHEN true THEN 'Should appear'
                        ELSE 'Fallback'
                    END AS boolean_case
            FROM Test 
            GROUP BY type
            """);
        
        if (result.isSuccess()) {
            var table = result.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("Row " + i + ": type=" + table.getString(i, "type") 
                    + ", count=" + table.getNumber(i, "count")
                    + ", boolean_case=" + table.getString(i, "boolean_case"));
            }
        } else {
            System.out.println("Query failed: " + result.getError().getMessage());
        }
    }
}
