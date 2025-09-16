package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DebugCaseDetailedTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Simplified test data for debugging
        framework.createTable("Events", """
            eventType | duration
            A | 100
            A | 200
            A | 300
            A | 400
            A | 500
            B | 100
            B | 200
            B | 300
            C | 100
            C | 200
            """);
    }
    
    @Test
    void debugCaseStep1_BasicCount() {
        System.out.println("=== STEP 1: Basic COUNT ===");
        var result = framework.executeQuery("""
            @SELECT eventType, COUNT(*) AS count
            FROM Events 
            GROUP BY eventType
            """);
        
        if (result.isSuccess()) {
            var table = result.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("Row " + i + ": eventType=" + table.getString(i, "eventType") 
                    + ", count=" + table.getNumber(i, "count"));
            }
        } else {
            System.out.println("Query failed: " + result.getError().getMessage());
        }
    }
    
    @Test
    void debugCaseStep2_SimpleComparison() {
        System.out.println("=== STEP 2: Simple comparison ===");
        var result = framework.executeQuery("""
            @SELECT eventType, 
                    COUNT(*) AS count,
                    COUNT(*) > 4 as gt4,
                    COUNT(*) > 2 as gt2
            FROM Events 
            GROUP BY eventType
            """);
        
        if (result.isSuccess()) {
            var table = result.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("Row " + i + ": eventType=" + table.getString(i, "eventType") 
                    + ", count=" + table.getNumber(i, "count")
                    + ", gt4=" + table.getBoolean(i, "gt4")
                    + ", gt2=" + table.getBoolean(i, "gt2"));
            }
        } else {
            System.out.println("Query failed: " + result.getError().getMessage());
        }
    }
    
    @Test
    void debugCaseStep3_CaseExpression() {
        System.out.println("=== STEP 3: CASE expression ===");
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
                System.out.println("Row " + i + ": eventType=" + table.getString(i, "eventType") 
                    + ", count=" + table.getNumber(i, "count") 
                    + ", frequency=" + table.getString(i, "frequency"));
            }
        } else {
            System.out.println("Query failed: " + result.getError().getMessage());
        }
    }
}
