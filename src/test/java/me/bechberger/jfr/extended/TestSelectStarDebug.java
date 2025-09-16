package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Debug test to compare query planning between explicit columns and SELECT *
 */
public class TestSelectStarDebug {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Create test tables
        framework.createTable("Users", """
            id | name
            1 | Alice
            2 | Bob
            """);
            
        framework.createTable("Items", """
            code | item
            A | Apple
            """);
    }
    
    @Test
    void testQueryPlanningDifferences() {
        System.out.println("=== TESTING QUERY PLANNING DIFFERENCES ===");
        
        // Test 1: Single table SELECT *
        System.out.println("\n1. Single table SELECT *:");
        var result1 = framework.executeQuery("@SELECT * FROM Users AS u");
        System.out.println("   Success: " + result1.isSuccess());
        if (!result1.isSuccess()) {
            System.out.println("   Error: " + result1.getError().getMessage());
        }
        
        // Test 2: Single table explicit columns
        System.out.println("\n2. Single table explicit columns:");
        var result2 = framework.executeQuery("@SELECT id, name FROM Users AS u");
        System.out.println("   Success: " + result2.isSuccess());
        if (!result2.isSuccess()) {
            System.out.println("   Error: " + result2.getError().getMessage());
        }
        
        // Test 3: CROSS JOIN SELECT *
        System.out.println("\n3. CROSS JOIN SELECT *:");
        var result3 = framework.executeQuery("@SELECT * FROM Users AS u CROSS JOIN Items AS i");
        System.out.println("   Success: " + result3.isSuccess());
        if (!result3.isSuccess()) {
            System.out.println("   Error: " + result3.getError().getMessage());
        }
        
        // Test 4: CROSS JOIN explicit columns  
        System.out.println("\n4. CROSS JOIN explicit columns:");
        var result4 = framework.executeQuery("@SELECT id, name, code, item FROM Users AS u CROSS JOIN Items AS i");
        System.out.println("   Success: " + result4.isSuccess());
        if (!result4.isSuccess()) {
            System.out.println("   Error: " + result4.getError().getMessage());
        } else {
            System.out.println("   Rows: " + result4.getTable().getRowCount());
            System.out.println("   Columns: " + result4.getTable().getColumnCount());
        }
        
        System.out.println("\n=== ANALYSIS ===");
        System.out.println("The pattern shows that explicit column queries work but SELECT * queries fail.");
        System.out.println("This suggests the issue is in SELECT * processing, not in the JOIN logic itself.");
    }
}
