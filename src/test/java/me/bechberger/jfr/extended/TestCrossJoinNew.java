package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Simple test for CROSS JOIN functionality in the new query plan system
 */
public class TestCrossJoinNew {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testBasicCrossJoinNewSystem() {
        // Create test tables using proper AS keyword syntax
        framework.createTable("Users", """
            id | name
            1 | Alice
            2 | Bob
            """);
            
        framework.createTable("Items", """
            code | item
            A | Apple
            B | Banana
            """);
        
        // Test CROSS JOIN with specific column names instead of SELECT *
        var result = framework.executeQuery("@SELECT id, name, code, item FROM Users AS u CROSS JOIN Items AS i");
        
        System.out.println("Query executed successfully: " + result.isSuccess());
        if (result.isSuccess()) {
            var table = result.getTable();
            System.out.println("Row count: " + table.getRowCount());
            System.out.println("Column count: " + table.getColumnCount());
            
            for (int i = 0; i < table.getColumnCount(); i++) {
                System.out.println("Column " + i + ": " + table.getColumns().get(i).name());
            }
            
            // Print first few rows
            for (int row = 0; row < Math.min(4, table.getRowCount()); row++) {
                System.out.print("Row " + row + ": [");
                for (int col = 0; col < table.getColumnCount(); col++) {
                    if (col > 0) System.out.print(", ");
                    var cell = table.getRows().get(row).getCells().get(col);
                    System.out.print(cell.toString());
                }
                System.out.println("]");
            }
        } else {
            System.out.println("Error: " + result.getError().getMessage());
        }
    }
    
    @Test 
    void testSelectStarWithCrossJoin() {
        // Recreate tables to ensure clean state
        framework.createTable("Users", """
            id | name
            1 | Alice
            2 | Bob
            """);
            
        framework.createTable("Items", """
            code | item
            A | Apple
            B | Banana
            """);
        
        System.out.println("=== DEBUGGING TABLE CREATION AND ACCESS ===");
        
        // Debug: Check what tables are actually registered
        System.out.println("\n1. Checking table registration...");
        
        // The mock executor is internal, so let's focus on testing behavior
        
        // Test 2: Check individual tables with explicit columns
        System.out.println("\n2. Testing explicit column access:");
        var explicitResult = framework.executeQuery("@SELECT id, name FROM Users AS u");
        System.out.println("   Explicit columns - Success: " + explicitResult.isSuccess());
        if (explicitResult.isSuccess()) {
            var table = explicitResult.getTable();
            System.out.println("   Explicit - Rows: " + table.getRowCount() + ", Columns: " + table.getColumnCount());
            for (int i = 0; i < table.getColumnCount(); i++) {
                System.out.println("   Explicit - Column " + i + ": " + table.getColumns().get(i).name());
            }
            if (table.getRowCount() > 0) {
                System.out.print("   Explicit - Row 0: [");
                for (int col = 0; col < table.getColumnCount(); col++) {
                    if (col > 0) System.out.print(", ");
                    var cell = table.getRows().get(0).getCells().get(col);
                    System.out.print(cell.toString());
                }
                System.out.println("]");
            }
        }
        
        // Test 3: SELECT * from single table
        System.out.println("\n3. Testing SELECT * from single table:");
        var starResult = framework.executeQuery("@SELECT * FROM Users AS u");
        System.out.println("   SELECT * single - Success: " + starResult.isSuccess());
        if (starResult.isSuccess()) {
            var table = starResult.getTable();
            System.out.println("   SELECT * single - Rows: " + table.getRowCount() + ", Columns: " + table.getColumnCount());
            for (int i = 0; i < table.getColumnCount(); i++) {
                System.out.println("   SELECT * single - Column " + i + ": " + table.getColumns().get(i).name());
            }
        }
        
        System.out.println("\n=== ANALYSIS ===");
        System.out.println("If explicit columns work but SELECT * doesn't, the issue is in SELECT * expansion logic.");
        System.out.println("If both fail, the issue is in table registration or scanning.");
    }
}
