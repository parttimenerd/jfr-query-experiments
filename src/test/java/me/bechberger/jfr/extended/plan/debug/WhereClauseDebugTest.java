package me.bechberger.jfr.extended.plan.debug;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Debug test to understand exactly what's happening in WHERE clause evaluation
 */
class WhereClauseDebugTest {

    private QueryTestFramework framework;

    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }

    @Test
    void debugWhereClauseEvaluation() {
        // Create exact test data from failing test
        framework.mockTable("Users")
            .withStringColumn("name")
            .withStringColumn("role")
            .withRow("Alice", "admin")
            .withRow("Bob", "user")
            .build();

        System.out.println("=== WHERE CLAUSE DEBUG TEST ===");
        
        // Test 1: Check the basic table data
        var allDataResult = framework.executeQuery("@SELECT name, role FROM Users");
        System.out.println("1. All data query result:");
        System.out.println("   Success: " + allDataResult.isSuccess());
        if (allDataResult.isSuccess()) {
            var table = allDataResult.getTable();
            System.out.println("   Rows: " + table.getRowCount());
            System.out.println("   Columns: " + table.getColumnCount());
            for (int i = 0; i < table.getColumnCount(); i++) {
                System.out.println("   Column " + i + ": '" + table.getColumns().get(i).name() + "'");
            }
            for (int r = 0; r < table.getRowCount(); r++) {
                System.out.println("   Row " + r + ":");
                for (int c = 0; c < table.getColumnCount(); c++) {
                    var cell = table.getCell(r, c);
                    System.out.println("     " + table.getColumns().get(c).name() + " = '" + cell.getValue() + "' (type: " + cell.getClass().getSimpleName() + ")");
                }
            }
        }
        
        // Test 2: Single quotes WHERE (this should work)
        System.out.println("\n2. Single quotes WHERE test:");
        var singleQuoteResult = framework.executeQuery("@SELECT name FROM Users WHERE role = 'admin'");
        System.out.println("   Query: @SELECT name FROM Users WHERE role = 'admin'");
        System.out.println("   Success: " + singleQuoteResult.isSuccess());
        if (singleQuoteResult.isSuccess()) {
            var table = singleQuoteResult.getTable();
            System.out.println("   Result rows: " + table.getRowCount());
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("   Row " + i + ": name = '" + table.getString(i, "name") + "'");
            }
        } else {
            System.out.println("   Error: " + singleQuoteResult.getError().getMessage());
        }
        
        // Test 3: Double quotes WHERE (this is failing)
        System.out.println("\n3. Double quotes WHERE test:");
        var doubleQuoteResult = framework.executeQuery("@SELECT name FROM Users WHERE role = \"admin\"");
        System.out.println("   Query: @SELECT name FROM Users WHERE role = \"admin\"");
        System.out.println("   Success: " + doubleQuoteResult.isSuccess());
        if (doubleQuoteResult.isSuccess()) {
            var table = doubleQuoteResult.getTable();
            System.out.println("   Result rows: " + table.getRowCount());
            for (int i = 0; i < table.getRowCount(); i++) {
                System.out.println("   Row " + i + ": name = '" + table.getString(i, "name") + "'");
            }
        } else {
            System.out.println("   Error: " + doubleQuoteResult.getError().getMessage());
        }
        
        // Test 4: Check if double quotes are being parsed as string literals
        System.out.println("\n4. Double quote literal test:");
        var literalResult = framework.executeQuery("@SELECT \"admin\" as literal FROM Users LIMIT 1");
        System.out.println("   Query: @SELECT \"admin\" as literal FROM Users LIMIT 1");
        System.out.println("   Success: " + literalResult.isSuccess());
        if (literalResult.isSuccess()) {
            var table = literalResult.getTable();
            if (table.getRowCount() > 0) {
                var cell = table.getCell(0, 0);
                System.out.println("   Literal value: '" + cell.getValue() + "' (type: " + cell.getClass().getSimpleName() + ")");
            }
        }
        
        System.out.println("\n=== END DEBUG TEST ===");
    }
}
