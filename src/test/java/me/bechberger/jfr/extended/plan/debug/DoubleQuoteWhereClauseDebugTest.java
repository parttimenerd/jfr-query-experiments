package me.bechberger.jfr.extended.plan.debug;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Targeted test to debug the exact issue with double-quoted strings in WHERE clauses.
 */
class DoubleQuoteWhereClauseDebugTest {

    private QueryTestFramework framework;

    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Create the exact same data as the failing test
        framework.mockTable("Users")
            .withStringColumn("name")
            .withStringColumn("role")
            .withRow("Alice", "admin")
            .withRow("Bob", "user")
            .build();
    }

    @Test
    @DisplayName("Debug double-quoted string WHERE clause issue")
    void debugDoubleQuotedWhereClause() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("DEBUGGING DOUBLE-QUOTED STRING WHERE CLAUSE");
        System.out.println("=".repeat(60));
        
        // First, let's see the data
        var allData = framework.executeQuery("@SELECT name, role FROM Users");
        System.out.println("1. All data in Users table:");
        if (allData.isSuccess()) {
            var table = allData.getTable();
            for (int i = 0; i < table.getRowCount(); i++) {
                String name = table.getString(i, "name");
                String role = table.getString(i, "role");
                System.out.println("   Row " + i + ": name='" + name + "', role='" + role + "'");
            }
        }
        
        // Test single-quoted string (should work)
        System.out.println("\n2. Testing single-quoted string WHERE clause:");
        var singleQuoteResult = framework.executeQuery("@SELECT COUNT(*) FROM Users WHERE role = 'admin'");
        System.out.println("   Query: @SELECT COUNT(*) FROM Users WHERE role = 'admin'");
        if (singleQuoteResult.isSuccess()) {
            try {
                var count = singleQuoteResult.getTable().getNumber(0, 0);
                System.out.println("   Result: " + count + " (should be 1)");
            } catch (Exception e) {
                System.out.println("   Error: " + e.getMessage());
            }
        } else {
            System.out.println("   Query failed: " + singleQuoteResult.getError().getMessage());
        }
        
        // Test double-quoted string (currently broken)
        System.out.println("\n3. Testing double-quoted string WHERE clause:");
        var doubleQuoteResult = framework.executeQuery("@SELECT COUNT(*) FROM Users WHERE role = \"admin\"");
        System.out.println("   Query: @SELECT COUNT(*) FROM Users WHERE role = \"admin\"");
        if (doubleQuoteResult.isSuccess()) {
            try {
                var count = doubleQuoteResult.getTable().getNumber(0, 0);
                System.out.println("   Result: " + count + " (should be 1, currently returns 0)");
                if (count == 0) {
                    System.out.println("   ❌ CONFIRMED: Double-quoted strings in WHERE clauses don't work");
                }
            } catch (Exception e) {
                System.out.println("   Error: " + e.getMessage());
            }
        } else {
            System.out.println("   Query failed: " + doubleQuoteResult.getError().getMessage());
        }
        
        // Test what the double-quoted string is being interpreted as
        System.out.println("\n4. Testing if double quotes are parsed as identifiers:");
        
        // Try using "admin" as a column name instead of a string literal
        framework.mockTable("TestIdentifier")
            .withStringColumn("admin")  // Column named "admin"
            .withRow("test_value")
            .build();
            
        var identifierTest = framework.executeQuery("@SELECT \"admin\" FROM TestIdentifier");
        System.out.println("   Query: @SELECT \"admin\" FROM TestIdentifier");
        if (identifierTest.isSuccess()) {
            try {
                var result = identifierTest.getTable().getString(0, 0);
                System.out.println("   Result: '" + result + "'");
                if ("test_value".equals(result)) {
                    System.out.println("   ✅ Double quotes ARE being parsed as identifiers (column names)");
                } else {
                    System.out.println("   ❓ Unexpected result for identifier test");
                }
            } catch (Exception e) {
                System.out.println("   Error: " + e.getMessage());
            }
        } else {
            System.out.println("   Query failed: " + identifierTest.getError().getMessage());
        }
        
        System.out.println("\n" + "=".repeat(60));
        System.out.println("DIAGNOSIS:");
        System.out.println("The issue is that double-quoted strings are being parsed as");
        System.out.println("IDENTIFIERS (column/table names) instead of STRING LITERALS.");
        System.out.println("This is why role = \"admin\" doesn't match role = 'admin'");
        System.out.println("=".repeat(60));
    }
}
