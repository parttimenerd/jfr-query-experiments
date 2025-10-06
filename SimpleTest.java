import me.bechberger.jfr.duckdb.util.SQLUtil;
import java.util.Set;

public class SimpleTest {
    public static void main(String[] args) {
        System.out.println("Testing Grammar-Based SQL Parser");
        System.out.println("================================");

        // Test basic functionality first
        testCase("SELECT * FROM table1", Set.of("table1"));
        testCase("SELECT * FROM table1, table2", Set.of("table1", "table2"));
        testCase("CREATE VIEW my_view AS SELECT * FROM users", Set.of("users"));
        testCase("SELECT * FROM Method m JOIN Class c ON m.type = c._id", Set.of("Method", "Class"));

        // Test the complex case that was failing
        String complexQuery = """
                CREATE VIEW "deprecated-methods-for-removal" AS
                    SELECT
                        (c.javaName || '.' || m.name || m.descriptor) AS "Deprecated Method",
                        list(DISTINCT (cc.javaName) ORDER BY cc.javaName) AS "Called from Class"
                    FROM DeprecatedInvocation di
                    JOIN Method m ON di.method = m._id
                    JOIN Class c ON m.type = c._id
                    JOIN Method cm ON di.stackTrace$topMethod = cm._id
                    JOIN Class cc ON cm.type = cc._id
                    WHERE forRemoval = 'true'
                    GROUP BY di.method, c.javaName, m.name, m.descriptor
                    ORDER BY c.javaName, m.name, m.descriptor
                """;
        testCase("Complex CREATE VIEW", Set.of("DeprecatedInvocation", "Method", "Class"), complexQuery);

        // Test quoted identifiers
        testCase("SELECT * FROM \"weird-table-name\", normal_table", Set.of("weird-table-name", "normal_table"));

        // Test subqueries
        testCase("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)", Set.of("users", "orders"));

        System.out.println("\nAll tests completed.");
    }

    private static void testCase(String description, Set<String> expected) {
        testCase(description, expected, description);
    }

    private static void testCase(String description, Set<String> expected, String query) {
        System.out.println("Test: " + description);
        System.out.println("Query: " + (query.length() > 80 ? query.substring(0, 80) + "..." : query));

        try {
            Set<String> result = SQLUtil.getReferencedTables(query);
            System.out.println("Expected: " + expected);
            System.out.println("Actual:   " + result);

            boolean passed = result.equals(expected);
            System.out.println("Status:   " + (passed ? "PASS" : "FAIL"));

            if (!passed) {
                Set<String> missing = new java.util.HashSet<>(expected);
                missing.removeAll(result);
                Set<String> extra = new java.util.HashSet<>(result);
                extra.removeAll(expected);

                if (!missing.isEmpty()) {
                    System.out.println("Missing:  " + missing);
                }
                if (!extra.isEmpty()) {
                    System.out.println("Extra:    " + extra);
                }
            }
        } catch (Exception e) {
            System.out.println("ERROR:    " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("---");
    }
}