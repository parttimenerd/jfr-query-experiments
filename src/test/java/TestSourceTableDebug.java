import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;

public class TestSourceTableDebug {
    public static void main(String[] args) {
        QueryTestFramework framework = new QueryTestFramework();
        
        // Create a simple table
        framework.createTable("DebugTable", """
            id | name
            1 | Alice
            2 | Bob
            """);
            
        System.out.println("=== DEBUG: Testing simple single table queries ===");
        
        // Test 1: Simple SELECT * from single table
        var result1 = framework.executeQuery("@SELECT * FROM DebugTable");
        if (result1.isSuccess()) {
            var table1 = result1.getTable();
            System.out.println("Single table SELECT * - Columns: " + table1.getColumnCount());
            for (int i = 0; i < table1.getColumnCount(); i++) {
                System.out.println("  Column " + i + ": " + table1.getColumns().get(i).name());
            }
        }
        
        // Test 2: Simple SELECT specific columns from single table
        var result2 = framework.executeQuery("@SELECT id, name FROM DebugTable");
        if (result2.isSuccess()) {
            var table2 = result2.getTable();
            System.out.println("Single table SELECT id, name - Columns: " + table2.getColumnCount());
            for (int i = 0; i < table2.getColumnCount(); i++) {
                System.out.println("  Column " + i + ": " + table2.getColumns().get(i).name());
            }
        }
    }
}
