package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;

public class SimpleDebugColumnAccess {
    public static void main(String[] args) {
        try {
            var framework = new QueryTestFramework();
            
            // Create simple test data
            framework.mockTable("Events")
                .withStringColumn("name")
                .withStringColumn("type")
                .withRow("Alice", "user")
                .withRow("Bob", "admin")
                .build();
            
            // Test simple SELECT
            System.out.println("=== Testing Simple SELECT ===");
            var result = framework.executeQuery("@SELECT name FROM Events");
            System.out.println("Success: " + result.isSuccess());
            if (result.isSuccess()) {
                var table = result.getTable();
                System.out.println("Rows: " + table.getRowCount());
                System.out.println("Columns: " + table.getColumnCount());
                for (int c = 0; c < table.getColumnCount(); c++) {
                    System.out.println("Column " + c + ": '" + table.getColumns().get(c).name() + "'");
                }
                
                // Test different ways to access data
                System.out.println("Row 0, Column 0 (direct): " + table.getCell(0, 0));
                System.out.println("Row 0, Column 'name' (string): " + table.getString(0, "name"));
                
                // Check if the table has the 'name' column
                boolean hasNameColumn = table.getColumns().stream()
                    .anyMatch(col -> "name".equals(col.name()));
                System.out.println("Has 'name' column: " + hasNameColumn);
            } else {
                System.out.println("Error: " + result.getError().getMessage());
                result.getError().printStackTrace();
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
