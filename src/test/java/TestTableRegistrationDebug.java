import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;

public class TestTableRegistrationDebug {
    public static void main(String[] args) {
        QueryTestFramework framework = new QueryTestFramework();
        
        // Create a simple table
        framework.createTable("DebugTable", """
            id | name
            1 | Alice
            2 | Bob
            """);
            
        System.out.println("=== DEBUG: Check what gets registered ===");
        
        // Use the raw executor directly to see what table gets returned
        // FIXME: getMockExecutor() method is not available
        // var mockExecutor = framework.getMockExecutor();
        
        /*
        try {
            // Test what the raw executor returns for the table
            var rawResult = mockExecutor.execute(new me.bechberger.jfr.extended.ast.ASTNodes.RawJfrQueryNode("SELECT * FROM DebugTable", null));
            System.out.println("Raw executor SELECT * result:");
            System.out.println("  Columns: " + rawResult.getColumnCount());
            for (int i = 0; i < rawResult.getColumnCount(); i++) {
                System.out.println("    Column " + i + ": " + rawResult.getColumns().get(i).name());
            }
            
            var rawResult2 = mockExecutor.execute(new me.bechberger.jfr.extended.ast.ASTNodes.RawJfrQueryNode("SELECT id, name FROM DebugTable", null));
            System.out.println("Raw executor SELECT id, name result:");
            System.out.println("  Columns: " + rawResult2.getColumnCount());
            for (int i = 0; i < rawResult2.getColumnCount(); i++) {
                System.out.println("    Column " + i + ": " + rawResult2.getColumns().get(i).name());
            }
            
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
        */
    }
}
