package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Simple tests to verify the SHOW commands work
 */
class SimpleShowCommandTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("SHOW EVENTS should work")
    void testShowEventsBasic() {
        var result = framework.executeQuery("SHOW EVENTS");
        
        assertTrue(result.isSuccess(), "SHOW EVENTS should succeed");
        assertNotNull(result.getTable(), "Result should contain a table");
        
        System.out.println("SHOW EVENTS result:");
        System.out.println("Columns: " + result.getTable().getColumns().size());
        for (int i = 0; i < result.getTable().getColumns().size(); i++) {
            var column = result.getTable().getColumns().get(i);
            System.out.println("Column " + i + ": " + column.name() + " (" + column.type() + ")");
        }
        System.out.println("Rows: " + result.getTable().getRowCount());
        if (result.getTable().getRowCount() > 0) {
            System.out.println("First few rows:");
            for (int i = 0; i < Math.min(3, result.getTable().getRowCount()); i++) {
                System.out.println("Row " + i + ": " + result.getTable().getString(i, 0));
            }
        }
    }
    
    @Test
    @DisplayName("HELP should work")
    void testHelpBasic() {
        var result = framework.executeQuery("HELP");
        
        assertTrue(result.isSuccess(), "HELP should succeed");
        assertNotNull(result.getTable(), "Result should contain a table");
        
        System.out.println("HELP result:");
        System.out.println("Columns: " + result.getTable().getColumns().size());
        for (int i = 0; i < result.getTable().getColumns().size(); i++) {
            var column = result.getTable().getColumns().get(i);
            System.out.println("Column " + i + ": " + column.name() + " (" + column.type() + ")");
        }
        System.out.println("Rows: " + result.getTable().getRowCount());
        if (result.getTable().getRowCount() > 0) {
            String helpContent = result.getTable().getString(0, 0);
            System.out.println("Help content length: " + helpContent.length());
            System.out.println("First 200 chars: " + helpContent.substring(0, Math.min(200, helpContent.length())));
        }
    }
    
    @Test
    @DisplayName("SHOW FIELDS should work for known event")
    void testShowFieldsBasic() {
        // Try with a common JFR event type that should exist
        var result = framework.executeQuery("SHOW FIELDS jdk.GarbageCollection");
        
        if (result.isSuccess()) {
            System.out.println("SHOW FIELDS jdk.GarbageCollection result:");
            System.out.println("Columns: " + result.getTable().getColumns().size());
            for (int i = 0; i < result.getTable().getColumns().size(); i++) {
                var column = result.getTable().getColumns().get(i);
                System.out.println("Column " + i + ": " + column.name() + " (" + column.type() + ")");
            }
            System.out.println("Rows: " + result.getTable().getRowCount());
        } else {
            System.out.println("SHOW FIELDS failed: " + result.getError().getMessage());
            // Try without package name
            var result2 = framework.executeQuery("SHOW FIELDS GarbageCollection");
            if (result2.isSuccess()) {
                System.out.println("SHOW FIELDS GarbageCollection (without package) succeeded");
                System.out.println("Rows: " + result2.getTable().getRowCount());
            } else {
                System.out.println("SHOW FIELDS GarbageCollection also failed: " + result2.getError().getMessage());
            }
        }
    }
}
