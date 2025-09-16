package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.engine.framework.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Simple debug test for CROSS JOIN
 */
public class DebugCrossJoin {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void debugSimpleCrossJoin() {
        framework.createTable("A", """
            col1
            X
            """);
            
        framework.createTable("B", """
            col2
            Y
            """);
            
        QueryResult result = framework.executeQuery("@SELECT * FROM A CROSS JOIN B");
        System.out.println("Success: " + result.isSuccess());
        if (result.isSuccess()) {
            System.out.println("Row count: " + result.getTable().getRowCount());
            System.out.println("Column count: " + result.getTable().getColumns().size());
            for (int i = 0; i < result.getTable().getColumns().size(); i++) {
                System.out.println("Column " + i + ": " + result.getTable().getColumns().get(i).name());
            }
            System.out.println("Table content:");
            for (int i = 0; i < result.getTable().getRowCount(); i++) {
                System.out.println("Row " + i + ": " + result.getTable().getRows().get(i));
            }
        } else {
            System.out.println("Error: " + result.getError().getMessage());
        }
    }
}
