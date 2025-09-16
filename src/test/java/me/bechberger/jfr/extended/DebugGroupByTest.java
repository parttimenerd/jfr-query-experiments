package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryResult;
import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Debug test to understand what's being returned by GROUP BY queries
 */
@DisplayName("Debug GROUP BY Test")
public class DebugGroupByTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("Debug GROUP BY query results")
    void testDebugGroupBy() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            David | Sales | 70000
            Eve | Engineering | 85000
            """);
        
        QueryResult result = framework.executeQuery("@SELECT department, COUNT(*) as count FROM Users GROUP BY department");
        
        System.out.println("Result columns: " + result.getTable().getColumns().stream().map(c -> c.name()).toList());
        System.out.println("Result rows: " + result.getTable().getRowCount());
        for (int i = 0; i < result.getTable().getRowCount(); i++) {
            System.out.println("Row " + i + ": " + result.getTable().getRows().get(i).getCells());
        }
    }
    
    @Test
    @DisplayName("Debug simple SELECT query")
    void testDebugSimpleSelect() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            """);
        
        QueryResult result = framework.executeQuery("@SELECT department, COUNT(*) as count FROM Users");
        
        if (!result.isSuccess()) {
            System.err.println("Query failed: " + result.getError().getMessage());
            fail("Query execution failed: " + result.getError().getMessage());
            return;
        }
        
        System.out.println("Simple result columns: " + result.getTable().getColumns().stream().map(c -> c.name()).toList());
        System.out.println("Simple result rows: " + result.getTable().getRowCount());
        for (int i = 0; i < result.getTable().getRowCount(); i++) {
            System.out.println("Row " + i + ": " + result.getTable().getRows().get(i).getCells());
        }
    }
}
