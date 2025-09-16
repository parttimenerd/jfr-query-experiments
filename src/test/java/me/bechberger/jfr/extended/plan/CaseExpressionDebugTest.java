package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.engine.framework.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Debug test for CASE expression issues
 */
class CaseExpressionDebugTest {

    private QueryTestFramework framework;

    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }

    @Test
    void testSimpleCaseExpressionInSelect() {
        framework.mockTable("TestTable")
            .withStringColumn("name")
            .withNumberColumn("value")
            .withRow("Alice", 10L)
            .withRow("Bob", 20L)
            .build();

        QueryResult result = framework.executeQuery("""
            @SELECT name, value,
                   CASE 
                       WHEN value < 15 THEN 'Low'
                       ELSE 'High'
                   END as category
            FROM TestTable
            """);
        
        System.out.println("Query result success: " + result.isSuccess());
        if (!result.isSuccess()) {
            System.out.println("Error: " + result.getError().getMessage());
        } else {
            System.out.println("Table rows: " + result.getTable().getRowCount());
            for (int i = 0; i < result.getTable().getRowCount(); i++) {
                System.out.println("Row " + i + ": name=" + result.getTable().getString(i, "name") + 
                                   ", value=" + result.getTable().getNumber(i, "value") + 
                                   ", category=" + result.getTable().getString(i, "category"));
            }
        }
        
        assertTrue(result.isSuccess());
        assertEquals(2, result.getTable().getRowCount());
    }

    @Test
    void testCaseExpressionInWhere() {
        framework.mockTable("TestTable")
            .withStringColumn("name")
            .withNumberColumn("value")
            .withRow("Alice", 10L)
            .withRow("Bob", 20L)
            .build();

        QueryResult result = framework.executeQuery("""
            @SELECT name, value
            FROM TestTable
            WHERE CASE 
                      WHEN value < 15 THEN true
                      ELSE false
                  END
            """);
        
        System.out.println("WHERE Query result success: " + result.isSuccess());
        if (!result.isSuccess()) {
            System.out.println("Error: " + result.getError().getMessage());
        } else {
            System.out.println("Table rows: " + result.getTable().getRowCount());
            for (int i = 0; i < result.getTable().getRowCount(); i++) {
                System.out.println("Row " + i + ": name=" + result.getTable().getString(i, "name") + 
                                   ", value=" + result.getTable().getNumber(i, "value"));
            }
        }
        
        assertTrue(result.isSuccess());
        assertEquals(1, result.getTable().getRowCount());
    }
}
