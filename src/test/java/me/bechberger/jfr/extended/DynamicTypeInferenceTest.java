package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.plan.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for dynamic type inference in aggregate functions.
 * 
 * This test verifies that the column types are correctly inferred
 * from the actual return values of aggregate functions rather than
 * using hardcoded type mappings.
 */
class DynamicTypeInferenceTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("COUNT(*) should return NUMBER type")
    void testCountReturnType() {
        framework.createTable("Numbers", """
            value
            10
            20
            30
            """);
        
        var result = framework.executeAndAssertSuccess("@SELECT COUNT(*) FROM Numbers");
        JfrTable table = result.getTable();
        
        assertEquals(1, table.getColumns().size());
        assertEquals(CellType.NUMBER, table.getColumns().get(0).type());
        assertEquals(3L, table.getNumber(0, 0));
    }
    
    @Test
    @DisplayName("SUM of integers should return NUMBER type")
    void testSumIntegerReturnType() {
        framework.createTable("Numbers", """
            value
            10
            20
            30
            """);
        
        var result = framework.executeAndAssertSuccess("@SELECT SUM(value) FROM Numbers");
        JfrTable table = result.getTable();
        
        assertEquals(1, table.getColumns().size());
        assertEquals(CellType.NUMBER, table.getColumns().get(0).type());
        assertEquals(60L, table.getNumber(0, 0));
    }
    
    @Test
    @DisplayName("AVG should return NUMBER type")
    void testAverageReturnType() {
        framework.createTable("Numbers", """
            value
            10
            20
            30
            """);
        
        var result = framework.executeAndAssertSuccess("@SELECT AVG(value) FROM Numbers");
        JfrTable table = result.getTable();
        
        assertEquals(1, table.getColumns().size());
        assertEquals(CellType.NUMBER, table.getColumns().get(0).type());
        assertEquals(20.0, table.getFloat(0, 0), 0.001);
    }
    
    @Test
    @DisplayName("COLLECT should return ARRAY type")
    void testCollectReturnType() {
        framework.createTable("Names", """
            name
            Alice
            Bob
            Charlie
            """);
        
        var result = framework.executeAndAssertSuccess("@SELECT COLLECT(name) FROM Names");
        JfrTable table = result.getTable();
        
        assertEquals(1, table.getColumns().size());
        assertEquals(CellType.ARRAY, table.getColumns().get(0).type());
        var collected = table.getArray(0, 0);
        assertEquals(3, collected.size());
        assertTrue(collected.contains("Alice"));
        assertTrue(collected.contains("Bob"));
        assertTrue(collected.contains("Charlie"));
    }
    
    @Test
    @DisplayName("MIN/MAX should return same type as the data")
    void testMinMaxReturnType() {
        framework.createTable("Numbers", """
            value
            10
            20
            30
            """);
        
        var result = framework.executeAndAssertSuccess("@SELECT MIN(value), MAX(value) FROM Numbers");
        JfrTable table = result.getTable();
        
        assertEquals(2, table.getColumns().size());
        assertEquals(CellType.NUMBER, table.getColumns().get(0).type());
        assertEquals(CellType.NUMBER, table.getColumns().get(1).type());
        assertEquals(10L, table.getNumber(0, 0));
        assertEquals(30L, table.getNumber(0, 1));
    }
    
    @Test
    @DisplayName("GROUP BY with aggregates should have correct types")
    void testGroupByAggregateTypes() {
        framework.createTable("Sales", """
            region | amount
            North | 100
            South | 200
            North | 150
            South | 300
            """);
        
        var result = framework.executeAndAssertSuccess(
            "@SELECT region, COUNT(*), SUM(amount), AVG(amount) FROM Sales GROUP BY region");
        JfrTable table = result.getTable();
        
        assertEquals(4, table.getColumns().size());
        assertEquals(CellType.STRING, table.getColumns().get(0).type()); // region
        assertEquals(CellType.NUMBER, table.getColumns().get(1).type()); // COUNT(*)
        assertEquals(CellType.NUMBER, table.getColumns().get(2).type()); // SUM(amount)
        assertEquals(CellType.NUMBER, table.getColumns().get(3).type());  // AVG(amount)
    }
    
    @Test
    @DisplayName("Empty table should use fallback type inference")
    void testEmptyTableFallback() {
        framework.createTable("Empty", """
            value
            """);
        
        var result = framework.executeAndAssertSuccess("@SELECT COUNT(*), SUM(value) FROM Empty");
        JfrTable table = result.getTable();
        
        assertEquals(2, table.getColumns().size());
        assertEquals(CellType.NUMBER, table.getColumns().get(0).type()); // COUNT(*)
        assertEquals(CellType.NUMBER, table.getColumns().get(1).type());  // SUM fallback
    }
}
