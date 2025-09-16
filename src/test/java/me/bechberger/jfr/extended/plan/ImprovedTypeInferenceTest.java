package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for improved dynamic type inference in aggregate functions.
 * 
 * This test verifies that the column types are correctly inferred
 * from the FunctionRegistry defaults and actual return values.
 */
class ImprovedTypeInferenceTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    @DisplayName("Aggregate functions should use registry defaults for empty tables")
    void testEmptyTableUsesDefaults() {
        framework.createTable("Empty", """
            value
            """);
        
        // Test with empty table - should use FunctionRegistry defaults
        var result = framework.executeAndAssertSuccess("@SELECT COUNT(*), SUM(value), COLLECT(value) FROM Empty");
        JfrTable table = result.getTable();
        
        assertEquals(3, table.getColumns().size());
        assertEquals(CellType.NUMBER, table.getColumns().get(0).type()); // COUNT(*) -> NUMBER
        assertEquals(CellType.NULL, table.getColumns().get(1).type());   // SUM -> NULL (no data to compute)
        assertEquals(CellType.ARRAY, table.getColumns().get(2).type());  // COLLECT -> ARRAY
        
        // COUNT(*) should return 0 for empty table
        assertEquals(0L, table.getNumber(0, 0));
        // SUM should return NULL for empty data
        assertTrue(table.getRows().get(0).getCells().get(1) instanceof CellValue.NullValue);
        // COLLECT should return empty array
        assertEquals(0, table.getArray(0, 2).size());
    }
    
    @Test
    @DisplayName("Aggregate functions should use actual return types for data")
    void testDataDrivenTypes() {
        framework.createTable("Numbers", """
            value
            10
            20
            30
            """);
        
        var result = framework.executeAndAssertSuccess("@SELECT COUNT(*), SUM(value), AVG(value), COLLECT(value) FROM Numbers");
        JfrTable table = result.getTable();
        
        assertEquals(4, table.getColumns().size());
        assertEquals(CellType.NUMBER, table.getColumns().get(0).type()); // COUNT(*) -> NUMBER
        assertEquals(CellType.NUMBER, table.getColumns().get(1).type()); // SUM(integers) -> NUMBER
        assertEquals(CellType.NUMBER, table.getColumns().get(2).type()); // AVG(integers) -> NUMBER (maintains input type)
        assertEquals(CellType.ARRAY, table.getColumns().get(3).type());  // COLLECT -> ARRAY
        
        // Verify the actual values
        assertEquals(3L, table.getNumber(0, 0));
        assertEquals(60L, table.getNumber(0, 1));
        assertEquals(20L, table.getNumber(0, 2)); // AVG result as NUMBER
        var collected = table.getArray(0, 3);
        assertEquals(3, collected.size());
    }
    
    @Test
    @DisplayName("GROUP BY should use sample-based type inference")
    void testGroupByTypeInference() {
        framework.createTable("Sales", """
            region | amount
            North | 100
            South | 200
            North | 150
            """);
        
        var result = framework.executeAndAssertSuccess(
            "@SELECT region, COUNT(*), SUM(amount), AVG(amount) FROM Sales GROUP BY region");
        JfrTable table = result.getTable();
        
        assertEquals(4, table.getColumns().size());
        assertEquals(CellType.STRING, table.getColumns().get(0).type()); // region
        assertEquals(CellType.NUMBER, table.getColumns().get(1).type()); // COUNT(*)
        assertEquals(CellType.NUMBER, table.getColumns().get(2).type()); // SUM(amount)
        assertEquals(CellType.NUMBER, table.getColumns().get(3).type()); // AVG(amount) maintains input type
    }
    
    @Test
    @DisplayName("Mixed types should work correctly")
    void testMixedTypes() {
        framework.createTable("Mixed", """
            name | score
            Alice | 95.5
            Bob | 87.2
            """);
        
        var result = framework.executeAndAssertSuccess("@SELECT MIN(score), MAX(score), COLLECT(name) FROM Mixed");
        JfrTable table = result.getTable();
        
        assertEquals(3, table.getColumns().size());
        assertEquals(CellType.NUMBER, table.getColumns().get(0).type());  // MIN(float) -> NUMBER
        assertEquals(CellType.NUMBER, table.getColumns().get(1).type());  // MAX(float) -> NUMBER
        assertEquals(CellType.ARRAY, table.getColumns().get(2).type());  // COLLECT -> ARRAY
    }
}
