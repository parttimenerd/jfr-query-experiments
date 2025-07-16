package me.bechberger.jfr.extended.engine.framework;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import me.bechberger.jfr.extended.table.SingleCellTable;
import me.bechberger.jfr.extended.table.CellValue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test demonstrating the usage of optimized SingleCellTable in QueryTestFramework.
 * Shows how to replace manual JfrTable creation with efficient SingleCellTable.
 */
public class SingleCellTableOptimizationTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void testSingleCellFactoryMethods() {
        // Test the factory methods for common patterns
        
        // Temporary result
        SingleCellTable tempTable = QueryTestFramework.SingleCellFactory.temp("test_value");
        assertEquals("temp", tempTable.getColumns().get(0).name());
        assertEquals("test_value", tempTable.getValue().toString());
        
        // Result table
        SingleCellTable resultTable = QueryTestFramework.SingleCellFactory.result(42);
        assertEquals("result", resultTable.getColumns().get(0).name());
        assertEquals("42", resultTable.getValue().toString());
        
        // Count table
        SingleCellTable countTable = QueryTestFramework.SingleCellFactory.count(100L);
        assertEquals("count", countTable.getColumns().get(0).name());
        assertEquals("100", countTable.getValue().toString());
        
        // Sum table
        SingleCellTable sumTable = QueryTestFramework.SingleCellFactory.sum(250.5);
        assertEquals("sum", sumTable.getColumns().get(0).name());
        assertEquals("250.5", sumTable.getValue().toString());
        
        // Average table
        SingleCellTable avgTable = QueryTestFramework.SingleCellFactory.avg(12.75);
        assertEquals("avg", avgTable.getColumns().get(0).name());
        assertEquals("12.75", avgTable.getValue().toString());
        
        // Custom table
        SingleCellTable customTable = QueryTestFramework.SingleCellFactory.custom("duration", "5ms");
        assertEquals("duration", customTable.getColumns().get(0).name());
        assertEquals("5ms", customTable.getValue().toString());
    }
    
    @Test
    void testFrameworkIntegration() {
        // Test the framework integration methods
        
        // Create and register a single-cell table
        framework.createSingleCellTable("TestTable", "value", "Hello");
        var result = framework.executeQuery("@SELECT * FROM TestTable");
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRowCount());
        assertEquals("Hello", result.getTable().getCell(0, 0).toString());
        
        // Create temporary single-cell table
        framework.createTempSingleCell("Temporary");
        var tempResult = framework.executeQuery("@SELECT * FROM temp");
        assertTrue(tempResult.isSuccess());
        assertEquals("Temporary", tempResult.getTable().getCell(0, 0).toString());
        
        // Create numeric result
        framework.createNumericResult("NumTable", 99.9);
        var numResult = framework.executeQuery("@SELECT * FROM NumTable");
        assertTrue(numResult.isSuccess());
        assertEquals("99.9", numResult.getTable().getCell(0, 0).toString());
        
        // Create string result
        framework.createStringResult("StrTable", "Success");
        var strResult = framework.executeQuery("@SELECT * FROM StrTable");
        assertTrue(strResult.isSuccess());
        assertEquals("Success", strResult.getTable().getCell(0, 0).toString());
        
        // Create boolean result
        framework.createBooleanResult("BoolTable", true);
        var boolResult = framework.executeQuery("@SELECT * FROM BoolTable");
        assertTrue(boolResult.isSuccess());
        assertEquals("true", boolResult.getTable().getCell(0, 0).toString());
    }
    
    @Test
    void testTypePreservation() {
        // Test that type information is preserved correctly
        
        CellValue booleanValue = new CellValue.BooleanValue(true);
        framework.createSingleCellTable("TypedTable", "enabled", booleanValue);
        
        var result = framework.executeQuery("@SELECT * FROM TypedTable");
        assertTrue(result.isSuccess());
        
        CellValue retrievedValue = result.getTable().getCell(0, 0);
        assertTrue(retrievedValue instanceof CellValue.BooleanValue);
        assertEquals(true, ((CellValue.BooleanValue) retrievedValue).value());
    }
    
    @Test
    void testSingleValueResultPatterns() {
        // Test common patterns for single-value results
        
        // Aggregate result pattern
        framework.createSingleValueResult("AggResult", "total_count", 1000);
        var aggResult = framework.executeQuery("@SELECT * FROM AggResult");
        assertTrue(aggResult.isSuccess());
        assertEquals("total_count", aggResult.getTable().getColumns().get(0).name());
        assertEquals("1000", aggResult.getTable().getCell(0, 0).toString());
        
        // Mathematical operation result
        framework.createNumericResult("MathResult", 3.14159);
        var mathResult = framework.executeQuery("@SELECT * FROM MathResult");
        assertTrue(mathResult.isSuccess());
        assertEquals("result", mathResult.getTable().getColumns().get(0).name());
        assertEquals("3.14159", mathResult.getTable().getCell(0, 0).toString());
    }
    
    @Test
    void testOptimizationBenefits() {
        // Demonstrate the optimization benefits
        
        // Instead of manual JfrTable creation:
        // JfrTable singleRowTable = new StandardJfrTable(List.of(new JfrTable.Column("temp", CellType.STRING)));
        // singleRowTable.addRow(new JfrTable.Row(List.of(new CellValue.StringValue("test"))));
        
        // Use optimized SingleCellTable:
        SingleCellTable optimizedTable = QueryTestFramework.SingleCellFactory.temp("test");
        
        // Verify it works the same
        assertEquals(1, optimizedTable.getRowCount());
        assertEquals(1, optimizedTable.getColumnCount());
        assertEquals("temp", optimizedTable.getColumns().get(0).name());
        assertEquals("test", optimizedTable.getValue().toString());
        
        // But it's more efficient and cleaner
        assertTrue(optimizedTable instanceof SingleCellTable);
    }
}
