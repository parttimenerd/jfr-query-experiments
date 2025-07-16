package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.SingleCellTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

/**
 * Demonstration of SingleCellTable optimization patterns.
 * 
 * This class shows how to replace manual JfrTable creation for single-cell results
 * with optimized SingleCellTable usage and convenient utility methods.
 * 
 * @author SingleCellTable Optimization Demo
 * @since 1.0
 */
public class SingleCellTableOptimizationExample {

    /**
     * Test demonstrating the old manual approach vs the new optimized approach.
     */
    @Test
    @DisplayName("SingleCellTable optimization - before and after")
    public void demonstrateSingleCellTableOptimization() {
        
        // ========== BEFORE: Manual JfrTable creation ==========
        System.out.println("=== BEFORE: Manual JfrTable creation ===");
        
        // Old way - manually creating JfrTable for single values
        JfrTable oldSingleRowTable = new StandardJfrTable(List.of(new JfrTable.Column("temp", CellType.STRING)));
        oldSingleRowTable.addRow(new JfrTable.Row(List.of(new CellValue.StringValue("TestValue"))));
        System.out.println("Manual creation result: " + oldSingleRowTable);
        
        JfrTable oldResultTable = new StandardJfrTable(List.of(new JfrTable.Column("result", CellType.NUMBER)));
        oldResultTable.addRow(new JfrTable.Row(List.of(new CellValue.NumberValue(42.0))));
        System.out.println("Manual result table: " + oldResultTable);
        
        // ========== AFTER: Optimized SingleCellTable creation ==========
        System.out.println("\n=== AFTER: Optimized SingleCellTable creation ===");
        
        // New way - using SingleCellTable factory methods
        SingleCellTable newTempTable = SingleCellTable.of("temp", "TestValue");
        System.out.println("Optimized temp table: " + newTempTable);
        
        SingleCellTable newResultTable = SingleCellTable.of("result", 42.0);
        System.out.println("Optimized result table: " + newResultTable);
        
        // ========== AFTER: Using SingleCellFactory for common patterns ==========
        System.out.println("\n=== AFTER: Using SingleCellFactory for common patterns ===");
        
        // Using the factory for common patterns
        SingleCellTable tempFactory = QueryTestFramework.SingleCellFactory.temp("TestValue");
        System.out.println("Factory temp table: " + tempFactory);
        
        SingleCellTable resultFactory = QueryTestFramework.SingleCellFactory.result(42.0);
        System.out.println("Factory result table: " + resultFactory);
        
        SingleCellTable countFactory = QueryTestFramework.SingleCellFactory.count(100L);
        System.out.println("Factory count table: " + countFactory);
        
        SingleCellTable avgFactory = QueryTestFramework.SingleCellFactory.avg(85.5);
        System.out.println("Factory avg table: " + avgFactory);
        
        // ========== AFTER: Using QueryTestFramework utilities ==========
        System.out.println("\n=== AFTER: Using QueryTestFramework utilities ===");
        
        QueryTestFramework framework = new QueryTestFramework();
        
        // Register single-cell tables with the framework
        framework.createSingleCellTable("MyTemp", "temp", "TestValue");
        framework.createSingleCellTable("MyResult", "result", 42.0);
        framework.createTempSingleCell("QuickValue");
        framework.createDefaultResult("DefaultTable");
        framework.createEmptyResult("EmptyTable");
        
        System.out.println("Framework methods create and register tables automatically!");
        
        // ========== Performance and Memory Benefits ==========
        System.out.println("\n=== Performance and Memory Benefits ===");
        System.out.println("✓ Reduced object creation overhead");
        System.out.println("✓ Optimized memory usage for single-cell tables");
        System.out.println("✓ Cleaner, more readable test code");
        System.out.println("✓ Type-safe value handling");
        System.out.println("✓ Consistent API across the framework");
    }
    
    /**
     * Test demonstrating different value types with SingleCellTable.
     */
    @Test
    @DisplayName("SingleCellTable with different value types")
    public void demonstrateValueTypes() {
        System.out.println("=== SingleCellTable with different value types ===");
        
        // String values
        SingleCellTable stringTable = SingleCellTable.ofString("Hello World");
        System.out.println("String table: " + stringTable);
        
        // Numeric values
        SingleCellTable numberTable = SingleCellTable.ofNumber(123.45);
        System.out.println("Number table: " + numberTable);
        
        // Boolean values
        SingleCellTable booleanTable = SingleCellTable.ofBoolean(true);
        System.out.println("Boolean table: " + booleanTable);
        
        // Null values
        SingleCellTable nullTable = SingleCellTable.ofNull();
        System.out.println("Null table: " + nullTable);
        
        // Custom column names
        SingleCellTable customTable = SingleCellTable.of("customColumn", "Custom Value");
        System.out.println("Custom column table: " + customTable);
    }
    
    /**
     * Test demonstrating common aggregation patterns.
     */
    @Test
    @DisplayName("Common aggregation result patterns")
    public void demonstrateAggregationPatterns() {
        System.out.println("=== Common aggregation result patterns ===");
        
        // Before: Manual aggregation result creation
        System.out.println("BEFORE - Manual creation:");
        JfrTable manualCount = new StandardJfrTable(List.of(new JfrTable.Column("count", CellType.NUMBER)));
        manualCount.addRow(new JfrTable.Row(List.of(new CellValue.NumberValue(50L))));
        System.out.println("Manual count: " + manualCount);
        
        JfrTable manualSum = new StandardJfrTable(List.of(new JfrTable.Column("sum", CellType.NUMBER)));
        manualSum.addRow(new JfrTable.Row(List.of(new CellValue.NumberValue(1250.75))));
        System.out.println("Manual sum: " + manualSum);
        
        // After: Using SingleCellFactory
        System.out.println("\nAFTER - Using SingleCellFactory:");
        SingleCellTable optimizedCount = QueryTestFramework.SingleCellFactory.count(50L);
        System.out.println("Optimized count: " + optimizedCount);
        
        SingleCellTable optimizedSum = QueryTestFramework.SingleCellFactory.sum(1250.75);
        System.out.println("Optimized sum: " + optimizedSum);
        
        SingleCellTable optimizedAvg = QueryTestFramework.SingleCellFactory.avg(25.015);
        System.out.println("Optimized avg: " + optimizedAvg);
        
        // Custom aggregation results
        SingleCellTable customAgg = QueryTestFramework.SingleCellFactory.custom("median", 22.5);
        System.out.println("Custom aggregation: " + customAgg);
    }
}
