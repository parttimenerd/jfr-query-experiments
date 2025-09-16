package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Debug CASE evaluation by tracing what values are being compared
 */
public class DebugCaseEvalTrace {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Test
    void traceCaseEvaluation() {
        // Create simple table to trace CASE evaluation
        framework.createTable("Simple", """
            id | value
            1 | 10
            2 | 5
            """);
        
        System.out.println("=== Tracing basic CASE evaluation ===");
        
        // Test simple CASE with different conditions per row
        var result = framework.executeAndAssertSuccess("""
            @SELECT 
                id,
                value,
                CASE 
                    WHEN value > 7 THEN 'Big' 
                    WHEN value > 3 THEN 'Medium'
                    ELSE 'Small' 
                END as size_category
            FROM Simple
            """);
        
        var table = result.getTable();
        for (int i = 0; i < table.getRowCount(); i++) {
            System.out.printf("Row %d: id=%s, value=%s, size_category=%s%n", 
                i, 
                table.getCell(i, "id"),
                table.getCell(i, "value"), 
                table.getCell(i, "size_category"));
        }
    }
    
    @Test  
    void traceCaseWithLiterals() {
        // Create simple table to trace CASE evaluation
        framework.createTable("Test", """
            id
            1
            2
            """);
        
        System.out.println("=== Tracing CASE with pure literals ===");
        
        // Test CASE with pure literal conditions
        var result = framework.executeAndAssertSuccess("""
            @SELECT 
                id,
                CASE 
                    WHEN 5 > 4 THEN 'High'
                    WHEN 2 > 1 THEN 'Medium'  
                    ELSE 'Low' 
                END as literal_case
            FROM Test
            """);
        
        var table = result.getTable();
        for (int i = 0; i < table.getRowCount(); i++) {
            System.out.printf("Row %d: id=%s, literal_case=%s%n", 
                i, 
                table.getCell(i, "id"),
                table.getCell(i, "literal_case"));
        }
        
        // Expected: Both rows should show "High" since 5 > 4 is always true
        // If both show "High", then CASE is working correctly with literals
    }
}
