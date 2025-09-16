package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for AggregationPlan extractColumnName enhancement.
 * Tests the new behavior where anonymous columns are named as $0, $1, $2, etc.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class AggregationPlanColumnNamingTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Create test data for aggregation
        framework.mockTable("TestData", """
            value | category
            10 | A
            20 | B
            30 | A
            40 | B
            50 | C
            """);
    }
    
    @Test
    @DisplayName("Anonymous expressions should be named $0, $1, $2, etc.")
    void testAnonymousColumnNaming() {
        var result = framework.executeQuery("@SELECT value + 5, value * 2, value / 2 FROM TestData WHERE value = 10");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check that anonymous expressions are named $0, $1, $2
        assertEquals(3, columns.size());
        assertEquals("$0", columns.get(0).name());
        assertEquals("$1", columns.get(1).name());
        assertEquals("$2", columns.get(2).name());
        
        // Check values
        var rows = table.getRows();
        assertEquals(1, rows.size());
        var row = rows.get(0);
        assertEquals(15.0, ((Number) row.getCells().get(0).getValue()).doubleValue());
        assertEquals(20.0, ((Number) row.getCells().get(1).getValue()).doubleValue());
        assertEquals(5.0, ((Number) row.getCells().get(2).getValue()).doubleValue());
    }
    
    @Test
    @DisplayName("Mixed named and anonymous columns")
    void testMixedNamedAndAnonymousColumns() {
        var result = framework.executeQuery("@SELECT value as original, value + 5, category, value * 2 FROM TestData WHERE value = 20");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check column names
        assertEquals(4, columns.size());
        assertEquals("original", columns.get(0).name());    // aliased
        assertEquals("$1", columns.get(1).name());          // anonymous expression
        assertEquals("category", columns.get(2).name());    // field name
        assertEquals("$3", columns.get(3).name());          // anonymous expression
    }
    
    @Test
    @DisplayName("Function calls should retain function names")
    void testFunctionCallNaming() {
        var result = framework.executeQuery("@SELECT COUNT(*), SUM(value), AVG(value) FROM TestData");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check that function calls retain their names
        assertEquals(3, columns.size());
        assertEquals("COUNT(*)", columns.get(0).name());
        assertEquals("SUM(...)", columns.get(1).name());
        assertEquals("AVG(...)", columns.get(2).name());
    }
    
    @Test
    @DisplayName("Field access should retain field names")
    void testFieldAccessNaming() {
        var result = framework.executeQuery("@SELECT value, category FROM TestData WHERE value = 30");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check that field access retains field names
        assertEquals(2, columns.size());
        assertEquals("value", columns.get(0).name());
        assertEquals("category", columns.get(1).name());
    }
    
    @Test
    @DisplayName("Complex expressions with aliases")
    void testComplexExpressionsWithAliases() {
        var result = framework.executeQuery("@SELECT value + 10 as increased, value * value as squared, ABS(value - 25) as distance FROM TestData WHERE value = 20");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check that aliases are used properly
        assertEquals(3, columns.size());
        assertEquals("increased", columns.get(0).name());
        assertEquals("squared", columns.get(1).name());
        assertEquals("distance", columns.get(2).name());
    }
    
    @Test
    @DisplayName("Aggregate functions with complex expressions")
    void testAggregateWithComplexExpressions() {
        var result = framework.executeQuery("@SELECT COUNT(*), SUM(value * 2), AVG(value + 5) FROM TestData WHERE category = 'A'");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check column names: function names for aggregates, $N for anonymous expressions
        assertEquals(3, columns.size());
        assertEquals("COUNT(*)", columns.get(0).name());
        assertEquals("SUM(...)", columns.get(1).name());
        assertEquals("AVG(...)", columns.get(2).name());   // aggregate expression
    }
    
    @Test
    @DisplayName("Simple arithmetic expressions should be named $N")
    void testArithmeticExpressions() {
        var result = framework.executeQuery("@SELECT duration + 2, value * 1.5, value - 10 FROM TestData WHERE value = 30");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check that arithmetic expressions are named $0, $1, $2
        assertEquals(3, columns.size());
        assertEquals("$0", columns.get(0).name());
        assertEquals("$1", columns.get(1).name());
        assertEquals("$2", columns.get(2).name());
    }
    
    @Test
    @DisplayName("Parenthesized expressions should be named $N")
    void testParenthesizedExpressions() {
        var result = framework.executeQuery("@SELECT (value + 5) * 2, (value - 10) / 2 FROM TestData WHERE value = 20");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check that parenthesized expressions are named $0, $1
        assertEquals(2, columns.size());
        assertEquals("$0", columns.get(0).name());
        assertEquals("$1", columns.get(1).name());
    }
    
    @Test
    @DisplayName("Non-aggregate function calls should be named $N")
    void testNonAggregateFunctionCalls() {
        var result = framework.executeQuery("@SELECT ABS(value), SQRT(value), CEIL(value / 10.0) FROM TestData WHERE value = 25");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check that non-aggregate function calls are treated as anonymous expressions
        assertEquals(3, columns.size());
        assertEquals("$0", columns.get(0).name());
        assertEquals("$1", columns.get(1).name());
        assertEquals("$2", columns.get(2).name());
    }
    
    @Test
    @DisplayName("Mixed field access and expressions")
    void testMixedFieldAccessAndExpressions() {
        var result = framework.executeQuery("@SELECT category, value + 10, value * 2, category FROM TestData WHERE value = 20");
        assertTrue(result.isSuccess());
        
        var table = result.getTable();
        var columns = table.getColumns();
        
        // Check column names: field names for fields, $N for expressions
        assertEquals(4, columns.size());
        assertEquals("category", columns.get(0).name());    // field access
        assertEquals("$1", columns.get(1).name());          // expression
        assertEquals("$2", columns.get(2).name());          // expression
        assertEquals("category", columns.get(3).name());    // field access (duplicate)
    }
}
