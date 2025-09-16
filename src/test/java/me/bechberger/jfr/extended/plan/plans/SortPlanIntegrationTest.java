package me.bechberger.jfr.extended.plan.plans;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for ORDER BY functionality using QueryTestFramework.
 * 
 * Tests cover:
 * - Simple field sorting (ASC/DESC)
 * - Multi-field sorting with precedence
 * - Complex expression sorting
 * - Mathematical expressions in ORDER BY
 * - Function calls in ORDER BY
 * - Error handling for invalid ORDER BY clauses
 * - Integration with SELECT, WHERE, and other clauses
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class SortPlanIntegrationTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Create test data for sorting
        framework.mockTable("Events", """
            name | duration | priority | timestamp
            EventA | 100 | 1 | 1000
            EventB | 50 | 3 | 2000
            EventC | 200 | 2 | 1500
            EventD | 50 | 1 | 3000
            EventE | 150 | 2 | 2500
            """);
        
        // Create test data for complex expressions
        framework.mockTable("Numbers", """
            value | category
            10 | A
            -5 | B
            25 | A
            -15 | B
            30 | C
            """);
        
        // Create test data for functions
        framework.mockTable("Products", """
            name | price | quantity
            Apple | 1.50 | 100
            Banana | 0.80 | 50
            Cherry | 3.20 | 25
            Date | 5.00 | 10
            """);
    }
    
    // ===== BASIC SORTING TESTS =====
    
    @Test
    @DisplayName("Simple ORDER BY field ASC")
    void testSimpleOrderByAsc() {
        framework.executeAndExpectTable("@SELECT name, duration FROM Events ORDER BY duration ASC", """
            name | duration
            EventB | 50
            EventD | 50
            EventA | 100
            EventE | 150
            EventC | 200
            """);
    }
    
    @Test
    @DisplayName("Simple ORDER BY field DESC")
    void testSimpleOrderByDesc() {
        framework.executeAndExpectTable("@SELECT name, duration FROM Events ORDER BY duration DESC", """
            name | duration
            EventC | 200
            EventE | 150
            EventA | 100
            EventB | 50
            EventD | 50
            """);
    }
    
    @ParameterizedTest
    @CsvSource({
        "ASC, 'EventB,EventD,EventA,EventE,EventC'",
        "DESC, 'EventC,EventE,EventA,EventB,EventD'"
    })
    @DisplayName("Parameterized ORDER BY direction test")
    void testOrderByDirection(String direction, String expectedOrder) {
        var result = framework.executeQuery("@SELECT name, duration FROM Events ORDER BY duration " + direction);
        assertTrue(result.isSuccess());
        
        var rows = result.getTable().getRows();
        assertEquals(5, rows.size());
        
        String[] expectedNames = expectedOrder.split(",");
        for (int i = 0; i < expectedNames.length; i++) {
            assertEquals(expectedNames[i], rows.get(i).getCells().get(0).getValue());
        }
    }
    
    // ===== MULTI-FIELD SORTING TESTS =====
    
    @Test
    @DisplayName("Multi-field ORDER BY with different directions")
    void testMultiFieldOrderBy() {
        framework.executeAndExpectTable("@SELECT name, duration, priority FROM Events ORDER BY priority ASC, duration DESC", """
            name | duration | priority
            EventA | 100 | 1
            EventD | 50 | 1
            EventC | 200 | 2
            EventE | 150 | 2
            EventB | 50 | 3
            """);
    }
    
    @Test
    @DisplayName("Multi-field ORDER BY with same direction")
    void testMultiFieldOrderBySameDirection() {
        framework.executeAndExpectTable("@SELECT name, duration, priority FROM Events ORDER BY priority ASC, duration ASC", """
            name | duration | priority
            EventD | 50 | 1
            EventA | 100 | 1
            EventE | 150 | 2
            EventC | 200 | 2
            EventB | 50 | 3
            """);
    }
    
    @Test
    @DisplayName("Three-field ORDER BY")
    void testThreeFieldOrderBy() {
        framework.executeAndExpectTable("@SELECT name, duration, priority, timestamp FROM Events ORDER BY priority ASC, duration ASC, timestamp DESC", """
            name | duration | priority | timestamp
            EventD | 50 | 1 | 3000
            EventA | 100 | 1 | 1000
            EventE | 150 | 2 | 2500
            EventC | 200 | 2 | 1500
            EventB | 50 | 3 | 2000
            """);
    }
    
    // ===== EXPRESSION SORTING TESTS =====
    
    @Test
    @DisplayName("ORDER BY with mathematical expressions")
    void testOrderByMathematicalExpression() {
        framework.executeAndExpectTable("@SELECT value, category FROM Numbers ORDER BY ABS(value) ASC", """
            value | category
            -5 | B
            10 | A
            -15 | B
            25 | A
            30 | C
            """);
    }
    
    @Test
    @DisplayName("ORDER BY with arithmetic expressions")
    void testOrderByArithmeticExpression() {
        framework.executeAndExpectTable("@SELECT name, price, quantity FROM Products ORDER BY price * quantity DESC", """
            name | price | quantity
            Apple | 1.50 | 100
            Cherry | 3.20 | 25
            Date | 5.00 | 10
            Banana | 0.80 | 50
            """);
    }
    
    @Test
    @DisplayName("ORDER BY with complex expressions")
    void testOrderByComplexExpression() {
        framework.executeAndExpectTable("@SELECT value, category FROM Numbers ORDER BY (value + 10) * 2 DESC", """
            value | category
            30 | C
            25 | A
            10 | A
            -5 | B
            -15 | B
            """);
    }
    
    // ===== FUNCTION SORTING TESTS =====
    
    @Test
    @DisplayName("ORDER BY with function calls")
    void testOrderByFunctionCalls() {
        framework.executeAndExpectTable("@SELECT value, category FROM Numbers ORDER BY CEIL(value / 10.0) DESC", """
            value | category
            25 | A
            30 | C
            10 | A
            -5 | B
            -15 | B
            """);
    }
    
    @Test
    @DisplayName("ORDER BY with string functions")
    void testOrderByStringFunctions() {
        framework.executeAndExpectTable("@SELECT name, category FROM Products ORDER BY LENGTH(name) DESC", """
            name | category
            Banana | null
            Cherry | null
            Apple | null
            Date | null
            """);
    }
    
    // ===== INTEGRATION TESTS =====
    
    @Test
    @DisplayName("ORDER BY with WHERE clause")
    void testOrderByWithWhere() {
        framework.executeAndExpectTable("@SELECT name, duration FROM Events WHERE duration > 75 ORDER BY duration ASC", """
            name | duration
            EventA | 100
            EventE | 150
            EventC | 200
            """);
    }
    
    @Test
    @DisplayName("ORDER BY with SELECT expressions")
    void testOrderByWithSelectExpressions() {
        framework.executeAndExpectTable("@SELECT name, duration * 2 as doubled FROM Events ORDER BY doubled DESC", """
            name | doubled
            EventC | 400
            EventE | 300
            EventA | 200
            EventB | 100
            EventD | 100
            """);
    }
    
    @Test
    @DisplayName("ORDER BY with aliased expressions")
    void testOrderByWithAliasedExpressions() {
        framework.executeAndExpectTable("@SELECT name, duration * priority as score FROM Events ORDER BY score DESC", """
            name | score
            EventC | 400
            EventE | 300
            EventB | 150
            EventA | 100
            EventD | 50
            """);
    }
    
    // ===== EDGE CASES =====
    
    @Test
    @DisplayName("ORDER BY with identical values")
    void testOrderByIdenticalValues() {
        // Events B and D both have duration 50
        framework.executeAndExpectTable("@SELECT name, duration FROM Events WHERE duration = 50 ORDER BY name ASC", """
            name | duration
            EventB | 50
            EventD | 50
            """);
    }
    
    @Test
    @DisplayName("ORDER BY with single row")
    void testOrderBySingleRow() {
        framework.executeAndExpectTable("@SELECT name, duration FROM Events WHERE name = 'EventA' ORDER BY duration DESC", """
            name | duration
            EventA | 100
            """);
    }
    
    @Test
    @DisplayName("ORDER BY with empty result")
    void testOrderByEmptyResult() {
        framework.executeAndExpectTable("@SELECT name, duration FROM Events WHERE duration > 1000 ORDER BY duration ASC", """
            name | duration
            """);
    }
    
    // ===== ERROR HANDLING TESTS =====
    
    @Test
    @DisplayName("ORDER BY with invalid column")
    void testOrderByInvalidColumn() {
        var result = framework.executeQuery("@SELECT name FROM Events ORDER BY invalid_column ASC");
        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
        assertTrue(result.getError().getMessage().contains("not found"));
    }
    
    @Test
    @DisplayName("ORDER BY with invalid expression")
    void testOrderByInvalidExpression() {
        var result = framework.executeQuery("@SELECT name FROM Events ORDER BY duration / 0 ASC");
        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
    }
    
    // ===== PERFORMANCE TESTS =====
    
    @Test
    @DisplayName("ORDER BY with large dataset")
    void testOrderByPerformance() {
        // Create a larger dataset for performance testing
        StringBuilder tableSpec = new StringBuilder("id | value\n");
        for (int i = 1000; i >= 1; i--) {
            tableSpec.append(i).append(" | ").append(i * 2).append("\n");
        }
        
        framework.mockTable("LargeData", tableSpec.toString());
        
        var result = framework.executeQuery("@SELECT id, value FROM LargeData ORDER BY value ASC");
        assertTrue(result.isSuccess());
        
        var rows = result.getTable().getRows();
        assertEquals(1000, rows.size());
        
        // Verify first few rows are in correct order
        assertEquals(1, ((Number) rows.get(0).getCells().get(0).getValue()).intValue());
        assertEquals(2, ((Number) rows.get(1).getCells().get(0).getValue()).intValue());
        assertEquals(3, ((Number) rows.get(2).getCells().get(0).getValue()).intValue());
    }
    
    // ===== COMPREHENSIVE INTEGRATION TESTS =====
    
    @Test
    @DisplayName("Full query with ORDER BY")
    void testFullQueryWithOrderBy() {
        framework.executeAndExpectTable("""
            @SELECT name, duration, priority, 
                   duration * priority as score 
            FROM Events 
            WHERE duration > 50 
            ORDER BY score DESC, name ASC
            """, """
            name | duration | priority | score
            EventC | 200 | 2 | 400
            EventE | 150 | 2 | 300
            EventA | 100 | 1 | 100
            """);
    }
    
    @Test
    @DisplayName("ORDER BY with multiple complex expressions")
    void testOrderByMultipleComplexExpressions() {
        framework.executeAndExpectTable("""
            @SELECT name, duration, priority 
            FROM Events 
            ORDER BY ABS(duration - 100) ASC, CEIL(priority / 2.0) DESC
            """, """
            name | duration | priority
            EventA | 100 | 1
            EventB | 50 | 3
            EventD | 50 | 1
            EventE | 150 | 2
            EventC | 200 | 2
            """);
    }
}
