package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Comprehensive test suite for ORDER BY functionality.
 * 
 * This test suite verifies the core ORDER BY functionality that is currently working:
 * - Basic ORDER BY with ASC/DESC
 * - Multi-field sorting with different directions
 * - ORDER BY with different column selections
 */
@DisplayName("ORDER BY Comprehensive Test Suite")
public class ComprehensiveOrderByTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Create test data for basic sorting
        framework.mockTable("Events", """
            name | duration
            EventA | 100
            EventB | 50
            EventC | 200
            """);

        // Create test data for multi-field sorting
        framework.mockTable("EventsWithCost", """
            name | duration | cost
            EventA | 100 | 10
            EventB | 50 | 5
            EventC | 200 | 20
            EventD | 200 | 15
            """);
    }
    
    // ===== BASIC ORDER BY TESTS =====
    
    @Test
    @DisplayName("Simple ORDER BY ASC")
    void testSimpleOrderByASC() {
        framework.executeAndExpectTable("@SELECT name FROM Events ORDER BY duration ASC",
            """
            name
            EventB
            EventA
            EventC
            """);
    }
    
    @Test
    @DisplayName("Simple ORDER BY DESC")
    void testSimpleOrderByDESC() {
        framework.executeAndExpectTable("@SELECT name FROM Events ORDER BY duration DESC",
            """
            name
            EventC
            EventA
            EventB
            """);
    }
    
    @Test
    @DisplayName("ORDER BY with multiple columns selected")
    void testOrderByWithMultipleColumns() {
        framework.executeAndExpectTable("@SELECT name, duration FROM Events ORDER BY duration ASC",
            """
            name | duration
            EventB | 50
            EventA | 100
            EventC | 200
            """);
    }
    
    // ===== MULTI-FIELD ORDER BY TESTS =====
    
    @Test
    @DisplayName("Multi-field ORDER BY: duration ASC, name DESC")
    void testMultiFieldOrderByDurationAscNameDesc() {
        framework.executeAndExpectTable("@SELECT name, duration FROM EventsWithCost ORDER BY duration ASC, name DESC",
            """
            name | duration
            EventB | 50
            EventA | 100
            EventD | 200
            EventC | 200
            """);
    }
    
    @Test
    @DisplayName("Multi-field ORDER BY: duration ASC, cost DESC")
    void testMultiFieldOrderByDurationAscCostDesc() {
        framework.executeAndExpectTable("@SELECT name, duration FROM EventsWithCost ORDER BY duration ASC, cost DESC",
            """
            name | duration
            EventB | 50
            EventA | 100
            EventC | 200
            EventD | 200
            """);
    }
    
    @Test
    @DisplayName("Multi-field ORDER BY: duration ASC, cost ASC")
    void testMultiFieldOrderByDurationAscCostAsc() {
        framework.executeAndExpectTable("@SELECT name, duration FROM EventsWithCost ORDER BY duration ASC, cost ASC",
            """
            name | duration
            EventB | 50
            EventA | 100
            EventD | 200
            EventC | 200
            """);
    }
    
    // ===== EXPRESSION ORDER BY TESTS =====
    
    @Test
    @DisplayName("ORDER BY with arithmetic expression")
    void testOrderByWithArithmeticExpression() {
        framework.executeAndExpectTable("@SELECT name, duration FROM Events ORDER BY duration + 10 ASC",
            """
            name | duration
            EventB | 50
            EventA | 100
            EventC | 200
            """);
    }
    
    // ===== GROUP BY WITH ORDER BY TESTS =====
    
    @Test
    @DisplayName("GROUP BY with ORDER BY - debug")
    void testGroupByWithOrderBy() {
        // Let's debug what's happening
        var result = framework.executeAndAssertSuccess("@SELECT cost, COUNT(*) FROM EventsWithCost GROUP BY cost ORDER BY cost ASC");
        System.out.println("Result rows: " + result.getTable().getRowCount());
        System.out.println("Columns: " + result.getTable().getColumns());
        
        // Expected: 4 groups with cost values [5, 10, 15, 20]
        framework.executeAndExpectTable("@SELECT cost, COUNT(*) FROM EventsWithCost GROUP BY cost ORDER BY cost ASC",
            """
            cost | COUNT(*)
            5 | 1
            10 | 1
            15 | 1
            20 | 1
            """);
    }
    
    // ===== PARAMETERIZED TESTS =====
    
    @ParameterizedTest
    @CsvSource({
        "ASC, 'EventB,EventA,EventC'",
        "DESC, 'EventC,EventA,EventB'"
    })
    @DisplayName("Parameterized ORDER BY direction test")
    void testOrderByDirections(String direction, String expectedOrder) {
        String[] expectedNames = expectedOrder.split(",");
        
        // Build expected table format
        StringBuilder expectedTable = new StringBuilder("name\n");
        for (String name : expectedNames) {
            expectedTable.append(name).append("\n");
        }
        
        framework.executeAndExpectTable("@SELECT name FROM Events ORDER BY duration " + direction, 
            expectedTable.toString().trim());
    }
    
    // ===== EDGE CASES =====
    
    @Test
    @DisplayName("ORDER BY with duplicate values")
    void testOrderByWithDuplicateValues() {
        framework.mockTable("DuplicateEvents", """
            name | duration
            Event1 | 100
            Event2 | 100
            Event3 | 200
            Event4 | 100
            """);
            
        // For duplicate values, the order should be stable (preserve original order)
        framework.executeAndExpectTable("@SELECT name FROM DuplicateEvents ORDER BY duration ASC",
            """
            name
            Event1
            Event2
            Event4
            Event3
            """);
    }
    
    @Test
    @DisplayName("ORDER BY with empty table")
    void testOrderByWithEmptyTable() {
        framework.mockTable("EmptyEvents", """
            name | duration
            """);
            
        framework.executeAndExpectTable("@SELECT name FROM EmptyEvents ORDER BY duration ASC",
            """
            name
            """);
    }
    
    @Test
    @DisplayName("ORDER BY with single row")
    void testOrderByWithSingleRow() {
        framework.mockTable("SingleEvent", """
            name | duration
            Event1 | 100
            """);
            
        framework.executeAndExpectTable("@SELECT name FROM SingleEvent ORDER BY duration ASC",
            """
            name
            Event1
            """);
    }
}
