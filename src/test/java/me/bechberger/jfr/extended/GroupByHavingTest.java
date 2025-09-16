package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryResult;
import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for GROUP BY and HAVING functionality.
 * 
 * Tests the core aggregation functionality including:
 * - Simple GROUP BY operations
 * - GROUP BY with aggregate functions
 * - HAVING clause filtering
 * - Combinations of GROUP BY and HAVING
 */
@DisplayName("GROUP BY and HAVING Tests")
public class GroupByHavingTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    // ===== BASIC GROUP BY TESTS =====
    
    @Test
    @DisplayName("Basic GROUP BY with COUNT")
    void testBasicGroupByWithCount() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            David | Sales | 70000
            Eve | Engineering | 85000
            """);
        
        framework.executeAndExpectTable("@SELECT department, COUNT(*) as count FROM Users GROUP BY department", """
            department | count
            Engineering | 3
            Sales | 2
            """);
    }
    
    @Test
    @DisplayName("GROUP BY with SUM aggregate")
    void testGroupByWithSum() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            David | Sales | 70000
            Eve | Engineering | 85000
            """);
        
        framework.executeAndExpectTable("@SELECT department, SUM(salary) as total_salary FROM Users GROUP BY department", """
            department | total_salary
            Engineering | 240000
            Sales | 135000
            """);
    }
    
    @Test
    @DisplayName("GROUP BY with AVG aggregate")
    void testGroupByWithAvg() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            David | Sales | 70000
            Eve | Engineering | 85000
            """);
        
        framework.executeAndExpectTable("@SELECT department, AVG(salary) as avg_salary FROM Users GROUP BY department", """
            department | avg_salary
            Engineering | 80000.0
            Sales | 67500.0
            """);
    }
    
    @Test
    @DisplayName("GROUP BY with MAX and MIN")
    void testGroupByWithMaxMin() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            David | Sales | 70000
            Eve | Engineering | 85000
            """);
        
        framework.executeAndExpectTable("@SELECT department, MAX(salary) as max_salary, MIN(salary) as min_salary FROM Users GROUP BY department", """
            department | max_salary | min_salary
            Engineering | 85000 | 75000
            Sales | 70000 | 65000
            """);
    }
    
    // ===== HAVING CLAUSE TESTS =====
    
    @Test
    @DisplayName("HAVING with COUNT filter")
    void testHavingWithCount() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            David | Sales | 70000
            Eve | Engineering | 85000
            Frank | Marketing | 60000
            """);
        
        framework.executeAndExpectTable("@SELECT department, COUNT(*) as count FROM Users GROUP BY department HAVING COUNT(*) > 2", """
            department | count
            Engineering | 3
            """);
    }
    
    @Test
    @DisplayName("HAVING with SUM filter")
    void testHavingWithSum() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            David | Sales | 70000
            Eve | Engineering | 85000
            Frank | Marketing | 60000
            """);
        
        framework.executeAndExpectTable("@SELECT department, SUM(salary) as total_salary FROM Users GROUP BY department HAVING SUM(salary) > 150000", """
            department | total_salary
            Engineering | 240000
            """);
    }
    
    @Test
    @DisplayName("HAVING with AVG filter")
    void testHavingWithAvg() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            David | Sales | 70000
            Eve | Engineering | 85000
            Frank | Marketing | 60000
            """);
        
        framework.executeAndExpectTable("@SELECT department, AVG(salary) as avg_salary FROM Users GROUP BY department HAVING AVG(salary) > 70000", """
            department | avg_salary
            Engineering | 80000.0
            """);
    }
    
    // ===== COMPLEX COMBINATION TESTS =====
    
    @Test
    @DisplayName("Complex GROUP BY with multiple aggregates and HAVING")
    void testComplexGroupByWithHaving() {
        framework.createTable("Sales", """
            region | product | quantity | price
            North | Widget | 100 | 10.50
            North | Widget | 150 | 10.50
            North | Gadget | 75 | 25.00
            South | Widget | 200 | 10.50
            South | Gadget | 50 | 25.00
            East | Widget | 120 | 10.50
            East | Gadget | 100 | 25.00
            """);
        
        framework.executeAndExpectTable("""
            @SELECT region, 
                   COUNT(*) as total_sales,
                   SUM(quantity * price) as total_revenue
            FROM Sales 
            GROUP BY region 
            HAVING COUNT(*) >= 2 AND SUM(quantity * price) > 3000
            """, """
            region | total_sales | total_revenue
            North | 3 | 4500.0
            East | 2 | 3760.0
            """);
    }
    
    // ===== PARAMETERIZED TESTS =====
    
    @ParameterizedTest
    @DisplayName("GROUP BY with different aggregate functions")
    @CsvSource({
        "COUNT(*), 3, 2, 1",
        "SUM(salary), 240000, 135000, 60000",
        "AVG(salary), 80000.0, 67500.0, 60000.0",
        "MAX(salary), 85000, 70000, 60000",
        "MIN(salary), 75000, 65000, 60000"
    })
    void testGroupByWithVariousAggregates(String aggregateFunction, String engineeringValue, String salesValue, String marketingValue) {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            David | Sales | 70000
            Eve | Engineering | 85000
            Frank | Marketing | 60000
            """);
        
        String expectedTable = String.format("""
            department | result
            Engineering | %s
            Sales | %s
            Marketing | %s
            """, engineeringValue, salesValue, marketingValue);
        
        framework.executeAndExpectTable("@SELECT department, " + aggregateFunction + " as result FROM Users GROUP BY department", expectedTable);
    }
    
    // ===== ERROR HANDLING TESTS =====
    
    @Test
    @DisplayName("GROUP BY with non-aggregate column should work")
    void testGroupByWithNonAggregateColumn() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            """);
        
        framework.executeAndExpectTable("@SELECT department, COUNT(*) as count FROM Users GROUP BY department", """
            department | count
            Engineering | 2
            Sales | 1
            """);
    }
    
    @Test
    @DisplayName("Simple aggregation without GROUP BY should work")
    void testSimpleAggregation() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            """);
        
        framework.executeAndExpectTable("@SELECT COUNT(*) as total_count FROM Users", """
            total_count
            3
            """);
    }
    
    @Test
    @DisplayName("Simple aggregation with SUM should work")
    void testSimpleAggregationSum() {
        framework.createTable("Users", """
            name | department | salary
            Alice | Engineering | 75000
            Bob | Engineering | 80000
            Carol | Sales | 65000
            """);
        
        framework.executeAndExpectTable("@SELECT SUM(salary) as total_salary FROM Users", """
            total_salary
            220000
            """);
    }
}
