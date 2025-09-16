package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for all implemented features including HAVING, JOIN, and variable assignments.
 * Tests cover all combinations of aggregate functions, expressions, and query language features.
 * 
 * @author JFR Query Language Test Suite
 * @since 3.0
 */
public class ComprehensiveFeatureTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        setupTestData();
    }
    
    /**
     * Setup comprehensive test data for all feature combinations
     */
    private void setupTestData() {
        // Employee data for GROUP BY and HAVING tests
        framework.createTable("Employees", """
            name | department | salary | age | experience
            Alice | Engineering | 95000 | 30 | 5
            Bob | Engineering | 85000 | 28 | 3
            Charlie | Sales | 75000 | 35 | 8
            Diana | Sales | 80000 | 32 | 6
            Eve | Marketing | 70000 | 29 | 4
            Frank | Marketing | 90000 | 40 | 12
            Grace | Engineering | 110000 | 38 | 10
            Henry | Sales | 65000 | 26 | 2
            """);
        
        // Order data for JOIN tests
        framework.createTable("Orders", """
            order_id | customer_name | amount | order_date
            1 | Alice | 1500.00 | 2024-01-15
            2 | Bob | 2500.00 | 2024-01-16
            3 | Charlie | 800.00 | 2024-01-17
            4 | Alice | 1200.00 | 2024-01-18
            5 | Diana | 3000.00 | 2024-01-19
            6 | Unknown | 500.00 | 2024-01-20
            """);
        
        // Product data for complex JOINs
        framework.createTable("Products", """
            product_id | product_name | category | price
            101 | Laptop | Electronics | 1200.00
            102 | Mouse | Electronics | 25.00
            103 | Desk | Furniture | 300.00
            104 | Chair | Furniture | 150.00
            105 | Monitor | Electronics | 400.00
            """);
        
        // Order details for multi-table JOINs
        framework.createTable("OrderDetails", """
            order_id | product_id | quantity | unit_price
            1 | 101 | 1 | 1200.00
            1 | 102 | 2 | 25.00
            2 | 101 | 2 | 1200.00
            2 | 105 | 1 | 400.00
            3 | 103 | 1 | 300.00
            3 | 104 | 2 | 150.00
            4 | 102 | 5 | 25.00
            4 | 105 | 3 | 400.00
            5 | 101 | 1 | 1200.00
            5 | 103 | 2 | 300.00
            5 | 104 | 4 | 150.00
            """);
        
        // Time-series data for FUZZY JOINs
        framework.createTable("Events", """
            event_id | event_type | timestamp | duration
            1 | START | 1609459200000 | 1000
            2 | PROCESS | 1609459202000 | 5000
            3 | END | 1609459210000 | 500
            4 | START | 1609459220000 | 2000
            5 | PROCESS | 1609459223000 | 3000
            6 | END | 1609459230000 | 800
            """);
        
        // Log data for FUZZY JOINs
        framework.createTable("Logs", """
            log_id | message | timestamp | level
            1 | System started | 1609459199000 | INFO
            2 | Processing request | 1609459201000 | DEBUG
            3 | Request completed | 1609459205000 | INFO
            4 | Warning occurred | 1609459208000 | WARN
            5 | System restarted | 1609459218000 | INFO
            6 | Processing batch | 1609459222000 | DEBUG
            7 | Batch completed | 1609459228000 | INFO
            8 | Error detected | 1609459235000 | ERROR
            """);
    }
    
    @Nested
    @DisplayName("HAVING Clause Tests")
    class HavingTests {
        
        @Test
        @DisplayName("HAVING with COUNT aggregate")
        void testHavingWithCount() {
            framework.executeAndExpectTable("""
                @SELECT department, COUNT(*) as employee_count 
                FROM Employees 
                GROUP BY department 
                HAVING COUNT(*) > 2
                """, """
                department | employee_count
                Engineering | 3
                Sales | 3
                """);
        }
        
        @Test
        @DisplayName("HAVING with SUM aggregate")
        void testHavingWithSum() {
            framework.executeAndExpectTable("""
                @SELECT department, SUM(salary) as total_salary 
                FROM Employees 
                GROUP BY department 
                HAVING SUM(salary) > 200000
                """, """
                department | total_salary
                Engineering | 290000
                Sales | 220000
                """);
        }
        
        @Test
        @DisplayName("HAVING with AVG aggregate")
        void testHavingWithAvg() {
            framework.executeAndExpectTable("""
                @SELECT department, AVG(salary) as avg_salary 
                FROM Employees 
                GROUP BY department 
                HAVING AVG(salary) > 80000
                """, """
                department | avg_salary
                Engineering | 96666.67
                """);
        }
        
        @Test
        @DisplayName("HAVING with MAX aggregate")
        void testHavingWithMax() {
            framework.executeAndExpectTable("""
                @SELECT department, MAX(salary) as max_salary 
                FROM Employees 
                GROUP BY department 
                HAVING MAX(salary) > 100000
                """, """
                department | max_salary
                Engineering | 110000
                """);
        }
        
        @Test
        @DisplayName("HAVING with MIN aggregate")
        void testHavingWithMin() {
            framework.executeAndExpectTable("""
                @SELECT department, MIN(salary) as min_salary 
                FROM Employees 
                GROUP BY department 
                HAVING MIN(salary) > 70000
                """, """
                department | min_salary
                Engineering | 85000
                """);
        }
        
        @Test
        @DisplayName("HAVING with multiple conditions")
        void testHavingWithMultipleConditions() {
            framework.executeAndExpectTable("""
                @SELECT department, COUNT(*) as count, AVG(salary) as avg_salary 
                FROM Employees 
                GROUP BY department 
                HAVING COUNT(*) >= 2 AND AVG(salary) > 75000
                """, """
                department | count | avg_salary
                Engineering | 3 | 96666.67
                Sales | 3 | 73333.33
                Marketing | 2 | 80000
                """);
        }
        
        @Test
        @DisplayName("HAVING with expressions")
        void testHavingWithExpressions() {
            framework.executeAndExpectTable("""
                @SELECT department, SUM(salary) as total_salary, COUNT(*) as count 
                FROM Employees 
                GROUP BY department 
                HAVING SUM(salary) / COUNT(*) > 80000
                """, """
                department | total_salary | count
                Engineering | 290000 | 3
                Marketing | 160000 | 2
                """);
        }
        
        @Test
        @DisplayName("HAVING with ORDER BY")
        void testHavingWithOrderBy() {
            framework.executeAndExpectTable("""
                @SELECT department, AVG(age) as avg_age 
                FROM Employees 
                GROUP BY department 
                HAVING AVG(age) > 30 
                ORDER BY avg_age DESC
                """, """
                department | avg_age
                Marketing | 34.5
                Engineering | 32
                Sales | 31
                """);
        }
    }
    
    @Nested
    @DisplayName("JOIN Tests")
    class JoinTests {
        
        @Test
        @DisplayName("INNER JOIN basic")
        void testInnerJoinBasic() {
            framework.executeAndExpectTable("""
                @SELECT e.name, o.amount 
                FROM Employees e 
                INNER JOIN Orders o ON e.name = o.customer_name
                """, """
                name | amount
                Alice | 1500.00
                Alice | 1200.00
                Bob | 2500.00
                Charlie | 800.00
                Diana | 3000.00
                """);
        }
        
        @Test
        @DisplayName("LEFT JOIN with NULL values")
        void testLeftJoinWithNulls() {
            framework.executeAndExpectTable("""
                @SELECT e.name, e.department, o.amount 
                FROM Employees e 
                LEFT JOIN Orders o ON e.name = o.customer_name 
                ORDER BY e.name
                """, """
                name | department | amount
                Alice | Engineering | 1500.00
                Alice | Engineering | 1200.00
                Bob | Engineering | 2500.00
                Charlie | Sales | 800.00
                Diana | Sales | 3000.00
                Eve | Marketing | null
                Frank | Marketing | null
                Grace | Engineering | null
                Henry | Sales | null
                """);
        }
        
        @Test
        @DisplayName("RIGHT JOIN with unmatched orders")
        void testRightJoinWithUnmatched() {
            framework.executeAndExpectTable("""
                @SELECT e.name, e.department, o.customer_name, o.amount 
                FROM Employees e 
                RIGHT JOIN Orders o ON e.name = o.customer_name 
                ORDER BY o.order_id
                """, """
                name | department | customer_name | amount
                Alice | Engineering | Alice | 1500.00
                Bob | Engineering | Bob | 2500.00
                Charlie | Sales | Charlie | 800.00
                Alice | Engineering | Alice | 1200.00
                Diana | Sales | Diana | 3000.00
                null | null | Unknown | 500.00
                """);
        }
        
        @Test
        @DisplayName("FULL OUTER JOIN")
        void testFullOuterJoin() {
            framework.executeAndExpectTable("""
                @SELECT e.name, o.customer_name, o.amount 
                FROM Employees e 
                FULL OUTER JOIN Orders o ON e.name = o.customer_name 
                ORDER BY COALESCE(e.name, o.customer_name)
                """, """
                name | customer_name | amount
                Alice | Alice | 1500.00
                Alice | Alice | 1200.00
                Bob | Bob | 2500.00
                Charlie | Charlie | 800.00
                Diana | Diana | 3000.00
                Eve | null | null
                Frank | null | null
                Grace | null | null
                Henry | null | null
                null | Unknown | 500.00
                """);
        }
        
        @Test
        @DisplayName("Multiple JOINs")
        void testMultipleJoins() {
            framework.executeAndExpectTable("""
                @SELECT o.order_id, o.customer_name, p.product_name, od.quantity 
                FROM Orders o 
                INNER JOIN OrderDetails od ON o.order_id = od.order_id 
                INNER JOIN Products p ON od.product_id = p.product_id 
                ORDER BY o.order_id, p.product_name
                """, """
                order_id | customer_name | product_name | quantity
                1 | Alice | Laptop | 1
                1 | Alice | Mouse | 2
                2 | Bob | Laptop | 2
                2 | Bob | Monitor | 1
                3 | Charlie | Chair | 2
                3 | Charlie | Desk | 1
                4 | Alice | Monitor | 3
                4 | Alice | Mouse | 5
                5 | Diana | Chair | 4
                5 | Diana | Desk | 2
                5 | Diana | Laptop | 1
                """);
        }
        
        @Test
        @DisplayName("JOIN with aggregates")
        void testJoinWithAggregates() {
            framework.executeAndExpectTable("""
                @SELECT o.customer_name, COUNT(*) as order_count, SUM(o.amount) as total_amount 
                FROM Orders o 
                INNER JOIN Employees e ON o.customer_name = e.name 
                GROUP BY o.customer_name 
                ORDER BY total_amount DESC
                """, """
                customer_name | order_count | total_amount
                Diana | 1 | 3000.00
                Alice | 2 | 2700.00
                Bob | 1 | 2500.00
                Charlie | 1 | 800.00
                """);
        }
        
        @Test
        @DisplayName("JOIN with WHERE and HAVING")
        void testJoinWithWhereAndHaving() {
            framework.executeAndExpectTable("""
                @SELECT e.department, COUNT(*) as employee_count, AVG(o.amount) as avg_order_amount 
                FROM Employees e 
                INNER JOIN Orders o ON e.name = o.customer_name 
                WHERE e.salary > 80000 
                GROUP BY e.department 
                HAVING COUNT(*) > 1 
                ORDER BY avg_order_amount DESC
                """, """
                department | employee_count | avg_order_amount
                Engineering | 3 | 2066.67
                """);
        }
    }
    
    @Nested
    @DisplayName("FUZZY JOIN Tests")
    class FuzzyJoinTests {
        
        @Test
        @DisplayName("FUZZY JOIN NEAREST")
        void testFuzzyJoinNearest() {
            framework.executeAndExpectTable("""
                @SELECT e.event_id, e.event_type, l.message, l.level 
                FROM Events e 
                FUZZY JOIN Logs l ON e.timestamp WITH NEAREST 
                ORDER BY e.event_id
                """, """
                event_id | event_type | message | level
                1 | START | System started | INFO
                2 | PROCESS | Processing request | DEBUG
                3 | END | Request completed | INFO
                4 | START | System restarted | INFO
                5 | PROCESS | Processing batch | DEBUG
                6 | END | Batch completed | INFO
                """);
        }
        
        @Test
        @DisplayName("FUZZY JOIN PREVIOUS")
        void testFuzzyJoinPrevious() {
            framework.executeAndExpectTable("""
                @SELECT e.event_id, e.event_type, l.message, l.level 
                FROM Events e 
                FUZZY JOIN Logs l ON e.timestamp WITH PREVIOUS 
                ORDER BY e.event_id
                """, """
                event_id | event_type | message | level
                1 | START | System started | INFO
                2 | PROCESS | Processing request | DEBUG
                3 | END | Request completed | INFO
                4 | START | System restarted | INFO
                5 | PROCESS | Processing batch | DEBUG
                6 | END | Batch completed | INFO
                """);
        }
        
        @Test
        @DisplayName("FUZZY JOIN AFTER")
        void testFuzzyJoinAfter() {
            framework.executeAndExpectTable("""
                @SELECT e.event_id, e.event_type, l.message, l.level 
                FROM Events e 
                FUZZY JOIN Logs l ON e.timestamp WITH AFTER 
                ORDER BY e.event_id
                """, """
                event_id | event_type | message | level
                1 | START | Processing request | DEBUG
                2 | PROCESS | Request completed | INFO
                3 | END | System restarted | INFO
                4 | START | Processing batch | DEBUG
                5 | PROCESS | Batch completed | INFO
                6 | END | Error detected | ERROR
                """);
        }
        
        @Test
        @DisplayName("FUZZY JOIN with tolerance")
        void testFuzzyJoinWithTolerance() {
            framework.executeAndExpectTable("""
                @SELECT e.event_id, e.event_type, l.message 
                FROM Events e 
                FUZZY JOIN Logs l ON e.timestamp WITH NEAREST TOLERANCE 2s 
                WHERE l.message IS NOT NULL 
                ORDER BY e.event_id
                """, """
                event_id | event_type | message
                1 | START | System started
                2 | PROCESS | Processing request
                3 | END | Request completed
                4 | START | System restarted
                5 | PROCESS | Processing batch
                6 | END | Batch completed
                """);
        }
    }
    
    @Nested
    @DisplayName("Variable Assignment Tests")
    class VariableAssignmentTests {
        
        @Test
        @DisplayName("Variable assignment in WHERE")
        void testVariableAssignmentInWhere() {
            framework.executeAndExpectTable("""
                @SELECT name, department, salary_category 
                FROM Employees 
                WHERE salary_category := CASE 
                    WHEN salary > 90000 THEN 'High' 
                    WHEN salary > 75000 THEN 'Medium' 
                    ELSE 'Low' 
                END
                """, """
                name | department | salary_category
                Alice | Engineering | High
                Bob | Engineering | Medium
                Charlie | Sales | Low
                Diana | Sales | Medium
                Eve | Marketing | Low
                Frank | Marketing | Medium
                Grace | Engineering | High
                Henry | Sales | Low
                """);
        }
        
        @Test
        @DisplayName("Variable assignment with calculations")
        void testVariableAssignmentWithCalculations() {
            framework.executeAndExpectTable("""
                @SELECT name, salary, bonus 
                FROM Employees 
                WHERE bonus := salary * 0.15 
                ORDER BY bonus DESC
                """, """
                name | salary | bonus
                Grace | 110000 | 16500.00
                Alice | 95000 | 14250.00
                Frank | 90000 | 13500.00
                Bob | 85000 | 12750.00
                Diana | 80000 | 12000.00
                Charlie | 75000 | 11250.00
                Eve | 70000 | 10500.00
                Henry | 65000 | 9750.00
                """);
        }
        
        @Test
        @DisplayName("Variable assignment with aggregates")
        void testVariableAssignmentWithAggregates() {
            framework.executeAndExpectTable("""
                @SELECT department, avg_salary, salary_rank 
                FROM (
                    @SELECT department, AVG(salary) as avg_salary 
                    FROM Employees 
                    GROUP BY department
                ) subquery 
                WHERE salary_rank := CASE 
                    WHEN avg_salary > 90000 THEN 1 
                    WHEN avg_salary > 75000 THEN 2 
                    ELSE 3 
                END 
                ORDER BY salary_rank
                """, """
                department | avg_salary | salary_rank
                Engineering | 96666.67 | 1
                Marketing | 80000 | 2
                Sales | 73333.33 | 3
                """);
        }
        
        @Test
        @DisplayName("Multiple variable assignments")
        void testMultipleVariableAssignments() {
            framework.executeAndExpectTable("""
                @SELECT name, salary, tax, net_salary 
                FROM Employees 
                WHERE tax := salary * 0.25 
                  AND net_salary := salary - tax 
                ORDER BY net_salary DESC
                """, """
                name | salary | tax | net_salary
                Grace | 110000 | 27500.00 | 82500.00
                Alice | 95000 | 23750.00 | 71250.00
                Frank | 90000 | 22500.00 | 67500.00
                Bob | 85000 | 21250.00 | 63750.00
                Diana | 80000 | 20000.00 | 60000.00
                Charlie | 75000 | 18750.00 | 56250.00
                Eve | 70000 | 17500.00 | 52500.00
                Henry | 65000 | 16250.00 | 48750.00
                """);
        }
    }
    
    @Nested
    @DisplayName("Complex Combined Feature Tests")
    class ComplexCombinedTests {
        
        @Test
        @DisplayName("JOIN with HAVING and expressions")
        void testJoinWithHavingAndExpressions() {
            framework.executeAndExpectTable("""
                @SELECT e.department, COUNT(*) as employee_count, 
                       AVG(o.amount) as avg_order_value,
                       SUM(o.amount) as total_sales 
                FROM Employees e 
                INNER JOIN Orders o ON e.name = o.customer_name 
                GROUP BY e.department 
                HAVING COUNT(*) > 1 AND AVG(o.amount) > 1500 
                ORDER BY total_sales DESC
                """, """
                department | employee_count | avg_order_value | total_sales
                Engineering | 3 | 2066.67 | 6200.00
                """);
        }
        
        @Test
        @DisplayName("FUZZY JOIN with aggregates and HAVING")
        void testFuzzyJoinWithAggregatesAndHaving() {
            framework.executeAndExpectTable("""
                @SELECT l.level, COUNT(*) as event_count, 
                       AVG(e.duration) as avg_duration 
                FROM Events e 
                FUZZY JOIN Logs l ON e.timestamp WITH NEAREST 
                GROUP BY l.level 
                HAVING COUNT(*) > 1 
                ORDER BY avg_duration DESC
                """, """
                level | event_count | avg_duration
                DEBUG | 2 | 4000
                INFO | 4 | 1325
                """);
        }
        
        @Test
        @DisplayName("Variable assignment with JOIN and HAVING")
        void testVariableAssignmentWithJoinAndHaving() {
            framework.executeAndExpectTable("""
                @SELECT e.department, performance_rating, COUNT(*) as employee_count 
                FROM Employees e 
                INNER JOIN Orders o ON e.name = o.customer_name 
                WHERE performance_rating := CASE 
                    WHEN e.salary > 90000 AND o.amount > 2000 THEN 'Excellent' 
                    WHEN e.salary > 80000 OR o.amount > 1000 THEN 'Good' 
                    ELSE 'Average' 
                END 
                GROUP BY e.department, performance_rating 
                HAVING COUNT(*) >= 1 
                ORDER BY e.department, performance_rating
                """, """
                department | performance_rating | employee_count
                Engineering | Excellent | 2
                Engineering | Good | 1
                Sales | Average | 1
                Sales | Good | 1
                """);
        }
        
        @Test
        @DisplayName("Complex subquery with all features")
        void testComplexSubqueryWithAllFeatures() {
            framework.executeAndExpectTable("""
                @SELECT dept_stats.department, 
                       dept_stats.avg_salary, 
                       dept_stats.employee_count,
                       order_stats.total_orders,
                       dept_performance 
                FROM (
                    @SELECT department, AVG(salary) as avg_salary, COUNT(*) as employee_count 
                    FROM Employees 
                    GROUP BY department 
                    HAVING COUNT(*) > 1
                ) dept_stats 
                LEFT JOIN (
                    @SELECT e.department, COUNT(*) as total_orders 
                    FROM Employees e 
                    INNER JOIN Orders o ON e.name = o.customer_name 
                    GROUP BY e.department
                ) order_stats ON dept_stats.department = order_stats.department 
                WHERE dept_performance := CASE 
                    WHEN dept_stats.avg_salary > 90000 THEN 'High Performance' 
                    WHEN dept_stats.avg_salary > 75000 THEN 'Medium Performance' 
                    ELSE 'Needs Improvement' 
                END 
                ORDER BY dept_stats.avg_salary DESC
                """, """
                department | avg_salary | employee_count | total_orders | dept_performance
                Engineering | 96666.67 | 3 | 3 | High Performance
                Marketing | 80000 | 2 | null | Medium Performance
                Sales | 73333.33 | 3 | 2 | Needs Improvement
                """);
        }
    }
    
    @Nested
    @DisplayName("Advanced Expression Tests")
    class AdvancedExpressionTests {
        
        @ParameterizedTest
        @CsvSource({
            "COUNT(*), 8",
            "SUM(salary), 635000",
            "AVG(salary), 79375",
            "MAX(salary), 110000",
            "MIN(salary), 65000"
        })
        @DisplayName("Aggregate functions in different contexts")
        void testAggregateFunctions(String aggregateExpr, String expectedValue) {
            framework.executeAndExpectTable(
                "@SELECT " + aggregateExpr + " as result FROM Employees",
                "result\n" + expectedValue
            );
        }
        
        @Test
        @DisplayName("Mathematical expressions in SELECT")
        void testMathematicalExpressions() {
            framework.executeAndExpectTable("""
                @SELECT name, salary, 
                       salary * 1.1 as salary_with_raise,
                       salary * 0.25 as tax_amount,
                       salary - (salary * 0.25) as net_salary,
                       ROUND(salary / 12, 2) as monthly_salary 
                FROM Employees 
                WHERE name = 'Alice'
                """, """
                name | salary | salary_with_raise | tax_amount | net_salary | monthly_salary
                Alice | 95000 | 104500 | 23750 | 71250 | 7916.67
                """);
        }
        
        @Test
        @DisplayName("String functions and expressions")
        void testStringFunctions() {
            framework.executeAndExpectTable("""
                @SELECT name, 
                       UPPER(name) as upper_name,
                       LOWER(department) as lower_dept,
                       CONCAT(name, ' - ', department) as name_dept,
                       LENGTH(name) as name_length 
                FROM Employees 
                WHERE name IN ['Alice', 'Bob', 'Charlie'] 
                ORDER BY name
                """, """
                name | upper_name | lower_dept | name_dept | name_length
                Alice | ALICE | engineering | Alice - Engineering | 5
                Bob | BOB | engineering | Bob - Engineering | 3
                Charlie | CHARLIE | sales | Charlie - Sales | 7
                """);
        }
        
        @Test
        @DisplayName("Conditional expressions with CASE")
        void testConditionalExpressions() {
            framework.executeAndExpectTable("""
                @SELECT name, salary, age,
                       CASE 
                           WHEN salary > 90000 THEN 'Senior'
                           WHEN salary > 75000 THEN 'Mid-level'
                           ELSE 'Junior'
                       END as level,
                       CASE 
                           WHEN age < 30 THEN 'Young'
                           WHEN age < 35 THEN 'Mid-career'
                           ELSE 'Experienced'
                       END as career_stage 
                FROM Employees 
                ORDER BY salary DESC
                """, """
                name | salary | age | level | career_stage
                Grace | 110000 | 38 | Senior | Experienced
                Alice | 95000 | 30 | Senior | Mid-career
                Frank | 90000 | 40 | Mid-level | Experienced
                Bob | 85000 | 28 | Mid-level | Young
                Diana | 80000 | 32 | Mid-level | Mid-career
                Charlie | 75000 | 35 | Mid-level | Experienced
                Eve | 70000 | 29 | Junior | Young
                Henry | 65000 | 26 | Junior | Young
                """);
        }
    }
    
    @Nested
    @DisplayName("Edge Cases and Error Handling")
    class EdgeCaseTests {
        
        @Test
        @DisplayName("HAVING with no GROUP BY")
        void testHavingWithoutGroupBy() {
            framework.executeAndExpectTable("""
                @SELECT COUNT(*) as total_employees 
                FROM Employees 
                HAVING COUNT(*) > 5
                """, """
                total_employees
                8
                """);
        }
        
        @Test
        @DisplayName("Empty result with HAVING")
        void testEmptyResultWithHaving() {
            framework.executeAndExpectTable("""
                @SELECT department, COUNT(*) as employee_count 
                FROM Employees 
                GROUP BY department 
                HAVING COUNT(*) > 10
                """, """
                department | employee_count
                """);
        }
        
        @Test
        @DisplayName("JOIN with no matches")
        void testJoinWithNoMatches() {
            framework.createTable("EmptyTable", "id | name");
            
            framework.executeAndExpectTable("""
                @SELECT e.name, et.name as empty_name 
                FROM Employees e 
                INNER JOIN EmptyTable et ON e.name = et.name
                """, """
                name | empty_name
                """);
        }
        
        @Test
        @DisplayName("Complex WHERE with variable assignments")
        void testComplexWhereWithVariables() {
            framework.executeAndExpectTable("""
                @SELECT name, salary, category, bonus 
                FROM Employees 
                WHERE category := CASE 
                    WHEN salary > 90000 THEN 'A' 
                    WHEN salary > 75000 THEN 'B' 
                    ELSE 'C' 
                END 
                AND bonus := salary * 0.1 
                AND category IN ('A', 'B') 
                ORDER BY bonus DESC
                """, """
                name | salary | category | bonus
                Grace | 110000 | A | 11000
                Alice | 95000 | A | 9500
                Frank | 90000 | B | 9000
                Bob | 85000 | B | 8500
                Diana | 80000 | B | 8000
                """);
        }
        
        @Test
        @DisplayName("Nested aggregates with HAVING")
        void testNestedAggregatesWithHaving() {
            framework.executeAndExpectTable("""
                @SELECT department, 
                       COUNT(*) as employee_count,
                       AVG(salary) as avg_salary,
                       MAX(salary) - MIN(salary) as salary_range 
                FROM Employees 
                GROUP BY department 
                HAVING AVG(salary) > 75000 
                   AND MAX(salary) - MIN(salary) > 10000 
                ORDER BY salary_range DESC
                """, """
                department | employee_count | avg_salary | salary_range
                Engineering | 3 | 96666.67 | 25000
                Marketing | 2 | 80000 | 20000
                Sales | 3 | 73333.33 | 15000
                """);
        }
    }
}
