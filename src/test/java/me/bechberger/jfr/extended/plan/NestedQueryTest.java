package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.engine.framework.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for nested query functionality.
 * Tests extended (@SELECT) nested queries.
 */
class NestedQueryTest {

    private QueryTestFramework framework;

    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }

    // ===== BASIC NESTED QUERY TESTS =====

    @Test
    @DisplayName("Simple nested query - extended in extended")
    void testSimpleNestedExtendedQuery() {
        // Create test data
        framework.mockTable("Users", """
            name | age
            Alice | 25
            Bob | 30
            Charlie | 35
            """);

        // Test nested query
        framework.executeAndExpectTable("""
            @SELECT name, age 
            FROM (@SELECT name, age FROM Users WHERE age > 25) 
            WHERE age < 35
            """, """
            name | age
            Bob | 30
            """);
    }

    @Test
    @DisplayName("Extended query nested in extended query with aggregation")
    void testExtendedNestedWithAggregation() {
        // Create test data
        framework.mockTable("Events", """
            type | duration | thread
            GC | 100 | main
            GC | 200 | main
            IO | 150 | worker-1
            IO | 300 | worker-2
            """);

        // Test nested query with aggregation
        framework.executeAndExpectTable("""
            @SELECT type, AVG(duration) as avg_duration
            FROM (@SELECT type, duration FROM Events WHERE duration > 100) 
            GROUP BY type
            ORDER BY avg_duration DESC
            """, """
            type | avg_duration
            IO | 225.0
            GC | 200.0
            """);
    }

    // ===== COMPLEX NESTED QUERY TESTS =====

    @Test
    @DisplayName("Multiple levels of nesting")
    void testMultipleLevelsOfNesting() {
        // Create test data with different age groups
        framework.mockTable("People", """
            name | age | department
            Alice | 25 | Engineering
            Bob | 30 | Engineering
            Charlie | 35 | Marketing
            Diana | 40 | Marketing
            Eve | 45 | Sales
            """);

        // Test 3-level nested query
        framework.executeAndExpectTable("""
            @SELECT name, age, department
            FROM (
                @SELECT name, age, department 
                FROM (
                    @SELECT name, age, department 
                    FROM People 
                    WHERE age >= 25
                ) 
                WHERE age <= 40
            ) 
            WHERE department = 'Engineering'
            ORDER BY age ASC
            """, """
            name | age | department
            Alice | 25 | Engineering
            Bob | 30 | Engineering
            """);
    }

    @Test
    @DisplayName("Nested query with aggregation in outer query")
    void testNestedQueryWithOuterAggregation() {
        // Create sales data
        framework.mockTable("Sales", """
            product | amount | region
            Widget | 100 | North
            Widget | 200 | South
            Gadget | 150 | North
            Gadget | 300 | South
            Tool | 80 | North
            """);

        // Test nested query with outer aggregation
        framework.executeAndExpectTable("""
            @SELECT region, COUNT(*) as product_count, SUM(amount) as total_sales
            FROM (@SELECT product, amount, region FROM Sales WHERE amount > 100) 
            GROUP BY region
            ORDER BY total_sales DESC
            """, """
            region | product_count | total_sales
            South | 2 | 500
            North | 1 | 150
            """);
    }

    // ===== RAW QUERY NESTING TESTS =====


    @ParameterizedTest
    @CsvSource({
        "duration > 100, 1",
        "duration > 50, 2", 
        "duration > 25, 3"
    })
    @DisplayName("Parameterized nested query tests")
    void testParameterizedNestedQueries(String condition, long expectedCount) {
        // Create test data
        framework.mockTable("Operations", """
            operation | duration
            Read | 30
            Write | 75
            Delete | 150
            """);

        // Test parameterized nested query
        String query = String.format("""
            @SELECT COUNT(*) as count
            FROM (@SELECT operation, duration FROM Operations WHERE %s)
            """, condition);

        QueryResult result = framework.executeQuery(query);
        assertTrue(result.isSuccess());
        assertEquals(expectedCount, result.getTable().getNumber(0, "count"));
    }

    // ===== EDGE CASES AND ERROR HANDLING =====

    @Test
    @DisplayName("Empty nested query result")
    void testEmptyNestedQueryResult() {
        // Create test data
        framework.mockTable("Items", """
            name | value
            Item1 | 10
            Item2 | 20
            """);

        // Test nested query that returns no rows
        framework.executeAndExpectTable("""
            @SELECT name, value
            FROM (@SELECT name, value FROM Items WHERE value > 100)
            """, """
            name | value
            """);
    }

    @Test
    @DisplayName("Nested query with column aliasing")
    void testNestedQueryWithAliases() {
        // Create test data
        framework.mockTable("Measurements", """
            sensor | reading | unit
            Temperature | 25 | C
            Humidity | 60 | Percent
            Pressure | 1013 | hPa
            """);

        // Test nested query with column aliases
        framework.executeAndExpectTable("""
            @SELECT sensor_name, measurement_value
            FROM (@SELECT sensor as sensor_name, reading as measurement_value FROM Measurements WHERE reading > 30)
            ORDER BY measurement_value DESC
            """, """
            sensor_name | measurement_value
            Pressure | 1013
            Humidity | 60
            """);
    }

    @Test
    @DisplayName("Nested query with ORDER BY in both inner and outer queries")
    void testNestedQueryWithMultipleOrderBy() {
        // Create test data
        framework.mockTable("Scores", """
            player | score | game
            Alice | 100 | Game1
            Bob | 200 | Game1
            Charlie | 150 | Game1
            Alice | 300 | Game2
            Bob | 250 | Game2
            """);

        // Test nested query with ORDER BY in both levels
        framework.executeAndExpectTable("""
            @SELECT player, score, game
            FROM (@SELECT player, score, game FROM Scores WHERE score > 150 ORDER BY score ASC)
            ORDER BY player DESC
            """, """
            player | score | game
            Bob | 200 | Game1
            Bob | 250 | Game2
            Alice | 300 | Game2
            """);
    }

    // ===== COMPLEX REAL-WORLD SCENARIOS =====

    @Test
    @DisplayName("Mixed raw and extended nested queries")
    void testMixedRawAndExtendedQueries() {
        // Create JFR-style garbage collection data
        framework.mockTable("GarbageCollection", """
            name | duration | cause | startTime
            G1GC | 100 | Allocation Rate | 1000
            G1GC | 150 | System.gc() | 2000
            ParallelGC | 200 | Allocation Rate | 3000
            G1GC | 75 | Allocation Rate | 4000
            """);

        // Test raw query nested in extended query with complex processing
        framework.executeAndExpectTable("""
            @SELECT 
                name,
                cause,
                COUNT(*) as event_count,
                AVG(duration) as avg_duration,
                MAX(duration) as max_duration
            FROM (
                SELECT name, duration, cause 
                FROM GarbageCollection 
                WHERE duration > 50 AND cause = 'Allocation Rate'
            )
            GROUP BY name, cause
            ORDER BY avg_duration DESC
            """, """
            name | cause | event_count | avg_duration | max_duration
            ParallelGC | Allocation Rate | 1 | 200.0 | 200
            G1GC | Allocation Rate | 2 | 87.5 | 100
            """);
    }
    
    // ===== SUBQUERIES IN SELECT CLAUSE =====
    
    @Test
    @DisplayName("Subquery in SELECT clause")
    void testSubqueryInSelect() {
        // Create related tables
        framework.mockTable("Employees", """
            name | department | salary
            Alice | Sales | 60000
            Bob | Sales | 70000
            Charlie | IT | 80000
            Diana | IT | 90000
            """);
            
        framework.mockTable("Departments", """
            dept_name | budget
            Sales | 500000
            IT | 800000
            """);

        // Test subquery in SELECT clause
        framework.executeAndExpectTable("""
            @SELECT 
                name,
                department,
                salary,
                (@SELECT AVG(salary) FROM Employees e2 WHERE e2.department = e1.department) as avg_dept_salary
            FROM Employees e1
            ORDER BY department, name
            """, """
            name | department | salary | avg_dept_salary
            Charlie | IT | 80000 | 85000.0
            Diana | IT | 90000 | 85000.0
            Alice | Sales | 60000 | 65000.0
            Bob | Sales | 70000 | 65000.0
            """);
    }
    
    // ===== NESTED QUERIES WITH CASE EXPRESSIONS =====
    
    @Test
    @DisplayName("CASE expression in nested query")
    void testCaseExpressionInNestedQuery() {
        framework.mockTable("Performance", """
            employee | sales | rating
            Alice | 50000 | 4
            Bob | 80000 | 5
            Charlie | 30000 | 3
            Diana | 90000 | 5
            """);

        // Test CASE expression within nested query
        framework.executeAndExpectTable("""
            @SELECT 
                performance_level,
                COUNT(*) as level_count
            FROM (
                @SELECT 
                    employee,
                    CASE 
                        WHEN sales > 70000 AND rating = 5 THEN 'Top'
                        WHEN sales > 40000 AND rating >= 4 THEN 'Good'
                        ELSE 'Needs Improvement'
                    END as performance_level
                FROM Performance
            )
            GROUP BY performance_level
            ORDER BY performance_level
            """, """
            performance_level | level_count
            Good | 1
            Needs Improvement | 1
            Top | 2
            """);
    }
    
    @Test
    @DisplayName("Nested query with CASE expression in WHERE")
    void testNestedQueryWithCaseInWhere() {
        framework.mockTable("Products", """
            name | price | category
            Laptop | 1200 | Electronics
            Book | 25 | Education
            Phone | 800 | Electronics
            Desk | 300 | Furniture
            Chair | 150 | Furniture
            """);

        // Test nested query with CASE expression in WHERE clause
        framework.executeAndExpectTable("""
            @SELECT name, price, category, price_range
            FROM (
                @SELECT 
                    name, 
                    price, 
                    category,
                    CASE 
                        WHEN price > 1000 THEN 'Premium'
                        WHEN price > 500 THEN 'Mid-Range'
                        ELSE 'Budget'
                    END as price_range
                FROM Products
            )
            WHERE CASE 
                      WHEN category = 'Electronics' THEN price_range != 'Budget'
                      ELSE price_range = 'Budget'
                  END
            ORDER BY price DESC
            """, """
            name | price | category | price_range
            Laptop | 1200 | Electronics | Premium
            Phone | 800 | Electronics | Mid-Range
            Desk | 300 | Furniture | Budget
            Chair | 150 | Furniture | Budget
            Book | 25 | Education | Budget
            """);
    }
    
    // ===== COMPREHENSIVE CASE EXPRESSION TESTS =====
    
    @Test
    @DisplayName("CASE expression in SELECT clause")
    void testCaseExpressionInSelect() {
        framework.mockTable("Employees", """
            name | age | salary
            Alice | 25 | 50000
            Bob | 35 | 75000
            Charlie | 45 | 90000
            Diana | 55 | 95000
            """);

        // Test CASE expression in SELECT clause
        framework.executeAndExpectTable("""
            @SELECT name, age, salary,
                   CASE 
                       WHEN age < 30 THEN 'Junior'
                       WHEN age < 40 THEN 'Mid-level'
                       WHEN age < 50 THEN 'Senior'
                       ELSE 'Executive'
                   END as level,
                   CASE 
                       WHEN salary < 60000 THEN 'Low'
                       WHEN salary < 80000 THEN 'Medium'
                       ELSE 'High'
                   END as salary_grade
            FROM Employees
            ORDER BY age
            """, """
            name | age | salary | level | salary_grade
            Alice | 25 | 50000 | Junior | Low
            Bob | 35 | 75000 | Mid-level | Medium
            Charlie | 45 | 90000 | Senior | High
            Diana | 55 | 95000 | Executive | High
            """);
    }
    
    @Test
    @DisplayName("CASE expression in WHERE clause")
    void testCaseExpressionInWhere() {
        framework.mockTable("Products", """
            name | price | category
            Laptop | 1200 | Electronics
            Book | 25 | Education
            Phone | 800 | Electronics
            Desk | 300 | Furniture
            Chair | 150 | Furniture
            """);

        // Test CASE expression in WHERE clause
        framework.executeAndExpectTable("""
            @SELECT name, price, category
            FROM Products
            WHERE CASE 
                      WHEN category = 'Electronics' THEN price > 500
                      WHEN category = 'Furniture' THEN price < 200
                      ELSE price < 100
                  END
            ORDER BY price DESC
            """, """
            name | price | category
            Laptop | 1200 | Electronics
            Phone | 800 | Electronics
            Chair | 150 | Furniture
            Book | 25 | Education
            """);
    }
    
    @Test
    @DisplayName("CASE expression in GROUP BY clause")
    void testCaseExpressionInGroupBy() {
        framework.mockTable("Sales", """
            product | amount | region
            Widget | 100 | North
            Gadget | 200 | South
            Widget | 150 | North
            Gadget | 300 | South
            Widget | 250 | East
            Gadget | 180 | West
            """);

        // Test CASE expression in GROUP BY clause
        framework.executeAndExpectTable("""
            @SELECT 
                CASE 
                    WHEN region = 'North' THEN 'Northern'
                    WHEN region = 'South' THEN 'Southern'
                    ELSE 'Other'
                END as region_group,
                COUNT(*) as sales_count,
                SUM(amount) as total_amount
            FROM Sales
            GROUP BY CASE 
                         WHEN region = 'North' THEN 'Northern'
                         WHEN region = 'South' THEN 'Southern'
                         ELSE 'Other'
                     END
            ORDER BY region_group
            """, """
            region_group | sales_count | total_amount
            Northern | 2 | 250
            Other | 2 | 430
            Southern | 2 | 500
            """);
    }
    
    @Test
    @DisplayName("CASE expression in ORDER BY clause")
    void testCaseExpressionInOrderBy() {
        framework.mockTable("Students", """
            name | score | subject
            Alice | 95 | Math
            Bob | 87 | Science
            Charlie | 92 | Math
            Diana | 78 | Science
            Eve | 85 | History
            """);

        // Test CASE expression in ORDER BY clause
        framework.executeAndExpectTable("""
            @SELECT name, score, subject
            FROM Students
            ORDER BY 
                CASE 
                    WHEN subject = 'Math' THEN 1
                    WHEN subject = 'Science' THEN 2
                    ELSE 3
                END,
                score DESC
            """, """
            name | score | subject
            Alice | 95 | Math
            Charlie | 92 | Math
            Bob | 87 | Science
            Diana | 78 | Science
            Eve | 85 | History
            """);
    }
    
    @Test
    @DisplayName("Nested CASE expressions")
    void testNestedCaseExpressions() {
        framework.mockTable("Students", """
            name | score | subject
            Alice | 95 | Math
            Bob | 87 | Science
            Charlie | 92 | Math
            Diana | 78 | Science
            Eve | 85 | History
            """);

        // Test nested CASE expressions
        framework.executeAndExpectTable("""
            @SELECT name, score, subject,
                   CASE 
                       WHEN subject = 'Math' THEN 
                           CASE 
                               WHEN score > 90 THEN 'Math Excellence'
                               WHEN score > 80 THEN 'Math Proficient'
                               ELSE 'Math Developing'
                           END
                       WHEN subject = 'Science' THEN 
                           CASE 
                               WHEN score > 85 THEN 'Science Excellence'
                               WHEN score > 75 THEN 'Science Proficient'
                               ELSE 'Science Developing'
                           END
                       ELSE 'Other Subject'
                   END as detailed_assessment
            FROM Students
            ORDER BY score DESC
            """, """
            name | score | subject | detailed_assessment
            Alice | 95 | Math | Math Excellence
            Charlie | 92 | Math | Math Excellence
            Bob | 87 | Science | Science Excellence
            Eve | 85 | History | Other Subject
            Diana | 78 | Science | Science Proficient
            """);
    }
    
    @Test
    @DisplayName("CASE expression with complex conditions")
    void testCaseExpressionWithComplexConditions() {
        framework.mockTable("Performance")
            .withStringColumn("employee")
            .withNumberColumn("sales")
            .withNumberColumn("rating")
            .withStringColumn("department")
            .withRow("Alice", 50000L, 4L, "Sales")
            .withRow("Bob", 75000L, 5L, "Sales")
            .withRow("Charlie", 30000L, 3L, "Marketing")
            .withRow("Diana", 90000L, 5L, "Sales")
            .withRow("Eve", 45000L, 4L, "Marketing")
            .build();

        // Test complex CASE expression with multiple conditions
        framework.executeAndExpectTable("""
            @SELECT employee, sales, rating, department,
                   CASE 
                       WHEN sales > 80000 THEN 'Top Performer'
                       WHEN sales > 60000 THEN 'High Performer'
                       WHEN sales > 40000 THEN 'Good Performer'
                       ELSE 'Needs Improvement'
                   END as performance_level
            FROM Performance
            ORDER BY sales DESC
            """, """
            employee | sales | rating | department | performance_level
            Diana | 90000 | 5 | Sales | Top Performer
            Bob | 75000 | 5 | Sales | High Performer
            Alice | 50000 | 4 | Sales | Good Performer
            Eve | 45000 | 4 | Marketing | Good Performer
            Charlie | 30000 | 3 | Marketing | Needs Improvement
            """);
    }
    
    // ===== CASE EXPRESSION WITH AGGREGATES =====
    
    @Test
    @DisplayName("CASE expression with aggregate functions")
    void testCaseExpressionWithAggregates() {
        framework.mockTable("Orders", """
            customer | amount | status
            CompanyA | 1000 | Completed
            CompanyA | 500 | Pending
            CompanyB | 2000 | Completed
            CompanyB | 800 | Cancelled
            CompanyC | 1500 | Completed
            """);

        // Test CASE expression with aggregate functions
        framework.executeAndExpectTable("""
            @SELECT 
                customer,
                SUM(amount) as total_amount,
                SUM(CASE WHEN status = 'Completed' THEN amount ELSE 0 END) as completed_amount,
                COUNT(CASE WHEN status = 'Completed' THEN 1 END) as completed_count,
                COUNT(CASE WHEN status = 'Pending' THEN 1 END) as pending_count
            FROM Orders
            GROUP BY customer
            ORDER BY customer
            """, """
            customer | total_amount | completed_amount | completed_count | pending_count
            CompanyA | 1500 | 1000 | 1 | 1
            CompanyB | 2800 | 2000 | 1 | 0
            CompanyC | 1500 | 1500 | 1 | 0
            """);
    }
    
    @Test
    @DisplayName("CASE expression with aggregates in WHEN clause")
    void testCaseExpressionWithAggregatesInWhen() {
        framework.mockTable("SalesData", """
            salesperson | month | sales_amount
            Alice | Jan | 10000
            Alice | Feb | 15000
            Alice | Mar | 12000
            Bob | Jan | 8000
            Bob | Feb | 9000
            Bob | Mar | 11000
            Charlie | Jan | 20000
            Charlie | Feb | 18000
            Charlie | Mar | 22000
            """);

        // Test CASE expression with aggregates in WHEN conditions
        framework.executeAndExpectTable("""
            @SELECT 
                salesperson,
                SUM(sales_amount) as total_sales,
                CASE 
                    WHEN SUM(sales_amount) > 50000 THEN 'Top Performer'
                    WHEN SUM(sales_amount) > 30000 THEN 'High Performer'
                    WHEN AVG(sales_amount) > 10000 THEN 'Good Performer'
                    ELSE 'Needs Improvement'
                END as performance_category,
                CASE 
                    WHEN COUNT(*) = 3 AND MIN(sales_amount) > 8000 THEN 'Consistent'
                    WHEN MAX(sales_amount) - MIN(sales_amount) > 10000 THEN 'Variable'
                    ELSE 'Stable'
                END as consistency_rating
            FROM SalesData
            GROUP BY salesperson
            ORDER BY total_sales DESC
            """, """
            salesperson | total_sales | performance_category | consistency_rating
            Charlie | 60000 | Top Performer | Consistent
            Alice | 37000 | High Performer | Consistent
            Bob | 28000 | Needs Improvement | Stable
            """);
    }
    
    @Test
    @DisplayName("Complex CASE with nested aggregates")
    void testComplexCaseWithNestedAggregates() {
        framework.mockTable("TransactionData", """
            account_id | transaction_type | amount | date
            ACC001 | Deposit | 1000 | 2024-01-01
            ACC001 | Withdrawal | 500 | 2024-01-02
            ACC001 | Deposit | 2000 | 2024-01-03
            ACC002 | Deposit | 5000 | 2024-01-01
            ACC002 | Withdrawal | 1000 | 2024-01-02
            ACC002 | Withdrawal | 2000 | 2024-01-03
            ACC003 | Deposit | 800 | 2024-01-01
            ACC003 | Deposit | 1200 | 2024-01-02
            """);

        // Test complex CASE with nested aggregates
        framework.executeAndExpectTable("""
            @SELECT 
                account_id,
                COUNT(*) as transaction_count,
                SUM(amount) as total_amount,
                CASE 
                    WHEN SUM(CASE WHEN transaction_type = 'Deposit' THEN amount ELSE 0 END) > 
                         SUM(CASE WHEN transaction_type = 'Withdrawal' THEN amount ELSE 0 END) THEN 'Net Positive'
                    WHEN SUM(CASE WHEN transaction_type = 'Deposit' THEN amount ELSE 0 END) < 
                         SUM(CASE WHEN transaction_type = 'Withdrawal' THEN amount ELSE 0 END) THEN 'Net Negative'
                    ELSE 'Balanced'
                END as net_flow_status,
                CASE 
                    WHEN COUNT(CASE WHEN transaction_type = 'Deposit' THEN 1 END) > 
                         COUNT(CASE WHEN transaction_type = 'Withdrawal' THEN 1 END) THEN 'Deposit Heavy'
                    WHEN COUNT(CASE WHEN transaction_type = 'Deposit' THEN 1 END) < 
                         COUNT(CASE WHEN transaction_type = 'Withdrawal' THEN 1 END) THEN 'Withdrawal Heavy'
                    ELSE 'Balanced Activity'
                END as activity_pattern
            FROM TransactionData
            GROUP BY account_id
            ORDER BY account_id
            """, """
            account_id | transaction_count | total_amount | net_flow_status | activity_pattern
            ACC001 | 3 | 3500 | Net Positive | Deposit Heavy
            ACC002 | 3 | 8000 | Net Positive | Withdrawal Heavy
            ACC003 | 2 | 2000 | Net Positive | Deposit Heavy
            """);
    }
    
    // ===== ALIAS TESTS =====
    
    @Test
    @DisplayName("Column aliases in SELECT with CASE expressions")
    void testColumnAliasesWithCaseExpressions() {
        framework.mockTable("ProductSales", """
            product_name | category | price | quantity_sold
            Laptop | Electronics | 1200 | 50
            Mouse | Electronics | 25 | 200
            Desk | Furniture | 300 | 30
            Chair | Furniture | 150 | 80
            Book | Education | 20 | 100
            """);

        // Test column aliases with CASE expressions
        framework.executeAndExpectTable("""
            @SELECT 
                product_name as product,
                price as unit_price,
                quantity_sold as qty,
                (price * quantity_sold) as total_revenue,
                CASE 
                    WHEN price * quantity_sold > 30000 THEN 'High Revenue'
                    WHEN price * quantity_sold > 10000 THEN 'Medium Revenue'
                    ELSE 'Low Revenue'
                END as revenue_category,
                CASE 
                    WHEN category = 'Electronics' THEN 'Tech'
                    WHEN category = 'Furniture' THEN 'Home'
                    ELSE 'Other'
                END as category_group
            FROM ProductSales
            ORDER BY total_revenue DESC
            """, """
            product | unit_price | qty | total_revenue | revenue_category | category_group
            Laptop | 1200 | 50 | 60000 | High Revenue | Tech
            Chair | 150 | 80 | 12000 | Medium Revenue | Home
            Desk | 300 | 30 | 9000 | Low Revenue | Home
            Mouse | 25 | 200 | 5000 | Low Revenue | Tech
            Book | 20 | 100 | 2000 | Low Revenue | Other
            """);
    }
    
    @Test
    @DisplayName("Aliases in nested subqueries with CASE")
    void testAliasesInNestedSubqueriesWithCase() {
        framework.mockTable("OrderDetails", """
            order_id | customer_id | product_id | quantity | unit_price
            1 | 101 | 1001 | 2 | 100
            1 | 101 | 1002 | 1 | 50
            2 | 102 | 1001 | 3 | 100
            2 | 102 | 1003 | 2 | 200
            3 | 103 | 1002 | 5 | 50
            3 | 103 | 1003 | 1 | 200
            """);

        // Test aliases in nested subqueries with CASE expressions
        framework.executeAndExpectTable("""
            @SELECT 
                customer_summary.customer_id as cust_id,
                customer_summary.order_count as orders,
                customer_summary.total_spent as spent,
                customer_summary.avg_order_value as avg_order,
                CASE 
                    WHEN customer_summary.total_spent > 500 THEN 'Premium'
                    WHEN customer_summary.total_spent > 200 THEN 'Standard'
                    ELSE 'Basic'
                END as customer_tier
            FROM (
                @SELECT 
                    customer_id,
                    COUNT(DISTINCT order_id) as order_count,
                    SUM(quantity * unit_price) as total_spent,
                    AVG(quantity * unit_price) as avg_order_value
                FROM OrderDetails
                GROUP BY customer_id
            ) as customer_summary
            ORDER BY total_spent DESC
            """, """
            cust_id | orders | spent | avg_order | customer_tier
            102 | 1 | 700 | 350.0 | Premium
            103 | 1 | 450 | 225.0 | Standard
            101 | 1 | 250 | 125.0 | Standard
            """);
    }
    
    @Test
    @DisplayName("Complex aliases with aggregates in WHEN clauses")
    void testComplexAliasesWithAggregatesInWhen() {
        framework.mockTable("PerformanceMetrics", """
            employee_id | metric_name | metric_value | measurement_date
            1 | Sales | 10000 | 2024-01-01
            1 | Calls | 50 | 2024-01-01
            1 | Sales | 12000 | 2024-02-01
            1 | Calls | 45 | 2024-02-01
            2 | Sales | 8000 | 2024-01-01
            2 | Calls | 60 | 2024-01-01
            2 | Sales | 9000 | 2024-02-01
            2 | Calls | 55 | 2024-02-01
            """);

        // Test complex aliases with aggregates in WHEN clauses
        framework.executeAndExpectTable("""
            @SELECT 
                emp_summary.emp_id as employee,
                emp_summary.total_sales as sales_total,
                emp_summary.avg_calls as call_average,
                emp_summary.performance_months as months_active,
                CASE 
                    WHEN emp_summary.total_sales > 20000 AND emp_summary.avg_calls > 50 THEN 'Excellent'
                    WHEN emp_summary.total_sales > 15000 OR emp_summary.avg_calls > 55 THEN 'Good'
                    WHEN emp_summary.total_sales > 10000 THEN 'Satisfactory'
                    ELSE 'Needs Improvement'
                END as overall_rating,
                CASE 
                    WHEN emp_summary.performance_months >= 2 THEN 'Consistent'
                    ELSE 'New'
                END as tenure_status
            FROM (
                @SELECT 
                    employee_id as emp_id,
                    SUM(CASE WHEN metric_name = 'Sales' THEN metric_value ELSE 0 END) as total_sales,
                    AVG(CASE WHEN metric_name = 'Calls' THEN metric_value ELSE NULL END) as avg_calls,
                    COUNT(DISTINCT measurement_date) as performance_months
                FROM PerformanceMetrics
                GROUP BY employee_id
            ) as emp_summary
            ORDER BY sales_total DESC
            """, """
            employee | sales_total | call_average | months_active | overall_rating | tenure_status
            1 | 22000 | 47.5 | 2 | Good | Consistent
            2 | 17000 | 57.5 | 2 | Good | Consistent
            """);
    }
    
    @Test
    @DisplayName("Subquery aliases with CASE in WHERE clause")
    void testSubqueryAliasesWithCaseInWhere() {
        framework.mockTable("InventoryData", """
            product_id | product_name | category | stock_level | reorder_point
            1 | Widget A | Electronics | 50 | 20
            2 | Widget B | Electronics | 15 | 25
            3 | Gadget X | Furniture | 100 | 30
            4 | Gadget Y | Furniture | 5 | 15
            5 | Tool Z | Hardware | 200 | 50
            """);

        // Test subquery aliases with CASE in WHERE clause
        framework.executeAndExpectTable("""
            @SELECT 
                inventory_status.product_name as product,
                inventory_status.category,
                inventory_status.stock_level as current_stock,
                inventory_status.reorder_point as reorder_at,
                inventory_status.stock_status
            FROM (
                @SELECT 
                    product_name,
                    category,
                    stock_level,
                    reorder_point,
                    CASE 
                        WHEN stock_level <= reorder_point THEN 'Reorder Required'
                        WHEN stock_level <= reorder_point * 2 THEN 'Low Stock'
                        ELSE 'Adequate Stock'
                    END as stock_status
                FROM InventoryData
            ) as inventory_status
            WHERE CASE 
                      WHEN inventory_status.category = 'Electronics' THEN inventory_status.stock_status != 'Adequate Stock'
                      WHEN inventory_status.category = 'Furniture' THEN inventory_status.stock_status = 'Reorder Required'
                      ELSE inventory_status.stock_level < 100
                  END
            ORDER BY current_stock ASC
            """, """
            product | category | current_stock | reorder_at | stock_status
            Gadget Y | Furniture | 5 | 15 | Reorder Required
            Widget B | Electronics | 15 | 25 | Reorder Required
            """);
    }
}
