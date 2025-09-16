package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Comprehensive tests for HAVING clause functionality with various aggregate functions and conditions.
 * Tests filtering of grouped results based on aggregate values.
 */
public class HavingClauseTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    // ===== BASIC HAVING TESTS =====
    
    @Test
    @DisplayName("HAVING with COUNT(*) - basic filtering")
    void testHavingWithCount() {
        framework.createTable("Events", """
            eventType | duration | thread
            GarbageCollection | 100 | main
            GarbageCollection | 150 | main
            GarbageCollection | 200 | main
            GarbageCollection | 120 | main
            GarbageCollection | 180 | main
            ExecutionSample | 50 | worker-1
            ExecutionSample | 60 | worker-1
            ExecutionSample | 40 | worker-1
            ThreadSample | 25 | worker-2
            ThreadSample | 30 | worker-2
            """);
        
        framework.executeAndExpectTable("""
            @SELECT 
                eventType,
                COUNT(*) as count
            FROM Events 
            GROUP BY eventType 
            HAVING COUNT(*) > 3
            ORDER BY COUNT(*) DESC
            """, """
            eventType | count
            GarbageCollection | 5
            """);
    }
    
    @Test
    @DisplayName("HAVING with SUM - filtering by total values")
    void testHavingWithSum() {
        framework.createTable("Sales", """
            region | amount | quarter
            North | 1000 | Q1
            North | 1500 | Q2
            North | 2000 | Q3
            South | 800 | Q1
            South | 900 | Q2
            East | 1200 | Q1
            East | 1800 | Q2
            East | 2200 | Q3
            West | 600 | Q1
            """);
        
        framework.executeAndExpectTable("""
            @SELECT 
                region,
                SUM(amount) as total_sales
            FROM Sales 
            GROUP BY region 
            HAVING SUM(amount) >= 4000
            ORDER BY SUM(amount) DESC
            """, """
            region | total_sales
            East | 5200
            North | 4500
            """);
    }
    
    @Test
    @DisplayName("HAVING with AVG - filtering by average values")
    void testHavingWithAverage() {
        framework.createTable("Scores", """
            student | subject | score
            Alice | Math | 95
            Alice | Science | 87
            Alice | English | 92
            Bob | Math | 78
            Bob | Science | 82
            Bob | English | 85
            Charlie | Math | 65
            Charlie | Science | 70
            David | Math | 98
            David | Science | 96
            David | English | 94
            """);
        
        framework.executeAndExpectTable("""
            @SELECT 
                student,
                AVG(score) as average_score,
                COUNT(*) as subject_count
            FROM Scores 
            GROUP BY student 
            HAVING AVG(score) > 85
            ORDER BY average_score DESC
            """, """
            student | average_score | subject_count
            David | 96.0 | 3
            Alice | 91.33333333333333 | 3
            """);
    }
    
    @Test
    @DisplayName("HAVING with MAX and MIN - filtering by extreme values")
    void testHavingWithMaxMin() {
        framework.createTable("Performance", """
            team | metric | value
            Alpha | latency | 100
            Alpha | latency | 120
            Alpha | latency | 90
            Alpha | throughput | 500
            Alpha | throughput | 600
            Beta | latency | 200
            Beta | latency | 180
            Beta | throughput | 300
            Gamma | latency | 50
            Gamma | latency | 60
            Gamma | latency | 70
            Gamma | throughput | 800
            Gamma | throughput | 850
            """);
        
        framework.executeAndExpectTable("""
            @SELECT 
                team,
                MAX(value) as max_value,
                MIN(value) as min_value
            FROM Performance 
            GROUP BY team 
            HAVING MAX(value) > 700
            ORDER BY max_value DESC
            """, """
            team | max_value | min_value
            Gamma | 850 | 50
            """);
    }
    
    // ===== COMPLEX HAVING CONDITIONS =====
    
    @Test
    @DisplayName("HAVING with multiple conditions using AND")
    void testHavingWithMultipleConditions() {
        framework.createTable("Orders", """
            customer | order_date | amount
            Customer1 | 2023-01-01 | 100
            Customer1 | 2023-01-02 | 150
            Customer1 | 2023-01-03 | 200
            Customer1 | 2023-01-04 | 120
            Customer2 | 2023-01-01 | 80
            Customer2 | 2023-01-02 | 90
            Customer3 | 2023-01-01 | 300
            Customer3 | 2023-01-02 | 400
            Customer3 | 2023-01-03 | 350
            Customer4 | 2023-01-01 | 50
            """);
        
        framework.executeAndExpectTable("""
            @SELECT 
                customer,
                COUNT(*) as order_count,
                SUM(amount) as total_spent,
                AVG(amount) as avg_order
            FROM Orders 
            GROUP BY customer 
            HAVING COUNT(*) >= 3 AND SUM(amount) > 500
            ORDER BY total_spent DESC
            """, """
            customer | order_count | total_spent | avg_order
            Customer3 | 3 | 1050 | 350.0
            Customer1 | 4 | 570 | 142.5
            """);
    }
    
    @Test
    @DisplayName("HAVING with OR conditions")
    void testHavingWithOrConditions() {
        framework.createTable("Products", """
            category | product | price | units_sold
            Electronics | Laptop | 1000 | 50
            Electronics | Phone | 800 | 100
            Electronics | Tablet | 600 | 30
            Clothing | Shirt | 50 | 200
            Clothing | Pants | 80 | 150
            Clothing | Jacket | 120 | 80
            Books | Novel | 20 | 500
            Books | Textbook | 100 | 100
            """);
        
        framework.executeAndExpectTable("""
            @SELECT 
                category,
                COUNT(*) as product_count,
                SUM(units_sold) as total_units
            FROM Products 
            GROUP BY category 
            HAVING COUNT(*) > 3 OR SUM(units_sold) > 400
            ORDER BY total_units DESC
            """, """
            category | product_count | total_units
            Books | 2 | 600
            Clothing | 3 | 430
            """);
    }
    
    @Test
    @DisplayName("HAVING with arithmetic expressions")
    void testHavingWithArithmeticExpressions() {
        framework.createTable("Inventory", """
            warehouse | item | quantity | unit_cost
            North | Item1 | 100 | 10
            North | Item2 | 200 | 15
            North | Item3 | 150 | 12
            South | Item1 | 80 | 10
            South | Item2 | 120 | 15
            East | Item1 | 200 | 10
            East | Item2 | 300 | 15
            East | Item3 | 250 | 12
            East | Item4 | 100 | 20
            """);
        
        framework.executeAndExpectTable("""
            @SELECT 
                warehouse,
                SUM(quantity * unit_cost) as total_value,
                COUNT(*) as item_count
            FROM Inventory 
            GROUP BY warehouse 
            HAVING SUM(quantity * unit_cost) > 5000
            ORDER BY total_value DESC
            """, """
            warehouse | total_value | item_count
            East | 10300 | 4
            """);
    }
    
    // ===== PARAMETERIZED TESTS FOR VARIOUS CONDITIONS =====
    
    @ParameterizedTest
    @CsvSource({
        "COUNT(*) > 2, 2",
        "COUNT(*) >= 3, 1", 
        "SUM(duration) > 1000, 1",
        "AVG(duration) > 100, 2",
        "MAX(duration) >= 200, 1"
    })
    @DisplayName("HAVING with various aggregate conditions")
    void testHavingVariousConditions(String condition, int expectedRows) {
        framework.createTable("Events", """
            eventType | duration
            TypeA | 100
            TypeA | 150
            TypeA | 200
            TypeB | 50
            TypeB | 60
            TypeB | 40
            TypeC | 300
            """);
        
        var result = framework.executeAndAssertSuccess(
            "@SELECT eventType, COUNT(*) as count, SUM(duration) as total " +
            "FROM Events GROUP BY eventType HAVING " + condition);
        
        var table = result.getTable();
        assertEquals(expectedRows, table.getRowCount(), 
            "Expected " + expectedRows + " rows for condition: " + condition);
    }
    
    // ===== EDGE CASES =====
    
    @Test
    @DisplayName("HAVING with no matching groups")
    void testHavingWithNoMatches() {
        framework.createTable("SmallData", """
            group | value
            A | 1
            A | 2
            B | 3
            C | 4
            """);
        
        framework.executeAndExpectTable("""
            @SELECT 
                group,
                SUM(value) as total
            FROM SmallData 
            GROUP BY group 
            HAVING SUM(value) > 100
            """, """
            group | total
            """);
    }
    
    @Test
    @DisplayName("HAVING with all groups matching")
    void testHavingWithAllMatches() {
        framework.createTable("LargeData", """
            group | value
            A | 100
            A | 200
            B | 150
            B | 250
            C | 300
            C | 400
            """);
        
        framework.executeAndExpectTable("""
            @SELECT 
                group,
                SUM(value) as total
            FROM LargeData 
            GROUP BY group 
            HAVING SUM(value) > 0
            ORDER BY group
            """, """
            group | total
            A | 300
            B | 400
            C | 700
            """);
    }
    
    @Test
    @DisplayName("HAVING with CASE expressions")
    void testHavingWithCaseExpressions() {
        framework.createTable("Sales", """
            region | amount
            North | 1000
            North | 1500
            North | 2000
            South | 800
            South | 900
            East | 1200
            East | 1800
            East | 2200
            West | 600
            West | 700
            """);
        
        framework.executeAndExpectTable("""
            @SELECT 
                region,
                COUNT(*) as order_count,
                CASE 
                    WHEN SUM(amount) > 4000 THEN 'High'
                    WHEN SUM(amount) > 2000 THEN 'Medium'
                    ELSE 'Low'
                END as sales_category
            FROM Sales 
            GROUP BY region 
            HAVING CASE 
                WHEN COUNT(*) >= 3 THEN SUM(amount) > 3000
                ELSE SUM(amount) > 1500
            END
            ORDER BY region
            """, """
            region | order_count | sales_category
            East | 3 | High
            North | 3 | High
            """);
    }
    
    @Test
    @DisplayName("HAVING with nested aggregate expressions")
    void testHavingWithNestedAggregates() {
        framework.createTable("Performance", """
            team | score1 | score2
            Alpha | 80 | 90
            Alpha | 85 | 95
            Alpha | 90 | 100
            Beta | 70 | 75
            Beta | 75 | 80
            Gamma | 95 | 98
            Gamma | 92 | 96
            Gamma | 88 | 94
            """);
        
        framework.executeAndExpectTable("""
            @SELECT 
                team,
                AVG(score1) as avg_score1,
                AVG(score2) as avg_score2
            FROM Performance 
            GROUP BY team 
            HAVING (AVG(score1) + AVG(score2)) / 2 > 85
            ORDER BY (AVG(score1) + AVG(score2)) / 2 DESC
            """, """
            team | avg_score1 | avg_score2
            Gamma | 91.66666666666667 | 96.0
            Alpha | 85.0 | 95.0
            """);
    }
    
    // ===== HELPER METHODS =====
    
    private void assertEquals(int expected, int actual, String message) {
        if (expected != actual) {
            throw new AssertionError(message + " - Expected: " + expected + ", Actual: " + actual);
        }
    }
}
