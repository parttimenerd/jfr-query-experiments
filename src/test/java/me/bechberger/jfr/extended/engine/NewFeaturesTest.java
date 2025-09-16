package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for new JFR Extended Query Language features:
 * - HAVING clauses for grouped data filtering
 * - FUZZY JOIN operations for temporal correlations
 * - Variable assignment in WHERE clauses
 * 
 * Uses the QueryTestFramework's executeAndExpectTable method for validation.
 * 
 * @author JFR Query Plan Architecture Test Suite
 * @since 3.0
 */
public class NewFeaturesTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    @Nested
    @DisplayName("HAVING Clause Tests")
    class HavingClauseTests {
        
        @Test
        @DisplayName("HAVING with simple aggregate condition")
        void testHavingWithSimpleAggregate() {
            framework.createTable("Sales", """
                product STRING | amount NUMBER | region STRING
                Laptop | 1200 | North
                Mouse | 25 | North
                Laptop | 1300 | South
                Keyboard | 75 | North
                Monitor | 500 | South
                Laptop | 1100 | East
                """);
            
            framework.executeAndExpectTable(
                "@SELECT product, SUM(amount) as total FROM Sales GROUP BY product HAVING SUM(amount) > 1000",
                """
                product | total
                Laptop | 3600
                """
            );
        }
        
        @Test
        @DisplayName("HAVING with COUNT condition")
        void testHavingWithCount() {
            framework.createTable("Orders", """
                customer STRING | order_id NUMBER | amount NUMBER
                Alice | 1 | 100
                Bob | 2 | 200
                Alice | 3 | 150
                Charlie | 4 | 300
                Alice | 5 | 75
                """);
            
            framework.executeAndExpectTable(
                "@SELECT customer, COUNT(*) as order_count FROM Orders GROUP BY customer HAVING COUNT(*) >= 2",
                """
                customer | order_count
                Alice | 3
                """
            );
        }
        
        @Test
        @DisplayName("HAVING with multiple conditions")
        void testHavingWithMultipleConditions() {
            framework.createTable("Performance", """
                team STRING | score NUMBER | games NUMBER
                TeamA | 85 | 10
                TeamB | 92 | 8
                TeamC | 78 | 12
                TeamD | 95 | 6
                """);
            
            framework.executeAndExpectTable(
                "@SELECT team, AVG(score) as avg_score, COUNT(*) as games FROM Performance GROUP BY team HAVING AVG(score) > 80 AND COUNT(*) >= 8",
                """
                team | avg_score | games
                TeamA | 85 | 1
                TeamB | 92 | 1
                """
            );
        }
        
        @ParameterizedTest
        @ValueSource(strings = {"SUM", "AVG", "MAX", "MIN"})
        @DisplayName("HAVING with different aggregate functions")
        void testHavingWithDifferentAggregates(String aggregateFunction) {
            framework.createTable("TestData", """
                category STRING | value NUMBER
                A | 10
                A | 20
                A | 30
                B | 5
                B | 15
                C | 25
                C | 35
                C | 45
                """);
            
            // Test that each aggregate function works in HAVING clause
            var result = framework.executeQuery(
                "@SELECT category, " + aggregateFunction + "(value) as result FROM TestData GROUP BY category HAVING " + aggregateFunction + "(value) > 20"
            );
            
            assertTrue(result.isSuccess(), "HAVING with " + aggregateFunction + " should work");
            assertTrue(result.getTable().getRowCount() > 0, "Should have results for " + aggregateFunction);
        }
    }
    
    @Nested
    @DisplayName("FUZZY JOIN Tests")
    class FuzzyJoinTests {
        
        @Test
        @DisplayName("FUZZY JOIN with NEAREST temporal correlation")
        void testFuzzyJoinNearest() {
            framework.createTable("Events", """
                event STRING | timestamp TIMESTAMP
                Login | 1609459200000
                Action | 1609459210000
                Logout | 1609459220000
                """);
            
            framework.createTable("Logs", """
                message STRING | timestamp TIMESTAMP
                User connected | 1609459198000
                Database query | 1609459212000
                User disconnected | 1609459222000
                """);
            
            framework.executeAndExpectTable(
                "@SELECT Events.event, Logs.message FROM Events FUZZY JOIN Logs ON timestamp WITH NEAREST",
                """
                event | message
                Login | User connected
                Action | Database query
                Logout | User disconnected
                """
            );
        }
        
        @Test
        @DisplayName("FUZZY JOIN with PREVIOUS temporal correlation")
        void testFuzzyJoinPrevious() {
            framework.createTable("MainEvents", """
                event STRING | timestamp TIMESTAMP
                ProcessStart | 1609459210000
                ProcessEnd | 1609459220000
                """);
            
            framework.createTable("SystemLogs", """
                log STRING | timestamp TIMESTAMP
                Memory allocated | 1609459200000
                CPU spike | 1609459205000
                Network activity | 1609459215000
                """);
            
            framework.executeAndExpectTable(
                "@SELECT MainEvents.event, SystemLogs.log FROM MainEvents FUZZY JOIN SystemLogs ON timestamp WITH PREVIOUS",
                """
                event | log
                ProcessStart | CPU spike
                ProcessEnd | Network activity
                """
            );
        }
        
        @Test
        @DisplayName("FUZZY JOIN with AFTER temporal correlation")
        void testFuzzyJoinAfter() {
            framework.createTable("Triggers", """
                trigger STRING | timestamp TIMESTAMP
                UserClick | 1609459200000
                UserSubmit | 1609459210000
                """);
            
            framework.createTable("Responses", """
                response STRING | timestamp TIMESTAMP
                PageLoad | 1609459202000
                FormValidation | 1609459212000
                DataSaved | 1609459215000
                """);
            
            framework.executeAndExpectTable(
                "@SELECT Triggers.trigger, Responses.response FROM Triggers FUZZY JOIN Responses ON timestamp WITH AFTER",
                """
                trigger | response
                UserClick | PageLoad
                UserSubmit | FormValidation
                """
            );
        }
        
        @Test
        @DisplayName("FUZZY JOIN with tolerance")
        void testFuzzyJoinWithTolerance() {
            framework.createTable("Orders", """
                order_id NUMBER | timestamp TIMESTAMP
                1 | 1609459200000
                2 | 1609459210000
                """);
            
            framework.createTable("Payments", """
                payment_id NUMBER | timestamp TIMESTAMP
                101 | 1609459203000
                102 | 1609459220000
                """);
            
            // Test with 5-second tolerance
            framework.executeAndExpectTable(
                "@SELECT Orders.order_id, Payments.payment_id FROM Orders FUZZY JOIN Payments ON timestamp WITH NEAREST TOLERANCE 5s",
                """
                order_id | payment_id
                1 | 101
                """
            );
        }
    }
    
    @Nested
    @DisplayName("Variable Assignment in WHERE Tests")
    class VariableAssignmentTests {
        
        @Test
        @DisplayName("Simple variable assignment in WHERE clause")
        void testSimpleVariableAssignment() {
            framework.createTable("Products", """
                name STRING | price NUMBER | category STRING
                Laptop | 1200 | Electronics
                Mouse | 25 | Electronics
                Desk | 300 | Furniture
                Chair | 150 | Furniture
                """);
            
            framework.executeAndExpectTable(
                "@SELECT name, price, calculated_tax FROM Products WHERE calculated_tax := price * 0.1",
                """
                name | price | calculated_tax
                Laptop | 1200 | 120.0
                Mouse | 25 | 2.5
                Desk | 300 | 30.0
                Chair | 150 | 15.0
                """
            );
        }
        
        @Test
        @DisplayName("Variable assignment with complex expression")
        void testVariableAssignmentWithComplexExpression() {
            framework.createTable("Employees", """
                name STRING | salary NUMBER | bonus NUMBER | years NUMBER
                Alice | 50000 | 5000 | 3
                Bob | 60000 | 8000 | 5
                Charlie | 45000 | 3000 | 2
                """);
            
            framework.executeAndExpectTable(
                "@SELECT name, total_comp FROM Employees WHERE total_comp := salary + bonus + (years * 1000)",
                """
                name | total_comp
                Alice | 58000
                Bob | 73000
                Charlie | 50000
                """
            );
        }
        
        @Test
        @DisplayName("Multiple variable assignments in WHERE clause")
        void testMultipleVariableAssignments() {
            framework.createTable("Rectangle", """
                name STRING | width NUMBER | height NUMBER
                Room1 | 10 | 12
                Room2 | 8 | 15
                Room3 | 20 | 10
                """);
            
            framework.executeAndExpectTable(
                "@SELECT name, area, perimeter FROM Rectangle WHERE area := width * height AND perimeter := 2 * (width + height)",
                """
                name | area | perimeter
                Room1 | 120 | 44
                Room2 | 120 | 46
                Room3 | 200 | 60
                """
            );
        }
        
        @Test
        @DisplayName("Variable assignment with conditional logic")
        void testVariableAssignmentWithConditional() {
            framework.createTable("Students", """
                name STRING | score NUMBER
                Alice | 85
                Bob | 92
                Charlie | 78
                Diana | 95
                """);
            
            framework.executeAndExpectTable(
                "@SELECT name, score, grade FROM Students WHERE grade := CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END",
                """
                name | score | grade
                Alice | 85 | B
                Bob | 92 | A
                Charlie | 78 | C
                Diana | 95 | A
                """
            );
        }
        
        @Test
        @DisplayName("Variable assignment used in JOIN condition")
        void testVariableAssignmentInJoinCondition() {
            framework.createTable("Orders", """
                order_id NUMBER | customer_id NUMBER | amount NUMBER
                1 | 101 | 250
                2 | 102 | 180
                3 | 101 | 320
                """);
            
            framework.createTable("Customers", """
                customer_id NUMBER | name STRING | discount NUMBER
                101 | Alice | 0.1
                102 | Bob | 0.05
                """);
            
            framework.executeAndExpectTable(
                "@SELECT Orders.order_id, Customers.name, final_amount FROM Orders INNER JOIN Customers ON Orders.customer_id = Customers.customer_id WHERE final_amount := Orders.amount * (1 - Customers.discount)",
                """
                order_id | name | final_amount
                1 | Alice | 225.0
                2 | Bob | 171.0
                3 | Alice | 288.0
                """
            );
        }
    }
    
    @Nested
    @DisplayName("Combined Features Integration Tests")
    class CombinedFeaturesTests {
        
        @Test
        @DisplayName("HAVING with variable assignment")
        void testHavingWithVariableAssignment() {
            framework.createTable("SalesData", """
                product STRING | sales NUMBER | cost NUMBER
                Laptop | 1200 | 800
                Mouse | 25 | 10
                Laptop | 1300 | 850
                Keyboard | 75 | 30
                """);
            
            framework.executeAndExpectTable(
                "@SELECT product, SUM(profit) as total_profit FROM SalesData WHERE profit := sales - cost GROUP BY product HAVING SUM(profit) > 100",
                """
                product | total_profit
                Laptop | 850
                """
            );
        }
        
        @Test
        @DisplayName("FUZZY JOIN with variable assignment and HAVING")
        void testFuzzyJoinWithVariableAssignmentAndHaving() {
            framework.createTable("UserActions", """
                user_id NUMBER | action STRING | timestamp TIMESTAMP
                1 | login | 1609459200000
                2 | login | 1609459210000
                1 | logout | 1609459220000
                """);
            
            framework.createTable("SystemMetrics", """
                metric STRING | value NUMBER | timestamp TIMESTAMP
                cpu_usage | 45 | 1609459202000
                memory_usage | 60 | 1609459212000
                cpu_usage | 30 | 1609459222000
                """);
            
            framework.executeAndExpectTable(
                "@SELECT UserActions.user_id, AVG(load_factor) as avg_load FROM UserActions FUZZY JOIN SystemMetrics ON timestamp WITH NEAREST WHERE load_factor := SystemMetrics.value / 100.0 GROUP BY UserActions.user_id HAVING AVG(load_factor) > 0.4",
                """
                user_id | avg_load
                1 | 0.375
                2 | 0.6
                """
            );
        }
        
        @Test
        @DisplayName("Complex query with all new features")
        void testComplexQueryWithAllFeatures() {
            framework.createTable("JfrGarbageCollection", """
                name STRING | duration NUMBER | timestamp TIMESTAMP | heap_before NUMBER | heap_after NUMBER
                G1GC | 50 | 1609459200000 | 1000 | 800
                G1GC | 75 | 1609459210000 | 1200 | 900
                ParallelGC | 60 | 1609459220000 | 1100 | 850
                """);
            
            framework.createTable("JfrExecutionSample", """
                thread STRING | method STRING | timestamp TIMESTAMP | state STRING
                main | run | 1609459202000 | RUNNABLE
                worker | execute | 1609459212000 | BLOCKED
                main | cleanup | 1609459222000 | RUNNABLE
                """);
            
            framework.executeAndExpectTable(
                """
                @SELECT 
                    gc.name, 
                    COUNT(*) as gc_count,
                    AVG(efficiency) as avg_efficiency,
                    COLLECT(sample.thread) as active_threads
                FROM JfrGarbageCollection gc 
                FUZZY JOIN JfrExecutionSample sample ON timestamp WITH NEAREST TOLERANCE 5s
                WHERE efficiency := (gc.heap_before - gc.heap_after) / gc.duration
                GROUP BY gc.name 
                HAVING COUNT(*) >= 1 AND AVG(efficiency) > 5
                ORDER BY avg_efficiency DESC
                """,
                """
                name | gc_count | avg_efficiency | active_threads
                G1GC | 2 | 7.0 | ["main", "worker"]
                ParallelGC | 1 | 4.17 | ["main"]
                """
            );
        }
    }
    
    @Nested
    @DisplayName("Error Handling Tests")
    class ErrorHandlingTests {
        
        @Test
        @DisplayName("Invalid HAVING clause should fail gracefully")
        void testInvalidHavingClause() {
            framework.createTable("TestData", """
                category STRING | value NUMBER
                A | 10
                B | 20
                """);
            
            var result = framework.executeQuery(
                "@SELECT category FROM TestData HAVING invalid_column > 10"
            );
            
            assertFalse(result.isSuccess(), "Invalid HAVING clause should fail");
            assertNotNull(result.getError(), "Error should be present");
        }
        
        @Test
        @DisplayName("FUZZY JOIN with non-timestamp column should fail")
        void testFuzzyJoinWithNonTimestampColumn() {
            framework.createTable("Events", """
                event STRING | value NUMBER
                Login | 1
                Logout | 2
                """);
            
            framework.createTable("Logs", """
                message STRING | value NUMBER
                Connected | 1
                Disconnected | 2
                """);
            
            var result = framework.executeQuery(
                "@SELECT * FROM Events FUZZY JOIN Logs ON value WITH NEAREST"
            );
            
            assertFalse(result.isSuccess(), "FUZZY JOIN with non-timestamp column should fail");
            assertNotNull(result.getError(), "Error should be present");
        }
        
        @Test
        @DisplayName("Variable assignment with invalid expression should fail")
        void testVariableAssignmentWithInvalidExpression() {
            framework.createTable("TestData", """
                name STRING | value NUMBER
                Test | 10
                """);
            
            var result = framework.executeQuery(
                "@SELECT name FROM TestData WHERE invalid_var := non_existent_column * 2"
            );
            
            assertFalse(result.isSuccess(), "Variable assignment with invalid expression should fail");
            assertNotNull(result.getError(), "Error should be present");
        }
    }
}
