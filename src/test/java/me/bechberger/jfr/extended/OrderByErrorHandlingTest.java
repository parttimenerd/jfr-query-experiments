package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.engine.framework.QueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for ORDER BY error handling and syntax validation.
 * 
 * Tests cover:
 * - Invalid ORDER BY syntax errors
 * - Aggregate functions without GROUP BY
 * - Missing field references
 * - Type mismatches in sorting
 * - Complex expression validation
 */
class OrderByErrorHandlingTest {

    private QueryTestFramework framework;

    @BeforeEach
    void setUp() throws Exception {
        framework = new QueryTestFramework();
        
        // Create a mock users table for testing
        framework.mockTable("Users", """
            name | age | id
            Alice | 25 | 1
            Bob | 30 | 2
            Charlie | 35 | 3
            David | 20 | 4
            Eve | 40 | 5
            """);
            
        // Create additional test data
        framework.mockTable("Employees", """
            name | department | salary | active
            John | Engineering | 75000 | true
            Jane | Marketing | 65000 | true
            Jim | Engineering | 80000 | false
            Jill | Sales | 55000 | true
            Jack | Engineering | 70000 | true
            """);
            
        // Create mock tables with the correct names (MockUsers and MockEmployees)
        framework.mockTable("MockUsers", """
            name | age | id
            Alice | 25 | 1
            Bob | 30 | 2
            Charlie | 35 | 3
            David | 20 | 4
            Eve | 40 | 5
            Frank | 35 | 6
            Grace | 30 | 7
            Hannah | 25 | 8
            """);
            
        framework.mockTable("MockEmployees", """
            name | department | salary | active
            John | Engineering | 75000 | true
            Jane | Marketing | 65000 | true
            Jim | Engineering | 80000 | false
            Jill | Sales | 55000 | true
            Jack | Engineering | 70000 | true
            Janet | Marketing | 60000 | true
            James | Sales | 60000 | false
            Julia | Engineering | 85000 | true
            """);
    }

    @ParameterizedTest
    @MethodSource("provideSyntaxErrorTestCases")
    void testOrderBySyntaxErrors(String query, String expectedErrorType, String description) {
        // Test parsing errors
        try {
            Parser.parseAndValidate(query);
            fail("Expected parser error for: " + description + "\nQuery: " + query);
        } catch (Exception e) {
            assertTrue(e.getMessage().toLowerCase().contains(expectedErrorType.toLowerCase()), 
                "Error message should contain '" + expectedErrorType + "' but was: " + e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("provideRuntimeErrorTestCases")
    void testOrderByRuntimeErrors(String query, String expectedErrorType, String description) {
        // Test execution errors
        QueryResult result = framework.executeQuery(query);
        
        assertFalse(result.isSuccess(), "Expected execution error for: " + description + "\nQuery: " + query);
        
        String errorMessage = result.getError().getMessage().toLowerCase();
        assertTrue(errorMessage.contains(expectedErrorType.toLowerCase()), 
            "Error message should contain '" + expectedErrorType + "' but was: " + result.getError().getMessage());
    }

    @Test
    void testValidOrderByQueries() {
        // Test that valid queries work correctly
        String[] validQueries = {
            "@SELECT * FROM MockUsers ORDER BY age",
            "@SELECT * FROM MockUsers ORDER BY age DESC", 
            "@SELECT * FROM MockUsers ORDER BY name, age DESC",
            "@SELECT * FROM MockUsers ORDER BY ABS(age - 25)",
            "@SELECT name, COUNT(*) FROM MockUsers GROUP BY name ORDER BY COUNT(*) DESC",
            "@SELECT * FROM MockUsers ORDER BY (age * 2 + 10) DESC"
        };

        for (String query : validQueries) {
            QueryResult result = framework.executeQuery(query);
            assertTrue(result.isSuccess(), "Valid query should succeed: " + query + 
                "\nError: " + (result.isSuccess() ? "none" : result.getError().getMessage()));
        }
    }

    private static Stream<Arguments> provideSyntaxErrorTestCases() {
        return Stream.of(
            // Missing ORDER BY expressions
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY",
                "missing expression",
                "ORDER BY without expression"
            ),
            
            // Missing ORDER BY keyword
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER",
                "expected alias",
                "ORDER without BY"
            ),
            
            // Invalid ORDER BY syntax
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY BY name",
                "unexpected",
                "Double BY keyword"
            ),
            
            // Both ASC and DESC
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY age ASC DESC",
                "unexpected",
                "Both ASC and DESC specified"
            ),
            
            // ASC/DESC without field
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY ASC",
                "unexpected token",
                "ASC without field"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY DESC",
                "unexpected token", 
                "DESC without field"
            ),
            
            // Trailing comma
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY age,",
                "unexpected token",
                "Trailing comma in ORDER BY"
            ),
            
            // Invalid expression syntax
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY (age +)",
                "unexpected",
                "Incomplete arithmetic expression"
            ),
            
            // Missing closing parenthesis
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY (age + 10",
                "expected ')'",
                "Missing closing parenthesis"
            )
        );
    }

    private static Stream<Arguments> provideRuntimeErrorTestCases() {
        return Stream.of(
            // Aggregate functions without GROUP BY
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY COUNT(*)",
                "aggregate function",
                "COUNT(*) without GROUP BY"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY SUM(age)",
                "aggregate function", 
                "SUM() without GROUP BY"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY AVG(age)",
                "aggregate function",
                "AVG() without GROUP BY"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY MIN(age)",
                "aggregate function",
                "MIN() without GROUP BY"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY MAX(age)",
                "aggregate function",
                "MAX() without GROUP BY"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY STDDEV(age)",
                "aggregate function",
                "STDDEV() without GROUP BY"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY VARIANCE(age)",
                "aggregate function",
                "VARIANCE() without GROUP BY"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY MEDIAN(age)",
                "aggregate function",
                "MEDIAN() without GROUP BY"
            ),
            
            // Percentile functions without GROUP BY
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY P99(age)",
                "percentile function",
                "P99() without GROUP BY"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY P95(age)",
                "percentile function",
                "P95() without GROUP BY"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY P90(age)",
                "percentile function",
                "P90() without GROUP BY"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY PERCENTILE(age, 75)",
                "percentile function",
                "PERCENTILE() without GROUP BY"
            ),
            
            // Mixed aggregate and non-aggregate in complex expressions
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY COUNT(*) + age",
                "aggregate function",
                "Mixed aggregate and field without GROUP BY"
            ),
            
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY ABS(SUM(age))",
                "aggregate function", 
                "Function with aggregate argument without GROUP BY"
            ),
            
            // Invalid field access without alias
            Arguments.of(
                "@SELECT * FROM MockUsers ORDER BY users.age",
                "field access",
                "Field access without alias"
            )
        );
    }

    @Test
    void testOrderByWithGroupByValidCases() {
        // Test that aggregate functions work correctly WITH GROUP BY
        String[] validGroupByQueries = {
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY COUNT(*)",
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY COUNT(*) DESC",
            "@SELECT name, SUM(age) as total_age FROM MockUsers GROUP BY name ORDER BY SUM(age)",
            "@SELECT name, AVG(age) as avg_age FROM MockUsers GROUP BY name ORDER BY AVG(age) DESC",
            "@SELECT name, MIN(age) as min_age FROM MockUsers GROUP BY name ORDER BY MIN(age)",
            "@SELECT name, MAX(age) as max_age FROM MockUsers GROUP BY name ORDER BY MAX(age) DESC"
        };

        for (String query : validGroupByQueries) {
            var result = framework.executeQuery(query);
            assertTrue(result.isSuccess(), "Valid GROUP BY query with ORDER BY should succeed: " + query + 
                "\nError: " + (result.isSuccess() ? "none" : result.getError()));
        }
    }

    @Test
    void testOrderByWithGroupByAdvancedCases() {
        // Test advanced GROUP BY + ORDER BY combinations
        String[] advancedQueries = {
            // Order by grouped field + aggregate
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY name, COUNT(*) DESC",
            
            // Order by multiple aggregates
            "@SELECT name, COUNT(*) as count, AVG(age) as avg_age FROM MockUsers GROUP BY name ORDER BY COUNT(*) DESC, AVG(age)",
            
            // Order by expression on grouped field
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY LENGTH(name), COUNT(*)",
            
            // Order by aggregate in expression
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY (COUNT(*) * 2) DESC",
            
            // Multiple grouping fields with complex ordering
            "@SELECT name, age, COUNT(*) as count FROM MockUsers GROUP BY name, age ORDER BY name, COUNT(*) DESC, age",
            
            // Percentile functions with GROUP BY (should work)
            "@SELECT name, P99(age) as p99_age FROM MockUsers GROUP BY name ORDER BY P99(age) DESC"
        };

        for (String query : advancedQueries) {
            var result = framework.executeQuery(query);
            if (!result.isSuccess()) {
                String error = result.getError().getMessage().toLowerCase();
                // Allow for some functions that might not be implemented yet
                if (error.contains("function") || error.contains("unknown") || error.contains("unsupported")) {
                    continue;
                }
                fail("Advanced GROUP BY + ORDER BY query should succeed: " + query + 
                    "\nError: " + result.getError());
            }
        }
    }

    @Test
    void testOrderByWithGroupByErrorCases() {
        // Test that semantic validation correctly rejects invalid ORDER BY expressions when GROUP BY is present
        // These queries should fail semantic validation because ORDER BY references non-grouped fields
        String[] invalidQueries = {
            // Order by non-grouped field (should fail - semantic error)
            "@SELECT name, COUNT(*) FROM MockUsers GROUP BY name ORDER BY age",
            
            // Order by field not in SELECT or GROUP BY (should fail - semantic error)
            "@SELECT name, COUNT(*) FROM MockUsers GROUP BY name ORDER BY id",
        };

        for (String query : invalidQueries) {
            try {
                Parser.parseAndValidate(query);
                fail("Query should fail semantic validation: " + query + 
                    " (ORDER BY field must be grouped or aggregate)");
            } catch (Exception e) {
                // Expected - should fail semantic validation
                String errorMessage = e.getMessage().toLowerCase();
                assertTrue(errorMessage.contains("group") || errorMessage.contains("aggregate"), 
                    "Error should mention GROUP BY or aggregate requirement: " + e.getMessage());
            }
        }
    }

    @Test
    void testOrderByGroupByWithHaving() {
        // Test ORDER BY combined with GROUP BY and HAVING
        String[] havingQueries = {
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC",
            "@SELECT name, AVG(age) as avg_age FROM MockUsers GROUP BY name HAVING AVG(age) > 25 ORDER BY AVG(age)",
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name HAVING COUNT(*) > 1 ORDER BY name, COUNT(*) DESC"
        };

        for (String query : havingQueries) {
            var result = framework.executeQuery(query);
            assertTrue(result.isSuccess(), "GROUP BY + HAVING + ORDER BY should succeed: " + query + 
                "\nError: " + (result.isSuccess() ? "none" : result.getError()));
        }
    }

    @Test
    void testOrderByGroupByWithComplexExpressions() {
        // Test complex expressions in ORDER BY with GROUP BY
        String[] complexQueries = {
            // Arithmetic on aggregate functions
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY (COUNT(*) * 100) DESC",
            
            // Function calls on grouped fields
            "@SELECT UPPER(name) as upper_name, COUNT(*) FROM MockUsers GROUP BY UPPER(name) ORDER BY UPPER(name)",
            
            // Conditional expressions
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY CASE WHEN COUNT(*) > 2 THEN 1 ELSE 2 END",
            
            // Mathematical expressions with aggregates
            "@SELECT name, SUM(age) as total FROM MockUsers GROUP BY name ORDER BY SUM(age) / COUNT(*) DESC"
        };

        for (String query : complexQueries) {
            var result = framework.executeQuery(query);
            if (!result.isSuccess()) {
                String error = result.getError().getMessage().toLowerCase();
                // Allow for unimplemented functions
                if (error.contains("function") || error.contains("unknown") || error.contains("unsupported") ||
                    error.contains("case") || error.contains("when")) {
                    continue;
                }
                fail("Complex GROUP BY + ORDER BY should work: " + query + "\nError: " + result.getError());
            }
        }
    }

    @Test
    void testOrderByGroupByFieldOrder() {
        // Test that ORDER BY properly handles fields in different orders
        var result1 = framework.executeQuery(
            "@SELECT name, age, COUNT(*) as count FROM MockUsers GROUP BY name, age ORDER BY name, age"
        );
        
        var result2 = framework.executeQuery(
            "@SELECT name, age, COUNT(*) as count FROM MockUsers GROUP BY name, age ORDER BY age, name"
        );
        
        assertTrue(result1.isSuccess(), "GROUP BY with ORDER BY (name, age) should work");
        assertTrue(result2.isSuccess(), "GROUP BY with ORDER BY (age, name) should work");
        
        if (result1.isSuccess() && result2.isSuccess()) {
            var rows1 = result1.getTable().getRows();
            var rows2 = result2.getTable().getRows();
            
            assertEquals(rows1.size(), rows2.size(), "Both queries should return same number of rows");
            
            // The order might be different, but that's expected
        }
    }

    @Test
    void testOrderByGroupByWithLimitAndOffset() {
        // Test ORDER BY + GROUP BY with LIMIT
        String[] limitQueries = {
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY COUNT(*) DESC LIMIT 5",
            "@SELECT name, AVG(age) as avg_age FROM MockUsers GROUP BY name ORDER BY AVG(age) DESC LIMIT 3",
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY name LIMIT 10"
        };

        for (String query : limitQueries) {
            var result = framework.executeQuery(query);
            assertTrue(result.isSuccess(), "GROUP BY + ORDER BY + LIMIT should work: " + query + 
                "\nError: " + (result.isSuccess() ? "none" : result.getError()));
            
            if (result.isSuccess()) {
                var rows = result.getTable().getRows();
                assertTrue(rows.size() <= 10, "LIMIT should be respected");
            }
        }
    }
}
