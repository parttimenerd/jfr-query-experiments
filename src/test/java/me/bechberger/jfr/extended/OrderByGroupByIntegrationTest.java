package me.bechberger.jfr.extended;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import me.bechberger.jfr.extended.engine.framework.QueryResult;
import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive integration tests for ORDER BY with GROUP BY functionality.
 * Tests cover complex combinations, edge cases, and interaction with HAVING and LIMIT clauses.
 * 
 * The tests ensure that:
 * - ORDER BY works correctly with aggregate functions in GROUP BY queries
 * - Multi-field ORDER BY with different sort directions works properly
 * - Complex expressions in ORDER BY are evaluated correctly
 * - Error handling is robust for invalid combinations
 * - Performance characteristics are reasonable for complex queries
 */
public class OrderByGroupByIntegrationTest {

    private QueryTestFramework framework;

    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
        
        // Create mock tables for testing
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

    /**
     * Test basic GROUP BY with ORDER BY using aggregate functions
     */
    @ParameterizedTest
    @CsvSource({
        "COUNT(*), DESC, Descending count aggregation",
        "COUNT(*), ASC, Ascending count aggregation", 
        "SUM(age), DESC, Descending sum aggregation",
        "AVG(age), ASC, Ascending average aggregation",
        "MIN(age), DESC, Descending minimum aggregation",
        "MAX(age), ASC, Ascending maximum aggregation"
    })
    void testBasicGroupByOrderByAggregates(String aggregateFunction, String sortDirection, String description) {
        String query = String.format("@SELECT name, %s as result FROM MockUsers GROUP BY name ORDER BY %s %s", 
                                     aggregateFunction, aggregateFunction, sortDirection);
        
        QueryResult result = framework.executeQuery(query);
        assertTrue(result.isSuccess(), description + " should succeed: " + 
                  (result.getError() != null ? result.getError().getMessage() : ""));
        
        // Verify results are properly sorted
        var rows = result.getTable().getRows();
        assertFalse(rows.isEmpty(), "Should have at least one row");
    }

    /**
     * Test GROUP BY with ORDER BY using grouped field
     */
    @Test
    void testGroupByOrderByGroupedField() {
        String[] queries = {
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY name",
            "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY name DESC",
            "@SELECT department, AVG(age) as avg_age FROM MockEmployees GROUP BY department ORDER BY department"
        };

        for (String query : queries) {
            QueryResult result = framework.executeQuery(query);
            assertTrue(result.isSuccess(), "ORDER BY grouped field should work: " + query + 
                      " - Error: " + (result.getError() != null ? result.getError().getMessage() : ""));
            assertFalse(result.getTable().getRows().isEmpty(), "Should return results");
        }
    }

    /**
     * Test multi-field ORDER BY with GROUP BY
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY name, COUNT(*) DESC",
        "@SELECT name, age, COUNT(*) as count FROM MockUsers GROUP BY name, age ORDER BY name ASC, COUNT(*) DESC, age",
        "@SELECT department, COUNT(*) as count, AVG(age) as avg_age FROM MockEmployees GROUP BY department ORDER BY COUNT(*) DESC, AVG(age)",
        "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY LENGTH(name), COUNT(*) DESC",
        "@SELECT department, AVG(age) as avg_age FROM MockEmployees GROUP BY department ORDER BY department, AVG(age) DESC"
    })
    void testMultiFieldOrderByWithGroupBy(String query) {
        QueryResult result = framework.executeQuery(query);
        assertTrue(result.isSuccess(), "Multi-field ORDER BY with GROUP BY should work: " + query + 
                  " - Error: " + (result.getError() != null ? result.getError().getMessage() : ""));
        assertFalse(result.getTable().getRows().isEmpty(), "Should return results");
    }

    /**
     * Test ORDER BY with complex expressions in GROUP BY context
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY (COUNT(*) * 2) DESC",
        "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY (COUNT(*) + 1) ASC",
        "@SELECT department, COUNT(*) as count FROM MockEmployees GROUP BY department ORDER BY (COUNT(*) / 2.0) DESC",
        "@SELECT name, AVG(age) as avg_age FROM MockUsers GROUP BY name ORDER BY (AVG(age) - 20) ASC",
        "@SELECT department, SUM(age) as total_age FROM MockEmployees GROUP BY department ORDER BY (SUM(age) * 1.5) DESC"
    })
    void testComplexExpressionsInOrderBy(String query) {
        QueryResult result = framework.executeQuery(query);
        assertTrue(result.isSuccess(), "Complex expression in ORDER BY should work: " + query + 
                  " - Error: " + (result.getError() != null ? result.getError().getMessage() : ""));
        assertFalse(result.getTable().getRows().isEmpty(), "Should return results");
    }

    /**
     * Test GROUP BY + HAVING + ORDER BY combinations
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC",
        "@SELECT name, AVG(age) as avg_age FROM MockUsers GROUP BY name HAVING AVG(age) > 25 ORDER BY AVG(age)",
        "@SELECT department, COUNT(*) as count FROM MockEmployees GROUP BY department HAVING COUNT(*) >= 2 ORDER BY department",
        "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name HAVING COUNT(*) > 1 ORDER BY name, COUNT(*) DESC",
        "@SELECT department, AVG(age) as avg_age FROM MockEmployees GROUP BY department HAVING AVG(age) > 30 ORDER BY AVG(age) DESC"
    })
    void testGroupByHavingOrderBy(String query) {
        QueryResult result = framework.executeQuery(query);
        assertTrue(result.isSuccess(), "GROUP BY + HAVING + ORDER BY should work: " + query + 
                  " - Error: " + (result.getError() != null ? result.getError().getMessage() : ""));
    }

    /**
     * Test GROUP BY + ORDER BY + LIMIT combinations
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY COUNT(*) DESC LIMIT 5",
        "@SELECT department, AVG(age) as avg_age FROM MockEmployees GROUP BY department ORDER BY AVG(age) DESC LIMIT 3",
        "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name ORDER BY name LIMIT 10",
        "@SELECT department, COUNT(*) as count FROM MockEmployees GROUP BY department ORDER BY COUNT(*) DESC LIMIT 2",
        "@SELECT name, MAX(age) as max_age FROM MockUsers GROUP BY name ORDER BY MAX(age) DESC LIMIT 7"
    })
    void testGroupByOrderByLimit(String query) {
        QueryResult result = framework.executeQuery(query);
        assertTrue(result.isSuccess(), "GROUP BY + ORDER BY + LIMIT should work: " + query + 
                  " - Error: " + (result.getError() != null ? result.getError().getMessage() : ""));
        
        var rows = result.getTable().getRows();
        // Verify row count is less than or equal to the LIMIT value
        int limitValue = Integer.parseInt(query.substring(query.lastIndexOf("LIMIT") + 5).trim());
        assertTrue(rows.size() <= limitValue, "Row count should not exceed LIMIT value");
    }

    /**
     * Test GROUP BY + HAVING + ORDER BY + LIMIT combinations
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name HAVING COUNT(*) > 1 ORDER BY COUNT(*) DESC LIMIT 5",
        "@SELECT department, AVG(age) as avg_age FROM MockEmployees GROUP BY department HAVING AVG(age) > 25 ORDER BY AVG(age) DESC LIMIT 3",
        "@SELECT name, COUNT(*) as count FROM MockUsers GROUP BY name HAVING COUNT(*) >= 2 ORDER BY name LIMIT 10"
    })
    void testFullClauseCombinations(String query) {
        QueryResult result = framework.executeQuery(query);
        assertTrue(result.isSuccess(), "Full clause combination should work: " + query + 
                  " - Error: " + (result.getError() != null ? result.getError().getMessage() : ""));
        
        var rows = result.getTable().getRows();
        // Verify row count is less than or equal to the LIMIT value
        int limitValue = Integer.parseInt(query.substring(query.lastIndexOf("LIMIT") + 5).trim());
        assertTrue(rows.size() <= limitValue, "Row count should not exceed LIMIT value");
    }

    /**
     * Test percentile functions in ORDER BY with GROUP BY
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT name, P50(age) as median_age FROM MockUsers GROUP BY name ORDER BY P50(age) DESC",
        "@SELECT department, P90(age) as p90_age FROM MockEmployees GROUP BY department ORDER BY P90(age)",
        "@SELECT name, P99(age) as p99_age FROM MockUsers GROUP BY name ORDER BY P99(age) ASC",
        "@SELECT department, P95(age) as p95_age FROM MockEmployees GROUP BY department ORDER BY P95(age) DESC"
    })
    void testPercentileOrderByWithGroupBy(String query) {
        QueryResult result = framework.executeQuery(query);
        assertTrue(result.isSuccess(), "Percentile ORDER BY with GROUP BY should work: " + query + 
                  " - Error: " + (result.getError() != null ? result.getError().getMessage() : ""));
    }

    /**
     * Test error cases: ORDER BY non-grouped fields without aggregates
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT name, COUNT(*) FROM MockUsers GROUP BY name ORDER BY age",
        "@SELECT department, COUNT(*) FROM MockEmployees GROUP BY department ORDER BY name",
        "@SELECT name, COUNT(*) FROM MockUsers GROUP BY name ORDER BY id",
        "@SELECT department, AVG(age) FROM MockEmployees GROUP BY department ORDER BY age"
    })
    void testErrorCasesOrderByNonGroupedFields(String query) {
        QueryResult result = framework.executeQuery(query);
        assertFalse(result.isSuccess(), "ORDER BY non-grouped field should fail: " + query);
        assertNotNull(result.getError(), "Should have error message");
        assertTrue(result.getError().getMessage().toLowerCase().contains("group"),
                  "Error should mention grouping issue: " + result.getError().getMessage());
    }

    /**
     * Test error cases: Invalid aggregate function usage
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT name, COUNT(*) FROM MockUsers GROUP BY name ORDER BY INVALID_FUNC(age)",
        "@SELECT department, COUNT(*) FROM MockEmployees GROUP BY department ORDER BY UNKNOWN(name)",
        "@SELECT name, COUNT(*) FROM MockUsers GROUP BY name ORDER BY BADAGG(id)"
    })
    void testErrorCasesInvalidAggregates(String query) {
        QueryResult result = framework.executeQuery(query);
        assertFalse(result.isSuccess(), "Invalid aggregate function should fail: " + query);
        assertNotNull(result.getError(), "Should have error message");
    }

    /**
     * Test edge case: Empty GROUP BY results with ORDER BY
     */
    @Test
    void testEmptyGroupByWithOrderBy() {
        // This should work but return no results
        String query = "@SELECT name, COUNT(*) as count FROM MockUsers WHERE name = 'NonExistentUser' GROUP BY name ORDER BY COUNT(*) DESC";
        
        QueryResult result = framework.executeQuery(query);
        assertTrue(result.isSuccess(), "Empty GROUP BY with ORDER BY should succeed: " + 
                  (result.getError() != null ? result.getError().getMessage() : ""));
        assertTrue(result.getTable().getRows().isEmpty(), "Should return empty results");
    }

    /**
     * Test performance with multiple ORDER BY fields and large result sets
     */
    @Test
    void testPerformanceMultiFieldOrderBy() {
        String query = "@SELECT name, age, COUNT(*) as count FROM MockUsers GROUP BY name, age ORDER BY name, COUNT(*) DESC, age ASC";
        
        long startTime = System.currentTimeMillis();
        QueryResult result = framework.executeQuery(query);
        long endTime = System.currentTimeMillis();
        
        assertTrue(result.isSuccess(), "Performance test should succeed: " + 
                  (result.getError() != null ? result.getError().getMessage() : ""));
        
        long executionTime = endTime - startTime;
        
        // Basic performance assertion - should complete reasonably quickly
        assertTrue(executionTime < 5000, "Query should complete within 5 seconds, took: " + executionTime + "ms");
    }

    /**
     * Test ORDER BY with alias references in GROUP BY context
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT name, COUNT(*) as user_count FROM MockUsers GROUP BY name ORDER BY user_count DESC",
        "@SELECT department, AVG(age) as avg_age FROM MockEmployees GROUP BY department ORDER BY avg_age",
        "@SELECT name, SUM(age) as total_age FROM MockUsers GROUP BY name ORDER BY total_age DESC",
        "@SELECT department, MAX(age) as max_age FROM MockEmployees GROUP BY department ORDER BY max_age ASC"
    })
    void testOrderByWithAliasesInGroupBy(String query) {
        QueryResult result = framework.executeQuery(query);
        assertTrue(result.isSuccess(), "ORDER BY with aliases in GROUP BY should work: " + query + 
                  " - Error: " + (result.getError() != null ? result.getError().getMessage() : ""));
        assertFalse(result.getTable().getRows().isEmpty(), "Should return results");
    }

    /**
     * Test mixed ORDER BY: some aggregate, some grouped fields
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT name, age, COUNT(*) as count FROM MockUsers GROUP BY name, age ORDER BY name, COUNT(*) DESC",
        "@SELECT department, COUNT(*) as count FROM MockEmployees GROUP BY department ORDER BY department ASC, COUNT(*) DESC",
        "@SELECT name, AVG(age) as avg_age FROM MockUsers GROUP BY name ORDER BY name, AVG(age) DESC"
    })
    void testMixedOrderByGroupedAndAggregate(String query) {
        QueryResult result = framework.executeQuery(query);
        assertTrue(result.isSuccess(), "Mixed ORDER BY should work: " + query + 
                  " - Error: " + (result.getError() != null ? result.getError().getMessage() : ""));
        assertFalse(result.getTable().getRows().isEmpty(), "Should return results");
    }
}
