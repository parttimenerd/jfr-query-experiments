package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.engine.QuerySemanticValidator.QuerySemanticException;
import me.bechberger.jfr.extended.ast.ASTNodes.QueryNode;
import me.bechberger.jfr.extended.ast.ASTNodes.ProgramNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for semantic validation of GROUP BY constraints in SELECT and HAVING clauses.
 * 
 * Key Rules:
 * 1. Without GROUP BY: Cannot mix aggregate and non-aggregate expressions in SELECT
 * 2. With GROUP BY: Only grouped fields or aggregate functions allowed in SELECT and HAVING
 */
class InvalidAggregateValidationTest {
    
    private final QuerySemanticValidator validator = new QuerySemanticValidator();

    // ===== SELECT CLAUSE WITHOUT GROUP BY TESTS =====

    @ParameterizedTest
    @DisplayName("Mixed aggregate and non-aggregate expressions without GROUP BY should be rejected")
    @ValueSource(strings = {
        "@SELECT name, COUNT(*) FROM TestEvents",
        "@SELECT id, name, COUNT(*), SUM(value) FROM TestEvents",
        "@SELECT eventType, AVG(duration) FROM TestEvents",
        "@SELECT name, id, eventType, COUNT(*) FROM TestEvents",
        "@SELECT name, value, COUNT(*) FROM TestEvents",
        "@SELECT eventType, duration, SUM(value), AVG(duration) FROM TestEvents",
        "@SELECT *, COUNT(*) FROM TestEvents",
        "@SELECT *, SUM(value), AVG(duration) FROM TestEvents"
    })
    void testMixedAggregateAndNonAggregateWithoutGroupBy(String query) {
        assertInvalidQuery(query);
    }

    @ParameterizedTest
    @DisplayName("Pure aggregate expressions without GROUP BY should be allowed")
    @ValueSource(strings = {
        "@SELECT COUNT(*) FROM TestEvents",
        "@SELECT COUNT(*), SUM(value), AVG(duration) FROM TestEvents", 
        "@SELECT COUNT(*) as total FROM TestEvents",
        "@SELECT MAX(duration), MIN(duration) FROM TestEvents",
        "@SELECT P95(duration), P99(duration) FROM TestEvents"
    })
    void testPureAggregateWithoutGroupBy(String query) {
        assertValidQuery(query);
    }

    @ParameterizedTest
    @DisplayName("Pure non-aggregate expressions without GROUP BY should be allowed")
    @ValueSource(strings = {
        "@SELECT name FROM TestEvents",
        "@SELECT name, id, eventType FROM TestEvents",
        "@SELECT name as event_name, id as event_id FROM TestEvents",
        "@SELECT * FROM TestEvents",
        "@SELECT eventType, duration FROM TestEvents"
    })
    void testPureNonAggregateWithoutGroupBy(String query) {
        assertValidQuery(query);
    }

    // ===== SELECT CLAUSE WITH GROUP BY TESTS =====
    
    @ParameterizedTest
    @DisplayName("Valid SELECT with GROUP BY - only grouped fields and aggregates")
    @ValueSource(strings = {
        // Single grouped field with aggregates
        "@SELECT name, COUNT(*) FROM TestEvents GROUP BY name",
        "@SELECT eventType, COUNT(*), SUM(value) FROM TestEvents GROUP BY eventType", 
        "@SELECT name, MAX(duration), MIN(duration) FROM TestEvents GROUP BY name",
        
        // Multiple grouped fields
        "@SELECT name, eventType, COUNT(*) FROM TestEvents GROUP BY name, eventType",
        "@SELECT name, eventType FROM TestEvents GROUP BY name, eventType",
        
        // Only aggregates (no grouped fields needed)
        "@SELECT COUNT(*), SUM(value) FROM TestEvents GROUP BY name",
        "@SELECT AVG(duration), MAX(duration) FROM TestEvents GROUP BY eventType",
        
        // Grouped fields with aliases
        "@SELECT name as event_name, COUNT(*) as total FROM TestEvents GROUP BY name"
    })
    void testValidSelectWithGroupBy(String query) {
        assertValidQuery(query);
    }

    @ParameterizedTest
    @DisplayName("Invalid SELECT with GROUP BY - non-grouped fields should be rejected")
    @CsvSource({
        "'@SELECT name, id FROM TestEvents GROUP BY name', 'id not in GROUP BY'",
        "'@SELECT eventType, name, COUNT(*) FROM TestEvents GROUP BY eventType', 'name not in GROUP BY'", 
        "'@SELECT name, eventType, duration FROM TestEvents GROUP BY name', 'eventType and duration not in GROUP BY'",
        "'@SELECT id, name, eventType FROM TestEvents GROUP BY name, eventType', 'id not in GROUP BY'",
        "'@SELECT duration, COUNT(*) FROM TestEvents GROUP BY name', 'duration not in GROUP BY'"
    })
    void testInvalidSelectWithGroupBy(String query, String description) {
        assertInvalidQuery(query);
    }

    // ===== HAVING CLAUSE TESTS =====
    
    @ParameterizedTest
    @DisplayName("Valid HAVING with GROUP BY - only grouped fields and aggregates")
    @ValueSource(strings = {
        // Aggregate functions in HAVING
        "@SELECT name, COUNT(*) FROM TestEvents GROUP BY name HAVING COUNT(*) > 5",
        "@SELECT eventType, SUM(value) FROM TestEvents GROUP BY eventType HAVING SUM(value) > 100",
        "@SELECT name, AVG(duration) FROM TestEvents GROUP BY name HAVING AVG(duration) < 1000",
        
        // Grouped fields in HAVING
        "@SELECT name, COUNT(*) FROM TestEvents GROUP BY name HAVING name = 'test'",
        "@SELECT eventType, COUNT(*) FROM TestEvents GROUP BY eventType HAVING eventType LIKE 'gc%'",
        
        // Complex HAVING conditions
        "@SELECT name, COUNT(*) FROM TestEvents GROUP BY name HAVING COUNT(*) > 5 AND name != 'system'",
        "@SELECT eventType, AVG(duration) FROM TestEvents GROUP BY eventType HAVING AVG(duration) > 100 OR eventType = 'gc'"
    })
    void testValidHavingWithGroupBy(String query) {
        assertValidQuery(query);
    }

    @ParameterizedTest
    @DisplayName("Invalid HAVING with GROUP BY - non-grouped fields should be rejected")
    @CsvSource({
        "'@SELECT name, COUNT(*) FROM TestEvents GROUP BY name HAVING duration > 1000', 'duration not in GROUP BY'",
        "'@SELECT eventType, COUNT(*) FROM TestEvents GROUP BY eventType HAVING name = \"test\"', 'name not in GROUP BY'",
        "'@SELECT name, COUNT(*) FROM TestEvents GROUP BY name HAVING id > 100', 'id not in GROUP BY'",
        "'@SELECT eventType, COUNT(*) FROM TestEvents GROUP BY eventType HAVING duration > 100 AND eventType = \"gc\"', 'duration not in GROUP BY'"
    })
    void testInvalidHavingWithGroupBy(String query, String description) {
        assertInvalidQuery(query);
    }

    // ===== EDGE CASES AND COMPLEX SCENARIOS =====
    
    @ParameterizedTest
    @DisplayName("Complex valid queries with GROUP BY")
    @ValueSource(strings = {
        "@SELECT name, eventType, COUNT(*), AVG(duration) FROM TestEvents GROUP BY name, eventType HAVING COUNT(*) > 1",
        "@SELECT name, COUNT(*) FROM TestEvents GROUP BY name HAVING name = 'test1'",
        "@SELECT eventType, COUNT(*), SUM(value) FROM TestEvents GROUP BY eventType HAVING COUNT(*) > 5 AND SUM(value) > 1000"
    })
    void testComplexValidGroupByQueries(String query) {
        assertValidQuery(query);
    }

    @ParameterizedTest
    @DisplayName("Complex invalid queries with GROUP BY")
    @ValueSource(strings = {
        "@SELECT name, eventType, duration FROM TestEvents GROUP BY name",
        "@SELECT name, COUNT(*) FROM TestEvents GROUP BY name HAVING duration > 1000",
        "@SELECT name, eventType, duration FROM TestEvents GROUP BY name HAVING duration > 1000 AND eventType = 'gc'"
    })
    void testComplexInvalidGroupByQueries(String query) {
        assertInvalidQuery(query);
    }

    // ===== HELPER METHODS =====

    private void assertInvalidQuery(String query) {
        try {
            Parser parser = new Parser(query);
            ProgramNode program = parser.parse();
            
            // Get the first statement which should be a QueryNode
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            
            QuerySemanticException exception = assertThrows(QuerySemanticException.class, () -> {
                validator.validate(queryNode);
            }, "Query should have been rejected: " + query);
            
            // Verify the error message mentions the specific issue
            String errorMessage = exception.getMessage();
            assertTrue(errorMessage.contains("aggregate") || errorMessage.contains("GROUP BY"), 
                "Error message should mention aggregate functions or GROUP BY. Got: " + errorMessage);
                
        } catch (Exception e) {
            fail("Unexpected exception while parsing query: " + query + " - " + e.getMessage());
        }
    }

    private void assertValidQuery(String query) {
        try {
            Parser parser = new Parser(query);
            ProgramNode program = parser.parse();
            
            // Get the first statement which should be a QueryNode
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            
            assertDoesNotThrow(() -> {
                validator.validate(queryNode);
            }, "Query should have been accepted: " + query);
            
        } catch (Exception e) {
            fail("Unexpected exception while parsing query: " + query + " - " + e.getMessage());
        }
    }
}
