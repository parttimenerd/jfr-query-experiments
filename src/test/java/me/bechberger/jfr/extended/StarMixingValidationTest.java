package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.QuerySemanticValidator;
import me.bechberger.jfr.extended.engine.QuerySemanticValidator.QuerySemanticException;
import me.bechberger.jfr.extended.ast.ASTNodes.QueryNode;
import me.bechberger.jfr.extended.ast.ASTNodes.ProgramNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for validation of mixing star (*) with other expressions in SELECT clauses.
 * 
 * Key Rules:
 * 1. @SELECT *, COUNT(*) should fail in semantic validation (not parser)
 * 2. @SELECT name, * should work syntactically and semantically
 * 3. @SELECT * alone should continue to work
 */
class StarMixingValidationTest {
    
    private final QuerySemanticValidator validator = new QuerySemanticValidator();

    @ParameterizedTest
    @DisplayName("Mixing star (*) with aggregate functions should be rejected by semantic validation")
    @ValueSource(strings = {
        "@SELECT *, COUNT(*) FROM TestEvents",
        "@SELECT *, SUM(value) FROM TestEvents",
        "@SELECT *, AVG(duration), COUNT(*) FROM TestEvents",
        "@SELECT COUNT(*), * FROM TestEvents",
        "@SELECT *, COUNT(*), SUM(value) FROM TestEvents"
    })
    void testStarWithAggregatesShouldBeRejectedBySemantic(String query) {
        try {
            Parser parser = new Parser(query);
            ProgramNode program = parser.parse();
            
            // Should parse successfully (no syntax error)
            assertNotNull(program, "Query should parse successfully: " + query);
            
            // Get the first statement which should be a QueryNode
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            
            // Should fail semantic validation
            QuerySemanticException exception = assertThrows(QuerySemanticException.class, () -> {
                validator.validate(queryNode);
            }, "Query should be rejected by semantic validation: " + query);
            
            // Verify the error message mentions the issue
            String errorMessage = exception.getMessage();
            assertTrue(errorMessage.contains("aggregate") || errorMessage.contains("*") || errorMessage.contains("mixed"), 
                "Error message should mention the issue with mixing * and aggregates. Got: " + errorMessage);
                
        } catch (Exception e) {
            fail("Unexpected exception while parsing query: " + query + " - " + e.getMessage());
        }
    }

    @ParameterizedTest
    @DisplayName("Mixing star (*) with non-aggregate expressions should work")
    @ValueSource(strings = {
        "@SELECT name, * FROM TestEvents",
        "@SELECT *, name FROM TestEvents", 
        "@SELECT eventType, *, duration FROM TestEvents",
        "@SELECT *, name, eventType FROM TestEvents"
    })
    void testStarWithNonAggregatesShouldWork(String query) {
        try {
            Parser parser = new Parser(query);
            ProgramNode program = parser.parse();
            
            // Should parse successfully
            assertNotNull(program, "Query should parse successfully: " + query);
            
            // Get the first statement which should be a QueryNode  
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            
            // Should pass semantic validation
            assertDoesNotThrow(() -> {
                validator.validate(queryNode);
            }, "Query should pass semantic validation: " + query);
            
        } catch (Exception e) {
            fail("Unexpected exception while parsing or validating query: " + query + " - " + e.getMessage());
        }
    }

    @ParameterizedTest
    @DisplayName("Star (*) alone should continue to work")
    @ValueSource(strings = {
        "@SELECT * FROM TestEvents",
        "@SELECT * FROM TestEvents WHERE name = 'test'",
        "@SELECT * FROM TestEvents GROUP BY name",
        "@SELECT * FROM TestEvents ORDER BY duration"
    })
    void testStarAloneShouldWork(String query) {
        try {
            Parser parser = new Parser(query);
            ProgramNode program = parser.parse();
            
            // Should parse successfully
            assertNotNull(program, "Query should parse successfully: " + query);
            
            // Get the first statement which should be a QueryNode
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            
            // Should pass semantic validation
            assertDoesNotThrow(() -> {
                validator.validate(queryNode);
            }, "Query should pass semantic validation: " + query);
            
        } catch (Exception e) {
            fail("Unexpected exception while parsing or validating query: " + query + " - " + e.getMessage());
        }
    }
}
