package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.QuerySemanticValidator;
import me.bechberger.jfr.extended.engine.QuerySemanticValidator.QuerySemanticException;
import me.bechberger.jfr.extended.ast.ASTNodes.QueryNode;
import me.bechberger.jfr.extended.ast.ASTNodes.ProgramNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Demonstration test showing the fix for star (*) mixing validation.
 * 
 * This test verifies that:
 * 1. @SELECT *, COUNT(*) is rejected by semantic validation (not parser)
 * 2. @SELECT name, * works correctly
 * 3. @SELECT COUNT(*) works correctly (the * inside COUNT is not treated as SELECT *)
 */
class StarMixingDemoTest {
    
    private final QuerySemanticValidator validator = new QuerySemanticValidator();

    @Test
    @DisplayName("Demo: SELECT *, COUNT(*) should be rejected by semantic validation")
    void testStarWithCountIsSemanticallyInvalid() {
        String query = "@SELECT *, COUNT(*) FROM TestEvents";
        
        try {
            Parser parser = new Parser(query);
            ProgramNode program = parser.parse();
            
            // Should parse successfully (no syntax error)
            assertNotNull(program, "Query should parse successfully - this is not a syntax error");
            
            // Get the first statement which should be a QueryNode
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            
            // Should fail semantic validation
            QuerySemanticException exception = assertThrows(QuerySemanticException.class, () -> {
                validator.validate(queryNode);
            }, "Query should be rejected by semantic validation");
            
            // Verify the error message mentions the issue
            String errorMessage = exception.getMessage();
            assertTrue(errorMessage.contains("star") || errorMessage.contains("*"), 
                "Error message should mention star (*) mixing issue. Got: " + errorMessage);
                
        } catch (Exception e) {
            fail("Unexpected exception while parsing query: " + query + " - " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Demo: SELECT name, * should work correctly")
    void testNameWithStarIsValid() {
        String query = "@SELECT name, * FROM TestEvents";
        
        try {
            Parser parser = new Parser(query);
            ProgramNode program = parser.parse();
            
            // Should parse successfully
            assertNotNull(program, "Query should parse successfully");
            
            // Get the first statement which should be a QueryNode
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            
            // Should pass semantic validation
            assertDoesNotThrow(() -> {
                validator.validate(queryNode);
            }, "Query should pass semantic validation - mixing non-aggregate fields with * is allowed");
            
        } catch (Exception e) {
            fail("Unexpected exception while parsing or validating query: " + query + " - " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Demo: SELECT COUNT(*) should work correctly - * inside COUNT is not treated as SELECT *")
    void testCountStarIsValid() {
        String query = "@SELECT COUNT(*) FROM TestEvents";
        
        try {
            Parser parser = new Parser(query);
            ProgramNode program = parser.parse();
            
            // Should parse successfully
            assertNotNull(program, "Query should parse successfully");
            
            // Get the first statement which should be a QueryNode
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            
            // Should pass semantic validation
            assertDoesNotThrow(() -> {
                validator.validate(queryNode);
            }, "Query should pass semantic validation - COUNT(*) is a valid aggregate function");
            
        } catch (Exception e) {
            fail("Unexpected exception while parsing or validating query: " + query + " - " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Demo: SELECT COUNT(*), SUM(value) should work correctly")
    void testMultipleAggregatesWithStarIsValid() {
        String query = "@SELECT COUNT(*), SUM(value) FROM TestEvents";
        
        try {
            Parser parser = new Parser(query);
            ProgramNode program = parser.parse();
            
            // Should parse successfully
            assertNotNull(program, "Query should parse successfully");
            
            // Get the first statement which should be a QueryNode
            QueryNode queryNode = (QueryNode) program.statements().get(0);
            
            // Should pass semantic validation
            assertDoesNotThrow(() -> {
                validator.validate(queryNode);
            }, "Query should pass semantic validation - multiple aggregates including COUNT(*) are allowed");
            
        } catch (Exception e) {
            fail("Unexpected exception while parsing or validating query: " + query + " - " + e.getMessage());
        }
    }
}
