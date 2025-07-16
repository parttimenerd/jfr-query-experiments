package me.bechberger.jfr.extended;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

/**
 * Test cases for operator confusion, particularly the common == vs = mistake
 */
public class OperatorConfusionTest {

    /**
     * Test that double equals (==) operators are properly detected and provide helpful error messages
     */
    @ParameterizedTest
    @ValueSource(strings = {
        "@SELECT * FROM GarbageCollection WHERE duration == 1s",
        "@SELECT eventType FROM ThreadSleep WHERE startTime == '2021-01-01T00:00:00Z'",
        "@SELECT * FROM JavaExceptionThrow WHERE message == 'error'",
        "@SELECT * FROM ProfiledEvent WHERE stackTrace == null"
    })
    public void testDoubleEqualsOperatorConfusion(String query) {
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, query);
            parser.parse();
            
            fail("Expected ParserException for query with '==' operator: " + query);
        } catch (ParserException e) {
            String errorMessage = e.getMessage();
            
            // Verify the error message contains specific guidance about == vs =
            assertTrue(errorMessage.contains("Use single '=' for comparison, not '==' (double equals is not supported in this query language)"),
                "Error message should suggest using single '=' instead of '=='. Actual message: " + errorMessage);
            
            // Verify that the error indicates the problematic token
            assertTrue(errorMessage.contains("Unexpected token: EQUALS"),
                "Error message should indicate unexpected EQUALS token. Actual message: " + errorMessage);
            
        } catch (Exception e) {
            fail("Unexpected exception type: " + e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    /**
     * Test that single equals operator works correctly (no errors should be thrown)
     */
    @Test
    public void testSingleEqualsOperatorWorks() {
        String[] validQueries = {
            "SELECT * FROM GarbageCollection WHERE duration = 1s",
            "SELECT eventType FROM ThreadSleep WHERE startTime = '2021-01-01T00:00:00Z'",
            "SELECT * FROM JavaExceptionThrow WHERE message = 'error'"
        };
        
        for (String query : validQueries) {
            try {
                Lexer lexer = new Lexer(query);
                List<Token> tokens = lexer.tokenize();
                Parser parser = new Parser(tokens, query);
                parser.parse();
                
                // Should not throw any exception - single = is correct
            } catch (Exception e) {
                fail("Single equals operator should work without errors. Query: " + query + ", Error: " + e.getMessage());
            }
        }
    }

    /**
     * Test other operator confusion scenarios
     */
    @Test
    public void testOtherOperatorConfusion() {
        // Test incomplete operators or other common mistakes
        String[] problematicQueries = {
            "@SELECT * FROM GarbageCollection WHERE duration >",  // Incomplete comparison
            "@SELECT * FROM GarbageCollection WHERE duration !!= 1s",  // Invalid != variation
        };
        
        for (String query : problematicQueries) {
            try {
                Lexer lexer = new Lexer(query);
                List<Token> tokens = lexer.tokenize();
                Parser parser = new Parser(tokens, query);
                parser.parse();
                
                fail("Expected ParserException for problematic query: " + query);
            } catch (ParserException e) {
                // Expected - should provide helpful error message
                String errorMessage = e.getMessage();
                assertTrue(errorMessage.length() > 0, "Error message should not be empty");
                
            } catch (Exception e) {
                // Also acceptable - lexer might catch some of these
                assertTrue(e.getMessage().length() > 0, "Error message should not be empty");
            }
        }
    }
}
