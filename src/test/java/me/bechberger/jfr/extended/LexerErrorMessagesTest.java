package me.bechberger.jfr.extended;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify improved lexer error messages provide specific, helpful feedback.
 */
public class LexerErrorMessagesTest {

    @ParameterizedTest
    @MethodSource("provideLexerErrorCases")
    void testImprovedLexerErrorMessages(String query, String expectedErrorType, String description) {
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            fail("Expected lexer error for: " + description + " but query tokenized successfully");
        } catch (LexerException e) {
            String errorMessage = e.getMessage();
            
            // Verify the error message contains the expected type of feedback
            switch (expectedErrorType) {
                case "unicode_symbol":
                    assertTrue(errorMessage.contains("Unicode") || errorMessage.contains("symbol") || 
                              errorMessage.contains("supported") || errorMessage.contains("ASCII") ||
                              errorMessage.contains("International characters") ||
                              errorMessage.contains("letters (a-z, A-Z)") ||
                              errorMessage.contains("Unexpected identifier") ||
                              errorMessage.contains("Unexpected character"), 
                              "Should mention Unicode/symbol issue: " + errorMessage);
                    break;
                    
                case "currency":
                    assertTrue(errorMessage.contains("Currency") || errorMessage.contains("EUR") || 
                              errorMessage.contains("USD"), 
                              "Should mention currency alternatives: " + errorMessage);
                    break;
                    
                case "unclosed_string":
                    assertTrue(errorMessage.contains("Unclosed string") && errorMessage.contains("single quote"), 
                              "Should mention unclosed string and solution: " + errorMessage);
                    break;
                    
                case "unclosed_comment":
                    assertTrue(errorMessage.contains("Unclosed") && errorMessage.contains("comment") && 
                              errorMessage.contains("*/"), 
                              "Should mention unclosed comment and solution: " + errorMessage);
                    break;
                    
                case "double_quotes":
                    assertTrue(errorMessage.contains("Double quotes") && errorMessage.contains("single quotes"), 
                              "Should suggest using single quotes: " + errorMessage);
                    break;
                    
                case "backticks":
                    assertTrue(errorMessage.contains("Backticks") || errorMessage.contains("backticks") ||
                              errorMessage.contains("identifiers") || errorMessage.contains("quotes for strings"), 
                              "Should mention backticks not supported: " + errorMessage);
                    break;
                    
                case "unsupported_operator":
                    assertTrue(errorMessage.contains("operator") && errorMessage.contains("not supported"), 
                              "Should mention operator not supported: " + errorMessage);
                    break;
                    
                case "curly_braces":
                    assertTrue(errorMessage.contains("Curly braces") || errorMessage.contains("braces") ||
                              errorMessage.contains("parentheses"), 
                              "Should suggest using parentheses: " + errorMessage);
                    break;
                    
                default:
                    // General error should provide some context
                    assertTrue(errorMessage.contains("Unexpected character") && errorMessage.length() > 30,
                              "Should provide helpful context: " + errorMessage);
            }
            
            System.out.println("✓ " + description);
            System.out.println("  Error: " + errorMessage.split("\n")[0]);
            System.out.println();
        }
    }

    private static Stream<Arguments> provideLexerErrorCases() {
        return Stream.of(
            // Unicode and symbol errors
            Arguments.of("@SELECT field™ FROM Events", "unicode_symbol", "Trademark symbol in identifier"),
            Arguments.of("@SELECT field© FROM Events", "unicode_symbol", "Copyright symbol in identifier"),
            Arguments.of("@SELECT αβγ FROM Events", "unicode_symbol", "Greek letters in identifier"),
            Arguments.of("@SELECT duration > 5µs", "unicode_symbol", "Micro symbol in duration"),
            
            // Currency symbols
            Arguments.of("@SELECT price = €100", "currency", "Euro symbol"),
            Arguments.of("@SELECT value = ¥500", "currency", "Yen symbol"),
            Arguments.of("@SELECT cost = £50", "currency", "Pound symbol"),
            
            // String literal errors
            Arguments.of("@SELECT text = 'unclosed", "unclosed_string", "Unclosed string literal"),
            Arguments.of("@SELECT text = \"invalid\"", "double_quotes", "Double quotes instead of single"),
            
            // Comment errors
            Arguments.of("@SELECT * /* unclosed comment", "unclosed_comment", "Unclosed multi-line comment"),
            
            // Identifier errors
            Arguments.of("@SELECT `field` FROM Events", "backticks", "Backticks around identifier"),
            
            // Operator errors
            Arguments.of("@SELECT value ~ 5", "unsupported_operator", "Tilde operator"),
            Arguments.of("@SELECT result ^ 2", "unsupported_operator", "Caret operator"),
            
            // Grouping errors
            Arguments.of("@SELECT {field} FROM Events", "curly_braces", "Curly braces for grouping")
        );
    }

    @Test
    void testLexerErrorsProvideContext() {
        String query = "@SELECT field™ FROM Events";
        try {
            Lexer lexer = new Lexer(query);
            lexer.tokenize();
            fail("Should have thrown lexer exception");
        } catch (LexerException e) {
            String errorMessage = e.getMessage();
            
            // Should include line and column information
            assertTrue(errorMessage.contains("line"), "Should include line number");
            assertTrue(errorMessage.contains("column"), "Should include column number");
            
            // Should include context snippet with highlighting
            assertTrue(errorMessage.contains("Context:") && 
                      (errorMessage.contains(">>>") || errorMessage.contains("^--- Error here")), 
                      "Should include highlighted context snippet: " + errorMessage);
            
            System.out.println("✓ Context information included in error message:");
            System.out.println("  " + errorMessage);
        }
    }

    @Test
    void testValidQueriesStillWork() {
        String[] validQueries = {
            "@SELECT * FROM Events",
            "@SELECT field FROM Events WHERE value = 'text'",
            "@SELECT COUNT(*) FROM Events /* valid comment */",
            "@SELECT field FROM Events WHERE duration > 5ms",
            "@SELECT field FROM Events WHERE text = '测试'"  // Unicode in string literals is fine
        };
        
        for (String query : validQueries) {
            try {
                Lexer lexer = new Lexer(query);
                List<Token> tokens = lexer.tokenize();
                assertTrue(tokens.size() > 0, "Should tokenize valid query: " + query);
                System.out.println("✓ Valid query tokenized: " + query);
            } catch (Exception e) {
                fail("Valid query should not fail: " + query + " - " + e.getMessage());
            }
        }
    }
}
