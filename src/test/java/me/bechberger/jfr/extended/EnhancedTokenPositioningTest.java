package me.bechberger.jfr.extended;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for enhanced token positioning that provides from/to ranges for better error messages.
 * 
 * This test class validates that the enhanced Token class with fromPosition, toPosition,
 * endLine, and endColumn fields correctly improves error message precision, especially
 * for multi-character tokens and range highlighting.
 * 
 * @author Johannes Bechberger
 */
public class EnhancedTokenPositioningTest {

    @Test
    @DisplayName("Single-character token should have from equals to position")
    public void testSingleCharacterToken() {
        Token token = new Token(TokenType.SEMICOLON, ";", 1, 10, 10);
        
        assertEquals(10, token.fromPosition());
        assertEquals(10, token.toPosition());
        assertEquals(1, token.line());
        assertEquals(1, token.endLine());
        assertEquals(10, token.column());
        assertEquals(10, token.endColumn());
    }

    @Test
    @DisplayName("Multi-character token should have correct from/to range")
    public void testMultiCharacterToken() {
        Token token = new Token(TokenType.IDENTIFIER, "username", 1, 5, 5);
        
        assertEquals(5, token.fromPosition());
        assertEquals(12, token.toPosition()); // 5 + "username".length() - 1
        assertEquals(1, token.line());
        assertEquals(1, token.endLine());
        assertEquals(5, token.column());
        assertEquals(12, token.endColumn());
    }

    @Test
    @DisplayName("Token constructor with explicit to position should work correctly")
    public void testExplicitToPositionConstructor() {
        Token token = new Token(TokenType.STRING, "hello world", 1, 5, 5, 15, 1, 15);
        
        assertEquals(5, token.fromPosition());
        assertEquals(15, token.toPosition());
        assertEquals(1, token.line());
        assertEquals(1, token.endLine());
        assertEquals(5, token.column());
        assertEquals(15, token.endColumn());
    }

    @Test
    @DisplayName("Multiline token should have correct end line and column")
    public void testMultilineToken() {
        Token token = new Token(TokenType.STRING, "hello\nworld", 1, 5, 5, 15, 2, 5);
        
        assertEquals(5, token.fromPosition());
        assertEquals(1, token.line());
        assertEquals(2, token.endLine());
        assertEquals(5, token.column());
        assertEquals(5, token.endColumn());
    }

    @Test
    @DisplayName("QueryError should use token range for context generation")
    public void testQueryErrorWithTokenRange() {
        String query = "SELECT * FROM users WHERE name = \"invalid\"";
        Token errorToken = new Token(TokenType.STRING, "\"invalid\"", 1, 34, 33);
        QueryError error = new QueryError(
            errorToken,
            JFRQueryException.ErrorOrigin.LEXER,
            JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER,
            "Unexpected string: '\"invalid\"'",
            "Double quotes are not supported for strings",
            "Use single quotes instead: 'text'"
        );
        
        String context = error.generateContextSnippet(query, 3);
        
        // Should contain the query line
        assertTrue(context.contains("SELECT * FROM users WHERE name = \"invalid\""));
        
        // Should contain the error pointer spanning multiple characters
        assertTrue(context.contains("^"));
        
        // Should show the error range
        assertTrue(context.contains("Error here"));
    }

    @Test
    @DisplayName("Long token should have correct range calculation")
    public void testLongToken() {
        String longIdentifier = "very_long_identifier_name_that_spans_many_characters";
        Token token = new Token(TokenType.IDENTIFIER, longIdentifier, 1, 10, 10);
        
        assertEquals(10, token.fromPosition());
        assertEquals(10 + longIdentifier.length() - 1, token.toPosition());
        assertEquals(longIdentifier.length(), token.toPosition() - token.fromPosition() + 1);
    }

    @Test
    @DisplayName("Range error should show appropriate span in context")
    public void testRangeErrorContext() {
        String query = "SELECT COUNT(*) FROM events WHERE invalid_function_call(param1, param2) > 100";
        Token errorToken = new Token(TokenType.IDENTIFIER, "invalid_function_call", 1, 39, 38);
        QueryError error = new QueryError(
            errorToken,
            JFRQueryException.ErrorOrigin.PARSER,
            JFRQueryException.ErrorCategory.SEMANTIC_ERROR,
            "Unexpected identifier: 'invalid_function_call'",
            "Function 'invalid_function_call' is not recognized",
            "Check function name spelling or use a supported JFR function"
        );
        
        String context = error.generateContextSnippet(query, 3);
        
        // Should show the function name with proper range highlighting
        assertTrue(context.contains("invalid_function_call"));
        assertTrue(context.contains("^"));
        
        // Should indicate error presence
        assertTrue(context.contains("Error here"));
    }

    @Test
    @DisplayName("Error at end of line should not cause issues")
    public void testErrorAtEndOfLine() {
        String query = "SELECT * FROM events WHERE condition = @";
        Token errorToken = new Token(TokenType.IDENTIFIER, "@", 1, 40, 39);
        QueryError error = new QueryError(
            errorToken,
            JFRQueryException.ErrorOrigin.LEXER,
            JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER,
            "Unexpected identifier: '@'",
            "Unexpected character '@' at end of query",
            "Remove the '@' character or complete the parameter reference"
        );
        
        String context = error.generateContextSnippet(query, 3);
        
        // Should handle end-of-line errors gracefully
        assertTrue(context.contains("@"));
        assertTrue(context.contains("^"));
        assertTrue(context.contains("Error here"));
    }

    @Test
    @DisplayName("Backward compatibility with legacy position() method")
    @SuppressWarnings("deprecation")
    public void testBackwardCompatibility() {
        Token token = new Token(TokenType.IDENTIFIER, "test", 1, 5, 5);
        
        // Legacy position() should return fromPosition for compatibility
        assertEquals(token.fromPosition(), token.position());
        assertEquals(token.fromPosition(), token.column());
    }

    @Test
    @DisplayName("Token length calculation should be correct")
    public void testTokenLength() {
        Token shortToken = new Token(TokenType.IDENTIFIER, "x", 1, 1, 1);
        assertEquals(1, shortToken.length());
        
        Token longToken = new Token(TokenType.STRING, "hello world", 1, 1, 1);
        assertEquals(11, longToken.length());
    }

    @Test
    @DisplayName("Multiline token detection should work")
    public void testMultilineDetection() {
        Token singleLineToken = new Token(TokenType.STRING, "hello", 1, 1, 1, 5, 1, 5);
        assertFalse(singleLineToken.isMultiline());
        
        Token multiLineToken = new Token(TokenType.STRING, "hello\nworld", 1, 1, 1, 11, 2, 5);
        assertTrue(multiLineToken.isMultiline());
    }

    @Test
    @DisplayName("Position and range string formatting should be correct")
    public void testPositionFormatting() {
        Token token = new Token(TokenType.IDENTIFIER, "test", 2, 10, 15);
        
        assertTrue(token.getPositionString().contains("line 2"));
        assertTrue(token.getPositionString().contains("column 10"));
        
        assertTrue(token.getRangeString().contains("line 2"));
        assertTrue(token.getRangeString().contains("columns 10"));
    }
}
