package me.bechberger.jfr.extended;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Arrays;
import java.util.List;

/**
 * Comprehensive test suite for the unified JFR query error system.
 * 
 * Tests the complete implementation of the user requirement:
 * "LexerException should have the same structure as ParserExceptions and have the same parent class,
 * they should be collected into the same structure"
 * 
 * @author Johannes Bechberger
 */
public class UnifiedErrorSystemTest {

    @Test
    @DisplayName("JFRQueryException should be the common parent for all query errors")
    public void testCommonParentClass() {
        String query = "SELECT * FROM invalid";
        Token errorToken = new Token(TokenType.IDENTIFIER, "invalid", 1, 15, 14);
        
        QueryError lexerError = new QueryError(
            errorToken,
            JFRQueryException.ErrorOrigin.LEXER,
            JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER,
            "Invalid character",
            "Character not allowed",
            "Use valid characters"
        );
        
        QueryError parserError = new QueryError(
            errorToken,
            JFRQueryException.ErrorOrigin.PARSER,
            JFRQueryException.ErrorCategory.UNEXPECTED_TOKEN,
            "Unexpected token",
            "Token not expected here",
            "Check syntax"
        );
        
        // Both should create JFRQueryException instances
        JFRQueryException lexerException = new JFRQueryException(lexerError, query);
        JFRQueryException parserException = new JFRQueryException(parserError, query);
        
        // Verify common parent
        assertTrue(lexerException instanceof JFRQueryException);
        assertTrue(parserException instanceof JFRQueryException);
        
        // Verify error classification
        assertTrue(lexerException.hasLexerErrors());
        assertFalse(lexerException.hasParserErrors());
        
        assertFalse(parserException.hasLexerErrors());
        assertTrue(parserException.hasParserErrors());
    }

    @Test
    @DisplayName("Multiple errors should be collected into the same structure")
    public void testUnifiedErrorCollection() {
        String query = "SELECT * FROM table WHERE condition = \"invalid\" AND missing_keyword";
        
        Token lexerErrorToken = new Token(TokenType.STRING, "\"invalid\"", 1, 40, 39);
        Token parserErrorToken = new Token(TokenType.IDENTIFIER, "missing_keyword", 1, 55, 54);
        
        QueryError lexerError = new QueryError(
            lexerErrorToken,
            JFRQueryException.ErrorOrigin.LEXER,
            JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER,
            "Double quotes not supported",
            "Use single quotes for strings",
            "Replace with 'invalid'"
        );
        
        QueryError parserError = new QueryError(
            parserErrorToken,
            JFRQueryException.ErrorOrigin.PARSER,
            JFRQueryException.ErrorCategory.MISSING_TOKEN,
            "Missing operator",
            "Expected comparison operator",
            "Add = or != operator"
        );
        
        // Create unified exception with multiple errors
        List<QueryError> errors = Arrays.asList(lexerError, parserError);
        JFRQueryException unifiedException = new JFRQueryException(errors, query);
        
        // Verify unified collection
        assertEquals(2, unifiedException.getErrors().size());
        assertTrue(unifiedException.hasLexerErrors());
        assertTrue(unifiedException.hasParserErrors());
        
        // Verify error filtering
        assertEquals(1, unifiedException.getLexerErrors().size());
        assertEquals(1, unifiedException.getParserErrors().size());
        
        assertEquals(JFRQueryException.ErrorOrigin.LEXER, unifiedException.getLexerErrors().get(0).getOrigin());
        assertEquals(JFRQueryException.ErrorOrigin.PARSER, unifiedException.getParserErrors().get(0).getOrigin());
    }

    @ParameterizedTest
    @EnumSource(JFRQueryException.ErrorOrigin.class)
    @DisplayName("All error origins should have consistent structure")
    public void testErrorOriginConsistency(JFRQueryException.ErrorOrigin origin) {
        Token errorToken = new Token(TokenType.IDENTIFIER, "test", 1, 1, 0);
        
        QueryError error = new QueryError(
            errorToken,
            origin,
            JFRQueryException.ErrorCategory.UNEXPECTED_TOKEN,
            "Test error message",
            "Test context",
            "Test suggestion"
        );
        
        JFRQueryException exception = new JFRQueryException(error, "SELECT test");
        
        // All error origins should have the same structure
        assertNotNull(exception.getErrors());
        assertEquals(1, exception.getErrors().size());
        assertEquals(origin, exception.getErrors().get(0).getOrigin());
        assertTrue(exception.getMessage().contains(origin.getDisplayName()));
        assertNotNull(exception.getOriginalQuery());
    }

    @ParameterizedTest
    @EnumSource(JFRQueryException.ErrorCategory.class)
    @DisplayName("All error categories should be supported")
    public void testErrorCategorySupport(JFRQueryException.ErrorCategory category) {
        Token errorToken = new Token(TokenType.IDENTIFIER, "test", 1, 1, 0);
        
        QueryError error = new QueryError(
            errorToken,
            JFRQueryException.ErrorOrigin.LEXER,
            category,
            "Test error for " + category.getDisplayName(),
            "Test context",
            "Test suggestion"
        );
        
        JFRQueryException exception = new JFRQueryException(error, "SELECT test");
        
        assertEquals(category, exception.getErrors().get(0).getCategory());
        assertNotNull(category.getDisplayName());
    }

    @Test
    @DisplayName("Error formatting should be consistent across origins")
    public void testConsistentErrorFormatting() {
        String query = "SELECT * FROM table WHERE invalid_syntax";
        Token errorToken = new Token(TokenType.IDENTIFIER, "invalid_syntax", 1, 27, 26);
        
        QueryError lexerError = new QueryError.Builder(JFRQueryException.ErrorOrigin.LEXER)
            .category(JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER)
            .errorMessage("Invalid character sequence")
            .context("Characters not valid in this context")
            .suggestion("Use valid identifier")
            .errorToken(errorToken)
            .originalQuery(query)
            .build();
        
        QueryError parserError = new QueryError.Builder(JFRQueryException.ErrorOrigin.PARSER)
            .category(JFRQueryException.ErrorCategory.UNEXPECTED_TOKEN)
            .errorMessage("Unexpected token in WHERE clause")
            .context("Token not expected at this position")
            .suggestion("Complete the WHERE condition")
            .errorToken(errorToken)
            .originalQuery(query)
            .build();
        
        String lexerMessage = new JFRQueryException(lexerError, query).getMessage();
        String parserMessage = new JFRQueryException(parserError, query).getMessage();
        
        // Both should have consistent structure
        assertTrue(lexerMessage.contains("Error at line 1, column 27"));
        assertTrue(parserMessage.contains("Error at line 1, column 27"));
        
        assertTrue(lexerMessage.contains("Problem:"));
        assertTrue(parserMessage.contains("Problem:"));
        
        assertTrue(lexerMessage.contains("Suggestion:"));
        assertTrue(parserMessage.contains("Suggestion:"));
        
        assertTrue(lexerMessage.contains("Context:"));
        assertTrue(parserMessage.contains("Context:"));
    }

    @Test
    @DisplayName("Token-based positioning should eliminate duplicate position data")
    public void testTokenBasedPositioning() {
        Token errorToken = new Token(TokenType.STRING, "\"test\"", 1, 10, 9);
        
        QueryError error = new QueryError(
            errorToken,
            JFRQueryException.ErrorOrigin.LEXER,
            JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER,
            "Double quotes not supported",
            "Use single quotes",
            "Replace with 'test'"
        );
        
        // Position information should come from token, not duplicated
        assertEquals(errorToken.line(), error.getLine());
        assertEquals(errorToken.column(), error.getColumn());
        assertEquals(errorToken.fromPosition(), error.getFromPosition());
        assertEquals(errorToken.toPosition(), error.getToPosition());
        
        // Error should not store separate position fields
        assertEquals(errorToken, error.getErrorToken());
    }

    @Test
    @DisplayName("Enhanced context generation should work for all error types")
    public void testEnhancedContextGeneration() {
        String query = "SELECT name, age FROM users WHERE status = \"active\" ORDER BY name";
        Token errorToken = new Token(TokenType.STRING, "\"active\"", 1, 45, 44);
        
        QueryError error = new QueryError(
            errorToken,
            JFRQueryException.ErrorOrigin.LEXER,
            JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER,
            "Double quotes not supported",
            "Use single quotes for strings",
            "Replace with 'active'"
        );
        
        String context = error.generateContextSnippet(query, 3);
        
        // Should show the query with error highlighting
        assertTrue(context.contains(query));
        assertTrue(context.contains("^"));
        assertTrue(context.contains("Error here"));
        
        // Should not show character count
        assertFalse(context.contains("characters)"));
    }

    @Test
    @DisplayName("Range errors should highlight token spans correctly")
    public void testRangeErrorHighlighting() {
        String query = "SELECT COUNT(*) FROM events WHERE invalid_function_call()";
        Token startToken = new Token(TokenType.IDENTIFIER, "invalid_function_call", 1, 35, 34);
        Token endToken = new Token(TokenType.RPAREN, ")", 1, 57, 56);
        
        QueryError error = new QueryError.Builder(JFRQueryException.ErrorOrigin.PARSER)
            .category(JFRQueryException.ErrorCategory.SEMANTIC_ERROR)
            .errorMessage("Unknown function")
            .context("Function not recognized")
            .suggestion("Use a valid JFR function")
            .errorToken(startToken)
            .endToken(endToken)
            .originalQuery(query)
            .build();
        
        String context = error.generateContextSnippet(query, 3);
        
        // Should show range highlighting
        assertTrue(context.contains("^"));
        assertTrue(context.contains("Range error"));
        
        // Should not show character count
        assertFalse(context.contains("characters)"));
    }

    @Test
    @DisplayName("Multiple error formatting should be properly structured")
    public void testMultipleErrorFormatting() {
        String query = "SELECT * FROM table WHERE col1 = \"val1\" AND col2 = \"val2\"";
        
        QueryError error1 = new QueryError(
            new Token(TokenType.STRING, "\"val1\"", 1, 34, 33),
            JFRQueryException.ErrorOrigin.LEXER,
            JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER,
            "First error",
            "First context",
            "First suggestion"
        );
        
        QueryError error2 = new QueryError(
            new Token(TokenType.STRING, "\"val2\"", 1, 50, 49),
            JFRQueryException.ErrorOrigin.LEXER,
            JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER,
            "Second error",
            "Second context",
            "Second suggestion"
        );
        
        JFRQueryException exception = new JFRQueryException(Arrays.asList(error1, error2), query);
        String message = exception.getMessage();
        
        // Should indicate multiple errors
        assertTrue(message.contains("Multiple syntax errors found (2)"));
        assertTrue(message.contains("1. "));
        assertTrue(message.contains("2. "));
        
        // Should contain both error messages
        assertTrue(message.contains("First error"));
        assertTrue(message.contains("Second error"));
    }

    @Test
    @DisplayName("Error addition should work correctly")
    public void testErrorAddition() {
        String query = "SELECT test";
        JFRQueryException exception = new JFRQueryException("Initial message", query);
        
        assertFalse(exception.hasErrors());
        assertEquals(0, exception.getErrors().size());
        
        QueryError error = new QueryError(
            new Token(TokenType.IDENTIFIER, "test", 1, 8, 7),
            JFRQueryException.ErrorOrigin.PARSER,
            JFRQueryException.ErrorCategory.UNEXPECTED_TOKEN,
            "Added error",
            "Added context",
            "Added suggestion"
        );
        
        exception.addError(error);
        
        assertTrue(exception.hasErrors());
        assertEquals(1, exception.getErrors().size());
        assertTrue(exception.hasParserErrors());
        assertFalse(exception.hasLexerErrors());
    }

    @Test
    @DisplayName("Backward compatibility should be maintained")
    public void testBackwardCompatibility() {
        String query = "SELECT test";
        Token errorToken = new Token(TokenType.IDENTIFIER, "test", 1, 8, 7);
        
        QueryError error = new QueryError(
            errorToken,
            JFRQueryException.ErrorOrigin.LEXER,
            JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER,
            "Test error",
            "Test context",
            "Test suggestion"
        );
        
        JFRQueryException exception = new JFRQueryException(error, query);
        
        // Legacy methods should still work
        assertEquals(errorToken, exception.getErrorToken());
        assertEquals(query, exception.getOriginalQuery());
        
        // Should be compatible with Exception interface
        assertNotNull(exception.getMessage());
        assertTrue(exception instanceof Exception);
    }
}
