package me.bechberger.jfr.extended;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test suite for token positioning and range highlighting improvements.
 * 
 * Validates the enhanced error message generation with precise token positioning
 * that makes error messages "slightly better" as requested.
 * 
 * @author Johannes Bechberger
 */
public class TokenPositioningTest {

    @Test
    @DisplayName("Single character tokens should have precise positioning")
    public void testSingleCharacterTokens() {
        // Test various single character tokens
        Token semicolon = new Token(TokenType.SEMICOLON, ";", 1, 10, 10);
        Token comma = new Token(TokenType.COMMA, ",", 1, 15, 15);
        Token asterisk = new Token(TokenType.STAR, "*", 1, 8, 8);
        
        assertEquals(10, semicolon.fromPosition());
        assertEquals(10, semicolon.toPosition());
        assertEquals(1, semicolon.length());
        
        assertEquals(15, comma.fromPosition());
        assertEquals(15, comma.toPosition());
        assertEquals(1, comma.length());
        
        assertEquals(8, asterisk.fromPosition());
        assertEquals(8, asterisk.toPosition());
        assertEquals(1, asterisk.length());
    }

    @Test
    @DisplayName("Multi-character tokens should have correct range")
    public void testMultiCharacterTokens() {
        Token selectKeyword = new Token(TokenType.SELECT, "SELECT", 1, 1, 1);
        Token identifier = new Token(TokenType.IDENTIFIER, "user_name", 1, 8, 8);
        Token stringLiteral = new Token(TokenType.STRING, "'hello world'", 1, 20, 20);
        
        // SELECT: positions 1-6 (length 6)
        assertEquals(1, selectKeyword.fromPosition());
        assertEquals(6, selectKeyword.toPosition());
        assertEquals(6, selectKeyword.length());
        
        // user_name: positions 8-16 (length 9)
        assertEquals(8, identifier.fromPosition());
        assertEquals(16, identifier.toPosition());
        assertEquals(9, identifier.length());
        
        // 'hello world': positions 20-32 (length 13)
        assertEquals(20, stringLiteral.fromPosition());
        assertEquals(32, stringLiteral.toPosition());
        assertEquals(13, stringLiteral.length());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "a", "ab", "abc", "identifier", "very_long_identifier_name", 
        "SELECT", "WHERE", "FROM", "'string'", "\"double_quoted\""
    })
    @DisplayName("Token length calculation should be consistent")
    public void testTokenLengthCalculation(String value) {
        Token token = new Token(TokenType.IDENTIFIER, value, 1, 1, 1);
        
        assertEquals(value.length(), token.length());
        assertEquals(1 + value.length() - 1, token.toPosition());
        assertEquals(value.length(), token.toPosition() - token.fromPosition() + 1);
    }

    @Test
    @DisplayName("Range highlighting should work for different token lengths")
    public void testRangeHighlighting() {
        String query = "SELECT username FROM users WHERE status = 'active'";
        
        // Short token (6 chars): SELECT
        Token shortToken = new Token(TokenType.SELECT, "SELECT", 1, 1, 0);
        QueryError shortError = createError(shortToken, "Keyword error");
        String shortContext = shortError.generateContextSnippet(query, 1);
        
        assertTrue(shortContext.contains("^"));
        assertTrue(shortContext.contains("~"));
        assertTrue(shortContext.contains("Error here"));
        assertFalse(shortContext.contains("characters)")); // No character count
        
        // Medium token (8 chars): username
        Token mediumToken = new Token(TokenType.IDENTIFIER, "username", 1, 8, 7);
        QueryError mediumError = createError(mediumToken, "Identifier error");
        String mediumContext = mediumError.generateContextSnippet(query, 1);
        
        assertTrue(mediumContext.contains("^"));
        assertTrue(mediumContext.contains("~"));
        assertTrue(mediumContext.contains("Error here"));
        
        // Long token (8 chars): 'active'
        Token longToken = new Token(TokenType.STRING, "'active'", 1, 44, 43);
        QueryError longError = createError(longToken, "String error");
        String longContext = longError.generateContextSnippet(query, 1);
        
        assertTrue(longContext.contains("^"));
        assertTrue(longContext.contains("Error here"));
    }

    @Test
    @DisplayName("Very long tokens should be handled gracefully")
    public void testVeryLongTokens() {
        String veryLongIdentifier = "this_is_an_extremely_long_identifier_name_that_goes_on_and_on_and_on";
        Token longToken = new Token(TokenType.IDENTIFIER, veryLongIdentifier, 1, 1, 0);
        
        assertEquals(veryLongIdentifier.length(), longToken.length());
        assertEquals(0 + veryLongIdentifier.length() - 1, longToken.toPosition());
        
        QueryError error = createError(longToken, "Very long identifier");
        String context = error.generateContextSnippet("SELECT " + veryLongIdentifier, 1);
        
        assertTrue(context.contains("^"));
        assertTrue(context.contains("Error here"));
        assertFalse(context.contains("characters)")); // No character count even for long tokens
    }

    @Test
    @DisplayName("Multiline tokens should have correct end positions")
    public void testMultilineTokens() {
        // Simulate a multiline string or comment
        Token multilineToken = new Token(
            TokenType.STRING, 
            "\"first line\\nsecond line\"", 
            1, 10,    // start: line 1, column 10
            10,       // fromPosition
            35,       // toPosition
            2, 25     // end: line 2, column 25
        );
        
        assertEquals(1, multilineToken.line());
        assertEquals(10, multilineToken.column());
        assertEquals(2, multilineToken.endLine());
        assertEquals(25, multilineToken.endColumn());
        assertEquals(10, multilineToken.fromPosition());
        assertEquals(35, multilineToken.toPosition());
        assertTrue(multilineToken.isMultiline());
        
        String rangeString = multilineToken.getRangeString();
        assertTrue(rangeString.contains("lines 1-2"));
        assertTrue(rangeString.contains("columns 10-25"));
    }

    @Test
    @DisplayName("Error context should show precise token spans")
    public void testPreciseTokenSpans() {
        // Test query with multiple problematic tokens
        String query = "SELECT name, age FROM users WHERE invalid_col = \"bad_quotes\" AND";
        
        // Error on double quoted string
        Token quotedString = new Token(TokenType.STRING, "\"bad_quotes\"", 1, 49, 48);
        QueryError quoteError = createError(quotedString, "Double quotes not supported");
        String quoteContext = quoteError.generateContextSnippet(query, 1);
        
        // Should highlight the exact quoted string
        assertTrue(quoteContext.contains("\"bad_quotes\""));
        assertTrue(quoteContext.contains("^"));
        assertTrue(quoteContext.contains("~"));
        
        // Error on trailing AND
        Token trailingAnd = new Token(TokenType.AND, "AND", 1, 64, 63);
        QueryError andError = createError(trailingAnd, "Incomplete condition");
        String andContext = andError.generateContextSnippet(query, 1);
        
        // Should highlight just the AND token
        assertTrue(andContext.contains("^"));
        assertTrue(andContext.contains("Error here"));
    }

    @Test
    @DisplayName("Range errors should highlight token spans correctly")
    public void testRangeErrors() {
        String query = "SELECT invalid_function_call(param1, param2) FROM events";
        
        Token startToken = new Token(TokenType.IDENTIFIER, "invalid_function_call", 1, 8, 7);
        Token endToken = new Token(TokenType.RPAREN, ")", 1, 45, 44);
        
        QueryError rangeError = new QueryError.Builder(JFRQueryException.ErrorOrigin.PARSER)
            .category(JFRQueryException.ErrorCategory.SEMANTIC_ERROR)
            .errorMessage("Unknown function")
            .context("Function not recognized")
            .suggestion("Use a supported function")
            .errorToken(startToken)
            .endToken(endToken)
            .originalQuery(query)
            .build();
        
        String context = rangeError.generateContextSnippet(query, 1);
        
        // Should show range highlighting from start to end token
        assertTrue(context.contains("^"));
        assertTrue(context.contains("Range error"));
        assertFalse(context.contains("characters)")); // No character count
    }

    @Test
    @DisplayName("Position strings should be human readable")
    public void testPositionStrings() {
        Token singleLineToken = new Token(TokenType.IDENTIFIER, "test", 5, 12, 50);
        
        String posString = singleLineToken.getPositionString();
        assertTrue(posString.contains("line 5"));
        assertTrue(posString.contains("column 12"));
        
        String rangeString = singleLineToken.getRangeString();
        assertTrue(rangeString.contains("line 5"));
        assertTrue(rangeString.contains("columns 12"));
        
        // Multiline token
        Token multilineToken = new Token(TokenType.STRING, "multi\\nline", 3, 8, 20, 35, 4, 4);
        String multiRangeString = multilineToken.getRangeString();
        assertTrue(multiRangeString.contains("lines 3-4"));
        assertTrue(multiRangeString.contains("columns 8-4"));
    }

    @Test
    @DisplayName("Legacy compatibility should be maintained")
    @SuppressWarnings("deprecation")
    public void testLegacyCompatibility() {
        Token token = new Token(TokenType.IDENTIFIER, "legacy_test", 2, 5, 10);
        
        // Legacy position() method should return fromPosition
        assertEquals(token.fromPosition(), token.position());
        
        // Legacy constructor should work
        Token legacyToken = Token.legacy(TokenType.SELECT, "SELECT", 1, 1, 0);
        assertEquals(0, legacyToken.fromPosition());
        assertEquals(5, legacyToken.toPosition()); // 0 + 6 - 1
        assertEquals(6, legacyToken.length());
    }

    @Test
    @DisplayName("Error messages should not include character counts")
    public void testNoCharacterCounts() {
        String query = "SELECT * FROM users WHERE name = \"invalid_string\"";
        Token errorToken = new Token(TokenType.STRING, "\"invalid_string\"", 1, 34, 33);
        
        QueryError error = createError(errorToken, "Double quotes not supported");
        String context = error.generateContextSnippet(query, 1);
        
        // Should have error highlighting but no character count
        assertTrue(context.contains("^"));
        assertTrue(context.contains("Error here"));
        assertFalse(context.contains("("));
        assertFalse(context.contains("characters"));
        assertFalse(context.contains(") characters"));
    }

    private QueryError createError(Token token, String message) {
        return new QueryError(
            token,
            JFRQueryException.ErrorOrigin.LEXER,
            JFRQueryException.ErrorCategory.UNEXPECTED_CHARACTER,
            message,
            "Error context",
            "Error suggestion"
        );
    }
}
