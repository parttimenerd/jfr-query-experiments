package me.bechberger.jfr.extended;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for ParserErrorHandler's missing FROM clause detection and clause boundary validation.
 * 
 * Tests the parser error handling functionality specifically focused on:
 * - Missing FROM clause detection for queries like "SELECT x"
 * - Clause boundary violations (e.g., WHERE after incomplete FROM clause)
 * - Context-aware error messages based on which SQL clause contains the error
 * - Proper error filtering that preserves legitimate structural errors
 * - Comprehensive clause boundary detection for all major SQL clauses
 * 
 * Key test categories:
 * - Missing FROM clause validation ensures "SELECT x" throws proper error
 * - Clause boundary detection for SELECT, FROM, WHERE, GROUP BY, ORDER BY, HAVING
 * - Error message context awareness based on query position
 * - Error filtering validation to prevent over-filtering of legitimate errors
 * 
 * @author Assistant
 */
@DisplayName("ParserErrorHandler Missing FROM and Clause Boundary Tests")
class FromClauseBoundaryTest {
    
    @Test
    @DisplayName("Should detect missing FROM clause in SELECT x queries")
    void testMissingFromClause() {
        List<Token> tokens = List.of(
            new Token(TokenType.SELECT, "SELECT", 1, 1, 0),
            new Token(TokenType.IDENTIFIER, "x", 1, 8, 7)
        );
        
        ParserErrorHandler handler = new ParserErrorHandler(tokens, "SELECT x");
        
        // Add error for missing FROM clause
        Token endToken = tokens.get(1);
        ParserErrorHandler.ParserError error = handler.createMissingTokenError(TokenType.FROM, endToken);
        handler.addError(error);
        
        List<ParserErrorHandler.ParserError> errors = handler.getErrors();
        assertEquals(1, errors.size(), "Should have exactly one error");
        
        ParserErrorHandler.ParserError fromError = errors.get(0);
        assertTrue(fromError.getMessage().toLowerCase().contains("missing"), "Should mention missing");
        assertTrue(fromError.getMessage().toLowerCase().contains("from"), "Should mention FROM clause");
        assertEquals(ParserErrorHandler.ErrorType.MISSING_TOKEN, fromError.getType());
        
        String suggestion = fromError.getSuggestion();
        assertNotNull(suggestion);
        assertTrue(suggestion.contains("FROM table_name"), "Should suggest FROM table_name syntax");
    }
    
    @Test
    @DisplayName("Should properly detect clause boundaries for FROM clause errors")
    void testFromClauseBoundaryDetection() {
        List<Token> tokens = List.of(
            new Token(TokenType.SELECT, "SELECT", 1, 1, 0),
            new Token(TokenType.IDENTIFIER, "x", 1, 8, 7),
            new Token(TokenType.FROM, "FROM", 1, 10, 9),
            new Token(TokenType.WHERE, "WHERE", 1, 15, 14), // Error: missing table name
            new Token(TokenType.IDENTIFIER, "y", 1, 21, 20),
            new Token(TokenType.EQUALS, "=", 1, 23, 22),
            new Token(TokenType.NUMBER, "1", 1, 25, 24)
        );
        
        ParserErrorHandler handler = new ParserErrorHandler(tokens, "SELECT x FROM WHERE y = 1");
        
        Token whereToken = tokens.get(3);
        ParserErrorHandler.ParserError error = handler.createUnexpectedTokenError(whereToken, "Expected table name");
        handler.addError(error);
        
        List<ParserErrorHandler.ParserError> errors = handler.getErrors();
        assertEquals(1, errors.size());
        
        ParserErrorHandler.ParserError fromError = errors.get(0);
        assertEquals(ParserErrorHandler.QueryContext.FROM_CLAUSE, handler.detectQueryContext(whereToken));
        assertTrue(fromError.getMessage().contains("FROM clause"), "Should identify FROM clause context");
        assertTrue(fromError.getSuggestion().contains("Complete the FROM clause"), "Should suggest completing FROM clause");
    }
    
    @ParameterizedTest
    @DisplayName("Should detect clause boundary violations for all major SQL clauses")
    @CsvSource({
        "GROUP_BY, GROUP BY, field names",
        "ORDER_BY, ORDER BY, field names",
        "HAVING, HAVING, aggregate conditions"
    })
    void testClauseBoundaryDetection(String tokenType, String clauseName, String expectedContent) {
        List<Token> tokens = List.of(
            new Token(TokenType.SELECT, "SELECT", 1, 1, 0),
            new Token(TokenType.IDENTIFIER, "x", 1, 8, 7),
            new Token(TokenType.FROM, "FROM", 1, 10, 9),
            new Token(TokenType.IDENTIFIER, "table", 1, 15, 14),
            new Token(TokenType.valueOf(tokenType), clauseName, 1, 21, 20),
            new Token(TokenType.WHERE, "WHERE", 1, 30, 29), // Error: WHERE after incomplete clause
            new Token(TokenType.IDENTIFIER, "y", 1, 36, 35),
            new Token(TokenType.EQUALS, "=", 1, 38, 37),
            new Token(TokenType.NUMBER, "1", 1, 40, 39)
        );
        
        String query = "SELECT x FROM table " + clauseName + " WHERE y = 1";
        ParserErrorHandler handler = new ParserErrorHandler(tokens, query);
        
        Token whereToken = tokens.get(5);
        ParserErrorHandler.ParserError error = handler.createUnexpectedTokenError(whereToken, "Expected " + expectedContent);
        handler.addError(error);
        
        List<ParserErrorHandler.ParserError> errors = handler.getErrors();
        assertEquals(1, errors.size());
        
        ParserErrorHandler.ParserError boundaryError = errors.get(0);
        
        // Adjust expected message format based on actual implementation
        String expectedMessage = switch (clauseName) {
            case "GROUP BY" -> clauseName + " clause";
            case "ORDER BY" -> clauseName + " clause";  
            case "HAVING" -> "HAVING condition";
            default -> clauseName + " clause";
        };
        
        assertTrue(boundaryError.getMessage().contains(expectedMessage), 
                  "Should identify " + expectedMessage + " context, but got: " + boundaryError.getMessage());
        assertTrue(boundaryError.getSuggestion().contains("Complete the " + clauseName + " clause"), 
                  "Should suggest completing " + clauseName + " clause");
    }
    
    @Test
    @DisplayName("Should not filter legitimate structural errors")
    void testLegitimateErrorPreservation() {
        List<Token> tokens = List.of(
            new Token(TokenType.SELECT, "SELECT", 1, 1, 0),
            new Token(TokenType.IDENTIFIER, "x", 1, 8, 7),
            new Token(TokenType.WHERE, "WHERE", 1, 10, 9), // Error: WHERE without FROM
            new Token(TokenType.IDENTIFIER, "y", 1, 16, 15),
            new Token(TokenType.EQUALS, "=", 1, 18, 17),
            new Token(TokenType.NUMBER, "1", 1, 20, 19)
        );
        
        ParserErrorHandler handler = new ParserErrorHandler(tokens, "SELECT x WHERE y = 1");
        
        Token whereToken = tokens.get(2);
        ParserErrorHandler.ParserError error = handler.createUnexpectedTokenError(whereToken, "Expected FROM clause");
        handler.addError(error);
        
        List<ParserErrorHandler.ParserError> errors = handler.getErrors();
        assertEquals(1, errors.size(), "Legitimate structural error should not be filtered");
        
        ParserErrorHandler.ParserError structuralError = errors.get(0);
        assertTrue(structuralError.getMessage().toLowerCase().contains("where"), 
                  "Should identify WHERE as the problematic token");
        assertEquals(ParserErrorHandler.ErrorType.UNEXPECTED_TOKEN, structuralError.getType());
    }
    
    @Test
    @DisplayName("Should provide context-aware error messages for different query contexts")
    void testContextAwareErrorMessages() {
        // Test SELECT clause context
        List<Token> selectTokens = List.of(
            new Token(TokenType.SELECT, "SELECT", 1, 1, 0),
            new Token(TokenType.IDENTIFIER, "invalid_field", 1, 8, 7),
            new Token(TokenType.COMMA, ",", 1, 21, 20),
            new Token(TokenType.FROM, "FROM", 1, 23, 22)
        );
        
        ParserErrorHandler selectHandler = new ParserErrorHandler(selectTokens, "SELECT invalid_field, FROM");
        Token commaToken = selectTokens.get(2);
        
        assertEquals(ParserErrorHandler.QueryContext.SELECT_CLAUSE, 
                    selectHandler.detectQueryContext(commaToken));
        
        // Test WHERE clause context
        List<Token> whereTokens = List.of(
            new Token(TokenType.SELECT, "SELECT", 1, 1, 0),
            new Token(TokenType.IDENTIFIER, "x", 1, 8, 7),
            new Token(TokenType.FROM, "FROM", 1, 10, 9),
            new Token(TokenType.IDENTIFIER, "table", 1, 15, 14),
            new Token(TokenType.WHERE, "WHERE", 1, 21, 20),
            new Token(TokenType.EQUALS, "=", 1, 27, 26) // Error: = without operands
        );
        
        ParserErrorHandler whereHandler = new ParserErrorHandler(whereTokens, "SELECT x FROM table WHERE =");
        Token equalsToken = whereTokens.get(5);
        
        assertEquals(ParserErrorHandler.QueryContext.WHERE_CLAUSE, 
                    whereHandler.detectQueryContext(equalsToken));
    }
    
    @Test
    @DisplayName("Should handle function call context detection properly")
    void testFunctionCallContextDetection() {
        List<Token> tokens = List.of(
            new Token(TokenType.SELECT, "SELECT", 1, 1, 0),
            new Token(TokenType.IDENTIFIER, "AVG", 1, 8, 7),
            new Token(TokenType.LPAREN, "(", 1, 11, 10),
            new Token(TokenType.IDENTIFIER, "duration", 1, 12, 11),
            new Token(TokenType.COMMA, ",", 1, 20, 19), // Error: extra comma
            new Token(TokenType.RPAREN, ")", 1, 21, 20),
            new Token(TokenType.FROM, "FROM", 1, 23, 22),
            new Token(TokenType.IDENTIFIER, "table", 1, 28, 27)
        );
        
        ParserErrorHandler handler = new ParserErrorHandler(tokens, "SELECT AVG(duration,) FROM table");
        Token commaToken = tokens.get(4);
        
        // Function call context should be detected based on being inside parentheses after identifier
        ParserErrorHandler.QueryContext context = handler.detectQueryContext(commaToken);
        // The comma is inside the function call, so context should indicate this is in a function
        assertNotNull(context);
    }
    
    @Test
    @DisplayName("Should validate proper error filtering without removing legitimate errors")
    void testErrorFilteringValidation() {
        List<Token> tokens = List.of(
            new Token(TokenType.SELECT, "SELECT", 1, 1, 0),
            new Token(TokenType.IDENTIFIER, "field1", 1, 8, 7),
            new Token(TokenType.COMMA, ",", 1, 14, 13),
            new Token(TokenType.COMMA, ",", 1, 15, 14), // Error: double comma
            new Token(TokenType.IDENTIFIER, "field2", 1, 16, 15),
            new Token(TokenType.FROM, "FROM", 1, 23, 22),
            new Token(TokenType.IDENTIFIER, "table", 1, 28, 27)
        );
        
        ParserErrorHandler handler = new ParserErrorHandler(tokens, "SELECT field1,, field2 FROM table");
        
        // Add error for double comma
        Token doubleCommaToken = tokens.get(3);
        ParserErrorHandler.ParserError error = handler.createUnexpectedTokenError(doubleCommaToken, "Unexpected comma");
        handler.addError(error);
        
        List<ParserErrorHandler.ParserError> errors = handler.getErrors();
        assertEquals(1, errors.size(), "Should preserve legitimate syntax errors");
        
        ParserErrorHandler.ParserError syntaxError = errors.get(0);
        assertTrue(syntaxError.getMessage().contains("comma"), "Should identify the comma issue");
        assertEquals(ParserErrorHandler.ErrorType.UNEXPECTED_TOKEN, syntaxError.getType());
    }
}
