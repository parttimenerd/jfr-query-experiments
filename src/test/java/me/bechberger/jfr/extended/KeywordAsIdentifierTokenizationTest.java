package me.bechberger.jfr.extended;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

/**
 * Test suite for verifying that keywords can be used as parts of identifiers
 * and that the lexer correctly applies word boundary checking for keywords.
 * 
 * This addresses the issue where "outerValue" was being incorrectly tokenized
 * as OUTER + Value instead of a single IDENTIFIER token.
 * 
 * @author Enhanced Testing Framework
 * @since 3.0
 */
@DisplayName("Keyword as Identifier Tokenization Tests")
public class KeywordAsIdentifierTokenizationTest {
    
    // ===== CORE FUNCTIONALITY TESTS =====
    
    @Test
    @DisplayName("outerValue should be tokenized as single IDENTIFIER, not OUTER + Value")
    void testOuterValueTokenization() throws LexerException {
        // Arrange
        String input = "outerValue";
        Lexer lexer = new Lexer(input);
        
        // Act
        List<Token> tokens = lexer.tokenize();
        
        // Assert
        assertEquals(2, tokens.size(), "Should have IDENTIFIER + EOF tokens");
        
        Token identifierToken = tokens.get(0);
        assertEquals(TokenType.IDENTIFIER, identifierToken.type(), 
            "outerValue should be tokenized as IDENTIFIER");
        assertEquals("outerValue", identifierToken.value(), 
            "Token value should be the complete outerValue string");
        
        Token eofToken = tokens.get(1);
        assertEquals(TokenType.EOF, eofToken.type(), "Second token should be EOF");
    }
    
    @ParameterizedTest
    @DisplayName("Keywords followed by letters should be treated as identifiers")
    @ValueSource(strings = {
        "outerValue", "innerCount", "leftMargin", "rightBorder", "fullName",
        "countTotal", "sumValue", "avgScore", "minValue", "maxLength",
        "selectAll", "fromClause", "whereCondition", "orderByColumn",
        "joinTable", "groupByField", "havingClause", "limitRows"
    })
    void testKeywordPrefixedIdentifiers(String input) throws LexerException {
        // Arrange
        Lexer lexer = new Lexer(input);
        
        // Act
        List<Token> tokens = lexer.tokenize();
        
        // Assert
        assertEquals(2, tokens.size(), "Should have IDENTIFIER + EOF tokens for: " + input);
        assertEquals(TokenType.IDENTIFIER, tokens.get(0).type(), 
            input + " should be tokenized as single IDENTIFIER");
        assertEquals(input, tokens.get(0).value(), 
            "Token value should match input for: " + input);
    }
    
    @ParameterizedTest
    @DisplayName("Standalone keywords should still be tokenized as keywords")
    @CsvSource({
        "OUTER, OUTER",
        "INNER, INNER", 
        "LEFT, LEFT",
        "RIGHT, RIGHT",
        "FULL, FULL",
        "COUNT, IDENTIFIER",  // COUNT is not in our keyword list, should be IDENTIFIER
        "SUM, IDENTIFIER",    // SUM is not in our keyword list, should be IDENTIFIER
        "SELECT, SELECT",
        "FROM, FROM",
        "WHERE, WHERE"
    })
    void testStandaloneKeywords(String input, String expectedTokenType) throws LexerException {
        // Arrange
        Lexer lexer = new Lexer(input);
        
        // Act
        List<Token> tokens = lexer.tokenize();
        
        // Assert
        assertEquals(2, tokens.size(), "Should have keyword/identifier + EOF tokens");
        TokenType expected = TokenType.valueOf(expectedTokenType);
        assertEquals(expected, tokens.get(0).type(), 
            input + " should be tokenized as " + expectedTokenType);
        assertEquals(input, tokens.get(0).value(), 
            "Token value should match input");
    }
    
    // ===== COMPLEX EXPRESSION TESTS =====
    
    @Test
    @DisplayName("Field references with keyword prefixes should work in SQL expressions")
    void testFieldReferencesInSQLExpressions() throws LexerException {
        // Arrange
        String input = "SELECT o.outerValue, i.innerCount FROM OuterEvents o";
        Lexer lexer = new Lexer(input);
        
        // Act
        List<Token> tokens = lexer.tokenize();
        
        // Assert
        // Find the outerValue and innerCount tokens
        boolean foundOuterValue = false;
        boolean foundInnerCount = false;
        
        for (Token token : tokens) {
            if (token.type() == TokenType.IDENTIFIER) {
                if ("outerValue".equals(token.value())) {
                    foundOuterValue = true;
                } else if ("innerCount".equals(token.value())) {
                    foundInnerCount = true;
                }
            }
        }
        
        assertTrue(foundOuterValue, "outerValue should be found as single IDENTIFIER token");
        assertTrue(foundInnerCount, "innerCount should be found as single IDENTIFIER token");
    }
    
    @Test
    @DisplayName("Mixed keywords and keyword-prefixed identifiers should tokenize correctly")
    void testMixedKeywordsAndIdentifiers() throws LexerException {
        // Arrange
        String input = "outerValue, OUTER, innerCount, INNER";
        Lexer lexer = new Lexer(input);
        
        // Act
        List<Token> tokens = lexer.tokenize();
        
        // Assert
        // Extract non-punctuation, non-EOF tokens
        List<Token> contentTokens = tokens.stream()
            .filter(t -> t.type() != TokenType.COMMA && t.type() != TokenType.EOF)
            .toList();
        
        assertEquals(4, contentTokens.size(), "Should have 4 content tokens");
        
        // Check each token
        assertEquals(TokenType.IDENTIFIER, contentTokens.get(0).type());
        assertEquals("outerValue", contentTokens.get(0).value());
        
        assertEquals(TokenType.OUTER, contentTokens.get(1).type());
        assertEquals("OUTER", contentTokens.get(1).value());
        
        assertEquals(TokenType.IDENTIFIER, contentTokens.get(2).type());
        assertEquals("innerCount", contentTokens.get(2).value());
        
        assertEquals(TokenType.INNER, contentTokens.get(3).type());
        assertEquals("INNER", contentTokens.get(3).value());
    }
    
    // ===== EDGE CASE TESTS =====
    
    @ParameterizedTest
    @DisplayName("Keywords with underscores should be treated as identifiers")
    @ValueSource(strings = {
        "outer_value", "inner_count", "left_margin", "right_border",
        "select_all", "from_table", "where_clause"
    })
    void testKeywordWithUnderscores(String input) throws LexerException {
        // Arrange
        Lexer lexer = new Lexer(input);
        
        // Act
        List<Token> tokens = lexer.tokenize();
        
        // Assert
        assertEquals(2, tokens.size(), "Should have IDENTIFIER + EOF tokens");
        assertEquals(TokenType.IDENTIFIER, tokens.get(0).type(), 
            input + " should be tokenized as IDENTIFIER");
        assertEquals(input, tokens.get(0).value(), 
            "Token value should match input");
    }
    
    @ParameterizedTest
    @DisplayName("Keywords with numbers should be treated as identifiers") 
    @ValueSource(strings = {
        "outer1", "inner2", "left3", "right4", "select5", "from6"
    })
    void testKeywordWithNumbers(String input) throws LexerException {
        // Arrange
        Lexer lexer = new Lexer(input);
        
        // Act
        List<Token> tokens = lexer.tokenize();
        
        // Assert
        assertEquals(2, tokens.size(), "Should have IDENTIFIER + EOF tokens");
        assertEquals(TokenType.IDENTIFIER, tokens.get(0).type(), 
            input + " should be tokenized as IDENTIFIER");
        assertEquals(input, tokens.get(0).value(), 
            "Token value should match input");
    }
    
    @Test
    @DisplayName("Case sensitivity should be preserved in identifiers")
    void testCaseSensitivityInIdentifiers() throws LexerException {
        // Arrange
        String[] inputs = {"outerValue", "OuterValue", "OUTERVALUE", "outerVALUE"};
        
        for (String input : inputs) {
            // Act
            Lexer lexer = new Lexer(input);
            List<Token> tokens = lexer.tokenize();
            
            // Assert
            assertEquals(2, tokens.size(), "Should have IDENTIFIER + EOF tokens for: " + input);
            assertEquals(TokenType.IDENTIFIER, tokens.get(0).type(), 
                input + " should be tokenized as IDENTIFIER");
            assertEquals(input, tokens.get(0).value(), 
                "Token value should preserve case for: " + input);
        }
    }
    
    // ===== REGRESSION TESTS =====
    
    @Test
    @DisplayName("Regression test: Complex JOIN query with keyword-prefixed fields")
    void testComplexJoinQueryRegression() throws LexerException {
        // Arrange - This is the type of query that was failing before
        String input = """
            SELECT 
                o.category,
                o.outerValue,
                COUNT(i.innerValue) as innerCount,
                SUM(i.innerValue) as innerSum
            FROM OuterEvents o
            LEFT JOIN InnerEvents i ON o.category = i.category
            GROUP BY o.category, o.outerValue
            ORDER BY o.category
            """;
        
        Lexer lexer = new Lexer(input);
        
        // Act
        List<Token> tokens = lexer.tokenize();
        
        // Assert
        // Verify that outerValue and innerValue are single IDENTIFIER tokens
        long outerValueCount = tokens.stream()
            .filter(t -> t.type() == TokenType.IDENTIFIER && "outerValue".equals(t.value()))
            .count();
        
        long innerValueCount = tokens.stream()
            .filter(t -> t.type() == TokenType.IDENTIFIER && "innerValue".equals(t.value()))
            .count();
        
        long innerCountCount = tokens.stream()
            .filter(t -> t.type() == TokenType.IDENTIFIER && "innerCount".equals(t.value()))
            .count();
        
        long innerSumCount = tokens.stream()
            .filter(t -> t.type() == TokenType.IDENTIFIER && "innerSum".equals(t.value()))
            .count();
        
        assertEquals(2, outerValueCount, "Should find outerValue as IDENTIFIER tokens (appears twice)");
        assertEquals(2, innerValueCount, "Should find innerValue as IDENTIFIER tokens (appears twice)");
        assertEquals(1, innerCountCount, "Should find innerCount as IDENTIFIER token");
        assertEquals(1, innerSumCount, "Should find innerSum as IDENTIFIER token");
        
        // Verify no OUTER tokens appear where they shouldn't
        long incorrectOuterTokens = tokens.stream()
            .filter(t -> t.type() == TokenType.OUTER)
            .count();
        
        assertEquals(0, incorrectOuterTokens, "Should not find any OUTER tokens in this query");
    }
    
    @Test
    @DisplayName("Boundary test: Keywords at word boundaries should work correctly")
    void testKeywordBoundaries() throws LexerException {
        // Test various boundary conditions
        String[] testCases = {
            "outer",      // Should be OUTER keyword
            "outer ",     // Should be OUTER keyword (space boundary)
            "outer,",     // Should be OUTER keyword (comma boundary)  
            "outer)",     // Should be OUTER keyword (paren boundary)
            "outera",     // Should be IDENTIFIER (letter boundary)
            "outer1",     // Should be IDENTIFIER (digit boundary)
            "outer_",     // Should be IDENTIFIER (underscore boundary)
        };
        
        for (String testCase : testCases) {
            Lexer lexer = new Lexer(testCase);
            List<Token> tokens = lexer.tokenize();
            
            Token firstToken = tokens.get(0);
            
            if (testCase.matches("outer[\\s,)]?")) {
                assertEquals(TokenType.OUTER, firstToken.type(), 
                    "'" + testCase + "' should tokenize as OUTER keyword");
            } else {
                assertEquals(TokenType.IDENTIFIER, firstToken.type(), 
                    "'" + testCase + "' should tokenize as IDENTIFIER");
            }
        }
    }
}
