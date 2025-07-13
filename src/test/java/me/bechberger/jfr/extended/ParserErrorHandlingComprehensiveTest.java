package me.bechberger.jfr.extended;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for parser error handling.
 * 
 * This test class verifies that the parser properly catches and reports syntax errors
 * for a wide range of invalid queries, rather than being overly lenient. The tests
 * ensure that:
 * 
 * 1. Missing FROM clauses are properly detected and reported
 * 2. Clause boundary violations are caught (e.g., WHERE immediately after FROM without table name)
 * 3. Context-aware error messages are generated based on which SQL clause contains the error
 * 4. Error filtering doesn't remove legitimate structural errors
 * 5. All major SQL syntax violations are caught and reported
 * 
 * Key test categories:
 * - Basic syntax errors (missing components)
 * - Missing FROM clause validation 
 * - Clause boundary detection (SELECT, FROM, WHERE, GROUP BY, ORDER BY, HAVING)
 * - Function argument validation
 * - Expression malformation detection
 * - Invalid operator usage
 * - Parameterized tests for comprehensive coverage
 * 
 * The tests use both individual test methods and parameterized tests to ensure
 * comprehensive coverage of error conditions. Each test verifies that the parser
 * either reports parsing errors or throws appropriate exceptions for invalid syntax.
 * 
 * @see ParserErrorHandler for the error handling implementation
 * @see Parser for the main parsing logic
 */
public class ParserErrorHandlingComprehensiveTest {

    private Parser createParser(String query) {
        try {
            Lexer lexer = new Lexer(query);
            List<Token> tokens = lexer.tokenize();
            return new Parser(tokens, query);
        } catch (Exception e) {
            fail("Failed to create parser for query: " + query + " - " + e.getMessage());
            return null;
        }
    }

    @Test
    void testBasicSyntaxErrors() {
        // Test basic missing components
        assertParserError("", "Empty query should fail");
        assertParserError("@SELECT", "Missing FROM clause");
        assertParserError("@SELECT *", "Missing FROM clause");
        assertParserError("FROM GarbageCollection", "Missing SELECT clause");
        assertParserError("WHERE duration > 5ms", "Missing SELECT and FROM clauses");
    }

    @Test
    void testMissingSelectItems() {
        assertParserError("@SELECT FROM GarbageCollection", "Missing SELECT items");
        assertParserError("@SELECT , * FROM GarbageCollection", "Empty SELECT item");
        assertParserError("@SELECT *, FROM GarbageCollection", "Trailing comma in SELECT");
    }

    @Test
    void testMissingFromClause() {
        assertParserError("@SELECT * WHERE duration > 5ms", "Missing FROM clause with WHERE");
        assertParserError("@SELECT COUNT(*) GROUP BY eventType", "Missing FROM clause with GROUP BY");
        assertParserError("@SELECT * ORDER BY duration", "Missing FROM clause with ORDER BY");
    }

    @Test
    void testMissingTableName() {
        assertParserError("@SELECT * FROM", "Missing table name after FROM");
        assertParserError("@SELECT * FROM WHERE duration > 5ms", "Missing table name with WHERE");
        assertParserError("@SELECT * FROM GROUP BY eventType", "Missing table name with GROUP BY");
    }

    @Test
    void testUnknownFunctions() {
        assertParserError("@SELECT UNKNOWN_FUNC() FROM GarbageCollection", "Unknown function should fail");
        assertParserError("@SELECT INVALID_FUNCTION(duration) FROM GarbageCollection", "Invalid function should fail");
        assertParserError("@SELECT FAKE_AGG(*, duration) FROM GarbageCollection", "Fake aggregate function should fail");
    }

    @Test
    void testFunctionArgumentCountErrors() {
        // COUNT should take 0 or 1 argument
        assertParserError("@SELECT COUNT(a, b) FROM GarbageCollection", "COUNT with too many arguments");
        assertParserError("@SELECT COUNT(a, b, c) FROM GarbageCollection", "COUNT with way too many arguments");
        
        // SUM, AVG, MIN, MAX should take exactly 1 argument
        assertParserError("@SELECT SUM() FROM GarbageCollection", "SUM with no arguments");
        assertParserError("@SELECT SUM(a, b) FROM GarbageCollection", "SUM with too many arguments");
        assertParserError("@SELECT AVG() FROM GarbageCollection", "AVG with no arguments");
        assertParserError("@SELECT AVG(a, b) FROM GarbageCollection", "AVG with too many arguments");
        assertParserError("@SELECT MIN() FROM GarbageCollection", "MIN with no arguments");
        assertParserError("@SELECT MIN(a, b, c) FROM GarbageCollection", "MIN with too many arguments");
        assertParserError("@SELECT MAX() FROM GarbageCollection", "MAX with no arguments");
        assertParserError("@SELECT MAX(a, b) FROM GarbageCollection", "MAX with too many arguments");
        
        // PERCENTILE should take exactly 2 arguments
        assertParserError("@SELECT PERCENTILE() FROM GarbageCollection", "PERCENTILE with no arguments");
        assertParserError("@SELECT PERCENTILE(90) FROM GarbageCollection", "PERCENTILE with one argument");
        assertParserError("@SELECT PERCENTILE(90, duration, extra) FROM GarbageCollection", "PERCENTILE with too many arguments");
        
        // SUBSTRING should take 2 or 3 arguments
        assertParserError("@SELECT SUBSTRING() FROM GarbageCollection", "SUBSTRING with no arguments");
        assertParserError("@SELECT SUBSTRING(field) FROM GarbageCollection", "SUBSTRING with one argument");
        assertParserError("@SELECT SUBSTRING(field, 1, 2, 3, 4) FROM GarbageCollection", "SUBSTRING with too many arguments");
        
        // REPLACE should take exactly 3 arguments
        assertParserError("@SELECT REPLACE() FROM GarbageCollection", "REPLACE with no arguments");
        assertParserError("@SELECT REPLACE(field) FROM GarbageCollection", "REPLACE with one argument");
        assertParserError("@SELECT REPLACE(field, old) FROM GarbageCollection", "REPLACE with two arguments");
        assertParserError("@SELECT REPLACE(field, old, new, extra) FROM GarbageCollection", "REPLACE with too many arguments");
    }

    @Test
    void testAggregatesInWhereClause() {
        assertParserError("@SELECT * FROM GarbageCollection WHERE COUNT(*) > 5", "Aggregate in WHERE clause");
        assertParserError("@SELECT * FROM GarbageCollection WHERE SUM(duration) > 100", "SUM in WHERE clause");
        assertParserError("@SELECT * FROM GarbageCollection WHERE AVG(duration) > 50", "AVG in WHERE clause");
        assertParserError("@SELECT * FROM GarbageCollection WHERE MAX(duration) > 1000", "MAX in WHERE clause");
        assertParserError("@SELECT * FROM GarbageCollection WHERE MIN(duration) < 10", "MIN in WHERE clause");
        assertParserError("@SELECT * FROM GarbageCollection WHERE PERCENTILE(90, duration) > 100", "PERCENTILE in WHERE clause");
    }

    @Test
    void testMissingWhereCondition() {
        assertParserError("@SELECT * FROM GarbageCollection WHERE", "Missing WHERE condition");
        assertParserError("@SELECT * FROM GarbageCollection WHERE AND", "Missing WHERE condition with AND");
        assertParserError("@SELECT * FROM GarbageCollection WHERE OR", "Missing WHERE condition with OR");
    }

    @Test
    void testMissingGroupByExpression() {
        assertParserError("@SELECT COUNT(*) FROM GarbageCollection GROUP BY", "Missing GROUP BY expression");
        assertParserError("@SELECT * FROM GarbageCollection GROUP BY HAVING COUNT(*) > 5", "Missing GROUP BY expression with HAVING");
        assertParserError("@SELECT * FROM GarbageCollection GROUP BY ORDER BY duration", "Missing GROUP BY expression with ORDER BY");
    }

    @Test
    void testMissingOrderByExpression() {
        assertParserError("@SELECT * FROM GarbageCollection ORDER BY", "Missing ORDER BY expression");
        assertParserError("@SELECT * FROM GarbageCollection ORDER BY LIMIT 10", "Missing ORDER BY expression with LIMIT");
    }

    @Test
    void testMissingLimitValue() {
        assertParserError("@SELECT * FROM GarbageCollection LIMIT", "Missing LIMIT value");
    }

    @Test
    void testMalformedExpressions() {
        assertParserError("@SELECT duration + FROM GarbageCollection", "Incomplete arithmetic expression");
        assertParserError("@SELECT duration * * 2 FROM GarbageCollection", "Double operator");
        assertParserError("@SELECT (duration FROM GarbageCollection", "Missing closing parenthesis");
        assertParserError("@SELECT duration) FROM GarbageCollection", "Missing opening parenthesis");
        assertParserError("@SELECT duration > FROM GarbageCollection", "Incomplete comparison");
    }

    @Test
    void testInvalidTokens() {
        assertParserError("@SELECT * FROM GarbageCollection WHERE duration > 5ms AND", "Incomplete AND expression");
        assertParserError("@SELECT * FROM GarbageCollection WHERE duration > 5ms OR", "Incomplete OR expression");
        assertParserError("@SELECT * FROM GarbageCollection WHERE NOT", "Incomplete NOT expression");
    }

    @ParameterizedTest
    @MethodSource("provideMalformedQueries")
    void testMalformedQueriesParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideMalformedQueries() {
        return Stream.of(
            Arguments.of("@SELECT", "Missing FROM clause"),
            Arguments.of("@SELECT *", "Missing FROM clause"),
            Arguments.of("@SELECT FROM", "Missing SELECT items and table name"),
            Arguments.of("FROM GarbageCollection", "Missing SELECT clause"),
            Arguments.of("@SELECT * FROM WHERE", "Missing table name"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE", "Missing WHERE condition"),
            Arguments.of("@SELECT * FROM GarbageCollection GROUP BY", "Missing GROUP BY expression"),
            Arguments.of("@SELECT * FROM GarbageCollection ORDER BY", "Missing ORDER BY expression"),
            Arguments.of("@SELECT * FROM GarbageCollection LIMIT", "Missing LIMIT value"),
            Arguments.of("@SELECT SUBSTRING() FROM GarbageCollection", "SUBSTRING with no arguments should be invalid"),
            Arguments.of("@SELECT UNKNOWN_FUNC() FROM GarbageCollection", "Unknown function"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE COUNT(*) > 5", "Aggregate in WHERE"),
            Arguments.of("@SELECT ( FROM GarbageCollection", "Unmatched parenthesis"),
            Arguments.of("@SELECT duration + FROM GarbageCollection", "Incomplete expression"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration >", "Incomplete comparison"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE AND duration > 5ms", "Missing left operand for AND"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration > 5ms AND", "Missing right operand for AND")
        );
    }

    @ParameterizedTest
    @MethodSource("provideInvalidSQLSyntaxQueries")
    void testInvalidSQLSyntaxParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideInvalidSQLSyntaxQueries() {
        return Stream.of(
            // Invalid SELECT syntax
            Arguments.of("@SELECT SELECT * FROM GarbageCollection", "Double SELECT keyword"),
            Arguments.of("@SELECT * * FROM GarbageCollection", "Double star in SELECT"),
            Arguments.of("@SELECT field field FROM GarbageCollection", "Duplicate field name without comma"),
            Arguments.of("@SELECT field, FROM GarbageCollection", "Trailing comma after field"),
            Arguments.of("@SELECT ,field FROM GarbageCollection", "Leading comma before field"),
            Arguments.of("@SELECT field,, FROM GarbageCollection", "Double comma in SELECT"),
            
            // Invalid FROM syntax
            Arguments.of("@SELECT * FROM FROM GarbageCollection", "Double FROM keyword"),
            Arguments.of("@SELECT * FROM GarbageCollection GarbageCollection", "Duplicate table name without alias"),
            Arguments.of("@SELECT * FROM GarbageCollection AS", "Missing alias after AS"),
            Arguments.of("@SELECT * FROM AS GarbageCollection", "AS without table name"),
            Arguments.of("@SELECT * FROM 123InvalidTable", "Table name starting with number"),
            Arguments.of("@SELECT * FROM", "Missing table name completely"),
            
            // Invalid WHERE syntax
            Arguments.of("@SELECT * FROM GarbageCollection WHERE WHERE duration > 5ms", "Double WHERE keyword"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration", "WHERE without comparison"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE > 5ms", "Missing left operand in WHERE"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration >", "Missing right operand in WHERE"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration = = 5", "Double equals operator"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE AND OR", "AND followed by OR without operands"),
            
            // Invalid function syntax
            Arguments.of("@SELECT COUNT COUNT(*) FROM GarbageCollection", "Function name without parentheses"),
            Arguments.of("@SELECT COUNT((duration)) FROM GarbageCollection", "Double parentheses in function"),
            Arguments.of("@SELECT COUNT(duration,) FROM GarbageCollection", "Trailing comma in function args"),
            Arguments.of("@SELECT COUNT(,duration) FROM GarbageCollection", "Leading comma in function args"),
            Arguments.of("@SELECT COUNT(duration,,value) FROM GarbageCollection", "Double comma in function args"),
            Arguments.of("@SELECT COUNT(*,) FROM GarbageCollection", "Comma after star in COUNT"),
            
            // Invalid GROUP BY syntax
            Arguments.of("@SELECT * FROM GarbageCollection GROUP GROUP BY eventType", "Double GROUP keyword"),
            Arguments.of("@SELECT * FROM GarbageCollection GROUP BY BY eventType", "Double BY keyword"),
            Arguments.of("@SELECT * FROM GarbageCollection GROUP BY eventType,", "Trailing comma in GROUP BY"),
            Arguments.of("@SELECT * FROM GarbageCollection GROUP BY ,eventType", "Leading comma in GROUP BY"),
            Arguments.of("@SELECT * FROM GarbageCollection GROUP BY eventType eventType", "Duplicate field without comma"),
            
            // Invalid ORDER BY syntax
            Arguments.of("@SELECT * FROM GarbageCollection ORDER ORDER BY duration", "Double ORDER keyword"),
            Arguments.of("@SELECT * FROM GarbageCollection ORDER BY BY duration", "Double BY keyword"),
            Arguments.of("@SELECT * FROM GarbageCollection ORDER BY duration ASC DESC", "Both ASC and DESC"),
            Arguments.of("@SELECT * FROM GarbageCollection ORDER BY duration,", "Trailing comma in ORDER BY"),
            Arguments.of("@SELECT * FROM GarbageCollection ORDER BY ASC", "ASC without field"),
            Arguments.of("@SELECT * FROM GarbageCollection ORDER BY DESC", "DESC without field"),
            
            // Invalid arithmetic expressions
            Arguments.of("@SELECT 5 + + 3 FROM GarbageCollection", "Double plus operator"),
            Arguments.of("@SELECT 5 - - 3 FROM GarbageCollection", "Double minus operator"),
            Arguments.of("@SELECT 5 * / 3 FROM GarbageCollection", "Mixed operators without operand"),
            Arguments.of("@SELECT (5 + FROM GarbageCollection", "Incomplete parenthesized expression"),
            Arguments.of("@SELECT 5 + ) FROM GarbageCollection", "Closing paren without opening"),
            Arguments.of("@SELECT ((5 + 3) FROM GarbageCollection", "Unmatched nested parentheses"),
            
            // Invalid comparison operators
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration == 5", "Double equals (should be single =)"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration !== 5", "Invalid not equals operator"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration <> > 5", "Mixed comparison operators"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration < = 5", "Spaced less-than-equals"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration > = 5", "Spaced greater-than-equals"),
            
            // Invalid HAVING syntax
            Arguments.of("@SELECT COUNT(*) FROM GarbageCollection HAVING HAVING COUNT(*) > 5", "Double HAVING keyword"),
            Arguments.of("@SELECT COUNT(*) FROM GarbageCollection GROUP BY eventType HAVING", "Missing HAVING condition"),
            Arguments.of("@SELECT COUNT(*) FROM GarbageCollection HAVING COUNT(*) >", "Incomplete HAVING condition"),
            
            // Invalid LIMIT syntax
            Arguments.of("@SELECT * FROM GarbageCollection LIMIT LIMIT 10", "Double LIMIT keyword"),
            Arguments.of("@SELECT * FROM GarbageCollection LIMIT -5", "Negative LIMIT value"),
            Arguments.of("@SELECT * FROM GarbageCollection LIMIT 0", "Zero LIMIT value"),
            Arguments.of("@SELECT * FROM GarbageCollection LIMIT 'ten'", "String LIMIT value"),
            Arguments.of("@SELECT * FROM GarbageCollection LIMIT 10.5", "Decimal LIMIT value"),
            
            // Invalid clause ordering
            Arguments.of("@SELECT * FROM GarbageCollection LIMIT 10 WHERE duration > 5ms", "LIMIT before WHERE"),
            Arguments.of("@SELECT * FROM GarbageCollection ORDER BY duration WHERE duration > 5ms", "ORDER BY before WHERE"),
            Arguments.of("@SELECT * FROM GarbageCollection HAVING COUNT(*) > 5 GROUP BY eventType", "HAVING before GROUP BY"),
            Arguments.of("@SELECT * WHERE duration > 5ms FROM GarbageCollection", "WHERE before FROM"),
            Arguments.of("FROM GarbageCollection SELECT *", "FROM before SELECT"),
            
            // Invalid string and literal syntax
            Arguments.of("@SELECT 'unclosed string FROM GarbageCollection", "Unclosed string literal"),
            Arguments.of("@SELECT 'nested ' string' FROM GarbageCollection", "Nested quotes in string"),
            Arguments.of("@SELECT 123abc FROM GarbageCollection", "Invalid number format"),
            Arguments.of("@SELECT .123 FROM GarbageCollection", "Number starting with dot"),
            
            // Invalid field references
            Arguments.of("@SELECT field. FROM GarbageCollection", "Field reference ending with dot"),
            Arguments.of("@SELECT .field FROM GarbageCollection", "Field reference starting with dot"),
            Arguments.of("@SELECT field..subfield FROM GarbageCollection", "Double dot in field reference"),
            Arguments.of("@SELECT field.123 FROM GarbageCollection", "Numeric field name after dot"),
            
            // Invalid AS alias syntax
            Arguments.of("@SELECT field AS AS alias FROM GarbageCollection", "Double AS keyword"),
            Arguments.of("@SELECT field AS 123alias FROM GarbageCollection", "Alias starting with number"),
            Arguments.of("@SELECT field AS 'quoted alias' FROM GarbageCollection", "Quoted alias"),
            Arguments.of("@SELECT COUNT(*) AS FROM GarbageCollection", "AS without alias name"),
            
            // Invalid JOIN syntax (if supported)
            Arguments.of("@SELECT * FROM GarbageCollection JOIN", "JOIN without table"),
            Arguments.of("@SELECT * FROM GarbageCollection JOIN Events ON", "JOIN without condition"),
            Arguments.of("@SELECT * FROM GarbageCollection JOIN Events ON =", "JOIN with incomplete condition"),
            
            // Invalid nested queries
            Arguments.of("@SELECT * FROM (@SELECT FROM Events)", "Nested query with missing SELECT items"),
            Arguments.of("@SELECT * FROM (@SELECT * FROM)", "Nested query with missing table"),
            Arguments.of("@SELECT * FROM (@SELECT *)", "Nested query without FROM"),
            Arguments.of("@SELECT * FROM SELECT * FROM Events", "Missing parentheses around subquery"),
            
            // Reserved keyword misuse
            Arguments.of("@SELECT SELECT FROM GarbageCollection", "@SELECT used as field name"),
            Arguments.of("@SELECT FROM FROM GarbageCollection", "FROM used as field name"),
            Arguments.of("@SELECT WHERE FROM GarbageCollection", "WHERE used as field name"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE SELECT > 5", "@SELECT used in WHERE"),
            
            // Invalid duration/timestamp literals
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration > 5x", "Invalid duration unit"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration > ms", "Duration unit without value"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration > 5mss", "Invalid duration format"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > '2024-13-01T00:00:00Z'", "Invalid month in timestamp"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > '2024-01-32T00:00:00Z'", "Invalid day in timestamp")
        );
    }

    @ParameterizedTest
    @MethodSource("provideEdgeCaseQueries")
    void testEdgeCaseQueriesParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideEdgeCaseQueries() {
        return Stream.of(
            // Whitespace and formatting edge cases
            Arguments.of("@SELECT\t\t\tFROM GarbageCollection", "Multiple tabs instead of field"),
            Arguments.of("@SELECT   FROM GarbageCollection", "Multiple spaces instead of field"),
            Arguments.of("@SELECT\nFROM GarbageCollection", "Newline instead of field"),
            Arguments.of("@SELECT * FROM\t\t\tWHERE duration > 5ms", "Multiple tabs instead of table"),
            Arguments.of("@SELECT * FROM   WHERE duration > 5ms", "Multiple spaces instead of table"),
            
            // Unicode and special character edge cases
            Arguments.of("@SELECT * FROM Garbage™Collection", "Unicode character in table name"),
            Arguments.of("@SELECT field© FROM GarbageCollection", "Copyright symbol in field name"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration > 5µs", "Micro symbol in duration"),
            Arguments.of("@SELECT 'text with\ttab' FROM GarbageCollection", "Tab character in string"),
            Arguments.of("@SELECT 'text with\nnewline' FROM GarbageCollection", "Newline in string"),
                              
            // Operator precedence edge cases
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration > 5 AND", "Incomplete AND at end"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE OR duration > 5", "OR at beginning"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE NOT", "NOT without expression"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE NOT NOT", "Double NOT"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration > 5 AND OR", "AND followed by OR"),
            
            // Function nesting edge cases
            Arguments.of("@SELECT COUNT(COUNT(*)) FROM GarbageCollection", "Nested aggregate functions"),
            Arguments.of("@SELECT SUM(AVG(duration)) FROM GarbageCollection", "Mixed nested aggregates"),
            Arguments.of("@SELECT COUNT(SELECT * FROM Events) FROM GarbageCollection", "Subquery in function"),
            Arguments.of("@SELECT COUNT() + SUM() FROM GarbageCollection", "Multiple invalid functions"),
            
            // Array and indexing
            Arguments.of("@SELECT field[] FROM GarbageCollection", "Empty array index"),
            Arguments.of("@SELECT field[0.5] FROM GarbageCollection", "Decimal array index"),
            Arguments.of("@SELECT field[-1] FROM GarbageCollection", "Negative array index"),
            Arguments.of("@SELECT field[field2] FROM GarbageCollection", "Field as array index"),
            Arguments.of("@SELECT field[1][2] FROM GarbageCollection", "Double array indexing")
            
        );
    }

    private void assertParserError(String query, String description) {
        try {
            Parser parser = createParser(query);
            parser.parse();
            
            // Check if parser reported any errors
            if (parser.hasParsingErrors()) {
                // Good - parser caught the error
                List<ParserErrorHandler.ParserError> errors = parser.getParsingErrors();
                assertFalse(errors.isEmpty(), "Parser should have reported at least one error for: " + description);
                
                System.out.println("✓ " + description + " - Parser correctly caught error:");
                System.out.println("  Query: " + query);
                System.out.println("  Error: " + errors.get(0).getMessage());
                System.out.println();
            } else {
                // Bad - parser was too lenient
                fail("Parser should have failed for query: '" + query + "' (" + description + ") but it parsed successfully. This indicates the parser is being too lenient.");
            }
        } catch (ParserException e) {
            // Also good - parser threw an exception for invalid syntax
            System.out.println("✓ " + description + " - Parser correctly threw exception:");
            System.out.println("  Query: " + query);
            System.out.println("  Exception: " + e.getMessage());
            System.out.println();
        } catch (Exception e) {
            fail("Unexpected exception for query: '" + query + "' (" + description + "): " + e.getMessage());
        }
    }

    @Test
    void testMissingFromClauseValidation() {
        // Test that "SELECT x" without FROM throws proper missing FROM error
        assertParserError("@SELECT x", "SELECT with field but missing FROM clause");
        assertParserError("@SELECT duration", "SELECT with single field but missing FROM clause");
        assertParserError("@SELECT COUNT(*)", "SELECT with function but missing FROM clause");
        assertParserError("@SELECT duration, eventType", "SELECT with multiple fields but missing FROM clause");
        
        // Test that these don't get filtered as spurious errors
        assertParserError("@SELECT duration + 5", "SELECT with expression but missing FROM clause");
        assertParserError("@SELECT MAX(duration)", "SELECT with aggregate function but missing FROM clause");
    }
    
    @Test
    void testClauseBoundaryDetection() {
        // Test FROM clause boundary detection
        assertParserError("@SELECT x FROM WHERE y = 1", "WHERE immediately after FROM without table name");
        assertParserError("@SELECT x FROM GROUP BY eventType", "GROUP BY immediately after FROM without table name");
        assertParserError("@SELECT x FROM ORDER BY duration", "ORDER BY immediately after FROM without table name");
        assertParserError("@SELECT x FROM HAVING COUNT(*) > 5", "HAVING immediately after FROM without table name");
        
        // Test GROUP BY clause boundary detection
        assertParserError("@SELECT x FROM table GROUP BY WHERE y = 1", "WHERE after incomplete GROUP BY clause");
        assertParserError("@SELECT x FROM table GROUP BY ORDER BY duration", "ORDER BY after incomplete GROUP BY clause");
        assertParserError("@SELECT x FROM table GROUP BY HAVING COUNT(*) > 5", "HAVING after incomplete GROUP BY clause");
        
        // Test ORDER BY clause boundary detection
        assertParserError("@SELECT x FROM table ORDER BY WHERE y = 1", "WHERE after incomplete ORDER BY clause");
        assertParserError("@SELECT x FROM table ORDER BY HAVING COUNT(*) > 5", "HAVING after incomplete ORDER BY clause");
        assertParserError("@SELECT x FROM table ORDER BY LIMIT 10", "LIMIT after incomplete ORDER BY clause");
    }
    
    @Test
    void testContextAwareErrorMessages() {
        // This test verifies that errors provide context-aware suggestions
        // based on which SQL clause they occur in
        
        // Test FROM clause context
        try {
            Parser parser = createParser("@SELECT x FROM WHERE y = 1");
            parser.parse();
            if (parser.hasParsingErrors()) {
                List<ParserErrorHandler.ParserError> errors = parser.getParsingErrors();
                assertFalse(errors.isEmpty(), "Should have parsing errors");
                
                String errorMessage = errors.get(0).getMessage();
                String suggestion = errors.get(0).getSuggestion();
                
                // Verify the error is recognized as being in FROM clause context
                assertTrue(errorMessage.contains("FROM clause") || suggestion.contains("FROM clause"),
                    "Error should mention FROM clause context. Message: " + errorMessage + ", Suggestion: " + suggestion);
            }
        } catch (Exception e) {
            // Parser exception is also acceptable for this invalid syntax
        }
        
        // Test GROUP BY clause context
        try {
            Parser parser = createParser("@SELECT x FROM table GROUP BY WHERE y = 1");
            parser.parse();
            if (parser.hasParsingErrors()) {
                List<ParserErrorHandler.ParserError> errors = parser.getParsingErrors();
                assertFalse(errors.isEmpty(), "Should have parsing errors");
                
                String errorMessage = errors.get(0).getMessage();
                String suggestion = errors.get(0).getSuggestion();
                
                // Verify the error is recognized as being in GROUP BY clause context
                assertTrue(errorMessage.contains("GROUP BY") || suggestion.contains("GROUP BY"),
                    "Error should mention GROUP BY clause context. Message: " + errorMessage + ", Suggestion: " + suggestion);
            }
        } catch (Exception e) {
            // Parser exception is also acceptable for this invalid syntax
        }
        
        // Test ORDER BY clause context
        try {
            Parser parser = createParser("@SELECT x FROM table ORDER BY WHERE y = 1");
            parser.parse();
            if (parser.hasParsingErrors()) {
                List<ParserErrorHandler.ParserError> errors = parser.getParsingErrors();
                assertFalse(errors.isEmpty(), "Should have parsing errors");
                
                String errorMessage = errors.get(0).getMessage();
                String suggestion = errors.get(0).getSuggestion();
                
                // Verify the error is recognized as being in ORDER BY clause context
                assertTrue(errorMessage.contains("ORDER BY") || suggestion.contains("ORDER BY"),
                    "Error should mention ORDER BY clause context. Message: " + errorMessage + ", Suggestion: " + suggestion);
            }
        } catch (Exception e) {
            // Parser exception is also acceptable for this invalid syntax
        }
    }
    
    @ParameterizedTest
    @MethodSource("provideMissingFromClauseQueries")
    void testMissingFromClauseParameterized(String query, String description) {
        assertParserError(query, description);
    }
    
    private static Stream<Arguments> provideMissingFromClauseQueries() {
        return Stream.of(
            // Basic SELECT without FROM
            Arguments.of("@SELECT x", "Simple SELECT with identifier missing FROM"),
            Arguments.of("@SELECT duration", "SELECT with field name missing FROM"),
            Arguments.of("@SELECT *", "SELECT star missing FROM"),
            Arguments.of("@SELECT 1", "SELECT with literal missing FROM"),
            Arguments.of("@SELECT 'hello'", "SELECT with string literal missing FROM"),
            
            // SELECT with expressions but missing FROM
            Arguments.of("@SELECT duration + 5", "SELECT with arithmetic expression missing FROM"),
            Arguments.of("@SELECT duration * 2", "SELECT with multiplication missing FROM"),
            Arguments.of("@SELECT duration / 1000", "SELECT with division missing FROM"),
            Arguments.of("@SELECT duration - 10", "SELECT with subtraction missing FROM"),
            Arguments.of("@SELECT (duration + 5)", "SELECT with parenthesized expression missing FROM"),
            
            // SELECT with functions but missing FROM
            Arguments.of("@SELECT COUNT(*)", "SELECT with COUNT function missing FROM"),
            Arguments.of("@SELECT SUM(duration)", "SELECT with SUM function missing FROM"),
            Arguments.of("@SELECT AVG(duration)", "SELECT with AVG function missing FROM"),
            Arguments.of("@SELECT MAX(duration)", "SELECT with MAX function missing FROM"),
            Arguments.of("@SELECT MIN(duration)", "SELECT with MIN function missing FROM"),
            Arguments.of("@SELECT SUBSTRING(eventType, 1, 5)", "SELECT with SUBSTRING function missing FROM"),
            
            // SELECT with multiple items but missing FROM
            Arguments.of("@SELECT duration, eventType", "SELECT with multiple fields missing FROM"),
            Arguments.of("@SELECT COUNT(*), AVG(duration)", "SELECT with multiple functions missing FROM"),
            Arguments.of("@SELECT duration, COUNT(*)", "SELECT with field and function missing FROM"),
            Arguments.of("@SELECT duration AS d, eventType AS e", "SELECT with aliases missing FROM"),
            
            // SELECT followed by other clauses but missing FROM
            Arguments.of("@SELECT duration WHERE duration > 5ms", "SELECT with WHERE but missing FROM"),
            Arguments.of("@SELECT duration GROUP BY eventType", "SELECT with GROUP BY but missing FROM"),
            Arguments.of("@SELECT duration ORDER BY duration", "SELECT with ORDER BY but missing FROM"),
            Arguments.of("@SELECT duration HAVING COUNT(*) > 5", "SELECT with HAVING but missing FROM"),
            Arguments.of("@SELECT duration LIMIT 10", "SELECT with LIMIT but missing FROM"),
            
            // SELECT with complex expressions and multiple clauses but missing FROM
            Arguments.of("@SELECT COUNT(*) WHERE eventType = 'GC'", "SELECT with aggregate and WHERE missing FROM"),
            Arguments.of("@SELECT duration + 5 ORDER BY duration", "SELECT with expression and ORDER BY missing FROM"),
            Arguments.of("@SELECT AVG(duration) GROUP BY eventType", "SELECT with aggregate and GROUP BY missing FROM"),
            Arguments.of("@SELECT duration, eventType WHERE duration > 100 ORDER BY duration", "Complex SELECT missing FROM")
        );
    }
    
    @ParameterizedTest
    @MethodSource("provideClauseBoundaryViolations") 
    void testClauseBoundaryViolationsParameterized(String query, String description) {
        assertParserError(query, description);
    }
    
    private static Stream<Arguments> provideClauseBoundaryViolations() {
        return Stream.of(
            // FROM clause boundary violations
            Arguments.of("@SELECT x FROM WHERE y = 1", "WHERE keyword immediately after FROM without table"),
            Arguments.of("@SELECT x FROM GROUP BY eventType", "GROUP BY immediately after FROM without table"),
            Arguments.of("@SELECT x FROM ORDER BY duration", "ORDER BY immediately after FROM without table"),
            Arguments.of("@SELECT x FROM HAVING COUNT(*) > 5", "HAVING immediately after FROM without table"),
            Arguments.of("@SELECT x FROM LIMIT 10", "LIMIT immediately after FROM without table"),
            Arguments.of("@SELECT x FROM AND duration > 5", "AND immediately after FROM without table"),
            Arguments.of("@SELECT x FROM OR eventType = 'GC'", "OR immediately after FROM without table"),
            
            // GROUP BY clause boundary violations
            Arguments.of("@SELECT x FROM table GROUP BY WHERE y = 1", "WHERE after incomplete GROUP BY"),
            Arguments.of("@SELECT x FROM table GROUP BY ORDER BY duration", "ORDER BY after incomplete GROUP BY"),
            Arguments.of("@SELECT x FROM table GROUP BY HAVING COUNT(*) > 5", "HAVING after incomplete GROUP BY"),
            Arguments.of("@SELECT x FROM table GROUP BY LIMIT 10", "LIMIT after incomplete GROUP BY"),
            Arguments.of("@SELECT x FROM table GROUP BY SELECT COUNT(*)", "SELECT after incomplete GROUP BY"),
            Arguments.of("@SELECT x FROM table GROUP BY FROM otherTable", "FROM after incomplete GROUP BY"),
            
            // ORDER BY clause boundary violations
            Arguments.of("@SELECT x FROM table ORDER BY WHERE y = 1", "WHERE after incomplete ORDER BY"),
            Arguments.of("@SELECT x FROM table ORDER BY GROUP BY eventType", "GROUP BY after incomplete ORDER BY"),
            Arguments.of("@SELECT x FROM table ORDER BY HAVING COUNT(*) > 5", "HAVING after incomplete ORDER BY"),
            Arguments.of("@SELECT x FROM table ORDER BY LIMIT 10", "LIMIT after incomplete ORDER BY"),
            Arguments.of("@SELECT x FROM table ORDER BY SELECT COUNT(*)", "SELECT after incomplete ORDER BY"),
            Arguments.of("@SELECT x FROM table ORDER BY FROM otherTable", "FROM after incomplete ORDER BY"),
            
            // WHERE clause boundary violations (less common but possible)
            Arguments.of("@SELECT x FROM table WHERE SELECT COUNT(*)", "SELECT keyword in WHERE clause"),
            Arguments.of("@SELECT x FROM table WHERE FROM otherTable", "FROM keyword in WHERE clause"),
            Arguments.of("@SELECT x FROM table WHERE GROUP BY eventType", "GROUP BY keywords in WHERE clause"),
            Arguments.of("@SELECT x FROM table WHERE ORDER BY duration", "ORDER BY keywords in WHERE clause"),
            
            // HAVING clause boundary violations
            Arguments.of("@SELECT COUNT(*) FROM table GROUP BY eventType HAVING SELECT COUNT(*)", "SELECT in HAVING clause"),
            Arguments.of("@SELECT COUNT(*) FROM table GROUP BY eventType HAVING FROM otherTable", "FROM in HAVING clause"),
            Arguments.of("@SELECT COUNT(*) FROM table GROUP BY eventType HAVING GROUP BY duration", "GROUP BY in HAVING clause"),
            Arguments.of("@SELECT COUNT(*) FROM table GROUP BY eventType HAVING ORDER BY duration", "ORDER BY in HAVING clause"),
            
            // Multiple boundary violations in single query
            Arguments.of("@SELECT x FROM WHERE GROUP BY ORDER BY", "Multiple consecutive clause keywords"),
            Arguments.of("@SELECT x FROM WHERE y = 1 GROUP BY ORDER BY duration", "WHERE without table, incomplete GROUP BY and ORDER BY"),
            Arguments.of("@SELECT FROM WHERE GROUP BY eventType HAVING ORDER BY", "Missing SELECT items, table, and incomplete clauses")
        );
    }
}
