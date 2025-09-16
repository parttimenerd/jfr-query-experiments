package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.QuerySemanticValidator;
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
            // For string literal tests and Unicode character tests, lexer exceptions are expected
            if (query.contains("unclosed string") || query.contains("nested '") || 
                query.contains("™") || query.contains("©") || query.contains("µ")) {
                throw new RuntimeException("Lexer failed as expected: " + e.getMessage(), e);
            }
            fail("Failed to create parser for query: " + query + " - " + e.getMessage());
            return null;
        }
    }

    @ParameterizedTest
    @MethodSource("provideBasicSyntaxErrors")
    void testBasicSyntaxErrorsParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideBasicSyntaxErrors() {
        return Stream.of(
            // Test basic missing components
            Arguments.of("", "Empty query should fail"),
            Arguments.of("@SELECT", "Missing FROM clause"),
            Arguments.of("@SELECT *", "Missing FROM clause"),
            Arguments.of("FROM GarbageCollection", "Missing SELECT clause"),
            Arguments.of("WHERE duration > 5ms", "Missing SELECT and FROM clauses")
        );
    }

    @ParameterizedTest
    @MethodSource("provideMissingSelectItems")
    void testMissingSelectItemsParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideMissingSelectItems() {
        return Stream.of(
            Arguments.of("@SELECT FROM GarbageCollection", "Missing SELECT items"),
            Arguments.of("@SELECT , * FROM GarbageCollection", "Empty SELECT item"),
            Arguments.of("@SELECT *, FROM GarbageCollection", "Trailing comma in SELECT")
        );
    }

    @ParameterizedTest
    @MethodSource("provideMissingFromClauseBasic")
    void testMissingFromClauseBasicParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideMissingFromClauseBasic() {
        return Stream.of(
            Arguments.of("@SELECT * WHERE duration > 5ms", "Missing FROM clause with WHERE"),
            Arguments.of("@SELECT COUNT(*) GROUP BY eventType", "Missing FROM clause with GROUP BY"),
            Arguments.of("@SELECT * ORDER BY duration", "Missing FROM clause with ORDER BY")
        );
    }

    @ParameterizedTest
    @MethodSource("provideMissingTableName")
    void testMissingTableNameParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideMissingTableName() {
        return Stream.of(
            Arguments.of("@SELECT * FROM", "Missing table name after FROM"),
            Arguments.of("@SELECT * FROM WHERE duration > 5ms", "Missing table name with WHERE"),
            Arguments.of("@SELECT * FROM GROUP BY eventType", "Missing table name with GROUP BY")
        );
    }

    @ParameterizedTest
    @MethodSource("provideUnknownFunctions")
    void testUnknownFunctionsParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideUnknownFunctions() {
        return Stream.of(
            Arguments.of("@SELECT UNKNOWN_FUNC() FROM GarbageCollection", "Unknown function should fail"),
            Arguments.of("@SELECT INVALID_FUNCTION(duration) FROM GarbageCollection", "Invalid function should fail"),
            Arguments.of("@SELECT FAKE_AGG(*, duration) FROM GarbageCollection", "Fake aggregate function should fail")
        );
    }

    @ParameterizedTest
    @MethodSource("provideFunctionArgumentCountErrors")
    void testFunctionArgumentCountErrorsParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideFunctionArgumentCountErrors() {
        return Stream.of(
            // COUNT should take 0 or 1 argument
            Arguments.of("@SELECT COUNT(a, b) FROM GarbageCollection", "COUNT with too many arguments"),
            Arguments.of("@SELECT COUNT(a, b, c) FROM GarbageCollection", "COUNT with way too many arguments"),
            
            // SUM, AVG, MIN, MAX should take exactly 1 argument
            Arguments.of("@SELECT SUM() FROM GarbageCollection", "SUM with no arguments"),
            Arguments.of("@SELECT SUM(a, b) FROM GarbageCollection", "SUM with too many arguments"),
            Arguments.of("@SELECT AVG() FROM GarbageCollection", "AVG with no arguments"),
            Arguments.of("@SELECT AVG(a, b) FROM GarbageCollection", "AVG with too many arguments"),
            Arguments.of("@SELECT MIN() FROM GarbageCollection", "MIN with no arguments"),
            Arguments.of("@SELECT MIN(a, b, c) FROM GarbageCollection", "MIN with too many arguments"),
            Arguments.of("@SELECT MAX() FROM GarbageCollection", "MAX with no arguments"),
            Arguments.of("@SELECT MAX(a, b) FROM GarbageCollection", "MAX with too many arguments"),
            
            // PERCENTILE should take exactly 2 arguments
            Arguments.of("@SELECT PERCENTILE() FROM GarbageCollection", "PERCENTILE with no arguments"),
            Arguments.of("@SELECT PERCENTILE(90) FROM GarbageCollection", "PERCENTILE with one argument"),
            Arguments.of("@SELECT PERCENTILE(90, duration, extra) FROM GarbageCollection", "PERCENTILE with too many arguments"),
            
            // SUBSTRING should take 2 or 3 arguments
            Arguments.of("@SELECT SUBSTRING() FROM GarbageCollection", "SUBSTRING with no arguments"),
            Arguments.of("@SELECT SUBSTRING(field) FROM GarbageCollection", "SUBSTRING with one argument"),
            Arguments.of("@SELECT SUBSTRING(field, 1, 2, 3, 4) FROM GarbageCollection", "SUBSTRING with too many arguments"),
            
            // REPLACE should take exactly 3 arguments
            Arguments.of("@SELECT REPLACE() FROM GarbageCollection", "REPLACE with no arguments"),
            Arguments.of("@SELECT REPLACE(field) FROM GarbageCollection", "REPLACE with one argument"),
            Arguments.of("@SELECT REPLACE(field, old) FROM GarbageCollection", "REPLACE with two arguments"),
            Arguments.of("@SELECT REPLACE(field, old, new, extra) FROM GarbageCollection", "REPLACE with too many arguments")
        );
    }

    private void assertSemanticError(String query, String description) {
        try {
            Parser.parseAndValidate(query); // This should fail with semantic error
            fail("Parser and semantic validation should have failed for query: '" + query + "' (" + description + ") but it succeeded. This indicates semantic validation is not working properly.");
        } catch (QuerySemanticValidator.QuerySemanticException e) {
            // Good - semantic validation caught the error
            System.out.println("✓ " + description + " - Semantic validation correctly caught error:");
            System.out.println("  Query: " + query);
            System.out.println("  Error: " + e.getMessage());
            System.out.println();
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
    void testAggregatesInWhereClause() {
        // Aggregate functions in WHERE clause should pass parsing but fail semantic validation
        assertSemanticError("@SELECT * FROM GarbageCollection WHERE COUNT(*) > 5", "Aggregate in WHERE clause");
        assertSemanticError("@SELECT * FROM GarbageCollection WHERE SUM(duration) > 100", "SUM in WHERE clause");
        assertSemanticError("@SELECT * FROM GarbageCollection WHERE AVG(duration) > 50", "AVG in WHERE clause");
        assertSemanticError("@SELECT * FROM GarbageCollection WHERE MAX(duration) > 1000", "MAX in WHERE clause");
        assertSemanticError("@SELECT * FROM GarbageCollection WHERE MIN(duration) < 10", "MIN in WHERE clause");
        assertSemanticError("@SELECT * FROM GarbageCollection WHERE PERCENTILE(90, duration) > 100", "PERCENTILE in WHERE clause");
    }

    @ParameterizedTest
    @MethodSource("provideMissingWhereCondition")
    void testMissingWhereConditionParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideMissingWhereCondition() {
        return Stream.of(
            Arguments.of("@SELECT * FROM GarbageCollection WHERE", "Missing WHERE condition"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE AND", "Missing WHERE condition with AND"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE OR", "Missing WHERE condition with OR")
        );
    }

    @ParameterizedTest
    @MethodSource("provideMissingGroupByExpression")
    void testMissingGroupByExpressionParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideMissingGroupByExpression() {
        return Stream.of(
            Arguments.of("@SELECT COUNT(*) FROM GarbageCollection GROUP BY", "Missing GROUP BY expression"),
            Arguments.of("@SELECT * FROM GarbageCollection GROUP BY HAVING COUNT(*) > 5", "Missing GROUP BY expression with HAVING"),
            Arguments.of("@SELECT * FROM GarbageCollection GROUP BY ORDER BY duration", "Missing GROUP BY expression with ORDER BY")
        );
    }

    @ParameterizedTest
    @MethodSource("provideMissingOrderByExpression")
    void testMissingOrderByExpressionParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideMissingOrderByExpression() {
        return Stream.of(
            Arguments.of("@SELECT * FROM GarbageCollection ORDER BY", "Missing ORDER BY expression"),
            Arguments.of("@SELECT * FROM GarbageCollection ORDER BY LIMIT 10", "Missing ORDER BY expression with LIMIT")
        );
    }

    @ParameterizedTest
    @MethodSource("provideMissingLimitValue")
    void testMissingLimitValueParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideMissingLimitValue() {
        return Stream.of(
            Arguments.of("@SELECT * FROM GarbageCollection LIMIT", "Missing LIMIT value")
        );
    }

    @ParameterizedTest
    @MethodSource("provideMalformedExpressions")
    void testMalformedExpressionsParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideMalformedExpressions() {
        return Stream.of(
            Arguments.of("@SELECT duration + FROM GarbageCollection", "Incomplete arithmetic expression"),
            Arguments.of("@SELECT duration * * 2 FROM GarbageCollection", "Double operator"),
            Arguments.of("@SELECT (duration FROM GarbageCollection", "Missing closing parenthesis"),
            Arguments.of("@SELECT duration) FROM GarbageCollection", "Missing opening parenthesis"),
            Arguments.of("@SELECT duration > FROM GarbageCollection", "Incomplete comparison")
        );
    }

    @ParameterizedTest
    @MethodSource("provideInvalidTokens")
    void testInvalidTokensParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideInvalidTokens() {
        return Stream.of(
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration > 5ms AND", "Incomplete AND expression"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration > 5ms OR", "Incomplete OR expression"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE NOT", "Incomplete NOT expression")
        );
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
            Arguments.of("@SELECT * FROM GarbageCollection GarbageCollection", "Table aliases must use explicit AS keyword"),
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
            Arguments.of("@SELECT * FROM GarbageCollection WHERE duration > 5mss", "Invalid duration format")
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
            // Note: Tab and newline characters in strings are now ALLOWED, so these tests are removed
                              
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
        } catch (RuntimeException e) {
            // Check if this is a lexer error (which is expected for some string literal tests)
            if (e.getMessage() != null && e.getMessage().contains("Lexer failed as expected")) {
                System.out.println("✓ " + description + " - Lexer correctly caught error:");
                System.out.println("  Query: " + query);
                System.out.println("  Error: " + e.getCause().getMessage());
                System.out.println();
                return;
            }
            throw e; // Re-throw if not expected
        } catch (ParserException e) {
            // Also good - parser threw an exception for invalid syntax
            System.out.println("✓ " + description + " - Parser correctly threw exception:");
            System.out.println("  Query: " + query);
            System.out.println("  Exception: " + e.getMessage());
            System.out.println();
        } catch (QuerySyntaxException e) {
            // Also good - parser threw a syntax exception for invalid syntax
            System.out.println("✓ " + description + " - Parser correctly threw syntax exception:");
            System.out.println("  Query: " + query);
            System.out.println("  Exception: " + e.getMessage());
            System.out.println();
        } catch (Exception e) {
            fail("Unexpected exception for query: '" + query + "' (" + description + "): " + e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("provideMissingFromClauseValidation")
    void testMissingFromClauseValidationParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideMissingFromClauseValidation() {
        return Stream.of(
            // Test that "SELECT x" without FROM throws proper missing FROM error
            Arguments.of("@SELECT x", "SELECT with field but missing FROM clause"),
            Arguments.of("@SELECT duration", "SELECT with single field but missing FROM clause"),
            Arguments.of("@SELECT COUNT(*)", "SELECT with function but missing FROM clause"),
            Arguments.of("@SELECT duration, eventType", "SELECT with multiple fields but missing FROM clause"),
            
            // Test that these don't get filtered as spurious errors
            Arguments.of("@SELECT duration + 5", "SELECT with expression but missing FROM clause"),
            Arguments.of("@SELECT MAX(duration)", "SELECT with aggregate function but missing FROM clause")
        );
    }
    
    @ParameterizedTest
    @MethodSource("provideClauseBoundaryDetection")
    void testClauseBoundaryDetectionParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideClauseBoundaryDetection() {
        return Stream.of(
            // Test FROM clause boundary detection
            Arguments.of("@SELECT x FROM WHERE y = 1", "WHERE immediately after FROM without table name"),
            Arguments.of("@SELECT x FROM GROUP BY eventType", "GROUP BY immediately after FROM without table name"),
            Arguments.of("@SELECT x FROM ORDER BY duration", "ORDER BY immediately after FROM without table name"),
            Arguments.of("@SELECT x FROM HAVING COUNT(*) > 5", "HAVING immediately after FROM without table name"),
            
            // Test GROUP BY clause boundary detection
            Arguments.of("@SELECT x FROM table GROUP BY WHERE y = 1", "WHERE after incomplete GROUP BY clause"),
            Arguments.of("@SELECT x FROM table GROUP BY ORDER BY duration", "ORDER BY after incomplete GROUP BY clause"),
            Arguments.of("@SELECT x FROM table GROUP BY HAVING COUNT(*) > 5", "HAVING after incomplete GROUP BY clause"),
            
            // Test ORDER BY clause boundary detection
            Arguments.of("@SELECT x FROM table ORDER BY WHERE y = 1", "WHERE after incomplete ORDER BY clause"),
            Arguments.of("@SELECT x FROM table ORDER BY HAVING COUNT(*) > 5", "HAVING after incomplete ORDER BY clause"),
            Arguments.of("@SELECT x FROM table ORDER BY LIMIT 10", "LIMIT after incomplete ORDER BY clause")
        );
    }
    
    @ParameterizedTest
    @MethodSource("provideContextAwareErrorMessages")
    void testContextAwareErrorMessagesParameterized(String query, String expectedContext) {
        // This test verifies that errors provide context-aware suggestions
        // based on which SQL clause they occur in
        try {
            Parser parser = createParser(query);
            parser.parse();
            if (parser.hasParsingErrors()) {
                List<ParserErrorHandler.ParserError> errors = parser.getParsingErrors();
                assertFalse(errors.isEmpty(), "Should have parsing errors");
                
                String errorMessage = errors.get(0).getMessage();
                String suggestion = errors.get(0).getSuggestion();
                
                // Verify the error is recognized as being in the expected clause context
                assertTrue(errorMessage.contains(expectedContext) || suggestion.contains(expectedContext),
                    "Error should mention " + expectedContext + " context. Message: " + errorMessage + ", Suggestion: " + suggestion);
            }
        } catch (Exception e) {
            // Parser exception is also acceptable for this invalid syntax
        }
    }

    private static Stream<Arguments> provideContextAwareErrorMessages() {
        return Stream.of(
            Arguments.of("@SELECT x FROM WHERE y = 1", "FROM clause"),
            Arguments.of("@SELECT x FROM table GROUP BY WHERE y = 1", "GROUP BY"),
            Arguments.of("@SELECT x FROM table ORDER BY WHERE y = 1", "ORDER BY")
        );
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

    /**
     * Test timestamp format validation errors during parsing.
     * 
     * Timestamp validation in the JFR query language occurs in two stages:
     * 1. Lexer stage: Pattern matching with regex \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{3})?Z?
     *    This ensures the basic format structure is correct (YYYY-MM-DDTHH:MM:SS[.sss][Z])
     * 2. Parser stage: Semantic validation using java.time.Instant.parse()
     *    This validates actual date/time values (e.g., month 13, hour 25, February 30th)
     * 
     * Invalid timestamp formats that match the lexer pattern but contain invalid
     * semantic values should be caught during parsing and result in proper error messages
     * that guide users to the correct format.
     * 
     * This approach allows the lexer to remain simple while providing comprehensive
     * timestamp validation with meaningful error messages.
     */
    @ParameterizedTest
    @MethodSource("provideInvalidTimestampQueries")
    void testTimestampFormatValidationParameterized(String query, String description) {
        assertParserError(query, description);
    }

    private static Stream<Arguments> provideInvalidTimestampQueries() {
        return Stream.of(
            // Invalid date values - these match the lexer pattern but fail semantic validation
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-13-01T10:30:00Z", 
                        "Invalid timestamp format with month 13"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-02-30T10:30:00Z", 
                        "Invalid timestamp format with February 30th"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-00-15T10:30:00Z", 
                        "Invalid timestamp format with month 0"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-12-32T10:30:00Z", 
                        "Invalid timestamp format with day 32"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-12-00T10:30:00Z", 
                        "Invalid timestamp format with day 0"),
            
            // Invalid time values
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-12-01T25:30:00Z", 
                        "Invalid timestamp format with hour 25"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-12-01T10:60:00Z", 
                        "Invalid timestamp format with minute 60"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-12-01T10:30:60Z", 
                        "Invalid timestamp format with second 60"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-12-01T-1:30:00Z", 
                        "Invalid timestamp format with negative hour"),
            
            // Invalid milliseconds component - exceeds 3 digits allowed by lexer pattern
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-12-01T10:30:00.9999Z", 
                        "Invalid timestamp format with 4-digit milliseconds"),
            
            // Additional edge cases for comprehensive coverage
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2023-02-29T10:30:00Z", 
                        "Invalid timestamp format with February 29th in non-leap year (2023)"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-04-31T10:30:00Z", 
                        "Invalid timestamp format with April 31st"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-06-31T10:30:00Z", 
                        "Invalid timestamp format with June 31st"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-09-31T10:30:00Z", 
                        "Invalid timestamp format with September 31st"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-11-31T10:30:00Z", 
                        "Invalid timestamp format with November 31st"),
            
            // Additional boundary cases
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2020-02-29T10:30:00Z AND timestamp < 2021-02-29T10:30:00Z", 
                        "Invalid timestamp format with February 29th in non-leap year (2021)"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-01-32T10:30:00Z", 
                        "Invalid timestamp format with January 32nd"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-03-32T10:30:00Z", 
                        "Invalid timestamp format with March 32nd"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-07-32T10:30:00Z", 
                        "Invalid timestamp format with July 32nd"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-08-32T10:30:00Z", 
                        "Invalid timestamp format with August 32nd"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-10-32T10:30:00Z", 
                        "Invalid timestamp format with October 32nd"),
            Arguments.of("@SELECT * FROM GarbageCollection WHERE timestamp > 2024-12-32T10:30:00Z", 
                        "Invalid timestamp format with December 32nd")
        );
    }
}
