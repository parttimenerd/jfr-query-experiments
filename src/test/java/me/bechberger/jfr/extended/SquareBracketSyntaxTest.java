package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.table.CellValue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;
import java.util.function.Supplier;

import static me.bechberger.jfr.extended.ASTBuilder.*;
import static me.bechberger.jfr.extended.ArrayBuilder.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test cases for square bracket syntax in JFR queries.
 * 
 * Validates that the parser correctly handles array literals with square brackets,
 * generating the expected AST nodes. This test suite covers:
 *   - Simple array literals with different data types
 *   - Function calls within arrays
 *   - Array operations (HEAD, TAIL, SLICE, etc.)
 *   - Boolean logic with array expressions
 *   - Complex nested expressions
 *   - Error handling
 * 
 * Tests use parameterized tests with ASTBuilder pattern to create expected ASTs.
 */
class SquareBracketSyntaxTest {

    // Using ArrayBuilder methods for creating array literals and ASTBuilder for other AST nodes
    // Helper methods are defined in ArrayBuilder class:
    //   - arrayLiteral(ExpressionNode...)
    //   - intArray(int...)
    //   - stringArray(String...)
    //   - booleanArray(boolean...)
    //   - doubleArray(double...)
    //   - durationArray(String...)
    //   - emptyArray()
    //   - mixedArray()
    //   - nestedArray(ArrayLiteralNode...)

    /**
     * Simple array literal test cases
     */
    static Stream<Arguments> simpleArrayLiteralTestCases() {
        return Stream.of(
            // Basic data types
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId IN [1, 2, 3]",
                "Simple integer array",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("gcId"),
                            BinaryOperator.IN,
                            intArray(1, 2, 3)
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE name IN ['gc', 'allocation', 'thread']",
                "String array",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("name"),
                            BinaryOperator.IN,
                            stringArray("gc", "allocation", "thread")
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE flag IN [true, false]",
                "Boolean array",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("flag"),
                            BinaryOperator.IN,
                            booleanArray(true, false)
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE priority IN [1.5, 2.0, 3.14]",
                "Float array",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("priority"),
                            BinaryOperator.IN,
                            doubleArray(1.5, 2.0, 3.14)
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE duration IN [10ms, 100ms, 1s]",
                "Duration array",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("duration"),
                            BinaryOperator.IN,
                            durationArray("10ms", "100ms", "1s")
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId IN [42]",
                "Single element array",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("gcId"),
                            BinaryOperator.IN,
                            intArray(42)
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId IN []",
                "Empty array",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("gcId"),
                            BinaryOperator.IN,
                            emptyArray()
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE value IN [1, 'mixed', true]",
                "Mixed type array",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("value"),
                            BinaryOperator.IN,
                            arrayLiteral(
                                numberLiteral(1),
                                stringLiteral("mixed"),
                                literal(new CellValue.BooleanValue(true))
                            )
                        )
                    )
                )
            )
        );
    }

    @ParameterizedTest(name = "Simple array literals - {1}")
    @MethodSource("simpleArrayLiteralTestCases")
    @DisplayName("Test simple array literals with various data types")
    void testSimpleArrayLiterals(String query, String description, Supplier<WhereNode> expectedWhereSupplier) throws Exception {
        QueryNode ast = parseQuery(query);
        WhereNode expectedWhere = expectedWhereSupplier.get();
        
        // Validate where clause
        WhereNode actualWhere = ast.where();
        assertNotNull(actualWhere, "WHERE clause should exist for " + description);
        
        // Use ASTBuilder.astEquals to compare the expected and actual AST nodes
        assertTrue(astEquals(expectedWhere, actualWhere), 
            "WHERE clause should match expected for " + description);
    }

    /**
     * Function calls in array literals test cases
     */
    static Stream<Arguments> functionCallArrayTestCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId IN [BEFORE_GC(timestamp), AFTER_GC(timestamp)]",
                "Simple function calls",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("gcId"),
                            BinaryOperator.IN,
                            arrayLiteral(
                                function("BEFORE_GC", identifier("timestamp")),
                                function("AFTER_GC", identifier("timestamp"))
                            )
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE id IN [P99Select(GarbageCollection, id, duration)]",
                "Parameterized function call",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("id"),
                            BinaryOperator.IN,
                            arrayLiteral(
                                percentileSelection("P99Select", "GarbageCollection", "id", identifier("duration"), 99.0)
                            )
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE threadId IN [MIN(threadIds), MAX(threadIds), AVG(threadIds)]",
                "Aggregate functions",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("threadId"),
                            BinaryOperator.IN,
                            arrayLiteral(
                                function("MIN", identifier("threadIds")),
                                function("MAX", identifier("threadIds")),
                                function("AVG", identifier("threadIds"))
                            )
                        )
                    )
                )
            )
        );
    }

    @ParameterizedTest(name = "Function call arrays - {1}")
    @MethodSource("functionCallArrayTestCases")
    @DisplayName("Test array literals containing function calls")
    void testFunctionCallArrays(String query, String description, Supplier<WhereNode> expectedWhereSupplier) throws Exception {
        QueryNode ast = parseQuery(query);
        WhereNode expectedWhere = expectedWhereSupplier.get();
        
        // Validate where clause
        WhereNode actualWhere = ast.where();
        assertNotNull(actualWhere, "WHERE clause should exist for " + description);
        
        // Use ASTBuilder.astEquals to compare the expected and actual AST nodes
        assertTrue(astEquals(expectedWhere, actualWhere), 
            "WHERE clause should match expected for " + description);
    }

    /**
     * Array operation test cases
     */
    static Stream<Arguments> arrayOperationTestCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE threadId = HEAD([1, 2, 3])",
                "HEAD with integer array",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("threadId"),
                            BinaryOperator.EQUALS,
                            function("HEAD", intArray(1, 2, 3))
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId IN SLICE([1, 2, 3, 4, 5], 1, 3)",
                "SLICE with array and indices",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("gcId"),
                            BinaryOperator.IN,
                            function("SLICE", 
                                intArray(1, 2, 3, 4, 5),
                                numberLiteral(1),
                                numberLiteral(3)
                            )
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE id IN TAIL([100, 200, 300], 2)",
                "TAIL with array and count",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("id"),
                            BinaryOperator.IN,
                            function("TAIL",
                                intArray(100, 200, 300),
                                numberLiteral(2)
                            )
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId IN UNIQUE([1, 1, 2, 2, 3])",
                "UNIQUE operation",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("gcId"),
                            BinaryOperator.IN,
                            function("UNIQUE", intArray(1, 1, 2, 2, 3))
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE threadId IN SORT([30, 10, 20])",
                "SORT operation",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("threadId"),
                            BinaryOperator.IN,
                            function("SORT", intArray(30, 10, 20))
                        )
                    )
                )
            )
        );
    }

    @ParameterizedTest(name = "Array operations - {1}")
    @MethodSource("arrayOperationTestCases")
    @DisplayName("Test array operations like HEAD, TAIL, SLICE, UNIQUE, and SORT")
    void testArrayOperations(String query, String description, Supplier<WhereNode> expectedWhereSupplier) throws Exception {
        QueryNode ast = parseQuery(query);
        WhereNode expectedWhere = expectedWhereSupplier.get();
        
        // Validate where clause
        WhereNode actualWhere = ast.where();
        assertNotNull(actualWhere, "WHERE clause should exist for " + description);
        if (!astEquals(expectedWhere, actualWhere)) {
            System.out.println("Expected: " + expectedWhere);
            System.out.println("Actual: " + actualWhere);
        }
        // Use ASTBuilder.astEquals to compare the expected and actual AST nodes
        assertTrue(astEquals(expectedWhere, actualWhere), 
            "WHERE clause should match expected for " + description);
    }

    /**
     * Boolean logic test cases with array literals
     */
    static Stream<Arguments> booleanLogicTestCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE (gcId IN [1, 2, 3]) OR (threadId IN [4, 5, 6])",
                "OR with two IN clauses",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            binary(
                                identifier("gcId"),
                                BinaryOperator.IN,
                                intArray(1, 2, 3)
                            ),
                            BinaryOperator.OR,
                            binary(
                                identifier("threadId"),
                                BinaryOperator.IN,
                                intArray(4, 5, 6)
                            )
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE NOT (gcId IN [1, 2, 3])",
                "NOT with IN clause",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        unary(
                            UnaryOperator.NOT,
                            binary(
                                identifier("gcId"),
                                BinaryOperator.IN,
                                intArray(1, 2, 3)
                            )
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE (gcId IN [1, 2, 3]) AND (duration > 10ms)",
                "IN with comparison operator",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            binary(
                                identifier("gcId"),
                                BinaryOperator.IN,
                                intArray(1, 2, 3)
                            ),
                            BinaryOperator.AND,
                            binary(
                                identifier("duration"),
                                BinaryOperator.GREATER_THAN,
                                durationLiteral("10ms")
                            )
                        )
                    )
                )
            )
        );
    }

    @ParameterizedTest(name = "Boolean logic - {1}")
    @MethodSource("booleanLogicTestCases")
    @DisplayName("Test boolean logic operations with array literals")
    void testBooleanLogic(String query, String description, Supplier<WhereNode> expectedWhereSupplier) throws Exception {
        QueryNode ast = parseQuery(query);
        WhereNode expectedWhere = expectedWhereSupplier.get();
        
        // Validate where clause
        WhereNode actualWhere = ast.where();
        assertNotNull(actualWhere, "WHERE clause should exist for " + description);
        
        // Use ASTBuilder.astEquals to compare the expected and actual AST nodes
        assertTrue(astEquals(expectedWhere, actualWhere), 
            "WHERE clause should match expected for " + description);
    }

    /**
     * Complex nested expressions test cases
     */
    static Stream<Arguments> complexNestedTestCases() {
        return Stream.of(
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId = HEAD(TAIL([1, 2, 3, 4, 5], 2))",
                "Nested function calls",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("gcId"),
                            BinaryOperator.EQUALS,
                            function("HEAD",
                                function("TAIL",
                                    intArray(1, 2, 3, 4, 5),
                                    numberLiteral(2)
                                )
                            )
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE values IN [[1, 2], [3, 4]]",
                "Nested array literals",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            identifier("values"),
                            BinaryOperator.IN,
                            arrayLiteral(
                                intArray(1, 2),
                                intArray(3, 4)
                            )
                        )
                    )
                )
            ),
            Arguments.of(
                "@SELECT * FROM ExecutionSample WHERE gcId IN [1, 2, 3] AND threadId = BEFORE_GC(timestamp)",
                "Mixed syntax",
                (Supplier<WhereNode>) () -> where(
                    condition(
                        binary(
                            binary(
                                identifier("gcId"),
                                BinaryOperator.IN,
                                intArray(1, 2, 3)
                            ),
                            BinaryOperator.AND,
                            binary(
                                identifier("threadId"),
                                BinaryOperator.EQUALS,
                                function("BEFORE_GC", identifier("timestamp"))
                            )
                        )
                    )
                )
            )
        );
    }

    @ParameterizedTest(name = "Complex nested expressions - {1}")
    @MethodSource("complexNestedTestCases")
    @DisplayName("Test complex nested expressions with arrays and functions")
    void testComplexNestedExpressions(String query, String description, Supplier<WhereNode> expectedWhereSupplier) throws Exception {
        QueryNode ast = parseQuery(query);
        WhereNode expectedWhere = expectedWhereSupplier.get();
        
        // Validate where clause
        WhereNode actualWhere = ast.where();
        assertNotNull(actualWhere, "WHERE clause should exist for " + description);
        
        // Use ASTBuilder.astEquals to compare the expected and actual AST nodes
        assertTrue(astEquals(expectedWhere, actualWhere), 
            "WHERE clause should match expected for " + description);
    }

    /**
     * Error test cases
     */
    static Stream<Arguments> errorTestCases() {
        return Stream.of(
            Arguments.of("@SELECT * FROM ExecutionSample WHERE gcId IN [1, 2,]", "Trailing comma"),
            Arguments.of("@SELECT * FROM ExecutionSample WHERE gcId IN [, 1, 2]", "Leading comma"),
            Arguments.of("@SELECT * FROM ExecutionSample WHERE gcId IN [1,, 2]", "Double comma"),
            Arguments.of("@SELECT * FROM ExecutionSample WHERE gcId IN [1 2 3]", "Missing commas"),
            Arguments.of("@SELECT * FROM ExecutionSample WHERE gcId IN [", "Unclosed bracket"),
            Arguments.of("@SELECT * FROM ExecutionSample WHERE gcId IN [1, 2, 3", "Missing closing bracket"),
            Arguments.of("@SELECT * FROM ExecutionSample WHERE gcId IN [SLICE([1, 2, 3], 1, 2]", "Unmatched brackets"),
            Arguments.of("@SELECT * FROM ExecutionSample WHERE gcId IN [HEAD([1, 2, 3)]", "Mismatched brackets")
        );
    }

    @ParameterizedTest(name = "Error cases - {1}")
    @MethodSource("errorTestCases")
    @DisplayName("Test error handling with malformed array syntax")
    void testErrorCases(String query, String description) {
        // All test cases should throw ParserException
        Throwable thrown = assertThrows(Exception.class, () -> {
            Lexer lexer = new Lexer(query);
            Parser parser = new Parser(lexer.tokenize());
            parser.parse();
        }, "Should throw an exception for " + description);
        
        // Verify the exception type is either ParserException or LexerException
        assertTrue(thrown instanceof me.bechberger.jfr.extended.ParserException || thrown instanceof Lexer.LexerException,
            "Expected ParserException or LexerException, but got: " + thrown.getClass().getName());
    }

    // ========================================================================================
    // Helper methods for parsing
    // ========================================================================================

    private static QueryNode parseQuery(String query) throws Exception {
        Lexer lexer = new Lexer(query);
        Parser parser = new Parser(lexer.tokenize(), query);
        ProgramNode programNode = parser.parse();
        // Extract the first query statement from the program
        for (StatementNode statement : programNode.statements()) {
            if (statement instanceof QueryNode) {
                return (QueryNode) statement;
            }
        }
        throw new ParserException("No QueryNode found in: " + programNode);
    }
}
