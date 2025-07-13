package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static me.bechberger.jfr.extended.ASTBuilder.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test ORDER BY clause with various expressions and ASC/DESC keywords.
 * 
 * These tests verify that the parser correctly handles:
 * - Simple field references with ASC/DESC
 * - Complex expressions with ASC/DESC
 * - Function calls in ORDER BY
 * - Arithmetic expressions in ORDER BY
 * - Multiple ORDER BY fields with different directions
 */
class OrderByExpressionTest {

    private Parser createParser(String query) throws Lexer.LexerException {
        Lexer lexer = new Lexer(query);
        return new Parser(lexer.tokenize());
    }

    @ParameterizedTest
    @MethodSource("provideOrderByTestCases")
    void testOrderByExpressions(String query, ProgramNode expectedAST, String description) throws Exception {
        Parser parser = createParser(query);
        ProgramNode result = parser.parse();
        
        assertTrue(astEquals(expectedAST, result), 
            "AST should match for: " + description + "\nQuery: " + query);
    }

    private static Stream<Arguments> provideOrderByTestCases() {
        return Stream.of(
            // Simple field (defaults to ASC)
            Arguments.of(
                "@SELECT * FROM GarbageCollection ORDER BY duration",
                program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .orderBy(orderBy(orderField(identifier("duration"), SortOrder.ASC)))
                ),
                "Simple field (defaults to ASC)"
            ),
            
            // Simple field with explicit DESC
            Arguments.of(
                "@SELECT * FROM GarbageCollection ORDER BY duration DESC",
                program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .orderBy(orderBy(orderField(identifier("duration"), SortOrder.DESC)))
                ),
                "Simple field with explicit DESC"
            ),
            
            // Function call (defaults to ASC)
            Arguments.of(
                "@SELECT * FROM GarbageCollection ORDER BY ABS(duration - 10ms)",
                program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .orderBy(orderBy(orderField(
                            function("ABS", 
                                binary(identifier("duration"), BinaryOperator.SUBTRACT, durationLiteral("10ms"))
                            ),
                            SortOrder.ASC
                        )))
                ),
                "Function call with complex expression (defaults to ASC)"
            ),
            
            // Complex arithmetic expression with DESC
            Arguments.of(
                "@SELECT * FROM GarbageCollection ORDER BY (duration * 2 + 5ms) DESC",
                program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .orderBy(orderBy(orderField(
                            binary(
                                binary(identifier("duration"), BinaryOperator.MULTIPLY, numberLiteral(2)),
                                BinaryOperator.ADD,
                                durationLiteral("5ms")
                            ),
                            SortOrder.DESC
                        )))
                ),
                "Complex arithmetic expression with DESC"
            ),
            
            // Multiple order fields with different directions
            Arguments.of(
                "@SELECT * FROM GarbageCollection ORDER BY type, duration DESC, timestamp",
                program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .orderBy(orderBy(
                            orderField(identifier("type"), SortOrder.ASC),
                            orderField(identifier("duration"), SortOrder.DESC),
                            orderField(identifier("timestamp"), SortOrder.ASC)
                        ))
                ),
                "Multiple order fields with different directions"
            ),
            
            // Aggregate function with GROUP BY (sorting by group size)
            Arguments.of(
                "@SELECT type, COUNT(*) FROM GarbageCollection GROUP BY type ORDER BY COUNT(*) DESC",
                program(
                    query(
                        select(
                            selectItem(identifier("type")),
                            selectItem(function("COUNT", star()))
                        ), 
                        from(source("GarbageCollection"))
                    )
                    .groupBy(groupBy(identifier("type")))
                    .orderBy(orderBy(orderField(
                        function("COUNT", star()),
                        SortOrder.DESC
                    )))
                ),
                "Aggregate function with GROUP BY (sorting by group size)"
            ),
            
            // Field access with alias (defaults to ASC)
            Arguments.of(
                "@SELECT * FROM GarbageCollection AS gc ORDER BY gc.duration",
                program(
                    query(selectAll(), from(source("GarbageCollection", "gc")))
                        .orderBy(orderBy(orderField(
                            fieldAccess("gc", "duration"),
                            SortOrder.ASC
                        )))
                ),
                "Field access with alias (defaults to ASC)"
            ),
            
            // Percentile function with DESC
            Arguments.of(
                "@SELECT * FROM GarbageCollection ORDER BY P99(duration) DESC",
                program(
                    query(selectAll(), from(source("GarbageCollection")))
                        .orderBy(orderBy(orderField(
                            p99(identifier("duration")).build(),
                            SortOrder.DESC
                        )))
                ),
                "Percentile function with DESC"
            )
        );
    }

    @Test
    void testComplexOrderByWithCompleteQuery() throws Exception {
        String query = """
            @SELECT type, COUNT(*) as count, AVG(duration) as avg_duration
            FROM GarbageCollection 
            WHERE duration > 5ms 
            GROUP BY type 
            HAVING COUNT(*) > 10 
            ORDER BY AVG(duration) DESC, COUNT(*) ASC 
            LIMIT 100
            """;
        
        ProgramNode expected = program(
            query(
                select(
                    selectItem(identifier("type")),
                    selectItem(function("COUNT", star()), "count"),
                    selectItem(function("AVG", identifier("duration")), "avg_duration")
                ),
                from(source("GarbageCollection"))
            )
            .where(where(condition(
                binary(identifier("duration"), BinaryOperator.GREATER_THAN, durationLiteral("5ms"))
            )))
            .groupBy(groupBy(identifier("type")))
            .having(having(condition(
                binary(function("COUNT", star()), BinaryOperator.GREATER_THAN, numberLiteral(10))
            )))
            .orderBy(orderBy(
                orderField(function("AVG", identifier("duration")), SortOrder.DESC),
                orderField(function("COUNT", star()), SortOrder.ASC)
            ))
            .limit(limit(100))
        );
        
        Parser parser = createParser(query);
        ProgramNode result = parser.parse();
        
        assertTrue(astEquals(expected, result), 
            "Complete query with ORDER BY should match expected AST");
    }
}
