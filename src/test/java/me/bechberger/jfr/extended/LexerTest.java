package me.bechberger.jfr.extended;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.stream.Stream;

/**
 * Parameterized tests for the Lexer covering various tokenization scenarios.
 * Uses parameterized tests for numbers, strings, operators, durations, assignments, and subqueries.
 */
public class LexerTest {
    static Stream<Arguments> tokenizationCases() {
        return Stream.of(
            // Basic SQL
            Arguments.of(
                "SELECT * FROM GarbageCollection",
                new TokenType[] {TokenType.SELECT, TokenType.STAR, TokenType.FROM, TokenType.IDENTIFIER, TokenType.EOF},
                new String[] {"SELECT", "*", "FROM", "GarbageCollection", null}
            ),
            Arguments.of(
                "WHERE name = 'test string'",
                new TokenType[] {TokenType.WHERE, TokenType.IDENTIFIER, TokenType.EQUALS, TokenType.STRING, TokenType.EOF},
                new String[] {"WHERE", "name", "=", "'test string'", null}
            ),
            Arguments.of(
                "LIMIT 100",
                new TokenType[] {TokenType.LIMIT, TokenType.NUMBER, TokenType.EOF},
                new String[] {"LIMIT", "100", null}
            ),
            // Operators
            Arguments.of(
                "a >= b",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.GREATER_EQUAL, TokenType.IDENTIFIER, TokenType.EOF},
                new String[] {"a", ">=", "b", null}
            ),
            Arguments.of(
                "a <= b",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.LESS_EQUAL, TokenType.IDENTIFIER, TokenType.EOF},
                new String[] {"a", "<=", "b", null}
            ),
            Arguments.of(
                "a != b",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.NOT_EQUALS, TokenType.IDENTIFIER, TokenType.EOF},
                new String[] {"a", "!=", "b", null}
            ),
            Arguments.of(
                "a = b",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.EQUALS, TokenType.IDENTIFIER, TokenType.EOF},
                new String[] {"a", "=", "b", null}
            ),
            Arguments.of(
                "a > b",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.GREATER_THAN, TokenType.IDENTIFIER, TokenType.EOF},
                new String[] {"a", ">", "b", null}
            ),
            Arguments.of(
                "a < b",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.LESS_THAN, TokenType.IDENTIFIER, TokenType.EOF},
                new String[] {"a", "<", "b", null}
            ),
            // Durations and time units
            Arguments.of(
                "duration > 10ms",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.GREATER_THAN, TokenType.DURATION_LITERAL, TokenType.EOF},
                new String[] {"duration", ">", "10ms", null}
            ),
            Arguments.of(
                "duration > 5s",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.GREATER_THAN, TokenType.DURATION_LITERAL, TokenType.EOF},
                new String[] {"duration", ">", "5s", null}
            ),
            Arguments.of(
                "duration > 1min",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.GREATER_THAN, TokenType.DURATION_LITERAL, TokenType.EOF},
                new String[] {"duration", ">", "1min", null}
            ),
            Arguments.of(
                "duration > 2h",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.GREATER_THAN, TokenType.DURATION_LITERAL, TokenType.EOF},
                new String[] {"duration", ">", "2h", null}
            ),
            Arguments.of(
                "duration > 3d",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.GREATER_THAN, TokenType.DURATION_LITERAL, TokenType.EOF},
                new String[] {"duration", ">", "3d", null}
            ),
            Arguments.of(
                "duration > 100ns",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.GREATER_THAN, TokenType.DURATION_LITERAL, TokenType.EOF},
                new String[] {"duration", ">", "100ns", null}
            ),
            Arguments.of(
                "duration > 50us",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.GREATER_THAN, TokenType.DURATION_LITERAL, TokenType.EOF},
                new String[] {"duration", ">", "50us", null}
            ),
            // Assignment
            Arguments.of(
                "x := 42",
                new TokenType[] {TokenType.IDENTIFIER, TokenType.ASSIGN, TokenType.NUMBER, TokenType.EOF},
                new String[] {"x", ":=", "42", null}
            ),
            // Subquery
            Arguments.of(
                "SELECT * FROM [SELECT * FROM jdk.GarbageCollection]",
                new TokenType[] {TokenType.SELECT, TokenType.STAR, TokenType.FROM, TokenType.LBRACKET, TokenType.SELECT, TokenType.STAR, TokenType.FROM, TokenType.IDENTIFIER, TokenType.DOT, TokenType.IDENTIFIER, TokenType.RBRACKET, TokenType.EOF},
                new String[] {"SELECT", "*", "FROM", "[", "SELECT", "*", "FROM", "jdk", ".", "GarbageCollection", "]", null}
            ),
            // Edge: empty input
            Arguments.of(
                "",
                new TokenType[] {TokenType.EOF},
                new String[] {null}
            ),
            // Edge: only whitespace
            Arguments.of(
                "   \t\n  ",
                new TokenType[] {TokenType.EOF},
                new String[] {null}
            )
        );
    }

    @ParameterizedTest
    @MethodSource("tokenizationCases")
    void testTokenization(String input, TokenType[] expectedTypes, String[] expectedValues) throws Exception {
        Lexer lexer = new Lexer(input);
        List<Token> tokens = lexer.tokenize();
        assertEquals(expectedTypes.length, tokens.size(), "Token count");
        for (int i = 0; i < expectedTypes.length; i++) {
            assertEquals(expectedTypes[i], tokens.get(i).type(), "Token type at " + i);
            if (expectedValues[i] != null) {
                assertEquals(expectedValues[i], tokens.get(i).value(), "Token value at " + i);
            }
        }
    }

    static Stream<Arguments> numberCases() {
        return Stream.of(
            Arguments.of("100"),
            Arguments.of("0"),
            Arguments.of("42"),
            Arguments.of("1000"),
            Arguments.of("999")
        );
    }

    @ParameterizedTest
    @MethodSource("numberCases")
    void testNumbers(String numberValue) throws Exception {
        Lexer lexer = new Lexer("LIMIT " + numberValue);
        List<Token> tokens = lexer.tokenize();
        assertEquals(3, tokens.size());
        assertEquals(TokenType.LIMIT, tokens.get(0).type());
        assertEquals(TokenType.NUMBER, tokens.get(1).type());
        assertEquals(numberValue, tokens.get(1).value());
        assertEquals(TokenType.EOF, tokens.get(2).type());
    }
}
