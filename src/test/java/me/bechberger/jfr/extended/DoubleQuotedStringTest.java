package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test double-quoted string support in the query language.
 * Verifies that both single and double quotes work for string literals.
 */
class DoubleQuotedStringTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    static Stream<Arguments> stringLiteralTestCases() {
        return Stream.of(
            Arguments.of("'Hello World'", "Hello World"),
            Arguments.of("\"Hello World\"", "Hello World"),
            Arguments.of("'It\\'s working'", "It's working"),
            Arguments.of("\"It's working\"", "It's working"),
            Arguments.of("\"Say \\\"hello\\\"\"", "Say \"hello\""),
            Arguments.of("'Say \"hello\"'", "Say \"hello\""),
            // Tab characters should be allowed
            Arguments.of("'text with\ttab'", "text with\ttab"),
            Arguments.of("\"text with\ttab\"", "text with\ttab"),
            // Newline characters should be allowed  
            Arguments.of("'text with\nnewline'", "text with\nnewline"),
            Arguments.of("\"text with\nnewline\"", "text with\nnewline"),
            // Other special characters
            Arguments.of("'text with © symbol'", "text with © symbol"),
            Arguments.of("\"text with µ symbol\"", "text with µ symbol"),
            Arguments.of("'emoji 🚀 test'", "emoji 🚀 test"),
            Arguments.of("\"emoji 🚀 test\"", "emoji 🚀 test"),
            // Control characters
            Arguments.of("'text\r\nwith\rcrlf'", "text\r\nwith\rcrlf"),
            Arguments.of("\"text\r\nwith\rcrlf\"", "text\r\nwith\rcrlf")
        );
    }
    
    @ParameterizedTest
    @MethodSource("stringLiteralTestCases")
    @DisplayName("String literals should work with both single and double quotes")
    void testStringLiteralQuoting(String quotedString, String expectedValue) {
        framework.createSingleCellTable("Test", "value", "dummy");
        
        var result = framework.executeAndAssertSuccess(
            "@SELECT " + quotedString + " as result FROM Test"
        );
        
        assertEquals(expectedValue, result.getTable().getString(0, "result"));
    }
    
    @Test
    @DisplayName("String literals vs column references should be distinguished")
    void testStringLiteralsVsColumnNames() {
        framework.mockTable("Events")
            .withStringColumn("name")
            .withStringColumn("type")
            .withRow("Alice", "user")
            .withRow("Bob", "admin")
            .build();
        
        // Test that single quotes create string literals (not column references)
        var result1 = framework.executeAndAssertSuccess(
            "@SELECT 'constant' AS literal, name FROM Events"
        );
        assertEquals("constant", result1.getTable().getString(0, "literal"));
        assertEquals("Alice", result1.getTable().getString(0, "name"));
        
        // Test that double quotes also create string literals (not column references)
        System.out.println("Testing double quote literal query...");
        var result2 = framework.executeAndAssertSuccess(
            "@SELECT \"constant\" AS literal, name FROM Events"
        );
        System.out.println("Result2 rows: " + result2.getTable().getRowCount());
        System.out.println("Result2 columns: " + result2.getTable().getColumnCount());
        for (int i = 0; i < result2.getTable().getColumnCount(); i++) {
            System.out.println("Column " + i + ": " + result2.getTable().getColumns().get(i).name());
        }
        if (result2.getTable().getRowCount() > 0) {
            Object literalValue = result2.getTable().getCell(0, 0);
            Object nameValue = result2.getTable().getCell(0, 1);
            System.out.println("First row - literal: '" + literalValue + "', name: '" + nameValue + "'");
        }
        assertEquals("constant", result2.getTable().getString(0, "literal"));
        assertEquals("Alice", result2.getTable().getString(0, "name"));
        
        // Test column references without quotes
        var result3 = framework.executeAndAssertSuccess(
            "@SELECT name, type FROM Events WHERE name = 'Alice'"
        );
        assertEquals("Alice", result3.getTable().getString(0, "name"));
        assertEquals("user", result3.getTable().getString(0, "type"));
    }
    
    @Test
    @DisplayName("String literals in WHERE clauses should work with both quote types")
    void testStringLiteralsInWhere() throws Exception {
        framework.mockTable("Users")
            .withStringColumn("name")
            .withStringColumn("role")
            .withRow("Alice", "admin")
            .withRow("Bob", "user")
            .build();
        
        // Test single quotes in WHERE
        var result1 = framework.executeAndAssertSuccess(
            "@SELECT name FROM Users WHERE role = 'admin'"
        );
        System.out.println(Parser.parseAndValidate("@SELECT name, role FROM Users WHERE role = \"admin\"").format());

                System.out.println(result1.getTable().toString());

        assertEquals(1, result1.getTable().getRowCount());
        assertEquals("Alice", result1.getTable().getString(0, "name"));
        
        // Debug: Check what double quotes actually do
        var debugResult = framework.executeAndAssertSuccess(
            "@SELECT name, role FROM Users WHERE role = \"admin\""
        );
        System.out.println("Debug - Double quote WHERE result rows: " + debugResult.getTable().getRowCount());
        for (int i = 0; i < debugResult.getTable().getRowCount(); i++) {
            System.out.println("Row " + i + ": name=" + debugResult.getTable().getString(i, "name") + 
                             ", role=" + debugResult.getTable().getString(i, "role"));
        }
        
        // Test double quotes in WHERE - should work the same as single quotes
        var result2 = framework.executeAndAssertSuccess(
            "@SELECT name FROM Users WHERE role = \"admin\""
        );
        // If this fails, it means double quotes aren't working as string literals in WHERE
        assertEquals(1, result2.getTable().getRowCount(), 
            "Double quotes should work as string literals in WHERE clause, but got " + 
            result2.getTable().getRowCount() + " rows instead of 1");
        assertEquals("Alice", result2.getTable().getString(0, "name"));
    }
    
    @Test
    @DisplayName("Empty strings should work with both quote types")
    void testEmptyStrings() {
        framework.createSingleCellTable("Test", "value", "dummy");
        
        // Test empty single-quoted string
        var result1 = framework.executeAndAssertSuccess(
            "@SELECT '' as empty FROM Test"
        );
        assertEquals("", result1.getTable().getString(0, "empty"));
        
        // Test empty double-quoted string
        var result2 = framework.executeAndAssertSuccess(
            "@SELECT \"\" as empty FROM Test"
        );
        assertEquals("", result2.getTable().getString(0, "empty"));
    }
    
    @Test
    @DisplayName("Escape sequences and special characters should work correctly in both quote types")
    void testEscapeSequences() {
        framework.createSingleCellTable("Test", "value", "dummy");
        
        // Test newline and tab escapes
        var result1 = framework.executeAndAssertSuccess(
            "@SELECT 'line1\\nline2\\ttab' as text FROM Test"
        );
        assertEquals("line1\nline2\ttab", result1.getTable().getString(0, "text"));
        
        var result2 = framework.executeAndAssertSuccess(
            "@SELECT \"line1\\nline2\\ttab\" as text FROM Test"
        );
        assertEquals("line1\nline2\ttab", result2.getTable().getString(0, "text"));
        
        // Test literal tab and newline characters (not escaped)
        var result3 = framework.executeAndAssertSuccess(
            "@SELECT 'literal\ttab\nnewline' as text FROM Test"
        );
        assertEquals("literal\ttab\nnewline", result3.getTable().getString(0, "text"));
        
        var result4 = framework.executeAndAssertSuccess(
            "@SELECT \"literal\ttab\nnewline\" as text FROM Test"
        );
        assertEquals("literal\ttab\nnewline", result4.getTable().getString(0, "text"));
    }
    
    @Test
    @DisplayName("Unicode and special characters should work in strings")
    void testUnicodeAndSpecialCharacters() {
        framework.createSingleCellTable("Test", "value", "dummy");
        
        // Test Unicode characters
        var result1 = framework.executeAndAssertSuccess(
            "@SELECT 'Unicode: © ™ µ 🚀 ñ ü' as text FROM Test"
        );
        assertEquals("Unicode: © ™ µ 🚀 ñ ü", result1.getTable().getString(0, "text"));
        
        var result2 = framework.executeAndAssertSuccess(
            "@SELECT \"Unicode: © ™ µ 🚀 ñ ü\" as text FROM Test"
        );
        assertEquals("Unicode: © ™ µ 🚀 ñ ü", result2.getTable().getString(0, "text"));
        
        // Test various control characters and symbols
        var result3 = framework.executeAndAssertSuccess(
            "@SELECT 'Special: \r\n\t\b\f' as text FROM Test"
        );
        assertEquals("Special: \r\n\t\b\f", result3.getTable().getString(0, "text"));
    }
}
