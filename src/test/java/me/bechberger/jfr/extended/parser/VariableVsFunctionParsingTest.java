package me.bechberger.jfr.extended.parser;

import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
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
 * Tests to verify that lowercase identifiers without parentheses are treated as variables,
 * not function calls, even when they match function names.
 */
public class VariableVsFunctionParsingTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    // ===== PARAMETERIZED PARSING TESTS =====
    
    static Stream<Arguments> variableNotFunctionTestCases() {
        return Stream.of(
            Arguments.of("@SELECT count FROM TestTable", "count", "should be parsed as variable"),
            Arguments.of("@SELECT max FROM TestTable", "max", "should be parsed as variable"),
            Arguments.of("@SELECT min FROM TestTable", "min", "should be parsed as variable"),
            Arguments.of("@SELECT avg FROM TestTable", "avg", "should be parsed as variable"),
            Arguments.of("@SELECT sum FROM TestTable", "sum", "should be parsed as variable")
        );
    }
    
    @ParameterizedTest
    @MethodSource("variableNotFunctionTestCases")
    @DisplayName("Lowercase function names without parentheses should be parsed as variables")
    void testVariableNotFunction(String input, String expectedVariable, String description) throws Exception {
        // Arrange & Act
        Parser parser = new Parser(input);
        ProgramNode program = parser.parse();
        
        // Assert - Should parse successfully without errors
        assertEquals(1, program.statements().size());
        StatementNode statement = program.statements().get(0);
        assertInstanceOf(QueryNode.class, statement);
        
        QueryNode queryNode = (QueryNode) statement;
        SelectNode selectNode = queryNode.select();
        
        // The SELECT should contain one item which is an identifier (variable)
        assertEquals(1, selectNode.items().size());
        SelectItemNode selectItem = selectNode.items().get(0);
        
        // The expression should be an IdentifierNode, not a FunctionCallNode
        ExpressionNode expression = selectItem.expression();
        assertInstanceOf(IdentifierNode.class, expression, 
            description + " but was parsed as " + expression.getClass().getSimpleName());
        
        IdentifierNode identifier = (IdentifierNode) expression;
        assertEquals(expectedVariable, identifier.name());
    }
    
    static Stream<Arguments> functionCallTestCases() {
        return Stream.of(
            Arguments.of("@SELECT COUNT() FROM TestTable", "COUNT"),
            Arguments.of("@SELECT MAX(value) FROM TestTable", "MAX"),
            Arguments.of("@SELECT MIN(value) FROM TestTable", "MIN"),
            Arguments.of("@SELECT AVG(value) FROM TestTable", "AVG"),
            Arguments.of("@SELECT SUM(value) FROM TestTable", "SUM")
        );
    }
    
    @ParameterizedTest
    @MethodSource("functionCallTestCases")
    @DisplayName("Function names with parentheses should be parsed as function calls")
    void testFunctionCall(String input, String expectedFunction) throws Exception {
        // Arrange & Act
        Parser parser = new Parser(input);
        ProgramNode program = parser.parse();
        
        // Assert - Should parse successfully
        assertEquals(1, program.statements().size());
        StatementNode statement = program.statements().get(0);
        assertInstanceOf(QueryNode.class, statement);
        
        QueryNode queryNode = (QueryNode) statement;
        SelectNode selectNode = queryNode.select();
        
        // The SELECT should contain one item which is a function call
        assertEquals(1, selectNode.items().size());
        SelectItemNode selectItem = selectNode.items().get(0);
        
        // The expression should be a FunctionCallNode
        ExpressionNode expression = selectItem.expression();
        assertInstanceOf(FunctionCallNode.class, expression, 
            "Expected function call but was " + expression.getClass().getSimpleName());
        
        FunctionCallNode functionCall = (FunctionCallNode) expression;
        assertEquals(expectedFunction, functionCall.functionName());
    }
    
    @Test
    @DisplayName("Variable assignment and usage should work with function-like names")
    void testVariableAssignmentAndUsage() {
        // Arrange
        framework.mockTable("TestTable")
            .withNumberColumn("value")
            .withRow(42L)
            .build();
        
        // Act - Execute assignment and then use variable
        var results = framework.executeMultiStatementQuery(
            "count := @SELECT COUNT(*) FROM TestTable;\n" +
            "@SELECT count FROM TestTable"
        );
        
        // Assert
        assertTrue(results.get(0).isSuccess(), "Assignment should succeed");
        assertTrue(results.get(1).isSuccess(), "Variable usage should succeed");
        
        var resultTable = results.get(1).getTable();
        assertEquals(1, resultTable.getRowCount());
        assertEquals(1L, resultTable.getNumber(0, 0)); // Should get the count value (1)
    }
    
    @Test
    @DisplayName("Mixed function calls and variable usage should work")
    void testMixedFunctionAndVariable() {
        // Arrange
        framework.mockTable("TestTable")
            .withNumberColumn("value")
            .withRow(10L)
            .withRow(20L)
            .withRow(30L)
            .build();
        
        // Act
        var results = framework.executeMultiStatementQuery(
            "total := @SELECT SUM(value) FROM TestTable;\n" +
            "count := @SELECT COUNT(*) FROM TestTable;\n" +
            "@SELECT total, count FROM TestTable LIMIT 1"
        );
        
        // Assert
        assertTrue(results.get(0).isSuccess(), "Total assignment should succeed");
        assertTrue(results.get(1).isSuccess(), "Count assignment should succeed");
        assertTrue(results.get(2).isSuccess(), "Variable usage should succeed");
        
        var resultTable = results.get(2).getTable();
        assertEquals(60L, resultTable.getNumber(0, "total"));
        assertEquals(3L, resultTable.getNumber(0, "count"));
    }
}
