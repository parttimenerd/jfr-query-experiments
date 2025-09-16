package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for global variable assignments using AST matching and parameterized tests.
 * 
 * This test suite validates global variable assignment parsing and execution
 * with comprehensive coverage of different expression types and value validations.
 */
public class GlobalVariableTest {
    
    @BeforeEach
    void setUp() {
        // Setup can be used for future test context if needed
    }
    
    // ===== PARAMETERIZED PARSING TESTS =====
    
    @ParameterizedTest
    @CsvSource({
        "count := 42, count, 42.0",
        "value := 3.14, value, 3.14",
        "zero := 0, zero, 0.0",
        "large := 1000000, large, 1000000.0"
    })
    @DisplayName("Global variable numeric literal assignments should parse and execute correctly")
    void testGlobalVariableNumericLiterals(String query, String variableName, double expectedValue) throws Exception {
        var assignment = parseGlobalAssignment(query);
        assertEquals(variableName, assignment.variable());
        assertThat(assignment.expression()).isInstanceOf(LiteralNode.class);
        
        LiteralNode literal = (LiteralNode) assignment.expression();
        assertEquals(expectedValue, literal.value().extractNumericValue(), 0.001);
    }    @ParameterizedTest
    @CsvSource({
        "name := 'hello', name, hello",
        "message := 'world', message, world",
        "empty := '', empty, ''",
        "quoted := 'test string', quoted, test string"
    })
    @DisplayName("Global variable assignment with string literals should parse correctly")
    void testGlobalVariableStringLiterals(String input, String expectedVariable, String expectedValue) throws Exception {
        // Handle empty string case
        if ("''".equals(expectedValue)) {
            expectedValue = "";
        }
        
        // Arrange & Act
        GlobalVariableAssignmentNode assignment = parseGlobalAssignment(input);
        
        // Assert
        assertEquals(expectedVariable, assignment.variable());
        assertThat(assignment.expression())
            .isInstanceOf(LiteralNode.class)
            .extracting(expr -> ((LiteralNode) expr).value().toString())
            .isEqualTo(expectedValue);
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        "timeout := 3m",
        "interval := 1h", 
        "duration := 500ms",
        "delay := 2s",
        "period := 1d"
    })
    @DisplayName("Global variable assignment with duration literals should parse correctly")
    void testGlobalVariableDurationLiterals(String input) throws Exception {
        // Arrange & Act
        GlobalVariableAssignmentNode assignment = parseGlobalAssignment(input);
        
        // Assert
        assertThat(assignment.expression())
            .isInstanceOf(LiteralNode.class);
        
        // The exact parsing of duration depends on implementation
        // We just verify it parses as a literal
        LiteralNode literal = (LiteralNode) assignment.expression();
        assertNotNull(literal.value());
    }
    
    @ParameterizedTest
    @CsvSource({
        "result := 10 + 5, BinaryExpressionNode, ADD",
        "diff := 20 - 3, BinaryExpressionNode, SUBTRACT", 
        "product := 4 * 7, BinaryExpressionNode, MULTIPLY",
        "quotient := 15 / 3, BinaryExpressionNode, DIVIDE"
    })
    @DisplayName("Global variable assignment with binary expressions should parse correctly")
    void testGlobalVariableBinaryExpressions(String input, String expectedNodeType, String expectedOperator) throws Exception {
        // Arrange & Act
        GlobalVariableAssignmentNode assignment = parseGlobalAssignment(input);
        
        // Assert
        assertThat(assignment.expression())
            .isInstanceOf(BinaryExpressionNode.class);
            
        BinaryExpressionNode binaryExpr = (BinaryExpressionNode) assignment.expression();
        assertEquals(expectedOperator, binaryExpr.operator().name());
    }
    
    @ParameterizedTest
    @ValueSource(strings = {
        "negative := -5",
        "negDecimal := -3.14", 
        "negZero := -0"
    })
    @DisplayName("Global variable assignment with negative numbers should parse as unary expressions")
    void testGlobalVariableNegativeNumbers(String input) throws Exception {
        // Arrange & Act
        GlobalVariableAssignmentNode assignment = parseGlobalAssignment(input);
        
        // Assert - Negative numbers are parsed as UnaryExpressionNode with MINUS operator
        assertThat(assignment.expression())
            .isInstanceOf(UnaryExpressionNode.class);
            
        UnaryExpressionNode unaryExpr = (UnaryExpressionNode) assignment.expression();
        assertEquals("MINUS", unaryExpr.operator().name());
        assertThat(unaryExpr.operand()).isInstanceOf(LiteralNode.class);
    }
    
    // ===== AST STRUCTURE VALIDATION TESTS =====
    
    @Test
    @DisplayName("Global variable assignment should have correct AST structure")
    void testGlobalVariableASTStructure() throws Exception {
        // Arrange
        String input = "x := 42";
        
        // Act
        Parser parser = new Parser(input);
        ProgramNode program = parser.parse();
        
        // Assert - Validate complete AST structure
        assertEquals(1, program.statements().size());
        
        StatementNode statement = program.statements().get(0);
        assertThat(statement)
            .isInstanceOf(GlobalVariableAssignmentNode.class)
            .extracting(s -> ((GlobalVariableAssignmentNode) s).variable())
            .isEqualTo("x");
            
        GlobalVariableAssignmentNode assignment = (GlobalVariableAssignmentNode) statement;
        assertThat(assignment.expression())
            .isInstanceOf(LiteralNode.class)
            .extracting(e -> ((LiteralNode) e).value().extractNumericValue())
            .isEqualTo(42.0);
    }
    
    @Test
    @DisplayName("Should distinguish between query and global variable assignments")
    void testDistinguishAssignmentTypes() throws Exception {
        // Test that query assignments are still parsed as AssignmentNode
        Parser parser = new Parser("table := SELECT * FROM TestEvent");
        ProgramNode program = parser.parse();
        
        assertEquals(1, program.statements().size());
        StatementNode statement = program.statements().get(0);
        
        assertThat(statement)
            .isInstanceOf(AssignmentNode.class)
            .extracting(s -> ((AssignmentNode) s).variable())
            .isEqualTo("table");
        
        AssignmentNode assignment = (AssignmentNode) statement;
        assertThat(assignment.query())
            .isInstanceOf(QueryNode.class);
    }
    
    // ===== HELPER METHODS =====
    
    /**
     * Helper method to parse global variable assignments with validation
     */
    private GlobalVariableAssignmentNode parseGlobalAssignment(String input) throws Exception {
        Parser parser = new Parser(input);
        ProgramNode program = parser.parse();
        
        assertEquals(1, program.statements().size(), 
            "Expected exactly one statement in: " + input);
        
        StatementNode statement = program.statements().get(0);
        assertInstanceOf(GlobalVariableAssignmentNode.class, statement,
            "Expected GlobalVariableAssignmentNode for: " + input);
        
        return (GlobalVariableAssignmentNode) statement;
    }
    
    /**
     * Custom assertion helper for better AST validation
     */
    private static ASTAssertion assertThat(Object actual) {
        return new ASTAssertion(actual);
    }
    
    private static class ASTAssertion {
        private final Object actual;
        
        public ASTAssertion(Object actual) {
            this.actual = actual;
        }
        
        public ASTAssertion isInstanceOf(Class<?> expectedType) {
            assertInstanceOf(expectedType, actual);
            return this;
        }
        
        public ASTAssertion extracting(java.util.function.Function<Object, Object> extractor) {
            return new ASTAssertion(extractor.apply(actual));
        }
        
        public void isEqualTo(Object expected) {
            assertEquals(expected, actual);
        }
    }
}
