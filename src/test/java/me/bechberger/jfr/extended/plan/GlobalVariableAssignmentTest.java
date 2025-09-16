package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.table.CellValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for global variable assignments with direct expression evaluation.
 */
public class GlobalVariableAssignmentTest {
    
    private QueryExecutionContext context;
    private StatementExecutionVisitor visitor;
    
    @BeforeEach
    void setUp() {
        // Create minimal metadata for testing
        JFRFileMetadata metadata = new JFRFileMetadata(
            java.util.Set.of("TestEvent")
        );
        context = new QueryExecutionContext(metadata);
        
        // Create visitor
        AstToPlanConverter converter = null; // Not needed for expression evaluation
        visitor = new StatementExecutionVisitor(converter, context);
    }
    
    @Test
    void testSimpleGlobalVariableAssignment() throws Exception {
        // Parse a global variable assignment
        Parser parser = new Parser("x := 3");
        ProgramNode program = parser.parse();
        StatementNode statement = program.statements().get(0);
        
        // Should be a GlobalVariableAssignmentNode
        assertTrue(statement instanceof GlobalVariableAssignmentNode);
        GlobalVariableAssignmentNode globalVar = (GlobalVariableAssignmentNode) statement;
        assertEquals("x", globalVar.variable());
        
        // Execute the assignment
        QueryResult result = visitor.executeStatement(statement);
        assertTrue(result.isSuccess());
        
        // Check that the variable was stored
        Object value = context.getVariable("x");
        assertNotNull(value);
        assertTrue(value instanceof CellValue);
        assertEquals(3.0, ((CellValue) value).extractNumericValue());
    }
    
    @Test
    void testDurationVariableAssignment() throws Exception {
        // Parse a duration assignment
        Parser parser = new Parser("timeout := 3m");
        ProgramNode program = parser.parse();
        StatementNode statement = program.statements().get(0);
        
        // Should be a GlobalVariableAssignmentNode
        assertTrue(statement instanceof GlobalVariableAssignmentNode);
        
        // Execute the assignment
        QueryResult result = visitor.executeStatement(statement);
        assertTrue(result.isSuccess());
        
        // Check that the variable was stored
        Object value = context.getVariable("timeout");
        assertNotNull(value);
        assertTrue(value instanceof CellValue);
    }
    
    @Test
    void testStringVariableAssignment() throws Exception {
        // Parse a string assignment
        Parser parser = new Parser("name := \"test\"");
        ProgramNode program = parser.parse();
        StatementNode statement = program.statements().get(0);
        
        // Should be a GlobalVariableAssignmentNode
        assertTrue(statement instanceof GlobalVariableAssignmentNode);
        
        // Execute the assignment
        QueryResult result = visitor.executeStatement(statement);
        assertTrue(result.isSuccess());
        
        // Check that the variable was stored
        Object value = context.getVariable("name");
        assertNotNull(value);
        assertTrue(value instanceof CellValue);
    }
    
    @Test
    void testVariableInQueryExpression() throws Exception {
        // First assign a variable
        Parser parser1 = new Parser("threshold := 100");
        ProgramNode program1 = parser1.parse();
        visitor.executeStatement(program1.statements().get(0));
        
        // Now try to parse an expression that uses the variable
        Parser parser2 = new Parser("x := threshold * 2");
        ProgramNode program2 = parser2.parse();
        StatementNode statement = program2.statements().get(0);
        
        assertTrue(statement instanceof GlobalVariableAssignmentNode);
        
        // Execute the assignment
        QueryResult result = visitor.executeStatement(statement);
        assertTrue(result.isSuccess());
        
        // Check that the computed variable was stored
        Object value = context.getVariable("x");
        assertNotNull(value);
        assertTrue(value instanceof CellValue);
        assertEquals(200.0, ((CellValue) value).extractNumericValue());
    }
}
