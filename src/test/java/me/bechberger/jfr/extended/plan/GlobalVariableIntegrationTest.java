package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.table.CellValue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test demonstrating global variable assignment functionality.
 */
public class GlobalVariableIntegrationTest {
    
    private QueryExecutionContext context;
    private StatementExecutionVisitor visitor;
    private AstToPlanConverter planConverter;
    
    @BeforeEach
    void setUp() {
        // Create minimal metadata for testing
        JFRFileMetadata metadata = new JFRFileMetadata(
            java.util.Set.of("TestEvent")
        );
        context = new QueryExecutionContext(metadata);
        planConverter = new AstToPlanConverter(null); // No raw executor needed for this test
        visitor = new StatementExecutionVisitor(planConverter, context);
    }
    
    @Test
    void testGlobalVariableAssignmentExecution() throws Exception {
        // Test execution of global variable assignment
        Parser parser = new Parser("x := 42");
        ProgramNode program = parser.parse();
        
        StatementNode statement = program.statements().get(0);
        assertInstanceOf(GlobalVariableAssignmentNode.class, statement);
        
        // Execute the assignment
        QueryResult result = visitor.executeStatement(statement);
        
        // Verify the assignment was successful
        assertTrue(result.isSuccess());
        
        // Verify the variable was stored in context
        Object value = context.getVariable("x");
        assertNotNull(value);
        assertInstanceOf(CellValue.class, value);
        
        CellValue cellValue = (CellValue) value;
        assertEquals(42L, cellValue.extractNumericValue());
    }
    
    @Test
    void testGlobalVariableUsageInExpressions() throws Exception {
        // First assign a global variable
        Parser parser1 = new Parser("multiplier := 5");
        ProgramNode program1 = parser1.parse();
        StatementNode assignment = program1.statements().get(0);
        
        QueryResult result1 = visitor.executeStatement(assignment);
        assertTrue(result1.isSuccess());
        
        // Verify the variable was stored
        Object value = context.getVariable("multiplier");
        assertNotNull(value);
        assertInstanceOf(CellValue.class, value);
        
        CellValue cellValue = (CellValue) value;
        assertEquals(5L, cellValue.extractNumericValue());
        
        // Now test using the variable in another expression
        Parser parser2 = new Parser("result := multiplier * 10");
        ProgramNode program2 = parser2.parse();
        StatementNode expression = program2.statements().get(0);
        
        QueryResult result2 = visitor.executeStatement(expression);
        assertTrue(result2.isSuccess());
        
        // Verify the computed result
        Object resultValue = context.getVariable("result");
        assertNotNull(resultValue);
        assertInstanceOf(CellValue.class, resultValue);
        
        CellValue resultCellValue = (CellValue) resultValue;
        assertEquals(50L, resultCellValue.extractNumericValue());
    }
    
    @Test
    void testDifferentVariableTypes() throws Exception {
        // Test string variable
        Parser parser1 = new Parser("name := \"test\"");
        ProgramNode program1 = parser1.parse();
        QueryResult result1 = visitor.executeStatement(program1.statements().get(0));
        assertTrue(result1.isSuccess());
        
        Object nameValue = context.getVariable("name");
        assertInstanceOf(CellValue.class, nameValue);
        
        // Test arithmetic expression
        Parser parser2 = new Parser("calc := 10 + 20 * 2");
        ProgramNode program2 = parser2.parse();
        QueryResult result2 = visitor.executeStatement(program2.statements().get(0));
        assertTrue(result2.isSuccess());
        
        Object calcValue = context.getVariable("calc");
        assertInstanceOf(CellValue.class, calcValue);
        CellValue calcCellValue = (CellValue) calcValue;
        assertEquals(50L, calcCellValue.extractNumericValue()); // 10 + (20 * 2) = 50
    }
}
