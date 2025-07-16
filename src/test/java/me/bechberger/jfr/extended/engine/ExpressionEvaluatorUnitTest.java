package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.ast.ASTNodes.BinaryOperator;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ExpressionEvaluatorUnitTest {
    
    @Test
    void testEqualsOperatorDirectly() {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        ExpressionEvaluator evaluator = new ExpressionEvaluator(registry);
        
        // Test that EQUALS operator works directly
        Object result = evaluator.evaluateBinaryOperation(BinaryOperator.EQUALS, "Alice", "Alice");
        assertEquals(true, result);
        
        result = evaluator.evaluateBinaryOperation(BinaryOperator.EQUALS, "Alice", "Bob");
        assertEquals(false, result);
        
        result = evaluator.evaluateBinaryOperation(BinaryOperator.EQUALS, "Alice", "NonExistent");
        assertEquals(false, result);
        
        System.out.println("EQUALS operator test passed!");
    }
}
