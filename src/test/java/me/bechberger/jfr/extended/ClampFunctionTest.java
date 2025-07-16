package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import me.bechberger.jfr.extended.evaluator.AggregateFunctions;
import me.bechberger.jfr.extended.table.CellValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for the CLAMP function.
 * 
 * Tests cover:
 * - Basic clamping behavior (value below min, above max, within range)
 * - Type preservation (number vs float values)
 * - Error conditions (invalid arguments, min > max)
 * - Edge cases (equal values, boundary conditions)
 */
public class ClampFunctionTest {
    
    private FunctionRegistry registry;
    private AggregateFunctions.EvaluationContext context;
    
    @BeforeEach
    void setUp() {
        registry = FunctionRegistry.getInstance();
        context = new AggregateFunctions.EvaluationContext();
    }
    
    @Test
    void testClampFunctionExists() {
        assertTrue(registry.isFunction("CLAMP"), "CLAMP function should be registered");
        assertTrue(registry.isFunction("clamp"), "clamp function should be case insensitive");
    }
    
    @ParameterizedTest
    @CsvSource({
        "0, 10, 5, 5",      // Value within range
        "0, 10, -5, 0",     // Value below min (clamped to min)
        "0, 10, 15, 10",    // Value above max (clamped to max)
        "0, 10, 0, 0",      // Value equals min
        "0, 10, 10, 10",    // Value equals max
        "-5, 5, 0, 0",      // Value within negative range
        "-10, -1, -15, -10", // All negative values, clamp to min
        "-10, -1, 5, -1",   // All negative range, positive value (clamp to max)
        "100, 200, 150, 150" // Large numbers
    })
    void testClampBasicBehavior(double min, double max, double value, double expected) {
        List<CellValue> args = List.of(
            new CellValue.NumberValue(min),
            new CellValue.NumberValue(max), 
            new CellValue.NumberValue(value)
        );
        
        CellValue result = registry.evaluateFunction("CLAMP", args, context);
        
        assertTrue(result instanceof CellValue.NumberValue, "Result should be a NumberValue");
        assertEquals(expected, ((CellValue.NumberValue) result).value(), 0.0001, 
            "CLAMP(" + min + ", " + max + ", " + value + ") should equal " + expected);
    }
    
    @Test
    void testClampWithFloats() {
        List<CellValue> args = List.of(
            new CellValue.FloatValue(0.5),
            new CellValue.FloatValue(10.5),
            new CellValue.FloatValue(15.7)
        );
        
        CellValue result = registry.evaluateFunction("CLAMP", args, context);
        
        assertTrue(result instanceof CellValue.FloatValue, "Result should preserve FloatValue type");
        assertEquals(10.5, ((CellValue.FloatValue) result).value(), 0.0001, 
            "CLAMP(0.5, 10.5, 15.7) should equal 10.5");
    }
    
    @Test
    void testClampPreservesOriginalValueType() {
        // Test with NumberValue input
        List<CellValue> numberArgs = List.of(
            new CellValue.NumberValue(0),
            new CellValue.NumberValue(10),
            new CellValue.NumberValue(5)
        );
        
        CellValue numberResult = registry.evaluateFunction("CLAMP", numberArgs, context);
        assertTrue(numberResult instanceof CellValue.NumberValue, 
            "Result should preserve NumberValue type when input value is NumberValue");
        
        // Test with FloatValue input  
        List<CellValue> floatArgs = List.of(
            new CellValue.NumberValue(0),
            new CellValue.NumberValue(10),
            new CellValue.FloatValue(5.0)
        );
        
        CellValue floatResult = registry.evaluateFunction("CLAMP", floatArgs, context);
        assertTrue(floatResult instanceof CellValue.FloatValue,
            "Result should preserve FloatValue type when input value is FloatValue");
    }
    
    @Test
    void testClampErrorConditions() {
        // Test wrong number of arguments
        assertThrows(IllegalArgumentException.class, () -> {
            List<CellValue> tooFewArgs = List.of(
                new CellValue.NumberValue(0),
                new CellValue.NumberValue(10)
            );
            registry.evaluateFunction("CLAMP", tooFewArgs, context);
        }, "Should throw exception for too few arguments");
        
        assertThrows(IllegalArgumentException.class, () -> {
            List<CellValue> tooManyArgs = List.of(
                new CellValue.NumberValue(0),
                new CellValue.NumberValue(10),
                new CellValue.NumberValue(5),
                new CellValue.NumberValue(7)
            );
            registry.evaluateFunction("CLAMP", tooManyArgs, context);
        }, "Should throw exception for too many arguments");
        
        // Test non-numeric arguments
        assertThrows(IllegalArgumentException.class, () -> {
            List<CellValue> stringArgs = List.of(
                new CellValue.StringValue("hello"),
                new CellValue.NumberValue(10),
                new CellValue.NumberValue(5)
            );
            registry.evaluateFunction("CLAMP", stringArgs, context);
        }, "Should throw exception for non-numeric min argument");
        
        assertThrows(IllegalArgumentException.class, () -> {
            List<CellValue> stringArgs = List.of(
                new CellValue.NumberValue(0),
                new CellValue.StringValue("world"),
                new CellValue.NumberValue(5)
            );
            registry.evaluateFunction("CLAMP", stringArgs, context);
        }, "Should throw exception for non-numeric max argument");
        
        assertThrows(IllegalArgumentException.class, () -> {
            List<CellValue> stringArgs = List.of(
                new CellValue.NumberValue(0),
                new CellValue.NumberValue(10),
                new CellValue.StringValue("value")
            );
            registry.evaluateFunction("CLAMP", stringArgs, context);
        }, "Should throw exception for non-numeric value argument");
        
        // Test min > max
        assertThrows(IllegalArgumentException.class, () -> {
            List<CellValue> invalidRangeArgs = List.of(
                new CellValue.NumberValue(10),
                new CellValue.NumberValue(5),  // max < min
                new CellValue.NumberValue(7)
            );
            registry.evaluateFunction("CLAMP", invalidRangeArgs, context);
        }, "Should throw exception when min > max");
    }
    
    @Test
    void testClampEdgeCases() {
        // Test equal min and max
        List<CellValue> equalArgs = List.of(
            new CellValue.NumberValue(5),
            new CellValue.NumberValue(5),
            new CellValue.NumberValue(10)
        );
        
        CellValue result = registry.evaluateFunction("CLAMP", equalArgs, context);
        assertEquals(5.0, ((CellValue.NumberValue) result).value(), 
            "CLAMP(5, 5, 10) should equal 5");
        
        // Test very small numbers
        List<CellValue> smallArgs = List.of(
            new CellValue.FloatValue(0.0001),
            new CellValue.FloatValue(0.0002),
            new CellValue.FloatValue(0.00015)
        );
        
        CellValue smallResult = registry.evaluateFunction("CLAMP", smallArgs, context);
        assertEquals(0.00015, ((CellValue.FloatValue) smallResult).value(), 0.000001,
            "CLAMP should work with very small numbers");
        
        // Test very large numbers
        List<CellValue> largeArgs = List.of(
            new CellValue.NumberValue(1000000),
            new CellValue.NumberValue(2000000),
            new CellValue.NumberValue(1500000)
        );
        
        CellValue largeResult = registry.evaluateFunction("CLAMP", largeArgs, context);
        assertEquals(1500000.0, ((CellValue.NumberValue) largeResult).value(),
            "CLAMP should work with large numbers");
    }
    
    @Test
    void testClampWithMixedNumericTypes() {
        // Test mixing NumberValue and FloatValue
        List<CellValue> mixedArgs = List.of(
            new CellValue.NumberValue(0),      // min as NumberValue
            new CellValue.FloatValue(10.5),    // max as FloatValue
            new CellValue.NumberValue(8)       // value as NumberValue
        );
        
        CellValue result = registry.evaluateFunction("CLAMP", mixedArgs, context);
        assertTrue(result instanceof CellValue.NumberValue, 
            "Result type should match value argument type");
        assertEquals(8.0, ((CellValue.NumberValue) result).value(),
            "CLAMP should work with mixed numeric types");
    }
}
