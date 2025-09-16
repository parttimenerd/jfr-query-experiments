package me.bechberger.jfr.extended.table;

import me.bechberger.jfr.extended.ast.ASTNodes.BinaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for arithmetic operations on all numeric CellValue types
 * 
 * These tests verify that all numeric types (NUMBER, NUMBER, DURATION, TIMESTAMP, MEMORY_SIZE, RATE)
 * can participate in arithmetic operations with proper type compatibility checking and result types.
 */
public class CellValueArithmeticTest {
    
    private CellValue.NumberValue number1;
    private CellValue.NumberValue number2;
    private CellValue.NumberValue float1;
    private CellValue.NumberValue float2;
    private CellValue.DurationValue duration1;
    private CellValue.DurationValue duration2;
    private CellValue.TimestampValue timestamp1;
    private CellValue.TimestampValue timestamp2;
    private CellValue.MemorySizeValue memory1;
    private CellValue.MemorySizeValue memory2;
    private CellValue.RateValue rate1;
    private CellValue.RateValue rate2;
    
    @BeforeEach
    public void setUp() {
        number1 = new CellValue.NumberValue(10.0);
        number2 = new CellValue.NumberValue(3.0);
        float1 = new CellValue.NumberValue(10.5);
        float2 = new CellValue.NumberValue(2.5);
        duration1 = new CellValue.DurationValue(Duration.ofSeconds(10));
        duration2 = new CellValue.DurationValue(Duration.ofSeconds(3));
        timestamp1 = new CellValue.TimestampValue(Instant.ofEpochMilli(1000000));
        timestamp2 = new CellValue.TimestampValue(Instant.ofEpochMilli(500000));
        memory1 = new CellValue.MemorySizeValue(1024);
        memory2 = new CellValue.MemorySizeValue(512);
        rate1 = new CellValue.RateValue(100.0, Duration.ofSeconds(1));
        rate2 = new CellValue.RateValue(50.0, Duration.ofSeconds(1));
    }
    
    // Helper method to assert numeric equality with tolerance for doubles
    private void assertNumericEquals(String message, double expected, Object actual) {
        if (actual instanceof Number) {
            double actualValue = ((Number) actual).doubleValue();
            assertEquals(expected, actualValue, 1e-9, message);
        } else {
            fail("Expected numeric value but got: " + actual.getClass());
        }
    }
    
    /**
     * Test scenario: NUMBER + NUMBER operations
     * Purpose: Verify that NUMBER type operations preserve NUMBER type
     * Expected: Result should be NumberValue with correct calculation
     */
    @Test
    public void testNumberArithmetic() {
        CellValue result = number1.mapBinary(number2, BinaryOperator.ADD);
        assertTrue(result instanceof CellValue.NumberValue, "Result should be NumberValue");
        assertNumericEquals("10 + 3 = 13", 13.0, result.getValue());
        
        result = number1.mapBinary(number2, BinaryOperator.SUBTRACT);
        assertTrue(result instanceof CellValue.NumberValue, "Result should be NumberValue");
        assertNumericEquals("10 - 3 = 7", 7.0, result.getValue());
        
        result = number1.mapBinary(number2, BinaryOperator.MULTIPLY);
        assertTrue(result instanceof CellValue.NumberValue, "Result should be NumberValue");
        assertNumericEquals("10 * 3 = 30", 30.0, result.getValue());
        
        result = number1.mapBinary(number2, BinaryOperator.DIVIDE);
        assertTrue(result instanceof CellValue.NumberValue, "Result should be NumberValue");
        assertNumericEquals("10 / 3 = 3.333...", 10.0/3.0, result.getValue());
    }
    
    /**
     * Test scenario: NUMBER + NUMBER operations
     * Purpose: Verify that NUMBER type operations preserve NUMBER type
     * Expected: Result should be NumberValue with correct calculation
     */
    @Test
    public void testFloatArithmetic() {
        CellValue result = float1.mapBinary(float2, BinaryOperator.ADD);
        assertTrue(result instanceof CellValue.NumberValue, "Result should be NumberValue");
        assertNumericEquals("10.5 + 2.5 = 13.0", 13.0, result.getValue());
        
        result = float1.mapBinary(float2, BinaryOperator.SUBTRACT);
        assertTrue(result instanceof CellValue.NumberValue, "Result should be NumberValue");
        assertNumericEquals("10.5 - 2.5 = 8.0", 8.0, result.getValue());
        
        result = float1.mapBinary(float2, BinaryOperator.MULTIPLY);
        assertTrue(result instanceof CellValue.NumberValue, "Result should be NumberValue");
        assertNumericEquals("10.5 * 2.5 = 26.25", 26.25, result.getValue());
    }
    
    /**
     * Test scenario: NUMBER + NUMBER operations (type promotion)
     * Purpose: Verify that mixed NUMBER/NUMBER operations promote to NUMBER
     * Expected: Result should be NumberValue when mixing NUMBER and NUMBER
     */
    @Test
    public void testNumberFloatMixed() {
        CellValue result = number1.mapBinary(float2, BinaryOperator.ADD);
        assertTrue(result instanceof CellValue.NumberValue, "NUMBER + NUMBER should promote to NumberValue");
        assertNumericEquals("10.0 + 2.5 = 12.5", 12.5, result.getValue());
        
        result = float1.mapBinary(number2, BinaryOperator.SUBTRACT);
        assertTrue(result instanceof CellValue.NumberValue, "NUMBER - NUMBER should promote to NumberValue");
        assertNumericEquals("10.5 - 3.0 = 7.5", 7.5, result.getValue());
    }
    
    /**
     * Test scenario: DURATION + DURATION operations
     * Purpose: Verify that DURATION type operations preserve DURATION type
     * Expected: Result should be DurationValue with correct calculation
     */
    @Test
    public void testDurationArithmetic() {
        CellValue result = duration1.mapBinary(duration2, BinaryOperator.ADD);
        assertTrue(result instanceof CellValue.DurationValue, "Result should be DurationValue");
        Duration resultDuration = (Duration) result.getValue();
        assertEquals(13000, resultDuration.toMillis(), "10s + 3s = 13s");
        
        result = duration1.mapBinary(duration2, BinaryOperator.SUBTRACT);
        assertTrue(result instanceof CellValue.DurationValue, "Result should be DurationValue");
        resultDuration = (Duration) result.getValue();
        assertEquals(7000, resultDuration.toMillis(), "10s - 3s = 7s");
    }
    
    /**
     * Test scenario: TIMESTAMP - TIMESTAMP operations
     * Purpose: Verify that timestamp difference returns duration
     * Expected: TIMESTAMP - TIMESTAMP should return DurationValue
     */
    @Test
    public void testTimestampSubtraction() {
        CellValue result = timestamp1.mapBinary(timestamp2, BinaryOperator.SUBTRACT);
        assertTrue(result instanceof CellValue.DurationValue, "TIMESTAMP - TIMESTAMP should return DurationValue");
        Duration resultDuration = (Duration) result.getValue();
        assertEquals(500000, resultDuration.toMillis(), "timestamp difference should be 500000ms");
    }
    
    /**
     * Test scenario: TIMESTAMP + DURATION operations
     * Purpose: Verify that timestamp + duration returns timestamp
     * Expected: TIMESTAMP + DURATION should return TimestampValue
     */
    @Test
    public void testTimestampDurationArithmetic() {
        CellValue result = timestamp1.mapBinary(duration1, BinaryOperator.ADD);
        assertTrue(result instanceof CellValue.TimestampValue, "TIMESTAMP + DURATION should return TimestampValue");
        Instant resultTime = (Instant) result.getValue();
        assertEquals(1010000, resultTime.toEpochMilli(), "timestamp + 10s duration");
        
        result = timestamp1.mapBinary(duration1, BinaryOperator.SUBTRACT);
        assertTrue(result instanceof CellValue.TimestampValue, "TIMESTAMP - DURATION should return TimestampValue");
        resultTime = (Instant) result.getValue();
        assertEquals(990000, resultTime.toEpochMilli(), "timestamp - 10s duration");
    }
    
    /**
     * Test scenario: MEMORY_SIZE operations
     * Purpose: Verify that MEMORY_SIZE type operations preserve type
     * Expected: Result should be MemorySizeValue with correct calculation
     */
    @Test
    public void testMemorySizeArithmetic() {
        CellValue result = memory1.mapBinary(memory2, BinaryOperator.ADD);
        assertTrue(result instanceof CellValue.MemorySizeValue, "Result should be MemorySizeValue");
        assertNumericEquals("1024 + 512 = 1536", 1536.0, result.getValue());
        
        result = memory1.mapBinary(memory2, BinaryOperator.SUBTRACT);
        assertTrue(result instanceof CellValue.MemorySizeValue, "Result should be MemorySizeValue");
        assertNumericEquals("1024 - 512 = 512", 512.0, result.getValue());
    }
    
    /**
     * Test scenario: RATE operations
     * Purpose: Verify that RATE type operations preserve type and time unit
     * Expected: Result should be RateValue with correct calculation and preserved time unit
     */
    @Test
    public void testRateArithmetic() {
        CellValue result = rate1.mapBinary(rate2, BinaryOperator.ADD);
        assertTrue(result instanceof CellValue.RateValue, "Result should be RateValue");
        CellValue.RateValue resultRate = (CellValue.RateValue) result;
        assertNumericEquals("100 + 50 = 150", 150.0, resultRate.count());
        assertEquals(Duration.ofSeconds(1), resultRate.timeUnit(), "Time unit should be preserved");
        
        result = rate1.mapBinary(rate2, BinaryOperator.SUBTRACT);
        assertTrue(result instanceof CellValue.RateValue, "Result should be RateValue");
        resultRate = (CellValue.RateValue) result;
        assertNumericEquals("100 - 50 = 50", 50.0, resultRate.count());
    }
    
    /**
     * Test scenario: Mixed numeric type operations
     * Purpose: Verify that NUMBER/NUMBER can operate with other numeric types
     * Expected: Operations preserve the non-NUMBER/NUMBER type
     */
    @Test
    public void testMixedNumericOperations() {
        // NUMBER + DURATION
        CellValue result = number1.mapBinary(duration1, BinaryOperator.ADD);
        assertTrue(result instanceof CellValue.DurationValue, "NUMBER + DURATION should return DurationValue");
        
        // NUMBER * MEMORY_SIZE
        result = float2.mapBinary(memory1, BinaryOperator.MULTIPLY);
        assertTrue(result instanceof CellValue.MemorySizeValue, "NUMBER * MEMORY_SIZE should return MemorySizeValue");
        assertNumericEquals("2.5 * 1024 = 2560", 2560.0, result.getValue());
        
        // RATE / NUMBER
        result = rate1.mapBinary(number2, BinaryOperator.DIVIDE);
        assertTrue(result instanceof CellValue.RateValue, "RATE / NUMBER should return RateValue");
        CellValue.RateValue resultRate = (CellValue.RateValue) result;
        assertNumericEquals("100 / 3 = 33.333...", 100.0/3.0, resultRate.count());
    }
    
    /**
     * Test scenario: Type compatibility checking
     * Purpose: Verify that incompatible types are rejected
     * Expected: Operations between incompatible types should throw exceptions
     */
    @Test
    public void testIncompatibleTypes() {
        CellValue stringValue = new CellValue.StringValue("test");
        CellValue boolValue = new CellValue.BooleanValue(true);
        
        // STRING + NUMBER should fail
        try {
            stringValue.mapBinary(number1, BinaryOperator.ADD);
            fail("STRING + NUMBER should throw exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("operation"), "Should mention operation incompatibility");
        }
        
        // BOOLEAN * NUMBER should fail
        try {
            boolValue.mapBinary(float1, BinaryOperator.MULTIPLY);
            fail("BOOLEAN * NUMBER should throw exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("operation"), "Should mention operation incompatibility");
        }
        
        // DURATION + MEMORY_SIZE should fail (different non-number types)
        try {
            duration1.mapBinary(memory1, BinaryOperator.ADD);
            fail("DURATION + MEMORY_SIZE should throw exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("operation"), "Should mention operation incompatibility");
        }
    }
    
    /**
     * Test scenario: Division by zero handling
     * Purpose: Verify that division by zero is properly handled
     * Expected: Should throw ArithmeticException for division by zero
     */
    @Test
    public void testDivisionByZero() {
        CellValue zero = new CellValue.NumberValue(0.0);
        
        try {
            number1.mapBinary(zero, BinaryOperator.DIVIDE);
            fail("Division by zero should throw ArithmeticException");
        } catch (ArithmeticException e) {
            assertTrue(e.getMessage().contains("Division by zero"), "Should mention division by zero");
        }
        
        try {
            float1.mapBinary(zero, BinaryOperator.MODULO);
            fail("Modulo by zero should throw ArithmeticException");
        } catch (ArithmeticException e) {
            assertTrue(e.getMessage().contains("Modulo by zero"), "Should mention modulo by zero");
        }
    }
    
    /**
     * Test scenario: Map function for unary operations
     * Purpose: Verify that mapNumeric works for unary transformations
     * Expected: Type should be preserved for unary operations
     */
    @Test
    public void testUnaryMapOperations() {
        // Test ABS on negative number
        CellValue negativeNumber = new CellValue.NumberValue(-5.0);
        CellValue result = negativeNumber.mapNumeric(Math::abs);
        assertTrue(result instanceof CellValue.NumberValue, "Result should be NumberValue");
        assertNumericEquals("abs(-5) = 5", 5.0, result.getValue());
        
        // Test SQRT on float
        result = float1.mapNumeric(Math::sqrt);
        assertTrue(result instanceof CellValue.NumberValue, "Result should be NumberValue");
        assertNumericEquals("sqrt(10.5)", Math.sqrt(10.5), result.getValue());
        
        // Test doubling a memory size
        result = memory1.mapNumeric(x -> x * 2);
        assertTrue(result instanceof CellValue.MemorySizeValue, "Result should be MemorySizeValue");
        assertNumericEquals("1024 * 2 = 2048", 2048.0, result.getValue());
    }
    
    /**
     * Test scenario: Type checking helper methods
     * Purpose: Verify that isNumeric and extractNumericValue work correctly
     * Expected: Proper identification of numeric types and value extraction
     */
    @Test
    public void testTypeHelpers() {
        // Test isNumeric
        assertTrue(number1.isNumeric(), "NumberValue should be numeric");
        assertTrue(float1.isNumeric(), "NumberValue should be numeric");
        assertTrue(duration1.isNumeric(), "DurationValue should be numeric");
        assertTrue(timestamp1.isNumeric(), "TimestampValue should be numeric");
        assertTrue(memory1.isNumeric(), "MemorySizeValue should be numeric");
        assertTrue(rate1.isNumeric(), "RateValue should be numeric");
        
        CellValue stringValue = new CellValue.StringValue("test");
        CellValue boolValue = new CellValue.BooleanValue(true);
        assertFalse(stringValue.isNumeric(), "StringValue should not be numeric");
        assertFalse(boolValue.isNumeric(), "BooleanValue should not be numeric");
        
        // Test extractNumericValue
        assertNumericEquals("Number extraction", 10.0, number1.extractNumericValue());
        assertNumericEquals("Float extraction", 10.5, float1.extractNumericValue());
        assertNumericEquals("Duration extraction (ms)", 10000.0, duration1.extractNumericValue());
        assertNumericEquals("Memory extraction", 1024.0, memory1.extractNumericValue());
        assertNumericEquals("Rate extraction", 100.0, rate1.extractNumericValue());
    }
    
    @ParameterizedTest
    @CsvSource({
        "10.0, 5.0, ADD, 15.0",
        "10.0, 5.0, SUBTRACT, 5.0",
        "10.0, 5.0, MULTIPLY, 50.0",
        "10.0, 5.0, DIVIDE, 2.0",
        "15.0, 3.0, ADD, 18.0",
        "15.0, 3.0, SUBTRACT, 12.0",
        "15.0, 3.0, MULTIPLY, 45.0",
        "15.0, 3.0, DIVIDE, 5.0"
    })
    public void testArithmeticOperations(double left, double right, String operation, double expected) {
        CellValue.NumberValue leftValue = new CellValue.NumberValue(left);
        CellValue.NumberValue rightValue = new CellValue.NumberValue(right);
        BinaryOperator operator = BinaryOperator.valueOf(operation);
        
        CellValue result = leftValue.mapBinary(rightValue, operator);
        
        assertTrue(result instanceof CellValue.NumberValue, "Result should be NumberValue");
        CellValue.NumberValue numberResult = (CellValue.NumberValue) result;
        assertEquals(expected, numberResult.value(), 1e-9, "Operation result should match expected value");
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"ADD", "SUBTRACT", "MULTIPLY", "DIVIDE"})
    public void testFloatArithmeticOperations(String operation) {
        BinaryOperator operator = BinaryOperator.valueOf(operation);
        CellValue result = float1.mapBinary(float2, operator);
        
        assertTrue(result instanceof CellValue.NumberValue, "Float operations should return NumberValue");
        CellValue.NumberValue floatResult = (CellValue.NumberValue) result;
        
        // Test that the operation was performed
        assertNotNull(floatResult.value(), "Result value should not be null");
        assertTrue(Double.isFinite(floatResult.value()), "Result should be a finite number");
    }
}
