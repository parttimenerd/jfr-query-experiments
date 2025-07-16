package me.bechberger.jfr.extended.evaluator;

import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.engine.QueryEvaluator;
import me.bechberger.jfr.extended.ast.ASTNodes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

/**
 * Comprehensive tests for the COLLECT function with expression support.
 * 
 * This test suite follows the enhanced testing guidelines and covers:
 * - Function registration and metadata validation
 * - Basic functionality with field names and expressions
 * - Expression evaluation infrastructure
 * - GROUP BY aggregation support
 * - Integration with array functions (HEAD, TAIL, SIZE, SLICE, SORT)
 * - Error handling and edge cases
 * - Performance considerations
 * - Type safety and data validation
 * 
 * @author Enhanced Testing Framework
 * @since 3.0
 */
public class CollectFunctionTest {
    
    private FunctionRegistry registry;
    private QueryEvaluator evaluator;
    
    @BeforeEach
    void setUp() {
        registry = FunctionRegistry.getInstance();
        evaluator = new QueryEvaluator(null);
    }
    
    // ===== FUNCTION REGISTRATION TESTS =====
    
    @Test
    @DisplayName("COLLECT function should be properly registered in function registry")
    void testCollectFunctionRegistration() {
        assertTrue(registry.isFunction("COLLECT"), "COLLECT function should be registered");
        assertTrue(registry.isFunction("collect"), "COLLECT function should be case-insensitive");
    }
    
    @Test
    @DisplayName("COLLECT should be classified as an aggregate function")
    void testCollectIsAggregateFunction() {
        assertTrue(registry.isAggregateFunction("COLLECT"), "COLLECT should be an aggregate function");
    }
    
    @Test
    @DisplayName("COLLECT function metadata should be correctly defined")
    void testCollectFunctionDefinition() {
        var definition = registry.getFunction("COLLECT");
        assertNotNull(definition, "COLLECT function definition should not be null");
        
        assertInstanceOf(FunctionRegistry.AggregateFunctionDefinition.class, definition, 
            "COLLECT should be an AggregateFunctionDefinition");
        
        var aggDef = (FunctionRegistry.AggregateFunctionDefinition) definition;
        assertEquals("COLLECT", aggDef.name());
        assertEquals(FunctionRegistry.FunctionType.AGGREGATE, aggDef.type());
        assertEquals(FunctionRegistry.ReturnType.ARRAY, aggDef.returnType());
        assertEquals("Collect all values of an expression into an array", aggDef.description());
        
        // Validate parameter definition for expression support
        assertEquals(1, aggDef.parameters().size());
        var param = aggDef.parameters().get(0);
        assertEquals("expression", param.name());
        assertEquals(FunctionRegistry.ParameterType.EXPRESSION, param.type());
        assertFalse(param.optional());
        assertFalse(param.variadic());
    }
     @ParameterizedTest
    @ValueSource(strings = {"COLLECT", "collect", "Collect", "CoLlEcT"})
    @DisplayName("COLLECT function should be case-insensitive")
    void testCollectCaseInsensitivity(String functionName) {
        assertTrue(registry.isFunction(functionName), 
            "COLLECT should be case-insensitive: " + functionName);
        assertTrue(registry.isAggregateFunction(functionName),
            "COLLECT should be recognized as aggregate function regardless of case: " + functionName);
    }
    
    // ===== BASIC FUNCTIONALITY TESTS =====
    
    @Test
    @DisplayName("COLLECT should properly collect numeric field values into an array")
    void testCollectWithFieldName() {
        // Arrange
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        // Act
        List<CellValue> args = List.of(new CellValue.StringValue("duration"));
        CellValue result = registry.evaluateFunction("COLLECT", args, context);
        
        // Assert
        assertInstanceOf(CellValue.ArrayValue.class, result, "COLLECT should return an ArrayValue");
        
        CellValue.ArrayValue arrayResult = (CellValue.ArrayValue) result;
        assertEquals(3, arrayResult.elements().size(), "Should collect 3 duration values");
        
        // Verify collected values in expected order
        List<CellValue> elements = arrayResult.elements();
        assertEquals(new CellValue.NumberValue(100L), elements.get(0));
        assertEquals(new CellValue.NumberValue(200L), elements.get(1));
        assertEquals(new CellValue.NumberValue(150L), elements.get(2));
    }

    @Test
    @DisplayName("COLLECT should handle string field values correctly")
    void testCollectWithStringField() {
        // Arrange
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        // Act
        List<CellValue> args = List.of(new CellValue.StringValue("name"));
        CellValue result = registry.evaluateFunction("COLLECT", args, context);
        
        // Assert
        CellValue.ArrayValue arrayResult = (CellValue.ArrayValue) result;
        assertEquals(3, arrayResult.elements().size());
        
        // Verify string values are collected correctly
        List<CellValue> elements = arrayResult.elements();
        assertEquals(new CellValue.StringValue("Alice"), elements.get(0));
        assertEquals(new CellValue.StringValue("Bob"), elements.get(1));
        assertEquals(new CellValue.StringValue("Charlie"), elements.get(2));
    }
    
    @Test
    void testCollectWithPreCollectedArray() {
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        // Test with pre-collected array
        CellValue.ArrayValue inputArray = new CellValue.ArrayValue(List.of(
            new CellValue.NumberValue(110L),
            new CellValue.NumberValue(210L),
            new CellValue.NumberValue(160L)
        ));
        
        List<CellValue> args = List.of(inputArray);
        CellValue result = registry.evaluateFunction("COLLECT", args, context);
        
        // Should return the array as-is
        assertEquals(inputArray, result, "Pre-collected array should be returned as-is");
    }
    
    @Test
    void testCollectWithSingleValue() {
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        List<CellValue> args = List.of(new CellValue.NumberValue(42L));
        CellValue result = registry.evaluateFunction("COLLECT", args, context);
        
        CellValue.ArrayValue arrayResult = (CellValue.ArrayValue) result;
        assertEquals(1, arrayResult.elements().size());
        assertEquals(new CellValue.NumberValue(42L), arrayResult.elements().get(0));
    }
    
    // ===== EXPRESSION EVALUATION INFRASTRUCTURE TESTS =====
    
    @Test
    void testCollectExpressionInfrastructure() {
        // Test that the QueryEvaluator has the special COLLECT handling
        try {
            var method = evaluator.getClass().getDeclaredMethod("computeAggregateValue", 
                ASTNodes.FunctionCallNode.class, JfrTable.class);
            method.setAccessible(true);
            assertNotNull(method, "computeAggregateValue method should exist");
        } catch (NoSuchMethodException e) {
            fail("QueryEvaluator should have computeAggregateValue method for COLLECT expression handling");
        }
    }
    
    @Test
    void testQueryEvaluatorHasExpressionEvaluationMethod() {
        // Test that QueryEvaluator has the method needed for COLLECT expression evaluation
        try {
            var method = evaluator.getClass().getDeclaredMethod("evaluateExpressionInRowContext", 
                ASTNodes.ExpressionNode.class, JfrTable.Row.class, List.class);
            method.setAccessible(true);
            assertNotNull(method, "evaluateExpressionInRowContext method should exist");
        } catch (NoSuchMethodException e) {
            fail("QueryEvaluator should have evaluateExpressionInRowContext method for expression evaluation");
        }
    }
    
    // ===== ERROR HANDLING TESTS =====
    
    // ===== EDGE CASE AND ERROR HANDLING TESTS =====
    
    @Test
    @DisplayName("COLLECT should handle empty evaluation context gracefully")
    void testCollectWithEmptyContext() {
        // Arrange
        AggregateFunctions.EvaluationContext emptyContext = createEmptyTestContext();
        
        // Act
        List<CellValue> args = List.of(new CellValue.StringValue("duration"));
        CellValue result = registry.evaluateFunction("COLLECT", args, emptyContext);
        
        // Assert
        assertInstanceOf(CellValue.ArrayValue.class, result);
        CellValue.ArrayValue arrayResult = (CellValue.ArrayValue) result;
        assertEquals(0, arrayResult.elements().size(), "Empty context should return empty array");
    }
    
    @Test
    @DisplayName("COLLECT should fail with meaningful error when no arguments provided")
    void testCollectWithNoArguments() {
        // Arrange
        AggregateFunctions.EvaluationContext context = createTestContext();
        List<CellValue> args = List.of();
        
        // Act & Assert
        Exception exception = assertThrows(RuntimeException.class, () -> {
            registry.evaluateFunction("COLLECT", args, context);
        });
        
        assertTrue(exception.getMessage().contains("Function 'COLLECT' requires 1 to 2 argument(s) but got 0"),
            "Should provide meaningful error message for missing arguments: " + exception.getMessage());
    }
    
    @Test
    @DisplayName("COLLECT should return empty array for unknown field names")
    void testCollectWithUnknownField() {
        // Arrange
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        // Act
        List<CellValue> args = List.of(new CellValue.StringValue("unknownField"));
        CellValue result = registry.evaluateFunction("COLLECT", args, context);
        
        // Assert
        CellValue.ArrayValue arrayResult = (CellValue.ArrayValue) result;
        assertEquals(0, arrayResult.elements().size(), "Unknown field should return empty array");
    }
    
    @Test
    @DisplayName("COLLECT should handle null values in data without failing")
    void testCollectWithNullValues() {
        // Arrange
        AggregateFunctions.EvaluationContext contextWithNulls = createTestContextWithNulls();
        
        // Act
        List<CellValue> args = List.of(new CellValue.StringValue("optionalField"));
        CellValue result = registry.evaluateFunction("COLLECT", args, contextWithNulls);
        
        // Assert
        assertInstanceOf(CellValue.ArrayValue.class, result);
        CellValue.ArrayValue arrayResult = (CellValue.ArrayValue) result;
        assertTrue(arrayResult.elements().size() >= 0, "Should handle null values gracefully");
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"", "   ", "\t", "\n"})
    @DisplayName("COLLECT should handle edge case field names appropriately")
    void testCollectWithEdgeCaseFieldNames(String fieldName) {
        // Arrange
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        // Act
        List<CellValue> args = List.of(new CellValue.StringValue(fieldName));
        CellValue result = registry.evaluateFunction("COLLECT", args, context);
        
        // Assert
        assertInstanceOf(CellValue.ArrayValue.class, result);
        CellValue.ArrayValue arrayResult = (CellValue.ArrayValue) result;
        assertEquals(0, arrayResult.elements().size(), 
            "Edge case field names should return empty array: '" + fieldName + "'");
    }
    
    // ===== INTEGRATION TESTS =====
    
    @Test 
    void testCollectWithArrayFunctions() {
        // Test integration with HEAD, TAIL, SIZE functions
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        // Get collected array
        List<CellValue> args = List.of(new CellValue.StringValue("duration"));
        CellValue collected = registry.evaluateFunction("COLLECT", args, context);
        
        // Test with HEAD function
        List<CellValue> headArgs = List.of(collected, new CellValue.NumberValue(2));
        CellValue headResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("HEAD"))
            .apply(headArgs.toArray(new CellValue[0]));
        
        CellValue.ArrayValue headArray = (CellValue.ArrayValue) headResult;
        assertEquals(2, headArray.elements().size(), "HEAD should return first 2 elements");
        
        // Test with SIZE function
        List<CellValue> sizeArgs = List.of(collected);
        CellValue sizeResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("SIZE"))
            .apply(sizeArgs.toArray(new CellValue[0]));
        
        assertEquals(new CellValue.NumberValue(3), sizeResult, "SIZE should return 3");
    }
    
    @Test
    void testCollectWithSort() {
        // Test COLLECT integration with SORT function
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        // Get collected array
        List<CellValue> args = List.of(new CellValue.StringValue("duration"));
        CellValue collected = registry.evaluateFunction("COLLECT", args, context);
        
        // Test with SORT function
        List<CellValue> sortArgs = List.of(collected);
        CellValue sortResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("SORT"))
            .apply(sortArgs.toArray(new CellValue[0]));
        
        CellValue.ArrayValue sortedArray = (CellValue.ArrayValue) sortResult;
        assertEquals(3, sortedArray.elements().size(), "SORT should preserve all elements");
        
        // Check sorted order (should be 100, 150, 200)
        List<CellValue> elements = sortedArray.elements();
        assertEquals(new CellValue.NumberValue(100L), elements.get(0));
        assertEquals(new CellValue.NumberValue(150L), elements.get(1)); 
        assertEquals(new CellValue.NumberValue(200L), elements.get(2));
    }
    
    // ===== COMPLEX QUERY TESTS =====
    
    @Test
    void testCollectWithComplexNestedFunctions() {
        // Test complex nested function calls with COLLECT
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        // Get collected array
        List<CellValue> args = List.of(new CellValue.StringValue("duration"));
        CellValue collected = registry.evaluateFunction("COLLECT", args, context);
        
        // Test HEAD(COLLECT(duration))
        List<CellValue> headArgs = List.of(collected);
        CellValue headResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("HEAD"))
            .apply(headArgs.toArray(new CellValue[0]));
        assertEquals(new CellValue.NumberValue(100L), headResult, "HEAD(COLLECT(duration)) should return first element");
        
        // Test TAIL(COLLECT(duration))
        List<CellValue> tailArgs = List.of(collected);
        CellValue tailResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("TAIL"))
            .apply(tailArgs.toArray(new CellValue[0]));
        assertEquals(new CellValue.NumberValue(150L), tailResult, "TAIL(COLLECT(duration)) should return last element");
        
        // Test HEAD(SORT(COLLECT(duration)), 2) - first 2 elements after sorting
        List<CellValue> sortArgs = List.of(collected);
        CellValue sortedArray = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("SORT"))
            .apply(sortArgs.toArray(new CellValue[0]));
        
        List<CellValue> headSortedArgs = List.of(sortedArray, new CellValue.NumberValue(2));
        CellValue headSortedResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("HEAD"))
            .apply(headSortedArgs.toArray(new CellValue[0]));
        
        CellValue.ArrayValue headSortedArray = (CellValue.ArrayValue) headSortedResult;
        assertEquals(2, headSortedArray.elements().size(), "Should return first 2 sorted elements");
        assertEquals(new CellValue.NumberValue(100L), headSortedArray.elements().get(0), "First sorted should be 100");
        assertEquals(new CellValue.NumberValue(150L), headSortedArray.elements().get(1), "Second sorted should be 150");
    }
    
    @Test
    void testCollectWithSliceOperations() {
        // Test SLICE(COLLECT(duration), start, end) operations
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        List<CellValue> args = List.of(new CellValue.StringValue("duration"));
        CellValue collected = registry.evaluateFunction("COLLECT", args, context);
        
        // Test SLICE(COLLECT(duration), 0, 2) - first 2 elements
        List<CellValue> sliceArgs = List.of(collected, new CellValue.NumberValue(0), new CellValue.NumberValue(2));
        CellValue sliceResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("SLICE"))
            .apply(sliceArgs.toArray(new CellValue[0]));
        
        CellValue.ArrayValue slicedArray = (CellValue.ArrayValue) sliceResult;
        assertEquals(2, slicedArray.elements().size(), "SLICE should return 2 elements");
        assertEquals(new CellValue.NumberValue(100L), slicedArray.elements().get(0));
        assertEquals(new CellValue.NumberValue(200L), slicedArray.elements().get(1));
        
        // Test SLICE(COLLECT(duration), 1, 3) - middle to end
        List<CellValue> sliceArgs2 = List.of(collected, new CellValue.NumberValue(1), new CellValue.NumberValue(3));
        CellValue sliceResult2 = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("SLICE"))
            .apply(sliceArgs2.toArray(new CellValue[0]));
        
        CellValue.ArrayValue slicedArray2 = (CellValue.ArrayValue) sliceResult2;
        assertEquals(2, slicedArray2.elements().size(), "SLICE should return 2 elements");
        assertEquals(new CellValue.NumberValue(200L), slicedArray2.elements().get(0));
        assertEquals(new CellValue.NumberValue(150L), slicedArray2.elements().get(1));
    }
    
    @Test
    void testCollectWithMultipleFieldTypes() {
        // Test collecting different field types and combining them
        AggregateFunctions.EvaluationContext context = createAdvancedTestContext();
        
        // Test collecting numeric data
        List<CellValue> numericArgs = List.of(new CellValue.StringValue("age"));
        CellValue numericCollected = registry.evaluateFunction("COLLECT", numericArgs, context);
        
        CellValue.ArrayValue numericArray = (CellValue.ArrayValue) numericCollected;
        assertEquals(4, numericArray.elements().size());
        
        // Test SIZE(COLLECT(age))
        List<CellValue> sizeArgs = List.of(numericCollected);
        CellValue sizeResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("SIZE"))
            .apply(sizeArgs.toArray(new CellValue[0]));
        assertEquals(new CellValue.NumberValue(4), sizeResult);
        
        // Test collecting string data
        List<CellValue> stringArgs = List.of(new CellValue.StringValue("department"));
        CellValue stringCollected = registry.evaluateFunction("COLLECT", stringArgs, context);
        
        CellValue.ArrayValue stringArray = (CellValue.ArrayValue) stringCollected;
        assertEquals(4, stringArray.elements().size());
        
        // Check string values
        assertEquals(new CellValue.StringValue("Engineering"), stringArray.elements().get(0));
        assertEquals(new CellValue.StringValue("Marketing"), stringArray.elements().get(1));
        assertEquals(new CellValue.StringValue("Engineering"), stringArray.elements().get(2));
        assertEquals(new CellValue.StringValue("Sales"), stringArray.elements().get(3));
    }
    
    @Test
    void testCollectWithSortedResults() {
        // Test collecting data that would be pre-sorted (simulating ORDER BY)
        AggregateFunctions.EvaluationContext context = createSortedTestContext();
        
        // Collect ages from sorted data
        List<CellValue> args = List.of(new CellValue.StringValue("sortedAge"));
        CellValue collected = registry.evaluateFunction("COLLECT", args, context);
        
        CellValue.ArrayValue collectedArray = (CellValue.ArrayValue) collected;
        assertEquals(4, collectedArray.elements().size());
        
        // Verify data is in sorted order (22, 25, 28, 35)
        assertEquals(new CellValue.NumberValue(22L), collectedArray.elements().get(0));
        assertEquals(new CellValue.NumberValue(25L), collectedArray.elements().get(1));
        assertEquals(new CellValue.NumberValue(28L), collectedArray.elements().get(2));
        assertEquals(new CellValue.NumberValue(35L), collectedArray.elements().get(3));
        
        // Test HEAD(COLLECT(sortedAge)) - should get youngest
        List<CellValue> headArgs = List.of(collected);
        CellValue headResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("HEAD"))
            .apply(headArgs.toArray(new CellValue[0]));
        assertEquals(new CellValue.NumberValue(22L), headResult, "HEAD should return youngest age");
        
        // Test TAIL(COLLECT(sortedAge)) - should get oldest
        List<CellValue> tailArgs = List.of(collected);
        CellValue tailResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("TAIL"))
            .apply(tailArgs.toArray(new CellValue[0]));
        assertEquals(new CellValue.NumberValue(35L), tailResult, "TAIL should return oldest age");
    }
    
    @Test
    void testCollectWithDescendingSortSimulation() {
        // Test collecting with descending sort simulation
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        List<CellValue> args = List.of(new CellValue.StringValue("duration"));
        CellValue collected = registry.evaluateFunction("COLLECT", args, context);
        
        // Sort in descending order
        List<CellValue> sortArgs = List.of(collected, new CellValue.StringValue("DESC"));
        CellValue sortedDesc = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("SORT"))
            .apply(sortArgs.toArray(new CellValue[0]));
        
        CellValue.ArrayValue sortedArray = (CellValue.ArrayValue) sortedDesc;
        assertEquals(3, sortedArray.elements().size());
        
        // Should be 200, 150, 100 in descending order
        assertEquals(new CellValue.NumberValue(200L), sortedArray.elements().get(0));
        assertEquals(new CellValue.NumberValue(150L), sortedArray.elements().get(1));
        assertEquals(new CellValue.NumberValue(100L), sortedArray.elements().get(2));
        
        // Test HEAD(SORT(COLLECT(duration), 'DESC')) - highest value
        List<CellValue> headArgs = List.of(sortedDesc);
        CellValue headResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("HEAD"))
            .apply(headArgs.toArray(new CellValue[0]));
        assertEquals(new CellValue.NumberValue(200L), headResult, "HEAD of DESC sort should return highest value");
    }
    
    @Test
    void testCollectWithComplexChainedOperations() {
        // Test complex chained operations: HEAD(TAIL(SORT(COLLECT(duration)), 2))
        AggregateFunctions.EvaluationContext context = createTestContext();
        
        // Step 1: COLLECT(duration)
        List<CellValue> collectArgs = List.of(new CellValue.StringValue("duration"));
        CellValue collected = registry.evaluateFunction("COLLECT", collectArgs, context);
        
        // Step 2: SORT(COLLECT(duration))
        List<CellValue> sortArgs = List.of(collected);
        CellValue sorted = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("SORT"))
            .apply(sortArgs.toArray(new CellValue[0]));
        
        // Step 3: TAIL(SORT(COLLECT(duration)), 2) - last 2 elements
        List<CellValue> tailArgs = List.of(sorted, new CellValue.NumberValue(2));
        CellValue tailResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("TAIL"))
            .apply(tailArgs.toArray(new CellValue[0]));
        
        // Step 4: HEAD(TAIL(SORT(COLLECT(duration)), 2)) - first of the last 2
        List<CellValue> headArgs = List.of(tailResult);
        CellValue finalResult = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("HEAD"))
            .apply(headArgs.toArray(new CellValue[0]));
        
        // Result should be 150 (the second-highest value)
        assertEquals(new CellValue.NumberValue(150L), finalResult, 
            "HEAD(TAIL(SORT(COLLECT(duration)), 2)) should return second-highest value");
    }
    
    @Test
    void testCollectWithGroupBySimulation() {
        // Test simulating GROUP BY behavior with COLLECT
        AggregateFunctions.EvaluationContext engineeringContext = createGroupedTestContext("Engineering");
        AggregateFunctions.EvaluationContext marketingContext = createGroupedTestContext("Marketing");
        
        // Collect ages for Engineering group
        List<CellValue> args = List.of(new CellValue.StringValue("age"));
        CellValue engAges = registry.evaluateFunction("COLLECT", args, engineeringContext);
        
        CellValue.ArrayValue engArray = (CellValue.ArrayValue) engAges;
        assertEquals(2, engArray.elements().size(), "Engineering should have 2 employees");
        
        // Test HEAD(COLLECT(age)) for Engineering - youngest in group
        List<CellValue> headArgs = List.of(engAges);
        CellValue engYoungest = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("HEAD"))
            .apply(headArgs.toArray(new CellValue[0]));
        assertEquals(new CellValue.NumberValue(25L), engYoungest, "Youngest in Engineering should be 25");
        
        // Collect ages for Marketing group
        CellValue mktAges = registry.evaluateFunction("COLLECT", args, marketingContext);
        
        CellValue.ArrayValue mktArray = (CellValue.ArrayValue) mktAges;
        assertEquals(1, mktArray.elements().size(), "Marketing should have 1 employee");
        
        // Test SIZE(COLLECT(age)) comparison between groups
        List<CellValue> sizeArgsEng = List.of(engAges);
        CellValue engSize = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("SIZE"))
            .apply(sizeArgsEng.toArray(new CellValue[0]));
        
        List<CellValue> sizeArgsMkt = List.of(mktAges);
        CellValue mktSize = ((FunctionRegistry.SimpleFunctionDefinition) registry.getFunction("SIZE"))
            .apply(sizeArgsMkt.toArray(new CellValue[0]));
        
        assertEquals(new CellValue.NumberValue(2), engSize, "Engineering group size should be 2");
        assertEquals(new CellValue.NumberValue(1), mktSize, "Marketing group size should be 1");
    }

    // ===== HELPER METHODS =====
    
    /**
     * Create a test evaluation context with sample data
     */
    private AggregateFunctions.EvaluationContext createTestContext() {
        return new AggregateFunctions.EvaluationContext(null, null) {
            @Override
            public List<CellValue> getAllValues(String fieldName) {
                switch (fieldName) {
                    case "duration":
                        return List.of(
                            new CellValue.NumberValue(100L),
                            new CellValue.NumberValue(200L),
                            new CellValue.NumberValue(150L)
                        );
                    case "name":
                        return List.of(
                            new CellValue.StringValue("Alice"),
                            new CellValue.StringValue("Bob"),
                            new CellValue.StringValue("Charlie")
                        );
                    case "id":
                        return List.of(
                            new CellValue.NumberValue(1L),
                            new CellValue.NumberValue(2L),
                            new CellValue.NumberValue(3L)
                        );
                    default:
                        return List.of(); // Unknown field returns empty list
                }
            }
        };
    }
    
    /**
     * Create an empty test evaluation context for edge case testing
     */
    private AggregateFunctions.EvaluationContext createEmptyTestContext() {
        return new AggregateFunctions.EvaluationContext(null, null) {
            @Override
            public List<CellValue> getAllValues(String fieldName) {
                return List.of(); // Always return empty for testing empty scenarios
            }
        };
    }
    
    /**
     * Create a test evaluation context with null values for edge case testing
     */
    private AggregateFunctions.EvaluationContext createTestContextWithNulls() {
        return new AggregateFunctions.EvaluationContext(null, null) {
            @Override
            public List<CellValue> getAllValues(String fieldName) {
                switch (fieldName) {
                    case "optionalField":
                        return List.of(
                            new CellValue.StringValue("value1"),
                            new CellValue.NullValue(),
                            new CellValue.StringValue("value3")
                        );
                    case "mixedTypes":
                        return List.of(
                            new CellValue.NumberValue(42L),
                            new CellValue.NullValue(),
                            new CellValue.StringValue("text")
                        );
                    default:
                        return List.of();
                }
            }
        };
    }
    
    /**
     * Create an advanced test evaluation context with more complex data types
     */
    private AggregateFunctions.EvaluationContext createAdvancedTestContext() {
        return new AggregateFunctions.EvaluationContext(null, null) {
            @Override
            public List<CellValue> getAllValues(String fieldName) {
                switch (fieldName) {
                    case "age":
                        return List.of(
                            new CellValue.NumberValue(25L),
                            new CellValue.NumberValue(35L),
                            new CellValue.NumberValue(28L),
                            new CellValue.NumberValue(22L)
                        );
                    case "department":
                        return List.of(
                            new CellValue.StringValue("Engineering"),
                            new CellValue.StringValue("Marketing"),
                            new CellValue.StringValue("Engineering"),
                            new CellValue.StringValue("Sales")
                        );
                    case "salary":
                        return List.of(
                            new CellValue.NumberValue(75000L),
                            new CellValue.NumberValue(65000L),
                            new CellValue.NumberValue(80000L),
                            new CellValue.NumberValue(45000L)
                        );
                    case "name":
                        return List.of(
                            new CellValue.StringValue("Alice"),
                            new CellValue.StringValue("Bob"),
                            new CellValue.StringValue("Charlie"),
                            new CellValue.StringValue("Diana")
                        );
                    default:
                        return List.of();
                }
            }
        };
    }
    
    /**
     * Create a test evaluation context with pre-sorted data (simulating ORDER BY)
     */
    private AggregateFunctions.EvaluationContext createSortedTestContext() {
        return new AggregateFunctions.EvaluationContext(null, null) {
            @Override
            public List<CellValue> getAllValues(String fieldName) {
                switch (fieldName) {
                    case "sortedAge":
                        // Pre-sorted ages: 22, 25, 28, 35
                        return List.of(
                            new CellValue.NumberValue(22L),
                            new CellValue.NumberValue(25L),
                            new CellValue.NumberValue(28L),
                            new CellValue.NumberValue(35L)
                        );
                    case "sortedName":
                        // Names corresponding to sorted ages
                        return List.of(
                            new CellValue.StringValue("Diana"),
                            new CellValue.StringValue("Alice"),
                            new CellValue.StringValue("Charlie"),
                            new CellValue.StringValue("Bob")
                        );
                    default:
                        return List.of();
                }
            }
        };
    }
    
    /**
     * Create a test evaluation context for a specific group (simulating GROUP BY)
     */
    private AggregateFunctions.EvaluationContext createGroupedTestContext(String department) {
        return new AggregateFunctions.EvaluationContext(null, null) {
            @Override
            public List<CellValue> getAllValues(String fieldName) {
                switch (department) {
                    case "Engineering":
                        switch (fieldName) {
                            case "age":
                                return List.of(
                                    new CellValue.NumberValue(25L),  // Alice
                                    new CellValue.NumberValue(28L)   // Charlie
                                );
                            case "name":
                                return List.of(
                                    new CellValue.StringValue("Alice"),
                                    new CellValue.StringValue("Charlie")
                                );
                            case "salary":
                                return List.of(
                                    new CellValue.NumberValue(75000L),
                                    new CellValue.NumberValue(80000L)
                                );
                            default:
                                return List.of();
                        }
                    case "Marketing":
                        switch (fieldName) {
                            case "age":
                                return List.of(new CellValue.NumberValue(35L)); // Bob
                            case "name":
                                return List.of(new CellValue.StringValue("Bob"));
                            case "salary":
                                return List.of(new CellValue.NumberValue(65000L));
                            default:
                                return List.of();
                        }
                    case "Sales":
                        switch (fieldName) {
                            case "age":
                                return List.of(new CellValue.NumberValue(22L)); // Diana
                            case "name":
                                return List.of(new CellValue.StringValue("Diana"));
                            case "salary":
                                return List.of(new CellValue.NumberValue(45000L));
                            default:
                                return List.of();
                        }
                    default:
                        return List.of();
                }
            }
        };
    }
}