package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.evaluator.FunctionRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test the FunctionRegistry functionality using parameterized tests for registration and lookup.
 */
public class FunctionRegistryTest {

    @ParameterizedTest
    @ValueSource(strings = {"AVG", "COUNT", "MAX", "MIN", "SUM", "P99SELECT", "PERCENTILE_SELECT", "HEAD", "TAIL", "SLICE"})
    public void testBuiltInFunctionRegistration(String functionName) {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        assertTrue(registry.isFunction(functionName), functionName + " should be registered");
    }
    
    @ParameterizedTest
    @CsvSource({
        "AVG, avg",
        "COUNT, count", 
        "MAX, max",
        "MIN, min",
        "SUM, sum",
        "P99SELECT, p99select",
        "PERCENTILE_SELECT, percentile_select"
    })
    public void testCaseInsensitiveFunctionLookup(String upperCase, String lowerCase) {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        assertTrue(registry.isFunction(upperCase), upperCase + " should be registered");
        assertTrue(registry.isFunction(lowerCase), lowerCase + " should be registered");
    }
    
    @Test
    public void testFunctionRegistration() {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Test that built-in functions are registered
        assertTrue(registry.isFunction("AVG"), "AVG should be registered");
        assertTrue(registry.isFunction("COUNT"), "COUNT should be registered");
        assertTrue(registry.isFunction("MAX"), "MAX should be registered");
        assertTrue(registry.isFunction("MIN"), "MIN should be registered");
        assertTrue(registry.isFunction("SUM"), "SUM should be registered");
        
        // Test case insensitive lookup
        assertTrue(registry.isFunction("avg"), "avg should be registered");
        assertTrue(registry.isFunction("Count"), "Count should be registered");
        
        // Test non-existent function
        assertFalse(registry.isFunction("NONEXISTENT"), "NONEXISTENT should not be registered");
    }
    
    @Test
    public void testFunctionDefinitionLookup() {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Test getting function definition
        FunctionRegistry.FunctionDefinition avgDef = registry.getFunction("AVG");
        assertNotNull(avgDef, "AVG definition should exist");
        assertEquals("AVG", avgDef.name());
        assertEquals(null, avgDef.token()); // No token for aggregate functions
        assertEquals(FunctionRegistry.FunctionType.AGGREGATE, avgDef.type());
        
        // Test non-existent function
        FunctionRegistry.FunctionDefinition nonExistentDef = registry.getFunction("NONEXISTENT");
        assertNull(nonExistentDef, "NONEXISTENT definition should not exist");
    }
    
    @Test
    public void testTokenTypeLookup() {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Test token type lookup for percentile functions that still have tokens
        assertTrue(registry.isFunction(TokenType.P90), "P90 token should be a function");
        assertTrue(registry.isFunction(TokenType.P95), "P95 token should be a function");
        
        // Test non-function token
        assertFalse(registry.isFunction(TokenType.SELECT), "SELECT token should not be a function");
    }
    
    @Test
    public void testFunctionTypes() {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Test that aggregate functions are correctly categorized
        var aggregateFunctions = registry.getFunctionsByType(FunctionRegistry.FunctionType.AGGREGATE);
        assertTrue(aggregateFunctions.size() > 0, "Should have aggregate functions");
        
        boolean hasAvg = aggregateFunctions.stream().anyMatch(f -> f.name().equals("AVG"));
        assertTrue(hasAvg, "Should contain AVG function");
        
        boolean hasCount = aggregateFunctions.stream().anyMatch(f -> f.name().equals("COUNT"));
        assertTrue(hasCount, "Should contain COUNT function");
    }
    
    @Test
    public void testCustomFunctionRegistration() {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Since the registry doesn't allow custom registration in its current design,
        // we'll test that the registry correctly rejects unknown functions
        assertFalse(registry.isFunction("CUSTOM_FUNC"), "CUSTOM_FUNC should not be registered");
        
        FunctionRegistry.FunctionDefinition customDef = registry.getFunction("CUSTOM_FUNC");
        assertNull(customDef, "CUSTOM_FUNC definition should not exist");
    }
    
    @Test
    public void testMathematicalFunctionRegistration() {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Test that mathematical functions are registered
        String[] mathFunctions = {"ABS", "CEIL", "FLOOR", "ROUND", "SQRT", "POW", "MOD", 
                                  "LOG", "LOG10", "EXP", "SIN", "COS", "TAN", "CLAMP"};
        
        for (String funcName : mathFunctions) {
            assertTrue(registry.isFunction(funcName), funcName + " should be registered");
            
            // Test case insensitive lookup
            assertTrue(registry.isFunction(funcName.toLowerCase()), funcName.toLowerCase() + " should be registered");
            
            // Test function definition lookup
            FunctionRegistry.FunctionDefinition funcDef = registry.getFunction(funcName);
            assertNotNull(funcDef, funcName + " definition should exist");
            assertEquals(funcName, funcDef.name());
            assertEquals(FunctionRegistry.FunctionType.MATHEMATICAL, funcDef.type());
        }
    }
    
    @Test
    public void testMathematicalFunctionTokenTypes() {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Test that basic percentile function tokens are properly mapped as AGGREGATE
        TokenType[] aggregatePercentileTokens = {TokenType.P90, TokenType.P95, TokenType.P99, TokenType.P999};
        
        for (TokenType token : aggregatePercentileTokens) {
            assertTrue(registry.isFunction(token), token + " should be recognized as a function token");
            
            FunctionRegistry.FunctionDefinition funcDef = registry.getFunction(token);
            assertNotNull(funcDef, token + " function definition should exist");
            assertEquals(FunctionRegistry.FunctionType.AGGREGATE, funcDef.type());
        }
        
        // Test that percentile SELECT function tokens are properly mapped as DATA_ACCESS
        TokenType[] dataAccessPercentileTokens = {TokenType.P90SELECT, TokenType.P95SELECT, TokenType.P99SELECT, 
                                                  TokenType.P999SELECT, TokenType.PERCENTILE_SELECT};
        
        for (TokenType token : dataAccessPercentileTokens) {
            assertTrue(registry.isFunction(token), token + " should be recognized as a function token");
            
            FunctionRegistry.FunctionDefinition funcDef = registry.getFunction(token);
            assertNotNull(funcDef, token + " function definition should exist");
            assertEquals(FunctionRegistry.FunctionType.DATA_ACCESS, funcDef.type());
        }
    }
    
    @Test
    public void testMathematicalFunctionTypes() {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Test that mathematical functions are correctly categorized
        var mathFunctions = registry.getFunctionsByType(FunctionRegistry.FunctionType.MATHEMATICAL);
        assertTrue(mathFunctions.size() >= 14, "Should have mathematical functions");
        
        String[] expectedMathFunctions = {"ABS", "CEIL", "FLOOR", "ROUND", "SQRT", "POW", "MOD", 
                                          "LOG", "LOG10", "EXP", "SIN", "COS", "TAN", "CLAMP"};
        
        for (String funcName : expectedMathFunctions) {
            boolean hasFunction = mathFunctions.stream().anyMatch(f -> f.name().equals(funcName));
            assertTrue(hasFunction, "Should contain " + funcName + " function");
        }
    }
    
    @Test
    public void testAllFunctionTypesAreRegistered() {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Test that all function types have at least one function
        for (FunctionRegistry.FunctionType type : FunctionRegistry.FunctionType.values()) {
            var functions = registry.getFunctionsByType(type);
            if (type != FunctionRegistry.FunctionType.CONDITIONAL) { // CONDITIONAL not implemented yet
                assertTrue(functions.size() > 0, "Should have functions for type " + type);
            }
        }
    }
    
    @ParameterizedTest
    @CsvSource({
        "AVG, AGGREGATE",
        "COUNT, AGGREGATE",
        "MAX, AGGREGATE",
        "MIN, AGGREGATE",
        "SUM, AGGREGATE",
        "ABS, MATHEMATICAL",
        "CEIL, MATHEMATICAL",
        "FLOOR, MATHEMATICAL",
        "ROUND, MATHEMATICAL",
        "SQRT, MATHEMATICAL",
        "POW, MATHEMATICAL",
        "MOD, MATHEMATICAL",
        "LOG, MATHEMATICAL",
        "LOG10, MATHEMATICAL",
        "EXP, MATHEMATICAL",
        "SIN, MATHEMATICAL",
        "COS, MATHEMATICAL",
        "TAN, MATHEMATICAL",
        "CLAMP, MATHEMATICAL"
    })
    void testFunctionTypeAndRegistration(String functionName, String typeName) {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        assertTrue(registry.isFunction(functionName), functionName + " should be registered");
        FunctionRegistry.FunctionDefinition def = registry.getFunction(functionName);
        assertNotNull(def, functionName + " definition should exist");
        assertEquals(functionName, def.name());
        assertEquals(FunctionRegistry.FunctionType.valueOf(typeName), def.type());
    }

    @ParameterizedTest
    @CsvSource({
        "P90, AGGREGATE",
        "P95, AGGREGATE",
        "P99, AGGREGATE",
        "P999, AGGREGATE",
        "P90SELECT, DATA_ACCESS",
        "P95SELECT, DATA_ACCESS",
        "P99SELECT, DATA_ACCESS",
        "P999SELECT, DATA_ACCESS",
        "PERCENTILE_SELECT, DATA_ACCESS"
    })
    void testPercentileFunctionTokens(String tokenName, String typeName) {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        TokenType token = TokenType.valueOf(tokenName);
        assertTrue(registry.isFunction(token), token + " should be recognized as a function token");
        FunctionRegistry.FunctionDefinition def = registry.getFunction(token);
        assertNotNull(def, token + " function definition should exist");
        assertEquals(FunctionRegistry.FunctionType.valueOf(typeName), def.type());
    }
    
    @Test
    void testClampFunction() {
        FunctionRegistry registry = FunctionRegistry.getInstance();
        
        // Test basic CLAMP functionality
        assertTrue(registry.isFunction("CLAMP"), "CLAMP should be registered");
        assertTrue(registry.isFunction("clamp"), "clamp should be registered (case insensitive)");
        
        // Test function definition
        FunctionRegistry.FunctionDefinition def = registry.getFunction("CLAMP");
        assertNotNull(def, "CLAMP definition should exist");
        assertEquals("CLAMP", def.name());
        assertEquals(FunctionRegistry.FunctionType.MATHEMATICAL, def.type());
        
        // Test that parameters are correctly defined
        assertEquals(3, def.parameters().size(), "CLAMP should have 3 parameters");
        assertEquals("min", def.parameters().get(0).name());
        assertEquals("max", def.parameters().get(1).name());
        assertEquals("value", def.parameters().get(2).name());
    }
}
