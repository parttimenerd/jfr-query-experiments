package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.util.StringSimilarity;
import me.bechberger.jfr.extended.evaluator.FunctionRegistry;

import java.util.Set;

/**
 * Utility class for validating function arguments and providing function-related suggestions.
 * 
 * <p>This class centralizes all function validation logic and integrates with the FunctionRegistry
 * to provide dynamic validation without hardcoded function names or argument counts.</p>
 */
public class FunctionValidator {
    
    private final FunctionRegistry registry;
    
    public FunctionValidator() {
        this.registry = FunctionRegistry.getInstance();
    }
    
    /**
     * Validate function arguments using FunctionRegistry only.
     * No hardcoded function names or argument counts; all validation is based on the registry.
     */
    public void validateFunctionArguments(String functionName, int argCount) throws ParserException {
        var def = registry.getFunction(functionName);
        if (def == null) {
            throw new ParserException("Unknown function: " + functionName);
        }
        
        int minArgs = (int) def.parameters().stream().filter(p -> !p.optional()).count();
        int maxArgs = def.parameters().size();
        
        if (argCount < minArgs) {
            throw new ParserException(functionName + " requires at least " + minArgs + " argument(s), but got " + argCount);
        }
        
        // Check if function accepts variadic arguments
        boolean isVariadic = !def.parameters().isEmpty() && 
                           def.parameters().get(def.parameters().size() - 1).variadic();
        
        if (!isVariadic && argCount > maxArgs) {
            throw new ParserException(functionName + " accepts at most " + maxArgs + " argument(s), but got " + argCount);
        }
    }
    
    /**
     * Suggest function name corrections using FunctionRegistry only.
     * No hardcoded function names; suggestions are based on available functions in the registry.
     */
    public String suggestFunctionName(String funcName) {
        Set<String> functionNames = registry.getFunctionNames();
        
        // Simple suggestion logic - find closest match by name similarity
        return StringSimilarity.findClosestMatch(funcName, functionNames, 3, true);
    }
    
    /**
     * Check if a function is an aggregate function using FunctionRegistry
     */
    public boolean isAggregateFunction(String functionName) {
        var def = registry.getFunction(functionName);
        if (def == null) {
            return false;
        }
        return def.type() == FunctionRegistry.FunctionType.AGGREGATE;
    }
    
    /**
     * Check if a function exists in the registry
     */
    public boolean functionExists(String functionName) {
        return registry.getFunction(functionName) != null;
    }
    
    /**
     * Get all available function names
     */
    public Set<String> getAvailableFunctions() {
        return registry.getFunctionNames();
    }
    
    /**
     * Get all aggregate function names
     */
    public Set<String> getAggregateFunctions() {
        return registry.getFunctionNames().stream()
            .filter(this::isAggregateFunction)
            .collect(java.util.stream.Collectors.toSet());
    }
    
    /**
     * Create a suggestion message for unknown functions
     */
    public String createFunctionSuggestion(String funcName) {
        // Common function name corrections
        String correctedName = suggestFunctionName(funcName);
        if (correctedName != null) {
            return "Did you mean '" + correctedName + "'?";
        } else {
            return "Function '" + funcName + "' does not exist.";
        }
    }
}
