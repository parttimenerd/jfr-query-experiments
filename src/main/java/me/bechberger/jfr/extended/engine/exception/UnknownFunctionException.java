package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.engine.util.StringSimilarity;
import java.util.Set;

/**
 * Exception thrown when an unknown function is called during query execution.
 * 
 * This exception provides specific information about the unknown function name,
 * available functions, and suggestions for similar function names.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class UnknownFunctionException extends QueryExecutionException {
    
    private final String functionName;
    private final Set<String> availableFunctions;
    private final boolean isAggregateContext;
    
    /**
     * Creates a new UnknownFunctionException with detailed information.
     * 
     * @param functionName The name of the unknown function
     * @param availableFunctions Set of available function names
     * @param isAggregateContext Whether this function was called in an aggregate context
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public UnknownFunctionException(String functionName, Set<String> availableFunctions, 
                                   boolean isAggregateContext, ASTNode errorNode) {
        super(
            String.format("Unknown function '%s'", functionName),
            errorNode,
            buildContext(functionName, isAggregateContext),
            buildUserHint(functionName, availableFunctions),
            null
        );
        this.functionName = functionName;
        this.availableFunctions = availableFunctions != null ? Set.copyOf(availableFunctions) : Set.of();
        this.isAggregateContext = isAggregateContext;
    }
    
    /**
     * Creates a new UnknownFunctionException without aggregate context information.
     */
    public UnknownFunctionException(String functionName, Set<String> availableFunctions, ASTNode errorNode) {
        this(functionName, availableFunctions, false, errorNode);
    }
    
    /**
     * Builds the context string for this exception.
     */
    private static String buildContext(String functionName, boolean isAggregateContext) {
        if (isAggregateContext) {
            return String.format("Attempting to call function '%s' in aggregate context", functionName);
        } else {
            return String.format("Attempting to call function '%s'", functionName);
        }
    }
    
    /**
     * Builds a helpful user hint for resolving the unknown function error.
     */
    private static String buildUserHint(String functionName, Set<String> availableFunctions) {
        if (availableFunctions == null || availableFunctions.isEmpty()) {
            return "No functions are currently available. Check if the function registry is properly initialized.";
        }
        
        StringBuilder hint = new StringBuilder();
        
        // Look for similar function names
        String suggestion = findSimilarFunctionName(functionName, availableFunctions);
        if (suggestion != null) {
            hint.append("Did you mean '").append(suggestion).append("'? ");
        }
        
        hint.append("Available functions: ");
        int count = 0;
        for (String func : availableFunctions) {
            if (count > 0) hint.append(", ");
            if (count >= 10) {
                hint.append("... and ").append(availableFunctions.size() - 10).append(" more");
                break;
            }
            hint.append(func);
            count++;
        }
        
        return hint.toString();
    }
    
    /**
     * Finds a similar function name using simple heuristics.
     */
    private static String findSimilarFunctionName(String functionName, Set<String> availableFunctions) {
        String lowerFunctionName = functionName.toLowerCase();
        
        // First pass: exact case-insensitive match
        for (String available : availableFunctions) {
            if (available.toLowerCase().equals(lowerFunctionName)) {
                return available;
            }
        }
        
        // Second pass: starts with or contains
        for (String available : availableFunctions) {
            String lowerAvailable = available.toLowerCase();
            if (lowerAvailable.startsWith(lowerFunctionName) || lowerFunctionName.startsWith(lowerAvailable)) {
                return available;
            }
        }
        
        // Third pass: simple edit distance for typos
        return StringSimilarity.findClosestMatch(functionName, availableFunctions, 2, true);
    }

    // Getters for accessing specific error information
    
    public String getFunctionName() {
        return functionName;
    }
    
    public Set<String> getAvailableFunctions() {
        return availableFunctions;
    }
    
    public boolean isAggregateContext() {
        return isAggregateContext;
    }
    
    /**
     * Returns true if this exception has information about available functions.
     */
    public boolean hasAvailableFunctions() {
        return availableFunctions != null && !availableFunctions.isEmpty();
    }
}
