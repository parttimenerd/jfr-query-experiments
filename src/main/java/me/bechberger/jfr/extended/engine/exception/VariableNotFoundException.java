package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.engine.util.StringSimilarity;

/**
 * Exception thrown when a variable is not found or is undefined during query execution.
 * 
 * This exception provides specific information about the missing variable name,
 * available variables, and suggestions for resolving the issue.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class VariableNotFoundException extends QueryExecutionException {
    
    private final String variableName;
    private final String[] availableVariables;
    private final String scopeContext;
    
    /**
     * Creates a new VariableNotFoundException with detailed information.
     * 
     * @param variableName The name of the variable that was not found
     * @param availableVariables Array of available variable names in current scope
     * @param scopeContext Description of the current scope context
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public VariableNotFoundException(String variableName, String[] availableVariables, 
                                   String scopeContext, ASTNode errorNode) {
        super(
            String.format("Variable '%s' not found", variableName),
            errorNode,
            buildContext(variableName, scopeContext),
            buildUserHint(variableName, availableVariables),
            null
        );
        this.variableName = variableName;
        this.availableVariables = availableVariables != null ? availableVariables.clone() : new String[0];
        this.scopeContext = scopeContext;
    }
    
    /**
     * Creates a new VariableNotFoundException without scope context.
     */
    public VariableNotFoundException(String variableName, String[] availableVariables, ASTNode errorNode) {
        this(variableName, availableVariables, "current scope", errorNode);
    }
    
    /**
     * Builds the context string for this exception.
     */
    private static String buildContext(String variableName, String scopeContext) {
        return String.format("Attempting to access variable '%s' in %s", variableName, scopeContext);
    }
    
    /**
     * Builds a helpful user hint for resolving the variable not found error.
     */
    private static String buildUserHint(String variableName, String[] availableVariables) {
        if (availableVariables == null || availableVariables.length == 0) {
            return "No variables are defined in the current scope. Define variables using assignment syntax: var := expression";
        }
        
        StringBuilder hint = new StringBuilder();
        
        // Look for similar variable names
        String suggestion = findSimilarVariableName(variableName, availableVariables);
        if (suggestion != null) {
            hint.append("Did you mean '").append(suggestion).append("'? ");
        }
        
        hint.append("Available variables: ");
        for (int i = 0; i < Math.min(availableVariables.length, 10); i++) {
            if (i > 0) hint.append(", ");
            hint.append("'").append(availableVariables[i]).append("'");
        }
        
        if (availableVariables.length > 10) {
            hint.append(" ... and ").append(availableVariables.length - 10).append(" more");
        }
        
        return hint.toString();
    }
    
    /**
     * Finds a similar variable name using simple heuristics.
     */
    private static String findSimilarVariableName(String variableName, String[] availableVariables) {
        String lowerVariableName = variableName.toLowerCase();
        
        // First pass: exact case-insensitive match
        for (String available : availableVariables) {
            if (available.toLowerCase().equals(lowerVariableName)) {
                return available;
            }
        }
        
        // Second pass: contains or starts with
        for (String available : availableVariables) {
            String lowerAvailable = available.toLowerCase();
            if (lowerAvailable.contains(lowerVariableName) || lowerVariableName.contains(lowerAvailable)) {
                return available;
            }
        }
        
        // Third pass: simple edit distance for typos
        return StringSimilarity.findClosestMatch(variableName, availableVariables, 2, true);
    }

    // Getters for accessing specific error information
    
    public String getVariableName() {
        return variableName;
    }
    
    public String[] getAvailableVariables() {
        return availableVariables != null ? availableVariables.clone() : new String[0];
    }
    
    public String getScopeContext() {
        return scopeContext;
    }
    
    /**
     * Returns true if this exception has information about available variables.
     */
    public boolean hasAvailableVariables() {
        return availableVariables != null && availableVariables.length > 0;
    }
}
