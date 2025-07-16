package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.engine.util.StringSimilarity;

/**
 * Exception thrown when a table or data source is not found during query execution.
 * 
 * This exception provides specific information about the missing table name,
 * available tables/sources, and suggestions for resolving the issue.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class TableNotFoundException extends QueryExecutionException {
    
    private final String tableName;
    private final String[] availableTables;
    private final String sourceType; // "table", "view", "event type", etc.
    
    /**
     * Creates a new TableNotFoundException with detailed information.
     * 
     * @param tableName The name of the table that was not found
     * @param availableTables Array of available table/source names
     * @param sourceType The type of source being accessed (e.g., "table", "view", "event type")
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public TableNotFoundException(String tableName, String[] availableTables, 
                                 String sourceType, ASTNode errorNode) {
        super(
            String.format("%s '%s' not found", 
                sourceType != null ? capitalizeFirst(sourceType) : "Table", tableName),
            errorNode,
            buildContext(tableName, sourceType),
            buildUserHint(tableName, availableTables, sourceType),
            null
        );
        this.tableName = tableName;
        this.availableTables = availableTables != null ? availableTables.clone() : new String[0];
        this.sourceType = sourceType != null ? sourceType : "table";
    }
    
    /**
     * Creates a new TableNotFoundException without source type specification.
     */
    public TableNotFoundException(String tableName, String[] availableTables, ASTNode errorNode) {
        this(tableName, availableTables, "table", errorNode);
    }
    
    /**
     * Creates a new TableNotFoundException for event types specifically.
     */
    public static TableNotFoundException forEventType(String eventTypeName, String[] availableEventTypes, ASTNode errorNode) {
        return new TableNotFoundException(eventTypeName, availableEventTypes, "event type", errorNode);
    }
    
    /**
     * Creates a new TableNotFoundException for views specifically.
     */
    public static TableNotFoundException forView(String viewName, String[] availableViews, ASTNode errorNode) {
        return new TableNotFoundException(viewName, availableViews, "view", errorNode);
    }
    
    /**
     * Builds the context string for this exception.
     */
    private static String buildContext(String tableName, String sourceType) {
        return String.format("Attempting to access %s '%s' in FROM clause", 
            sourceType != null ? sourceType : "table", tableName);
    }
    
    /**
     * Builds a helpful user hint for resolving the table not found error.
     */
    private static String buildUserHint(String tableName, String[] availableTables, String sourceType) {
        if (availableTables == null || availableTables.length == 0) {
            return String.format("No %ss are available. Check if the data source is properly loaded.", 
                sourceType != null ? sourceType : "table");
        }
        
        StringBuilder hint = new StringBuilder();
        
        // Look for similar table names
        String suggestion = findSimilarTableName(tableName, availableTables);
        if (suggestion != null) {
            hint.append("Did you mean '").append(suggestion).append("'? ");
        }
        
        hint.append("Available ").append(sourceType != null ? sourceType : "table").append("s: ");
        for (int i = 0; i < Math.min(availableTables.length, 10); i++) {
            if (i > 0) hint.append(", ");
            hint.append("'").append(availableTables[i]).append("'");
        }
        
        if (availableTables.length > 10) {
            hint.append(" ... and ").append(availableTables.length - 10).append(" more");
        }
        
        return hint.toString();
    }
    
    /**
     * Finds a similar table name using simple heuristics.
     */
    private static String findSimilarTableName(String tableName, String[] availableTables) {
        String lowerTableName = tableName.toLowerCase();
        
        // First pass: exact case-insensitive match
        for (String available : availableTables) {
            if (available.toLowerCase().equals(lowerTableName)) {
                return available;
            }
        }
        
        // Second pass: contains or starts with
        for (String available : availableTables) {
            String lowerAvailable = available.toLowerCase();
            if (lowerAvailable.contains(lowerTableName) || lowerTableName.contains(lowerAvailable)) {
                return available;
            }
        }
        
        // Third pass: simple edit distance for typos
        return StringSimilarity.findClosestMatch(tableName, availableTables, 3, true);
    }

    /**
     * Capitalizes the first letter of a string.
     */
    private static String capitalizeFirst(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
    
    // Getters for accessing specific error information
    
    public String getTableName() {
        return tableName;
    }
    
    public String[] getAvailableTables() {
        return availableTables != null ? availableTables.clone() : new String[0];
    }
    
    public String getSourceType() {
        return sourceType;
    }
    
    /**
     * Returns true if this exception has information about available tables.
     */
    public boolean hasAvailableTables() {
        return availableTables != null && availableTables.length > 0;
    }
}
