package me.bechberger.jfr.extended.engine.exception;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.engine.util.StringSimilarity;

/**
 * Exception thrown when a column is not found in a table during query execution.
 * 
 * This exception provides specific information about the missing column name,
 * the available columns, and helpful suggestions for resolving the issue.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class ColumnNotFoundException extends QueryExecutionException {
    
    private final String columnName;
    private final String[] availableColumns;
    private final String tableName;
    
    /**
     * Creates a new ColumnNotFoundException with detailed information.
     * 
     * @param columnName The name of the column that was not found
     * @param availableColumns Array of available column names in the table
     * @param tableName The name of the table (can be null)
     * @param errorNode The AST node where the error occurred (can be null)
     */
    public ColumnNotFoundException(String columnName, String[] availableColumns, String tableName, ASTNode errorNode) {
        super(
            String.format("Column '%s' not found", columnName),
            errorNode,
            buildContext(columnName, tableName),
            buildUserHint(columnName, availableColumns),
            null
        );
        this.columnName = columnName;
        this.availableColumns = availableColumns != null ? availableColumns.clone() : new String[0];
        this.tableName = tableName;
    }
    
    /**
     * Creates a new ColumnNotFoundException without table name.
     */
    public ColumnNotFoundException(String columnName, String[] availableColumns, ASTNode errorNode) {
        this(columnName, availableColumns, null, errorNode);
    }
    
    /**
     * Builds the context string for this exception.
     */
    private static String buildContext(String columnName, String tableName) {
        if (tableName != null) {
            return String.format("Attempting to access column '%s' in table '%s'", columnName, tableName);
        } else {
            return String.format("Attempting to access column '%s'", columnName);
        }
    }
    
    /**
     * Builds a helpful user hint for resolving the column not found error.
     */
    private static String buildUserHint(String columnName, String[] availableColumns) {
        if (availableColumns == null || availableColumns.length == 0) {
            return "Check if the table has any columns or if the FROM clause is correct";
        }
        
        StringBuilder hint = new StringBuilder();
        hint.append("Available columns: ");
        for (int i = 0; i < availableColumns.length; i++) {
            if (i > 0) hint.append(", ");
            hint.append("'").append(availableColumns[i]).append("'");
        }
        
        // Look for similar column names
        String suggestion = findSimilarColumnName(columnName, availableColumns);
        if (suggestion != null) {
            hint.append(". Did you mean '").append(suggestion).append("'?");
        }
        
        return hint.toString();
    }
    
    /**
     * Finds a similar column name using simple heuristics.
     */
    private static String findSimilarColumnName(String columnName, String[] availableColumns) {
        String lowerColumnName = columnName.toLowerCase();
        
        // First pass: exact case-insensitive match
        for (String available : availableColumns) {
            if (available.toLowerCase().equals(lowerColumnName)) {
                return available;
            }
        }
        
        // Second pass: contains or starts with
        for (String available : availableColumns) {
            String lowerAvailable = available.toLowerCase();
            if (lowerAvailable.contains(lowerColumnName) || lowerColumnName.contains(lowerAvailable)) {
                return available;
            }
        }
        
        // Third pass: simple edit distance for typos
        return StringSimilarity.findClosestMatch(columnName, availableColumns, 2, true);
    }

    // Getters for accessing specific error information
    
    public String getColumnName() {
        return columnName;
    }
    
    public String[] getAvailableColumns() {
        return availableColumns != null ? availableColumns.clone() : new String[0];
    }
    
    public String getTableName() {
        return tableName;
    }
    
    /**
     * Returns true if this exception has information about available columns.
     */
    public boolean hasAvailableColumns() {
        return availableColumns != null && availableColumns.length > 0;
    }
}
