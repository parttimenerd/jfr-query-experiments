package me.bechberger.jfr.extended.table.exception;

/**
 * Exception thrown when attempting to access a column using an invalid index.
 * 
 * This exception provides specific information about the invalid column access,
 * including the requested index and the valid range.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class ColumnIndexOutOfBoundsException extends DataAccessException {
    
    private final int requestedIndex;
    private final int columnCount;
    
    /**
     * Creates a new ColumnIndexOutOfBoundsException.
     * 
     * @param requestedIndex The column index that was requested
     * @param columnCount The actual number of columns in the table
     * @param tableName Optional name of the table (can be null)
     */
    public ColumnIndexOutOfBoundsException(int requestedIndex, int columnCount, String tableName) {
        super(
            buildMessage(requestedIndex, columnCount, tableName),
            String.format("column index %d", requestedIndex),
            String.format("valid column index (0-%d)", columnCount - 1),
            "invalid index",
            requestedIndex,
            null
        );
        this.requestedIndex = requestedIndex;
        this.columnCount = columnCount;
    }
    
    /**
     * Creates a new ColumnIndexOutOfBoundsException without table name.
     * 
     * @param requestedIndex The column index that was requested
     * @param columnCount The actual number of columns in the table
     */
    public ColumnIndexOutOfBoundsException(int requestedIndex, int columnCount) {
        this(requestedIndex, columnCount, null);
    }
    
    private static String buildMessage(int requestedIndex, int columnCount, String tableName) {
        String tableInfo = tableName != null ? " in table '" + tableName + "'" : "";
        if (columnCount == 0) {
            return String.format("Cannot access column at index %d%s: table has no columns", 
                requestedIndex, tableInfo);
        } else if (requestedIndex < 0) {
            return String.format("Cannot access column at index %d%s: negative column index (valid range: 0-%d)", 
                requestedIndex, tableInfo, columnCount - 1);
        } else {
            return String.format("Cannot access column at index %d%s: index out of bounds (valid range: 0-%d)", 
                requestedIndex, tableInfo, columnCount - 1);
        }
    }
    
    /**
     * Gets the column index that was requested.
     * 
     * @return The requested column index
     */
    public int getRequestedIndex() {
        return requestedIndex;
    }
    
    /**
     * Gets the actual number of columns in the table.
     * 
     * @return The column count
     */
    public int getColumnCount() {
        return columnCount;
    }
    
    /**
     * Checks if the requested index was negative.
     * 
     * @return true if the index was negative, false otherwise
     */
    public boolean isNegativeIndex() {
        return requestedIndex < 0;
    }
    
    /**
     * Checks if the table was empty (no columns).
     * 
     * @return true if the table has no columns, false otherwise
     */
    public boolean isEmptyTable() {
        return columnCount == 0;
    }
}
