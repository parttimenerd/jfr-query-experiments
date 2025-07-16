package me.bechberger.jfr.extended.table.exception;

/**
 * Exception thrown when attempting to access a cell using an invalid index within a row.
 * 
 * This exception provides specific information about the invalid cell access,
 * including the requested index and the valid range.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class CellIndexOutOfBoundsException extends DataAccessException {
    
    private final int requestedIndex;
    private final int cellCount;
    private final int rowIndex;
    
    /**
     * Creates a new CellIndexOutOfBoundsException.
     * 
     * @param requestedIndex The cell index that was requested
     * @param cellCount The actual number of cells in the row
     * @param rowIndex The row index where the error occurred
     */
    public CellIndexOutOfBoundsException(int requestedIndex, int cellCount, int rowIndex) {
        super(
            buildMessage(requestedIndex, cellCount, rowIndex),
            String.format("row %d, cell index %d", rowIndex, requestedIndex),
            String.format("valid cell index (0-%d)", cellCount - 1),
            "invalid index",
            requestedIndex,
            null
        );
        this.requestedIndex = requestedIndex;
        this.cellCount = cellCount;
        this.rowIndex = rowIndex;
    }
    
    /**
     * Creates a new CellIndexOutOfBoundsException without row context.
     * 
     * @param requestedIndex The cell index that was requested
     * @param cellCount The actual number of cells in the row
     */
    public CellIndexOutOfBoundsException(int requestedIndex, int cellCount) {
        this(requestedIndex, cellCount, -1);
    }
    
    private static String buildMessage(int requestedIndex, int cellCount, int rowIndex) {
        String rowInfo = rowIndex >= 0 ? " in row " + rowIndex : "";
        if (cellCount == 0) {
            return String.format("Cannot access cell at index %d%s: row has no cells", 
                requestedIndex, rowInfo);
        } else if (requestedIndex < 0) {
            return String.format("Cannot access cell at index %d%s: negative cell index (valid range: 0-%d)", 
                requestedIndex, rowInfo, cellCount - 1);
        } else {
            return String.format("Cannot access cell at index %d%s: index out of bounds (valid range: 0-%d)", 
                requestedIndex, rowInfo, cellCount - 1);
        }
    }
    
    /**
     * Gets the cell index that was requested.
     * 
     * @return The requested cell index
     */
    public int getRequestedIndex() {
        return requestedIndex;
    }
    
    /**
     * Gets the actual number of cells in the row.
     * 
     * @return The cell count
     */
    public int getCellCount() {
        return cellCount;
    }
    
    /**
     * Gets the row index where the error occurred.
     * 
     * @return The row index, or -1 if not available
     */
    public int getRowIndex() {
        return rowIndex;
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
     * Checks if the row was empty (no cells).
     * 
     * @return true if the row has no cells, false otherwise
     */
    public boolean isEmptyRow() {
        return cellCount == 0;
    }
}
