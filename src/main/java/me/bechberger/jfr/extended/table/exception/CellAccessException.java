package me.bechberger.jfr.extended.table.exception;

import me.bechberger.jfr.extended.table.CellValue;

/**
 * Exception thrown when attempting to access cell data with incorrect type assumptions.
 * 
 * This exception provides specific information about the attempted cell access,
 * including the cell location, expected type, actual type, and suggestions for correction.
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class CellAccessException extends DataAccessException {
    
    /**
     * Creates a new CellAccessException for row-based access.
     * 
     * @param rowIndex The row index where the error occurred
     * @param columnIndex The column index where the error occurred  
     * @param expectedType The expected type description
     * @param actualCell The actual cell value that caused the error
     */
    public CellAccessException(int rowIndex, int columnIndex, String expectedType, CellValue actualCell) {
        super(
            String.format("Cannot access cell at row %d, column %d as %s: cell contains %s with value '%s'", 
                rowIndex, columnIndex, expectedType, actualCell.getType(), actualCell.getValue()),
            String.format("row %d, column %d", rowIndex, columnIndex),
            expectedType,
            actualCell.getType().toString(),
            actualCell
        );
    }
    
    /**
     * Creates a new CellAccessException for column-based access.
     * 
     * @param rowIndex The row index where the error occurred
     * @param columnName The column name where the error occurred
     * @param expectedType The expected type description
     * @param actualCell The actual cell value that caused the error
     */
    public CellAccessException(int rowIndex, String columnName, String expectedType, CellValue actualCell) {
        super(
            String.format("Cannot access cell at row %d, column '%s' as %s: cell contains %s with value '%s'", 
                rowIndex, columnName, expectedType, actualCell.getType(), actualCell.getValue()),
            String.format("row %d, column '%s'", rowIndex, columnName),
            expectedType,
            actualCell.getType().toString(),
            actualCell
        );
    }
    
    /**
     * Creates a new CellAccessException for index-based access in a row.
     * 
     * @param columnIndex The column index where the error occurred
     * @param expectedType The expected type description
     * @param actualCell The actual cell value that caused the error
     */
    public CellAccessException(int columnIndex, String expectedType, CellValue actualCell) {
        super(
            String.format("Cannot access cell at column %d as %s: cell contains %s with value '%s'", 
                columnIndex, expectedType, actualCell.getType(), actualCell.getValue()),
            String.format("column %d", columnIndex),
            expectedType,
            actualCell.getType().toString(),
            actualCell
        );
    }
    
    /**
     * Creates a new CellAccessException for null value access.
     * 
     * @param location The location description (e.g., "row 0, column 1")
     * @param expectedType The expected type description
     */
    public static CellAccessException forNullValue(String location, String expectedType) {
        return new CellAccessException(
            String.format("Cannot access null cell at %s as %s: cell is null", location, expectedType),
            location,
            expectedType,
            "null",
            null
        );
    }
    
    /**
     * Internal constructor for null values.
     */
    private CellAccessException(String message, String location, String expectedType, String actualType, Object actualValue) {
        super(message, location, expectedType, actualType, actualValue);
    }
}
