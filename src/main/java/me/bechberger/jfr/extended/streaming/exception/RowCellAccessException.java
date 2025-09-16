package me.bechberger.jfr.extended.streaming.exception;

import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.exception.CellAccessException;

/**
 * Exception thrown when attempting to access EventRow cell data with incorrect type assumptions.
 * 
 * This exception is specific to EventRow operations and provides context about
 * field names and column indices in the streaming architecture.
 * 
 * @author JFR Query Streaming Architecture
 * @since 2.0
 */
public class RowCellAccessException extends CellAccessException {
    
    /**
     * Creates a new RowCellAccessException for field-based access.
     * 
     * @param fieldName The field name where the error occurred
     * @param expectedType The expected type description
     * @param actualCell The actual cell value that caused the error
     */
    public RowCellAccessException(String fieldName, String expectedType, CellValue actualCell) {
        super(
            0, // row index not relevant for EventRow
            fieldName,
            expectedType,
            actualCell
        );
    }
    
    /**
     * Creates a new RowCellAccessException for index-based access.
     * 
     * @param columnIndex The column index where the error occurred
     * @param expectedType The expected type description
     * @param actualCell The actual cell value that caused the error
     */
    public RowCellAccessException(int columnIndex, String expectedType, CellValue actualCell) {
        super(
            columnIndex,
            expectedType,
            actualCell
        );
    }
    
    /**
     * Creates a new RowCellAccessException for null value access by field name.
     * 
     * @param fieldName The field name where the error occurred
     * @param expectedType The expected type description
     */
    public static RowCellAccessException forNullField(String fieldName, String expectedType) {
        return new RowCellAccessException(fieldName, expectedType, new CellValue.NullValue());
    }
    
    /**
     * Creates a new RowCellAccessException for null value access by column index.
     * 
     * @param columnIndex The column index where the error occurred
     * @param expectedType The expected type description
     */
    public static RowCellAccessException forNullColumn(int columnIndex, String expectedType) {
        return new RowCellAccessException(columnIndex, expectedType, new CellValue.NullValue());
    }
}
