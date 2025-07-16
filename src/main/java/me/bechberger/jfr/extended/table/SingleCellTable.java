package me.bechberger.jfr.extended.table;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Highly optimized implementation of JfrTable for single-cell (one column, one row) results.
 * 
 * This implementation is designed for maximum performance when dealing with single values,
 * eliminating the overhead of lists, maps, and other data structures used in StandardJfrTable.
 * 
 * Used efficiently for:
 * - Array literals
 * - Scalar expressions  
 * - Aggregation results
 * - Function return values
 * - Single-value query results
 */
public class SingleCellTable implements JfrTable {
    
    private final CellValue value;
    private final JfrTable.Column column;
    private final JfrTable.Row row;
    
    /**
     * Create a SingleCellTable with the given column name and cell value
     */
    public SingleCellTable(String columnName, CellValue value) {
        this.value = value;
        this.column = new JfrTable.Column(columnName, value.getType());
        this.row = new JfrTable.Row(value);
    }
    
    /**
     * Create a SingleCellTable with default "value" column name
     */
    public SingleCellTable(CellValue value) {
        this("value", value);
    }
    
    /**
     * Factory method for creating a table with a string value
     */
    public static SingleCellTable ofString(String value) {
        return new SingleCellTable(new CellValue.StringValue(value));
    }
    
    /**
     * Factory method for creating a table with a number value
     */
    public static SingleCellTable ofNumber(double value) {
        return new SingleCellTable(new CellValue.NumberValue(value));
    }
    
    /**
     * Factory method for creating a table with a boolean value
     */
    public static SingleCellTable ofBoolean(boolean value) {
        return new SingleCellTable(new CellValue.BooleanValue(value));
    }
    
    /**
     * Factory method for creating a table with a null value
     */
    public static SingleCellTable ofNull() {
        return new SingleCellTable(new CellValue.NullValue());
    }
    
    /**
     * Factory method for creating a table with any CellValue
     */
    public static SingleCellTable of(CellValue value) {
        return new SingleCellTable(value);
    }
    
    /**
     * Factory method for creating a table with any object (converted to appropriate CellValue)
     */
    public static SingleCellTable of(Object value) {
        return new SingleCellTable(CellValue.of(value));
    }
    
    /**
     * Factory method for creating a table with a custom column name and any object
     */
    public static SingleCellTable of(String columnName, Object value) {
        return new SingleCellTable(columnName, CellValue.of(value));
    }
    
    /**
     * Get the single cell value in this table
     */
    public CellValue getValue() {
        return value;
    }
    
    // Optimized JfrTable interface implementation
    
    @Override
    public List<JfrTable.Column> getColumns() {
        return List.of(column);
    }
    
    @Override
    public List<JfrTable.Row> getRows() {
        return List.of(row);
    }
    
    @Override
    public Optional<JfrTable.Column> getColumn(String name) {
        return column.name().equals(name) ? Optional.of(column) : Optional.empty();
    }
    
    @Override
    public int getColumnIndex(String name) {
        return column.name().equals(name) ? 0 : -1;
    }
    
    @Override
    public CellValue getCell(int rowIndex, String columnName) {
        if (rowIndex != 0 || !column.name().equals(columnName)) {
            return new CellValue.NullValue();
        }
        return value;
    }
    
    @Override
    public CellValue getCell(int rowIndex, int columnIndex) {
        if (rowIndex != 0 || columnIndex != 0) {
            return new CellValue.NullValue();
        }
        return value;
    }
    
    @Override
    public List<CellValue> getColumnValues(String columnName) {
        if (!column.name().equals(columnName)) {
            return Collections.emptyList();
        }
        return List.of(value);
    }
    
    @Override
    public int getRowCount() {
        return 1;
    }
    
    @Override
    public int getColumnCount() {
        return 1;
    }
    
    @Override
    public void addRow(JfrTable.Row row) {
        throw new UnsupportedOperationException("Cannot add rows to SingleCellTable");
    }
    
    @Override
    public void addRow(CellValue... cells) {
        throw new UnsupportedOperationException("Cannot add rows to SingleCellTable");
    }
    
    @Override
    public JfrTable filter(JfrTable.RowPredicate predicate) {
        // For single-cell table, either return this or empty table
        if (predicate.test(row, this)) {
            return this;
        } else {
            // Return empty StandardJfrTable
            return new StandardJfrTable(List.of(column));
        }
    }
    
    @Override
    public JfrTable select(String... columnNames) {
        for (String columnName : columnNames) {
            if (column.name().equals(columnName)) {
                return this; // Return self if our column is selected
            }
        }
        // Return empty table if our column is not selected
        return new StandardJfrTable(Collections.emptyList());
    }
    
    @Override
    public JfrTable sort(String columnName, boolean ascending) {
        // Single row table is always sorted
        return this;
    }
    
    @Override
    public JfrTable copy() {
        // SingleCellTable is immutable, so we can return this
        return this;
    }
    
    @Override
    public String toString() {
        return column.name() + "\n" + value.toString();
    }
    
    /**
     * Factory method to create an array table from a list of expressions
     */
    public static SingleCellTable createArrayTable(List<String> elements) {
        String arrayString = "[" + String.join(", ", elements) + "]";
        return SingleCellTable.ofString(arrayString);
    }
}
