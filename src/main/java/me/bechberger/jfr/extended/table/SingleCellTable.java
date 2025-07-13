package me.bechberger.jfr.extended.table;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Optimized version of JfrTable for single-cell (one column, one row) results.
 * Used to efficiently store evaluation results that don't result in proper tables
 * (e.g., array literals, scalar expressions).
 */
public class SingleCellTable extends JfrTable {
    
    private final CellValue value;
    private final Column column;
    
    /**
     * Create a SingleCellTable with the given column name and cell value
     */
    public SingleCellTable(String columnName, CellValue value) {
        super(List.of(new Column(columnName, value.getType())));
        this.value = value;
        this.column = new Column(columnName, value.getType());
        // Add the single row
        super.addRow(new Row(value));
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
    public Optional<Column> getColumn(String name) {
        return column.name().equals(name) ? Optional.of(column) : Optional.empty();
    }
    
    @Override
    public int getColumnIndex(String name) {
        return column.name().equals(name) ? 0 : -1;
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
