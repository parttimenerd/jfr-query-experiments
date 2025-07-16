package me.bechberger.jfr.extended.table;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Special marker table representing the '*' symbol used in aggregate functions like COUNT(*).
 * This provides a type-safe way to represent "all rows" or "count all" semantics.
 * 
 * The StarMarkerTable is recognized by aggregate functions to indicate they should
 * operate on all rows rather than a specific field value.
 * 
 * @author QueryEvaluator
 * @since 3.0
 */
public class StarMarkerTable implements JfrTable {
    
    private static final StarMarkerTable INSTANCE = new StarMarkerTable();
    
    private static final String STAR_MARKER = "*";
    private static final JfrTable.Column STAR_COLUMN = new JfrTable.Column(STAR_MARKER, CellType.STRING);
    private static final CellValue STAR_VALUE = new CellValue.StringValue(STAR_MARKER);
    private static final JfrTable.Row STAR_ROW = new JfrTable.Row(List.of(STAR_VALUE));
    
    // Private constructor to enforce singleton pattern
    private StarMarkerTable() {
    }
    
    /**
     * Get the singleton instance of StarMarkerTable.
     * 
     * @return the singleton StarMarkerTable instance
     */
    public static StarMarkerTable getInstance() {
        return INSTANCE;
    }
    
    /**
     * Check if a table is a star marker.
     * 
     * @param table the table to check
     * @return true if the table is a StarMarkerTable
     */
    public static boolean isStarMarker(JfrTable table) {
        return table instanceof StarMarkerTable;
    }
    
    /**
     * Check if a CellValue represents a star marker.
     * 
     * @param value the value to check
     * @return true if the value is a star marker
     */
    public static boolean isStarValue(CellValue value) {
        return value instanceof CellValue.StringValue stringValue && 
               STAR_MARKER.equals(stringValue.value());
    }
    
    @Override
    public List<Column> getColumns() {
        return List.of(STAR_COLUMN);
    }
    
    @Override
    public List<Row> getRows() {
        return List.of(STAR_ROW);
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
    public void addRow(Row row) {
        throw new UnsupportedOperationException("Cannot add rows to StarMarkerTable");
    }
    
    @Override
    public void addRow(CellValue... cells) {
        throw new UnsupportedOperationException("Cannot add rows to StarMarkerTable");
    }
    
    @Override
    public Optional<Column> getColumn(String name) {
        if (STAR_MARKER.equals(name)) {
            return Optional.of(STAR_COLUMN);
        }
        return Optional.empty();
    }
    
    @Override
    public int getColumnIndex(String name) {
        if (STAR_MARKER.equals(name)) {
            return 0;
        }
        return -1;
    }
    
    @Override
    public List<CellValue> getColumnValues(String columnName) {
        if (STAR_MARKER.equals(columnName)) {
            return List.of(STAR_VALUE);
        }
        return Collections.emptyList();
    }
    
    @Override
    public JfrTable filter(RowPredicate predicate) {
        // Apply the predicate to our single row
        if (predicate.test(STAR_ROW, this)) {
            return this; // Return the same instance if the row passes the filter
        } else {
            // Return an empty table with the same structure
            return new StandardJfrTable(getColumns());
        }
    }
    
    @Override
    public JfrTable select(String... columnNames) {
        // Check if the star column is being selected
        for (String columnName : columnNames) {
            if (STAR_MARKER.equals(columnName)) {
                return this; // Return the same instance if selecting the star column
            }
        }
        // Return an empty table if the star column is not selected
        return new StandardJfrTable(Collections.emptyList());
    }
    
    @Override
    public JfrTable sort(String columnName, boolean ascending) {
        // Since we only have one row, sorting doesn't change anything
        return this;
    }
    
    @Override
    public JfrTable copy() {
        // Since this is a singleton and immutable, return the same instance
        return this;
    }
    
    @Override
    public CellValue getCell(int rowIndex, int columnIndex) {
        if (rowIndex == 0 && columnIndex == 0) {
            return STAR_VALUE;
        }
        throw new IndexOutOfBoundsException("StarMarkerTable only has one cell at (0,0)");
    }
    
    @Override
    public CellValue getCell(int rowIndex, String columnName) {
        if (rowIndex == 0 && STAR_MARKER.equals(columnName)) {
            return STAR_VALUE;
        }
        throw new IndexOutOfBoundsException("StarMarkerTable only has one cell with column '*'");
    }
    
    public String getString(int rowIndex, String columnName) {
        if (rowIndex == 0 && STAR_MARKER.equals(columnName)) {
            return STAR_MARKER;
        }
        throw new IndexOutOfBoundsException("StarMarkerTable only has one cell with column '*'");
    }
    
    public long getNumber(int rowIndex, String columnName) {
        throw new ClassCastException("StarMarkerTable contains string values, not numbers");
    }
    
    public List<CellValue> getArray(int rowIndex, String columnName) {
        throw new ClassCastException("StarMarkerTable contains string values, not arrays");
    }
    
    @Override
    public String toString() {
        return "StarMarkerTable[*]";
    }
    
    @Override
    public boolean equals(Object obj) {
        return obj instanceof StarMarkerTable;
    }
    
    @Override
    public int hashCode() {
        return StarMarkerTable.class.hashCode();
    }
}
