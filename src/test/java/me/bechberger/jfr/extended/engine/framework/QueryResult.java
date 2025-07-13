package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.*;

/**
 * Result wrapper for query execution
 */
public class QueryResult {
    private final JfrTable table;
    private final boolean success;
    private final Exception error;
    
    public QueryResult(JfrTable table, boolean success, Exception error) {
        this.table = table;
        this.success = success;
        this.error = error;
    }
    
    public boolean isSuccess() { return success; }
    public JfrTable getTable() { return table; }
    public Exception getError() { return error; }
    public int getRowCount() { return table != null ? table.getRowCount() : 0; }
    public int getColumnCount() { return table != null ? table.getColumnCount() : 0; }
    
    /**
     * Assert that the query succeeded
     */
    public QueryResult assertSuccess() {
        if (!success) {
            throw new AssertionError("Expected query to succeed but it failed: " + 
                (error != null ? error.getMessage() : "Unknown error"));
        }
        return this;
    }
    
    /**
     * Assert that the result matches the expected result
     */
    public QueryResult assertResult(ExpectedResult expected) {
        expected.assertMatches(this);
        return this;
    }
    
    /**
     * Assert that the result is sorted by a specific column
     */
    public QueryResult assertSortedByColumn(String columnName, boolean ascending) {
        if (table == null) {
            throw new AssertionError("Cannot check sorting on null table");
        }
        
        // Find the column index
        int columnIndex = -1;
        List<JfrTable.Column> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(columnName)) {
                columnIndex = i;
                break;
            }
        }
        
        if (columnIndex == -1) {
            throw new AssertionError("Column '" + columnName + "' not found in table. Available columns: " + 
                columns.stream().map(JfrTable.Column::name).toList());
        }
        
        // Check if the column is sorted
        List<JfrTable.Row> rows = table.getRows();
        if (rows.size() <= 1) {
            return this; // Single row or empty table is trivially sorted
        }
        
        for (int i = 1; i < rows.size(); i++) {
            CellValue prev = rows.get(i - 1).getCells().get(columnIndex);
            CellValue curr = rows.get(i).getCells().get(columnIndex);
            
            int comparison = compareCellValues(prev, curr);
            
            if (ascending && comparison > 0) {
                throw new AssertionError("Table is not sorted ascending by column '" + columnName + 
                    "' at row " + i + ": " + prev.getValue() + " > " + curr.getValue());
            } else if (!ascending && comparison < 0) {
                throw new AssertionError("Table is not sorted descending by column '" + columnName + 
                    "' at row " + i + ": " + prev.getValue() + " < " + curr.getValue());
            }
        }
        
        return this;
    }
    
    /**
     * Compare two cell values for sorting validation
     */
    private int compareCellValues(CellValue a, CellValue b) {
        if (a instanceof CellValue.NullValue && b instanceof CellValue.NullValue) {
            return 0;
        }
        if (a instanceof CellValue.NullValue) {
            return -1; // Nulls come first
        }
        if (b instanceof CellValue.NullValue) {
            return 1;
        }
        
        Object valA = a.getValue();
        Object valB = b.getValue();
        
        // Handle numeric comparisons
        if (valA instanceof Number && valB instanceof Number) {
            double doubleA = ((Number) valA).doubleValue();
            double doubleB = ((Number) valB).doubleValue();
            return Double.compare(doubleA, doubleB);
        }
        
        // Handle string comparisons
        if (valA instanceof String && valB instanceof String) {
            return ((String) valA).compareTo((String) valB);
        }
        
        // Handle boolean comparisons
        if (valA instanceof Boolean && valB instanceof Boolean) {
            return ((Boolean) valA).compareTo((Boolean) valB);
        }
        
        // Fallback to string comparison
        return valA.toString().compareTo(valB.toString());
    }
    
    /**
     * Convert the query result table to expectTable format for easy comparison and debugging
     */
    @Override
    public String toString() {
        if (!success) {
            return "Query failed: " + (error != null ? error.getMessage() : "Unknown error");
        }
        
        if (table == null) {
            return "No table result";
        }
        
        if (table.getRowCount() == 0) {
            // Show column headers even for empty tables
            List<String> columnNames = table.getColumns().stream()
                .map(JfrTable.Column::name)
                .toList();
            return String.join(" ", columnNames);
        }
        
        StringBuilder sb = new StringBuilder();
        
        // Add column headers
        List<String> columnNames = table.getColumns().stream()
            .map(JfrTable.Column::name)
            .toList();
        sb.append(String.join(" ", columnNames));
        
        // Add data rows
        for (JfrTable.Row row : table.getRows()) {
            sb.append("\n");
            List<String> cellValues = new ArrayList<>();
            
            for (CellValue cell : row.getCells()) {
                String cellStr;
                if (cell instanceof CellValue.NullValue) {
                    cellStr = "null";
                } else {
                    Object value = cell.getValue();
                    if (value == null) {
                        cellStr = "null";
                    } else if (value instanceof List) {
                        // Handle arrays - format as [item1,item2,item3]
                        List<?> list = (List<?>) value;
                        cellStr = "[" + list.stream()
                            .map(Object::toString)
                            .reduce((a, b) -> a + "," + b)
                            .orElse("") + "]";
                    } else {
                        cellStr = value.toString();
                    }
                }
                cellValues.add(cellStr);
            }
            
            sb.append(String.join(" ", cellValues));
        }
        
        return sb.toString();
    }
}
