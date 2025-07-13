package me.bechberger.jfr.extended.table;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents a JFR table with columns and rows of typed data
 */
public class JfrTable {
    
    private final List<Column> columns;
    private final List<Row> rows;
    private final Map<String, Integer> columnIndexMap;
    
    public JfrTable(List<Column> columns) {
        this.columns = new ArrayList<>(columns);
        this.rows = new ArrayList<>();
        this.columnIndexMap = new HashMap<>();
        
        for (int i = 0; i < columns.size(); i++) {
            columnIndexMap.put(columns.get(i).name(), i);
        }
    }
    
    /**
     * Column definition
     */
    public record Column(String name, CellType type, String description) {
        public Column(String name, CellType type) {
            this(name, type, null);
        }
    }
    
    /**
     * Row of data
     */
    public static class Row {
        private final List<CellValue> cells;
        
        public Row(List<CellValue> cells) {
            this.cells = new ArrayList<>(cells);
        }
        
        public Row(CellValue... cells) {
            this.cells = Arrays.asList(cells);
        }
        
        public List<CellValue> getCells() {
            return Collections.unmodifiableList(cells);
        }
        
        public CellValue getCell(int index) {
            if (index < 0 || index >= cells.size()) {
                return new CellValue.NullValue();
            }
            return cells.get(index);
        }
        
        public void setCell(int index, CellValue value) {
            if (index >= 0 && index < cells.size()) {
                cells.set(index, value);
            }
        }
        
        public int size() {
            return cells.size();
        }
    }
    
    /**
     * Add a row to the table
     */
    public void addRow(Row row) {
        if (row.size() != columns.size()) {
            throw new IllegalArgumentException("Row size (" + row.size() + 
                ") does not match column count (" + columns.size() + ")");
        }
        rows.add(row);
    }
    
    /**
     * Add a row with cell values
     */
    public void addRow(CellValue... cells) {
        addRow(new Row(cells));
    }
    
    /**
     * Get all columns
     */
    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }
    
    /**
     * Get all rows
     */
    public List<Row> getRows() {
        return Collections.unmodifiableList(rows);
    }
    
    /**
     * Get column by name
     */
    public Optional<Column> getColumn(String name) {
        Integer index = columnIndexMap.get(name);
        return index != null ? Optional.of(columns.get(index)) : Optional.empty();
    }
    
    /**
     * Get column index by name
     */
    public int getColumnIndex(String name) {
        return columnIndexMap.getOrDefault(name, -1);
    }
    
    /**
     * Get cell value by row and column name
     */
    public CellValue getCell(int rowIndex, String columnName) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            return new CellValue.NullValue();
        }
        
        int columnIndex = getColumnIndex(columnName);
        if (columnIndex == -1) {
            return new CellValue.NullValue();
        }
        
        return rows.get(rowIndex).getCell(columnIndex);
    }
    
    /**
     * Get cell value by row and column index
     */
    public CellValue getCell(int rowIndex, int columnIndex) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            return new CellValue.NullValue();
        }
        
        return rows.get(rowIndex).getCell(columnIndex);
    }
    
    /**
     * Get all values in a column
     */
    public List<CellValue> getColumnValues(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        if (columnIndex == -1) {
            return Collections.emptyList();
        }
        
        return rows.stream()
            .map(row -> row.getCell(columnIndex))
            .collect(Collectors.toList());
    }
    
    /**
     * Get table size
     */
    public int getRowCount() {
        return rows.size();
    }
    
    public int getColumnCount() {
        return columns.size();
    }
    
    /**
     * Filter rows based on a predicate
     */
    public JfrTable filter(RowPredicate predicate) {
        JfrTable result = new JfrTable(columns);
        
        for (Row row : rows) {
            if (predicate.test(row, this)) {
                result.addRow(row);
            }
        }
        
        return result;
    }
    
    /**
     * Select specific columns
     */
    public JfrTable select(String... columnNames) {
        List<Column> selectedColumns = new ArrayList<>();
        List<Integer> columnIndices = new ArrayList<>();
        
        for (String columnName : columnNames) {
            int index = getColumnIndex(columnName);
            if (index != -1) {
                selectedColumns.add(columns.get(index));
                columnIndices.add(index);
            }
        }
        
        JfrTable result = new JfrTable(selectedColumns);
        
        for (Row row : rows) {
            List<CellValue> selectedCells = new ArrayList<>();
            for (int index : columnIndices) {
                selectedCells.add(row.getCell(index));
            }
            result.addRow(new Row(selectedCells));
        }
        
        return result;
    }
    
    /**
     * Sort table by column
     */
    public JfrTable sort(String columnName, boolean ascending) {
        int columnIndex = getColumnIndex(columnName);
        if (columnIndex == -1) {
            return this; // Return copy if column not found
        }
        
        JfrTable result = new JfrTable(columns);
        List<Row> sortedRows = new ArrayList<>(rows);
        
        sortedRows.sort((a, b) -> {
            CellValue cellA = a.getCell(columnIndex);
            CellValue cellB = b.getCell(columnIndex);
            int comparison = CellValue.compare(cellA, cellB);
            return ascending ? comparison : -comparison;
        });
        
        for (Row row : sortedRows) {
            result.addRow(row);
        }
        
        return result;
    }
    
    /**
     * Create a copy of this table
     */
    public JfrTable copy() {
        JfrTable result = new JfrTable(columns);
        for (Row row : rows) {
            result.addRow(row);
        }
        return result;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        
        // Header
        sb.append(columns.stream()
            .map(Column::name)
            .collect(Collectors.joining("\t")));
        sb.append("\n");
        
        // Rows
        for (Row row : rows) {
            sb.append(row.getCells().stream()
                .map(CellValue::toString)
                .collect(Collectors.joining("\t")));
            sb.append("\n");
        }
        
        return sb.toString();
    }
    
    /**
     * Predicate for filtering rows
     */
    @FunctionalInterface
    public interface RowPredicate {
        boolean test(Row row, JfrTable table);
    }
}
