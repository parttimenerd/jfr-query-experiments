package me.bechberger.jfr.extended.table;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Standard implementation of JfrTable for multi-row tables.
 * 
 * This is the traditional implementation that supports multiple rows and columns
 * with full table manipulation capabilities.
 */
public class StandardJfrTable implements JfrTable {
    
    private final List<Column> columns;
    private final List<Row> rows;
    private final Map<String, Integer> columnIndexMap;
    
    public StandardJfrTable(List<Column> columns) {
        this.columns = new ArrayList<>(columns);
        this.rows = new ArrayList<>();
        this.columnIndexMap = new HashMap<>();
        
        for (int i = 0; i < columns.size(); i++) {
            columnIndexMap.put(columns.get(i).name(), i);
        }
    }
    
    @Override
    public void addRow(Row row) {
        if (row.size() != columns.size()) {
            throw new IllegalArgumentException(
                "Row size (" + row.size() + ") doesn't match column count (" + columns.size() + ")");
        }
        rows.add(row);
    }
    
    @Override
    public void addRow(CellValue... cells) {
        if (cells.length != columns.size()) {
            throw new IllegalArgumentException(
                "Cell count (" + cells.length + ") doesn't match column count (" + columns.size() + ")");
        }
        addRow(new Row(cells));
    }
    
    @Override
    public List<Column> getColumns() {
        return Collections.unmodifiableList(columns);
    }
    
    @Override
    public List<Row> getRows() {
        return Collections.unmodifiableList(rows);
    }
    
    @Override
    public Optional<Column> getColumn(String name) {
        Integer index = columnIndexMap.get(name);
        return index != null ? Optional.of(columns.get(index)) : Optional.empty();
    }
    
    @Override
    public int getColumnIndex(String name) {
        return columnIndexMap.getOrDefault(name, -1);
    }
    
    @Override
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
    
    @Override
    public CellValue getCell(int rowIndex, int columnIndex) {
        if (rowIndex < 0 || rowIndex >= rows.size()) {
            return new CellValue.NullValue();
        }
        
        return rows.get(rowIndex).getCell(columnIndex);
    }
    
    @Override
    public List<CellValue> getColumnValues(String columnName) {
        int columnIndex = getColumnIndex(columnName);
        if (columnIndex == -1) {
            return Collections.emptyList();
        }
        
        return rows.stream()
            .map(row -> row.getCell(columnIndex))
            .collect(Collectors.toList());
    }
    
    @Override
    public int getRowCount() {
        return rows.size();
    }
    
    @Override
    public int getColumnCount() {
        return columns.size();
    }
    
    @Override
    public JfrTable filter(RowPredicate predicate) {
        StandardJfrTable result = new StandardJfrTable(columns);
        
        for (Row row : rows) {
            if (predicate.test(row, this)) {
                result.addRow(row);
            }
        }
        
        return result;
    }
    
    @Override
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
        
        StandardJfrTable result = new StandardJfrTable(selectedColumns);
        
        for (Row row : rows) {
            List<CellValue> selectedCells = new ArrayList<>();
            for (int index : columnIndices) {
                selectedCells.add(row.getCell(index));
            }
            result.addRow(new Row(selectedCells));
        }
        
        return result;
    }
    
    @Override
    public JfrTable sort(String columnName, boolean ascending) {
        int columnIndex = getColumnIndex(columnName);
        if (columnIndex == -1) {
            return copy(); // Return copy if column not found
        }
        
        StandardJfrTable result = new StandardJfrTable(columns);
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
    
    @Override
    public JfrTable copy() {
        StandardJfrTable result = new StandardJfrTable(columns);
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
}
