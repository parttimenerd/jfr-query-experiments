package me.bechberger.jfr.extended.streaming;

import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.JfrTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of EventRow that wraps a JfrTable.Row.
 * 
 * @author JFR Query Streaming Architecture
 * @since 2.0
 */
public class TableEventRow implements EventRow {
    
    private final JfrTable.Row row;
    private final List<JfrTable.Column> columns;
    private final Map<String, Integer> columnIndexMap;
    
    public TableEventRow(JfrTable.Row row, List<JfrTable.Column> columns) {
        this.row = row;
        this.columns = columns;
        this.columnIndexMap = createColumnIndexMap(columns);
    }
    
    private static Map<String, Integer> createColumnIndexMap(List<JfrTable.Column> columns) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            map.put(columns.get(i).name(), i);
        }
        return map;
    }
    
    @Override
    public CellValue getCellValue(int columnIndex) {
        if (columnIndex < 0 || columnIndex >= columns.size()) {
            throw new IndexOutOfBoundsException("Column index " + columnIndex + " out of bounds");
        }
        return row.getCell(columnIndex);
    }
    
    @Override
    public CellValue getCellValue(String fieldName) {
        Integer index = columnIndexMap.get(fieldName);
        return index != null ? getCellValue(index) : new CellValue.NullValue();
    }
    
    @Override
    public boolean hasField(String fieldName) {
        return columnIndexMap.containsKey(fieldName);
    }
    
    @Override
    public List<String> getFieldNames() {
        return columns.stream().map(JfrTable.Column::name).toList();
    }
    
    @Override
    public List<JfrTable.Column> getColumns() {
        return columns;
    }
    
    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        for (JfrTable.Column column : columns) {
            map.put(column.name(), getValue(column.name()));
        }
        return map;
    }
    
    @Override
    public int getFieldCount() {
        return columns.size();
    }
    
    @Override
    public String toString() {
        return "TableEventRow{" + toMap() + "}";
    }
}
