package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.engine.RawJfrQueryExecutor;
import me.bechberger.jfr.extended.ast.ASTNodes.RawJfrQueryNode;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Enhanced mock executor that can handle basic SQL operations for testing
 */
public class MockRawJfrQueryExecutor implements RawJfrQueryExecutor {
    private final Map<String, JfrTable> tables = new HashMap<>();
    
    public void registerTable(String name, JfrTable table) {
        tables.put(name, table);
    }
    
    @Override
    public JfrTable execute(RawJfrQueryNode queryNode) throws Exception {
        String query = queryNode.rawQuery().trim();
        
        // Parse the query to extract components
        QueryInfo queryInfo = parseQuery(query);
        
        // Find the table
        JfrTable table = findTable(queryInfo.tableName);
        if (table == null) {
            throw new Exception("Table not found: " + queryInfo.tableName);
        }
        
        // Apply column selection
        JfrTable result = selectColumns(table, queryInfo.columns);
        
        // Apply sorting if specified
        if (queryInfo.orderByColumn != null) {
            result = sortTable(result, queryInfo.orderByColumn, queryInfo.ascending);
        }
        
        // Apply LIMIT if specified
        if (queryInfo.limit > 0) {
            result = limitRows(result, queryInfo.limit);
        }
        
        return result;
    }
    
    private JfrTable findTable(String tableName) {
        // Try exact match first
        JfrTable table = tables.get(tableName);
        if (table != null) {
            return table;
        }
        
        // Try case-insensitive match
        for (Map.Entry<String, JfrTable> entry : tables.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(tableName)) {
                return entry.getValue();
            }
        }
        
        return null;
    }
    
    private JfrTable selectColumns(JfrTable table, List<String> columnNames) {
        if (columnNames.size() == 1 && "*".equals(columnNames.get(0))) {
            return table; // Select all columns
        }
        
        // Build new table with selected columns
        List<JfrTable.Column> selectedColumns = new ArrayList<>();
        List<Integer> columnIndices = new ArrayList<>();
        
        for (String columnName : columnNames) {
            for (int i = 0; i < table.getColumns().size(); i++) {
                JfrTable.Column column = table.getColumns().get(i);
                if (column.name().equalsIgnoreCase(columnName)) {
                    selectedColumns.add(column);
                    columnIndices.add(i);
                    break;
                }
            }
        }
        
        if (selectedColumns.isEmpty()) {
            return table; // Fallback to all columns if none found
        }
        
        JfrTable result = new StandardJfrTable(selectedColumns);
        for (JfrTable.Row row : table.getRows()) {
            List<CellValue> selectedCells = new ArrayList<>();
            for (int index : columnIndices) {
                selectedCells.add(row.getCells().get(index));
            }
            result.addRow(new JfrTable.Row(selectedCells));
        }
        
        return result;
    }
    
    private JfrTable sortTable(JfrTable table, String columnName, boolean ascending) {
        // Find the column index
        int columnIndex = -1;
        for (int i = 0; i < table.getColumns().size(); i++) {
            if (table.getColumns().get(i).name().equalsIgnoreCase(columnName)) {
                columnIndex = i;
                break;
            }
        }
        
        if (columnIndex == -1) {
            return table; // Column not found, return unsorted
        }
        
        // Sort the rows
        List<JfrTable.Row> sortedRows = new ArrayList<>(table.getRows());
        final int finalColumnIndex = columnIndex;
        
        sortedRows.sort((row1, row2) -> {
            CellValue cell1 = row1.getCells().get(finalColumnIndex);
            CellValue cell2 = row2.getCells().get(finalColumnIndex);
            
            int comparison = compareCellValues(cell1, cell2);
            return ascending ? comparison : -comparison;
        });
        
        // Build new table with sorted rows
        JfrTable result = new StandardJfrTable(table.getColumns());
        for (JfrTable.Row row : sortedRows) {
            result.addRow(row);
        }
        
        return result;
    }
    
    private int compareCellValues(CellValue a, CellValue b) {
        if (a instanceof CellValue.NullValue && b instanceof CellValue.NullValue) {
            return 0;
        }
        if (a instanceof CellValue.NullValue) {
            return -1;
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
    
    private QueryInfo parseQuery(String query) {
        QueryInfo info = new QueryInfo();
        
        // Normalize query
        query = query.toUpperCase().trim();
        
        // Extract table name using regex
        Pattern fromPattern = Pattern.compile("FROM\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
        Matcher fromMatcher = fromPattern.matcher(query);
        if (fromMatcher.find()) {
            info.tableName = fromMatcher.group(1);
        }
        
        // Extract column names
        Pattern selectPattern = Pattern.compile("SELECT\\s+(.*?)\\s+FROM", Pattern.CASE_INSENSITIVE);
        Matcher selectMatcher = selectPattern.matcher(query);
        if (selectMatcher.find()) {
            String columnsStr = selectMatcher.group(1).trim();
            if ("*".equals(columnsStr)) {
                info.columns.add("*");
            } else {
                String[] columnArray = columnsStr.split(",");
                for (String column : columnArray) {
                    info.columns.add(column.trim());
                }
            }
        } else {
            info.columns.add("*"); // Default to all columns
        }
        
        // Extract ORDER BY clause
        Pattern orderPattern = Pattern.compile("ORDER\\s+BY\\s+(\\w+)(?:\\s+(ASC|DESC))?", Pattern.CASE_INSENSITIVE);
        Matcher orderMatcher = orderPattern.matcher(query);
        if (orderMatcher.find()) {
            info.orderByColumn = orderMatcher.group(1);
            String direction = orderMatcher.group(2);
            info.ascending = direction == null || "ASC".equalsIgnoreCase(direction);
        }
        
        // Extract LIMIT clause
        Pattern limitPattern = Pattern.compile("LIMIT\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
        Matcher limitMatcher = limitPattern.matcher(query);
        if (limitMatcher.find()) {
            info.limit = Integer.parseInt(limitMatcher.group(1));
        }
        
        return info;
    }
    
    /**
     * Apply a LIMIT clause to limit the number of rows
     */
    private JfrTable limitRows(JfrTable table, int limit) {
        if (limit <= 0 || table.getRows().size() <= limit) {
            return table; // No need to apply limit
        }
        
        // Create a new table with limited rows
        JfrTable result = new StandardJfrTable(table.getColumns());
        for (int i = 0; i < limit && i < table.getRows().size(); i++) {
            result.addRow(table.getRows().get(i));
        }
        
        return result;
    }
    
    private static class QueryInfo {
        String tableName;
        List<String> columns = new ArrayList<>();
        String orderByColumn;
        boolean ascending = true;
        int limit = -1; // -1 means no limit
    }
}
