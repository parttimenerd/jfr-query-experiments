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
        
        // Handle extended queries (starting with @)
        if (query.startsWith("@") || query.startsWith("@ ")) {
            query = query.substring(query.indexOf("SELECT"));
        }
        
        // System.err.println("DEBUG MockRawJfrQueryExecutor: Processing query: " + query);
        
        // Parse the query to extract components
        QueryInfo queryInfo = parseQuery(query);
        
        // System.err.println("DEBUG MockRawJfrQueryExecutor: Table: " + queryInfo.tableName + 
        //                   ", Columns: " + queryInfo.columns + 
        //                   ", WHERE: " + queryInfo.whereClause + 
        //                   ", HasAggregates: " + queryInfo.hasAggregates);
        
        // Find the table
        JfrTable table = findTable(queryInfo.tableName);
        if (table == null) {
            throw new Exception("Table not found: " + queryInfo.tableName);
        }
        
        // System.err.println("DEBUG MockRawJfrQueryExecutor: Found table with " + table.getRowCount() + " rows");
        
        // Apply WHERE clause filtering
        if (queryInfo.whereClause != null) {
            table = filterTable(table, queryInfo.whereClause);
            // System.err.println("DEBUG MockRawJfrQueryExecutor: After WHERE filtering: " + table.getRowCount() + " rows");
        }
        
        // Apply column selection
        JfrTable result = selectColumns(table, queryInfo.columns);
        // System.err.println("DEBUG MockRawJfrQueryExecutor: After column selection: " + result.getRowCount() + " rows, " + result.getColumnCount() + " columns");
        
        // Apply aggregate functions if needed
        if (queryInfo.hasAggregates) {
            result = applyAggregates(result, queryInfo.columns);
            // System.err.println("DEBUG MockRawJfrQueryExecutor: After aggregates: " + result.getRowCount() + " rows, " + result.getColumnCount() + " columns");
        }
        
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
        
        // Store original query for string literal preservation
        String originalQuery = query.trim();
        
        // Normalize query for pattern matching (but preserve original for WHERE clause)
        String normalizedQuery = query.toUpperCase().trim();
        
        // Extract table name using regex
        Pattern fromPattern = Pattern.compile("FROM\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
        Matcher fromMatcher = fromPattern.matcher(normalizedQuery);
        if (fromMatcher.find()) {
            info.tableName = fromMatcher.group(1);
        }
        
        // Extract column names
        Pattern selectPattern = Pattern.compile("SELECT\\s+(.*?)\\s+FROM", Pattern.CASE_INSENSITIVE);
        Matcher selectMatcher = selectPattern.matcher(normalizedQuery);
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
        Matcher orderMatcher = orderPattern.matcher(normalizedQuery);
        if (orderMatcher.find()) {
            info.orderByColumn = orderMatcher.group(1);
            String direction = orderMatcher.group(2);
            info.ascending = direction == null || "ASC".equalsIgnoreCase(direction);
        }
        
        // Extract WHERE clause (use original query to preserve string literal case)
        Pattern wherePattern = Pattern.compile("WHERE\\s+(.+?)(?:\\s+ORDER\\s+BY|\\s+LIMIT|$)", Pattern.CASE_INSENSITIVE);
        Matcher whereMatcher = wherePattern.matcher(originalQuery);
        if (whereMatcher.find()) {
            info.whereClause = whereMatcher.group(1).trim();
        }
        
        // Check for aggregate functions in columns
        for (String column : info.columns) {
            String normalizedColumn = column.toUpperCase().replaceAll("\\s+", "");
            if (normalizedColumn.contains("AVG(") || 
                normalizedColumn.contains("COUNT(") ||
                normalizedColumn.contains("SUM(") ||
                normalizedColumn.contains("MIN(") ||
                normalizedColumn.contains("MAX(")) {
                info.hasAggregates = true;
                break;
            }
        }
        
        // Extract LIMIT clause
        Pattern limitPattern = Pattern.compile("LIMIT\\s+(\\d+)", Pattern.CASE_INSENSITIVE);
        Matcher limitMatcher = limitPattern.matcher(normalizedQuery);
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

    @Override
    public List<jdk.jfr.EventType> getEventTypes() throws Exception {
        // Return an empty list for testing
        // The QueryPlanExecutor will handle this gracefully and use defaults
        return List.of();
    }
    
    /**
     * Apply WHERE clause filtering to the table
     */
    private JfrTable filterTable(JfrTable table, String whereClause) {
        if (whereClause == null || whereClause.trim().isEmpty()) {
            return table;
        }
        
        // For simplicity, handle basic equality conditions like "column = 'value'"
        // This covers the correlated subquery case: "e2.department = 'Sales'"
        List<JfrTable.Row> filteredRows = new ArrayList<>();
        
        for (JfrTable.Row row : table.getRows()) {
            if (evaluateWhereCondition(row, table.getColumns(), whereClause)) {
                filteredRows.add(row);
            }
        }
        
        JfrTable result = new StandardJfrTable(table.getColumns());
        for (JfrTable.Row row : filteredRows) {
            result.addRow(row);
        }
        
        return result;
    }
    
    /**
     * Evaluate a WHERE condition for a single row
     */
    private boolean evaluateWhereCondition(JfrTable.Row row, List<JfrTable.Column> columns, String whereClause) {
        // Handle IS NULL / IS NOT NULL
        Pattern nullPattern = Pattern.compile("(\\w+(?:\\.\\w+)?)\\s+IS\\s+(NOT\\s+)?NULL", Pattern.CASE_INSENSITIVE);
        Matcher nullMatcher = nullPattern.matcher(whereClause);
        if (nullMatcher.find()) {
            String columnName = nullMatcher.group(1);
            boolean isNotNull = nullMatcher.group(2) != null;
            
            // Handle qualified column names (e.g., "e2.manager_id" -> "manager_id")
            if (columnName.contains(".")) {
                columnName = columnName.substring(columnName.lastIndexOf('.') + 1);
            }
            
            // Find the column and check for null
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).name().equalsIgnoreCase(columnName)) {
                    CellValue cellValue = row.getCell(i);
                    boolean isNull = cellValue instanceof CellValue.NullValue;
                    return isNotNull ? !isNull : isNull;
                }
            }
        }
        
        // Handle equality comparisons: column = 'value' or column = value
        Pattern equalityPattern = Pattern.compile("(\\w+(?:\\.\\w+)?)\\s*=\\s*'([^']*)'|" +
                                                "(\\w+(?:\\.\\w+)?)\\s*=\\s*(\\w+)", Pattern.CASE_INSENSITIVE);
        Matcher equalityMatcher = equalityPattern.matcher(whereClause);
        
        if (equalityMatcher.find()) {
            String columnName;
            String expectedValue;
            
            if (equalityMatcher.group(1) != null) {
                // Quoted value: column = 'value'
                columnName = equalityMatcher.group(1);
                expectedValue = equalityMatcher.group(2);
            } else {
                // Unquoted value: column = value
                columnName = equalityMatcher.group(3);
                expectedValue = equalityMatcher.group(4);
            }
            
            // Handle qualified column names (e.g., "e2.department" -> "department")
            if (columnName.contains(".")) {
                columnName = columnName.substring(columnName.lastIndexOf('.') + 1);
            }
            
            // Find the column and compare values
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).name().equalsIgnoreCase(columnName)) {
                    CellValue cellValue = row.getCell(i);
                    String actualValue;
                    
                    // Properly extract the value based on cell type
                    if (cellValue instanceof CellValue.StringValue) {
                        actualValue = ((CellValue.StringValue) cellValue).value();
                    } else if (cellValue instanceof CellValue.NullValue) {
                        actualValue = null;
                    } else {
                        actualValue = cellValue.toString();
                    }
                    
                    return Objects.equals(expectedValue, actualValue);
                }
            }
        }
        
        // If we can't parse the condition, return true (no filtering)
        return true;
    }
    
    /**
     * Apply aggregate functions to the result table
     */
    private JfrTable applyAggregates(JfrTable table, List<String> columns) {
        List<JfrTable.Column> resultColumns = new ArrayList<>();
        List<CellValue> resultRow = new ArrayList<>();
        
        for (String column : columns) {
            String normalizedColumn = column.toUpperCase().replaceAll("\\s+", "");
            
            if (normalizedColumn.startsWith("AVG(")) {
                String columnName = extractColumnFromFunction(column);
                double avg = calculateAverage(table, columnName);
                resultColumns.add(new JfrTable.Column("AVG(" + columnName + ")", me.bechberger.jfr.extended.table.CellType.NUMBER));
                resultRow.add(CellValue.of(avg));
            } else if (normalizedColumn.startsWith("COUNT(")) {
                long count = table.getRowCount();
                resultColumns.add(new JfrTable.Column("COUNT(*)", me.bechberger.jfr.extended.table.CellType.NUMBER));
                resultRow.add(CellValue.of(count));
            } else if (normalizedColumn.startsWith("SUM(")) {
                String columnName = extractColumnFromFunction(column);
                double sum = calculateSum(table, columnName);
                resultColumns.add(new JfrTable.Column("SUM(" + columnName + ")", me.bechberger.jfr.extended.table.CellType.NUMBER));
                resultRow.add(CellValue.of(sum));
            } else if (normalizedColumn.startsWith("MIN(")) {
                String columnName = extractColumnFromFunction(column);
                double min = calculateMin(table, columnName);
                resultColumns.add(new JfrTable.Column("MIN(" + columnName + ")", me.bechberger.jfr.extended.table.CellType.NUMBER));
                resultRow.add(CellValue.of(min));
            } else if (normalizedColumn.startsWith("MAX(")) {
                String columnName = extractColumnFromFunction(column);
                double max = calculateMax(table, columnName);
                resultColumns.add(new JfrTable.Column("MAX(" + columnName + ")", me.bechberger.jfr.extended.table.CellType.NUMBER));
                resultRow.add(CellValue.of(max));
            } else {
                // Non-aggregate column - use first row value or null if no rows
                if (table.getRowCount() > 0) {
                    int columnIndex = findColumnIndex(table, column);
                    if (columnIndex >= 0) {
                        resultColumns.add(table.getColumns().get(columnIndex));
                        resultRow.add(table.getRows().get(0).getCell(columnIndex));
                    }
                }
            }
        }
        
        JfrTable result = new StandardJfrTable(resultColumns);
        if (!resultRow.isEmpty()) {
            result.addRow(new JfrTable.Row(resultRow));
        }
        
        return result;
    }
    
    /**
     * Extract column name from a function call like "AVG(salary)" or "AVG ( salary )"
     */
    private String extractColumnFromFunction(String functionCall) {
        // Handle both "AVG(salary)" and "AVG ( salary )" patterns
        Pattern pattern = Pattern.compile("\\w+\\s*\\(\\s*([^)]+)\\s*\\)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(functionCall);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return functionCall;
    }
    
    /**
     * Calculate average of a numeric column
     */
    private double calculateAverage(JfrTable table, String columnName) {
        int columnIndex = findColumnIndex(table, columnName);
        if (columnIndex < 0) {
            return 0.0;
        }
        
        double sum = 0.0;
        int count = 0;
        
        for (JfrTable.Row row : table.getRows()) {
            CellValue cell = row.getCell(columnIndex);
            if (cell instanceof CellValue.NumberValue) {
                sum += ((CellValue.NumberValue) cell).value();
                count++;
            }
        }
        
        return count > 0 ? sum / count : 0.0;
    }
    
    /**
     * Calculate sum of a numeric column
     */
    private double calculateSum(JfrTable table, String columnName) {
        int columnIndex = findColumnIndex(table, columnName);
        if (columnIndex < 0) {
            return 0.0;
        }
        
        double sum = 0.0;
        
        for (JfrTable.Row row : table.getRows()) {
            CellValue cell = row.getCell(columnIndex);
            if (cell instanceof CellValue.NumberValue) {
                sum += ((CellValue.NumberValue) cell).value();
            }
        }
        
        return sum;
    }
    
    /**
     * Calculate minimum of a numeric column
     */
    private double calculateMin(JfrTable table, String columnName) {
        int columnIndex = findColumnIndex(table, columnName);
        if (columnIndex < 0) {
            return 0.0;
        }
        
        double min = Double.MAX_VALUE;
        boolean found = false;
        
        for (JfrTable.Row row : table.getRows()) {
            CellValue cell = row.getCell(columnIndex);
            if (cell instanceof CellValue.NumberValue) {
                double value = ((CellValue.NumberValue) cell).value();
                if (value < min) {
                    min = value;
                }
                found = true;
            }
        }
        
        return found ? min : 0.0;
    }
    
    /**
     * Calculate maximum of a numeric column
     */
    private double calculateMax(JfrTable table, String columnName) {
        int columnIndex = findColumnIndex(table, columnName);
        if (columnIndex < 0) {
            return 0.0;
        }
        
        double max = Double.MIN_VALUE;
        boolean found = false;
        
        for (JfrTable.Row row : table.getRows()) {
            CellValue cell = row.getCell(columnIndex);
            if (cell instanceof CellValue.NumberValue) {
                double value = ((CellValue.NumberValue) cell).value();
                if (value > max) {
                    max = value;
                }
                found = true;
            }
        }
        
        return found ? max : 0.0;
    }
    
    /**
     * Find the index of a column by name
     */
    private int findColumnIndex(JfrTable table, String columnName) {
        for (int i = 0; i < table.getColumns().size(); i++) {
            if (table.getColumns().get(i).name().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }
    
    private static class QueryInfo {
        String tableName;
        List<String> columns = new ArrayList<>();
        String orderByColumn;
        boolean ascending = true;
        int limit = -1; // -1 means no limit
        String whereClause;
        boolean hasAggregates = false;
    }
}
