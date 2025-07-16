package me.bechberger.jfr.extended.table;

import java.util.List;
import java.util.Optional;

/**
 * A JfrTable implementation that represents the results of multi-statement query execution.
 * 
 * This table delegates all table operations to the last result table, but provides access
 * to all top-level query results through the all() method.
 * 
 * Used for multi-statement queries where we want to:
 * - Return the last table result as the primary result
 * - Provide access to all top-level query results (excluding subqueries and variable assignments)
 * - Maintain compatibility with existing single-table usage patterns
 */
public class MultiResultTable implements JfrTable {
    
    private final JfrTable lastResult;
    private final List<JfrTable> allResults;
    
    /**
     * Create a MultiResultTable with the given results.
     * 
     * @param allResults List of all top-level query results
     * @param lastResult The last result (should be the last element of allResults)
     */
    public MultiResultTable(List<JfrTable> allResults, JfrTable lastResult) {
        this.allResults = List.copyOf(allResults);
        this.lastResult = lastResult;
    }
    
    /**
     * Convenience constructor for when the last result is the last element of allResults
     */
    public MultiResultTable(List<JfrTable> allResults) {
        this.allResults = List.copyOf(allResults);
        this.lastResult = allResults.isEmpty() ? new StandardJfrTable(List.of()) : allResults.get(allResults.size() - 1);
    }
    
    /**
     * Get all result tables from multi-statement query execution
     */
    public List<JfrTable> all() {
        return allResults;
    }
    
    // Delegate all other methods to the last result
    
    @Override
    public List<Column> getColumns() {
        return lastResult.getColumns();
    }
    
    @Override
    public List<Row> getRows() {
        return lastResult.getRows();
    }
    
    @Override
    public Optional<Column> getColumn(String name) {
        return lastResult.getColumn(name);
    }
    
    @Override
    public int getColumnIndex(String name) {
        return lastResult.getColumnIndex(name);
    }
    
    @Override
    public CellValue getCell(int rowIndex, String columnName) {
        return lastResult.getCell(rowIndex, columnName);
    }
    
    @Override
    public CellValue getCell(int rowIndex, int columnIndex) {
        return lastResult.getCell(rowIndex, columnIndex);
    }
    
    @Override
    public List<CellValue> getColumnValues(String columnName) {
        return lastResult.getColumnValues(columnName);
    }
    
    @Override
    public int getRowCount() {
        return lastResult.getRowCount();
    }
    
    @Override
    public int getColumnCount() {
        return lastResult.getColumnCount();
    }
    
    @Override
    public void addRow(Row row) {
        lastResult.addRow(row);
    }
    
    @Override
    public void addRow(CellValue... cells) {
        lastResult.addRow(cells);
    }
    
    @Override
    public JfrTable filter(RowPredicate predicate) {
        return lastResult.filter(predicate);
    }
    
    @Override
    public JfrTable select(String... columnNames) {
        return lastResult.select(columnNames);
    }
    
    @Override
    public JfrTable sort(String columnName, boolean ascending) {
        return lastResult.sort(columnName, ascending);
    }
    
    @Override
    public JfrTable copy() {
        return lastResult.copy();
    }
    
    @Override
    public String toString() {
        return lastResult.toString();
    }
}
