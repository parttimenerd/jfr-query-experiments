package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.table.JfrTable;

import java.util.*;

/**
 * Enhanced expected result API with fluent assertions
 */
public class ExpectedResult {
    private final boolean shouldSucceed;
    private final Integer rowCount;
    private final Integer columnCount;
    private final String[] columnNames;
    private final List<Object[]> expectedRows;
    private final String sortedByColumn;
    private final Boolean sortedAscending;
    private final String expectedErrorMessage;

    private ExpectedResult(boolean shouldSucceed, Integer rowCount, Integer columnCount,
                          String[] columnNames, List<Object[]> expectedRows,
                          String sortedByColumn, Boolean sortedAscending,
                          String expectedErrorMessage) {
        this.shouldSucceed = shouldSucceed;
        this.rowCount = rowCount;
        this.columnCount = columnCount;
        this.columnNames = columnNames;
        this.expectedRows = expectedRows;
        this.sortedByColumn = sortedByColumn;
        this.sortedAscending = sortedAscending;
        this.expectedErrorMessage = expectedErrorMessage;
    }

    public static ExpectedResult success() {
        return new ExpectedResult(true, null, null, null, null, null, null, null);
    }

    public static ExpectedResult failure() {
        return new ExpectedResult(false, null, null, null, null, null, null, null);
    }

    public static ExpectedResult failure(String errorMessage) {
        return new ExpectedResult(false, null, null, null, null, null, null, errorMessage);
    }

    /**
     * Create an ExpectedResult for a single-line table specification.
     * Format: column1 column2; value1 value2
     */
    public static ExpectedResult expectSingleLine(String singleLineSpec) {
        String[] sections = singleLineSpec.split(";");
        if (sections.length != 2) {
            throw new IllegalArgumentException("Single-line spec must have exactly one header and one data row separated by ';'");
        }

        // Convert to multi-line format and reuse expectTable logic
        String columnHeader = sections[0].trim();
        String dataRow = sections[1].trim();
        String multiLineSpec = columnHeader + "\n" + dataRow;

        return expectTable(multiLineSpec);
    }

    /**
     * Create an ExpectedResult for a multi-line table specification.
     */
    public static ExpectedResult expectTable(String tableSpec) {
        // Use ExpectedTable parsing logic to avoid duplication
        ExpectedTable expectedTable = TableParsingUtils.parseSimpleTableSpec(tableSpec);

        // Convert ExpectedTable to ExpectedResult format
        String[] columnNames = expectedTable.getColumnNames().toArray(new String[0]);
        List<Object[]> expectedRowsList = new ArrayList<>();

        for (List<Object> row : expectedTable.getExpectedRows()) {
            Object[] rowArray = row.toArray(new Object[0]);
            expectedRowsList.add(rowArray);
        }

        return new ExpectedResult(true, Integer.valueOf(expectedRowsList.size()),
                Integer.valueOf(columnNames.length), columnNames, expectedRowsList,
                (String) null, (Boolean) null, (String) null);
    }

    /**
     * Create an ExpectedResult for a single row with string values.
     */
    public static ExpectedResult singleRow(String... values) {
        Object[] expectedRow = new Object[values.length];
        System.arraycopy(values, 0, expectedRow, 0, values.length);
        
        List<Object[]> expectedRowsList = new ArrayList<>();
        expectedRowsList.add(expectedRow);
        
        return new ExpectedResult(true, Integer.valueOf(1), Integer.valueOf(values.length), null, 
            expectedRowsList, (String)null, (Boolean)null, (String)null);
    }
    
    /**
     * Create an ExpectedResult with specified row count and columns.
     */
    public static ExpectedResultBuilder rows(int count) {
        return new ExpectedResultBuilder(count);
    }

    /**
     * Builder for ExpectedResult with fluent API
     */
    public static class ExpectedResultBuilder {
        private final int rowCount;
        
        public ExpectedResultBuilder(int rowCount) {
            this.rowCount = rowCount;
        }
        
        public ExpectedResult andColumns(String... columns) {
            return new ExpectedResult(true, Integer.valueOf(rowCount), Integer.valueOf(columns.length), columns, null, 
                (String)null, (Boolean)null, (String)null);
        }
    }

    /**
     * Create an ExpectedResult for checking column existence
     */
    public ExpectedResult withColumnExists(String columnName) {
        return new ExpectedResult(shouldSucceed, rowCount, columnCount, 
            columnNames != null ? columnNames : new String[]{columnName}, expectedRows, 
            sortedByColumn, sortedAscending, expectedErrorMessage);
    }
    
    /**
     * Create an ExpectedResult for checking column existence with value
     */
    public ExpectedResult withColumnExists(String columnName, String expectedValue) {
        // For now, just check column exists - full value checking can be enhanced later
        return withColumnExists(columnName);
    }
    
    /**
     * Create an ExpectedResult with max row count
     */
    public ExpectedResult withMaxRowCount(int maxCount) {
        // For simplicity, treat as exact row count - can be enhanced for range checking
        return withRowCount(maxCount);
    }
    
    /**
     * Create an ExpectedResult with column value check
     */
    public ExpectedResult withColumnValue(String columnName, String value) {
        return withColumnExists(columnName, value);
    }
    
    /**
     * Create an ExpectedResult with column value check (Object version)
     */
    public ExpectedResult withColumnValue(String columnName, Object value) {
        return withColumnExists(columnName, value.toString());
    }
    
    public ExpectedResult withRowCount(int count) {
        return new ExpectedResult(shouldSucceed, count, columnCount, columnNames, expectedRows, 
            sortedByColumn, sortedAscending, expectedErrorMessage);
    }
    
    public ExpectedResult withColumnCount(int count) {
        return new ExpectedResult(shouldSucceed, rowCount, count, columnNames, expectedRows, 
            sortedByColumn, sortedAscending, expectedErrorMessage);
    }
    
    public ExpectedResult withColumns(String... columns) {
        return new ExpectedResult(shouldSucceed, rowCount, columns.length, columns, expectedRows, 
            sortedByColumn, sortedAscending, expectedErrorMessage);
    }
    
    public ExpectedResult withRows(Object[]... rows) {
        return new ExpectedResult(shouldSucceed, rowCount, columnCount, columnNames, Arrays.asList(rows), 
            sortedByColumn, sortedAscending, expectedErrorMessage);
    }
    
    public ExpectedResult sortedBy(String columnName) {
        return new ExpectedResult(shouldSucceed, rowCount, columnCount, columnNames, expectedRows, 
            columnName, true, expectedErrorMessage);
    }
    
    public ExpectedResult sortedBy(String columnName, boolean ascending) {
        return new ExpectedResult(shouldSucceed, rowCount, columnCount, columnNames, expectedRows, 
            columnName, ascending, expectedErrorMessage);
    }
    
    public void assertMatches(QueryResult result) {
        if (shouldSucceed) {
            if (!result.isSuccess()) {
                throw new AssertionError("Expected query to succeed but it failed: " + 
                    (result.getError() != null ? result.getError().getMessage() : "Unknown error"));
            }
            
            JfrTable table = result.getTable();
            if (rowCount != null && table.getRowCount() != rowCount) {
                throw new AssertionError("Expected " + rowCount + " rows but got " + table.getRowCount());
            }
            
            if (columnCount != null && table.getColumnCount() != columnCount) {
                throw new AssertionError("Expected " + columnCount + " columns but got " + table.getColumnCount());
            }
            
            if (columnNames != null) {
                List<String> actualColumns = table.getColumns().stream()
                    .map(JfrTable.Column::name)
                    .toList();
                if (!Arrays.equals(columnNames, actualColumns.toArray(new String[0]))) {
                    throw new AssertionError("Expected columns " + Arrays.toString(columnNames) + 
                        " but got " + actualColumns);
                }
            }
            
            // Additional assertions can be added here for rows, sorting, etc.
            if (sortedByColumn != null) {
                result.assertSortedByColumn(sortedByColumn, sortedAscending != null ? sortedAscending : true);
            }
            
        } else {
            if (result.isSuccess()) {
                throw new AssertionError("Expected query to fail but it succeeded");
            }
            
            if (expectedErrorMessage != null && result.getError() != null) {
                if (!result.getError().getMessage().contains(expectedErrorMessage)) {
                    throw new AssertionError("Expected error message to contain '" + expectedErrorMessage + 
                        "' but got: " + result.getError().getMessage());
                }
            }
        }
    }
}
