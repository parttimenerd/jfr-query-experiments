package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;

import java.util.*;

/**
 * ExpectedTable for table content verification
 */
public class ExpectedTable {
    private final List<String> columnNames;
    private final List<CellType> columnTypes;
    private final List<List<Object>> expectedRows;

    public ExpectedTable(List<String> columnNames, List<CellType> columnTypes, List<List<Object>> expectedRows) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.expectedRows = expectedRows;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<CellType> getColumnTypes() {
        return columnTypes;
    }

    public List<List<Object>> getExpectedRows() {
        return expectedRows;
    }

    /**
     * Create ExpectedTable from multi-line format:
     * avg NUMBER | sum NUMBER
     * 10 | 100
     * 1000 | 100
     */
    public static ExpectedTable fromMultiLine(String tableSpec) {
        String[] lines = tableSpec.trim().split("\n");
        if (lines.length < 1) {
            throw new IllegalArgumentException("Table specification must have at least a header line");
        }

        // Parse header line for columns and types
        String headerLine = lines[0].trim();
        String[] columnSpecs = headerLine.split("\\|");
        List<String> columnNames = new ArrayList<>();
        List<CellType> columnTypes = new ArrayList<>();

        for (String columnSpec : columnSpecs) {
            String[] parts = columnSpec.trim().split("\\s+");
            if (parts.length < 1) {
                throw new IllegalArgumentException("Column specification cannot be empty: " + columnSpec);
            }
            String columnName = parts[0];
            // Default to STRING if no type specified
            CellType columnType = parts.length > 1 ? TableParsingUtils.parseColumnType(parts[1]) : CellType.STRING;
            columnNames.add(columnName);
            columnTypes.add(columnType);
        }

        // Parse data rows
        List<List<Object>> expectedRows = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) continue;

            String[] values = line.split("\\|");
            List<Object> rowValues = new ArrayList<>();
            for (int j = 0; j < values.length; j++) {
                String typeStr = columnTypes.size() > j ? columnTypes.get(j).name() : "STRING";
                rowValues.add(TableParsingUtils.parseValue(values[j].trim(), typeStr));
            }
            expectedRows.add(rowValues);
        }

        return new ExpectedTable(columnNames, columnTypes, expectedRows);
    }

    /**
     * Create ExpectedTable from single-line format:
     * avg value; 34.0 true; 50.2 false
     */
    public static ExpectedTable fromSingleLine(String singleLineSpec) {
        String[] sections = singleLineSpec.split(";");
        if (sections.length < 1) {
            throw new IllegalArgumentException("Table specification must have at least column definitions");
        }

        // Parse header (column names, types inferred from first data row)
        String headerSection = sections[0].trim();
        String[] columnNames = headerSection.split("\\s+");

        // Collect all data rows first to infer types
        List<List<String>> dataRows = new ArrayList<>();
        for (int i = 1; i < sections.length; i++) {
            String rowSection = sections[i].trim();
            if (rowSection.isEmpty()) continue;
            dataRows.add(Arrays.asList(rowSection.split("\\s+")));
        }

        // Infer column types from first data row
        List<CellType> columnTypes = new ArrayList<>();
        if (!dataRows.isEmpty()) {
            List<String> firstRow = dataRows.get(0);
            for (int j = 0; j < columnNames.length && j < firstRow.size(); j++) {
                columnTypes.add(TableParsingUtils.parseColumnType(TableParsingUtils.inferType(firstRow.get(j))));
            }
        } else {
            // Default all to STRING if no data
            for (int i = 0; i < columnNames.length; i++) {
                columnTypes.add(CellType.STRING);
            }
        }

        // Parse data rows
        List<List<Object>> expectedRows = new ArrayList<>();
        for (List<String> stringRow : dataRows) {
            List<Object> rowValues = new ArrayList<>();
            for (int j = 0; j < stringRow.size() && j < columnTypes.size(); j++) {
                rowValues.add(TableParsingUtils.parseValue(stringRow.get(j), columnTypes.get(j).name()));
            }
            expectedRows.add(rowValues);
        }

        return new ExpectedTable(Arrays.asList(columnNames), columnTypes, expectedRows);
    }

    /**
     * Verify that a JfrTable matches this expected table
     */
    public void assertMatches(JfrTable actualTable) {
        // Check column count
        if (actualTable.getColumnCount() != columnNames.size()) {
            throw new AssertionError("Expected " + columnNames.size() + " columns but got " + actualTable.getColumnCount());
        }

        // Check column names
        List<String> actualColumnNames = actualTable.getColumns().stream()
                .map(JfrTable.Column::name)
                .toList();
        if (!columnNames.equals(actualColumnNames)) {
            throw new AssertionError("Expected columns " + columnNames + " but got " + actualColumnNames);
        }

        // Check row count
        if (actualTable.getRowCount() != expectedRows.size()) {
            throw new AssertionError("Expected " + expectedRows.size() + " rows but got " + actualTable.getRowCount());
        }

        // Check row contents
        for (int i = 0; i < expectedRows.size(); i++) {
            List<Object> expectedRow = expectedRows.get(i);
            JfrTable.Row actualRow = actualTable.getRows().get(i);

            for (int j = 0; j < expectedRow.size() && j < actualRow.getCells().size(); j++) {
                Object expectedValue = expectedRow.get(j);
                CellValue actualValue = actualRow.getCells().get(j);

                if (!valuesEqual(expectedValue, actualValue)) {
                    throw new AssertionError("Row " + i + ", column " + j +
                            ": expected " + expectedValue + " but got " + actualValue.getValue());
                }
            }
        }
    }

    private boolean valuesEqual(Object expected, CellValue actual) {
        if (expected == null) {
            return actual instanceof CellValue.NullValue;
        }

        Object actualValue = actual.getValue();
        if (expected.equals(actualValue)) {
            return true;
        }

        // Handle numeric comparisons with tolerance
        if (expected instanceof Number && actualValue instanceof Number) {
            double expectedDouble = ((Number) expected).doubleValue();
            double actualDouble = ((Number) actualValue).doubleValue();
            return Math.abs(expectedDouble - actualDouble) < 1e-9;
        }

        return false;
    }
}
