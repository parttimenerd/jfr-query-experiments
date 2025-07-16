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
        List<Boolean> hasExplicitType = new ArrayList<>();

        for (String columnSpec : columnSpecs) {
            String[] parts = columnSpec.trim().split("\\s+");
            if (parts.length < 1) {
                throw new IllegalArgumentException("Column specification cannot be empty: " + columnSpec);
            }
            String columnName = parts[0];
            boolean explicitType = parts.length > 1;
            // Default to STRING if no type specified, but we'll try to infer from data
            CellType columnType = explicitType ? TableParsingUtils.parseColumnType(parts[1]) : CellType.STRING;
            columnNames.add(columnName);
            columnTypes.add(columnType);
            hasExplicitType.add(explicitType);
        }

        // Parse data rows first to enable type inference
        List<List<String>> rawRowValues = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) continue;

            String[] values = line.split("\\|");
            List<String> rowStringValues = new ArrayList<>();
            for (String value : values) {
                rowStringValues.add(value.trim());
            }
            rawRowValues.add(rowStringValues);
        }

        // Infer types for columns without explicit types using first data row
        if (!rawRowValues.isEmpty()) {
            List<String> firstDataRow = rawRowValues.get(0);
            for (int j = 0; j < columnTypes.size() && j < firstDataRow.size(); j++) {
                if (!hasExplicitType.get(j)) {
                    // Infer type from first data value
                    String firstValue = firstDataRow.get(j);
                    if (!firstValue.equalsIgnoreCase("null") && !firstValue.isEmpty()) {
                        CellType inferredType = TableParsingUtils.inferColumnType(firstValue);
                        columnTypes.set(j, inferredType);
                    }
                }
            }
        }

        // Parse data rows with correct types
        List<List<Object>> expectedRows = new ArrayList<>();
        for (List<String> stringRow : rawRowValues) {
            List<Object> rowValues = new ArrayList<>();
            for (int j = 0; j < stringRow.size(); j++) {
                String typeStr = columnTypes.size() > j ? columnTypes.get(j).name() : "STRING";
                rowValues.add(TableParsingUtils.parseValue(stringRow.get(j), typeStr));
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

        // Handle string representation of arrays (when expected is a string that looks like an array)
        if (expected instanceof String expectedStr && actual instanceof CellValue.ArrayValue arrayValue) {
            // Try to parse the expected string as an array
            if (expectedStr.startsWith("[") && expectedStr.endsWith("]")) {
                try {
                    Object parsedExpected = me.bechberger.jfr.extended.engine.framework.TableParsingUtils.parseArrayValue(expectedStr);
                    if (parsedExpected instanceof List<?> expectedList) {
                        return compareArrays(expectedList, arrayValue);
                    }
                } catch (Exception e) {
                    // Failed to parse, continue with other comparisons
                }
            }
        }

        // Handle array comparisons
        if (expected instanceof List<?> expectedList && actual instanceof CellValue.ArrayValue arrayValue) {
            return compareArrays(expectedList, arrayValue);
        }

        // Handle numeric comparisons with tolerance
        if (expected instanceof Number && actualValue instanceof Number) {
            double expectedDouble = ((Number) expected).doubleValue();
            double actualDouble = ((Number) actualValue).doubleValue();
            return Math.abs(expectedDouble - actualDouble) < 1e-9;
        }

        return false;
    }
    
    private boolean compareArrays(List<?> expectedList, CellValue.ArrayValue arrayValue) {
        List<CellValue> actualElements = arrayValue.elements();
        if (expectedList.size() != actualElements.size()) {
            return false;
        }
        
        for (int i = 0; i < expectedList.size(); i++) {
            Object expectedElement = expectedList.get(i);
            CellValue actualElement = actualElements.get(i);
            if (!valuesEqual(expectedElement, actualElement)) {
                return false;
            }
        }
        return true;
    }
}
