package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.Lexer;
import me.bechberger.jfr.extended.Token;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellType;

import java.util.*;

/**
 * Unified test framework for JFR Extended Query Language testing.
 * 
 * This framework provides:
 * 1. Clean table building API
 * 2. Enhanced ExpectedResult API with fluent assertions
 * 3. Comprehensive query parsing and execution
 * 4. Multi-statement query support with variable handling
 * 5. Performance and caching test utilities
 * 6. Specialized JFR event table builders
 * 
 * @author Unified Testing Framework
 * @since 3.0
 */
public class QueryTestFramework {
    
    private final QueryEvaluator evaluator;
    private final MockRawJfrQueryExecutor mockExecutor;
    private final Map<String, JfrTable> mockTables = new HashMap<>();
    private final Map<String, Object> sessionVariables = new HashMap<>();
    
    public QueryTestFramework() {
        this.mockExecutor = new MockRawJfrQueryExecutor();
        this.evaluator = new QueryEvaluator(mockExecutor);
        
        // Set up execution context with debug mode enabled for cache testing
        ExecutionContext context = new ExecutionContext(true);
        evaluator.setContext(context);
        
        setupDefaultMockTables();
    }
    
    /**
     * Parse a query string into a QueryNode
     */
    public QueryNode parseQuery(String queryString) {
        try {
            Lexer lexer = new Lexer(queryString);
            List<Token> tokens = lexer.tokenize();
            Parser parser = new Parser(tokens, queryString);
            ProgramNode program = parser.parse();
            
            if (!program.statements().isEmpty() && program.statements().get(0) instanceof QueryNode) {
                return (QueryNode) program.statements().get(0);
            } else {
                throw new IllegalArgumentException("Query string did not parse to a QueryNode: " + queryString);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse query: " + queryString, e);
        }
    }
    
    /**
     * Execute a query string and return the result
     */
    public QueryResult executeQuery(String queryString) {
        try {
            JfrTable result = evaluator.query(queryString);
            return new QueryResult(result, true, null);
        } catch (Exception e) {
            return new QueryResult(null, false, e);
        }
    }
    
    /**
     * Execute multi-statement queries with variable support
     */
    public List<QueryResult> executeMultiStatementQuery(String multiStatementQuery) {
        List<QueryResult> results = new ArrayList<>();
        String[] statements = multiStatementQuery.split(";");
        
        for (String statement : statements) {
            statement = statement.trim();
            if (statement.isEmpty()) continue;
            
            try {
                if (statement.contains(":=")) {
                    // Variable assignment - simplified for mock testing
                    String[] parts = statement.split(":=", 2);
                    String varName = parts[0].trim().replace("var ", "");
                    String expression = parts[1].trim();
                    
                    // Simple expression evaluation for testing
                    Object value = parseSimpleExpression(expression);
                    sessionVariables.put(varName, value);
                    results.add(new QueryResult(null, true, null));
                } else {
                    // Regular query
                    QueryResult result = executeQuery(statement);
                    results.add(result);
                }
            } catch (Exception e) {
                results.add(new QueryResult(null, false, e));
            }
        }
        
        return results;
    }
    
    /**
     * Create specialized table builders for JFR event types
     */
    public CustomTableBuilder withGarbageCollectionTable() {
        return customTable("GarbageCollection")
            .withStringColumn("name")
            .withDurationColumn("duration")
            .withTimestampColumn("startTime") 
            .withMemorySizeColumn("heapUsedBefore")
            .withMemorySizeColumn("heapUsedAfter")
            .withStringColumn("cause");
    }
    
    public CustomTableBuilder withExecutionSampleTable() {
        return customTable("ExecutionSample")
            .withStringColumn("thread")
            .withStringColumn("stackTrace")
            .withTimestampColumn("timestamp")
            .withStringColumn("state");
    }
    
    public CustomTableBuilder withAllocationTable() {
        return customTable("Allocation")
            .withStringColumn("thread")
            .withStringColumn("type")
            .withMemorySizeColumn("size")
            .withTimestampColumn("timestamp")
            .withStringColumn("stackTrace");
    }
    
    public CustomTableBuilder withJfrEventTable(String eventType) {
        return customTable(eventType)
            .withTimestampColumn("timestamp")
            .withDurationColumn("duration")
            .withStringColumn("thread");
    }
    
    /**
     * Create a custom table builder
     */
    public CustomTableBuilder customTable(String tableName) {
        return new CustomTableBuilder(tableName, this);
    }
    
    /**
     * Create a mock table builder
     */
    public TableBuilder mockTable(String tableName) {
        return new TableBuilder(tableName, this);
    }
    
    /**
     * Unified mock table creation method that automatically detects the format.
     * 
     * Supports multiple formats:
     * 1. Multi-line with pipe separators (auto type inference):
     *    "name | age | score\nAlice | 25 | 95.5\nBob | 30 | 87.2"
     * 
     * 2. Multi-line with explicit types:
     *    "name STRING | age NUMBER | score FLOAT\nAlice | 25 | 95.5\nBob | 30 | 87.2"
     * 
     * 3. Single-line with semicolon separators:
     *    "name age score; Alice 25 95.5; Bob 30 87.2"
     * 
     * 4. Single-line with explicit types (prefixed with table name):
     *    "TableName: name STRING age NUMBER; Alice 25; Bob 30"
     */
    public QueryTestFramework createTable(String tableName, String tableSpec) {
        tableSpec = tableSpec.trim();
        
        // Check if it's a single-line format with explicit types (contains colon)
        if (tableSpec.contains(":") && !tableSpec.contains("\n")) {
            return mockTableWithTypes(tableSpec);
        }
        
        // Check if it's a single-line format with semicolons
        if (tableSpec.contains(";") && !tableSpec.contains("\n")) {
            return mockTableSingleLine(tableName, tableSpec);
        }
        
        // Check if it's multi-line format with explicit types
        if (tableSpec.contains("\n") && containsExplicitTypes(tableSpec)) {
            return mockTableMultiLine(tableName, tableSpec);
        }
        
        // Default: multi-line format with automatic type inference
        return mockTable(tableName, tableSpec);
    }
    
    /**
     * Create a table from a multi-line table specification with explicit types.
     * Format:
     * avg NUMBER | sum NUMBER
     * 10 | 100
     * 1000 | 100
     */
    public QueryTestFramework mockTableMultiLine(String tableName, String tableSpec) {
        String[] lines = tableSpec.trim().split("\n");
        if (lines.length < 1) {
            throw new IllegalArgumentException("Table specification must have at least a header line");
        }
        
        // Parse header line for columns and types
        String headerLine = lines[0].trim();
        String[] columnSpecs = headerLine.split("\\|");
        TableBuilder builder = mockTable(tableName);
        
        for (String columnSpec : columnSpecs) {
            String[] parts = columnSpec.trim().split("\\s+");
            if (parts.length < 1) {
                throw new IllegalArgumentException("Column specification cannot be empty: " + columnSpec);
            }
            String columnName = parts[0];
            // Default to STRING if no type specified
            CellType columnType = parts.length > 1 ? TableParsingUtils.parseColumnType(parts[1]) : CellType.STRING;
            builder.withColumn(columnName, columnType);
        }
        
        // Parse data rows
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) continue;
            
            String[] values = line.split("\\|");
            Object[] rowValues = new Object[values.length];
            for (int j = 0; j < values.length; j++) {
                String typeStr = columnSpecs.length > j ? 
                    (columnSpecs[j].trim().split("\\s+").length > 1 ? 
                        columnSpecs[j].trim().split("\\s+")[1] : "STRING") : "STRING";
                rowValues[j] = TableParsingUtils.parseValue(values[j].trim(), typeStr);
            }
            builder.withRow(rowValues);
        }
        
        return builder.build();
    }
    
    /**
     * Check if the table specification contains explicit type declarations
     */
    private boolean containsExplicitTypes(String tableSpec) {
        String firstLine = tableSpec.split("\n")[0];
        String[] columns = firstLine.split("\\|");
        
        for (String column : columns) {
            String[] parts = column.trim().split("\\s+");
            if (parts.length > 1) {
                String potentialType = parts[1].toUpperCase();
                if (potentialType.matches("STRING|NUMBER|FLOAT|BOOLEAN|TIMESTAMP|DURATION|MEMORY_SIZE|ARRAY|NULL")) {
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * Create a table from a single-line table specification using space separators.
     */
    public QueryTestFramework mockTableSingleLine(String tableName, String singleLineSpec) {
        String[] sections = singleLineSpec.split(";");
        if (sections.length < 1) {
            throw new IllegalArgumentException("Table specification must have at least column definitions");
        }
        
        // Parse header (column names, types inferred from first data row)
        String headerSection = sections[0].trim();
        String[] columnNames = headerSection.split("\\s+");
        
        // Collect all data rows first to infer types
        List<String[]> dataRows = new ArrayList<>();
        for (int i = 1; i < sections.length; i++) {
            String rowSection = sections[i].trim();
            if (rowSection.isEmpty()) continue;
            dataRows.add(rowSection.split("\\s+"));
        }
        
        // Infer column types from first data row
        String[] columnTypes = new String[columnNames.length];
        if (!dataRows.isEmpty()) {
            String[] firstRow = dataRows.get(0);
            for (int j = 0; j < columnNames.length; j++) {
                columnTypes[j] = TableParsingUtils.inferType(firstRow[j]);
            }
        } else {
            // Default all to STRING if no data
            Arrays.fill(columnTypes, "STRING");
        }
        
        // Build table
        TableBuilder builder = mockTable(tableName);
        for (int i = 0; i < columnNames.length; i++) {
            CellType columnType = TableParsingUtils.parseColumnType(columnTypes[i]);
            builder.withColumn(columnNames[i], columnType);
        }
        
        // Add data rows
        for (String[] values : dataRows) {
            Object[] rowValues = new Object[values.length];
            for (int j = 0; j < values.length && j < columnTypes.length; j++) {
                rowValues[j] = TableParsingUtils.parseValue(values[j], columnTypes[j]);
            }
            builder.withRow(rowValues);
        }
        
        return builder.build();
    }

    /**
     * Create a table from a single-line table specification with explicit types.
     */
    public QueryTestFramework mockTableWithTypes(String singleLineSpec) {
        // Parse table name
        String[] mainParts = singleLineSpec.split(":", 2);
        if (mainParts.length != 2) {
            throw new IllegalArgumentException("Single-line table spec must start with 'TableName:'");
        }
        
        String tableName = mainParts[0].trim();
        String tableContent = mainParts[1].trim();
        
        // Split by semicolons to get header and rows
        String[] sections = tableContent.split(";");
        if (sections.length < 1) {
            throw new IllegalArgumentException("Table specification must have at least column definitions");
        }
        
        // Parse header (column definitions)
        String headerSection = sections[0].trim();
        String[] headerTokens = headerSection.split("\\s+");
        if (headerTokens.length % 2 != 0) {
            throw new IllegalArgumentException("Column definitions must be pairs of 'name TYPE'");
        }
        
        TableBuilder builder = mockTable(tableName);
        String[] columnTypes = new String[headerTokens.length / 2];
        
        for (int i = 0; i < headerTokens.length; i += 2) {
            String columnName = headerTokens[i];
            String columnTypeStr = headerTokens[i + 1];
            CellType columnType = TableParsingUtils.parseColumnType(columnTypeStr);
            builder.withColumn(columnName, columnType);
            columnTypes[i / 2] = columnTypeStr;
        }
        
        // Parse data rows
        for (int i = 1; i < sections.length; i++) {
            String rowSection = sections[i].trim();
            if (rowSection.isEmpty()) continue;
            
            String[] values = rowSection.split("\\s+");
            Object[] rowValues = new Object[values.length];
            for (int j = 0; j < values.length && j < columnTypes.length; j++) {
                rowValues[j] = TableParsingUtils.parseValue(values[j], columnTypes[j]);
            }
            builder.withRow(rowValues);
        }
        
        return builder.build();
    }
    
    /**
     * Create a table from a multi-line table specification with automatic type inference.
     */
    public QueryTestFramework mockTable(String tableName, String tableSpec) {
        String[] lines = tableSpec.trim().split("\n");
        if (lines.length < 1) {
            throw new IllegalArgumentException("Table specification must have at least a header line");
        }
        
        // Parse header line for column names only
        String headerLine = lines[0].trim();
        String[] columnNames = headerLine.split("\\|");
        for (int i = 0; i < columnNames.length; i++) {
            columnNames[i] = columnNames[i].trim();
        }
        
        TableBuilder builder = mockTable(tableName);
        
        // If there's no data, default all columns to STRING type
        if (lines.length < 2) {
            for (String columnName : columnNames) {
                builder.withColumn(columnName, CellType.STRING);
            }
            return builder.build();
        }
        
        // Parse first data row to infer types
        String firstDataLine = lines[1].trim();
        String[] firstRowValues = firstDataLine.split("\\|");
        
        // Add columns with inferred types
        for (int i = 0; i < columnNames.length; i++) {
            String columnName = columnNames[i];
            CellType columnType = CellType.STRING; // Default
            
            if (i < firstRowValues.length) {
                String sampleValue = firstRowValues[i].trim();
                columnType = TableParsingUtils.parseColumnType(TableParsingUtils.inferType(sampleValue));
            }
            
            builder.withColumn(columnName, columnType);
        }
        
        // Parse all data rows
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) continue;
            
            String[] values = line.split("\\|");
            Object[] rowValues = new Object[values.length];
            for (int j = 0; j < values.length && j < columnNames.length; j++) {
                String typeStr = j < firstRowValues.length ? TableParsingUtils.inferType(firstRowValues[j].trim()) : "STRING";
                rowValues[j] = TableParsingUtils.parseValue(values[j].trim(), typeStr);
            }
            builder.withRow(rowValues);
        }
        
        return builder.build();
    }
    
    /**
     * Simple expression parser for testing (just parses literals)
     */
    private Object parseSimpleExpression(String expression) {
        expression = expression.trim();
        if (expression.startsWith("\"") && expression.endsWith("\"")) {
            return expression.substring(1, expression.length() - 1);
        }
        try {
            return Long.parseLong(expression);
        } catch (NumberFormatException e) {
            try {
                return Double.parseDouble(expression);
            } catch (NumberFormatException e2) {
                return expression; // Return as string if all else fails
            }
        }
    }
    
    /**
     * Set up default mock tables for testing
     */
    private void setupDefaultMockTables() {
        // Create basic test tables using the new API with automatic type inference
        mockTable("TestAggregateData", """
            avg | sum
            10.5 | 100
            1000.2 | 200
            """);
            
        // Example with different data types
        mockTable("TestMixedTypes", """
            name | value | active | timestamp
            Alice | 42.5 | true | 1609459200000
            Bob | 17 | false | 1609545600000
            Charlie | 99.9 | true | 1609632000000
            """);
    }
    
    /**
     * Register a table with the mock executor
     */
    public void registerTable(String name, JfrTable table) {
        mockTables.put(name, table);
        mockExecutor.registerTable(name, table);
    }
    
    /**
     * Create an ExpectedResult for query assertions
     */
    public ExpectedResult expectSuccess() {
        return ExpectedResult.success();
    }
    
    public ExpectedResult expectFailure() {
        return ExpectedResult.failure();
    }
    
    public ExpectedResult expectFailure(String errorMessage) {
        return ExpectedResult.failure(errorMessage);
    }
    
    /**
     * Assert that a query result matches expectations
     */
    public void assertThat(QueryResult result, ExpectedResult expected) {
        expected.assertMatches(result);
    }
    
    /**
     * Create an ExpectedTable for table content verification using multi-line format.
     */
    public ExpectedTable expectTable(String tableSpec) {
        return ExpectedTable.fromMultiLine(tableSpec);
    }
    
    /**
     * Create an ExpectedTable for table content verification using single-line format.
     */
    public ExpectedTable expectTableSingleLine(String singleLineSpec) {
        return ExpectedTable.fromSingleLine(singleLineSpec);
    }
    
    // ================= HELPER METHODS FOR TESTING =================
    
    /**
     * Helper method to execute a query and validate it succeeded
     */
    public QueryResult executeAndAssertSuccess(String query) {
        var result = executeQuery(query);
        if (!result.isSuccess()) {
            throw new AssertionError("Query should succeed: " + query + 
                ". Error: " + (result.getError() != null ? result.getError().getMessage() : "Unknown"));
        }
        return result;
    }
    
    /**
     * Helper method to execute a query and validate the result with expectTable
     */
    public void executeAndExpectTable(String query, String expectedTableSpec) {
        var result = executeAndAssertSuccess(query);
        
        ExpectedTable expected = expectTable(expectedTableSpec);
        try {
            expected.assertMatches(result.getTable());
        } catch (Exception e) {
            throw new AssertionError("Table validation failed for query: " + query + 
                "\nExpected: " + expectedTableSpec + 
                "\nActual: " + result.toString(), e);
        }
    }
    
    /**
     * Helper method to create a table and validate a query result in one step
     */
    public void createTableAndExpect(String tableName, String tableSpec, String query, String expectedResult) {
        createTable(tableName, tableSpec);
        executeAndExpectTable(query, expectedResult);
    }
    
    /**
     * Helper method to test sorting on a column
     */
    public void testSortingOnColumn(String tableName, String columnName, boolean ascending) {
        String direction = ascending ? "ASC" : "DESC";
        String query = "@SELECT * FROM " + tableName + " ORDER BY " + columnName + " " + direction;
        
        var result = executeAndAssertSuccess(query);
        try {
            result.assertSortedByColumn(columnName, ascending);
        } catch (AssertionError e) {
            throw new AssertionError("Column should be sorted correctly: " + columnName + " " + direction + 
                "\nQuery: " + query + "\nResult: " + result.toString(), e);
        }
    }
    
    /**
     * Helper method to test sorting on multiple columns
     */
    public void testSortingOnColumns(String tableName, String... columnNames) {
        for (String columnName : columnNames) {
            testSortingOnColumn(tableName, columnName, true);  // Test ascending
            testSortingOnColumn(tableName, columnName, false); // Test descending
        }
    }
    
    /**
     * Helper method to validate error expectations
     */
    public void expectQueryFailure(String query, String expectedErrorMessage) {
        var result = executeQuery(query);
        if (result.isSuccess()) {
            throw new AssertionError("Query should fail but succeeded: " + query);
        }
        
        if (result.getError() == null) {
            throw new AssertionError("Error should be present for failed query: " + query);
        }
        
        if (expectedErrorMessage != null) {
            assertThat(result, expectFailure(expectedErrorMessage));
        }
    }
    
    /**
     * Helper method to test column selection queries
     */
    public void testColumnSelection(String tableName, String[] columns, String expectedResult) {
        String columnList = String.join(", ", columns);
        String query = "@SELECT " + columnList + " FROM " + tableName;
        executeAndExpectTable(query, expectedResult);
    }
    
    /**
     * Helper method to test multiple query variations on the same table
     */
    public void testQueryVariations(String tableName, String... queries) {
        for (String query : queries) {
            var result = executeQuery(query);
            if (!result.isSuccess()) {
                throw new AssertionError("Query should succeed: " + query + 
                    ". Error: " + (result.getError() != null ? result.getError().getMessage() : "Unknown"));
            }
        }
    }
    
    /**
     * Helper method to create a table with sample data for testing
     */
    public QueryTestFramework createSampleTable(String tableName, String type) {
        return switch (type.toLowerCase()) {
            case "users" -> {
                createTable(tableName, """
                    name | age | active
                    Alice | 25 | true
                    Bob | 30 | false
                    Charlie | 20 | true
                    """);
                yield this;
            }
            case "sales" -> {
                createTable(tableName, """
                    product | amount | date
                    Laptop | 1200.50 | 1609459200000
                    Mouse | 25.99 | 1609545600000
                    Keyboard | 75.00 | 1609632000000
                    """);
                yield this;
            }
            case "mixed_types" -> {
                createTable(tableName, """
                    name STRING | count NUMBER | ratio FLOAT | enabled BOOLEAN | timestamp TIMESTAMP
                    Test1 | 100 | 1.5 | true | 1609459200000
                    Test2 | 200 | 2.5 | false | 1609545600000
                    """);
                yield this;
            }
            default -> throw new IllegalArgumentException("Unknown sample table type: " + type);
        };
    }
    
    /**
     * Helper method to validate table structure (columns and types)
     */
    public void validateTableStructure(String tableName, String... expectedColumns) {
        var result = executeAndAssertSuccess("@SELECT * FROM " + tableName);
        var table = result.getTable();
        
        if (table.getColumnCount() != expectedColumns.length) {
            throw new AssertionError("Expected " + expectedColumns.length + " columns but got " + 
                table.getColumnCount() + " for table: " + tableName);
        }
        
        for (int i = 0; i < expectedColumns.length; i++) {
            String actualColumnName = table.getColumns().get(i).name();
            if (!actualColumnName.equals(expectedColumns[i])) {
                throw new AssertionError("Expected column " + i + " to be '" + expectedColumns[i] + 
                    "' but was '" + actualColumnName + "' for table: " + tableName);
            }
        }
    }
    
    /**
     * Helper method to test that a table has the expected number of rows
     */
    public void validateRowCount(String tableName, int expectedRowCount) {
        var result = executeAndAssertSuccess("@SELECT * FROM " + tableName);
        int actualRowCount = result.getRowCount();
        
        if (actualRowCount != expectedRowCount) {
            throw new AssertionError("Expected " + expectedRowCount + " rows but got " + 
                actualRowCount + " for table: " + tableName + ". Result: " + result.toString());
        }
    }
}
