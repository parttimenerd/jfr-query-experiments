package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.Parser;
import me.bechberger.jfr.extended.Lexer;
import me.bechberger.jfr.extended.Token;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.SingleCellTable;
import me.bechberger.jfr.extended.plan.QueryPlanExecutor;

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
    
    private final QueryPlanExecutor executor;
    private final MockRawJfrQueryExecutor mockExecutor;
    private final Map<String, JfrTable> mockTables = new HashMap<>();
    private final Map<String, Object> sessionVariables = new HashMap<>();
    
    public QueryTestFramework() {
        this.mockExecutor = new MockRawJfrQueryExecutor();
        this.executor = new QueryPlanExecutor(mockExecutor);
        
        setupDefaultMockTables();
    }
    
    /**
     * Get the QueryPlanExecutor for accessing execution context and traces
     */
    public QueryPlanExecutor getExecutor() {
        return executor;
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
            JfrTable result = executor.query(queryString);
            return new QueryResult(result, true, null);
        } catch (Exception e) {
            return new QueryResult(null, false, e);
        }
    }
    
    /**
     * Execute a query and print execution trace for debugging
     */
    public QueryResult executeQueryWithTrace(String queryString) {
        QueryResult result = executeQuery(queryString);
        
        // Try to get the execution trace if available
        // Note: This is a simplified approach since we don't have direct access to the context
        System.out.println("=== Query Execution Debug ===");
        System.out.println("Query: " + queryString);
        System.out.println("Success: " + result.isSuccess());
        if (!result.isSuccess()) {
            System.out.println("Error: " + result.getError());
        }
        System.out.println("===============================");
        
        return result;
    }
    
    /**
     * Execute a multi-statement query and return results for each statement.
     * Uses the QueryPlanExecutor's minimal API for proper lazy variable handling.
     * 
     * Note: With the simplified API, we can only get the final result.
     * For testing purposes, we simulate individual statement results.
     */
    public List<QueryResult> executeMultiStatementQuery(String multiStatementQuery) {
        try {
            // Execute the entire multi-statement query to get final result
            JfrTable finalTable = executor.execute(multiStatementQuery);
            
            // Parse to determine how many statements there are
            Parser parser = new Parser(multiStatementQuery);
            ProgramNode program = parser.parse();
            
            List<QueryResult> results = new ArrayList<>();
            
            // For all statements except the last, create success results with empty tables
            // (since variable assignments don't return meaningful tables)
            for (int i = 0; i < program.statements().size() - 1; i++) {
                // Create an empty success result for variable assignment statements
                JfrTable emptyTable = SingleCellTable.ofString("Variable assigned");
                results.add(new QueryResult(emptyTable, true, null));
            }
            
            // Add the final result (the actual query result)
            results.add(new QueryResult(finalTable, true, null));
            
            return results;
        } catch (Exception e) {
            List<QueryResult> errorResults = new ArrayList<>();
            errorResults.add(new QueryResult(null, false, e));
            return errorResults;
        }
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
    
    // ================= SINGLE CELL TABLE UTILITIES =================
    
    /**
     * Create an optimized single-cell table with a default column name.
     * This is more efficient than creating a full JfrTable for single values.
     */
    public QueryTestFramework createSingleCellTable(String tableName, Object value) {
        SingleCellTable table = SingleCellTable.of(value);
        registerTable(tableName, table);
        return this;
    }
    
    /**
     * Create a single-cell table with a specified column name and value.
     * This replaces the manual pattern:
     *   JfrTable singleRowTable = new StandardJfrTable(List.of(new JfrTable.Column("temp", CellType.STRING)));
     *   singleRowTable.addRow(row);
     * 
     * With the optimized:
     *   createSingleCellTable("MyTable", "temp", value);
     */
    public QueryTestFramework createSingleCellTable(String tableName, String columnName, Object value) {
        SingleCellTable table = SingleCellTable.of(columnName, value);
        registerTable(tableName, table);
        return this;
    }
    

    
    /**
     * Create a temporary single-cell table with "temp" column name - commonly used in tests.
     */
    public QueryTestFramework createTempSingleCell(Object value) {
        return createSingleCellTable("temp", "temp", value);
    }
    
    /**
     * Create a single-cell table for testing default/empty results.
     */
    public QueryTestFramework createDefaultResult(String tableName) {
        return createSingleCellTable(tableName, "default", "");
    }
    
    /**
     * Create a single-cell table for testing empty results.
     */
    public QueryTestFramework createEmptyResult(String tableName) {
        return createSingleCellTable(tableName, "empty", "");
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
                if (potentialType.matches("STRING|NUMBER|NUMBER|BOOLEAN|TIMESTAMP|DURATION|MEMORY_SIZE|ARRAY|NULL")) {
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
    
    // ================= UNIFIED TABLE CREATION =================
    
    /**
     * Unified mock table creation method that automatically detects the format.
     * 
     * Supports multiple formats:
     * 1. Multi-line with pipe separators (auto type inference):
     *    "name | age | score\nAlice | 25 | 95.5\nBob | 30 | 87.2"
     * 
     * 2. Multi-line with explicit types:
     *    "name STRING | age NUMBER | score NUMBER\nAlice | 25 | 95.5\nBob | 30 | 87.2"
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
     * Create a single-value result table for testing aggregation results.
     * This is optimized for cases where you need to test queries that return a single value.
     * 
     * Example: createSingleValueResult("count", 42) creates a table with one column "count" and value 42
     */
    public QueryTestFramework createSingleValueResult(String tableName, String columnName, Object value) {
        return createSingleCellTable(tableName, columnName, value);
    }
    
    /**
     * Create a single numeric result for testing mathematical operations.
     */
    public QueryTestFramework createNumericResult(String tableName, double value) {
        return createSingleCellTable(tableName, "result", value);
    }
    
    /**
     * Create a single string result for testing string operations.
     */
    public QueryTestFramework createStringResult(String tableName, String value) {
        return createSingleCellTable(tableName, "result", value);
    }
    
    /**
     * Create a single boolean result for testing boolean operations.
     */
    public QueryTestFramework createBooleanResult(String tableName, boolean value) {
        return createSingleCellTable(tableName, "result", value);
    }
    
    // ================= OPTIMIZED SINGLE-CELL TABLE FACTORY =================
    
    /**
     * Factory for creating optimized single-cell tables with commonly used patterns.
     * This replaces manual JfrTable creation for single values:
     * 
     * Instead of:
     *   JfrTable singleRowTable = new StandardJfrTable(List.of(new JfrTable.Column("temp", CellType.STRING)));
     *   singleRowTable.addRow(row);
     * 
     * Use:
     *   SingleCellFactory.temp(value);
     */
    public static class SingleCellFactory {
        
        /**
         * Create a single-cell table with "temp" column name - commonly used for temporary results.
         */
        public static SingleCellTable temp(Object value) {
            return SingleCellTable.of("temp", value);
        }
        
        /**
         * Create a single-cell table with "value" column name - commonly used for expression results.
         */
        public static SingleCellTable value(Object value) {
            return SingleCellTable.of("value", value);
        }
        
        /**
         * Create a single-cell table with "result" column name - commonly used for function results.
         */
        public static SingleCellTable result(Object value) {
            return SingleCellTable.of("result", value);
        }
        
        /**
         * Create a single-cell table with "count" column name - commonly used for count aggregations.
         */
        public static SingleCellTable count(long value) {
            return SingleCellTable.of("count", value);
        }
        
        /**
         * Create a single-cell table with "sum" column name - commonly used for sum aggregations.
         */
        public static SingleCellTable sum(double value) {
            return SingleCellTable.of("sum", value);
        }
        
        /**
         * Create a single-cell table with "avg" column name - commonly used for average aggregations.
         */
        public static SingleCellTable avg(double value) {
            return SingleCellTable.of("avg", value);
        }
        
        /**
         * Create a single-cell table with custom column name.
         */
        public static SingleCellTable custom(String columnName, Object value) {
            return SingleCellTable.of(columnName, value);
        }
    }
    
    /**
     * Helper method to execute a query and validate it succeeded
     */
    public QueryResult executeAndAssertSuccess(String query) {
        var result = executeQuery(query);
        if (!result.isSuccess()) {
            // Create a more detailed assertion error message
            String errorDetails = createDetailedErrorMessage(query, result.getError());
            throw new AssertionError(errorDetails, result.getError());
        }
        return result;
    }
    
    /**
     * Create a detailed error message for test failures.
     */
    private String createDetailedErrorMessage(String query, Exception error) {
        StringBuilder details = new StringBuilder();
        details.append("Query execution failed:\n");
        details.append("Query: ").append(query).append("\n");
        details.append("Error: ").append(error.getClass().getSimpleName());
        
        if (error.getMessage() != null) {
            details.append(": ").append(error.getMessage());
        }
        
        // If there's a cause, include it
        if (error.getCause() != null && error.getCause() != error) {
            details.append("\nCaused by: ").append(error.getCause().getClass().getSimpleName());
            if (error.getCause().getMessage() != null) {
                details.append(": ").append(error.getCause().getMessage());
            }
        }
        
        return details.toString();
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
                "\nActual: " + result.toString() + ": " + e.getCause().getMessage());
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
                throw new AssertionError(result.getError());
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
                    name STRING | count NUMBER | ratio NUMBER | enabled BOOLEAN | timestamp TIMESTAMP
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
