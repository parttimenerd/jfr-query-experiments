package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.engine.framework.ExpectedTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Test mock table creation functionality using the unified createTable API and expectTable validation.
 * Focuses on testing extended @SELECT query language features with comprehensive expectTable usage.
 * 
 * @author Enhanced Testing Framework
 * @since 3.0
 */
public class MockTableTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    // ================= HELPER METHODS =================
    
    /**
     * Helper method to execute a query and validate the result with expectTable
     */
    private void executeAndExpectTable(String query, String expectedTableSpec) {
        var result = framework.executeQuery(query);
        assertTrue(result.isSuccess(), "Query should succeed: " + query);
        
        ExpectedTable expected = framework.expectTable(expectedTableSpec);
        assertDoesNotThrow(() -> expected.assertMatches(result.getTable()), 
            "Table validation should pass for query: " + query);
        
        System.out.println("Query: " + query);
        System.out.println("Result: " + result);
    }
    
    /**
     * Helper method to create a table and validate a query result
     */
    private void createTableAndExpect(String tableName, String tableSpec, String query, String expectedResult) {
        framework.createTable(tableName, tableSpec);
        executeAndExpectTable(query, expectedResult);
    }
    
    // ================= TEST METHODS =================
    
    /**
     * Test single line format using expectTable for validation
     */
    @Test
    void testCreateTableSingleLineFormatWithExpectTable() {
        // Using the unified createTable method - automatically detects format
        createTableAndExpect("TestTable1", 
            "name age score; Alice 25 95.5; Bob 30 87.2",
            "@SELECT * FROM TestTable1",
            """
            name | age | score
            Alice | 25 | 95.5
            Bob | 30 | 87.2
            """);
    }
    
    /**
     * Test multi line format with expectTable validation
     */
    @Test
    void testCreateTableMultiLineFormatWithExpectTable() {
        // Using the unified createTable method with pipe separators
        createTableAndExpect("TestTable2",
            """
            name | age | score
            Alice | 25 | 95.5
            Bob | 30 | 87.2
            """,
            "@SELECT * FROM TestTable2",
            """
            name | age | score
            Alice | 25 | 95.5
            Bob | 30 | 87.2
            """);
    }
    
    /**
     * Test multi line format with explicit types and expectTable
     */
    @Test
    void testCreateTableWithExplicitTypesAndExpectTable() {
        // Using the unified createTable method with explicit types
        framework.createTable("TestTable3", """
            name STRING | age NUMBER | score FLOAT | active BOOLEAN
            Alice | 25 | 95.5 | true
            Bob | 30 | 87.2 | false
            """);
        
        // Test full table selection
        executeAndExpectTable("@SELECT * FROM TestTable3", """
            name | age | score | active
            Alice | 25 | 95.5 | true
            Bob | 30 | 87.2 | false
            """);
        
        // Test column selection with expectTable
        executeAndExpectTable("@SELECT name, active FROM TestTable3", """
            name | active
            Alice | true
            Bob | false
            """);
    }
    
    /**
     * Test builder-based table creation with expectTable validation
     */
    @Test
    void testBuilderBasedTableCreationWithExpectTable() {
        framework.mockTable("TestTable4")
            .withStringColumn("name")
            .withNumberColumn("age")
            .withFloatColumn("score")
            .withRow("Alice", 25L, 95.5)
            .withRow("Bob", 30L, 87.2)
            .build();
        
        // Use helper method for validation
        executeAndExpectTable("@SELECT * FROM TestTable4", """
            name | age | score
            Alice | 25 | 95.5
            Bob | 30 | 87.2
            """);
    }
    
    /**
     * Test extended query features with ORDER BY and expectTable
     */
    @ParameterizedTest
    @ValueSource(strings = {"name", "age", "score"})
    void testExtendedQueryFeaturesWithSorting(String sortColumn) {
        // Create a table using the unified API
        framework.createTable("SortableUsers", """
            name | age | score
            Charlie | 20 | 92.0
            Alice | 25 | 95.5
            Bob | 30 | 87.2
            """);
        
        // Test ascending sort with @SELECT
        var ascResult = framework.executeQuery("@SELECT * FROM SortableUsers ORDER BY " + sortColumn + " ASC");
        assertTrue(ascResult.isSuccess(), "Ascending sort should work for: " + sortColumn);
        assertDoesNotThrow(() -> ascResult.assertSortedByColumn(sortColumn, true));
        
        // Test descending sort with @SELECT
        var descResult = framework.executeQuery("@SELECT * FROM SortableUsers ORDER BY " + sortColumn + " DESC");
        assertTrue(descResult.isSuccess(), "Descending sort should work for: " + sortColumn);
        assertDoesNotThrow(() -> descResult.assertSortedByColumn(sortColumn, false));
        
        System.out.println("Sort by " + sortColumn + " ASC: " + ascResult);
        System.out.println("Sort by " + sortColumn + " DESC: " + descResult);
    }
    
    /**
     * Test column selection and filtering with expectTable
     */
    @Test
    void testColumnSelectionWithExpectTable() {
        framework.createTable("FilterableData", """
            name | department | salary | active
            Alice | Engineering | 75000 | true
            Bob | Sales | 60000 | false
            Charlie | Engineering | 80000 | true
            David | Marketing | 65000 | false
            """);
        
        // Test specific column selection
        var nameResult = framework.executeQuery("@SELECT name, salary FROM FilterableData");
        assertTrue(nameResult.isSuccess());
        
        ExpectedTable expectedNameSalary = framework.expectTable("""
            name | salary
            Alice | 75000
            Bob | 60000
            Charlie | 80000
            David | 65000
            """);
        
        assertDoesNotThrow(() -> expectedNameSalary.assertMatches(nameResult.getTable()));
        
        // Test with sorting
        var sortedResult = framework.executeQuery("@SELECT name, salary FROM FilterableData ORDER BY salary DESC");
        assertTrue(sortedResult.isSuccess());
        
        ExpectedTable expectedSorted = framework.expectTable("""
            name | salary
            Charlie | 80000
            Alice | 75000
            David | 65000
            Bob | 60000
            """);
        
        assertDoesNotThrow(() -> expectedSorted.assertMatches(sortedResult.getTable()));
    }
    
    /**
     * Test specialized table builders with expectTable
     */
    @Test
    void testSpecializedTableBuildersWithExpectTable() {
        // Test GC table builder
        framework.withGarbageCollectionTable()
            .withRow("G1", 50000L, 1609459200000L, 1073741824L, 536870912L, "System.gc()")
            .withRow("ParallelGC", 75000L, 1609459260000L, 2147483648L, 1073741824L, "Allocation Failure")
            .build();
        
        var gcResult = framework.executeQuery("@SELECT name, duration FROM GarbageCollection ORDER BY duration ASC");
        assertTrue(gcResult.isSuccess());
        
        ExpectedTable expectedGC = framework.expectTable("""
            name | duration
            G1 | 50000
            ParallelGC | 75000
            """);
        
        assertDoesNotThrow(() -> expectedGC.assertMatches(gcResult.getTable()));
        
        // Test custom table builder
        framework.customTable("CustomEvents")
            .withStringColumn("event")
            .withTimestampColumn("timestamp")
            .withStringColumn("status")
            .withRow("API_CALL", 1609459200000L, "SUCCESS")
            .withRow("DB_QUERY", 1609459210000L, "FAILED")
            .withRow("CACHE_HIT", 1609459220000L, "SUCCESS")
            .build();
        
        var customResult = framework.executeQuery("@SELECT event, status FROM CustomEvents");
        assertTrue(customResult.isSuccess());
        
        ExpectedTable expectedCustom = framework.expectTable("""
            event | status
            API_CALL | SUCCESS
            DB_QUERY | FAILED
            CACHE_HIT | SUCCESS
            """);
        
        assertDoesNotThrow(() -> expectedCustom.assertMatches(customResult.getTable()));
    }
    
    /**
     * Test error handling with expectTable
     */
    @Test
    void testErrorHandlingWithExpectTable() {
        // Test empty table
        framework.createTable("EmptyTable", "name | value");
        var emptyResult = framework.executeQuery("@SELECT * FROM EmptyTable");
        assertTrue(emptyResult.isSuccess());
        
        // Validate empty table structure
        ExpectedTable expectedEmpty = framework.expectTable("name | value");
        assertDoesNotThrow(() -> expectedEmpty.assertMatches(emptyResult.getTable()));
        
        // Test non-existent table
        var errorResult = framework.executeQuery("@SELECT * FROM DoesNotExist");
        assertFalse(errorResult.isSuccess());
        assertNotNull(errorResult.getError());
        
        // Validate error expectation
        framework.assertThat(errorResult, framework.expectFailure("Table not found"));
    }
    
    /**
     * Test complex data types with expectTable
     */
    @Test
    void testComplexDataTypesWithExpectTable() {
        framework.createTable("ComplexTypesTable", """
            name STRING | timestamp TIMESTAMP | duration DURATION | memory MEMORY_SIZE | active BOOLEAN
            Task1 | 1609459200000 | 5000 | 1024 | true
            Task2 | 1609459260000 | 3000 | 2048 | false
            """);
        
        var result = framework.executeQuery("@SELECT name, active FROM ComplexTypesTable ORDER BY name ASC");
        assertTrue(result.isSuccess());
        
        ExpectedTable expected = framework.expectTable("""
            name | active
            Task1 | true
            Task2 | false
            """);
        
        assertDoesNotThrow(() -> expected.assertMatches(result.getTable()));
        
        // Test toString output for debugging
        System.out.println("Complex types result toString:");
        System.out.println(result.toString());
    }
    
    /**
     * Test unified API auto-detection with expectTable
     */
    @Test
    void testUnifiedAPIAutoDetectionWithExpectTable() {
        // Test different formats are detected automatically
        
        // Single-line format
        framework.createTable("SingleLine", "id name; 1 Alice; 2 Bob");
        var singleResult = framework.executeQuery("@SELECT * FROM SingleLine");
        
        ExpectedTable expectedSingle = framework.expectTable("""
            id | name
            1 | Alice
            2 | Bob
            """);
        assertDoesNotThrow(() -> expectedSingle.assertMatches(singleResult.getTable()));
        
        // Multi-line with type inference
        framework.createTable("MultiInfer", """
            value | enabled
            100 | true
            200 | false
            """);
        
        var multiResult = framework.executeQuery("@SELECT * FROM MultiInfer");
        
        ExpectedTable expectedMulti = framework.expectTable("""
            value | enabled
            100 | true
            200 | false
            """);
        assertDoesNotThrow(() -> expectedMulti.assertMatches(multiResult.getTable()));
        
        // Multi-line with explicit types
        framework.createTable("MultiExplicit", """
            score FLOAT | count NUMBER
            95.5 | 10
            87.2 | 15
            """);
        
        var explicitResult = framework.executeQuery("@SELECT * FROM MultiExplicit");
        
        ExpectedTable expectedExplicit = framework.expectTable("""
            score | count
            95.5 | 10
            87.2 | 15
            """);
        assertDoesNotThrow(() -> expectedExplicit.assertMatches(explicitResult.getTable()));
    }
}
