package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;

/**
 * Test the simplified and unified mock table creation API.
 * Demonstrates the new createTable method that auto-detects format.
 * 
 * @author Enhanced Testing Framework
 * @since 3.0
 */
public class UnifiedMockTableTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    /**
     * Test data provider for different table format variations
     */
    static Stream<Arguments> tableFormatVariations() {
        return Stream.of(
            // Multi-line with type inference (pipe separators)
            Arguments.of("TypeInference", """
                name | age | score
                Alice | 25 | 95.5
                Bob | 30 | 87.2
                Charlie | 20 | 92.0
                """, 3, 3),
            
            // Multi-line with explicit types (pipe separators)
            Arguments.of("ExplicitTypes", """
                name STRING | age NUMBER | score FLOAT | active BOOLEAN
                Alice | 25 | 95.5 | true
                Bob | 30 | 87.2 | false
                Charlie | 20 | 92.0 | true
                """, 3, 4),
            
            // Single-line with semicolon separators
            Arguments.of("SingleLine", "name age score; Alice 25 95.5; Bob 30 87.2; Charlie 20 92.0", 3, 3),
            
            // Single table with minimal data
            Arguments.of("MinimalData", """
                id | name
                1 | Test
                """, 1, 2)
        );
    }
    
    /**
     * Parameterized test to validate all table format variations
     */
    @ParameterizedTest(name = "Table format: {0}")
    @MethodSource("tableFormatVariations")
    void testUnifiedTableCreation(String tableName, String tableSpec, int expectedRows, int expectedColumns) {
        // Use the unified createTable method
        framework.createTable(tableName, tableSpec);
        
        // Test basic query execution
        var result = framework.executeQuery("@SELECT * FROM " + tableName);
        assertTrue(result.isSuccess(), "Query should succeed for table: " + tableName);
        assertEquals(expectedRows, result.getRowCount(), "Row count mismatch for table: " + tableName);
        assertEquals(expectedColumns, result.getColumnCount(), "Column count mismatch for table: " + tableName);
        
        // Test column selection
        if (expectedColumns > 1) {
            var columnResult = framework.executeQuery("@SELECT " + getFirstColumnName(tableSpec) + " FROM " + tableName);
            assertTrue(columnResult.isSuccess(), "Column selection should work for table: " + tableName);
            assertEquals(expectedRows, columnResult.getRowCount());
            assertEquals(1, columnResult.getColumnCount());
        }
        
        // Test sorting if table has multiple rows
        if (expectedRows > 1) {
            var sortResult = framework.executeQuery("@SELECT * FROM " + tableName + " ORDER BY " + getFirstColumnName(tableSpec) + " ASC");
            assertTrue(sortResult.isSuccess(), "Sorting should work for table: " + tableName);
            assertEquals(expectedRows, sortResult.getRowCount());
        }
    }
    
    /**
     * Test builder-based table creation (should still work)
     */
    @Test
    void testBuilderBasedCreation() {
        framework.mockTable("BuilderTable")
            .withStringColumn("name")
            .withNumberColumn("value")
            .withFloatColumn("ratio")
            .withRow("Test1", 100L, 1.5)
            .withRow("Test2", 200L, 2.5)
            .build();
        
        var result = framework.executeQuery("SELECT * FROM BuilderTable");
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRowCount());
        assertEquals(3, result.getColumnCount());
    }
    
    /**
     * Test specialized JFR table builders
     */
    @Test
    void testSpecializedBuilders() {
        // Test GC table builder
        framework.withGarbageCollectionTable()
            .withRow("G1", 50000L, 1609459200000L, 1073741824L, 536870912L, "System.gc()")
            .build();
        
        var gcResult = framework.executeQuery("SELECT * FROM GarbageCollection");
        assertTrue(gcResult.isSuccess());
        assertEquals(1, gcResult.getRowCount());
        assertEquals(6, gcResult.getColumnCount());
        
        // Test custom table builder
        framework.customTable("CustomTable")
            .withStringColumn("event")
            .withTimestampColumn("when")
            .withRow("START", 1609459200000L)
            .withRow("END", 1609459260000L)
            .build();
        
        var customResult = framework.executeQuery("SELECT * FROM CustomTable");
        assertTrue(customResult.isSuccess());
        assertEquals(2, customResult.getRowCount());
        assertEquals(2, customResult.getColumnCount());
    }
    
    /**
     * Test error handling and edge cases
     */
    @Test
    void testErrorHandling() {
        // Test empty table creation
        framework.createTable("EmptyTable", "name | value");
        var emptyResult = framework.executeQuery("@SELECT * FROM EmptyTable");
        assertTrue(emptyResult.isSuccess());
        assertEquals(0, emptyResult.getRowCount());
        assertEquals(2, emptyResult.getColumnCount());
        
        // Test single row table
        framework.createTable("SingleRowTable", """
            id | name
            1 | Solo
            """);
        var singleResult = framework.executeQuery("@SELECT * FROM SingleRowTable");
        assertTrue(singleResult.isSuccess());
        assertEquals(1, singleResult.getRowCount());
        assertEquals(2, singleResult.getColumnCount());
        
        // Test non-existent table
        var errorResult = framework.executeQuery("@SELECT * FROM DoesNotExist");
        assertFalse(errorResult.isSuccess());
        assertNotNull(errorResult.getError());
    }
    
    /**
     * Test table toString functionality for debugging
     */
    @Test
    void testTableStringOutput() {
        framework.createTable("StringTestTable", """
            name | score
            Alice | 95.5
            Bob | 87.2
            """);
        
        var result = framework.executeQuery("@SELECT * FROM StringTestTable");
        assertTrue(result.isSuccess());
        
        String output = result.toString();
        assertNotNull(output);
        assertTrue(output.contains("name"));
        assertTrue(output.contains("score"));
        assertTrue(output.contains("Alice"));
        assertTrue(output.contains("95.5"));
        
        System.out.println("Table output format:");
        System.out.println(output);
    }
    
    /**
     * Test sorting assertions
     */
    @Test
    void testSortingAssertions() {
        framework.createTable("SortTestTable", """
            name | value
            Charlie | 30
            Alice | 10
            Bob | 20
            """);
        
        // Test ascending sort
        var ascResult = framework.executeQuery("@SELECT * FROM SortTestTable ORDER BY value ASC");
        assertTrue(ascResult.isSuccess());
        assertDoesNotThrow(() -> ascResult.assertSortedByColumn("value", true));
        
        // Test descending sort
        var descResult = framework.executeQuery("@SELECT * FROM SortTestTable ORDER BY value DESC");
        assertTrue(descResult.isSuccess());
        assertDoesNotThrow(() -> descResult.assertSortedByColumn("value", false));
        
        // Test sort assertion failure
        var unsortedResult = framework.executeQuery("@SELECT * FROM SortTestTable");
        assertTrue(unsortedResult.isSuccess());
        assertThrows(AssertionError.class, () -> 
            unsortedResult.assertSortedByColumn("value", true));
    }
    
    /**
     * Test mixed data types handling
     */
    @Test
    void testMixedDataTypes() {
        framework.createTable("MixedTypesTable", """
            name STRING | count NUMBER | ratio FLOAT | enabled BOOLEAN | timestamp TIMESTAMP
            Alice | 100 | 1.5 | true | 1609459200000
            Bob | 200 | 2.5 | false | 1609545600000
            """);
        
        var result = framework.executeQuery("@SELECT * FROM MixedTypesTable");
        assertTrue(result.isSuccess());
        assertEquals(2, result.getRowCount());
        assertEquals(5, result.getColumnCount());
        
        // Test column-specific queries
        var nameResult = framework.executeQuery("@SELECT name FROM MixedTypesTable");
        assertTrue(nameResult.isSuccess());
        assertEquals(2, nameResult.getRowCount());
        assertEquals(1, nameResult.getColumnCount());
    }
    
    /**
     * Helper method to extract the first column name from a table specification
     */
    private String getFirstColumnName(String tableSpec) {
        String firstLine = tableSpec.trim().split("\n")[0];
        if (firstLine.contains("|")) {
            return firstLine.split("\\|")[0].trim().split("\\s+")[0];
        } else {
            return firstLine.split("\\s+")[0];
        }
    }
}
