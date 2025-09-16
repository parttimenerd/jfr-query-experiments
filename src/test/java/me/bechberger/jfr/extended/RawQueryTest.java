package me.bechberger.jfr.extended;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for raw JFR queries (SELECT without @ prefix).
 * Tests the difference between raw queries and extended queries.
 * 
 * Raw queries are direct SQL-like queries against JFR event tables,
 * while extended queries (@SELECT) provide additional functionality.
 * 
 * @author JFR Query Language Testing Team
 * @since 3.0
 */
public class RawQueryTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    // ===== BASIC RAW QUERY TESTS =====
    
    @Test
    @DisplayName("Raw query: SELECT * FROM Event")
    void testSelectAllFromEvent() {
        // Create a mock Event table
        framework.mockTable("Event")
            .withStringColumn("eventType")
            .withStringColumn("thread")
            .withNumberColumn("duration")
            .withRow("GC", "main", 100L)
            .withRow("CPU", "worker-1", 50L)
            .withRow("Memory", "worker-2", 75L)
            .build();
        
        // Test raw query
        framework.executeAndExpectTable("SELECT * FROM Event", """
            eventType | thread | duration
            GC | main | 100
            CPU | worker-1 | 50
            Memory | worker-2 | 75
            """);
    }
    
    @Test
    @DisplayName("Raw query: SELECT specific columns")
    void testSelectSpecificColumns() {
        framework.mockTable("Event")
            .withStringColumn("eventType")
            .withStringColumn("thread")
            .withNumberColumn("duration")
            .withTimestampColumn("timestamp")
            .withRow("GC", "main", 100L, 1609459200000L)
            .withRow("CPU", "worker-1", 50L, 1609459260000L)
            .build();
        
        framework.executeAndExpectTable("SELECT eventType, duration FROM Event", """
            eventType | duration
            GC | 100
            CPU | 50
            """);
    }
    
    @Test
    @DisplayName("Raw query: SELECT with ORDER BY")
    void testSelectWithOrderBy() {
        framework.mockTable("Event")
            .withStringColumn("eventType")
            .withNumberColumn("duration")
            .withRow("GC", 100L)
            .withRow("CPU", 50L)
            .withRow("Memory", 75L)
            .build();
        
        framework.executeAndExpectTable("SELECT eventType, duration FROM Event ORDER BY duration ASC", """
            eventType | duration
            CPU | 50
            Memory | 75
            GC | 100
            """);
    }
    
    @Test
    @DisplayName("Raw query: SELECT with LIMIT")
    void testSelectWithLimit() {
        framework.mockTable("Event")
            .withStringColumn("eventType")
            .withNumberColumn("duration")
            .withRow("GC", 100L)
            .withRow("CPU", 50L)
            .withRow("Memory", 75L)
            .build();
        
        framework.executeAndExpectTable("SELECT * FROM Event LIMIT 2", """
            eventType | duration
            GC | 100
            CPU | 50
            """);
    }
    
    // ===== COMPARISON: RAW vs EXTENDED QUERIES =====
    
    @Test
    @DisplayName("Raw query basic functionality test")
    void testRawQueryBasicFunctionality() {
        framework.mockTable("TestTable")
            .withStringColumn("name")
            .withNumberColumn("value")
            .withRow("Alice", 100L)
            .withRow("Bob", 200L)
            .build();
        
        // Test raw query
        var rawResult = framework.executeAndAssertSuccess("SELECT * FROM TestTable");
        assertEquals(2, rawResult.getTable().getRowCount());
        assertEquals(2, rawResult.getTable().getColumnCount());
        
        // Test extended query (for comparison)
        var extendedResult = framework.executeAndAssertSuccess("@SELECT * FROM TestTable");
        assertEquals(2, extendedResult.getTable().getRowCount());
        
        // Both should be able to access the same data
        assertEquals("Alice", rawResult.getTable().getString(0, "name"));
        assertEquals(100L, rawResult.getTable().getNumber(0, "value"));
    }
    
    // ===== PARAMETERIZED TESTS =====
    
    @ParameterizedTest
    @ValueSource(strings = {
        "SELECT * FROM GarbageCollection",
        "SELECT * FROM ExecutionSample", 
        "SELECT * FROM ThreadSleep",
        "SELECT * FROM CPULoad",
        "SELECT * FROM AllocationSample"
    })
    @DisplayName("Raw queries for common JFR event types")
    void testRawJfrEventQueries(String query) {
        // Extract table name from query
        String tableName = query.substring(query.indexOf("FROM") + 5).trim();
        
        // Create a mock table for this event type
        framework.mockTable(tableName)
            .withStringColumn("eventType")
            .withStringColumn("thread")
            .withNumberColumn("duration")
            .withTimestampColumn("timestamp")
            .withRow("TestEvent", "main", 100L, 1609459200000L)
            .build();
        
        // Execute the query and verify it succeeds
        var result = framework.executeAndAssertSuccess(query);
        assertNotNull(result.getTable());
        assertEquals(1, result.getTable().getRowCount());
        assertEquals("TestEvent", result.getTable().getString(0, "eventType"));
    }
    
    @ParameterizedTest
    @CsvSource({
        "SELECT eventType FROM Event, eventType",
        "SELECT thread FROM Event, thread", 
        "SELECT duration FROM Event, duration",
        "SELECT timestamp FROM Event, timestamp"
    })
    @DisplayName("Raw queries with single column selection")
    void testSingleColumnSelection(String query, String expectedColumn) {
        framework.mockTable("Event")
            .withStringColumn("eventType")
            .withStringColumn("thread")
            .withNumberColumn("duration")
            .withTimestampColumn("timestamp")
            .withRow("GC", "main", 100L, 1609459200000L)
            .build();
        
        var result = framework.executeAndAssertSuccess(query);
        assertEquals(1, result.getTable().getColumnCount());
        assertEquals(expectedColumn, result.getTable().getColumns().get(0).name());
    }
    
    // ===== ADVANCED RAW QUERY TESTS =====
    
    @Test
    @DisplayName("Raw query with complex ORDER BY and LIMIT")
    void testComplexRawQuery() {
        framework.mockTable("PerformanceEvent")
            .withStringColumn("eventType")
            .withStringColumn("thread")
            .withNumberColumn("duration")
            .withNumberColumn("cpu")
            .withRow("GC", "main", 100L, 80L)
            .withRow("CPU", "worker-1", 50L, 95L)
            .withRow("Memory", "worker-2", 75L, 60L)
            .withRow("IO", "worker-3", 200L, 40L)
            .build();
        
        framework.executeAndExpectTable("SELECT eventType, duration, cpu FROM PerformanceEvent ORDER BY duration DESC LIMIT 2", """
            eventType | duration | cpu
            IO | 200 | 40
            GC | 100 | 80
            """);
    }
    
    @Test
    @DisplayName("Raw query with case-insensitive table names")
    void testCaseInsensitiveTableNames() {
        framework.mockTable("Event")
            .withStringColumn("name")
            .withNumberColumn("value")
            .withRow("test", 42L)
            .build();
        
        // Test different case variations
        framework.executeAndAssertSuccess("SELECT * FROM Event");
        framework.executeAndAssertSuccess("SELECT * FROM event");
        framework.executeAndAssertSuccess("SELECT * FROM EVENT");
        framework.executeAndAssertSuccess("SELECT * FROM eVeNt");
    }
    
    @Test
    @DisplayName("Raw query error handling for non-existent table")
    void testNonExistentTable() {
        var result = framework.executeQuery("SELECT * FROM NonExistentTable");
        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
        assertTrue(result.getError().getMessage().contains("Table not found"));
    }
    
    // ===== EDGE CASES =====
    
    @Test
    @DisplayName("Raw query with empty table")
    void testEmptyTable() {
        framework.mockTable("EmptyEvent")
            .withStringColumn("eventType")
            .withNumberColumn("duration")
            .build();
        
        framework.executeAndExpectTable("SELECT * FROM EmptyEvent", """
            eventType | duration
            """);
    }
    
    @Test
    @DisplayName("Raw query with single row")
    void testSingleRowTable() {
        framework.mockTable("SingleEvent")
            .withStringColumn("eventType")
            .withNumberColumn("duration")
            .withRow("GC", 100L)
            .build();
        
        framework.executeAndExpectTable("SELECT * FROM SingleEvent", """
            eventType | duration
            GC | 100
            """);
    }
    
    @Test
    @DisplayName("Raw query with ORDER BY on string column")
    void testOrderByStringColumn() {
        framework.mockTable("Event")
            .withStringColumn("eventType")
            .withNumberColumn("duration")
            .withRow("GC", 100L)
            .withRow("CPU", 50L)
            .withRow("Memory", 75L)
            .build();
        
        framework.executeAndExpectTable("SELECT * FROM Event ORDER BY eventType ASC", """
            eventType | duration
            CPU | 50
            GC | 100
            Memory | 75
            """);
    }
    
    // ===== PERFORMANCE TESTS =====
    
    @Test
    @DisplayName("Raw query performance with large dataset")
    void testPerformanceWithLargeDataset() {
        var builder = framework.mockTable("LargeEvent");
        builder.withStringColumn("eventType");
        builder.withNumberColumn("duration");
        builder.withNumberColumn("id");
        
        // Add 1000 rows
        for (int i = 0; i < 1000; i++) {
            builder.withRow("Event" + (i % 10), (long) (i * 10), (long) i);
        }
        builder.build();
        
        // Test that it can handle the large dataset
        var result = framework.executeAndAssertSuccess("SELECT * FROM LargeEvent");
        assertEquals(1000, result.getTable().getRowCount());
        
        // Test with LIMIT
        var limitedResult = framework.executeAndAssertSuccess("SELECT * FROM LargeEvent LIMIT 10");
        assertEquals(10, limitedResult.getTable().getRowCount());
    }
    
    // ===== DOCUMENTATION TESTS =====
    
    @Test
    @DisplayName("Raw query showcase: Basic JFR event analysis")
    void testBasicJfrEventAnalysis() {
        // Create a realistic JFR event table
        framework.mockTable("jdk.GarbageCollection")
            .withStringColumn("name")
            .withDurationColumn("duration")
            .withTimestampColumn("startTime")
            .withMemorySizeColumn("heapUsedBefore")
            .withMemorySizeColumn("heapUsedAfter")
            .withStringColumn("cause")
            .withRow("G1GC", 50L, 1609459200000L, 2048L, 1024L, "Allocation Rate")
            .withRow("ParallelGC", 75L, 1609459260000L, 4096L, 2048L, "System.gc()")
            .withRow("G1GC", 40L, 1609459320000L, 1024L, 512L, "Allocation Rate")
            .build();
        
        // Test basic selection
        framework.executeAndExpectTable("SELECT name, duration, cause FROM jdk.GarbageCollection", """
            name | duration | cause
            G1GC | 50 | Allocation Rate
            ParallelGC | 75 | System.gc()
            G1GC | 40 | Allocation Rate
            """);
        
        // Test with ordering
        framework.executeAndExpectTable("SELECT name, duration FROM jdk.GarbageCollection ORDER BY duration DESC", """
            name | duration
            ParallelGC | 75
            G1GC | 50
            G1GC | 40
            """);
    }
}
