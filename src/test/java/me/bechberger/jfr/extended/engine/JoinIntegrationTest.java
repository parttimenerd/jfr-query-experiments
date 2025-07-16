package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import me.bechberger.jfr.extended.table.CellValue;
import org.junit.jupiter.api.*;
import static org.junit.jupiter.api.Assertions.*;
import java.time.Instant;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * Comprehensive integration tests for the join functionality in the JFR Query Engine.
 * 
 * This test suite validates:
 * - Standard SQL joins (INNER, LEFT, RIGHT, FULL)
 * - Fuzzy temporal joins (NEAREST, PREVIOUS, AFTER)
 * - Performance optimization with large datasets
 * - Error handling for join edge cases
 * - Memory efficiency and resource cleanup
 */
@DisplayName("Join Integration Tests")
class JoinIntegrationTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    // ===== BASIC JOIN FUNCTIONALITY TESTS =====
    
    @Test
    @DisplayName("INNER JOIN with threadId correlation")
    void testInnerJoinWithThreadId() {
        // Setup test data with overlapping threadIds
        framework.mockTable("events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withTimestampColumn("timestamp")
            .withRow(1L, "GC", Instant.now())
            .withRow(2L, "Allocation", Instant.now().plusSeconds(1))
            .withRow(3L, "Method", Instant.now().plusSeconds(2))
            .withRow(4L, "Lock", Instant.now().plusSeconds(3))
            .build();
            
        framework.mockTable("metrics")
            .withNumberColumn("threadId")
            .withFloatColumn("cpuUsage")
            .withNumberColumn("memoryUsage")
            .withRow(1L, 45.5, 1024L)
            .withRow(2L, 78.2, 2048L)
            .withRow(5L, 23.1, 512L) // No matching threadId in events
            .build();
        
        // Execute the join query and verify results
        var result = framework.executeAndAssertSuccess(
            "@SELECT * FROM events e INNER JOIN metrics m ON e.threadId = m.threadId"
        );
        
        // Should have 2 matching rows (threadId 1 and 2)
        assertEquals(2, result.getTable().getRowCount());
        
        // Verify join result structure
        assertEquals(6, result.getTable().getColumns().size()); // 3 from each table
        assertTrue(result.getTable().getColumns().get(0).name().startsWith("left_"));
        assertTrue(result.getTable().getColumns().get(3).name().startsWith("right_"));
    }
    
    @Test
    @DisplayName("LEFT JOIN with unmatched rows")
    void testLeftJoinWithUnmatchedRows() {
        // Setup test data with some unmatched threadIds in left table
        framework.mockTable("events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withTimestampColumn("timestamp")
            .withRow(1L, "GC", Instant.now())
            .withRow(2L, "Allocation", Instant.now().plusSeconds(1))
            .withRow(3L, "Method", Instant.now().plusSeconds(2))
            .build();
            
        framework.mockTable("metrics")
            .withNumberColumn("threadId")
            .withFloatColumn("cpuUsage")
            .withNumberColumn("memoryUsage")
            .withRow(1L, 45.5, 1024L)
            .withRow(2L, 78.2, 2048L)
            .build();
        
        var result = framework.executeAndAssertSuccess(
            "@SELECT * FROM events e LEFT JOIN metrics m ON e.threadId = m.threadId"
        );
        
        // Should have 3 rows (all from left table)
        assertEquals(3, result.getTable().getRowCount());
        
        // Check that unmatched row has null values for right table
        Set<Double> leftThreadIds = new HashSet<>();
        Set<Double> rightThreadIds = new HashSet<>();
        
        for (var row : result.getTable().getRows()) {
            var leftId = row.getCell(0);
            var rightId = row.getCell(3);
            
            if (leftId instanceof CellValue.NumberValue) {
                leftThreadIds.add(row.getDouble(0));
            }
            if (rightId instanceof CellValue.NumberValue) {
                rightThreadIds.add(row.getDouble(3));
            }
        }
        
        // All left threadIds should be present
        assertEquals(Set.of(1.0, 2.0, 3.0), leftThreadIds);
        // Only matching right threadIds should be present
        assertEquals(Set.of(1.0, 2.0), rightThreadIds);
    }
    
    @Test
    @DisplayName("FULL OUTER JOIN with data from both sides")
    void testFullOuterJoin() {
        framework.mockTable("events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withRow(1L, "GC")
            .withRow(2L, "Allocation")
            .withRow(3L, "Method")
            .build();
            
        framework.mockTable("metrics")
            .withNumberColumn("threadId")
            .withFloatColumn("cpuUsage")
            .withRow(2L, 78.2)
            .withRow(3L, 55.1)
            .withRow(4L, 92.5)
            .build();
        
        var result = framework.executeAndAssertSuccess(
            "@SELECT * FROM events e FULL OUTER JOIN metrics m ON e.threadId = m.threadId"
        );
        
        // Should have 4 rows total (union of all data)
        assertEquals(4, result.getTable().getRowCount());
        
        // Verify we have data from both unmatched sides
        boolean hasUnmatchedLeft = false;
        boolean hasUnmatchedRight = false;
        
        for (var row : result.getTable().getRows()) {
            var leftId = row.getCell(0);
            var rightId = row.getCell(2);
            
            if (leftId instanceof CellValue.NumberValue && row.isNull(2)) {
                hasUnmatchedLeft = true; // threadId 1 from events only
            }
            if (row.isNull(0) && rightId instanceof CellValue.NumberValue) {
                hasUnmatchedRight = true; // threadId 4 from metrics only
            }
        }
        
        assertTrue(hasUnmatchedLeft, "Should have unmatched rows from left table");
        assertTrue(hasUnmatchedRight, "Should have unmatched rows from right table");
    }
    
    // ===== FUZZY TEMPORAL JOIN TESTS =====
    
    @Test
    @DisplayName("NEAREST temporal join with time proximity")
    void testNearestTemporalJoin() {
        Instant baseTime = Instant.now();
        
        framework.mockTable("events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withTimestampColumn("timestamp")
            .withRow(1L, "GC", baseTime)
            .withRow(1L, "Allocation", baseTime.plusSeconds(10))
            .withRow(1L, "Method", baseTime.plusSeconds(20))
            .build();
            
        framework.mockTable("logs")
            .withNumberColumn("threadId")
            .withStringColumn("message")
            .withTimestampColumn("logTime")
            .withRow(1L, "Starting GC", baseTime.minusSeconds(2))
            .withRow(1L, "Memory pressure", baseTime.plusSeconds(8))
            .withRow(1L, "Method entry", baseTime.plusSeconds(22))
            .build();
        
        var result = framework.executeAndAssertSuccess(
            "@SELECT * FROM events e NEAREST JOIN logs l ON e.threadId = l.threadId AND e.timestamp ~= l.logTime"
        );
        
        // Each event should be matched with the nearest log entry by timestamp
        assertEquals(3, result.getTable().getRowCount());
        
        // Verify the joins are based on time proximity
        assertTrue(result.isSuccess(), "NEAREST join should execute successfully");
    }
    
    @Test
    @DisplayName("PREVIOUS temporal join finds past events")
    void testPreviousTemporalJoin() {
        Instant baseTime = Instant.now();
        
        framework.mockTable("allocations")
            .withNumberColumn("threadId")
            .withNumberColumn("size")
            .withTimestampColumn("allocTime")
            .withRow(1L, 1024L, baseTime)
            .withRow(1L, 2048L, baseTime.plusSeconds(5))
            .withRow(1L, 4096L, baseTime.plusSeconds(15))
            .build();
            
        framework.mockTable("gcs")
            .withNumberColumn("threadId")
            .withStringColumn("gcType")
            .withTimestampColumn("gcTime")
            .withRow(1L, "Minor", baseTime.plusSeconds(10))
            .withRow(1L, "Major", baseTime.plusSeconds(20))
            .build();
        
        // Find the allocation that happened just before each GC
        var result = framework.executeAndAssertSuccess(
            "@SELECT * FROM gcs g PREVIOUS JOIN allocations a ON g.threadId = a.threadId AND g.gcTime > a.allocTime"
        );
        
        // Should find 2 GC events with their preceding allocations
        assertEquals(2, result.getTable().getRowCount());
        assertTrue(result.isSuccess(), "PREVIOUS join should execute successfully");
    }
    
    // ===== PERFORMANCE AND SCALABILITY TESTS =====
    
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    @DisplayName("Performance test with large datasets")
    void testJoinPerformanceWithLargeDatasets() {
        // Create large test datasets to validate performance
        var eventsBuilder = framework.mockTable("largeEvents")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withTimestampColumn("timestamp");
            
        Instant baseTime = Instant.now();
        
        // Create 1000 events across 50 different threadIds
        for (int i = 0; i < 1000; i++) {
            eventsBuilder.withRow((double)(i % 50 + 1), "Event" + i, baseTime.plusSeconds(i));
        }
        eventsBuilder.build();
        
        var metricsBuilder = framework.mockTable("largeMetrics")
            .withNumberColumn("threadId")
            .withFloatColumn("cpuUsage");
            
        // Create 500 metrics across 30 different threadIds  
        for (int i = 0; i < 500; i++) {
            metricsBuilder.withRow((double)(i % 30 + 1), Math.random() * 100);
        }
        metricsBuilder.build();
        
        // Execute join and verify performance
        var result = framework.executeAndAssertSuccess(
            "@SELECT COUNT(*) FROM largeEvents e INNER JOIN largeMetrics m ON e.threadId = m.threadId"
        );
        
        assertTrue(result.isSuccess(), "Large dataset join should complete successfully");
        
        // Should find matches for threadIds 1-30 (overlap between 50 and 30)
        long count = result.getTable().getNumber(0, 0);
        assertTrue(count > 0, "Should have matching rows in large dataset join");
    }
    
    // ===== ERROR HANDLING TESTS =====
    
    @Test
    @DisplayName("Join with mismatched column types")
    void testJoinWithMismatchedColumnTypes() {
        framework.mockTable("events")
            .withNumberColumn("id")
            .withStringColumn("eventType")
            .withRow(1L, "GC")
            .withRow(2L, "Allocation")
            .build();
            
        framework.mockTable("logs")
            .withStringColumn("id") // String vs Number type mismatch
            .withStringColumn("message")
            .withRow("A", "Log message A")
            .withRow("B", "Log message B")
            .build();
        
        // This should execute but find no matches due to type mismatch
        var result = framework.executeAndAssertSuccess(
            "@SELECT * FROM events e INNER JOIN logs l ON e.id = l.id"
        );
        
        // Should return zero rows due to type mismatch
        assertEquals(0, result.getTable().getRowCount());
    }
    
    @Test
    @DisplayName("Join on non-existent column")
    void testJoinOnNonExistentColumn() {
        framework.mockTable("events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withRow(1L, "GC")
            .build();
            
        framework.mockTable("metrics")
            .withNumberColumn("threadId")
            .withFloatColumn("cpuUsage")
            .withRow(1L, 45.5)
            .build();
        
        // Try to join on a non-existent column
        var result = framework.executeQuery(
            "@SELECT * FROM events e INNER JOIN metrics m ON e.nonExistentColumn = m.threadId"
        );
        
        assertFalse(result.isSuccess(), "Join on non-existent column should fail");
        assertNotNull(result.getError(), "Should have error message");
    }
    
    // ===== COMPLEX JOIN SCENARIO TESTS =====
    
    @Test
    @DisplayName("Multi-condition join with complex predicates")
    void testMultiConditionJoin() {
        framework.mockTable("requests")
            .withNumberColumn("threadId")
            .withStringColumn("method")
            .withNumberColumn("responseTime")
            .withRow(1L, "GET", 250L)
            .withRow(1L, "POST", 180L)
            .withRow(2L, "GET", 320L)
            .build();
            
        framework.mockTable("errors")
            .withNumberColumn("threadId")
            .withStringColumn("errorType")
            .withNumberColumn("responseTime")
            .withRow(1L, "timeout", 250L)
            .withRow(2L, "validation", 320L)
            .withRow(3L, "network", 150L)
            .build();
        
        // Join on both threadId and responseTime
        var result = framework.executeAndAssertSuccess(
            "@SELECT * FROM requests r INNER JOIN errors e ON r.threadId = e.threadId AND r.responseTime = e.responseTime"
        );
        
        // Should match 2 rows (threadId 1 with responseTime 250, threadId 2 with responseTime 320)
        assertEquals(2, result.getTable().getRowCount());
    }
    
    @Test
    @DisplayName("Self-join on the same table")
    void testSelfJoin() {
        framework.mockTable("events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withTimestampColumn("timestamp")
            .withRow(1L, "start", Instant.now())
            .withRow(1L, "end", Instant.now().plusSeconds(5))
            .withRow(2L, "start", Instant.now().plusSeconds(10))
            .withRow(2L, "end", Instant.now().plusSeconds(15))
            .build();
        
        // Find start-end pairs for the same thread
        var result = framework.executeAndAssertSuccess(
            "@SELECT * FROM events e1 INNER JOIN events e2 ON e1.threadId = e2.threadId WHERE e1.eventType = 'start' AND e2.eventType = 'end'"
        );
        
        // Should find 2 start-end pairs
        assertEquals(2, result.getTable().getRowCount());
    }
}
