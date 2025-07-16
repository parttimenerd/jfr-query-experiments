package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.engine.framework.QueryTestFramework;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Comprehensive test suite for JOIN optimization features in the query engine.
 * 
 * Tests various aspects of join performance including:
 * - Hash-based standard joins (INNER, LEFT, RIGHT, FULL)
 * - Time-sorted fuzzy joins (NEAREST, PREVIOUS, AFTER)
 * - Index caching for repeated operations
 * - Memory optimization for large datasets
 * - WHERE clause optimization during joins
 * 
 * @author Generated following QueryTestFramework guidelines
 */
@DisplayName("JOIN Optimization Tests")
class JoinOptimizationTest {
    
    private QueryTestFramework framework;
    
    @BeforeEach
    void setUp() {
        framework = new QueryTestFramework();
    }
    
    // ===== BASIC JOIN OPTIMIZATION TESTS =====
    
    @Test
    @DisplayName("Basic INNER JOIN should return matching rows efficiently")
    void testBasicInnerJoinOptimization() {
        // Arrange - Create test tables with overlapping data
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withTimestampColumn("timestamp")
            .withRow(1L, "GC", Instant.now())
            .withRow(2L, "Allocation", Instant.now().plusSeconds(1))
            .withRow(3L, "Method", Instant.now().plusSeconds(2))
            .build();
            
        framework.mockTable("Threads")
            .withNumberColumn("threadId")
            .withStringColumn("threadName")
            .withStringColumn("state")
            .withRow(1L, "main", "RUNNABLE")
            .withRow(2L, "worker-1", "BLOCKED")
            .withRow(4L, "worker-2", "WAITING") // No matching event
            .build();
        
        // Act & Assert
        framework.executeAndExpectTable("""
            @SELECT e.threadId, e.eventType, t.threadName, t.state
            FROM Events e
            INNER JOIN Threads t ON e.threadId = t.threadId
            ORDER BY e.threadId
            """, """
            threadId | eventType | threadName | state
            1 | GC | main | RUNNABLE
            2 | Allocation | worker-1 | BLOCKED
            """);
    }
    
    @Test
    @DisplayName("LEFT JOIN should include all left table rows")
    void testLeftJoinOptimization() {
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withRow(1L, "GC")
            .withRow(2L, "Allocation")
            .withRow(3L, "Method") // No matching thread
            .build();
            
        framework.mockTable("Threads")
            .withNumberColumn("threadId")
            .withStringColumn("threadName")
            .withRow(1L, "main")
            .withRow(2L, "worker-1")
            .build();
        
        framework.executeAndExpectTable("""
            @SELECT e.threadId, e.eventType, t.threadName
            FROM Events e
            LEFT JOIN Threads t ON e.threadId = t.threadId
            ORDER BY e.threadId
            """, """
            threadId | eventType | threadName
            1 | GC | main
            2 | Allocation | worker-1
            3 | Method | null
            """);
    }
    
    @Test
    @DisplayName("RIGHT JOIN should include all right table rows")
    void testRightJoinOptimization() {
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withRow(1L, "GC")
            .withRow(2L, "Allocation")
            .build();
            
        framework.mockTable("Threads")
            .withNumberColumn("threadId")
            .withStringColumn("threadName")
            .withRow(1L, "main")
            .withRow(2L, "worker-1")
            .withRow(3L, "worker-2") // No matching event
            .build();
        
        framework.executeAndExpectTable("""
            @SELECT e.eventType, t.threadId, t.threadName
            FROM Events e
            RIGHT JOIN Threads t ON e.threadId = t.threadId
            """, """
            eventType | threadId | threadName
            GC | 1 | main
            Allocation | 2 | worker-1
            null | 3 | worker-2
            """);
    }
    
    // ===== FUZZY JOIN OPTIMIZATION TESTS =====
    
    @Test
    @DisplayName("NEAREST fuzzy join should find closest timestamp matches")
    void testNearestFuzzyJoinOptimization() {
        Instant baseTime = Instant.now();
        
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withTimestampColumn("timestamp")
            .withStringColumn("eventType")
            .withRow(1L, baseTime, "GC")
            .withRow(1L, baseTime.plusSeconds(10), "Allocation")
            .withRow(1L, baseTime.plusSeconds(20), "Method")
            .build();
            
        framework.mockTable("Metrics")
            .withNumberColumn("threadId")
            .withTimestampColumn("timestamp")
            .withNumberColumn("cpuUsage")
            .withRow(1L, baseTime.plusSeconds(2), 50L)   // Closest to first event
            .withRow(1L, baseTime.plusSeconds(12), 75L)  // Closest to second event
            .withRow(1L, baseTime.plusSeconds(18), 90L)  // Closest to third event
            .build();
        
        var result = framework.executeAndAssertSuccess("""
            @SELECT e.eventType, e.timestamp, m.cpuUsage, m.timestamp as metricTime
            FROM Events e
            FUZZY JOIN Metrics m ON timestamp
            ORDER BY e.timestamp
            """);
        
        // Verify the result structure and counts
        assertEquals(3, result.getTable().getRowCount());
        assertEquals("GC", result.getTable().getString(0, "eventType"));
        assertEquals(50L, result.getTable().getNumber(0, "cpuUsage"));
        assertEquals("Allocation", result.getTable().getString(1, "eventType"));
        assertEquals(75L, result.getTable().getNumber(1, "cpuUsage"));
        assertEquals("Method", result.getTable().getString(2, "eventType"));
        assertEquals(90L, result.getTable().getNumber(2, "cpuUsage"));
    }
    
    @Test
    @DisplayName("PREVIOUS fuzzy join should find preceding timestamp matches")
    void testPreviousFuzzyJoinOptimization() {
        Instant baseTime = Instant.now();
        
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withTimestampColumn("timestamp")
            .withStringColumn("eventType")
            .withRow(1L, baseTime.plusSeconds(10), "GC")
            .withRow(1L, baseTime.plusSeconds(20), "Allocation")
            .build();
            
        framework.mockTable("Metrics")
            .withNumberColumn("threadId")
            .withTimestampColumn("timestamp")
            .withNumberColumn("cpuUsage")
            .withRow(1L, baseTime.plusSeconds(5), 30L)   // Before first event
            .withRow(1L, baseTime.plusSeconds(15), 60L)  // Between events
            .withRow(1L, baseTime.plusSeconds(25), 90L)  // After second event
            .build();
        
        var result = framework.executeAndAssertSuccess("""
            @SELECT e.eventType, e.timestamp, m.cpuUsage, m.timestamp as metricTime
            FROM Events e
            FUZZY JOIN PREVIOUS Metrics m ON timestamp
            ORDER BY e.timestamp
            """);
        
        // Verify the result structure and counts
        assertEquals(2, result.getTable().getRowCount());
        assertEquals("GC", result.getTable().getString(0, "eventType"));
        assertEquals(30L, result.getTable().getNumber(0, "cpuUsage"));
        assertEquals("Allocation", result.getTable().getString(1, "eventType"));
        assertEquals(60L, result.getTable().getNumber(1, "cpuUsage"));
    }
    
    // ===== WHERE CLAUSE OPTIMIZATION TESTS =====
    
    @Test
    @DisplayName("JOIN with WHERE clause should apply filtering efficiently")
    void testJoinWithWhereClauseOptimization() {
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withNumberColumn("duration")
            .withRow(1L, "GC", 100L)
            .withRow(2L, "Allocation", 50L)
            .withRow(3L, "Method", 200L)
            .build();
            
        framework.mockTable("Threads")
            .withNumberColumn("threadId")
            .withStringColumn("threadName")
            .withStringColumn("state")
            .withRow(1L, "main", "RUNNABLE")
            .withRow(2L, "worker-1", "BLOCKED")
            .withRow(3L, "worker-2", "RUNNABLE")
            .build();
        
        framework.executeAndExpectTable("""
            @SELECT e.threadId, e.eventType, e.duration, t.threadName
            FROM Events e
            INNER JOIN Threads t ON e.threadId = t.threadId
            WHERE e.duration > 75 AND t.state = 'RUNNABLE'
            ORDER BY e.duration
            """, """
            threadId | eventType | duration | threadName
            1 | GC | 100 | main
            3 | Method | 200 | worker-2
            """);
    }
    
    @ParameterizedTest
    @DisplayName("Multiple JOIN types should work with WHERE optimization")
    @CsvSource({
        "INNER JOIN, 2",
        "LEFT JOIN, 3", 
        "RIGHT JOIN, 3"
    })
    void testMultipleJoinTypesWithWhereOptimization(String joinType, int expectedRows) {
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withNumberColumn("duration")
            .withRow(1L, "GC", 100L)
            .withRow(2L, "Allocation", 50L)
            .withRow(3L, "Method", 200L)
            .build();
            
        framework.mockTable("Threads")
            .withNumberColumn("threadId")
            .withStringColumn("threadName")
            .withRow(1L, "main")
            .withRow(2L, "worker-1")
            .withRow(4L, "worker-2") // Different threadId for testing
            .build();
        
        var result = framework.executeAndAssertSuccess(
            "@SELECT COUNT(*) as count FROM Events e " + joinType + 
            " Threads t ON e.threadId = t.threadId WHERE e.duration >= 50"
        );
        
        assertEquals(expectedRows, result.getTable().getNumber(0, "count"));
    }
    
    // ===== PERFORMANCE AND CACHING TESTS =====
    
    @Test
    @DisplayName("Large dataset JOIN should complete within performance threshold")
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testLargeDatasetJoinPerformance() {
        // Create larger test datasets
        var eventsBuilder = framework.mockTable("LargeEvents")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withTimestampColumn("timestamp");
            
        Instant baseTime = Instant.now();
        for (int i = 1; i <= 1000; i++) {
            eventsBuilder.withRow((long)(i % 20) + 1, "Event" + (i % 5), baseTime.plusSeconds(i));
        }
        eventsBuilder.build();
        
        var threadsBuilder = framework.mockTable("LargeThreads")
            .withNumberColumn("threadId")
            .withStringColumn("threadName")
            .withStringColumn("state");
            
        for (int i = 1; i <= 25; i++) {
            threadsBuilder.withRow((long)i, "Thread-" + i, i % 2 == 0 ? "RUNNABLE" : "BLOCKED");
        }
        threadsBuilder.build();
        
        // Perform join and verify it completes quickly
        var result = framework.executeAndAssertSuccess("""
            @SELECT COUNT(*) as joinCount
            FROM LargeEvents e
            INNER JOIN LargeThreads t ON e.threadId = t.threadId
            WHERE t.state = 'RUNNABLE'
            """);
        
        assertTrue(result.getTable().getNumber(0, "joinCount") > 0);
    }
    
    @Test
    @DisplayName("Repeated JOINs should benefit from caching")
    void testJoinCachingOptimization() {
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withRow(1L, "GC")
            .withRow(2L, "Allocation")
            .build();
            
        framework.mockTable("Threads")
            .withNumberColumn("threadId")
            .withStringColumn("threadName")
            .withRow(1L, "main")
            .withRow(2L, "worker-1")
            .build();
        
        // First execution - should build cache
        var result1 = framework.executeAndAssertSuccess("""
            @SELECT COUNT(*) as count
            FROM Events e
            INNER JOIN Threads t ON e.threadId = t.threadId
            """);
        
        // Second execution - should use cache
        var result2 = framework.executeAndAssertSuccess("""
            @SELECT COUNT(*) as count
            FROM Events e
            INNER JOIN Threads t ON e.threadId = t.threadId
            """);
        
        // Both should return the same result
        assertEquals(result1.getTable().getNumber(0, "count"), 
                    result2.getTable().getNumber(0, "count"));
    }
    
    // ===== COMPLEX JOIN SCENARIOS =====
    
    @Test
    @DisplayName("Multi-table JOIN with aggregation should work efficiently")
    void testMultiTableJoinWithAggregation() {
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withNumberColumn("duration")
            .withRow(1L, "GC", 100L)
            .withRow(1L, "Allocation", 50L)
            .withRow(2L, "GC", 150L)
            .withRow(2L, "Method", 75L)
            .build();
            
        framework.mockTable("Threads")
            .withNumberColumn("threadId")
            .withStringColumn("threadName")
            .withStringColumn("type")
            .withRow(1L, "main", "APP")
            .withRow(2L, "worker-1", "BACKGROUND")
            .build();
            
        framework.mockTable("ThreadGroups")
            .withStringColumn("type")
            .withStringColumn("groupName")
            .withNumberColumn("priority")
            .withRow("APP", "Application", 10L)
            .withRow("BACKGROUND", "Background", 5L)
            .build();
        
        framework.executeAndExpectTable("""
            @SELECT t.type, tg.groupName, COUNT(*) as eventCount, AVG(e.duration) as avgDuration
            FROM Events e
            INNER JOIN Threads t ON e.threadId = t.threadId
            INNER JOIN ThreadGroups tg ON t.type = tg.type
            GROUP BY t.type, tg.groupName
            ORDER BY tg.priority DESC
            """, """
            type | groupName | eventCount | avgDuration
            APP | Application | 2 | 75
            BACKGROUND | Background | 2 | 112.5
            """);
    }
    
    @Test
    @DisplayName("JOIN with subquery should optimize correctly")
    void testJoinWithSubqueryOptimization() {
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withNumberColumn("duration")
            .withRow(1L, "GC", 100L)
            .withRow(1L, "Allocation", 50L)
            .withRow(2L, "GC", 150L)
            .withRow(2L, "Method", 75L)
            .build();
            
        framework.mockTable("Threads")
            .withNumberColumn("threadId")
            .withStringColumn("threadName")
            .withRow(1L, "main")
            .withRow(2L, "worker-1")
            .build();
        
        framework.executeAndExpectTable("""
            @SELECT t.threadName, highDurationEvents.avgDuration
            FROM Threads t
            INNER JOIN (
                @SELECT threadId, AVG(duration) as avgDuration
                FROM Events 
                WHERE duration > 60
                GROUP BY threadId
            ) highDurationEvents ON t.threadId = highDurationEvents.threadId
            ORDER BY highDurationEvents.avgDuration DESC
            """, """
            threadName | avgDuration
            worker-1 | 112.5
            main | 100
            """);
    }
    
    // ===== ERROR HANDLING TESTS =====
    
    @Test
    @DisplayName("JOIN with missing table should provide clear error message")
    void testJoinWithMissingTable() {
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withRow(1L, "GC")
            .build();
        
        var result = framework.executeQuery("""
            @SELECT e.eventType, t.threadName
            FROM Events e
            INNER JOIN NonExistentTable t ON e.threadId = t.threadId
            """);
        
        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
        assertTrue(result.getError().getMessage().toLowerCase().contains("table"));
    }
    
    @Test
    @DisplayName("JOIN with invalid column should provide clear error message")
    void testJoinWithInvalidColumn() {
        framework.mockTable("Events")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .withRow(1L, "GC")
            .build();
            
        framework.mockTable("Threads")
            .withNumberColumn("threadId")
            .withStringColumn("threadName")
            .withRow(1L, "main")
            .build();
        
        var result = framework.executeQuery("""
            @SELECT e.eventType, t.threadName
            FROM Events e
            INNER JOIN Threads t ON e.invalidColumn = t.threadId
            """);
        
        assertFalse(result.isSuccess());
        assertNotNull(result.getError());
        assertTrue(result.getError().getMessage().toLowerCase().contains("column"));
    }
    
    // ===== EDGE CASE TESTS =====
    
    @Test
    @DisplayName("JOIN with empty tables should return empty result")
    void testJoinWithEmptyTables() {
        framework.mockTable("EmptyEvents")
            .withNumberColumn("threadId")
            .withStringColumn("eventType")
            .build();
            
        framework.mockTable("EmptyThreads")
            .withNumberColumn("threadId")
            .withStringColumn("threadName")
            .build();
        
        var result = framework.executeAndAssertSuccess("""
            @SELECT COUNT(*) as count
            FROM EmptyEvents e
            INNER JOIN EmptyThreads t ON e.threadId = t.threadId
            """);
        
        assertEquals(0L, result.getTable().getNumber(0, "count"));
    }
    
    @Test
    @DisplayName("JOIN with null values should handle gracefully")
    void testJoinWithNullValues() {
        framework.createTable("EventsWithNulls", """
            threadId | eventType
            1 | GC
            null | Allocation
            3 | Method
            """);
            
        framework.createTable("ThreadsWithNulls", """
            threadId | threadName
            1 | main
            2 | null
            null | worker-1
            """);
        
        framework.executeAndExpectTable("""
            @SELECT e.threadId, e.eventType, t.threadName
            FROM EventsWithNulls e
            LEFT JOIN ThreadsWithNulls t ON e.threadId = t.threadId
            ORDER BY e.threadId
            """, """
            threadId | eventType | threadName
            1 | GC | main
            3 | Method | null
            null | Allocation | null
            """);
    }
}
