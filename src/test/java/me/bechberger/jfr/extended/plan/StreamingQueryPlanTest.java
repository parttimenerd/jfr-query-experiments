package me.bechberger.jfr.extended.plan;

import me.bechberger.jfr.extended.ast.ASTNode;
import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.engine.plan.EventRow;
import me.bechberger.jfr.extended.table.CellValue;

import java.time.Instant;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for the new streaming query plan architecture.
 * 
 * Validates that the new streaming architecture properly replaces
 * QueryEvaluator and JoinProcessor while maintaining functionality.
 * 
 * @author JFR Query Engine Team
 * @since 2.0 (Streaming Architecture)
 */
public class StreamingQueryPlanTest {
    
    private QueryExecutionContext context;
    private JFRFileMetadata metadata;
    
    @BeforeEach
    void setUp() {
        // Create test JFR metadata
        Set<String> eventTypes = Set.of("GarbageCollection", "ExecutionSample", "JavaMonitorEnter");
        Map<String, Long> eventCounts = Map.of(
            "GarbageCollection", 1000L,
            "ExecutionSample", 50000L,
            "JavaMonitorEnter", 5000L
        );
        
        metadata = new JFRFileMetadata(
            eventTypes,
            Instant.now().minusSeconds(3600),
            Instant.now(),
            56000L,
            eventCounts
        );
        
        context = new QueryExecutionContext(metadata);
    }
    
    // ===== CORE ARCHITECTURE TESTS =====
    
    @Test
    @DisplayName("StreamingQueryPlan should provide streaming execution")
    void testStreamingExecution() {
        // Create a simple scan plan
        ASTNode mockNode = createMockASTNode();
        JFRScanPlan scanPlan = new JFRScanPlan(mockNode, "GarbageCollection");
        
        // Verify basic plan properties
        assertEquals("GarbageCollection", scanPlan.getRequiredEventTypes().iterator().next());
        assertTrue(scanPlan.explain().contains("JFRScan"));
        assertEquals(mockNode, scanPlan.getSourceNode());
        
        // Verify size estimation works
        long estimatedSize = scanPlan.estimateOutputSize(context);
        assertEquals(1000L, estimatedSize);
    }
    
    @Test
    @DisplayName("QueryExecutionContext should manage alias scoping correctly")
    void testAliasScoping() throws PlanExecutionException {
        ASTNode mockNode = createMockASTNode();
        
        // Test scope management
        context.pushScope();
        context.registerAlias("gc", createMockTable());
        
        // Should resolve in current scope
        assertNotNull(context.resolveAlias("gc", mockNode));
        
        // Test nested scoping
        context.pushScope();
        context.registerAlias("es", createMockTable());
        
        // Should resolve both aliases
        assertNotNull(context.resolveAlias("gc", mockNode));
        assertNotNull(context.resolveAlias("es", mockNode));
        
        // Pop scope
        context.popScope();
        
        // Inner scope alias should be gone
        assertThrows(PlanExecutionException.class, () -> 
            context.resolveAlias("es", mockNode));
        
        // Outer scope alias should still exist
        assertNotNull(context.resolveAlias("gc", mockNode));
    }
    
    @Test
    @DisplayName("JFRErrorContext should provide enhanced error messages")
    void testEnhancedErrorReporting() {
        Set<String> availableTypes = Set.of("GarbageCollection", "ExecutionSample");
        JFRErrorContext errorContext = JFRErrorContext.withEventTypes(availableTypes);
        
        assertTrue(errorContext.hasEventTypeInfo());
        assertEquals(availableTypes, errorContext.getAvailableEventTypes());
        
        // Test suggestion creation
        JFRErrorContext withSuggestion = JFRErrorContext.withSuggestion("Did you mean 'GarbageCollection'?");
        assertTrue(withSuggestion.hasSuggestion());
        assertEquals("Did you mean 'GarbageCollection'?", withSuggestion.getSuggestion());
    }
    
    // ===== JFR-SPECIFIC OPTIMIZATION TESTS =====
    
    @Test
    @DisplayName("JFRScanPlan should filter unknown event types with helpful errors")
    void testEventTypeFiltering() {
        ASTNode mockNode = createMockASTNode();
        JFRScanPlan scanPlan = new JFRScanPlan(mockNode, "UnknownEventType");
        
        // Should throw helpful error for unknown event type
        PlanExecutionException exception = assertThrows(PlanExecutionException.class, () ->
            scanPlan.execute(context));
        
        assertTrue(exception.getMessage().contains("Unknown event type: UnknownEventType"));
        assertTrue(exception.getUserFriendlyMessage().contains("Available event types:"));
    }
    
    @Test
    @DisplayName("JFRJoinPlan should select appropriate join algorithms")
    void testJoinAlgorithmSelection() {
        ASTNode mockNode = createMockASTNode();
        
        // Create child plans
        JFRScanPlan leftChild = new JFRScanPlan(mockNode, "GarbageCollection");
        JFRScanPlan rightChild = new JFRScanPlan(mockNode, "ExecutionSample");
        
        // Create join plan
        JFRJoinPlan joinPlan = new JFRJoinPlan(
            mockNode,
            leftChild,
            rightChild,
            (left, right) -> true, // Simple join condition
            JFRJoinPlan.JoinType.INNER
        );
        
        // Verify join plan properties
        Set<String> requiredTypes = joinPlan.getRequiredEventTypes();
        assertTrue(requiredTypes.contains("GarbageCollection"));
        assertTrue(requiredTypes.contains("ExecutionSample"));
        
        assertTrue(joinPlan.explain().contains("JFRJoin"));
        assertTrue(joinPlan.explain().contains("INNER"));
        
        // Verify size estimation
        long estimatedSize = joinPlan.estimateOutputSize(context);
        assertEquals(1000L, estimatedSize); // Should be min of left/right for INNER join
    }
    
    @Test
    @DisplayName("JFRFileMetadata should provide accurate selectivity estimates")
    void testSelectivityEstimation() {
        // Test selectivity for different event type combinations
        Set<String> allTypes = Set.of("GarbageCollection", "ExecutionSample", "JavaMonitorEnter");
        double fullSelectivity = metadata.estimateSelectivity(allTypes);
        assertEquals(1.0, fullSelectivity, 0.01);
        
        Set<String> singleType = Set.of("GarbageCollection");
        double singleSelectivity = metadata.estimateSelectivity(singleType);
        assertTrue(singleSelectivity < 1.0);
        assertTrue(singleSelectivity > 0.0);
        
        Set<String> emptyTypes = Collections.emptySet();
        double emptySelectivity = metadata.estimateSelectivity(emptyTypes);
        assertEquals(1.0, emptySelectivity, 0.01);
    }
    
    // ===== CACHING AND PERFORMANCE TESTS =====
    
    @Test
    @DisplayName("JFRSegmentCache should implement LRU eviction")
    void testSegmentCaching() {
        JFRSegmentCache cache = new JFRSegmentCache(2); // Small cache for testing
        
        // Create test segments
        JFRSegmentCache.FileSegment segment1 = new JFRSegmentCache.FileSegment("file1.jfr", 0, 1000);
        JFRSegmentCache.FileSegment segment2 = new JFRSegmentCache.FileSegment("file1.jfr", 1000, 2000);
        JFRSegmentCache.FileSegment segment3 = new JFRSegmentCache.FileSegment("file1.jfr", 2000, 3000);
        
        // Create test events
        Stream<EventRow> events1 = Stream.empty();
        Stream<EventRow> events2 = Stream.empty();
        Stream<EventRow> events3 = Stream.empty();
        
        JFRSegmentCache.ParsedEvents parsed1 = new JFRSegmentCache.ParsedEvents(events1);
        JFRSegmentCache.ParsedEvents parsed2 = new JFRSegmentCache.ParsedEvents(events2);
        JFRSegmentCache.ParsedEvents parsed3 = new JFRSegmentCache.ParsedEvents(events3);
        
        // Add to cache
        cache.cacheSegment(segment1, parsed1);
        cache.cacheSegment(segment2, parsed2);
        assertEquals(2, cache.size());
        
        // Access first segment to make it more recent
        cache.getCachedEvents(segment1);
        
        // Add third segment - should evict second (LRU)
        cache.cacheSegment(segment3, parsed3);
        assertEquals(2, cache.size());
        
        // First and third should be present
        assertTrue(cache.getCachedEvents(segment1).isPresent());
        assertTrue(cache.getCachedEvents(segment3).isPresent());
        
        // Second should be evicted
        assertFalse(cache.getCachedEvents(segment2).isPresent());
    }
    
    @Test
    @DisplayName("EventRow should support efficient column operations")
    void testEventRowOperations() {
        // Create test event row
        Map<String, CellValue> columns = new HashMap<>();
        columns.put("name", CellValue.of("G1GC"));
        columns.put("duration", CellValue.of(100L));
        columns.put("timestamp", CellValue.of(Instant.now()));
        
        EventRow row = new EventRow(columns, "GarbageCollection", Instant.now());
        
        // Test column access
        assertEquals("G1GC", row.getValue("name").asString());
        assertEquals(100L, row.getValue("duration").asLong());
        assertTrue(row.hasColumn("name"));
        assertFalse(row.hasColumn("nonexistent"));
        
        // Test column projection
        EventRow projected = row.projectColumns(Set.of("name", "duration"));
        assertTrue(projected.hasColumn("name"));
        assertTrue(projected.hasColumn("duration"));
        assertFalse(projected.hasColumn("timestamp"));
        
        // Test column addition
        Map<String, CellValue> additional = Map.of("cause", CellValue.of("System.gc()"));
        EventRow withAdditional = row.withAdditionalColumns(additional);
        assertTrue(withAdditional.hasColumn("cause"));
        assertTrue(withAdditional.hasColumn("name")); // Original columns preserved
    }
    
    // ===== HELPER METHODS =====
    
    private ASTNode createMockASTNode() {
        // Create a simple mock AST node for testing
        return new SelectNode(null, null, null, null, null, null, null);
    }
    
    private me.bechberger.jfr.extended.table.JfrTable createMockTable() {
        // Create a simple mock table for testing
        return new me.bechberger.jfr.extended.table.StandardJfrTable(
            new String[]{"id", "value"},
            new me.bechberger.jfr.extended.table.CellType[]{
                me.bechberger.jfr.extended.table.CellType.LONG,
                me.bechberger.jfr.extended.table.CellType.STRING
            },
            new Object[][]{
                {1L, "test"}
            }
        );
    }
}
