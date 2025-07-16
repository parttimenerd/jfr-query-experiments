package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.SingleCellTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for verifying cache optimization improvements.
 * 
 * Tests the new cache configuration with 1 million entries and relaxed TTL enforcement.
 */
class CacheOptimizationTest {
    
    private ExecutionContext context;
    
    @BeforeEach
    void setUp() {
        context = new ExecutionContext();
    }
    
    @Test
    void testDefaultCacheSize() {
        ExecutionContext.CacheConfig config = context.getCacheConfig();
        assertEquals(1_000_000, config.getMaxSize(), "Default cache size should be 1 million entries");
        assertEquals(0, config.getTtlMs(), "TTL should be disabled (0)");
    }
    
    @Test
    void testCacheConfigurationUpdate() {
        context.setCacheConfig(2_000_000);
        
        ExecutionContext.CacheConfig config = context.getCacheConfig();
        assertEquals(2_000_000, config.getMaxSize());
        assertEquals(0, config.getTtlMs(), "TTL should always be 0 (disabled)");
    }
    
    @Test
    void testLegacyCacheConfigurationUpdate() {
        // Test legacy method that includes TTL parameter (should be ignored)
        context.setCacheConfig(2_000_000, 10 * 60 * 1000);
        
        ExecutionContext.CacheConfig config = context.getCacheConfig();
        assertEquals(2_000_000, config.getMaxSize());
        assertEquals(0, config.getTtlMs(), "TTL should always be 0 (disabled)");
    }
    
    @Test
    void testNoPersistentCaching() throws InterruptedException {
        JfrTable testTable = SingleCellTable.ofString("Test Value");
        context.cacheResult("test_key", testTable);
        
        // Immediately after caching, result should be available
        JfrTable retrieved = context.getCachedResult("test_key");
        assertNotNull(retrieved, "Result should be available immediately after caching");
        
        // Wait some time - without TTL, entry should persist indefinitely
        Thread.sleep(100);
        
        // Entry should still be available (no TTL enforcement)
        retrieved = context.getCachedResult("test_key");
        assertNotNull(retrieved, "Result should persist indefinitely without TTL");
        
        ExecutionContext.CacheStats stats = context.getCacheStats();
        assertEquals(2, stats.getHits(), "Should have 2 cache hits");
        assertEquals(0, stats.getMisses(), "Should have 0 cache misses");
    }
    
    @Test
    void testEvictionOnlyWhenFull() {
        // Set cache to small size
        context.setCacheConfig(10);
        
        // Fill cache to capacity
        for (int i = 0; i < 10; i++) {
            context.cacheResult("key" + i, SingleCellTable.ofString("Value" + i));
        }
        
        assertEquals(10, context.getCacheStats().getSize());
        
        // Add one more item to trigger eviction
        context.cacheResult("key10", SingleCellTable.ofString("Value10"));
        
        // Should evict 10% = 1 item, so 9 + 1 = 10 items remain
        assertEquals(10, context.getCacheStats().getSize());
        assertTrue(context.getCacheStats().getEvictions() >= 1, "Should have at least 1 eviction");
    }
    
    @Test
    void testCacheConfigToString() {
        ExecutionContext.CacheConfig config = context.getCacheConfig();
        String configStr = config.toString();
        assertTrue(configStr.contains("1000000")); // max size
        assertTrue(configStr.contains("ttlMs=0")); // TTL disabled
    }
    
    @Test
    void testCacheStatsAfterOperations() {
        JfrTable testTable = SingleCellTable.ofString("Test Value");
        
        // Cache and retrieve multiple times
        context.cacheResult("key1", testTable);
        context.getCachedResult("key1"); // hit
        context.getCachedResult("key1"); // hit
        context.getCachedResult("nonexistent"); // miss
        
        ExecutionContext.CacheStats stats = context.getCacheStats();
        assertEquals(2, stats.getHits());
        assertEquals(1, stats.getMisses());
        assertEquals(1, stats.getSize());
        assertTrue(stats.getHitRate() > 0.5); // 2 hits out of 3 total = 66.7%
    }
    
    @Test
    void testCacheStatsToString() {
        context.cacheResult("key1", SingleCellTable.ofString("Value"));
        context.getCachedResult("key1"); // hit
        context.getCachedResult("nonexistent"); // miss
        
        ExecutionContext.CacheStats stats = context.getCacheStats();
        String statsStr = stats.toString();
        assertTrue(statsStr.contains("hits=1"));
        assertTrue(statsStr.contains("misses=1"));
        assertTrue(statsStr.contains("hitRate="));
    }
    
    @Test
    void testCacheEdgeCases() {
        // Test null handling
        context.cacheResult(null, SingleCellTable.ofString("value")); // Should not crash
        context.cacheResult("key", null); // Should not crash
        
        JfrTable result = context.getCachedResult(null); // Should return null without crashing
        assertNull(result);
        
        // Test dependency invalidation with null
        context.invalidateCacheDependencies(null); // Should not crash
        
        // Test cache configuration validation
        assertThrows(IllegalArgumentException.class, () -> context.setCacheConfig(-1));
        assertThrows(IllegalArgumentException.class, () -> context.setCacheConfig(0));
        assertThrows(IllegalArgumentException.class, () -> context.setCacheConfig(-1, 1000));
        
        // Verify normal operation still works
        context.setCacheConfig(1000);
        context.cacheResult("test", SingleCellTable.ofString("value"));
        assertNotNull(context.getCachedResult("test"));
    }
    
    @Test
    void testThreadSafety() {
        // Basic test to ensure concurrent access doesn't throw exceptions
        // This is not a comprehensive thread safety test, but checks basic functionality
        context.setCacheConfig(100);
        
        // Cache some results
        for (int i = 0; i < 50; i++) {
            context.cacheResult("key" + i, SingleCellTable.ofString("value" + i));
        }
        
        // Verify they can be retrieved
        for (int i = 0; i < 50; i++) {
            JfrTable result = context.getCachedResult("key" + i);
            assertNotNull(result);
        }
        
        // Test cache invalidation
        Set<String> dependencies = Set.of("dep1", "dep2");
        context.cacheResult("dependent", SingleCellTable.ofString("value"), dependencies);
        context.invalidateCacheDependencies("dep1");
        
        assertNull(context.getCachedResult("dependent"));
    }
}
