package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.ast.ASTNodes.QueryNode;
import me.bechberger.jfr.extended.table.JfrTable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Execution context for query evaluation.
 * 
 * Maintains state during query execution including variable bindings,
 * intermediate results, and metadata.
 */
public class ExecutionContext {
    
    private final Map<String, Object> localVariables = new ConcurrentHashMap<>();
    private final Map<String, JfrTable> intermediateResults = new ConcurrentHashMap<>();
    private JfrTable currentResult;
    private boolean debugMode = false;
    
    // QueryEngine functionality - global state
    private final Map<String, Object> variables = new ConcurrentHashMap<>();
    private final Map<String, LazyQuery> lazyVariables = new ConcurrentHashMap<>();
    private final Map<String, QueryNode> views = new ConcurrentHashMap<>();
    private final Map<String, JfrTable> resultCache = new ConcurrentHashMap<>();
    private final Map<String, Set<String>> cacheDependencies = new ConcurrentHashMap<>(); // For dependency tracking
    private long cacheMaxSize = 1_000_000; // Default max cache entries (increased to 1 million)
    private boolean eventTypeVariablesInitialized = false;
    
    // Cache statistics
    private long cacheHits = 0;
    private long cacheMisses = 0;
    private long cacheEvictions = 0;
    
    public ExecutionContext() {
    }
    
    public ExecutionContext(boolean debugMode) {
        this.debugMode = debugMode;
    }
    
    /**
     * Set a local variable
     */
    public void setLocalVariable(String name, Object value) {
        localVariables.put(name, value);
    }
    
    /**
     * Get a local variable
     */
    public Object getLocalVariable(String name) {
        return localVariables.get(name);
    }
    
    /**
     * Check if local variable exists
     */
    public boolean hasLocalVariable(String name) {
        return localVariables.containsKey(name);
    }
    
    /**
     * Store intermediate result
     */
    public void setIntermediateResult(String key, JfrTable result) {
        intermediateResults.put(key, result);
    }
    
    /**
     * Get intermediate result
     */
    public JfrTable getIntermediateResult(String key) {
        return intermediateResults.get(key);
    }
    
    /**
     * Set the current result
     */
    public void setCurrentResult(JfrTable result) {
        this.currentResult = result;
    }
    
    /**
     * Get the current result
     */
    public JfrTable getCurrentResult() {
        return currentResult;
    }
    
    /**
     * Check if debug mode is enabled
     */
    public boolean isDebugMode() {
        return debugMode;
    }
    
    /**
     * Set debug mode
     */
    public void setDebugMode(boolean debugMode) {
        this.debugMode = debugMode;
    }
    
    /**
     * Get all local variables
     */
    public Map<String, Object> getLocalVariables() {
        return Map.copyOf(localVariables);
    }
    
    /**
     * Set a global variable
     */
    public void setVariable(String name, Object value) {
        variables.put(name, value);
    }
    
    /**
     * Get a global variable
     */
    public Object getVariable(String name) {
        return variables.get(name);
    }
    
    /**
     * Get all global variables
     */
    public Map<String, Object> getVariables() {
        return Map.copyOf(variables);
    }
    
    /**
     * Set a lazy variable
     */
    public void setLazyVariable(String name, LazyQuery lazyQuery) {
        lazyVariables.put(name, lazyQuery);
    }
    
    /**
     * Get a lazy variable
     */
    public LazyQuery getLazyVariable(String name) {
        return lazyVariables.get(name);
    }
    
    /**
     * Get all lazy variables
     */
    public Map<String, LazyQuery> getLazyVariables() {
        return Map.copyOf(lazyVariables);
    }
    
    /**
     * Set a view definition
     */
    public void setView(String name, QueryNode query) {
        views.put(name, query);
    }
    
    /**
     * Get a view definition
     */
    public QueryNode getView(String name) {
        return views.get(name);
    }
    
    /**
     * Get all views
     */
    public Map<String, QueryNode> getViews() {
        return Map.copyOf(views);
    }
    
    /**
     * Cache a result with advanced caching features
     */
    public void cacheResult(String key, JfrTable result) {
        cacheResult(key, result, null);
    }
    
    /**
     * Cache a result with dependency tracking
     */
    public void cacheResult(String key, JfrTable result, Set<String> dependencies) {
        if (key == null || result == null) {
            return; // Don't cache null keys or results
        }
        
        // Enforce cache size limit by evicting oldest entries
        if (resultCache.size() >= cacheMaxSize) {
            evictOldestCacheEntries();
        }
        
        resultCache.put(key, result);
        
        if (dependencies != null && !dependencies.isEmpty()) {
            cacheDependencies.put(key, new HashSet<>(dependencies));
        }
    }
    
    /**
     * Get cached result with dependency checking
     * No TTL enforcement - cache persists between query invocations
     */
    public JfrTable getCachedResult(String key) {
        if (key == null) {
            cacheMisses++;
            return null;
        }
        
        JfrTable result = resultCache.get(key);
        if (result == null) {
            cacheMisses++;
            return null;
        }
        
        cacheHits++;
        return result;
    }
    
    /**
     * Invalidate cache entries that depend on a specific resource
     */
    public void invalidateCacheDependencies(String dependency) {
        if (dependency == null) {
            return; // Nothing to invalidate
        }
        
        Set<String> toRemove = new HashSet<>();
        
        for (Map.Entry<String, Set<String>> entry : cacheDependencies.entrySet()) {
            Set<String> dependencies = entry.getValue();
            if (dependencies != null && dependencies.contains(dependency)) {
                toRemove.add(entry.getKey());
            }
        }
        
        for (String key : toRemove) {
            resultCache.remove(key);
            cacheDependencies.remove(key);
            cacheEvictions++;
        }
    }
    
    /**
     * Evict cache entries to make room for new ones
     * Uses a simple random eviction strategy since we don't track access times
     */
    private void evictOldestCacheEntries() {
        if (resultCache.size() < cacheMaxSize) {
            return;
        }
        
        // Remove 10% of entries to make space
        int toRemove = Math.max(1, (int) (cacheMaxSize * 0.10));
        
        // Simple eviction - remove the first entries we encounter
        List<String> keys = new ArrayList<>(resultCache.keySet());
        for (int i = 0; i < toRemove && i < keys.size(); i++) {
            String key = keys.get(i);
            resultCache.remove(key);
            cacheDependencies.remove(key);
            cacheEvictions++;
        }
    }
    
    /**
     * Get cache statistics
     */
    public CacheStats getCacheStats() {
        return new CacheStats(cacheHits, cacheMisses, cacheEvictions, resultCache.size());
    }
    
    /**
     * Set cache configuration
     */
    public void setCacheConfig(long maxSize) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Cache max size must be positive, got: " + maxSize);
        }
        this.cacheMaxSize = maxSize;
    }
    
    /**
     * Set cache configuration (legacy method for backward compatibility)
     */
    public void setCacheConfig(long maxSize, long ttlMs) {
        if (maxSize <= 0) {
            throw new IllegalArgumentException("Cache max size must be positive, got: " + maxSize);
        }
        this.cacheMaxSize = maxSize;
        // TTL is ignored - no longer supported
    }
    
    /**
     * Get cache configuration
     */
    public CacheConfig getCacheConfig() {
        return new CacheConfig(cacheMaxSize, 0); // TTL is always 0 (disabled)
    }
    
    /**
     * Clear result cache
     */
    public void clearCache() {
        resultCache.clear();
        cacheDependencies.clear();
    }
    
    /**
     * Check if event type variables are initialized
     */
    public boolean areEventTypeVariablesInitialized() {
        return eventTypeVariablesInitialized;
    }
    
    /**
     * Set event type variables initialized flag
     */
    public void setEventTypeVariablesInitialized(boolean initialized) {
        this.eventTypeVariablesInitialized = initialized;
    }

    /**
     * Clear all state
     */
    public void clear() {
        localVariables.clear();
        intermediateResults.clear();
        currentResult = null;
        variables.clear();
        lazyVariables.clear();
        views.clear();
        resultCache.clear();
        cacheDependencies.clear();
        eventTypeVariablesInitialized = false;
        // Reset cache statistics
        cacheHits = 0;
        cacheMisses = 0;
        cacheEvictions = 0;
    }
    
    /**
     * Create a child context (for nested scopes)
     */
    public ExecutionContext createChildContext() {
        ExecutionContext child = new ExecutionContext(debugMode);
        // Child inherits local variables but can shadow them
        child.localVariables.putAll(this.localVariables);
        // Child shares global state (variables, views, cache, etc.)
        child.variables.putAll(this.variables);
        child.lazyVariables.putAll(this.lazyVariables);
        child.views.putAll(this.views);
        child.resultCache.putAll(this.resultCache);
        child.cacheDependencies.putAll(this.cacheDependencies);
        child.cacheMaxSize = this.cacheMaxSize;
        child.eventTypeVariablesInitialized = this.eventTypeVariablesInitialized;
        return child;
    }
    
    /**
     * Update cache metadata (timestamps and dependencies)
     */
    /**
     * Cache statistics for monitoring cache performance
     */
    public static class CacheStats {
        private final long hits;
        private final long misses;
        private final long evictions;
        private final int size;
        
        public CacheStats(long hits, long misses, long evictions, int size) {
            this.hits = hits;
            this.misses = misses;
            this.evictions = evictions;
            this.size = size;
        }
        
        public long getHits() { return hits; }
        public long getMisses() { return misses; }
        public long getEvictions() { return evictions; }
        public int getSize() { return size; }
        
        public double getHitRate() {
            long total = hits + misses;
            return total > 0 ? (double) hits / total : 0.0;
        }
        
        @Override
        public String toString() {
            return String.format("CacheStats{hits=%d, misses=%d, evictions=%d, size=%d, hitRate=%.2f%%}", 
                hits, misses, evictions, size, getHitRate() * 100);
        }
    }

    /**
     * Cache configuration for monitoring cache settings
     */
    public static class CacheConfig {
        private final long maxSize;
        private final long ttlMs;
        
        public CacheConfig(long maxSize, long ttlMs) {
            this.maxSize = maxSize;
            this.ttlMs = ttlMs;
        }
        
        public long getMaxSize() { return maxSize; }
        public long getTtlMs() { return ttlMs; }
        
        @Override
        public String toString() {
            return String.format("CacheConfig{maxSize=%d, ttlMs=%d}", maxSize, ttlMs);
        }
    }

    /**
     * Check if a variable exists
     */
    public boolean hasVariable(String name) {
        return variables.containsKey(name);
    }
    
    /**
     * Check if a lazy variable exists
     */
    public boolean hasLazyVariable(String name) {
        return lazyVariables.containsKey(name);
    }
    
    /**
     * Remove a variable
     */
    public void removeVariable(String name) {
        variables.remove(name);
    }
    
    /**
     * Remove a lazy variable
     */
    public void removeLazyVariable(String name) {
        lazyVariables.remove(name);
    }
    
    /**
     * Clear all variables
     */
    public void clearVariables() {
        variables.clear();
    }
    
    /**
     * Clear all lazy variables
     */
    public void clearLazyVariables() {
        lazyVariables.clear();
    }
    
    /**
     * Get cache hits count
     */
    public long getCacheHits() {
        return cacheHits;
    }
    
    /**
     * Get cache misses count
     */
    public long getCacheMisses() {
        return cacheMisses;
    }
    
    /**
     * Get executed queries count (approximated by cache operations)
     */
    public long getExecutedQueries() {
        return cacheHits + cacheMisses;
    }
}
