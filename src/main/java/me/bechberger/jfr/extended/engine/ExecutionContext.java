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
    private final Map<String, Object> variables = new HashMap<>();
    private final Map<String, LazyQuery> lazyVariables = new HashMap<>();
    private final Map<String, QueryNode> views = new HashMap<>();
    private final Map<String, JfrTable> resultCache = new HashMap<>();
    private final Map<String, Long> cacheTimestamps = new HashMap<>(); // For TTL
    private final Map<String, Set<String>> cacheDependencies = new HashMap<>(); // For dependency tracking
    private long cacheMaxSize = 1000; // Default max cache entries
    private long cacheTtlMs = 5 * 60 * 1000; // 5 minutes default TTL
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
        // Enforce cache size limit by evicting oldest entries
        if (resultCache.size() >= cacheMaxSize) {
            evictOldestCacheEntries();
        }
        
        resultCache.put(key, result);
        cacheTimestamps.put(key, System.currentTimeMillis());
        
        if (dependencies != null && !dependencies.isEmpty()) {
            cacheDependencies.put(key, new HashSet<>(dependencies));
        }
        
        cacheMisses++; // This was a miss that we're now caching
    }
    
    /**
     * Get cached result with TTL and dependency checking
     */
    public JfrTable getCachedResult(String key) {
        JfrTable result = resultCache.get(key);
        if (result == null) {
            cacheMisses++;
            return null;
        }
        
        // Check TTL
        Long timestamp = cacheTimestamps.get(key);
        if (timestamp != null && System.currentTimeMillis() - timestamp > cacheTtlMs) {
            // Entry has expired
            resultCache.remove(key);
            cacheTimestamps.remove(key);
            cacheDependencies.remove(key);
            cacheEvictions++;
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
        Set<String> toRemove = new HashSet<>();
        
        for (Map.Entry<String, Set<String>> entry : cacheDependencies.entrySet()) {
            if (entry.getValue().contains(dependency)) {
                toRemove.add(entry.getKey());
            }
        }
        
        for (String key : toRemove) {
            resultCache.remove(key);
            cacheTimestamps.remove(key);
            cacheDependencies.remove(key);
            cacheEvictions++;
        }
    }
    
    /**
     * Evict the oldest cache entries to make room for new ones
     */
    private void evictOldestCacheEntries() {
        if (resultCache.size() < cacheMaxSize) {
            return;
        }
        
        // Remove 25% of the oldest entries
        int toRemove = Math.max(1, (int) (cacheMaxSize * 0.25));
        
        List<Map.Entry<String, Long>> timestampEntries = new ArrayList<>(cacheTimestamps.entrySet());
        timestampEntries.sort(Map.Entry.comparingByValue());
        
        for (int i = 0; i < toRemove && i < timestampEntries.size(); i++) {
            String key = timestampEntries.get(i).getKey();
            resultCache.remove(key);
            cacheTimestamps.remove(key);
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
    public void setCacheConfig(long maxSize, long ttlMs) {
        this.cacheMaxSize = maxSize;
        this.cacheTtlMs = ttlMs;
    }
    
    /**
     * Clear result cache
     */
    public void clearCache() {
        resultCache.clear();
        cacheTimestamps.clear();
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
        cacheTimestamps.clear();
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
        child.cacheTimestamps.putAll(this.cacheTimestamps);
        child.cacheDependencies.putAll(this.cacheDependencies);
        child.cacheMaxSize = this.cacheMaxSize;
        child.cacheTtlMs = this.cacheTtlMs;
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
}
