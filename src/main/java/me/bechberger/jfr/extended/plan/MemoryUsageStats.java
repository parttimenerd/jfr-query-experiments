package me.bechberger.jfr.extended.plan;

/**
 * Memory usage tracking for streaming operations.
 * 
 * This class tracks memory consumption during query plan execution
 * to enable memory-bounded streaming operations and prevent OOM errors
 * for large result sets.
 * 
 * @author JFR Query Plan Architecture
 * @since 2.0
 */
public class MemoryUsageStats {
    private long heapUsed;
    private long heapMax;
    private long heapCommitted;
    private long offHeapUsed;
    private long streamBufferSize;
    private long rowsProcessed;
    private long estimatedMemoryPerRow;
    
    public MemoryUsageStats() {
        this.heapUsed = 0;
        this.heapMax = Runtime.getRuntime().maxMemory();
        this.heapCommitted = Runtime.getRuntime().totalMemory();
        this.offHeapUsed = 0;
        this.streamBufferSize = 0;
        this.rowsProcessed = 0;
        this.estimatedMemoryPerRow = 0;
    }
    
    /**
     * Update memory usage statistics.
     */
    public void updateMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        this.heapUsed = runtime.totalMemory() - runtime.freeMemory();
        this.heapCommitted = runtime.totalMemory();
        this.heapMax = runtime.maxMemory();
    }
    
    /**
     * Record processing of additional rows for memory estimation.
     */
    public void recordRowsProcessed(long additionalRows) {
        this.rowsProcessed += additionalRows;
        if (rowsProcessed > 0) {
            this.estimatedMemoryPerRow = heapUsed / rowsProcessed;
        }
    }
    
    /**
     * Get current heap memory usage in bytes.
     */
    public long getHeapUsed() {
        return heapUsed;
    }
    
    /**
     * Get maximum heap memory available in bytes.
     */
    public long getHeapMax() {
        return heapMax;
    }
    
    /**
     * Get committed heap memory in bytes.
     */
    public long getHeapCommitted() {
        return heapCommitted;
    }
    
    /**
     * Get current heap usage as a percentage (0.0 to 1.0).
     */
    public double getHeapUsagePercentage() {
        if (heapMax == 0) return 0.0;
        return (double) heapUsed / heapMax;
    }
    
    /**
     * Check if memory usage is approaching critical levels.
     */
    public boolean isMemoryPressureHigh() {
        return getHeapUsagePercentage() > 0.8; // 80% threshold
    }
    
    /**
     * Get estimated memory usage per row in bytes.
     */
    public long getEstimatedMemoryPerRow() {
        return estimatedMemoryPerRow;
    }
    
    /**
     * Get total number of rows processed.
     */
    public long getRowsProcessed() {
        return rowsProcessed;
    }
    
    /**
     * Set stream buffer size for tracking streaming operations.
     */
    public void setStreamBufferSize(long bufferSize) {
        this.streamBufferSize = bufferSize;
    }
    
    /**
     * Get current stream buffer size.
     */
    public long getStreamBufferSize() {
        return streamBufferSize;
    }
    
    /**
     * Create a snapshot of current memory statistics.
     */
    public MemorySnapshot createSnapshot() {
        updateMemoryUsage();
        return new MemorySnapshot(
            heapUsed, heapMax, heapCommitted, 
            offHeapUsed, streamBufferSize, 
            rowsProcessed, estimatedMemoryPerRow
        );
    }
    
    /**
     * Immutable snapshot of memory usage at a point in time.
     */
    public static class MemorySnapshot {
        public final long heapUsed;
        public final long heapMax;
        public final long heapCommitted;
        public final long offHeapUsed;
        public final long streamBufferSize;
        public final long rowsProcessed;
        public final long estimatedMemoryPerRow;
        
        public MemorySnapshot(long heapUsed, long heapMax, long heapCommitted,
                            long offHeapUsed, long streamBufferSize,
                            long rowsProcessed, long estimatedMemoryPerRow) {
            this.heapUsed = heapUsed;
            this.heapMax = heapMax;
            this.heapCommitted = heapCommitted;
            this.offHeapUsed = offHeapUsed;
            this.streamBufferSize = streamBufferSize;
            this.rowsProcessed = rowsProcessed;
            this.estimatedMemoryPerRow = estimatedMemoryPerRow;
        }
        
        @Override
        public String toString() {
            return String.format(
                "MemorySnapshot{heapUsed=%d, heapMax=%d, usage=%.1f%%, rows=%d, memPerRow=%d}",
                heapUsed, heapMax, 
                heapMax > 0 ? (100.0 * heapUsed / heapMax) : 0.0,
                rowsProcessed, estimatedMemoryPerRow
            );
        }
    }
    
    @Override
    public String toString() {
        return String.format(
            "MemoryUsageStats{heapUsed=%d, heapMax=%d, usage=%.1f%%, pressure=%s, rows=%d}",
            heapUsed, heapMax, getHeapUsagePercentage() * 100,
            isMemoryPressureHigh() ? "HIGH" : "NORMAL",
            rowsProcessed
        );
    }
}
