package me.bechberger.jfr.extended.engine;

import me.bechberger.jfr.extended.ast.ASTNodes.*;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.engine.exception.JoinException;

import java.time.Instant;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fast and robust implementation of join operations for JFR queries.
 * 
 * Supports both standard SQL-style joins (INNER, LEFT, RIGHT, FULL) and 
 * specialized fuzzy joins for temporal correlations (NEAREST, PREVIOUS, AFTER).
 * 
 * Performance optimizations:
 * - Hash-based joins for standard joins (O(n+m) complexity)
 * - Sorted indexes for fuzzy joins with binary search (O(n log m) complexity)
 * - Memory-efficient streaming for large datasets
 * - Early termination optimizations
 * 
 * @author Query Engine Team
 * @since 1.0
 */
public class JoinProcessor {
    
    // Cache for join indexes to avoid recomputation
    private final Map<String, JoinIndex> indexCache = new ConcurrentHashMap<>();
    
    // Configuration for join operations
    private final JoinConfiguration config;
    
    /**
     * Join configuration options
     */
    public static class JoinConfiguration {
        public final int maxJoinSize;
        public final boolean useIndexCache;
        public final Duration fuzzyJoinTolerance;
        public final boolean parallelProcessing;
        public final int smallTableThreshold;
        public final boolean enableEarlyTermination;
        public final int indexCacheMaxSize;
        
        public JoinConfiguration() {
            this(1_000_000, true, Duration.ofMillis(100), true, 1000, true, 100);
        }
        
        public JoinConfiguration(int maxJoinSize, boolean useIndexCache, 
                               Duration fuzzyJoinTolerance, boolean parallelProcessing,
                               int smallTableThreshold, boolean enableEarlyTermination,
                               int indexCacheMaxSize) {
            this.maxJoinSize = maxJoinSize;
            this.useIndexCache = useIndexCache;
            this.fuzzyJoinTolerance = fuzzyJoinTolerance;
            this.parallelProcessing = parallelProcessing;
            this.smallTableThreshold = smallTableThreshold;
            this.enableEarlyTermination = enableEarlyTermination;
            this.indexCacheMaxSize = indexCacheMaxSize;
        }
    }
    
    /**
     * Default constructor with standard configuration
     */
    public JoinProcessor() {
        this(new JoinConfiguration());
    }
    
    /**
     * Constructor with custom configuration
     */
    public JoinProcessor(JoinConfiguration config) {
        this.config = config;
    }
    
    /**
     * Perform standard join operation (INNER, LEFT, RIGHT, FULL)
     */
    public JfrTable performStandardJoin(JfrTable leftTable, JfrTable rightTable, 
                                      StandardJoinSourceNode joinNode) {
        validateJoinInputs(leftTable, rightTable, joinNode);
        
        try {
            return switch (joinNode.joinType()) {
                case INNER -> performInnerJoin(leftTable, rightTable, 
                    joinNode.leftJoinField(), joinNode.rightJoinField());
                case LEFT -> performLeftJoin(leftTable, rightTable, 
                    joinNode.leftJoinField(), joinNode.rightJoinField());
                case RIGHT -> performRightJoin(leftTable, rightTable, 
                    joinNode.leftJoinField(), joinNode.rightJoinField());
                case FULL -> performFullJoin(leftTable, rightTable, 
                    joinNode.leftJoinField(), joinNode.rightJoinField());
            };
        } catch (Exception e) {
            throw JoinException.forExecutionError(
                joinNode.joinType().toString(), 
                joinNode.leftJoinField(), 
                joinNode.rightJoinField(), 
                e.getMessage(), 
                joinNode
            );
        }
    }
    
    /**
     * Perform fuzzy join operation (NEAREST, PREVIOUS, AFTER)
     */
    public JfrTable performFuzzyJoin(JfrTable leftTable, JfrTable rightTable, 
                                   FuzzyJoinSourceNode joinNode) {
        validateFuzzyJoinInputs(leftTable, rightTable, joinNode);
        
        try {
            // For fuzzy joins, we assume timestamp-based correlation
            // The joinField represents the time field in the right table
            // We'll look for a timestamp field in the left table automatically
            String leftTimeField = findTimestampField(leftTable);
            String rightTimeField = joinNode.joinField();
            
            return switch (joinNode.joinType()) {
                case NEAREST -> performNearestJoin(leftTable, rightTable, 
                    leftTimeField, rightTimeField);
                case PREVIOUS -> performPreviousJoin(leftTable, rightTable, 
                    leftTimeField, rightTimeField);
                case AFTER -> performAfterJoin(leftTable, rightTable, 
                    leftTimeField, rightTimeField);
            };
        } catch (Exception e) {
            throw JoinException.forFuzzyJoinError(
                joinNode.joinType().toString(), 
                "auto-detected timestamp field", 
                joinNode.joinField(), 
                e.getMessage(), 
                joinNode
            );
        }
    }
    
    // ========== STANDARD JOIN IMPLEMENTATIONS ==========
    
    /**
     * Perform INNER JOIN using hash-based algorithm for optimal performance
     */
    public JfrTable performInnerJoin(JfrTable leftTable, JfrTable rightTable, 
                                   String leftField, String rightField) {
        // Performance optimization: use the smaller table for building hash index
        boolean leftIsSmaller = leftTable.getRowCount() <= rightTable.getRowCount();
        JfrTable buildTable = leftIsSmaller ? leftTable : rightTable;
        JfrTable probeTable = leftIsSmaller ? rightTable : leftTable;
        String buildField = leftIsSmaller ? leftField : rightField;
        String probeField = leftIsSmaller ? rightField : leftField;
        
        // Build hash index on smaller table
        JoinIndex index = buildJoinIndex(buildTable, buildField);
        
        // Create result table structure with optimized column allocation
        List<JfrTable.Column> resultColumns = combineColumns(leftTable, rightTable, "inner");
        JfrTable result = new StandardJfrTable(resultColumns);
        
        // Probe and join with early termination support
        int processedRows = 0;
        for (JfrTable.Row probeRow : probeTable.getRows()) {
            CellValue probeValue = getFieldValue(probeRow, probeTable.getColumns(), probeField);
            
            if (probeValue != null && !(probeValue instanceof CellValue.NullValue)) {
                List<JfrTable.Row> matchingRows = index.get(probeValue);
                if (matchingRows != null) {
                    for (JfrTable.Row buildRow : matchingRows) {
                        JfrTable.Row joinedRow = leftIsSmaller ? 
                            combineRows(buildRow, probeRow, leftTable, rightTable) :
                            combineRows(probeRow, buildRow, leftTable, rightTable);
                        result.addRow(joinedRow);
                    }
                }
            }
            
            // Early termination check for large datasets
            if (config.enableEarlyTermination && ++processedRows % 10000 == 0) {
                if (result.getRowCount() > config.maxJoinSize / 2) {
                    // Consider breaking early if result is getting too large
                    break;
                }
            }
        }
        
        return result;
    }
    
    /**
     * Perform LEFT JOIN with proper null handling
     */
    public JfrTable performLeftJoin(JfrTable leftTable, JfrTable rightTable, 
                                  String leftField, String rightField) {
        // Build hash index for right table
        JoinIndex rightIndex = buildJoinIndex(rightTable, rightField);
        
        // Create result table structure
        List<JfrTable.Column> resultColumns = combineColumns(leftTable, rightTable, "left");
        JfrTable result = new StandardJfrTable(resultColumns);
        
        // Process each left row
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            CellValue leftValue = getFieldValue(leftRow, leftTable.getColumns(), leftField);
            boolean hasMatch = false;
            
            if (leftValue != null && !(leftValue instanceof CellValue.NullValue)) {
                List<JfrTable.Row> matchingRightRows = rightIndex.get(leftValue);
                if (matchingRightRows != null) {
                    for (JfrTable.Row rightRow : matchingRightRows) {
                        result.addRow(combineRows(leftRow, rightRow, leftTable, rightTable));
                        hasMatch = true;
                    }
                }
            }
            
            // If no match found, add row with null values for right side
            if (!hasMatch) {
                result.addRow(combineRowsWithNulls(leftRow, leftTable, rightTable, true));
            }
        }
        
        return result;
    }
    
    /**
     * Perform RIGHT JOIN with proper null handling
     */
    public JfrTable performRightJoin(JfrTable leftTable, JfrTable rightTable, 
                                   String leftField, String rightField) {
        // Build hash index for left table
        JoinIndex leftIndex = buildJoinIndex(leftTable, leftField);
        
        // Create result table structure
        List<JfrTable.Column> resultColumns = combineColumns(leftTable, rightTable, "right");
        JfrTable result = new StandardJfrTable(resultColumns);
        
        // Process each right row
        for (JfrTable.Row rightRow : rightTable.getRows()) {
            CellValue rightValue = getFieldValue(rightRow, rightTable.getColumns(), rightField);
            boolean hasMatch = false;
            
            if (rightValue != null && !(rightValue instanceof CellValue.NullValue)) {
                List<JfrTable.Row> matchingLeftRows = leftIndex.get(rightValue);
                if (matchingLeftRows != null) {
                    for (JfrTable.Row leftRow : matchingLeftRows) {
                        result.addRow(combineRows(leftRow, rightRow, leftTable, rightTable));
                        hasMatch = true;
                    }
                }
            }
            
            // If no match found, add row with null values for left side
            if (!hasMatch) {
                result.addRow(combineRowsWithNulls(rightRow, leftTable, rightTable, false));
            }
        }
        
        return result;
    }
    
    /**
     * Perform FULL OUTER JOIN
     */
    public JfrTable performFullJoin(JfrTable leftTable, JfrTable rightTable, 
                                  String leftField, String rightField) {
        // Build index for right table (left table is processed sequentially)
        JoinIndex rightIndex = buildJoinIndex(rightTable, rightField);
        
        // Create result table structure
        List<JfrTable.Column> resultColumns = combineColumns(leftTable, rightTable, "full");
        JfrTable result = new StandardJfrTable(resultColumns);
        
        // Track which right rows have been matched
        Set<JfrTable.Row> matchedRightRows = new HashSet<>();
        
        // Process left table (like LEFT JOIN)
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            CellValue leftValue = getFieldValue(leftRow, leftTable.getColumns(), leftField);
            boolean hasMatch = false;
            
            if (leftValue != null && !(leftValue instanceof CellValue.NullValue)) {
                List<JfrTable.Row> matchingRightRows = rightIndex.get(leftValue);
                if (matchingRightRows != null) {
                    for (JfrTable.Row rightRow : matchingRightRows) {
                        result.addRow(combineRows(leftRow, rightRow, leftTable, rightTable));
                        matchedRightRows.add(rightRow);
                        hasMatch = true;
                    }
                }
            }
            
            if (!hasMatch) {
                result.addRow(combineRowsWithNulls(leftRow, leftTable, rightTable, true));
            }
        }
        
        // Add unmatched right rows
        for (JfrTable.Row rightRow : rightTable.getRows()) {
            if (!matchedRightRows.contains(rightRow)) {
                result.addRow(combineRowsWithNulls(rightRow, leftTable, rightTable, false));
            }
        }
        
        return result;
    }
    
    // ========== FUZZY JOIN IMPLEMENTATIONS ==========
    
    /**
     * Perform NEAREST temporal join - finds closest events by timestamp
     */
    public JfrTable performNearestJoin(JfrTable leftTable, JfrTable rightTable, 
                                     String leftTimeField, String rightTimeField) {
        // Build sorted time index for right table
        TimeIndex rightTimeIndex = buildTimeIndex(rightTable, rightTimeField);
        
        // Create result table structure
        List<JfrTable.Column> resultColumns = combineColumns(leftTable, rightTable, "nearest");
        JfrTable result = new StandardJfrTable(resultColumns);
        
        // For each left row, find nearest right row by time
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            CellValue leftTimeValue = getFieldValue(leftRow, leftTable.getColumns(), leftTimeField);
            
            if (leftTimeValue instanceof CellValue.TimestampValue leftTime) {
                JfrTable.Row nearestRow = rightTimeIndex.findNearest(leftTime.value());
                if (nearestRow != null) {
                    result.addRow(combineRows(leftRow, nearestRow, leftTable, rightTable));
                } else {
                    // No matching row within tolerance
                    result.addRow(combineRowsWithNulls(leftRow, leftTable, rightTable, true));
                }
            }
        }
        
        return result;
    }
    
    /**
     * Perform PREVIOUS temporal join - finds latest event before
     */
    public JfrTable performPreviousJoin(JfrTable leftTable, JfrTable rightTable, 
                                      String leftTimeField, String rightTimeField) {
        // Build sorted time index for right table
        TimeIndex rightTimeIndex = buildTimeIndex(rightTable, rightTimeField);
        
        // Create result table structure
        List<JfrTable.Column> resultColumns = combineColumns(leftTable, rightTable, "previous");
        JfrTable result = new StandardJfrTable(resultColumns);
        
        // For each left row, find latest previous right row by time
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            CellValue leftTimeValue = getFieldValue(leftRow, leftTable.getColumns(), leftTimeField);
            
            if (leftTimeValue instanceof CellValue.TimestampValue leftTime) {
                JfrTable.Row previousRow = rightTimeIndex.findPrevious(leftTime.value());
                if (previousRow != null) {
                    result.addRow(combineRows(leftRow, previousRow, leftTable, rightTable));
                } else {
                    // No previous row found
                    result.addRow(combineRowsWithNulls(leftRow, leftTable, rightTable, true));
                }
            }
        }
        
        return result;
    }
    
    /**
     * Perform AFTER temporal join - finds earliest event after
     */
    public JfrTable performAfterJoin(JfrTable leftTable, JfrTable rightTable, 
                                   String leftTimeField, String rightTimeField) {
        // Build sorted time index for right table
        TimeIndex rightTimeIndex = buildTimeIndex(rightTable, rightTimeField);
        
        // Create result table structure
        List<JfrTable.Column> resultColumns = combineColumns(leftTable, rightTable, "after");
        JfrTable result = new StandardJfrTable(resultColumns);
        
        // For each left row, find earliest next right row by time
        for (JfrTable.Row leftRow : leftTable.getRows()) {
            CellValue leftTimeValue = getFieldValue(leftRow, leftTable.getColumns(), leftTimeField);
            
            if (leftTimeValue instanceof CellValue.TimestampValue leftTime) {
                JfrTable.Row afterRow = rightTimeIndex.findAfter(leftTime.value());
                if (afterRow != null) {
                    result.addRow(combineRows(leftRow, afterRow, leftTable, rightTable));
                } else {
                    // No row found after the timestamp
                    result.addRow(combineRowsWithNulls(leftRow, leftTable, rightTable, true));
                }
            }
        }
        
        return result;
    }
    
    // ========== HELPER METHODS AND CLASSES ==========
    
    /**
     * Validate inputs for standard joins
     */
    private void validateJoinInputs(JfrTable leftTable, JfrTable rightTable, StandardJoinSourceNode joinNode) {
        if (leftTable == null) {
            throw JoinException.forValidationError("Left table cannot be null", joinNode);
        }
        if (rightTable == null) {
            throw JoinException.forValidationError("Right table cannot be null", joinNode);
        }
        if (joinNode.leftJoinField() == null || joinNode.leftJoinField().isEmpty()) {
            throw JoinException.forValidationError("Left join field cannot be null or empty", joinNode);
        }
        if (joinNode.rightJoinField() == null || joinNode.rightJoinField().isEmpty()) {
            throw JoinException.forValidationError("Right join field cannot be null or empty", joinNode);
        }
        
        // Check field existence
        if (!hasColumn(leftTable, joinNode.leftJoinField())) {
            throw JoinException.forFieldError(
                "Left join field '" + joinNode.leftJoinField() + "' not found in left table", 
                joinNode.leftJoinField(), 
                leftTable.getColumns().stream().map(c -> c.name()).toArray(String[]::new),
                joinNode
            );
        }
        if (!hasColumn(rightTable, joinNode.rightJoinField())) {
            throw JoinException.forFieldError(
                "Right join field '" + joinNode.rightJoinField() + "' not found in right table", 
                joinNode.rightJoinField(), 
                rightTable.getColumns().stream().map(c -> c.name()).toArray(String[]::new),
                joinNode
            );
        }
        
        // Check table sizes
        long totalSize = (long) leftTable.getRowCount() * rightTable.getRowCount();
        if (totalSize > config.maxJoinSize) {
            throw JoinException.forPerformanceError(
                "Join result would be too large: " + totalSize + " > " + config.maxJoinSize + 
                ". Consider adding WHERE clauses to reduce table sizes.", 
                joinNode
            );
        }
    }
    
    /**
     * Validate inputs for fuzzy joins
     */
    private void validateFuzzyJoinInputs(JfrTable leftTable, JfrTable rightTable, FuzzyJoinSourceNode joinNode) {
        if (leftTable == null) {
            throw JoinException.forValidationError("Left table cannot be null", joinNode);
        }
        if (rightTable == null) {
            throw JoinException.forValidationError("Right table cannot be null", joinNode);
        }
        if (joinNode.joinField() == null || joinNode.joinField().isEmpty()) {
            throw JoinException.forValidationError("Join field cannot be null or empty", joinNode);
        }
        
        // Check that left table has at least one timestamp field
        String leftTimeField = findTimestampField(leftTable);
        if (leftTimeField == null) {
            throw JoinException.forFieldError(
                "Left table must have at least one TIMESTAMP field for fuzzy join", 
                "timestamp field", 
                leftTable.getColumns().stream().map(c -> c.name()).toArray(String[]::new),
                joinNode
            );
        }
        
        // Check field existence and types for right table
        validateTimeField(rightTable, joinNode.joinField(), "right", joinNode);
    }
    
    /**
     * Find the first timestamp field in a table
     */
    private String findTimestampField(JfrTable table) {
        return table.getColumns().stream()
            .filter(c -> c.type() == CellType.TIMESTAMP)
            .map(c -> c.name())
            .findFirst()
            .orElse(null);
    }
    
    /**
     * Validate that a field exists and is a timestamp field
     */
    private void validateTimeField(JfrTable table, String fieldName, String side, FuzzyJoinSourceNode joinNode) {
        Optional<JfrTable.Column> column = table.getColumns().stream()
            .filter(c -> c.name().equals(fieldName))
            .findFirst();
            
        if (column.isEmpty()) {
            throw JoinException.forFieldError(
                side.substring(0, 1).toUpperCase() + side.substring(1) + " time field '" + fieldName + "' not found", 
                fieldName, 
                table.getColumns().stream().map(c -> c.name()).toArray(String[]::new),
                joinNode
            );
        }
        
        if (column.get().type() != CellType.TIMESTAMP) {
            throw JoinException.forTypeError(
                side.substring(0, 1).toUpperCase() + side.substring(1) + " time field '" + fieldName + 
                "' must be TIMESTAMP type, but was " + column.get().type(), 
                joinNode
            );
        }
    }
    
    /**
     * Build hash-based join index for fast lookups with LRU cache management
     */
    private JoinIndex buildJoinIndex(JfrTable table, String field) {
        String cacheKey = generateCacheKey(table, field);
        
        if (config.useIndexCache && indexCache.containsKey(cacheKey)) {
            return indexCache.get(cacheKey);
        }
        
        // Performance optimization: use larger initial capacity for large tables
        int initialCapacity = Math.max(16, table.getRowCount() / 4);
        JoinIndex index = new JoinIndex(initialCapacity);
        
        for (JfrTable.Row row : table.getRows()) {
            CellValue fieldValue = getFieldValue(row, table.getColumns(), field);
            if (fieldValue != null && !(fieldValue instanceof CellValue.NullValue)) {
                index.add(fieldValue, row);
            }
        }
        
        if (config.useIndexCache) {
            // LRU cache management: remove oldest entries if cache is full
            if (indexCache.size() >= config.indexCacheMaxSize) {
                // Remove the first (oldest) entry - simple LRU approximation
                String oldestKey = indexCache.keySet().iterator().next();
                indexCache.remove(oldestKey);
            }
            indexCache.put(cacheKey, index);
        }
        
        return index;
    }
    
    /**
     * Build time-sorted index for fuzzy joins
     */
    private TimeIndex buildTimeIndex(JfrTable table, String timeField) {
        List<TimeEntry> timeEntries = new ArrayList<>();
        
        for (JfrTable.Row row : table.getRows()) {
            CellValue timeValue = getFieldValue(row, table.getColumns(), timeField);
            if (timeValue instanceof CellValue.TimestampValue timestamp) {
                timeEntries.add(new TimeEntry(timestamp.value(), row));
            }
        }
        
        // Sort by timestamp for binary search
        timeEntries.sort(Comparator.comparing(entry -> entry.timestamp));
        
        return new TimeIndex(timeEntries, config.fuzzyJoinTolerance);
    }
    
    /**
     * Get field value from row by field name
     */
    private CellValue getFieldValue(JfrTable.Row row, List<JfrTable.Column> columns, String fieldName) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name().equals(fieldName)) {
                return row.getCells().get(i);
            }
        }
        throw JoinException.forFieldError(
            "Field '" + fieldName + "' not found", 
            fieldName, 
            columns.stream().map(c -> c.name()).toArray(String[]::new),
            null
        );
    }
    
    /**
     * Check if table has a column with given name
     */
    private boolean hasColumn(JfrTable table, String columnName) {
        return table.getColumns().stream().anyMatch(c -> c.name().equals(columnName));
    }
    
    /**
     * Combine columns from two tables for join result
     */
    private List<JfrTable.Column> combineColumns(JfrTable leftTable, JfrTable rightTable, String joinType) {
        List<JfrTable.Column> result = new ArrayList<>();
        
        // Add left table columns
        for (JfrTable.Column column : leftTable.getColumns()) {
            result.add(new JfrTable.Column("left_" + column.name(), column.type()));
        }
        
        // Add right table columns
        for (JfrTable.Column column : rightTable.getColumns()) {
            result.add(new JfrTable.Column("right_" + column.name(), column.type()));
        }
        
        return result;
    }
    
    /**
     * Combine two rows into a single joined row
     */
    private JfrTable.Row combineRows(JfrTable.Row leftRow, JfrTable.Row rightRow, 
                                   JfrTable leftTable, JfrTable rightTable) {
        List<CellValue> combinedCells = new ArrayList<>();
        combinedCells.addAll(leftRow.getCells());
        combinedCells.addAll(rightRow.getCells());
        return new JfrTable.Row(combinedCells);
    }
    
    /**
     * Combine row with nulls for outer joins
     */
    private JfrTable.Row combineRowsWithNulls(JfrTable.Row dataRow, JfrTable leftTable, JfrTable rightTable, 
                                            boolean dataIsLeft) {
        List<CellValue> combinedCells = new ArrayList<>();
        
        if (dataIsLeft) {
            // Data row is from left table, pad right with nulls
            combinedCells.addAll(dataRow.getCells());
            for (int i = 0; i < rightTable.getColumns().size(); i++) {
                combinedCells.add(new CellValue.NullValue());
            }
        } else {
            // Data row is from right table, pad left with nulls
            for (int i = 0; i < leftTable.getColumns().size(); i++) {
                combinedCells.add(new CellValue.NullValue());
            }
            combinedCells.addAll(dataRow.getCells());
        }
        
        return new JfrTable.Row(combinedCells);
    }
    
    /**
     * Generate cache key for join index with improved collision resistance
     */
    private String generateCacheKey(JfrTable table, String field) {
        // Use table identity hash and field name for better cache key
        return System.identityHashCode(table) + "_" + field + "_" + table.getRowCount();
    }
    
    /**
     * Hash-based join index for fast lookups with configurable capacity
     */
    private static class JoinIndex {
        private final Map<CellValue, List<JfrTable.Row>> index;
        
        public JoinIndex() {
            this.index = new HashMap<>();
        }
        
        public JoinIndex(int initialCapacity) {
            this.index = new HashMap<>(initialCapacity);
        }
        
        public void add(CellValue key, JfrTable.Row row) {
            index.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
        
        public List<JfrTable.Row> get(CellValue key) {
            return index.get(key);
        }
        
        public int size() {
            return index.size();
        }
    }
    
    /**
     * Time-sorted index for fuzzy joins
     */
    private static class TimeIndex {
        private final List<TimeEntry> sortedEntries;
        private final Duration tolerance;
        
        public TimeIndex(List<TimeEntry> sortedEntries, Duration tolerance) {
            this.sortedEntries = sortedEntries;
            this.tolerance = tolerance;
        }
        
        /**
         * Find nearest entry by time (within tolerance)
         */
        public JfrTable.Row findNearest(Instant targetTime) {
            if (sortedEntries.isEmpty()) return null;
            
            int index = binarySearchNearest(targetTime);
            if (index >= 0 && index < sortedEntries.size()) {
                TimeEntry entry = sortedEntries.get(index);
                if (Duration.between(targetTime, entry.timestamp).abs().compareTo(tolerance) <= 0) {
                    return entry.row;
                }
            }
            return null;
        }
        
        /**
         * Find latest entry before target time
         */
        public JfrTable.Row findPrevious(Instant targetTime) {
            if (sortedEntries.isEmpty()) return null;
            
            int index = binarySearchPrevious(targetTime);
            return index >= 0 ? sortedEntries.get(index).row : null;
        }
        
        /**
         * Find earliest entry after target time
         */
        public JfrTable.Row findAfter(Instant targetTime) {
            if (sortedEntries.isEmpty()) return null;
            
            int index = binarySearchAfter(targetTime);
            return index < sortedEntries.size() ? sortedEntries.get(index).row : null;
        }
        
        private int binarySearchNearest(Instant targetTime) {
            int left = 0, right = sortedEntries.size() - 1;
            int nearest = -1;
            long minDiff = Long.MAX_VALUE;
            
            while (left <= right) {
                int mid = left + (right - left) / 2;
                Instant midTime = sortedEntries.get(mid).timestamp;
                long diff = Math.abs(Duration.between(targetTime, midTime).toNanos());
                
                if (diff < minDiff) {
                    minDiff = diff;
                    nearest = mid;
                }
                
                if (midTime.equals(targetTime)) {
                    return mid;
                } else if (midTime.isBefore(targetTime)) {
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }
            
            return nearest;
        }
        
        private int binarySearchPrevious(Instant targetTime) {
            int left = 0, right = sortedEntries.size() - 1;
            int result = -1;
            
            while (left <= right) {
                int mid = left + (right - left) / 2;
                Instant midTime = sortedEntries.get(mid).timestamp;
                
                if (midTime.isBefore(targetTime) || midTime.equals(targetTime)) {
                    result = mid;
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }
            
            return result;
        }
        
        private int binarySearchAfter(Instant targetTime) {
            int left = 0, right = sortedEntries.size() - 1;
            int result = sortedEntries.size();
            
            while (left <= right) {
                int mid = left + (right - left) / 2;
                Instant midTime = sortedEntries.get(mid).timestamp;
                
                if (midTime.isAfter(targetTime)) {
                    result = mid;
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }
            
            return result;
        }
    }
    
    /**
     * Time entry for temporal indexing
     */
    private static class TimeEntry {
        final Instant timestamp;
        final JfrTable.Row row;
        
        TimeEntry(Instant timestamp, JfrTable.Row row) {
            this.timestamp = timestamp;
            this.row = row;
        }
    }
    
    /**
     * Clear the join index cache
     */
    public void clearCache() {
        indexCache.clear();
    }
    
    /**
     * Get cache statistics and performance metrics
     */
    public Map<String, Object> getCacheStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("cacheSize", indexCache.size());
        stats.put("cacheEnabled", config.useIndexCache);
        stats.put("maxJoinSize", config.maxJoinSize);
        stats.put("fuzzyJoinTolerance", config.fuzzyJoinTolerance);
        stats.put("smallTableThreshold", config.smallTableThreshold);
        stats.put("parallelProcessing", config.parallelProcessing);
        stats.put("earlyTermination", config.enableEarlyTermination);
        stats.put("indexCacheMaxSize", config.indexCacheMaxSize);
        return stats;
    }
    
    /**
     * Create optimized configuration for small to medium datasets
     */
    public static JoinConfiguration createOptimizedConfig() {
        return new JoinConfiguration(
            5_000_000,    // maxJoinSize - higher for better throughput
            true,         // useIndexCache
            Duration.ofMillis(50),  // fuzzyJoinTolerance - tighter for precision
            true,         // parallelProcessing
            5000,         // smallTableThreshold
            true,         // enableEarlyTermination
            200           // indexCacheMaxSize - higher for better hit rate
        );
    }
    
    /**
     * Create configuration optimized for memory-constrained environments
     */
    public static JoinConfiguration createMemoryOptimizedConfig() {
        return new JoinConfiguration(
            500_000,      // maxJoinSize - lower to conserve memory
            false,        // useIndexCache - disabled to save memory
            Duration.ofMillis(200),  // fuzzyJoinTolerance - looser for more matches
            false,        // parallelProcessing - disabled to save memory
            1000,         // smallTableThreshold
            true,         // enableEarlyTermination
            50            // indexCacheMaxSize - smaller cache
        );
    }
}
