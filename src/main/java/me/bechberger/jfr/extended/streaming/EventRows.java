package me.bechberger.jfr.extended.streaming;

import me.bechberger.jfr.extended.table.JfrTable;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Interface for a collection of EventRows with Java Stream-like API.
 * This provides the primary abstraction for streaming JFR data processing.
 * 
 * Implementations:
 * - StreamingEventRows: Lazy evaluation, processes data on-demand
 * - MaterializedEventRows: Eagerly evaluated, data is already in memory
 * - SingleEventRows: Contains exactly one EventRow
 * 
 * @author JFR Query Streaming Architecture
 * @since 2.0
 */
public interface EventRows extends Iterable<EventRow> {
    
    // ===== CORE STREAM OPERATIONS =====
    
    /**
     * Filter rows using a predicate.
     * 
     * @param predicate the filter condition
     * @return filtered EventRows
     */
    EventRows filter(Predicate<EventRow> predicate);
    
    /**
     * Transform rows using a mapping function.
     * 
     * @param mapper the transformation function
     * @return mapped EventRows
     */
    <R> EventRows map(Function<EventRow, R> mapper);
    
    /**
     * FlatMap operation for nested data.
     * 
     * @param mapper function that maps each row to EventRows
     * @return flattened EventRows
     */
    EventRows flatMap(Function<EventRow, EventRows> mapper);
    
    /**
     * Limit the number of rows.
     * 
     * @param maxSize maximum number of rows
     * @return limited EventRows
     */
    EventRows limit(long maxSize);
    
    /**
     * Skip the first n rows.
     * 
     * @param n number of rows to skip
     * @return EventRows with first n rows skipped
     */
    EventRows skip(long n);
    
    /**
     * Sort rows using a comparator.
     * Note: This may force materialization for streaming implementations.
     * 
     * @param keyExtractor function to extract the sort key
     * @return sorted EventRows
     */
    <T extends Comparable<T>> EventRows sorted(Function<EventRow, T> keyExtractor);
    
    /**
     * Remove duplicate rows.
     * Note: This may force materialization for streaming implementations.
     * 
     * @return EventRows with duplicates removed
     */
    EventRows distinct();
    
    /**
     * Peek at each row (for debugging/logging).
     * 
     * @param action action to perform on each row
     * @return the same EventRows for chaining
     */
    EventRows peek(Consumer<EventRow> action);
    
    // ===== TERMINAL OPERATIONS =====
    
    /**
     * Count the number of rows.
     * 
     * @return the count
     */
    long count();
    
    /**
     * Check if any row matches the predicate.
     * 
     * @param predicate the condition to check
     * @return true if any row matches
     */
    boolean anyMatch(Predicate<EventRow> predicate);
    
    /**
     * Check if all rows match the predicate.
     * 
     * @param predicate the condition to check
     * @return true if all rows match
     */
    boolean allMatch(Predicate<EventRow> predicate);
    
    /**
     * Check if no rows match the predicate.
     * 
     * @param predicate the condition to check
     * @return true if no rows match
     */
    boolean noneMatch(Predicate<EventRow> predicate);
    
    /**
     * Find the first row.
     * 
     * @return Optional containing the first row, or empty if no rows
     */
    Optional<EventRow> findFirst();
    
    /**
     * Find any row (for parallel processing).
     * 
     * @return Optional containing any row, or empty if no rows
     */
    Optional<EventRow> findAny();
    
    /**
     * Reduce rows to a single value.
     * 
     * @param identity the identity value
     * @param accumulator the accumulation function
     * @return the reduced value
     */
    <T> T reduce(T identity, Function<T, Function<EventRow, T>> accumulator);
    
    /**
     * Collect rows using a Collector.
     * 
     * @param collector the collector
     * @return the collected result
     */
    <R, A> R collect(Collector<EventRow, A, R> collector);
    
    /**
     * Convert to a List (forces materialization).
     * 
     * @return List of EventRows
     */
    List<EventRow> toList();
    
    /**
     * Convert to a Java Stream.
     * 
     * @return Stream of EventRows
     */
    Stream<EventRow> stream();
    
    // ===== UTILITY METHODS =====
    
    /**
     * Check if this EventRows is empty.
     * 
     * @return true if empty
     */
    boolean isEmpty();
    
    /**
     * Get the schema/columns for the rows.
     * 
     * @return list of columns
     */
    List<JfrTable.Column> getSchema();
    
    /**
     * Convert to a JfrTable (forces materialization).
     * 
     * @return JfrTable representation
     */
    JfrTable toTable();
    
    /**
     * Check if this EventRows is materialized (data is in memory).
     * 
     * @return true if materialized, false if streaming
     */
    boolean isMaterialized();
    
    /**
     * Force materialization of all data.
     * For already materialized implementations, this is a no-op.
     * 
     * @return materialized EventRows
     */
    EventRows materialize();
    
    // ===== STATIC FACTORY METHODS =====
    
    /**
     * Create empty EventRows.
     * 
     * @param schema the schema for the empty rows
     * @return empty EventRows
     */
    static EventRows empty(List<JfrTable.Column> schema) {
        return new MaterializedEventRows(List.of(), schema);
    }
    
    /**
     * Create EventRows from a single row.
     * 
     * @param row the single row
     * @return EventRows containing one row
     */
    static EventRows of(EventRow row) {
        return new SingleEventRows(row);
    }
    
    /**
     * Create EventRows from multiple rows.
     * 
     * @param rows the rows
     * @return materialized EventRows
     */
    static EventRows of(List<EventRow> rows) {
        if (rows.isEmpty()) {
            throw new IllegalArgumentException("Cannot create EventRows from empty list without schema");
        }
        return new MaterializedEventRows(rows, rows.get(0).getColumns());
    }
    
    /**
     * Create EventRows from a Stream.
     * 
     * @param stream the stream of rows
     * @param schema the schema
     * @return streaming EventRows
     */
    static EventRows fromStream(Stream<EventRow> stream, List<JfrTable.Column> schema) {
        return new StreamingEventRows(stream, schema);
    }
    
    /**
     * Create EventRows from a JfrTable.
     * 
     * @param table the table
     * @return materialized EventRows
     */
    static EventRows fromTable(JfrTable table) {
        List<EventRow> rows = table.getRows().stream()
            .map(row -> (EventRow) new TableEventRow(row, table.getColumns()))
            .toList();
        return new MaterializedEventRows(rows, table.getColumns());
    }
}
