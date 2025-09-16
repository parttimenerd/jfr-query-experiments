package me.bechberger.jfr.extended.streaming;

import me.bechberger.jfr.extended.table.JfrTable;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Streaming EventRows implementation that processes data lazily.
 * This provides true streaming behavior without materializing all data
 * until a terminal operation is called.
 * 
 * @author JFR Query Streaming Architecture
 * @since 2.0
 */
public class StreamingEventRows implements EventRows {
    
    private final Stream<EventRow> stream;
    private final List<JfrTable.Column> schema;
    private MaterializedEventRows cached = null;
    
    public StreamingEventRows(Stream<EventRow> stream, List<JfrTable.Column> schema) {
        this.stream = stream;
        this.schema = new ArrayList<>(schema);
    }
    
    @Override
    public EventRows filter(Predicate<EventRow> predicate) {
        return new StreamingEventRows(stream.filter(predicate), schema);
    }
    
    @Override
    public <R> EventRows map(Function<EventRow, R> mapper) {
        Stream<EventRow> mappedStream = stream.map(mapper).map(r -> (EventRow) r);
        return new StreamingEventRows(mappedStream, schema);
    }
    
    @Override
    public EventRows flatMap(Function<EventRow, EventRows> mapper) {
        Stream<EventRow> flatMappedStream = stream.flatMap(row -> mapper.apply(row).stream());
        return new StreamingEventRows(flatMappedStream, schema);
    }
    
    @Override
    public EventRows limit(long maxSize) {
        return new StreamingEventRows(stream.limit(maxSize), schema);
    }
    
    @Override
    public EventRows skip(long n) {
        return new StreamingEventRows(stream.skip(n), schema);
    }
    
    @Override
    public <T extends Comparable<T>> EventRows sorted(Function<EventRow, T> keyExtractor) {
        // Sorting requires materialization
        materializeIfNeeded();
        return cached.sorted(keyExtractor);
    }
    
    @Override
    public EventRows distinct() {
        // Distinct requires materialization
        materializeIfNeeded();
        return cached.distinct();
    }
    
    @Override
    public EventRows peek(Consumer<EventRow> action) {
        return new StreamingEventRows(stream.peek(action), schema);
    }
    
    @Override
    public long count() {
        if (cached != null) {
            return cached.count();
        }
        return stream.count();
    }
    
    @Override
    public boolean anyMatch(Predicate<EventRow> predicate) {
        if (cached != null) {
            return cached.anyMatch(predicate);
        }
        return stream.anyMatch(predicate);
    }
    
    @Override
    public boolean allMatch(Predicate<EventRow> predicate) {
        if (cached != null) {
            return cached.allMatch(predicate);
        }
        return stream.allMatch(predicate);
    }
    
    @Override
    public boolean noneMatch(Predicate<EventRow> predicate) {
        if (cached != null) {
            return cached.noneMatch(predicate);
        }
        return stream.noneMatch(predicate);
    }
    
    @Override
    public Optional<EventRow> findFirst() {
        if (cached != null) {
            return cached.findFirst();
        }
        return stream.findFirst();
    }
    
    @Override
    public Optional<EventRow> findAny() {
        if (cached != null) {
            return cached.findAny();
        }
        return stream.findAny();
    }
    
    @Override
    public <T> T reduce(T identity, Function<T, Function<EventRow, T>> accumulator) {
        if (cached != null) {
            return cached.reduce(identity, accumulator);
        }
        
        // For streaming, we need to implement the reduction manually
        T result = identity;
        Iterator<EventRow> iterator = stream.iterator();
        while (iterator.hasNext()) {
            result = accumulator.apply(result).apply(iterator.next());
        }
        return result;
    }
    
    @Override
    public <R, A> R collect(Collector<EventRow, A, R> collector) {
        if (cached != null) {
            return cached.collect(collector);
        }
        return stream.collect(collector);
    }
    
    @Override
    public List<EventRow> toList() {
        materializeIfNeeded();
        return cached.toList();
    }
    
    @Override
    public Stream<EventRow> stream() {
        if (cached != null) {
            return cached.stream();
        }
        return stream;
    }
    
    @Override
    public boolean isEmpty() {
        if (cached != null) {
            return cached.isEmpty();
        }
        return !stream.findAny().isPresent();
    }
    
    @Override
    public List<JfrTable.Column> getSchema() {
        return new ArrayList<>(schema);
    }
    
    @Override
    public JfrTable toTable() {
        materializeIfNeeded();
        return cached.toTable();
    }
    
    @Override
    public boolean isMaterialized() {
        return cached != null;
    }
    
    @Override
    public EventRows materialize() {
        materializeIfNeeded();
        return cached;
    }
    
    @Override
    public Iterator<EventRow> iterator() {
        if (cached != null) {
            return cached.iterator();
        }
        return stream.iterator();
    }
    
    /**
     * Force materialization of the stream into memory.
     */
    private void materializeIfNeeded() {
        if (cached == null) {
            List<EventRow> rows = stream.toList();
            cached = new MaterializedEventRows(rows, schema);
        }
    }
    
    /**
     * Create a new StreamingEventRows with a fresh stream.
     * This is useful when the original stream has been consumed.
     * 
     * @param newStream the new stream
     * @return new StreamingEventRows instance
     */
    public StreamingEventRows withNewStream(Stream<EventRow> newStream) {
        return new StreamingEventRows(newStream, schema);
    }
    
    @Override
    public String toString() {
        return "StreamingEventRows{schema=" + schema.size() + " columns, materialized=" + isMaterialized() + "}";
    }
}
