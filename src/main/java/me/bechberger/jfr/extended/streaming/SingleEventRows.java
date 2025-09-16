package me.bechberger.jfr.extended.streaming;

import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellValue;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * EventRows implementation that contains exactly one EventRow.
 * This is an optimized implementation for single-row operations.
 * 
 * @author JFR Query Streaming Architecture
 * @since 2.0
 */
public class SingleEventRows implements EventRows {
    
    private final EventRow row;
    
    public SingleEventRows(EventRow row) {
        this.row = Objects.requireNonNull(row, "EventRow cannot be null");
    }
    
    @Override
    public EventRows filter(Predicate<EventRow> predicate) {
        if (predicate.test(row)) {
            return this;
        } else {
            return new MaterializedEventRows(List.of(), row.getColumns());
        }
    }
    
    @Override
    public <R> EventRows map(Function<EventRow, R> mapper) {
        R mapped = mapper.apply(row);
        if (mapped instanceof EventRow mappedRow) {
            return new SingleEventRows(mappedRow);
        } else {
            // This shouldn't happen if mapper returns EventRow, but handle gracefully
            return new MaterializedEventRows(List.of(), row.getColumns());
        }
    }
    
    @Override
    public EventRows flatMap(Function<EventRow, EventRows> mapper) {
        return mapper.apply(row);
    }
    
    @Override
    public EventRows limit(long maxSize) {
        return maxSize > 0 ? this : new MaterializedEventRows(List.of(), row.getColumns());
    }
    
    @Override
    public EventRows skip(long n) {
        return n > 0 ? new MaterializedEventRows(List.of(), row.getColumns()) : this;
    }
    
    @Override
    public <T extends Comparable<T>> EventRows sorted(Function<EventRow, T> keyExtractor) {
        return this; // Single row is already "sorted"
    }
    
    @Override
    public EventRows distinct() {
        return this; // Single row is already distinct
    }
    
    @Override
    public EventRows peek(Consumer<EventRow> action) {
        action.accept(row);
        return this;
    }
    
    @Override
    public long count() {
        return 1;
    }
    
    @Override
    public boolean anyMatch(Predicate<EventRow> predicate) {
        return predicate.test(row);
    }
    
    @Override
    public boolean allMatch(Predicate<EventRow> predicate) {
        return predicate.test(row);
    }
    
    @Override
    public boolean noneMatch(Predicate<EventRow> predicate) {
        return !predicate.test(row);
    }
    
    @Override
    public Optional<EventRow> findFirst() {
        return Optional.of(row);
    }
    
    @Override
    public Optional<EventRow> findAny() {
        return Optional.of(row);
    }
    
    @Override
    public <T> T reduce(T identity, Function<T, Function<EventRow, T>> accumulator) {
        return accumulator.apply(identity).apply(row);
    }
    
    @Override
    public <R, A> R collect(Collector<EventRow, A, R> collector) {
        A container = collector.supplier().get();
        collector.accumulator().accept(container, row);
        return collector.finisher().apply(container);
    }
    
    @Override
    public List<EventRow> toList() {
        return List.of(row);
    }
    
    @Override
    public Stream<EventRow> stream() {
        return Stream.of(row);
    }
    
    @Override
    public boolean isEmpty() {
        return false;
    }
    
    @Override
    public List<JfrTable.Column> getSchema() {
        return row.getColumns();
    }
    
    @Override
    public JfrTable toTable() {
        StandardJfrTable table = new StandardJfrTable(row.getColumns());
        List<CellValue> cells = new ArrayList<>();
        for (int i = 0; i < row.getFieldCount(); i++) {
            cells.add(CellValue.of(row.getValue(i)));
        }
        table.addRow(new JfrTable.Row(cells));
        return table;
    }
    
    @Override
    public boolean isMaterialized() {
        return true; // Single row is always materialized
    }
    
    @Override
    public EventRows materialize() {
        return this; // Already materialized
    }
    
    @Override
    public Iterator<EventRow> iterator() {
        return Collections.singletonList(row).iterator();
    }
    
    /**
     * Get the single EventRow.
     * 
     * @return the single row
     */
    public EventRow getRow() {
        return row;
    }
    
    @Override
    public String toString() {
        return "SingleEventRows{" + row + "}";
    }
}
