package me.bechberger.jfr.extended.streaming;

import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Non-streaming EventRows implementation that keeps all data in memory.
 * This is used when data has already been materialized or when streaming
 * operations require full materialization (like sorting).
 * 
 * @author JFR Query Streaming Architecture
 * @since 2.0
 */
public class MaterializedEventRows implements EventRows {
    
    private final List<EventRow> rows;
    private final List<JfrTable.Column> schema;
    
    public MaterializedEventRows(List<EventRow> rows, List<JfrTable.Column> schema) {
        this.rows = new ArrayList<>(rows);
        this.schema = new ArrayList<>(schema);
    }
    
    @Override
    public EventRows filter(Predicate<EventRow> predicate) {
        List<EventRow> filtered = rows.stream()
            .filter(predicate)
            .toList();
        return new MaterializedEventRows(filtered, schema);
    }
    
    @Override
    public <R> EventRows map(Function<EventRow, R> mapper) {
        List<EventRow> mapped = rows.stream()
            .map(mapper)
            .map(r -> (EventRow) r)
            .toList();
        return new MaterializedEventRows(mapped, schema);
    }
    
    @Override
    public EventRows flatMap(Function<EventRow, EventRows> mapper) {
        List<EventRow> result = new ArrayList<>();
        for (EventRow row : rows) {
            EventRows mapped = mapper.apply(row);
            mapped.forEach(result::add);
        }
        return new MaterializedEventRows(result, schema);
    }
    
    @Override
    public EventRows limit(long maxSize) {
        if (maxSize >= rows.size()) {
            return this;
        }
        List<EventRow> limited = rows.subList(0, (int) maxSize);
        return new MaterializedEventRows(limited, schema);
    }
    
    @Override
    public EventRows skip(long n) {
        if (n >= rows.size()) {
            return new MaterializedEventRows(List.of(), schema);
        }
        List<EventRow> skipped = rows.subList((int) n, rows.size());
        return new MaterializedEventRows(skipped, schema);
    }
    
    @Override
    public <T extends Comparable<T>> EventRows sorted(Function<EventRow, T> keyExtractor) {
        List<EventRow> sorted = rows.stream()
            .sorted(Comparator.comparing(keyExtractor))
            .toList();
        return new MaterializedEventRows(sorted, schema);
    }
    
    @Override
    public EventRows distinct() {
        Set<EventRow> uniqueRows = new LinkedHashSet<>(rows);
        return new MaterializedEventRows(new ArrayList<>(uniqueRows), schema);
    }
    
    @Override
    public EventRows peek(Consumer<EventRow> action) {
        rows.forEach(action);
        return this;
    }
    
    @Override
    public long count() {
        return rows.size();
    }
    
    @Override
    public boolean anyMatch(Predicate<EventRow> predicate) {
        return rows.stream().anyMatch(predicate);
    }
    
    @Override
    public boolean allMatch(Predicate<EventRow> predicate) {
        return rows.stream().allMatch(predicate);
    }
    
    @Override
    public boolean noneMatch(Predicate<EventRow> predicate) {
        return rows.stream().noneMatch(predicate);
    }
    
    @Override
    public Optional<EventRow> findFirst() {
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.get(0));
    }
    
    @Override
    public Optional<EventRow> findAny() {
        return findFirst();
    }
    
    @Override
    public <T> T reduce(T identity, Function<T, Function<EventRow, T>> accumulator) {
        T result = identity;
        for (EventRow row : rows) {
            result = accumulator.apply(result).apply(row);
        }
        return result;
    }
    
    @Override
    public <R, A> R collect(Collector<EventRow, A, R> collector) {
        return rows.stream().collect(collector);
    }
    
    @Override
    public List<EventRow> toList() {
        return new ArrayList<>(rows);
    }
    
    @Override
    public Stream<EventRow> stream() {
        return rows.stream();
    }
    
    @Override
    public boolean isEmpty() {
        return rows.isEmpty();
    }
    
    @Override
    public List<JfrTable.Column> getSchema() {
        return new ArrayList<>(schema);
    }
    
    @Override
    public JfrTable toTable() {
        StandardJfrTable table = new StandardJfrTable(schema);
        for (EventRow row : rows) {
            // Convert EventRow back to JfrTable.Row
            List<CellValue> cells = new ArrayList<>();
            for (int i = 0; i < schema.size(); i++) {
                Object value = row.getValue(i);
                cells.add(CellValue.of(value));
            }
            table.addRow(new JfrTable.Row(cells));
        }
        return table;
    }
    
    @Override
    public boolean isMaterialized() {
        return true;
    }
    
    @Override
    public EventRows materialize() {
        return this; // Already materialized
    }
    
    @Override
    public Iterator<EventRow> iterator() {
        return rows.iterator();
    }
    
    @Override
    public String toString() {
        return "MaterializedEventRows{" + rows.size() + " rows}";
    }

    StreamingEventRows toStreaming() {
        return new StreamingEventRows(rows.stream(), schema);
    }
}
