package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.StandardJfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;

import java.util.*;

/**
 * Builder for mock test tables
 */
public class TableBuilder {
    private final String tableName;
    private final QueryTestFramework framework;
    private final List<JfrTable.Column> columns = new ArrayList<>();
    private final List<List<Object>> rows = new ArrayList<>();
    
    public TableBuilder(String tableName, QueryTestFramework framework) {
        this.tableName = tableName;
        this.framework = framework;
    }
    
    public TableBuilder withColumn(String name, CellType type) {
        columns.add(new JfrTable.Column(name, type));
        return this;
    }
    
    // Convenience methods for common column types
    public TableBuilder withStringColumn(String name) {
        return withColumn(name, CellType.STRING);
    }
    
    public TableBuilder withNumberColumn(String name) {
        return withColumn(name, CellType.NUMBER);
    }
    
    public TableBuilder withFloatColumn(String name) {
        return withColumn(name, CellType.NUMBER);
    }
    
    public TableBuilder withBooleanColumn(String name) {
        return withColumn(name, CellType.BOOLEAN);
    }
    
    public TableBuilder withTimestampColumn(String name) {
        return withColumn(name, CellType.TIMESTAMP);
    }
    
    public TableBuilder withDurationColumn(String name) {
        return withColumn(name, CellType.DURATION);
    }
    
    public TableBuilder withMemorySizeColumn(String name) {
        return withColumn(name, CellType.MEMORY_SIZE);
    }
    
    public TableBuilder withArrayColumn(String name) {
        return withColumn(name, CellType.ARRAY);
    }
    
    public TableBuilder withRow(Object... values) {
        rows.add(Arrays.asList(values));
        return this;
    }
    
    public QueryTestFramework build() {
        JfrTable table = new StandardJfrTable(columns);
        for (List<Object> row : rows) {
            List<CellValue> cellValues = new ArrayList<>();
            for (int i = 0; i < row.size() && i < columns.size(); i++) {
                Object value = row.get(i);
                CellType type = columns.get(i).type();
                cellValues.add(convertToCellValue(value, type));
            }
            table.addRow(new JfrTable.Row(cellValues));
        }
        framework.registerTable(tableName, table);
        return framework;
    }
    
    private CellValue convertToCellValue(Object value, CellType type) {
        if (value == null) {
            return new CellValue.NullValue();
        }
        
        return switch (type) {
            case STRING -> new CellValue.StringValue(value.toString());
            case NUMBER -> new CellValue.NumberValue(value instanceof Number n ? n.doubleValue() : Double.parseDouble(value.toString()));
            case BOOLEAN -> new CellValue.BooleanValue(value instanceof Boolean b ? b : Boolean.parseBoolean(value.toString()));
            case TIMESTAMP -> {
                if (value instanceof java.time.Instant instant) {
                    yield new CellValue.TimestampValue(instant);
                } else {
                    String timestampStr = value.toString();
                    try {
                        // Try to parse as ISO date string first
                        if (timestampStr.contains("T") || timestampStr.contains("Z")) {
                            yield new CellValue.TimestampValue(java.time.Instant.parse(timestampStr));
                        } else {
                            // Try to parse as epoch milliseconds
                            yield new CellValue.TimestampValue(java.time.Instant.ofEpochMilli(Long.parseLong(timestampStr)));
                        }
                    } catch (Exception e) {
                        // If both fail, try to parse as number
                        yield new CellValue.TimestampValue(java.time.Instant.ofEpochMilli(value instanceof Number n ? n.longValue() : Long.parseLong(timestampStr)));
                    }
                }
            }
            case DURATION -> {
                if (value instanceof java.time.Duration duration) {
                    yield new CellValue.DurationValue(duration);
                } else {
                    yield new CellValue.DurationValue(java.time.Duration.ofMillis(value instanceof Number n ? n.longValue() : Long.parseLong(value.toString())));
                }
            }
            case MEMORY_SIZE -> new CellValue.MemorySizeValue(value instanceof Number n ? n.longValue() : Long.parseLong(value.toString()));
            case ARRAY -> {
                if (value instanceof List<?> list) {
                    List<CellValue> cellValues = new ArrayList<>();
                    for (Object element : list) {
                        cellValues.add(CellValue.of(element));
                    }
                    yield new CellValue.ArrayValue(cellValues);
                } else if (value instanceof Object[] array) {
                    List<CellValue> cellValues = new ArrayList<>();
                    for (Object element : array) {
                        cellValues.add(CellValue.of(element));
                    }
                    yield new CellValue.ArrayValue(cellValues);
                } else {
                    yield new CellValue.ArrayValue(Collections.singletonList(CellValue.of(value)));
                }
            }
            default -> new CellValue.StringValue(value.toString());
        };
    }
}
