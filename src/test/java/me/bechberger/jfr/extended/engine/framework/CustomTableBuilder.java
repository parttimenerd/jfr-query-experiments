package me.bechberger.jfr.extended.engine.framework;

import me.bechberger.jfr.extended.table.JfrTable;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.CellType;

import java.util.*;

/**
 * Builder for custom JFR event tables with specialized columns
 */
public class CustomTableBuilder {
    private final String tableName;
    private final QueryTestFramework framework;
    private final List<JfrTable.Column> columns = new ArrayList<>();
    private final List<List<Object>> rows = new ArrayList<>();
    
    public CustomTableBuilder(String tableName, QueryTestFramework framework) {
        this.tableName = tableName;
        this.framework = framework;
    }
    
    public CustomTableBuilder withStringColumn(String name) {
        columns.add(new JfrTable.Column(name, CellType.STRING));
        return this;
    }
    
    public CustomTableBuilder withNumberColumn(String name) {
        columns.add(new JfrTable.Column(name, CellType.NUMBER));
        return this;
    }
    
    public CustomTableBuilder withFloatColumn(String name) {
        columns.add(new JfrTable.Column(name, CellType.FLOAT));
        return this;
    }
    
    public CustomTableBuilder withBooleanColumn(String name) {
        columns.add(new JfrTable.Column(name, CellType.BOOLEAN));
        return this;
    }
    
    public CustomTableBuilder withTimestampColumn(String name) {
        columns.add(new JfrTable.Column(name, CellType.TIMESTAMP));
        return this;
    }
    
    public CustomTableBuilder withDurationColumn(String name) {
        columns.add(new JfrTable.Column(name, CellType.DURATION));
        return this;
    }
    
    public CustomTableBuilder withMemorySizeColumn(String name) {
        columns.add(new JfrTable.Column(name, CellType.MEMORY_SIZE));
        return this;
    }
    
    public CustomTableBuilder withRow(Object... values) {
        rows.add(Arrays.asList(values));
        return this;
    }
    
    public QueryTestFramework build() {
        JfrTable table = new JfrTable(columns);
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
            case NUMBER -> new CellValue.NumberValue(value instanceof Number n ? n.longValue() : Long.parseLong(value.toString()));
            case FLOAT -> new CellValue.FloatValue(value instanceof Number n ? n.doubleValue() : Double.parseDouble(value.toString()));
            case BOOLEAN -> new CellValue.BooleanValue(value instanceof Boolean b ? b : Boolean.parseBoolean(value.toString()));
            case TIMESTAMP -> new CellValue.TimestampValue(java.time.Instant.ofEpochMilli(value instanceof Number n ? n.longValue() : Long.parseLong(value.toString())));
            case DURATION -> new CellValue.DurationValue(java.time.Duration.ofMillis(value instanceof Number n ? n.longValue() : Long.parseLong(value.toString())));
            case MEMORY_SIZE -> new CellValue.MemorySizeValue(value instanceof Number n ? n.longValue() : Long.parseLong(value.toString()));
            default -> new CellValue.StringValue(value.toString());
        };
    }
}
