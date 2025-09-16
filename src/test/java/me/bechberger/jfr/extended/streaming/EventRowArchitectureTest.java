package me.bechberger.jfr.extended.streaming;

import me.bechberger.jfr.extended.streaming.exception.RowCellAccessException;
import me.bechberger.jfr.extended.table.CellType;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.JfrTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the new CellValue-based EventRow architecture.
 * 
 * @author JFR Query Streaming Architecture
 * @since 2.0
 */
@DisplayName("EventRow CellValue-based Architecture Tests")
public class EventRowArchitectureTest {
    
    @Test
    @DisplayName("TableEventRow should provide type-checked access to cell values")
    void testTableEventRowTypedAccess() {
        // Create test schema
        List<JfrTable.Column> columns = List.of(
            new JfrTable.Column("name", CellType.STRING, "Event name"),
            new JfrTable.Column("duration", CellType.DURATION, "Event duration"),
            new JfrTable.Column("timestamp", CellType.TIMESTAMP, "Event timestamp"),
            new JfrTable.Column("size", CellType.MEMORY_SIZE, "Memory size"),
            new JfrTable.Column("count", CellType.NUMBER, "Event count"),
            new JfrTable.Column("active", CellType.BOOLEAN, "Is active")
        );
        
        // Create test data
        Instant testTime = Instant.now();
        Duration testDuration = Duration.ofMillis(500);
        List<CellValue> cells = List.of(
            new CellValue.StringValue("TestEvent"),
            new CellValue.DurationValue(testDuration),
            new CellValue.TimestampValue(testTime, null),
            new CellValue.MemorySizeValue(1024L),
            new CellValue.NumberValue(42.0),
            new CellValue.BooleanValue(true)
        );
        
        JfrTable.Row row = new JfrTable.Row(cells);
        EventRow eventRow = new TableEventRow(row, columns);
        
        // Test typed access by field name - using CellValue types (primary interface)
        assertEquals("TestEvent", eventRow.getString("name").value());
        assertEquals(testDuration, eventRow.getDuration("duration").value());
        assertEquals(testTime, eventRow.getTimestamp("timestamp").value());
        assertEquals(1024L, eventRow.getMemorySize("size").value());
        assertEquals(42.0, eventRow.getNumber("count").value());
        assertTrue(eventRow.getBoolean("active").value());
        
        // Test typed access by index - using CellValue types (primary interface)
        assertEquals("TestEvent", eventRow.getString(0).value());
        assertEquals(testDuration, eventRow.getDuration(1).value());
        assertEquals(testTime, eventRow.getTimestamp(2).value());
        assertEquals(1024L, eventRow.getMemorySize(3).value());
        assertEquals(42.0, eventRow.getNumber(4).value());
        assertTrue(eventRow.getBoolean(5).value());
        
        // Test CellValue access - primary interface returns CellValue types
        CellValue.StringValue stringValue = eventRow.getString("name");
        assertEquals("TestEvent", stringValue.value());
        assertEquals(CellType.STRING, stringValue.getType());
        
        CellValue.DurationValue durationValue = eventRow.getDuration("duration");
        assertEquals(testDuration, durationValue.value());
        assertEquals(CellType.DURATION, durationValue.getType());
        
        // Test convenience methods (unwrapped values)
        assertEquals("TestEvent", eventRow.getStringValue("name"));
        assertEquals(testDuration, eventRow.getDurationValue("duration"));
        assertEquals(testTime, eventRow.getTimestampValue("timestamp"));
        assertEquals(1024L, eventRow.getMemorySizeValue("size"));
        assertEquals(42.0, eventRow.getNumberValue("count"));
        assertTrue(eventRow.getBooleanValue("active"));
    }
    
    @Test
    @DisplayName("EventRow should throw RowCellAccessException for type mismatches")
    void testEventRowTypeExceptions() {
        List<JfrTable.Column> columns = List.of(
            new JfrTable.Column("name", CellType.STRING, "Event name"),
            new JfrTable.Column("count", CellType.NUMBER, "Event count")
        );
        
        List<CellValue> cells = List.of(
            new CellValue.StringValue("TestEvent"),
            new CellValue.NumberValue(42.0)
        );
        
        JfrTable.Row row = new JfrTable.Row(cells);
        EventRow eventRow = new TableEventRow(row, columns);
        
        // Test type mismatch exceptions by field name
        RowCellAccessException exception1 = assertThrows(RowCellAccessException.class, 
            () -> eventRow.getNumber("name"));
        assertTrue(exception1.getMessage().contains("Cannot access"));
        
        RowCellAccessException exception2 = assertThrows(RowCellAccessException.class, 
            () -> eventRow.getString("count"));
        assertTrue(exception2.getMessage().contains("Cannot access"));
        
        // Test type mismatch exceptions by index
        RowCellAccessException exception3 = assertThrows(RowCellAccessException.class, 
            () -> eventRow.getDuration(0));
        assertTrue(exception3.getMessage().contains("Cannot access"));
        
        RowCellAccessException exception4 = assertThrows(RowCellAccessException.class, 
            () -> eventRow.getBoolean(1));
        assertTrue(exception4.getMessage().contains("Cannot access"));
    }
    
    @Test
    @DisplayName("EventRow should handle null fields gracefully")
    void testEventRowNullHandling() {
        List<JfrTable.Column> columns = List.of(
            new JfrTable.Column("name", CellType.STRING, "Event name")
        );
        
        List<CellValue> cells = List.of(
            new CellValue.NullValue()
        );
        
        JfrTable.Row row = new JfrTable.Row(cells);
        EventRow eventRow = new TableEventRow(row, columns);
        
        // Test null field access throws appropriate exceptions
        RowCellAccessException exception1 = assertThrows(RowCellAccessException.class, 
            () -> eventRow.getString("name"));
        assertTrue(exception1.getMessage().contains("null"));
        
        RowCellAccessException exception2 = assertThrows(RowCellAccessException.class, 
            () -> eventRow.getStringValue("name"));
        assertTrue(exception2.getMessage().contains("null"));
        
        // Test non-existent field
        RowCellAccessException exception3 = assertThrows(RowCellAccessException.class, 
            () -> eventRow.getString("nonexistent"));
        assertTrue(exception3.getMessage().contains("null"));
    }
    
    @Test
    @DisplayName("EventRow should provide correct field metadata")
    void testEventRowMetadata() {
        List<JfrTable.Column> columns = List.of(
            new JfrTable.Column("name", CellType.STRING, "Event name"),
            new JfrTable.Column("count", CellType.NUMBER, "Event count"),
            new JfrTable.Column("active", CellType.BOOLEAN, "Is active")
        );
        
        List<CellValue> cells = List.of(
            new CellValue.StringValue("TestEvent"),
            new CellValue.NumberValue(42.0),
            new CellValue.BooleanValue(true)
        );
        
        JfrTable.Row row = new JfrTable.Row(cells);
        EventRow eventRow = new TableEventRow(row, columns);
        
        // Test metadata methods
        assertEquals(3, eventRow.getFieldCount());
        assertEquals(List.of("name", "count", "active"), eventRow.getFieldNames());
        assertEquals(columns, eventRow.getColumns());
        
        assertTrue(eventRow.hasField("name"));
        assertTrue(eventRow.hasField("count"));
        assertTrue(eventRow.hasField("active"));
        assertFalse(eventRow.hasField("nonexistent"));
        
        // Test map conversion
        var map = eventRow.toMap();
        assertEquals("TestEvent", map.get("name"));
        assertEquals(42.0, map.get("count"));
        assertEquals(true, map.get("active"));
        
        // Test CellValue map conversion
        var cellValueMap = eventRow.toCellValueMap();
        assertTrue(cellValueMap.get("name") instanceof CellValue.StringValue);
        assertTrue(cellValueMap.get("count") instanceof CellValue.NumberValue);
        assertTrue(cellValueMap.get("active") instanceof CellValue.BooleanValue);
    }
    
    public static void main(String[] args) {
        System.out.println("=== EVENTROW CELLVALUE ARCHITECTURE DEMO ===");
        
        EventRowArchitectureTest test = new EventRowArchitectureTest();
        
        try {
            test.testTableEventRowTypedAccess();
            System.out.println("✓ Type-checked access works correctly");
            
            test.testEventRowTypeExceptions();
            System.out.println("✓ Type mismatch exceptions work correctly");
            
            test.testEventRowNullHandling();
            System.out.println("✓ Null handling works correctly");
            
            test.testEventRowMetadata();
            System.out.println("✓ Metadata access works correctly");
            
            System.out.println("\n=== All EventRow architecture tests passed! ===");
            
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
