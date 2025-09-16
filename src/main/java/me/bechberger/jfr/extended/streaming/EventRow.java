package me.bechberger.jfr.extended.streaming;

import me.bechberger.jfr.extended.streaming.exception.RowCellAccessException;
import me.bechberger.jfr.extended.table.CellValue;
import me.bechberger.jfr.extended.table.JfrTable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Represents a single event row from JFR data.
 * This is the atomic unit of data in the streaming architecture.
 * 
 * EventRow is based on CellValue for type-safe access to field data.
 * All accessor methods provide type checking and appropriate conversions.
 * 
 * @author JFR Query Streaming Architecture
 * @since 2.0
 */
public interface EventRow {
    
    // ===== CORE CELLVALUE-BASED ACCESS =====
    
    /**
     * Get the CellValue at a specific column index.
     * This is the fundamental access method that all others are based on.
     * 
     * @param columnIndex the zero-based column index
     * @return the CellValue at that index
     * @throws IndexOutOfBoundsException if index is invalid
     */
    CellValue getCellValue(int columnIndex);
    
    /**
     * Get the CellValue for a named field.
     * 
     * @param fieldName the name of the field
     * @return the CellValue, or CellValue.NullValue if field doesn't exist
     */
    CellValue getCellValue(String fieldName);
    
    // ===== RAW VALUE ACCESS =====
    
    /**
     * Get the raw value of a field by name.
     * 
     * @param fieldName the name of the field
     * @return the value, or null if field doesn't exist
     */
    default Object getValue(String fieldName) {
        return getCellValue(fieldName).getValue();
    }
    
    /**
     * Get the raw value of a field by column index.
     * 
     * @param columnIndex the zero-based column index
     * @return the value
     * @throws IndexOutOfBoundsException if index is invalid
     */
    default Object getValue(int columnIndex) {
        return getCellValue(columnIndex).getValue();
    }
    
    // ===== TYPE-CHECKED ACCESSORS BY INDEX =====
    
    /**
     * Get a StringValue from a column index with type checking.
     * 
     * @param columnIndex the column index
     * @return the StringValue
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not a string
     */
    default CellValue.StringValue getString(int columnIndex) {
        CellValue cell = getCellValue(columnIndex);
        if (cell instanceof CellValue.StringValue stringValue) {
            return stringValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullColumn(columnIndex, "String");
        } else {
            throw new RowCellAccessException(columnIndex, "String", cell);
        }
    }
    
    /**
     * Get the raw string value from a column index.
     * Convenience method that unwraps the StringValue.
     * 
     * @param columnIndex the column index
     * @return the raw string value
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not a string
     */
    default String getStringValue(int columnIndex) {
        return getString(columnIndex).value();
    }
    
    /**
     * Get a NumberValue from a column index with type checking.
     * 
     * @param columnIndex the column index
     * @return the NumberValue
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not a number
     */
    default CellValue.NumberValue getNumber(int columnIndex) {
        CellValue cell = getCellValue(columnIndex);
        if (cell instanceof CellValue.NumberValue numberValue) {
            return numberValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullColumn(columnIndex, "Number");
        } else {
            throw new RowCellAccessException(columnIndex, "Number", cell);
        }
    }
    
    /**
     * Get the raw numeric value from a column index.
     * Convenience method that unwraps the NumberValue.
     * 
     * @param columnIndex the column index
     * @return the raw numeric value
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not numeric
     */
    default Number getNumberValue(int columnIndex) {
        CellValue cell = getCellValue(columnIndex);
        if (cell instanceof CellValue.NumberValue numberValue) {
            return numberValue.value();
        } else if (cell instanceof CellValue.NumberValue floatValue) {
            return floatValue.value();
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullColumn(columnIndex, "Number");
        } else {
            throw new RowCellAccessException(columnIndex, "Number", cell);
        }
    }
    
    /**
     * Get a BooleanValue from a column index with type checking.
     * 
     * @param columnIndex the column index
     * @return the BooleanValue
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not a boolean
     */
    default CellValue.BooleanValue getBoolean(int columnIndex) {
        CellValue cell = getCellValue(columnIndex);
        if (cell instanceof CellValue.BooleanValue booleanValue) {
            return booleanValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullColumn(columnIndex, "Boolean");
        } else {
            throw new RowCellAccessException(columnIndex, "Boolean", cell);
        }
    }
    
    /**
     * Get the raw boolean value from a column index.
     * Convenience method that unwraps the BooleanValue.
     * 
     * @param columnIndex the column index
     * @return the raw boolean value
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not a boolean
     */
    default Boolean getBooleanValue(int columnIndex) {
        return getBoolean(columnIndex).value();
    }
    
    /**
     * Get a TimestampValue from a column index with type checking.
     * 
     * @param columnIndex the column index
     * @return the TimestampValue
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not a timestamp
     */
    default CellValue.TimestampValue getTimestamp(int columnIndex) {
        CellValue cell = getCellValue(columnIndex);
        if (cell instanceof CellValue.TimestampValue timestampValue) {
            return timestampValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullColumn(columnIndex, "Timestamp");
        } else {
            throw new RowCellAccessException(columnIndex, "Timestamp", cell);
        }
    }
    
    /**
     * Get the raw timestamp value from a column index.
     * Convenience method that unwraps the TimestampValue.
     * 
     * @param columnIndex the column index
     * @return the raw timestamp value
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not a timestamp
     */
    default Instant getTimestampValue(int columnIndex) {
        return getTimestamp(columnIndex).value();
    }
    
    /**
     * Get a DurationValue from a column index with type checking.
     * 
     * @param columnIndex the column index
     * @return the DurationValue
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not a duration
     */
    default CellValue.DurationValue getDuration(int columnIndex) {
        CellValue cell = getCellValue(columnIndex);
        if (cell instanceof CellValue.DurationValue durationValue) {
            return durationValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullColumn(columnIndex, "Duration");
        } else {
            throw new RowCellAccessException(columnIndex, "Duration", cell);
        }
    }
    
    /**
     * Get the raw duration value from a column index.
     * Convenience method that unwraps the DurationValue.
     * 
     * @param columnIndex the column index
     * @return the raw duration value
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not a duration
     */
    default Duration getDurationValue(int columnIndex) {
        return getDuration(columnIndex).value();
    }
    
    /**
     * Get a MemorySizeValue from a column index with type checking.
     * 
     * @param columnIndex the column index
     * @return the MemorySizeValue
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not a memory size
     */
    default CellValue.MemorySizeValue getMemorySize(int columnIndex) {
        CellValue cell = getCellValue(columnIndex);
        if (cell instanceof CellValue.MemorySizeValue memorySizeValue) {
            return memorySizeValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullColumn(columnIndex, "MemorySize");
        } else {
            throw new RowCellAccessException(columnIndex, "MemorySize", cell);
        }
    }
    
    /**
     * Get the raw memory size value from a column index.
     * Convenience method that unwraps the MemorySizeValue.
     * 
     * @param columnIndex the column index
     * @return the raw memory size value in bytes
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not a memory size
     */
    default Long getMemorySizeValue(int columnIndex) {
        return getMemorySize(columnIndex).value();
    }
    
    /**
     * Get an ArrayValue from a column index with type checking.
     * 
     * @param columnIndex the column index
     * @return the ArrayValue
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not an array
     */
    default CellValue.ArrayValue getArray(int columnIndex) {
        CellValue cell = getCellValue(columnIndex);
        if (cell instanceof CellValue.ArrayValue arrayValue) {
            return arrayValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullColumn(columnIndex, "Array");
        } else {
            throw new RowCellAccessException(columnIndex, "Array", cell);
        }
    }
    
    /**
     * Get the raw array value from a column index.
     * Convenience method that unwraps the ArrayValue.
     * 
     * @param columnIndex the column index
     * @return the raw array value
     * @throws IndexOutOfBoundsException if index is invalid
     * @throws RowCellAccessException if the value is not an array
     */
    default List<Object> getArrayValue(int columnIndex) {
        return getArray(columnIndex).elements().stream()
            .map(CellValue::getValue)
            .toList();
    }
    
    // ===== TYPE-CHECKED ACCESSORS BY NAME =====
    
    /**
     * Get a StringValue from a field name with type checking.
     * 
     * @param fieldName the field name
     * @return the StringValue
     * @throws RowCellAccessException if field doesn't exist or isn't a string
     */
    default CellValue.StringValue getString(String fieldName) {
        CellValue cell = getCellValue(fieldName);
        if (cell instanceof CellValue.StringValue stringValue) {
            return stringValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullField(fieldName, "String");
        } else {
            throw new RowCellAccessException(fieldName, "String", cell);
        }
    }
    
    /**
     * Get the raw string value from a field name.
     * Convenience method that unwraps the StringValue.
     * 
     * @param fieldName the field name
     * @return the raw string value
     * @throws RowCellAccessException if field doesn't exist or isn't a string
     */
    default String getStringValue(String fieldName) {
        return getString(fieldName).value();
    }
    
    /**
     * Get a NumberValue from a field name with type checking.
     * 
     * @param fieldName the field name
     * @return the NumberValue
     * @throws RowCellAccessException if field doesn't exist or isn't a number
     */
    default CellValue.NumberValue getNumber(String fieldName) {
        CellValue cell = getCellValue(fieldName);
        if (cell instanceof CellValue.NumberValue numberValue) {
            return numberValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullField(fieldName, "Number");
        } else {
            throw new RowCellAccessException(fieldName, "Number", cell);
        }
    }
    
    /**
     * Get the raw numeric value from a field name.
     * Convenience method that unwraps the NumberValue or NumberValue.
     * 
     * @param fieldName the field name
     * @return the raw numeric value
     * @throws RowCellAccessException if field doesn't exist or isn't numeric
     */
    default Number getNumberValue(String fieldName) {
        CellValue cell = getCellValue(fieldName);
        if (cell instanceof CellValue.NumberValue numberValue) {
            return numberValue.value();
        } else if (cell instanceof CellValue.NumberValue floatValue) {
            return floatValue.value();
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullField(fieldName, "Number");
        } else {
            throw new RowCellAccessException(fieldName, "Number", cell);
        }
    }
    
    /**
     * Get a BooleanValue from a field name with type checking.
     * 
     * @param fieldName the field name
     * @return the BooleanValue
     * @throws RowCellAccessException if field doesn't exist or isn't boolean
     */
    default CellValue.BooleanValue getBoolean(String fieldName) {
        CellValue cell = getCellValue(fieldName);
        if (cell instanceof CellValue.BooleanValue booleanValue) {
            return booleanValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullField(fieldName, "Boolean");
        } else {
            throw new RowCellAccessException(fieldName, "Boolean", cell);
        }
    }
    
    /**
     * Get the raw boolean value from a field name.
     * Convenience method that unwraps the BooleanValue.
     * 
     * @param fieldName the field name
     * @return the raw boolean value
     * @throws RowCellAccessException if field doesn't exist or isn't boolean
     */
    default Boolean getBooleanValue(String fieldName) {
        return getBoolean(fieldName).value();
    }
    
    /**
     * Get a TimestampValue from a field name with type checking.
     * 
     * @param fieldName the field name
     * @return the TimestampValue
     * @throws RowCellAccessException if field doesn't exist or isn't a timestamp
     */
    default CellValue.TimestampValue getTimestamp(String fieldName) {
        CellValue cell = getCellValue(fieldName);
        if (cell instanceof CellValue.TimestampValue timestampValue) {
            return timestampValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullField(fieldName, "Timestamp");
        } else {
            throw new RowCellAccessException(fieldName, "Timestamp", cell);
        }
    }
    
    /**
     * Get the raw timestamp value from a field name.
     * Convenience method that unwraps the TimestampValue.
     * 
     * @param fieldName the field name
     * @return the raw timestamp
     * @throws RowCellAccessException if field doesn't exist or isn't a timestamp
     */
    default Instant getTimestampValue(String fieldName) {
        return getTimestamp(fieldName).value();
    }
    
    /**
     * Get a DurationValue from a field name with type checking.
     * 
     * @param fieldName the field name
     * @return the DurationValue
     * @throws RowCellAccessException if field doesn't exist or isn't a duration
     */
    default CellValue.DurationValue getDuration(String fieldName) {
        CellValue cell = getCellValue(fieldName);
        if (cell instanceof CellValue.DurationValue durationValue) {
            return durationValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullField(fieldName, "Duration");
        } else {
            throw new RowCellAccessException(fieldName, "Duration", cell);
        }
    }
    
    /**
     * Get the raw duration value from a field name.
     * Convenience method that unwraps the DurationValue.
     * 
     * @param fieldName the field name
     * @return the raw duration
     * @throws RowCellAccessException if field doesn't exist or isn't a duration
     */
    default Duration getDurationValue(String fieldName) {
        return getDuration(fieldName).value();
    }
    
    /**
     * Get a MemorySizeValue from a field name with type checking.
     * 
     * @param fieldName the field name
     * @return the MemorySizeValue
     * @throws RowCellAccessException if field doesn't exist or isn't a memory size
     */
    default CellValue.MemorySizeValue getMemorySize(String fieldName) {
        CellValue cell = getCellValue(fieldName);
        if (cell instanceof CellValue.MemorySizeValue memorySizeValue) {
            return memorySizeValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullField(fieldName, "MemorySize");
        } else {
            throw new RowCellAccessException(fieldName, "MemorySize", cell);
        }
    }
    
    /**
     * Get the raw memory size value from a field name.
     * Convenience method that unwraps the MemorySizeValue.
     * 
     * @param fieldName the field name
     * @return the raw memory size in bytes
     * @throws RowCellAccessException if field doesn't exist or isn't a memory size
     */
    default Long getMemorySizeValue(String fieldName) {
        return getMemorySize(fieldName).value();
    }
    
    /**
     * Get an ArrayValue from a field name with type checking.
     * 
     * @param fieldName the field name
     * @return the ArrayValue
     * @throws RowCellAccessException if field doesn't exist or isn't an array
     */
    default CellValue.ArrayValue getArray(String fieldName) {
        CellValue cell = getCellValue(fieldName);
        if (cell instanceof CellValue.ArrayValue arrayValue) {
            return arrayValue;
        } else if (cell instanceof CellValue.NullValue) {
            throw RowCellAccessException.forNullField(fieldName, "Array");
        } else {
            throw new RowCellAccessException(fieldName, "Array", cell);
        }
    }
    
    /**
     * Get the raw array value from a field name.
     * Convenience method that unwraps the ArrayValue.
     * 
     * @param fieldName the field name
     * @return the raw list value
     * @throws RowCellAccessException if field doesn't exist or isn't a list
     */
    default List<Object> getArrayValue(String fieldName) {
        return getArray(fieldName).elements().stream()
            .map(CellValue::getValue)
            .toList();
    }
    
    // ===== UTILITY METHODS =====
    
    /**
     * Check if a field exists in this row.
     * 
     * @param fieldName the field name
     * @return true if the field exists
     */
    boolean hasField(String fieldName);
    
    /**
     * Get all field names in this row.
     * 
     * @return list of field names
     */
    List<String> getFieldNames();
    
    /**
     * Get the schema/columns for this row.
     * 
     * @return list of columns
     */
    List<JfrTable.Column> getColumns();
    
    /**
     * Convert this row to a map of field name -> raw value.
     * 
     * @return map representation
     */
    Map<String, Object> toMap();
    
    /**
     * Convert this row to a map of field name -> CellValue.
     * 
     * @return map of CellValues
     */
    default Map<String, CellValue> toCellValueMap() {
        Map<String, CellValue> map = new java.util.HashMap<>();
        List<String> fieldNames = getFieldNames();
        for (String fieldName : fieldNames) {
            map.put(fieldName, getCellValue(fieldName));
        }
        return map;
    }
    
    /**
     * Get the number of fields/columns in this row.
     * 
     * @return field count
     */
    int getFieldCount();
    
    /**
     * Get all CellValues in this row.
     * 
     * @return list of CellValues in column order
     */
    default List<CellValue> getCellValues() {
        List<CellValue> cells = new java.util.ArrayList<>();
        for (int i = 0; i < getFieldCount(); i++) {
            cells.add(getCellValue(i));
        }
        return cells;
    }
}
