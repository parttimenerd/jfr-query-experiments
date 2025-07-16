package me.bechberger.jfr.extended.table;

import me.bechberger.jfr.extended.table.exception.CellAccessException;
import me.bechberger.jfr.extended.table.exception.CellIndexOutOfBoundsException;
import me.bechberger.jfr.extended.table.exception.ColumnIndexOutOfBoundsException;
import java.util.List;
import java.util.Optional;

/**
 * Interface for JFR table implementations.
 * 
 * Provides a unified API for both multi-row tables (StandardJfrTable) 
 * and single-cell tables (SingleCellTable) with optimized implementations.
 */
public interface JfrTable {
    
    /**
     * Column definition
     */
    record Column(String name, CellType type, String description) {
        public Column(String name, CellType type) {
            this(name, type, null);
        }
    }
    
    /**
     * Row of data
     */
    class Row {
        private final List<CellValue> cells;
        
        public Row(List<CellValue> cells) {
            this.cells = List.copyOf(cells);
        }
        
        public Row(CellValue... cells) {
            this.cells = List.of(cells);
        }
        
        public List<CellValue> getCells() {
            return cells;
        }
        
        public CellValue getCell(int index) {
            if (index < 0 || index >= cells.size()) {
                return new CellValue.NullValue();
            }
            return cells.get(index);
        }
        
        /**
         * Get cell value at index with strict bounds checking.
         * Throws CellIndexOutOfBoundsException if index is invalid.
         * 
         * @param index The cell index
         * @return The cell value at the specified index
         * @throws CellIndexOutOfBoundsException if index is out of bounds
         */
        public CellValue getCellStrict(int index) {
            if (index < 0 || index >= cells.size()) {
                throw new CellIndexOutOfBoundsException(index, cells.size());
            }
            return cells.get(index);
        }
        
        public int size() {
            return cells.size();
        }
        
        // Convenient accessor methods for typed values
        
        /**
         * Get string value at index
         */
        public String getString(int index) {
            CellValue cell = getCell(index);
            if (cell instanceof CellValue.StringValue stringValue) {
                return stringValue.value();
            }
            return cell.toString();
        }
        
        /**
         * Get number value at index
         */
        public long getNumber(int index) {
            CellValue cell = getCell(index);
            if (cell instanceof CellValue.NumberValue numberValue) {
                return (long) numberValue.value();
            }
            if (cell instanceof CellValue.NullValue) {
                throw CellAccessException.forNullValue("column " + index, "number");
            }
            throw new CellAccessException(index, "number", cell);
        }
        
        /**
         * Get double value at index
         */
        public double getDouble(int index) {
            CellValue cell = getCell(index);
            if (cell instanceof CellValue.NumberValue numberValue) {
                return numberValue.value();
            }
            if (cell instanceof CellValue.FloatValue floatValue) {
                return floatValue.value();
            }
            if (cell instanceof CellValue.NullValue) {
                throw CellAccessException.forNullValue("column " + index, "double");
            }
            throw new CellAccessException(index, "double", cell);
        }
        
        /**
         * Get boolean value at index
         */
        public boolean getBoolean(int index) {
            CellValue cell = getCell(index);
            if (cell instanceof CellValue.BooleanValue booleanValue) {
                return booleanValue.value();
            }
            if (cell instanceof CellValue.NullValue) {
                throw CellAccessException.forNullValue("column " + index, "boolean");
            }
            throw new CellAccessException(index, "boolean", cell);
        }
        
        /**
         * Get array value at index
         */
        @SuppressWarnings("unchecked")
        public <T> List<T> getArray(int index) {
            CellValue cell = getCell(index);
            if (cell instanceof CellValue.ArrayValue arrayValue) {
                return (List<T>) arrayValue.elements();
            }
            if (cell instanceof CellValue.NullValue) {
                throw CellAccessException.forNullValue("column " + index, "array");
            }
            throw new CellAccessException(index, "array", cell);
        }
        
        /**
         * Get duration value at index
         */
        public java.time.Duration getDuration(int index) {
            CellValue cell = getCell(index);
            if (cell instanceof CellValue.DurationValue durationValue) {
                return durationValue.value();
            }
            if (cell instanceof CellValue.NullValue) {
                throw CellAccessException.forNullValue("column " + index, "duration");
            }
            throw new CellAccessException(index, "duration", cell);
        }
        
        /**
         * Get timestamp value at index
         */
        public java.time.Instant getTimestamp(int index) {
            CellValue cell = getCell(index);
            if (cell instanceof CellValue.TimestampValue timestampValue) {
                return timestampValue.value();
            }
            if (cell instanceof CellValue.NullValue) {
                throw CellAccessException.forNullValue("column " + index, "timestamp");
            }
            throw new CellAccessException(index, "timestamp", cell);
        }
        
        /**
         * Get memory size value at index (in bytes)
         */
        public long getMemorySize(int index) {
            CellValue cell = getCell(index);
            if (cell instanceof CellValue.MemorySizeValue memorySizeValue) {
                return memorySizeValue.value();
            }
            if (cell instanceof CellValue.NullValue) {
                throw CellAccessException.forNullValue("column " + index, "memory size");
            }
            throw new CellAccessException(index, "memory size", cell);
        }
        
        /**
         * Get rate value at index
         */
        public double getRate(int index) {
            CellValue cell = getCell(index);
            if (cell instanceof CellValue.RateValue rateValue) {
                return rateValue.count();
            }
            if (cell instanceof CellValue.NullValue) {
                throw CellAccessException.forNullValue("column " + index, "rate");
            }
            throw new CellAccessException(index, "rate", cell);
        }
        
        /**
         * Get float value at index
         */
        public double getFloat(int index) {
            CellValue cell = getCell(index);
            if (cell instanceof CellValue.FloatValue floatValue) {
                return floatValue.value();
            }
            if (cell instanceof CellValue.NullValue) {
                throw CellAccessException.forNullValue("column " + index, "float");
            }
            throw new CellAccessException(index, "float", cell);
        }
        
        /**
         * Check if cell at index is null
         */
        public boolean isNull(int index) {
            return getCell(index) instanceof CellValue.NullValue;
        }
        
        @Override
        public String toString() {
            return cells.toString();
        }
    }
    
    /**
     * Predicate for filtering rows
     */
    @FunctionalInterface
    interface RowPredicate {
        boolean test(Row row, JfrTable table);
    }
    
    // Core table access methods
    
    /**
     * Get all columns in this table
     */
    List<Column> getColumns();
    
    /**
     * Get all rows in this table
     */
    List<Row> getRows();
    
    /**
     * Get column by name
     */
    Optional<Column> getColumn(String name);
    
    /**
     * Get column index by name
     */
    int getColumnIndex(String name);
    
    /**
     * Get cell value by row and column name
     */
    CellValue getCell(int rowIndex, String columnName);
    
    /**
     * Get cell value by row and column index
     */
    CellValue getCell(int rowIndex, int columnIndex);
    
    /**
     * Get all values in a column
     */
    List<CellValue> getColumnValues(String columnName);
    
    /**
     * Get all values in a column by index
     */
    default List<CellValue> getColumnValues(int columnIndex) {
        String columnName = getColumnName(columnIndex);
        return getColumnValues(columnName);
    }
    
    /**
     * Get number of rows
     */
    int getRowCount();
    
    /**
     * Get number of columns
     */
    int getColumnCount();
    
    /**
     * Get column name by index
     */
    default String getColumnName(int columnIndex) {
        List<Column> columns = getColumns();
        if (columnIndex < 0 || columnIndex >= columns.size()) {
            throw new ColumnIndexOutOfBoundsException(columnIndex, columns.size());
        }
        return columns.get(columnIndex).name();
    }
    
    /**
     * Get column type by index
     */
    default CellType getColumnType(int columnIndex) {
        List<Column> columns = getColumns();
        if (columnIndex < 0 || columnIndex >= columns.size()) {
            throw new ColumnIndexOutOfBoundsException(columnIndex, columns.size());
        }
        return columns.get(columnIndex).type();
    }
    
    // Convenient accessor methods for typed values by row and column
    
    /**
     * Get string value by row and column name
     */
    default String getString(int rowIndex, String columnName) {
        CellValue cell = getCell(rowIndex, columnName);
        if (cell instanceof CellValue.StringValue stringValue) {
            return stringValue.value();
        }
        return cell.toString();
    }
    
    /**
     * Get string value by row and column index
     */
    default String getString(int rowIndex, int columnIndex) {
        CellValue cell = getCell(rowIndex, columnIndex);
        if (cell instanceof CellValue.StringValue stringValue) {
            return stringValue.value();
        }
        return cell.toString();
    }
    
    /**
     * Get number value by row and column name
     */
    default long getNumber(int rowIndex, String columnName) {
        CellValue cell = getCell(rowIndex, columnName);
        if (cell instanceof CellValue.NumberValue numberValue) {
            return (long) numberValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column '" + columnName + "'", "number");
        }
        throw new CellAccessException(rowIndex, columnName, "number", cell);
    }
    
    /**
     * Get number value by row and column index
     */
    default long getNumber(int rowIndex, int columnIndex) {
        CellValue cell = getCell(rowIndex, columnIndex);
        if (cell instanceof CellValue.NumberValue numberValue) {
            return (long) numberValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column " + columnIndex, "number");
        }
        throw new CellAccessException(rowIndex, columnIndex, "number", cell);
    }
    
    /**
     * Get double value by row and column name
     */
    default double getDouble(int rowIndex, String columnName) {
        CellValue cell = getCell(rowIndex, columnName);
        if (cell instanceof CellValue.NumberValue numberValue) {
            return numberValue.value();
        }
        if (cell instanceof CellValue.FloatValue floatValue) {
            return floatValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column '" + columnName + "'", "double");
        }
        throw new CellAccessException(rowIndex, columnName, "double", cell);
    }
    
    /**
     * Get double value by row and column index
     */
    default double getDouble(int rowIndex, int columnIndex) {
        CellValue cell = getCell(rowIndex, columnIndex);
        if (cell instanceof CellValue.NumberValue numberValue) {
            return numberValue.value();
        }
        if (cell instanceof CellValue.FloatValue floatValue) {
            return floatValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column " + columnIndex, "double");
        }
        throw new CellAccessException(rowIndex, columnIndex, "double", cell);
    }
    
    /**
     * Get boolean value by row and column name
     */
    default boolean getBoolean(int rowIndex, String columnName) {
        CellValue cell = getCell(rowIndex, columnName);
        if (cell instanceof CellValue.BooleanValue booleanValue) {
            return booleanValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column '" + columnName + "'", "boolean");
        }
        throw new CellAccessException(rowIndex, columnName, "boolean", cell);
    }
    
    /**
     * Get boolean value by row and column index
     */
    default boolean getBoolean(int rowIndex, int columnIndex) {
        CellValue cell = getCell(rowIndex, columnIndex);
        if (cell instanceof CellValue.BooleanValue booleanValue) {
            return booleanValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column " + columnIndex, "boolean");
        }
        throw new CellAccessException(rowIndex, columnIndex, "boolean", cell);
    }
    
    /**
     * Get array value by row and column name
     */
    @SuppressWarnings("unchecked")
    default <T> List<T> getArray(int rowIndex, String columnName) {
        CellValue cell = getCell(rowIndex, columnName);
        if (cell instanceof CellValue.ArrayValue arrayValue) {
            return (List<T>) arrayValue.elements();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column '" + columnName + "'", "array");
        }
        throw new CellAccessException(rowIndex, columnName, "array", cell);
    }
    
    /**
     * Get array value by row and column index
     */
    @SuppressWarnings("unchecked")
    default <T> List<T> getArray(int rowIndex, int columnIndex) {
        CellValue cell = getCell(rowIndex, columnIndex);
        if (cell instanceof CellValue.ArrayValue arrayValue) {
            return (List<T>) arrayValue.elements();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column " + columnIndex, "array");
        }
        throw new CellAccessException(rowIndex, columnIndex, "array", cell);
    }
    
    /**
     * Get duration value by row and column name
     */
    default java.time.Duration getDuration(int rowIndex, String columnName) {
        CellValue cell = getCell(rowIndex, columnName);
        if (cell instanceof CellValue.DurationValue durationValue) {
            return durationValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column '" + columnName + "'", "duration");
        }
        throw new CellAccessException(rowIndex, columnName, "duration", cell);
    }
    
    /**
     * Get duration value by row and column index
     */
    default java.time.Duration getDuration(int rowIndex, int columnIndex) {
        CellValue cell = getCell(rowIndex, columnIndex);
        if (cell instanceof CellValue.DurationValue durationValue) {
            return durationValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column " + columnIndex, "duration");
        }
        throw new CellAccessException(rowIndex, columnIndex, "duration", cell);
    }
    
    /**
     * Get timestamp value by row and column name
     */
    default java.time.Instant getTimestamp(int rowIndex, String columnName) {
        CellValue cell = getCell(rowIndex, columnName);
        if (cell instanceof CellValue.TimestampValue timestampValue) {
            return timestampValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column '" + columnName + "'", "timestamp");
        }
        throw new CellAccessException(rowIndex, columnName, "timestamp", cell);
    }
    
    /**
     * Get timestamp value by row and column index
     */
    default java.time.Instant getTimestamp(int rowIndex, int columnIndex) {
        CellValue cell = getCell(rowIndex, columnIndex);
        if (cell instanceof CellValue.TimestampValue timestampValue) {
            return timestampValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column " + columnIndex, "timestamp");
        }
        throw new CellAccessException(rowIndex, columnIndex, "timestamp", cell);
    }
    
    /**
     * Get memory size value by row and column name (in bytes)
     */
    default long getMemorySize(int rowIndex, String columnName) {
        CellValue cell = getCell(rowIndex, columnName);
        if (cell instanceof CellValue.MemorySizeValue memorySizeValue) {
            return memorySizeValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column '" + columnName + "'", "memory size");
        }
        throw new CellAccessException(rowIndex, columnName, "memory size", cell);
    }
    
    /**
     * Get memory size value by row and column index (in bytes)
     */
    default long getMemorySize(int rowIndex, int columnIndex) {
        CellValue cell = getCell(rowIndex, columnIndex);
        if (cell instanceof CellValue.MemorySizeValue memorySizeValue) {
            return memorySizeValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column " + columnIndex, "memory size");
        }
        throw new CellAccessException(rowIndex, columnIndex, "memory size", cell);
    }
    
    /**
     * Get rate value by row and column name
     */
    default double getRate(int rowIndex, String columnName) {
        CellValue cell = getCell(rowIndex, columnName);
        if (cell instanceof CellValue.RateValue rateValue) {
            return rateValue.count();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column '" + columnName + "'", "rate");
        }
        throw new CellAccessException(rowIndex, columnName, "rate", cell);
    }
    
    /**
     * Get rate value by row and column index
     */
    default double getRate(int rowIndex, int columnIndex) {
        CellValue cell = getCell(rowIndex, columnIndex);
        if (cell instanceof CellValue.RateValue rateValue) {
            return rateValue.count();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column " + columnIndex, "rate");
        }
        throw new CellAccessException(rowIndex, columnIndex, "rate", cell);
    }
    
    /**
     * Get float value by row and column name
     */
    default double getFloat(int rowIndex, String columnName) {
        CellValue cell = getCell(rowIndex, columnName);
        if (cell instanceof CellValue.FloatValue floatValue) {
            return floatValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column '" + columnName + "'", "float");
        }
        throw new CellAccessException(rowIndex, columnName, "float", cell);
    }
    
    /**
     * Get float value by row and column index
     */
    default double getFloat(int rowIndex, int columnIndex) {
        CellValue cell = getCell(rowIndex, columnIndex);
        if (cell instanceof CellValue.FloatValue floatValue) {
            return floatValue.value();
        }
        if (cell instanceof CellValue.NullValue) {
            throw CellAccessException.forNullValue("row " + rowIndex + ", column " + columnIndex, "float");
        }
        throw new CellAccessException(rowIndex, columnIndex, "float", cell);
    }
    
    // Table manipulation methods
    
    /**
     * Add a row to this table
     */
    void addRow(Row row);
    
    /**
     * Add a row to this table
     */
    void addRow(CellValue... cells);
    
    /**
     * Filter rows based on a predicate
     */
    JfrTable filter(RowPredicate predicate);
    
    /**
     * Select specific columns
     */
    JfrTable select(String... columnNames);
    
    /**
     * Sort table by column
     */
    JfrTable sort(String columnName, boolean ascending);
    
    /**
     * Create a copy of this table
     */
    JfrTable copy();
    
    /**
     * String representation of the table
     */
    String toString();
}
