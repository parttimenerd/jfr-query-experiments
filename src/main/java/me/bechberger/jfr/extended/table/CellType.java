package me.bechberger.jfr.extended.table;

/**
 * Types of cell values in a JFR table
 */
public enum CellType {
    STRING,
    NUMBER,     // Unified numeric type (integers and floats)
    DURATION,
    TIMESTAMP,
    MEMORY_SIZE,
    RATE,
    BOOLEAN,
    ARRAY,
    NULL,
    STAR        // Represents the * symbol in SQL contexts
}
