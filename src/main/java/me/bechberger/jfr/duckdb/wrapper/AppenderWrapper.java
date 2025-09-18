package me.bechberger.jfr.duckdb.wrapper;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

/**
 * This is required because the {@code DuckDBAppender} class is faster, so we want to use it,
 * but it doesn't support lists, so we need to have a wrapper that uses prepared statements for those cases.
 */
public interface AppenderWrapper {
    void begin() throws SQLException;

    void end() throws SQLException;

    void close() throws SQLException;

    void appendNull() throws SQLException;

    void append(String value) throws SQLException;

    void append(long value) throws SQLException;

    void append(int value) throws SQLException;

    void append(short value) throws SQLException;

    void append(byte value) throws SQLException;

    void append(boolean value) throws SQLException;

    void append(double value) throws SQLException;

    void append(float value) throws SQLException;

    void append(int[] value) throws SQLException;

    void append(Instant value) throws SQLException;

    default void append(Duration value) throws SQLException {
        throw new UnsupportedOperationException("Duration is not supported");
    }
}