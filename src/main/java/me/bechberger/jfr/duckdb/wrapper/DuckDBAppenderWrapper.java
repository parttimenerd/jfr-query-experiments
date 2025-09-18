package me.bechberger.jfr.duckdb.wrapper;

import org.duckdb.DuckDBAppender;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;

public class DuckDBAppenderWrapper implements AppenderWrapper {
    private final DuckDBAppender appender;
    private final String tableName;

    public DuckDBAppenderWrapper(DuckDBAppender appender, String tableName) {
        this.appender = appender;
        this.tableName = tableName;
    }

    @Override
    public void begin() throws SQLException {
        appender.beginRow();
    }

    @Override
    public void end() throws SQLException {
        appender.endRow();
    }

    @Override
    public void close() throws SQLException {
        appender.close();
    }

    public void append(String value) throws SQLException {
        appender.append(value);
    }

    public void append(long value) throws SQLException {
        appender.append(value);
    }

    public void append(int value) throws SQLException {
        appender.append(value);
    }

    public void append(short value) throws SQLException {
        appender.append(value);
    }

    public void append(byte value) throws SQLException {
        appender.append(value);
    }

    public void append(boolean value) throws SQLException {
        appender.append(value);
    }

    public void append(double value) throws SQLException {
        appender.append(value);
    }

    public void append(float value) throws SQLException {
        appender.append(value);
    }

    @Override
    public void appendNull() throws SQLException {
        appender.appendNull();
    }

    /**
     * Might fail if the array has not the expected size
     */
    @Override
    public void append(int[] value) throws SQLException {
        appender.append(value);
    }

    @Override
    public void append(Instant value) throws SQLException {
        appender.append(value.atOffset(java.time.ZoneOffset.UTC).toLocalDateTime());
    }

    @Override
    public void append(Duration value) throws SQLException {
        appender.append(value.toNanos() + "ns");
    }
}