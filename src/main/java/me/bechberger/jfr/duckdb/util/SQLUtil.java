package me.bechberger.jfr.duckdb.util;

import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetTime;
import java.util.*;
import java.util.stream.Collectors;

public class SQLUtil {
    
    public static Set<String> getTableNames(DuckDBConnection connection) throws SQLException {
        ResultSet rs = connection.createStatement().executeQuery("SHOW TABLES");
        Set<String> tableNames = new HashSet<>();
        while (rs.next()) {
            tableNames.add(rs.getString(1));
        }
        return tableNames;
    }

    public static void append(DuckDBAppender appender, Instant value) throws SQLException {
        if (value == null) {
            appender.appendNull();
            return;
        }
        appender.append(value.atOffset(OffsetTime.now().getOffset()).toLocalDateTime());
    }

    public static void append(DuckDBAppender appender, Duration value) throws SQLException {
        appender.append(value.toNanos() + "ns");
    }
}