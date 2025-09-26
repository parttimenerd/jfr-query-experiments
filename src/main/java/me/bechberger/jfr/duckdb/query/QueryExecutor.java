package me.bechberger.jfr.duckdb.query;

import com.jakewharton.fliptables.FlipTableConverters;
import me.bechberger.jfr.duckdb.RuntimeSQLException;
import org.duckdb.DuckDBConnection;

import java.io.BufferedOutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.IntStream;

/**
 * Executes queries on a DuckDB database connection and prints the results.
 */
public class QueryExecutor {

    public enum OutputFormat {
        TABLE,
        CSV;
    }

    private final DuckDBConnection conn;

    public QueryExecutor(DuckDBConnection connection) {
        conn = connection;
    }

    public void executeQuery(String query, OutputFormat format, PrintStream out) {
        try (var stmt = conn.createStatement()) {
            var rs = stmt.executeQuery(query);
            switch (format) {
                case TABLE -> printTable(rs, out);
                case CSV -> printCSV(rs, out);
            }
        } catch (SQLException e) {
            throw new RuntimeSQLException("Error executing query: " + query, e);
        }
    }

    private void printCSV(ResultSet rs, PrintStream out) throws SQLException {
        var meta = rs.getMetaData();
        String header = IntStream.range(1, meta.getColumnCount() + 1)
                .mapToObj(i -> {
                    try {
                        return meta.getColumnName(i);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .reduce((a, b) -> a + "," + b)
                .orElse("");
        out.println(header);
        while (rs.next()) {
            String row = IntStream.range(1, meta.getColumnCount() + 1)
                    .mapToObj(i -> getStringValue(rs, i))
                    .reduce((a, b) -> a + "," + b)
                    .orElse("");
            out.println(row);
        }
        out.flush();
    }

    private String getStringValue(ResultSet rs, int columnIndex) {
        try {
            String value = rs.getString(columnIndex);
            if (value == null) {
                return "";
            }
            return value.replace("\"", "\"\"")
                    .replace(",", "\\,")
                    .replace("\n", "\\n")
                    .replace("\r", "\\r");
        } catch (SQLException e) {
            throw new RuntimeSQLException("Cannot get value at column " + columnIndex, e);
        }
    }

    private void printTable(ResultSet rs, PrintStream out) throws SQLException {
        out.println(FlipTableConverters.fromResultSet(rs));
    }
}