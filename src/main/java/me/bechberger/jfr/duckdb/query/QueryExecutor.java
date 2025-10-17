package me.bechberger.jfr.duckdb.query;

import java.io.*;
import java.sql.*;
import java.util.stream.IntStream;
import me.bechberger.jfr.duckdb.RuntimeSQLException;
import org.duckdb.DuckDBConnection;

/** Executes queries on a DuckDB database connection and prints the results. */
public class QueryExecutor {

    public enum OutputFormat {
        TABLE,
        CSV
    }

    private final DuckDBConnection conn;

    public QueryExecutor(DuckDBConnection connection) {
        conn = connection;
    }

    public void executeQuery(String query, OutputFormat format, PrintStream out) {
        executeQuery(query, format, out, -1);
    }

    public void executeQuery(String query, OutputFormat format, PrintStream out, int maxCellWidth) {
        try (var stmt = conn.createStatement()) {
            var rs = stmt.executeQuery(query);
            switch (format) {
                case TABLE -> printTable(rs, out);
                case CSV -> printCSV(rs, out, maxCellWidth);
            }
        } catch (SQLException e) {
            throw new RuntimeSQLException("Error executing query: " + query, e);
        }
    }

    /** Executes a query and returns the result as a string in the specified format. */
    public String executeQuery(String query, OutputFormat format, int maxCellWidth) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(new BufferedOutputStream(baos))) {
            executeQuery(query, format, ps, maxCellWidth);
            ps.flush();
            return baos.toString();
        }
    }

    private void printCSV(ResultSet rs, PrintStream out, int maxCellWidth) throws SQLException {
        var meta = rs.getMetaData();
        String header =
                IntStream.range(1, meta.getColumnCount() + 1)
                        .mapToObj(
                                i -> {
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
            String row =
                    IntStream.range(1, meta.getColumnCount() + 1)
                            .mapToObj(i -> getStringValue(rs, i, maxCellWidth))
                            .reduce((a, b) -> a + "," + b)
                            .orElse("");
            out.println(row);
        }
        out.flush();
    }

    private String getStringValue(ResultSet rs, int columnIndex, int maxWidth) {
        try {
            String value = rs.getString(columnIndex);
            if (value == null) {
                return "";
            }
            var escaped =
                    value.replace("\"", "\"\"")
                            .replace(",", "\\,")
                            .replace("\n", "\\n")
                            .replace("\r", "\\r");
            if (maxWidth > 0 && escaped.length() > maxWidth) {
                return "\"" + escaped.substring(0, maxWidth - 3) + "...\"";
            }
            return escaped;
        } catch (SQLException e) {
            throw new RuntimeSQLException("Cannot get value at column " + columnIndex, e);
        }
    }

    private void printTable(ResultSet rs, PrintStream out) throws SQLException {
        TablePrinter.fromResultSet(rs).print(out, 120);
    }
}