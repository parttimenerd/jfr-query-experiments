package me.bechberger.jfr.duckdb.query;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TablePrinterFromResultSetTest {

    private Connection conn;

    private Connection openInMemoryDuckDb() throws Exception {
        // duckdb in-memory JDBC URL: jdbc:duckdb:memory:
        // The DuckDB JDBC driver must be present on the classpath.
        String url = "jdbc:duckdb:memory:";
        conn = DriverManager.getConnection(url);
        return conn;
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    @Test
    void testFromResultSetWithNullsAndTypes() throws Exception {
        try (Connection c = openInMemoryDuckDb(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE test (id INTEGER, name VARCHAR, value DOUBLE)");
            s.execute("INSERT INTO test VALUES (1, 'one', 1.5), (2, NULL, 2.5), (3, 'three', NULL)");

            try (ResultSet rs = s.executeQuery("SELECT id, name, value FROM test ORDER BY id")) {
                TablePrinter tp = TablePrinter.fromResultSet(rs);
                String out = tp.toString(80);
                // Assert the entire table output matches the expected rendered table
                String expected = """
                id name  value
                -- ----- -----
                 1 one     1.5
                 2         2.5
                 3 three      
                """;
                assertEquals(expected.trim(), out.trim());
            }
        }
    }

    @Test
    void testNumericRightAlignmentDeterminedFromResultSet() throws Exception {
        try (Connection c = openInMemoryDuckDb(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE test2 (label VARCHAR, count INTEGER)");
            s.execute("INSERT INTO test2 VALUES ('a', 10), ('bbb', 2000), ('c', 3)");

            try (ResultSet rs = s.executeQuery("SELECT label, count FROM test2 ORDER BY count DESC")) {
                TablePrinter tp = TablePrinter.fromResultSet(rs);
                String out = tp.toString(40);
                String expected = """
                label count
                ----- -----
                bbb    2000
                a        10
                c         3
                """;
                assertEquals(expected.trim(), out.trim());
            }
        }
    }

    @Test
    void testMetricRightAlignment() throws Exception {
        try (Connection c = openInMemoryDuckDb(); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE metrics (name VARCHAR, metric VARCHAR)");
            s.execute("INSERT INTO metrics VALUES ('A', '3.3%'), ('B', '4kB'), ('Long', '12.34%')");

            try (ResultSet rs = s.executeQuery("SELECT name, metric FROM metrics ORDER BY name")) {
                TablePrinter tp = TablePrinter.fromResultSet(rs);
                String out = tp.toString(40);
                // Assert whole-table output matches the expected rendered table (ensures right alignment preserved)
                String expected = """
                name metric
                ---- ------
                A      3.3%
                B       4kB
                Long 12.34%
                """;
                assertEquals(expected.trim(), out.trim());
            }
        }
    }
}