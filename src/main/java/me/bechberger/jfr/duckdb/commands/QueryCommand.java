package me.bechberger.jfr.duckdb.commands;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import me.bechberger.jfr.duckdb.BasicParallelImporter;
import me.bechberger.jfr.duckdb.Options;
import me.bechberger.jfr.duckdb.query.QueryExecutor;
import me.bechberger.jfr.duckdb.util.TextUtil;
import org.duckdb.DuckDBConnection;
import picocli.CommandLine;

@CommandLine.Command(
        name = "query",
        mixinStandardHelpOptions = true,
        description =
                "Execute a SQL query or view on the JFR DuckDB database and print the results.")
public class QueryCommand implements Runnable {

    @CommandLine.Parameters(index = "0", description = "The JFR file (or .db file) to query.")
    private String inputFile;

    @CommandLine.Parameters(
            index = "1",
            description =
                    "The SQL query to execute, if query is just a single word, it is interpreted as a view name and empty query lists available tables and views."
    )
    private String query;

    @CommandLine.Option(
            names = {"-f", "--format"},
            description = "Output format: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})")
    private QueryExecutor.OutputFormat format = QueryExecutor.OutputFormat.TABLE;

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Output file (default: stdout)")
    private String outputFile;

    @CommandLine.Option(
            names = {"-n", "--no-cache"},
            description = "Don't cache the DuckDB database in a .db file next to the JFR file.")
    private boolean noCache = false;

    @CommandLine.Mixin private Options commonOptions;

    @Override
    public void run() {
        // we have three scenarios:
        // 1. inputFile is a .db file -> open it directly
        // 2. inputFile is a .jfr file and noCache is false -> create/open a .db file next to it
        // 3. inputFile is a .jfr file and noCache is true -> create a temporary in-memory database
        if (inputFile.endsWith(".db")) {
            try (DuckDBConnection conn =
                    (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + inputFile)) {
                runWithDB(conn);
            } catch (Exception e) {
                throw new RuntimeException("Error opening database file: " + inputFile, e);
            }
        } else if (noCache) {
            try (DuckDBConnection conn =
                    (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                BasicParallelImporter.importIntoConnection(Path.of(inputFile), conn, commonOptions);
                runWithDB(conn);
            } catch (Exception e) {
                throw new RuntimeException("Error importing JFR file: " + inputFile, e);
            }
        } else {
            String dbFile = inputFile.replace(".jfr", ".db");
            Path path = Path.of(dbFile);
            try {
                if (!Files.exists(path) || Files.getLastModifiedTime(path).toMillis() <
                        Files.getLastModifiedTime(Path.of(inputFile)).toMillis()) {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        BasicParallelImporter.createFile(Path.of(inputFile), path, commonOptions);
                    } catch (IOException | SQLException e) {
                        throw new RuntimeException("Error importing JFR file: ", e);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + dbFile)) {
                runWithDB(conn);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void runWithDB(DuckDBConnection conn) {
        QueryExecutor executor = new QueryExecutor(conn);
        try (PrintStream out =
                outputFile != null
                        ? new PrintStream(
                                new BufferedOutputStream(new FileOutputStream(outputFile)), true)
                        : System.out) {
            if (query.isEmpty()) {
                System.out.println("Available tables:");
                for (String table : getAvailableTables(conn)) {
                    System.out.println(" - " + table);
                }

                System.out.println("Available views:");
                for (String view : getAvailableViews(conn)) {
                    System.out.println(" - " + view);
                }
            }
            if (!query.contains(" ")) {
                // interpret as view name
                handleView(conn, out, query);
                return;
            }
            executor.executeQuery(query, format, out);
        } catch (Exception e) {
            throw new RuntimeException("Error executing query: " + query, e);
        }
    }

    private void handleView(DuckDBConnection conn, PrintStream out, String query)
            throws SQLException {
        List<String> availableViews = getAvailableViews(conn);
        if (availableViews.contains(query)) {
            QueryExecutor executor = new QueryExecutor(conn);
            executor.executeQuery("SELECT * FROM \"" + query + "\"", format, out);
        } else {
            List<String> suggestions =
                    TextUtil.findClosestMatches(query, getAvailableTablesAndViews(conn), 3, 5);
            String suggestionText =
                    suggestions.isEmpty()
                            ? ""
                            : " Did you mean: " + String.join(", ", suggestions) + "?";
            throw new IllegalArgumentException(query + " not found: " + query + suggestionText);
        }
    }

    private List<String> getAvailableViews(DuckDBConnection conn) throws SQLException {
        try (var stmt = conn.createStatement();
                var rs = stmt.executeQuery("SELECT view_name FROM duckdb_views() WHERE NOT internal")) {
            List<String> views = new ArrayList<>();
            while (rs.next()) {
                views.add(rs.getString(1));
            }
            return views;
        }
    }

    private List<String> getAvailableTables(DuckDBConnection conn) throws SQLException {
        try (var stmt = conn.createStatement();
                var rs = stmt.executeQuery("SHOW TABLES")) {
            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
            return tables;
        }
    }

    private List<String> getAvailableTablesAndViews(DuckDBConnection conn) throws SQLException {
        List<String> combined = new ArrayList<>();
        combined.addAll(getAvailableTables(conn));
        combined.addAll(getAvailableViews(conn));
        return combined;
    }
}