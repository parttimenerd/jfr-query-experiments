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
import java.util.concurrent.Callable;

import me.bechberger.jfr.duckdb.BasicParallelImporter;
import me.bechberger.jfr.duckdb.Options;
import me.bechberger.jfr.duckdb.RuntimeSQLException;
import me.bechberger.jfr.duckdb.query.QueryExecutor;
import me.bechberger.jfr.duckdb.util.TextUtil;
import org.duckdb.DuckDBConnection;
import picocli.CommandLine;

import static me.bechberger.jfr.duckdb.util.SQLUtil.getTableNames;

@CommandLine.Command(
        name = "query",
        mixinStandardHelpOptions = true,
        description =
                "Execute a SQL query or view on the JFR DuckDB database and print the results.")
public class QueryCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "The JFR file (or .db file) to query.")
    private String inputFile;

    @CommandLine.Parameters(
            index = "1",
            description =
                    "The SQL query to execute, if query is just a single word, it is interpreted as a view name and empty query lists available tables and views.",
            defaultValue = ""
    )
    private String query;

    @CommandLine.Option(names = "--csv", description = "Output format: CSV")
    private boolean formatCsv = false;

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
    public Integer call() {
        // we have three scenarios:
        // 1. inputFile is a .db file -> open it directly
        // 2. inputFile is a .jfr file and noCache is false -> create/open a .db file next to it
        // 3. inputFile is a .jfr file and noCache is true -> create a temporary in-memory database
        if (inputFile.endsWith(".db")) {
            try (DuckDBConnection conn =
                    (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + inputFile)) {
                return runWithDB(conn, inputFile);
            } catch (SQLException e) {
                System.err.println("Error opening database file: " + inputFile);
                System.err.println(e.getMessage());
                return 1;
            }
        } else if (noCache) {
            try (DuckDBConnection conn =
                    (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                BasicParallelImporter.importIntoConnection(Path.of(inputFile), conn, commonOptions);
                runWithDB(conn, inputFile);
            } catch (SQLException e) {
                System.err.println("Error opening database file: " + inputFile);
                System.err.println(e.getMessage());
                return 1;
            } catch (IOException e) {
                System.err.println("Error transforming JFR file into a database: " + inputFile);
                System.err.println(e.getMessage());
                return 1;
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
                    }
                    try {
                        BasicParallelImporter.createFile(Path.of(inputFile), path, commonOptions);
                    } catch (IOException e) {
                        if (e.getMessage().equals("Not a complete Chunk header") || e.getMessage().equals("Chunk contains no data")) {
                            System.err.println("Error transforming JFR file into a database: " + inputFile + " (not a valid JFR file)");
                            return 1;
                        }
                        System.err.println("Error transforming JFR file into a database: " + e.getMessage());
                        return 1;
                    } catch (SQLException e) {
                        System.err.println("Error creating database for JFR file: " + inputFile);
                        System.err.println(e.getMessage());
                        return 1;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + dbFile)) {
                return runWithDB(conn, inputFile);
            } catch (SQLException e) {
                System.err.println("Error opening database file: " + dbFile);
                System.err.println(e.getMessage());
                return 1;
            }
        }
        return 0;
    }

    private QueryExecutor.OutputFormat getFormat() {
        if (formatCsv) {
            return QueryExecutor.OutputFormat.CSV;
        } else {
            return QueryExecutor.OutputFormat.TABLE;
        }
    }

    private int runWithDB(DuckDBConnection conn, String file) {
        QueryExecutor executor = new QueryExecutor(conn);
        try (PrintStream out =
                outputFile != null
                        ? new PrintStream(
                                new BufferedOutputStream(new FileOutputStream(outputFile)), true)
                        : System.out) {
            if (query.isEmpty()) {
                System.out.println("Available tables:");
                for (String table : getTableNames(conn).stream().sorted().toList()) {
                    System.out.println(" - " + table);
                }

                System.out.println("Available views:");
                for (String view : getAvailableViews(conn)) {
                    System.out.println(" - " + view);
                }
                return 0;
            }
            if (!query.contains(" ")) {
                // interpret as view name
                handleView(conn, out, query);
                return 0;
            }
            executor.executeQuery(query, getFormat(), out);
        } catch (SQLException e) {
            System.err.println("Error executing query '" + query + "' on file " + file);
            System.err.println(e.getMessage());
            return 1;
        } catch (RuntimeSQLException e) {
            System.err.println("Error executing query '" + query + "' on file " + file);
            System.err.println(e.getCause().getMessage());
            return 1;
        } catch (ViewNotFoundException e) {
            System.err.println("Error for file " + file + ":");
            System.err.println(e.getMessage());
            return 1;
        } catch (Exception e) {
            throw new RuntimeSQLException("Error executing query: " + query, e);
        }
        return 0;
    }

    private class ViewNotFoundException extends RuntimeException {
        public ViewNotFoundException(String message) {
            super(message);
        }
    }

    private void handleView(DuckDBConnection conn, PrintStream out, String query)
            throws SQLException {
        List<String> availableViewsAndTables = getAvailableTablesAndViews(conn);
        if (availableViewsAndTables.stream().anyMatch(v -> v.equalsIgnoreCase(query))) {
            QueryExecutor executor = new QueryExecutor(conn);
            executor.executeQuery("SELECT * FROM \"" + query + "\"", getFormat(), out);
        } else {
            List<String> suggestions =
                    TextUtil.findClosestMatches(query, availableViewsAndTables, 3, 5);
            String suggestionText =
                    suggestions.isEmpty()
                            ? ""
                            : ": Did you mean: " + String.join(", ", suggestions) + "?";
            throw new ViewNotFoundException("View " + query + " not found" + suggestionText);
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

    private List<String> getAvailableTablesAndViews(DuckDBConnection conn) throws SQLException {
        List<String> combined = new ArrayList<>();
        combined.addAll(getTableNames(conn).stream().sorted().toList());
        combined.addAll(getAvailableViews(conn));
        return combined;
    }
}