package me.bechberger.jfr.duckdb.commands;

import me.bechberger.jfr.duckdb.BasicParallelImporter;
import me.bechberger.jfr.duckdb.Options;
import me.bechberger.jfr.duckdb.query.QueryExecutor;
import org.duckdb.DuckDBConnection;
import picocli.CommandLine;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;

@CommandLine.Command(name = "query",
        mixinStandardHelpOptions = true,
        description = "Execute a SQL query or view on the JFR DuckDB database and print the results.")
public class QueryCommand implements Runnable {

    @CommandLine.Parameters(index = "0", description = "The JFR file (or .db file) to query.")
    private String inputFile;

    @CommandLine.Parameters(index = "1", description = "The SQL query to execute, if query is just a single word, it is interpreted as a view name.")
    private String query;

    @CommandLine.Option(names = {"-f", "--format"}, description = "Output format: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})")
    private QueryExecutor.OutputFormat format = QueryExecutor.OutputFormat.TABLE;

    @CommandLine.Option(names = {"-o", "--output"}, description = "Output file (default: stdout)")
    private String outputFile;

    @CommandLine.Option(names = {"-n", "--no-cache"}, description = "Don't cache the DuckDB database in a .db file next to the JFR file.")
    private boolean noCache = false;

    @CommandLine.Mixin
    private Options commonOptions;

    @Override
    public void run() {
        // we have three scenarios:
        // 1. inputFile is a .db file -> open it directly
        // 2. inputFile is a .jfr file and noCache is false -> create/open a .db file next to it
        // 3. inputFile is a .jfr file and noCache is true -> create a temporary in-memory database
        if (inputFile.endsWith(".db")) {
            try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection(inputFile)) {
                runWithDB(conn);
            } catch (Exception e) {
                throw new RuntimeException("Error opening database file: " + inputFile, e);
            }
        } else if (noCache) {
            try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                BasicParallelImporter.importIntoConnection(Path.of(inputFile), conn, commonOptions);
                runWithDB(conn);
            } catch (Exception e) {
                throw new RuntimeException("Error importing JFR file: " + inputFile, e);
            }
        } else {
            String dbFile = inputFile + ".db";
            try {
                BasicParallelImporter.createFile(Path.of(inputFile), Path.of(dbFile), commonOptions);
            } catch (IOException | SQLException e) {
                throw new RuntimeException("Error importing JFR file: ", e);
            }
            try (DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection(dbFile)) {
                runWithDB(conn);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void runWithDB(DuckDBConnection conn) {
        QueryExecutor executor = new QueryExecutor(conn);
        try (PrintStream out = outputFile != null
                ? new PrintStream(new BufferedOutputStream(new FileOutputStream(outputFile)), true)
                : System.out) {
            if (!query.contains(" ")) {
                // interpret as view name
                query = "SELECT * FROM \"" + query + "\"";
            }
            executor.executeQuery(query, format, out);
        } catch (Exception e) {
            throw new RuntimeException("Error executing query: " + query, e);
        }
    }
}