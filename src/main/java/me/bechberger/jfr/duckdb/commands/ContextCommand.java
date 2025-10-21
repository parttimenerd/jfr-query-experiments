package me.bechberger.jfr.duckdb.commands;

import me.bechberger.jfr.duckdb.BasicParallelImporter;
import me.bechberger.jfr.duckdb.Options;
import me.bechberger.jfr.duckdb.RuntimeSQLException;
import me.bechberger.jfr.duckdb.util.SQLUtil;
import org.duckdb.DuckDBConnection;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(
        name = "context",
        mixinStandardHelpOptions = true,
        description = "Create a description of the tables, macros and views for generating SQL queries using AI"
)
public class ContextCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "The JFR file (or .db file) to describe.")
    private String inputFile;

    @CommandLine.Option(
            names = {"-o", "--output"},
            description = "Output file (default: stdout)")
    private String outputFile = "";

    @CommandLine.Mixin private Options commonOptions;

    @Override
    public Integer call() throws IOException {
        PrintStream out = outputFile.isEmpty() ? System.out : new PrintStream(Files.newOutputStream(Path.of(outputFile)));
        if (inputFile.endsWith(".jfr")) {
            try (DuckDBConnection conn =
                    (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:")) {
                BasicParallelImporter.importIntoConnection(Path.of(inputFile), conn, commonOptions);
                return runWithDB(conn, out);
            }  catch (SQLException e) {
                System.err.println("Error opening database file: " + inputFile);
                System.err.println(e.getMessage());
                return 1;
            }
        } else {
            Path dbPath = Path.of(inputFile);
            try (DuckDBConnection conn =
                    (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + dbPath.toAbsolutePath())) {
                return runWithDB(conn, out);
            }  catch (SQLException e) {
                System.err.println("Error opening database file: " + inputFile);
                System.err.println(e.getMessage());
                return 1;
            }
        }
    }

    int runWithDB(DuckDBConnection conn, PrintStream out) {
        try {
            out.printf("""
                            You are an expert DuckDB assistant for analyzing Java Flight Recorder (JFR) data.
                            Your goal is to help users by writing SQL queries.
                            You can use the following tables, views, and macros in your DuckDB SQL queries.
                            
                            TABLES:
                            %s
                            VIEWS:
                            %s
                            MACROS:
                            %s
                            
                            GUIDELINES:
                            - If a query is needed, generate valid DuckDB SQL and use the built-in macros where appropriate.
                            - Do not make up any table, view, or macro names.
                            - Emit the SQL query with a short explanation
                            %n""", getTableDescriptions(conn),
                    getViewDescriptions(conn),
                    getMacroDescriptions(conn));
        } catch (SQLException e) {
            System.err.println("Error generating context: " + e.getMessage());
            return 1;
        } catch (RuntimeSQLException e) {
            System.err.println("Error generating context: " + e.getMessage());
            System.err.println(e.getCause().getMessage());
            return 1;
        }
        return 0;
    }

    String getTableDescriptions(DuckDBConnection conn) throws SQLException {
        return SQLUtil.getTableNames(conn).stream()
                .sorted()
                .map(tableName -> getTableContext(conn, tableName))
                .collect(Collectors.joining());
    }

    @Nullable String getTableComment(DuckDBConnection conn, String tableName) {
        try (var stmt = conn.createStatement()) {
            var rs = stmt.executeQuery(
                    "SELECT comment FROM duckdb_tables() WHERE table_name = '" + tableName + "'");
            if (rs.next()) {
                return rs.getString("comment");
            }
            return null;
        } catch (SQLException e) {
            throw new RuntimeSQLException("Getting comment for table " + tableName + " failed", e);
        }
    }

    /**
     * Returns the context (column names and types) for a given table.
     * <code>
     *     - table_name: comment
     *      - column1 (type1)
     *      ...
     * </code>
     */
    String getTableContext(DuckDBConnection conn, String tableName) {
        String comment = getTableComment(conn, tableName);
        try (var stmt = conn.createStatement()) {
            var rs = stmt.executeQuery(
                    "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='main' AND table_name=" + stmt.enquoteLiteral(tableName) + " ORDER BY ordinal_position");
            StringBuilder sb = new StringBuilder();
            sb.append("- ").append(tableName);
            if (comment != null && !comment.isEmpty()) {
                sb.append(": ").append(comment);
            }
            sb.append("\n");
            while (rs.next()) {
                String columnName = rs.getString("column_name");
                String dataType = rs.getString("data_type");
                sb.append("  - ").append(columnName).append(" (").append(dataType).append(")\n");
            }
            return sb.toString();
        } catch (SQLException e) {
            throw new RuntimeSQLException("Getting context for table name " + tableName + " failed", e);
        }
    }

    String getViewDescriptions(DuckDBConnection conn) throws SQLException {
        try (var stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT view_name, sql, comment FROM duckdb_views() WHERE NOT internal ORDER BY view_name")) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                String viewName = rs.getString("view_name");
                String viewDefinition = rs.getString("sql");
                String comment = rs.getString("comment");
                sb.append("- ").append(viewName).append(": ");
                if (comment != null && !comment.isEmpty()) {
                    sb.append(comment.trim().lines().collect(Collectors.joining("\n  ")));
                }
                sb.append("\n");
                sb.append("  - Definition: ").append(viewDefinition.replaceAll("\\s+", " ").trim()).append("\n");
            }
            return sb.toString();
        }
    }

    String getMacroDescriptions(DuckDBConnection conn) throws SQLException {
        try (var stmt = conn.createStatement();
             var rs = stmt.executeQuery("SELECT function_name, parameters, macro_definition, comment FROM duckdb_functions() where not internal ORDER BY function_name")) {
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                String macroName = rs.getString("function_name");
                String parameters = rs.getString("parameters");
                String macroDefinition = rs.getString("macro_definition");
                String comment = rs.getString("comment");
                sb.append("- ").append(macroName).append(parameters).append(": ");
                if (comment != null && !comment.isEmpty()) {
                    sb.append(comment.trim().lines().collect(Collectors.joining("\n  ")));
                }
                sb.append("\n");
                sb.append("  - Definition: ").append(macroDefinition.replaceAll("\\s+", " ").trim()).append("\n");
            }
            return sb.toString();
        }
    }
}