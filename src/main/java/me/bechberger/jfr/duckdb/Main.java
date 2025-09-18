package me.bechberger.jfr.duckdb;

import org.duckdb.DuckDBConnection;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

/**
 * Creates a DuckDB database from a JFR recording file.
 * <p>
 * java -jar jfr-query-tool.jar import recording.jfr recording.duckdb
 * <p>
 * ignores stack traces for now and makes classes, methods, threads and everything else to strings
 */
@CommandLine.Command(
        name = "import",
        mixinStandardHelpOptions = true,
        version = "0.1",
        description = "Import a JFR recording into a DuckDB database"
)
public class Main implements Runnable {

    @CommandLine.Mixin
    private Options options;

    @CommandLine.Parameters(index = "0", description = "The input JFR recording file")
    private String inputFile;

    @CommandLine.Parameters(index = "1", description = "The output DuckDB database file")
    private String outputFile;

    @Override
    public void run() {
        if (Files.exists(Path.of(outputFile))) {
            try {
                Files.delete(Path.of(outputFile));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            List<DuckDBConnection> conns = new ArrayList<>();
            new BasicParallelImporter(() -> {
                try {
                    DuckDBConnection duckDBConnection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + outputFile);
                    conns.add(duckDBConnection);
                    return duckDBConnection;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }, options).importRecording(Path.of(inputFile));
            for (DuckDBConnection duckDBConnection : conns) {
                duckDBConnection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}