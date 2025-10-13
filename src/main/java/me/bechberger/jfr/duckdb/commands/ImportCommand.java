package me.bechberger.jfr.duckdb.commands;

import static me.bechberger.jfr.duckdb.BasicParallelImporter.createFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import me.bechberger.jfr.duckdb.Options;
import picocli.CommandLine;

@CommandLine.Command(
        name = "import",
        mixinStandardHelpOptions = true,
        version = "0.1",
        description = "Import a JFR recording into a DuckDB database")
public class ImportCommand implements Runnable {

    @CommandLine.Mixin private Options options;

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
            createFile(Path.of(inputFile), Path.of(outputFile), options);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
