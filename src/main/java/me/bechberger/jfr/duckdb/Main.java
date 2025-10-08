package me.bechberger.jfr.duckdb;

import me.bechberger.jfr.duckdb.commands.*;
import picocli.CommandLine;

/**
 * Creates a DuckDB database from a JFR recording file.
 * <p>
 * java -jar jfr-query-tool.jar import recording.jfr recording.duckdb
 * <p>
 * ignores stack traces for now and makes classes, methods, threads and everything else to strings
 */
@CommandLine.Command(
        name = "duckdb",
        mixinStandardHelpOptions = true,
        version = "0.1",
        description = "DuckDB tools for JFR recordings",
        subcommands = {
                ImportCommand.class,
                QueryCommand.class,
                MacrosCommand.class,
                ViewsCommand.class,
                CommandLine.HelpCommand.class
        }
)
public class Main {
    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }
}