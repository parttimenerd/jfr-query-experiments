package me.bechberger.jfr.duckdb;

import jdk.jfr.consumer.RecordedObject;
import me.bechberger.jfr.duckdb.commands.*;
import picocli.CommandLine;

/**
 * Creates a DuckDB database from a JFR recording file.
 *
 * <p>java -jar jfr-query-tool.jar import recording.jfr recording.duckdb
 *
 * <p>ignores stack traces for now and makes classes, methods, threads and everything else to
 * strings
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
        })
public class Main implements Runnable {

    @CommandLine.Spec CommandLine.Model.CommandSpec spec;

    @SuppressWarnings("FieldCanBeLocal")
    private static final String INFO =
            """
            Before using this tool, you must have a recording file.
            A file can be created by starting a recording from command line:

             java -XX:StartFlightRecording:filename=recording.jfr,duration=30s ...

            A recording can also be started on an already running Java Virtual Machine:

             jcmd (to list available pids)
             jcmd <pid> JFR.start

            Recording data can be dumped to file using the JFR.dump command:

             jcmd <pid> JFR.dump filename=recording.jfr

            The contents of the recording can then be printed, for example:

                view gc recording.jfr
                view allocation-by-site recording.jfr

            For more information about available commands, use 'help'

            """;

    @Override
    public void run() {
        System.out.println(INFO);
        System.out.println();
        // print usage with picocli
        spec.commandLine().usage(System.out);
    }

    public static void main(String[] args) {
        if (!new Main().checkIfReflectiveAccessOnRecordedObjectIsAvailable()) {
            restartWithAddOpenModules(args);
        } else {
            int exitCode = new CommandLine(new Main()).execute(args);
            System.exit(exitCode);
        }
    }

    private static void restartWithAddOpenModules(String[] args) {
        try {
            String javaHome = System.getProperty("java.home");
            String javaBin = javaHome + "/bin/java";
            String classPath = System.getProperty("java.class.path");
            String className = Main.class.getCanonicalName();

            ProcessBuilder builder =
                    new ProcessBuilder(
                            javaBin,
                            "--add-opens",
                            "jdk.jfr/jdk.jfr.consumer=ALL-UNNAMED",
                            "--enable-native-access=ALL-UNNAMED", // for good measure
                            "-cp",
                            classPath,
                            className);
            for (String arg : args) {
                builder.command().add(arg);
            }
            builder.inheritIO();
            Process process = builder.start();
            int exitCode = process.waitFor();
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private boolean checkIfReflectiveAccessOnRecordedObjectIsAvailable() {
        try {
            var objectContextField = RecordedObject.class.getDeclaredField("objectContext");
            objectContextField.setAccessible(true);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}