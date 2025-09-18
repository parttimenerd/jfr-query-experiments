package me.bechberger;

import me.bechberger.jfr.tool.*;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "query",
        mixinStandardHelpOptions = true,
        version = "0.1",
        description = "Java Flight Recorder Query command line tool",
        subcommands = {
                HelpCommand.class,
                ViewCommand.class,
                QueryCommand.class,
                WebCommand.class,
                me.bechberger.jfr.duckdb.Main.class,
        }
)
public class Main implements Callable<Integer> {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    private static final int EXIT_OK = 0;
    private static final int EXIT_FAILED = 1;
    private static final int EXIT_WRONG_ARGUMENTS = 2;

    private static String INFO = """
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
    public Integer call() {
        System.out.println(INFO);
        System.out.println();
        // print usage with picocli
        spec.commandLine().usage(System.out);
        return EXIT_OK;
    }

    public static void main(String[] args) {
        CommandLine cli = new CommandLine(new Main());
        cli.getSubcommands().forEach((subcommandName, command) -> {
            if (command.getCommand() instanceof Footerable f) {
                command.getCommandSpec().usageMessage().footer(f.footer());
            }
        });
        int exitCode = cli.execute(args);
        if (exitCode != EXIT_OK) {
            System.exit(exitCode);
        }
    }
}