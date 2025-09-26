package me.bechberger.jfr.duckdb.commands;

import picocli.CommandLine;

import java.io.PrintWriter;

@CommandLine.Command(name = "macros",
        mixinStandardHelpOptions = true,
        description = "List available SQL macros (views) for JFR analysis.")
public class MacrosCommand implements Runnable {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli

    @Override
    public void run() {
        PrintWriter out = spec.commandLine().getOut();
        out.println("Not all macros are available for every file, as they depend");
        out.println("on the events present in the JFR recording and the options");
        out.println("used during import.");
        out.println();
        for (var macro : me.bechberger.jfr.duckdb.definitions.MacroCollection.getMacros()) {
            out.println(macro.nameWithArgs());
            out.println("  " + macro.description());
            out.println("  Sample Usage: " + macro.sampleUsages());
            out.println("  Definition:\n    " + macro.definition().lines().reduce((a, b) -> a + "\n    " + b).orElse(""));
            out.println();
        }
    }
}