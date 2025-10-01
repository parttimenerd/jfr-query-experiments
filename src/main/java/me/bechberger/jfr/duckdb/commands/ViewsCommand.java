package me.bechberger.jfr.duckdb.commands;

import me.bechberger.jfr.duckdb.definitions.View;
import me.bechberger.jfr.duckdb.definitions.ViewCollection;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.util.List;

@CommandLine.Command(name = "views",
        mixinStandardHelpOptions = true,
        description = "List available SQL views for JFR analysis.")
public class ViewsCommand implements Runnable {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec; // injected by picocli
    
    @Override
    public void run() {
        PrintWriter out = spec.commandLine().getOut();
        out.println("Not all views are available for every file, as they depend");
        out.println("on the events present in the JFR recording and the options");
        out.println("used during import.");
        out.println();
        ViewCollection.getViewsByCategory().forEach((category, views) -> {
            out.println(category + ":");
            List<View> sortedViews = views.stream().sorted((v1, v2) -> v1.name().compareTo(v2.name())).toList();
            int maxLength = sortedViews.stream().mapToInt(v -> v.name().length()).max().orElse(0) + 2;
            int columns = 3;
            int rowsNeeded = (sortedViews.size() + columns - 1) / columns; // ceiling division

            for (int i = 0; i < rowsNeeded; i++) {
                for (int j = 0; j < columns; j++) {
                    int index = i + j * rowsNeeded;
                    if (index < sortedViews.size()) {
                        String name = sortedViews.get(index).name();
                        out.print(name);
                        for (int k = name.length(); k < maxLength; k++) {
                            out.print(" ");
                        }
                    }
                }
                out.println(); // Add newline after each row
            }
        });
        out.println();
        out.println("Use the 'query' command to execute a view, e.g.:");
        out.println("  duckdb query recording.jfr jfr$active-recordings");
    }
}