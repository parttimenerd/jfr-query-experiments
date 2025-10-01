package me.bechberger.jfr.duckdb.commands;

import me.bechberger.jfr.duckdb.definitions.MacroCollection;
import me.bechberger.jfr.duckdb.definitions.ViewCollection;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the basic functionality of the {@link ViewsCommand} and
 * {@link MacrosCommand}.
 */
public class ViewsAndMacrosTest {

    @Test
    public void testThatEveryViewIsListed() {
        ViewsCommand viewsCommand = new ViewsCommand();
        CommandLine cmd = new CommandLine(viewsCommand);
        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));
        cmd.execute();
        String output = sw.toString();
        System.out.println(output);
        for (var viewAndCat : ViewCollection.getViewsByCategory().entrySet()) {
            assertThat(output).contains(viewAndCat.getKey() + ":");
            for (var view : viewAndCat.getValue()) {
                assertThat(output).contains(view.name());
            }
        }
        // assert that there is only one line break in the end and now empty lines
        assertThat(output).doesNotEndWith("\n\n");
    }

    @Test
    public void testThatEveryMacroIsListed() {
        MacrosCommand macrosCommand = new MacrosCommand();
        CommandLine cmd = new CommandLine(macrosCommand);
        StringWriter sw = new StringWriter();
        cmd.setOut(new PrintWriter(sw));
        cmd.execute();
        String output = sw.toString();
        System.out.println(output);
        for (var macro : MacroCollection.getMacros()) {
            assertThat(output).contains(macro.nameWithArgs());
            assertThat(output).contains(macro.description());
            assertThat(output).contains(macro.sampleUsages());
        }
    }
}