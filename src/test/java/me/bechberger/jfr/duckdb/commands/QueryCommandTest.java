package me.bechberger.jfr.duckdb.commands;

import me.bechberger.jfr.duckdb.JFRFile;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.junit.jupiter.api.Assertions.*;

@Disabled("not ready yet")
public class QueryCommandTest {
    private static final Path testFile = JFRFile.DEFAULT.getPath();
    private PrintStream originalOut;
    private ByteArrayOutputStream outContent;

    @BeforeEach
    void setUpStreams() {
        originalOut = System.out;
        outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
    }

    @AfterEach
    void restoreStreams() {
        System.setOut(originalOut);
    }

    /**
     * Helper to run QueryCommand with arguments and capture output as String.
     */
    private String runQueryCommandWithArgs(String... args) {
        QueryCommand command = new QueryCommand();
        CommandLine cmd = new CommandLine(command);
        int exitCode = cmd.execute(args);
        return outContent.toString();
    }

    @ParameterizedTest
    @CsvSource({
        "TABLE,false",
        "TABLE,true",
        "CSV,false",
        "CSV,true"
    })
    void testRecordingViewTableAndCsv(String format, boolean noCache) {
        String output = runQueryCommandWithArgs(
            testFile.toString(),
            "recording",
            "-f", format,
            noCache ? "--no-cache" : ""
        );
        String expectedTableOutput = "...full expected table output...";
        String expectedCsvOutput = "Start Time,Duration\n...full expected csv output...";
        if (format.equals("TABLE")) {
            assertEquals(expectedTableOutput, output, "Table output should match expected");
        } else {
            assertEquals(expectedCsvOutput, output, "CSV output should match expected");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRecSuggestion(boolean noCache) {
        Exception ex = assertThrows(RuntimeException.class, () -> {
            runQueryCommandWithArgs(
                testFile.toString(),
                "rec",
                noCache ? "--no-cache" : ""
            );
        });
        assertTrue(ex.getMessage().contains("Did you mean: recording"), "Should suggest 'recording' for 'rec'");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testNoQueryListsAllTablesAndViews(boolean noCache) {
        String output = runQueryCommandWithArgs(
            testFile.toString(),
            noCache ? "--no-cache" : ""
        );
        // Check for known table/view names (adjust as needed for your schema)
        assertTrue(output.contains("recording"), "Output should list 'recording' view");
        assertTrue(output.contains("Class"), "Output should list 'Class' table");
        // Add more asserts for other expected tables/views if needed
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSelectAllFromClassWorks(boolean noCache) {
        List<String> args = new ArrayList<>(List.of(
            testFile.toString(),
            "SELECT * FROM Class"
        ));
        if (noCache) {
            args.add("--no-cache");
        }
        String output = runQueryCommandWithArgs(args.toArray(new String[0]));
        // Check for expected column names (adjust as needed for your schema)
        assertTrue(output.contains("_id"), "Output should contain '_id' column");
        assertTrue(output.contains("javaName"), "Output should contain 'javaName' column");
        // Optionally check for at least one row (if test DB is not empty)
        // assertTrue(output.split("\n").length > 1, "Output should contain at least one row");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInvalidQueryShowsError(boolean noCache) {
        Exception ex = assertThrows(RuntimeException.class, () -> {
            runQueryCommandWithArgs(
                testFile.toString(),
                "SELECT * FROM NonExistentTable",
                noCache ? "--no-cache" : ""
            );
        });
        String msg = ex.getMessage();
        assertTrue(msg.contains("NonExistentTable"), "Error message should mention the missing table");
        assertTrue(msg.toLowerCase().contains("error"), "Error message should contain 'error'");
        // Optionally check for DuckDB-specific error text
        // assertTrue(msg.toLowerCase().contains("duckdb"), "Error message should mention DuckDB");
    }

    @Test
    void testOutputFile() throws IOException {
        Path tempOutput = Files.createTempFile("query-output", ".txt");
        try {
            runQueryCommandWithArgs(
                testFile.toString(),
                "recording",
                "-f", "CSV",
                "-o", tempOutput.toString(),
                "--no-cache"
            );
            String fileContent = Files.readString(tempOutput);
            assertTrue(fileContent.contains("Start Time,Duration"), "Output file should contain CSV header");
            assertTrue(fileContent.contains("2023-10-01"), "Output file should contain expected data");
        } finally {
            Files.deleteIfExists(tempOutput);
        }
    }
}