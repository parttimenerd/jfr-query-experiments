package me.bechberger.jfr.duckdb.commands;

import me.bechberger.jfr.duckdb.JFRFile;
import me.bechberger.jfr.duckdb.JFRFileHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueryTest {

    private static JFRFileHelper testJfrFile;

    static {
        try {
            testJfrFile = new JFRFileHelper(JFRFile.CONTAINER);
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    record Output(String out, String err, int returnCode) {}

    private Output runQueryCommand(String... args) {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
        try {
            QueryCommand command = new QueryCommand();
            CommandLine cmd = new CommandLine(command);
            int ret = cmd.execute(args);
            return new Output(outContent.toString(), errContent.toString(), ret);
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testSuccessfulSelect(boolean jfrFile) {
        Output output = runQueryCommand((jfrFile ? testJfrFile.getJfrFile() : testJfrFile.getDbFile()).toString(),
                "SELECT COUNT(*) AS event_count FROM events");
        assertEquals(0, output.returnCode());
        assertEquals("", output.err());
        assertEquals("""
                event_count
                -----------
                        125
                """, output.out());
    }

    @Test
    public void testInvalidQuery() {
        Output output = runQueryCommand(testJfrFile.getDbFile().toString(),
                "SELECT * FROM non_existing_table");
        assertEquals(1, output.returnCode());
        assertEquals("", output.out());
        System.out.println(output.err);
        assertEquals("""
                Error executing query 'SELECT * FROM non_existing_table' on file jfr_files/container.db
                Catalog Error: Table with name non_existing_table does not exist!
                Did you mean "longest-compilations"?
                
                LINE 1: SELECT * FROM non_existing_table
                                      ^
                """, output.err());
    }

    @Test
    public void testUnknownView() {
        Output output = runQueryCommand(testJfrFile.getDbFile().toString(),
                "non_existing_view");
        assertEquals(1, output.returnCode());
        assertEquals("", output.out());
        System.out.println(output.err);
        assertEquals("""
                Error for file jfr_files/container.db:
                View non_existing_view not found
                """, output.err);
    }

    @Test
    public void testFileNotFound() {
        Output output = runQueryCommand("non_existing_file.jfr",
                "SELECT COUNT(*) AS event_count FROM events");
        assertEquals(1, output.returnCode());
        assertEquals("", output.out());
        System.out.println(output.err);
        assertEquals("""
                Error transforming JFR file into a database: non_existing_file.jfr (No such file or directory)
                """, output.err);
    }

    @Test
    public void testFileInvalidFormat() throws IOException {
        // Create a temporary invalid file
        Path file = Files.createTempFile("invalid", ".jfr");
        file.toFile().deleteOnExit();
        Files.write(file, "This is not a valid JFR file".getBytes());
        Output output = runQueryCommand(file.toString(),
                "SELECT COUNT(*) AS event_count FROM events");
        assertEquals(1, output.returnCode());
        assertEquals("", output.out());
        System.out.println(output.err);
        assertEquals("""
                Error transforming JFR file into a database: $FILE (not a valid JFR file)
                """.replace("$FILE", file.toString()), output.err);
    }

    @Test
    public void testTableNamesAllowedAsViews() {
        Output output = runQueryCommand(testJfrFile.getDbFile().toString(),
                "Events");
        System.out.println(output);
        assertEquals(0, output.returnCode());
        assertEquals("", output.err());
        assertTrue(output.out().startsWith("""
                name                           count
                ----------------------------- ------
                CompilerInlining              358246
                FileRead                      314904
                CompilerPhase                 257994
                """));
    }

    @Test
    public void testLowerCaseTableName() {
        Output output = runQueryCommand(testJfrFile.getDbFile().toString(),
                "events");
        System.out.println(output);
        assertEquals(0, output.returnCode());
        assertEquals("", output.err());
        assertTrue(output.out().startsWith("""
                name                           count
                ----------------------------- ------
                CompilerInlining              358246
                FileRead                      314904
                CompilerPhase                 257994
                """));
    }

    @Test
    public void testNoPassedQueryListsTablesAndViews() {
        Output output = runQueryCommand(testJfrFile.getDbFile().toString());
        assertEquals(0, output.returnCode());
        assertEquals("", output.err());
        System.out.println(output.out());
        assertTrue(output.out().startsWith("""
                Available tables:
                 - ActiveRecording
                 - ActiveSetting
                """));
        assertTrue(output.out().contains("""
                Available views:
                 - active-recordings
                 - active-settings
                 - allocation-by-class
                """));
    }

    @Test
    public void testCSVOutputFormat() {
        Output output = runQueryCommand(testJfrFile.getDbFile().toString(),
                "--csv",
                "SELECT 'test', COUNT(*) AS event_count FROM events");
        assertEquals(0, output.returnCode());
        assertEquals("", output.err());
        assertEquals("""
                'test',event_count
                test,125
                """, output.out());
    }
}