package me.bechberger.jfr.duckdb.commands;

import me.bechberger.jfr.duckdb.JFRFile;
import me.bechberger.jfr.duckdb.JFRFileHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ContextTest {

    private static JFRFileHelper testJfrFile;

    static {
        try {
            testJfrFile = new JFRFileHelper(JFRFile.CONTAINER);
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    record Output(String out, String err, int returnCode) {}

    private QueryTest.Output runQueryCommand(String... args) {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));
        try {
            ContextCommand command = new ContextCommand();
            CommandLine cmd = new CommandLine(command);
            int ret = cmd.execute(args);
            return new QueryTest.Output(outContent.toString(), errContent.toString(), ret);
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testContextGeneration(boolean jfrFile) {
        QueryTest.Output output = runQueryCommand((jfrFile ? testJfrFile.getJfrFile() : testJfrFile.getDbFile()).toString());
        assertEquals(0, output.returnCode());
        System.out.println(output.out());
        assertTrue(output.out().contains("""
                TABLES:
                - ActiveRecording: Column "maxSize": DataAmount(BYTES)
                  - startTime (TIMESTAMP)
                  - id (BIGINT)
                  - name (VARCHAR)
                """));
    }
}