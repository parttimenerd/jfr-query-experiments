package me.bechberger.jfr.duckdb;

import me.bechberger.jfr.duckdb.definitions.ViewCollection;
import me.bechberger.jfr.duckdb.query.QueryExecutor;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.SQLException;
import java.util.stream.Stream;

/**
 * Idea: Compare the view output with expected output, uses CSV output and ignores spaces
 */
public class ViewsTest {

    private static final Path jfrFile = Path.of("jfr_files/renaissance-dotty_default_G1.jfr");
    private static Path tmpDBFile = null;

    @BeforeAll
    public static void setup() throws IOException, SQLException {
        tmpDBFile = Files.createTempFile("duckdb_test", ".db");
        Files.deleteIfExists(tmpDBFile);
        if (!Files.exists(jfrFile)) {
            throw new IOException("Test file not found: " + jfrFile.toAbsolutePath());
        }
        BasicParallelImporter.createFile(jfrFile, tmpDBFile.toAbsolutePath(), new Options());
    }

    @AfterAll
    public static void cleanup() throws IOException {
        if (tmpDBFile != null) {
            Files.deleteIfExists(tmpDBFile);
        }
    }

    private DuckDBConnection conn;

    @BeforeEach
    public void openConnection() throws SQLException {
        conn = (DuckDBConnection) java.sql.DriverManager.getConnection("jdbc:duckdb:" + tmpDBFile.toAbsolutePath());
    }

    @AfterEach
    public void closeConnection() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    public record ExpectedView(String view, String expectedCSV) {}

    static Stream<ExpectedView> expectedViews() {
        return Stream.of(
                new ExpectedView("active-recordings",
                        """
                                Start,Duration,Name,Destination,MaxAge,MaxSize
                                2024-05-2416:19:43.122,Infinity,1,file.jfr,Infinity,0
                                """),
                new ExpectedView("active-settings",
                        """
                                EventType,Enabled,Threshold,StackTrace,Period,Cutoff,Throttle
                                ActiveRecording,true,,,,,
                                ActiveSetting,true,,,,,
                                AllocationRequiringGC,false,,true,,,
                                ...
                                """),
                new ExpectedView("allocation-by-class",
                """
                        ObjectType,AllocationPressure
                        byte[],18.32%
                        scala.collection.immutable.$colon$colon,7.98%
                        java.lang.Object[],6.99%
                        ...
                        """),
                new ExpectedView("allocation-by-thread",
                        """
                                Thread,AllocationPressure
                                main,99.07%
                                Thread-33,0.03%
                                Thread-35,0.03%
                                Thread-40,0.03%
                                ...
                                """),
                new ExpectedView("allocation-by-site",
                        """
                                Method,AllocationPressure
                                java.lang.invoke.DirectMethodHandleallocateInstance,6.75%
                                java.util.ArrayscopyOfRange,5.18%
                                dotty.tools.dotc.util.PerfectHashingallocate,3.89%
                                ...
                                """)
        );
    }

    private String normalizeCSV(String csv) {
        return csv.replaceAll(" ", "")
                .replaceAll("/.+/.+\\.jfr", "file.jfr")
                .replaceAll("\r\n", "\n").trim();
    }

    /**
     * Runs the "jfr view" command
     */
    private String executeJFR(String viewName) throws SQLException, IOException, InterruptedException {
        ViewCollection.View view = ViewCollection.getView(viewName);
        if (view == null) {
            throw new IllegalArgumentException("View not found: " + viewName);
        }
        Process process = Runtime.getRuntime().exec(new String[]{
                "jfr", "view", view.relatedJFRView(), jfrFile.toAbsolutePath().toString()
        });
        process.waitFor();
        if (process.exitValue() != 0) {
            throw new IOException("Error executing jfr view command, exit code: " + process.exitValue());
        }
        return new String(process.getInputStream().readAllBytes());
    }

    @ParameterizedTest
    @MethodSource("expectedViews")
    public void testViewOutput(ExpectedView expectedView) throws SQLException, IOException, InterruptedException {
        var out = new ByteArrayOutputStream();
        var printStream = new PrintStream(new BufferedOutputStream(out));
        var executor = new QueryExecutor(conn);
        executor.executeQuery("SELECT * FROM \"" + expectedView.view + "\"", QueryExecutor.OutputFormat.CSV, printStream);
        printStream.flush();
        String result = normalizeCSV(out.toString());
        String expected = normalizeCSV(expectedView.expectedCSV);
        System.out.println(expectedView.view + ":\nExpected:\n" + expected + "\nGot:\n" + result);
        System.out.println("---");
        System.out.println(executeJFR(expectedView.view));
        if (expected.endsWith("...")) {
            expected = expected.substring(0, expected.length() - 3).trim();
            result = result.length() > expected.length() ? result.substring(0, expected.length()) : result;
        }
        Assertions.assertEquals(expected, result, "View output does not match for view: " + expectedView.view);
    }
}