package me.bechberger.jfr.duckdb;

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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Idea: Compare the view output with expected output, uses CSV output and ignores spaces
 */
public class ViewsTest {

    enum JFRFile {
        DEFAULT("default.jfr"),
        CONTAINER("container.jfr"),
        METAL("metal.jfr");
        private final Path path;
        JFRFile(String fileName) {
            this.path = Path.of("jfr_files").resolve(fileName);
        }

        public Path getPath() {
            return path;
        }
    }

    private static class JFRFileHelper {
        private final JFRFile jfrFile;
        private final JFRViewResultCache jfrViewResultCache;
        private Path tmpDBFile = null;

        public JFRFileHelper(JFRFile jfrFile) throws IOException, SQLException {
            this.jfrFile = jfrFile;
            this.tmpDBFile = Files.createTempFile("duckdb_test", ".db");
            Files.deleteIfExists(tmpDBFile);
            if (!Files.exists(jfrFile.getPath())) {
                throw new IOException("Test file not found: " + jfrFile.getPath().toAbsolutePath());
            }
            BasicParallelImporter.createFile(jfrFile.getPath(), tmpDBFile.toAbsolutePath(), new Options());
            this.jfrViewResultCache = new JFRViewResultCache(jfrFile.getPath());
        }

        public String executeJFR(String view) throws IOException, InterruptedException {
            return jfrViewResultCache.execute(view);
        }

        public void cleanup() throws IOException {
            if (tmpDBFile != null) {
                Files.deleteIfExists(tmpDBFile);
            }
        }

        public DuckDBConnection openConnection() throws SQLException {
            return (DuckDBConnection) java.sql.DriverManager.getConnection("jdbc:duckdb:" + tmpDBFile.toAbsolutePath());
        }
    }

    private static final Map<JFRFile, JFRFileHelper> jfrFileHelpers = new HashMap<>();

    private JFRFileHelper getJFRFileHelper(JFRFile jfrFile) throws IOException, SQLException {
        return jfrFileHelpers.computeIfAbsent(jfrFile, jf -> {
            try {
                return new JFRFileHelper(jf);
            } catch (IOException | SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @AfterAll
    public static void cleanup() throws IOException {
        for (JFRFileHelper helper : jfrFileHelpers.values()) {
            helper.cleanup();
        }
    }

    public record ExpectedView(String view, String expectedCSV, JFRFile jfrFile) {
        public ExpectedView(String view, String expectedCSV) {
            this(view, expectedCSV, JFRFile.DEFAULT);
        }
    }

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
                                """),
                new ExpectedView("class-loaders",
                        """
                                ClassLoader,HiddenClasses,Classes
                                java.net.URLClassLoader,0,5338
                                ,334,2755
                                java.net.URLClassLoader,0,572
                                jdk.internal.loader.ClassLoaders$AppClassLoader,0,84
                                jdk.internal.loader.ClassLoaders$PlatformClassLoader,0,7
                                """),
                new ExpectedView("compiler-configuration",
                        """
                                CompilerThreads,DynamicCompilerThreads,TieredCompilation
                                4,true,true
                                """),
                new ExpectedView("compiler-statistics",
                        """
                                CompiledMethods,PeakTime,TotalTime,Bailouts,OSRCompilations,StandardCompilations,OSRBytesCompiled,StandardBytesCompiled,CompilationResultingSize,CompilationResultingCodeSize
                                30730,591ms,1m10s,5,113,30617,0.00B,0.00B,135.72MB,67.83MB
                                """),
                new ExpectedView("contention-by-thread",
                        """
                                Thread,TimeBlocked,TimeWaited,CountBlocked,CountWaited
                                main,1.24s,1.24s,11,11
                                Reference Handler,0ns,0ns,0,0
                                Finalizer,0ns,0ns,0,0
                                Signal Dispatcher,0ns,0ns,0,0
                                ...
                                """, JFRFile.METAL)
        );
    }

    private String normalizeCSV(String csv) {
        return csv.replaceAll(" ", "")
                .replaceAll("/.+/.+\\.jfr", "file.jfr")
                .replace("\r\n", "\n").trim();
    }

    @ParameterizedTest
    @MethodSource("expectedViews")
    public void testViewOutput(ExpectedView expectedView) throws SQLException, IOException, InterruptedException {
        var out = new ByteArrayOutputStream();
        var printStream = new PrintStream(new BufferedOutputStream(out));
        var executor = new QueryExecutor(getJFRFileHelper(expectedView.jfrFile).openConnection());
        executor.executeQuery("SELECT * FROM \"" + expectedView.view + "\"", QueryExecutor.OutputFormat.CSV, printStream);
        printStream.flush();
        String result = normalizeCSV(out.toString());
        String expected = normalizeCSV(expectedView.expectedCSV);
        System.out.println(expectedView.view + ":\nExpected:\n" + expected + "\nGot:\n" + result);
        System.out.println("---");
        System.out.println(getJFRFileHelper(expectedView.jfrFile).executeJFR(expectedView.view));
        if (expected.endsWith("...")) {
            expected = expected.substring(0, expected.length() - 3).trim();
            result = result.length() > expected.length() ? result.substring(0, expected.length()) : result;
        }
        Assertions.assertEquals(expected, result, "View output does not match for view: " + expectedView.view);
    }
}