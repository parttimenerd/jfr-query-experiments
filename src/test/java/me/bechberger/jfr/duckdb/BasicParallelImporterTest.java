package me.bechberger.jfr.duckdb;

import jdk.jfr.*;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;
import org.assertj.core.api.Condition;
import org.assertj.db.api.Assertions;
import org.assertj.db.type.AssertDbConnection;
import org.assertj.db.type.AssertDbConnectionFactory;
import org.duckdb.DuckDBArray;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static org.assertj.db.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;


class BasicParallelImporterTest {

    @StackTrace(false)
    static class SimpleScalarFieldsEvent extends Event {
        public int intField = 42;
        public long longField = 123456789L;
        public boolean booleanField = true;
        public double doubleField = 3.14159;
        public String stringField = "Hello, World!";
        @Unsigned
        public long unsignedLongField = 9876543210L;
    }

    static class SimpleEventWithStackTrace extends Event {
        public String name = "Test Event";
        public int value = 100;
    }

    private static Path tmpDBFile = null;

    @BeforeEach
    public void setup() throws IOException {
        tmpDBFile = Files.createTempFile("duckdb_test", ".db");
        // make sure the file is empty
        Files.deleteIfExists(tmpDBFile);
    }

    @AfterEach
    public void teardown() throws IOException {
        if (tmpDBFile != null) {
            Files.deleteIfExists(tmpDBFile);
            tmpDBFile = null;
        }
    }

    @SafeVarargs
    public final List<RecordedEvent> createDatabase(Options options, Runnable runEvents, Class<? extends Event>... eventClasses) throws SQLException, IOException {
        List<RecordedEvent> recordedEvents = new ArrayList<>();
        try (RecordingStream recording = new RecordingStream()) {
            for (Class<? extends Event> eventClass : eventClasses) {
                recording.enable(eventClass);
                recording.onEvent(eventClass.getName(), recordedEvents::add);
            }
            recording.startAsync();
            runEvents.run();
            recording.stop();
        }
        DuckDBConnection conn = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + tmpDBFile.toAbsolutePath());
        List<DuckDBConnection> connections = new ArrayList<>();
        connections.add(conn);
        new BasicParallelImporter(() -> {
            var c = (DuckDBConnection) conn.duplicate();
            connections.add(c);
            return c;
        }, options).importRecording(recordedEvents);
        for (DuckDBConnection c : connections) {
            c.close();
        }
        return recordedEvents;
    }

    @SafeVarargs
    public final AssertDbConnection createDatabaseAndGetConnection(Options options, Runnable runEvents, Class<? extends Event>... eventClasses) throws SQLException, IOException {
        createDatabase(options, runEvents, eventClasses);
        return AssertDbConnectionFactory.of("jdbc:duckdb:" + tmpDBFile.toAbsolutePath(), "", "").create();
    }

    @Test
    public void testEmptyRecording() throws SQLException, IOException {
        var conn = createDatabaseAndGetConnection(new Options(), () -> {});
        assertThat(conn.request("SHOW TABLES;").build()).hasNumberOfRows(5);
        var table = conn.request("SELECT * FROM Events").build();
        assertThat(table).hasNumberOfRows(0);
    }

    @Test
    public void testSimpleScalarFieldsEvent() throws SQLException, IOException {
        var conn = createDatabaseAndGetConnection(new Options(), () -> {
            SimpleScalarFieldsEvent event = new SimpleScalarFieldsEvent();
            event.commit();
        }, SimpleScalarFieldsEvent.class);
        var table = conn.request("SELECT * FROM \"" + SimpleScalarFieldsEvent.class.getName() + "\"").build();
        assertThat(table).hasNumberOfRows(1);
        var obj = new SimpleScalarFieldsEvent();
        assertThat(table).row(0)
                .value("intField").isEqualTo(obj.intField)
                .value("longField").isEqualTo(obj.longField)
                .value("booleanField").isEqualTo(obj.booleanField)
                .value("doubleField").isEqualTo(obj.doubleField)
                .value("stringField").isEqualTo(obj.stringField)
                .value("unsignedLongField").isEqualTo(obj.unsignedLongField);
    }

    @Test
    public void testSimpleScalarFieldsEventHasEventThread() throws SQLException, IOException {
        var conn = createDatabaseAndGetConnection(new Options(), () -> {
            SimpleScalarFieldsEvent event = new SimpleScalarFieldsEvent();
            event.commit();
        }, SimpleScalarFieldsEvent.class);
        var table = conn.request("SELECT * FROM \"" + SimpleScalarFieldsEvent.class.getName() + "\"").build();
        assertThat(table).row(0)
                .value("eventThread").isEqualTo(1);
    }

    private void commitSimpleEventWithStackTraceEvent() {
        SimpleEventWithStackTrace event = new SimpleEventWithStackTrace();
        event.commit();
    }

    @Test
    public void testSimpleEventWithStackTrace() throws SQLException, IOException {
        var conn = createDatabaseAndGetConnection(new Options(), () -> {
            var t = new Thread(this::commitSimpleEventWithStackTraceEvent);
            t.setName("Thread");
            t.start();
            try {
                t.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, SimpleEventWithStackTrace.class);
        // Check event table
        var table = conn.request("SELECT * FROM \"" + SimpleEventWithStackTrace.class.getName() + "\"").build();
        assertThat(table).hasNumberOfRows(1);
        assertThat(table).row(0)
                .value("name").isEqualTo("Test Event")
                .value("value").isEqualTo(100)
                .value("stackTrace").isNotNull()
                .value("stackTrace$truncated").isEqualTo(false)
                .value("eventThread").isEqualTo(1)
                .value("duration").isEqualTo(0L);
        var arr = Arrays.stream((Object[])((DuckDBArray)(table.getRow(0).getColumnValue("stackTrace").getValue())).getArray()).mapToInt(o -> (int)o).toArray();
        System.out.println(Arrays.toString(arr));
//        assertArrayEquals(new int[]{1, 2, 3, 4}, arr);
        // Check Thread table
        var threadTable = conn.request("SELECT * FROM Thread").build();
        assertThat(threadTable).hasNumberOfRows(1);
        assertThat(threadTable).row(0)
                .value("_id").isEqualTo(1)
                .value("javaName").isEqualTo("Thread")
                .value("group").isEqualTo(1)
                .value("virtual").isFalse()
                .value("osName").isEqualTo("Thread");
        // Check ThreadGroup table
        var threadGroupTable = conn.request("SELECT * FROM ThreadGroup").build();
        assertThat(threadGroupTable).hasNumberOfRows(1);
        assertThat(threadGroupTable).row(0)
                .value("_id").isEqualTo(1)
                .value("name").isEqualTo("main");
        assertThat(threadGroupTable).hasNumberOfColumns(2);
        // StackFrame table
        var stackFrameTable = conn.request("SELECT * FROM StackFrame").build();
        assertThat(stackFrameTable).hasNumberOfRows(4);
        // method table
        var methodTable = conn.request("SELECT * FROM Method").build();
        assertThat(methodTable).hasNumberOfRows(4);
        assertThat(methodTable).row(0)
                .value("_id").isEqualTo(1)
                .value("name").isEqualTo("commitSimpleEventWithStackTraceEvent")
                .value("descriptor").isEqualTo("()V")
                .value("modifiers").isEqualTo(2)
                .value("hidden").isFalse();

        var eventsTable = conn.request("SELECT * FROM Events").build();
        assertThat(eventsTable).hasNumberOfRows(1);
        assertThat(eventsTable).row(0)
                .value("name").isEqualTo(SimpleEventWithStackTrace.class.getName())
                .value("count").isEqualTo(1);
    }

    @Test
    public void testStartTime() throws SQLException, IOException {
        var conn = createDatabaseAndGetConnection(new Options(), () -> {
            SimpleScalarFieldsEvent event = new SimpleScalarFieldsEvent();
            event.commit();
        }, SimpleScalarFieldsEvent.class);
    }

    @Test
    public void testLimitStackTraceFromTop() throws SQLException, IOException {
        var events = createDatabase(new Options(), () -> {
           var event = new SimpleEventWithStackTrace();
           event.begin();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            event.commit();
        }, SimpleEventWithStackTrace.class);
        var conn = AssertDbConnectionFactory.of("jdbc:duckdb:" + tmpDBFile.toAbsolutePath(), "", "").create();
        var table = conn.request("SELECT * FROM \"" + SimpleEventWithStackTrace.class.getName() + "\"").build();
        assertThat(table).hasNumberOfRows(1);
        var startTime = events.get(0).getStartTime();
        assertThat(table).row(0)
                .value("startTime").has(new Condition<Timestamp>(startTime.toString()) {
                    @Override
                    public boolean matches(Timestamp s) {
                        return s.toInstant().getNano() / 1000 == startTime.getNano() / 1000;
                    }
                })
                .value("duration").has(new Condition<Double>("duration between 10ms and 1s") {
                    @Override
                    public boolean matches(Double val) {
                        return val >= 0.01 && val <= 1;
                    }
                });
    }

    @Nested
    class MacroTest {

        private static Path tmpDBFile = null;
        private static AssertDbConnection conn;

        @BeforeEach
        public void setup() throws IOException, SQLException {
            tmpDBFile = Files.createTempFile("duckdb_test", ".db");
            // make sure the file is empty
            Files.deleteIfExists(tmpDBFile);
            // import the jfr_files/renaissance-dotty_default_G1.jfr file
            Path jfrFile = Path.of("jfr_files/renaissance-dotty_default_G1.jfr");
            if (!Files.exists(jfrFile)) {
                throw new IOException("Test file not found: " + jfrFile.toAbsolutePath());
            }
            BasicParallelImporter.createFile(jfrFile, tmpDBFile.toAbsolutePath(), new Options());
            conn = AssertDbConnectionFactory.of("jdbc:duckdb:" + tmpDBFile.toAbsolutePath(), "", "").create();
        }

        @AfterEach
        public void teardown() throws IOException {
            if (tmpDBFile != null) {
                Files.deleteIfExists(tmpDBFile);
                tmpDBFile = null;
            }
        }

        @Test
        public void testP90Macro() throws SQLException {
            var table = conn.request("SELECT p90(value) AS p90 FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) AS T(value)").build();
            assertThat(table).hasNumberOfRows(1);
            assertThat(table).row(0)
                    .value("p90").isEqualTo(9);
            System.out.println(table.getRow(0).toString());
        }

        @Test
        public void testNormalized() {
            var table = conn.request("""
                    SELECT value,
                          normalized(value) AS normalized 
                    FROM (VALUES (1), (2), (3), (4)) AS T(value)
                    ORDER BY value
                    """).build();
            assertThat(table).hasNumberOfRows(4);
            for (int i = 0; i < 4; i++) {
                assertThat(table).row(i)
                        .value("value").isEqualTo(i + 1)
                        .value("normalized").isEqualTo((i + 1) / 4.0);
            }
        }

        @Test
        public void testDiff() {
            var table = conn.request("""
                    SELECT value,
                           diff(value) AS diff
                    FROM (VALUES (1), (2), (4)) AS T(value)
                    ORDER BY value
                    """).build();
            assertThat(table).hasNumberOfRows(3);
            assertThat(table).row(0)
                    .value("value").isEqualTo(1)
                    .value("diff").isNull();
            assertThat(table).row(1)
                    .value("value").isEqualTo(2)
                    .value("diff").isEqualTo(1);
            assertThat(table).row(2)
                    .value("value").isEqualTo(4)
                    .value("diff").isEqualTo(2);
        }

        @Test
        public void testCountUnique() {
            var table = conn.request("""
                    SELECT value,
                           count_unique(value) AS count_unique
                    FROM (VALUES (1), (2), (2), (3), (3), (3)) AS T(value)
                    GROUP BY value
                    ORDER BY value
                    """).build();
            assertThat(table).hasNumberOfRows(3);
            assertThat(table).row(0)
                    .value("value").isEqualTo(1)
                    .value("count_unique").isEqualTo(1);
            assertThat(table).row(1)
                    .value("value").isEqualTo(2)
                    .value("count_unique").isEqualTo(1);
            assertThat(table).row(2)
                    .value("value").isEqualTo(3)
                    .value("count_unique").isEqualTo(1);
        }

        @Test
        public void testFormatMemory() {
            var table = conn.request("""
                    SELECT value,
                           format_memory(value) AS formatted,
                           format_memory(value, decimals := 0) AS formatted_0,
                    FROM (VALUES (1023), (1024), (1024 * 1.5), (1024 * 1024)) AS T(value)
                    ORDER BY value
                    """).build();
            assertThat(table).hasNumberOfRows(4);
            assertThat(table).row(0)
                    .value("formatted").isEqualTo("1023.00 B")
                    .value("formatted_0").isEqualTo("1023 B");
            assertThat(table).row(1)
                    .value("formatted").isEqualTo("1.00 KB");
            assertThat(table).row(2)
                    .value("formatted").isEqualTo("1.50 KB");
            assertThat(table).row(3)
                    .value("formatted").isEqualTo("1.00 MB");
        }
    }
}