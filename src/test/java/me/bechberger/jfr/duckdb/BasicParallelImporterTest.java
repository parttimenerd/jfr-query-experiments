package me.bechberger.jfr.duckdb;

import static org.assertj.db.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import jdk.jfr.*;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;
import org.assertj.core.api.Condition;
import org.assertj.db.type.AssertDbConnection;
import org.assertj.db.type.AssertDbConnectionFactory;
import org.duckdb.DuckDBArray;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.*;

class BasicParallelImporterTest {

    @StackTrace(false)
    static class SimpleScalarFieldsEvent extends Event {
        public final int intField = 42;
        public final long longField = 123456789L;
        public final boolean booleanField = true;
        public final double doubleField = 3.14159;
        public final String stringField = "Hello, World!";
        @Unsigned public final long unsignedLongField = 9876543210L;
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
    public final List<RecordedEvent> createDatabase(
            Options options, Runnable runEvents, Class<? extends Event>... eventClasses)
            throws SQLException, IOException {
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
        DuckDBConnection conn =
                (DuckDBConnection)
                        DriverManager.getConnection("jdbc:duckdb:" + tmpDBFile.toAbsolutePath());
        List<DuckDBConnection> connections = new ArrayList<>();
        connections.add(conn);
        new BasicParallelImporter(
                        () -> {
                            var c = (DuckDBConnection) conn.duplicate();
                            connections.add(c);
                            return c;
                        },
                        options)
                .importRecording(recordedEvents);
        for (DuckDBConnection c : connections) {
            c.close();
        }
        return recordedEvents;
    }

    @SafeVarargs
    public final AssertDbConnection createDatabaseAndGetConnection(
            Options options, Runnable runEvents, Class<? extends Event>... eventClasses)
            throws SQLException, IOException {
        createDatabase(options, runEvents, eventClasses);
        return AssertDbConnectionFactory.of("jdbc:duckdb:" + tmpDBFile.toAbsolutePath(), "", "")
                .create();
    }

    @Test
    public void testEmptyRecording() throws SQLException, IOException {
        var conn = createDatabaseAndGetConnection(new Options(), () -> {});
        var table = conn.request("SELECT * FROM Events").build();
        assertThat(table).hasNumberOfRows(0);
    }

    @Test
    public void testSimpleScalarFieldsEvent() throws SQLException, IOException {
        var conn =
                createDatabaseAndGetConnection(
                        new Options(),
                        () -> {
                            SimpleScalarFieldsEvent event = new SimpleScalarFieldsEvent();
                            event.commit();
                        },
                        SimpleScalarFieldsEvent.class);
        var table =
                conn.request("SELECT * FROM \"" + SimpleScalarFieldsEvent.class.getName() + "\"")
                        .build();
        assertThat(table).hasNumberOfRows(1);
        var obj = new SimpleScalarFieldsEvent();
        assertThat(table)
                .row(0)
                .value("intField")
                .isEqualTo(obj.intField)
                .value("longField")
                .isEqualTo(obj.longField)
                .value("booleanField")
                .isEqualTo(obj.booleanField)
                .value("doubleField")
                .isEqualTo(obj.doubleField)
                .value("stringField")
                .isEqualTo(obj.stringField)
                .value("unsignedLongField")
                .isEqualTo(obj.unsignedLongField);
    }

    @Test
    public void testSimpleScalarFieldsEventHasEventThread() throws SQLException, IOException {
        var conn =
                createDatabaseAndGetConnection(
                        new Options(),
                        () -> {
                            SimpleScalarFieldsEvent event = new SimpleScalarFieldsEvent();
                            event.commit();
                        },
                        SimpleScalarFieldsEvent.class);
        var table =
                conn.request("SELECT * FROM \"" + SimpleScalarFieldsEvent.class.getName() + "\"")
                        .build();
        assertThat(table).row(0).value("eventThread").isEqualTo(1);
    }

    private void commitSimpleEventWithStackTraceEvent() {
        SimpleEventWithStackTrace event = new SimpleEventWithStackTrace();
        event.commit();
    }

    @Test
    public void testSimpleEventWithStackTrace() throws SQLException, IOException {
        var conn =
                createDatabaseAndGetConnection(
                        new Options(),
                        () -> {
                            var t = new Thread(this::commitSimpleEventWithStackTraceEvent);
                            t.setName("Thread");
                            t.start();
                            try {
                                t.join();
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        },
                        SimpleEventWithStackTrace.class);
        // Check event table
        var table =
                conn.request("SELECT * FROM \"" + SimpleEventWithStackTrace.class.getName() + "\"")
                        .build();
        assertThat(table).hasNumberOfRows(1);
        assertThat(table)
                .row(0)
                .value("name")
                .isEqualTo("Test Event")
                .value("value")
                .isEqualTo(100)
                .value("stackTrace$methods")
                .isNotNull()
                .value("stackTrace$truncated")
                .isEqualTo(false)
                .value("eventThread")
                .isEqualTo(1)
                .value("duration")
                .isEqualTo(0L);
        var arr =
                Arrays.stream(
                                (Object[])
                                        ((DuckDBArray)
                                                        (table.getRow(0)
                                                                .getColumnValue(
                                                                        "stackTrace$methods")
                                                                .getValue()))
                                                .getArray())
                        .mapToInt(o -> (int) o)
                        .toArray();
        System.out.println(Arrays.toString(arr));
        //        assertArrayEquals(new int[]{1, 2, 3, 4}, arr);
        // Check Thread table
        var threadTable = conn.request("SELECT * FROM Thread").build();
        assertThat(threadTable).hasNumberOfRows(2);
        assertThat(threadTable)
                .row(1)
                .value("_id")
                .isEqualTo(1)
                .value("javaName")
                .isEqualTo("Thread")
                .value("group")
                .isEqualTo(1)
                .value("virtual")
                .isFalse()
                .value("osName")
                .isEqualTo("Thread");
        // Check ThreadGroup table
        var threadGroupTable = conn.request("SELECT * FROM ThreadGroup").build();
        assertThat(threadGroupTable).hasNumberOfRows(2);
        assertThat(threadGroupTable)
                .row(1)
                .value("_id")
                .isEqualTo(1)
                .value("name")
                .isEqualTo("main");
        assertThat(threadGroupTable).hasNumberOfColumns(2);
        // method table
        var methodTable = conn.request("SELECT * FROM Method").build();
        assertThat(methodTable).hasNumberOfRows(5);
        assertThat(methodTable)
                .row(1)
                .value("_id")
                .isEqualTo(1)
                .value("name")
                .isEqualTo("commitSimpleEventWithStackTraceEvent")
                .value("descriptor")
                .isEqualTo("()")
                .value("modifiers")
                .isEqualTo(2)
                .value("hidden")
                .isFalse();

        var eventsTable = conn.request("SELECT * FROM Events").build();
        assertThat(eventsTable).hasNumberOfRows(1);
        assertThat(eventsTable)
                .row(0)
                .value("name")
                .isEqualTo(SimpleEventWithStackTrace.class.getName())
                .value("count")
                .isEqualTo(1);
    }

    @Test
    public void testStartTime() throws SQLException, IOException {
        var conn =
                createDatabaseAndGetConnection(
                        new Options(),
                        () -> {
                            SimpleScalarFieldsEvent event = new SimpleScalarFieldsEvent();
                            event.commit();
                        },
                        SimpleScalarFieldsEvent.class);
    }

    @Test
    public void testLimitStackTraceFromTop() throws SQLException, IOException {
        var events =
                createDatabase(
                        new Options(),
                        () -> {
                            var event = new SimpleEventWithStackTrace();
                            event.begin();
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            event.commit();
                        },
                        SimpleEventWithStackTrace.class);
        var conn =
                AssertDbConnectionFactory.of("jdbc:duckdb:" + tmpDBFile.toAbsolutePath(), "", "")
                        .create();
        var table =
                conn.request("SELECT * FROM \"" + SimpleEventWithStackTrace.class.getName() + "\"")
                        .build();
        assertThat(table).hasNumberOfRows(1);
        var startTime = events.getFirst().getStartTime();
        assertThat(table)
                .row(0)
                .value("startTime")
                .has(
                        new Condition<Timestamp>(startTime.toString()) {
                            @Override
                            public boolean matches(Timestamp s) {
                                return s.toInstant().getNano() / 1000 == startTime.getNano() / 1000;
                            }
                        })
                .value("duration")
                .has(
                        new Condition<Double>("duration between 10ms and 1s") {
                            @Override
                            public boolean matches(Double val) {
                                return val >= 0.01 && val <= 1;
                            }
                        });
    }
}
