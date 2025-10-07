package me.bechberger.jfr.duckdb;

import jdk.jfr.EventType;
import jdk.jfr.FlightRecorder;
import jdk.jfr.Timespan;
import jdk.jfr.ValueDescriptor;
import jdk.jfr.consumer.*;
import jdk.management.jfr.RecordingInfo;
import me.bechberger.jfr.duckdb.definitions.MacroCollection;
import me.bechberger.jfr.duckdb.definitions.ViewCollection;
import me.bechberger.jfr.duckdb.query.QueryExecutor;
import me.bechberger.jfr.duckdb.util.SQLUtil;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static me.bechberger.jfr.duckdb.util.JFRUtil.decodeBytecodeClassName;
import static me.bechberger.jfr.duckdb.util.SQLUtil.append;

/**
 * Imports a JFR recording into a DuckDB database and creates the database tables
 */
public class BasicParallelImporter extends AbstractImporter {

    static final Function<RecordedObject, RecordedObject> IDENTITY_FUNCTION = (o) -> o;

    @FunctionalInterface
    interface AppendFunction {
        /**
         * Appends the value of the field to the DuckDBAppender
         *
         * @param object   the RecordedObject (event or struct)
         * @param appender the DuckDBAppender to append to
         * @throws SQLException if appending fails
         */
        void appendTo(RecordedObject object, DuckDBAppender appender) throws SQLException;
    }

    /**
     * Represents a table in the database with its columns and an appender to insert data into it
     */
    static class Table {
        final String name;
        final List<Column> columns;
        final DuckDBAppender appender;
        private final AtomicInteger counter = new AtomicInteger(0);

        /**
         * Wrapper for {@link RecordedObject} that implements {@link Object#hashCode()} and {@link Object#equals(Object)}
         * properly
         * <p>
         * Problem: There is no hashCode/equals implementation for RecordedObject that takes the actual content
         * into account. There the caching doesn't work properly.
         * <p>
         * Downside of this approach: Users need to pass <code>--add-opens jdk.jfr/jdk.jfr.consumer=ALL-UNNAMED</code>
         * to the JVM to allow access to the package private fields.
         * @param object
         */
        record Wrapper(RecordedObject object) {

            private static final Field objectContextField;
            private static final Field objectsField;

            static {
                try {
                    objectContextField = RecordedObject.class.getDeclaredField("objectContext");
                    objectContextField.setAccessible(true);
                    objectsField = RecordedObject.class.getDeclaredField("objects");
                    objectsField.setAccessible(true);
                } catch (NoSuchFieldException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public int hashCode() {
                try {
                    return objectContextField.get(object).hashCode() ^ objectsField.get(object).hashCode();
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean equals(Object obj) {
                try {
                    return this == obj || (obj instanceof Wrapper other && (
                            (objectContextField.get(object).equals(objectContextField.get(other.object)) &&
                            objectsField.get(object).equals(objectsField.get(other.object)))
                            || object.equals(other.object)
                    ));
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private final HashMap<Wrapper, Integer> objToIndex = new HashMap<>();
        private final boolean cache;

        Table(String name, List<Column> columns, ConnectionSupplier connectionSupplier, boolean cache) throws SQLException {
            this.name = name;
            this.columns = columns;
            var connection = connectionSupplier.get();
            this.cache = cache;
            createTable(connection);
            // Always use DuckDBAppenderWrapper
            this.appender = connection.createAppender(name);
        }

        /**
         * Defines a column in a table
         *
         * @param name   the name of the column (and the field in the RecordedObject)
         * @param type   the SQL type of the column
         * @param append a function that appends the value of the field to the DuckDBAppender
         * @param referencedTable optional SQL reference to other tables (for foreign keys)
         */
        record Column(String name, String type, AppendFunction append, @Nullable String referencedTable) {

            @Override
            public String toString() {
                return "\"" + name + "\" " + type;
            }

            public Column prependName(String prefix) {
                return new Column(prefix + name, type, append);
            }
            public Column(String name, String type, AppendFunction append) {
                this(name, type, append, null);
            }
        }

        @Override
        public String toString() {
            String idPrefix = doesUseCaching() ? "_id INTEGER PRIMARY KEY, " : "";
            return "CREATE TABLE IF NOT EXISTS \"" + name + "\" (" + idPrefix + String.join(", ", columns.stream().map(Column::toString).toList()) + ");";
        }

        public String getLLMDescription() {
            String idPrefix = doesUseCaching() ? "_id INTEGER PRIMARY KEY,\n  " : "";
            return "CREATE TABLE \"" + name + "\" (" + idPrefix +
                   String.join(",\n  ", columns.stream()
                           .map(c -> c.toString() +
                                     (c.referencedTable != null ? " -- references " + c.referencedTable + "(_id) if != 0" : "")).toList()) + "\n);";
        }

        private void createTable(DuckDBConnection conn) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(this.toString());
            } catch (Exception e) {
                throw new RuntimeSQLException("Failed to create table " + name, e);
            }
        }

        /**
         * Inserts the given object into the table, using caching if enabled.
         *
         * @return index of the inserted object (or the cached index if already present)
         */
        public int insertInto(RecordedObject object) {
            if (cache) {
                return objToIndex.computeIfAbsent(new Wrapper(object), (obj) -> {
                    int id = counter.incrementAndGet();
                    try {
                        insertIntoWithoutCaching(object);
                    } catch (SQLException e) {
                        throw new RuntimeSQLException("Failed to insert into table " + name + " at row " + id, e);
                    }
                    return id;
                });
            }
            try {
                insertIntoWithoutCaching(object);
            } catch (SQLException e) {
                throw new RuntimeSQLException("Failed to insert into table " + name + " at row " + counter.get(), e);
            }
            return counter.get();
        }

        public boolean doesUseCaching() {
            return cache;
        }

        public Table assumeCaching() {
            if (!doesUseCaching()) {
                throw new IllegalStateException("Table " + name + " does not use caching");
            }
            return this;
        }

        private void insertIntoWithoutCaching(RecordedObject object) throws SQLException {
            appender.beginRow();
            if (doesUseCaching()) {
                try {
                    appender.append(counter.get());
                } catch (SQLException e) {
                    throw new RuntimeSQLException("Failed to append ID for table " + name + " at row " + counter.get(), e);
                }
            }
            for (Column column : columns) {
                try {
                    column.append.appendTo(object, appender);
                } catch (SQLException e) {
                    throw new RuntimeSQLException("Failed to append column " + column.name + " for table " + name + " at row " + counter.get(), e);
                }
            }
            appender.endRow();
        }

        public void close() {
            try {
                appender.close();
            } catch (SQLException e) {
                throw new RuntimeSQLException("Failed to close appender for table " + name, e);
            }
        }
    }

    private static class RecordingInfo {
        private int eventCount;
        private Instant firstEvent;
        private Instant lastEvent;
        private String dumpReason;
        private Instant dumpTime;
        private long eventDurationNs;

        void processEvent(RecordedEvent event) {
            eventCount++;
            Instant startTime = event.getStartTime();
            if (firstEvent == null || startTime.isBefore(firstEvent)) {
                firstEvent = startTime;
            }
            if (lastEvent == null || startTime.isAfter(lastEvent)) {
                lastEvent = startTime;
            }
            eventDurationNs += event.getDuration().toNanos();
            if (event.getEventType().getName().equals("jdk.Shutdown") && (dumpTime == null || startTime.isAfter(dumpTime))) {
                dumpReason = event.getString("reason");
                dumpTime = event.getStartTime();
            }
        }

        void store(ConnectionSupplier connectionSupplier) throws SQLException {
            Table table = new Table("RecordingInfo", List.of(
                    new Table.Column("eventCount", "INTEGER", null),
                    new Table.Column("firstEvent", "TIMESTAMP", null),
                    new Table.Column("lastEvent", "TIMESTAMP", null),
                    new Table.Column("eventDurationSeconds", "DOUBLE", null),
                    new Table.Column("dumpReason", "VARCHAR", null)
            ), connectionSupplier, false);
            table.appender.beginRow();
            table.appender.append(eventCount);
            append(table.appender, firstEvent);
            append(table.appender, lastEvent);
            table.appender.append(eventDurationNs / 1_000_000_000.0);
            table.appender.append(dumpReason);
            table.appender.endRow();
            table.close();
        }
    }

    private final Map<EventType, Table> eventTypeToTable = new HashMap<>();
    private final Map<String, Table> structTypeToTable = new HashMap<>();
    private final Map<EventType, Integer> eventCount = new HashMap<>();
    private final RecordingInfo recordingInfo = new RecordingInfo();

    public BasicParallelImporter(ConnectionSupplier connectionSupplier, Options options) {
        super(connectionSupplier, options);
    }

    private Table getTableForEventType(EventType eventType) {
        return eventTypeToTable.computeIfAbsent(eventType, this::createTableForEventType);
    }

    private final Set<String> warned = new HashSet<>();

    private boolean isNumericField(ValueDescriptor descriptor) {
        return switch (descriptor.getTypeName()) {
            case "long", "byte", "short", "char", "int", "boolean", "double", "float" -> true;
            default -> false;
        };
    }

    private List<Table.Column> createColumnsForType(ValueDescriptor descriptor, @Nullable Function<RecordedObject, RecordedObject> getBaseObject) {
        List<Table.Column> columns;
        if (descriptor.isArray()) {
            throw new IllegalStateException("Array types not supported");
        } else {
            columns = createColumnsForTypeIgnoringArrays(descriptor, getBaseObject);
        }
        return columns;
    }

    private List<Table.Column> createColumnsForTypeIgnoringArrays(ValueDescriptor descriptor, Function<RecordedObject, RecordedObject> getBaseObject) {
        String fieldName = descriptor.getName();
        String typeName = descriptor.getTypeName();
        Table.Column col;
        switch (typeName) {
            case "java.lang.String" -> {
                col = new Table.Column(fieldName, "VARCHAR", (obj, app) -> app.append(getBaseObject.apply(obj).getString(fieldName)));
            }
            case "long" -> {
                var defaultCol = new Table.Column(fieldName, "BIGINT", (obj, app) -> app.append(getBaseObject.apply(obj).getLong(fieldName)));
                if (descriptor.getContentType() == null) {
                    col = defaultCol;
                    break;
                }
                var timespanAnnotation = descriptor.getAnnotation(Timespan.class);
                var timestampAnnotation = descriptor.getAnnotation(jdk.jfr.Timestamp.class);
                if (descriptor.getContentType().equals("jdk.jfr.Timestamp") || timestampAnnotation != null) {
                    col = new Table.Column(fieldName, "TIMESTAMP", (obj, app) -> {
                            var instant = getBaseObject.apply(obj).getInstant(fieldName);
                            if (instant.getEpochSecond() < 0) {
                                append(app, Instant.EPOCH);
                                return;
                            }
                            append(app, instant);
                        });
                    } else if (descriptor.getContentType().equals("jdk.jfr.Timespan") || timespanAnnotation != null) {
                        col = new Table.Column(fieldName, "DOUBLE", (obj, app) -> {
                            var duration = getBaseObject.apply(obj).getDuration(fieldName);
                            if (duration.toHours() > 24 * 365 * 10) {
                                // avoid overflow for very large durations
                                app.append(Double.POSITIVE_INFINITY);
                                return;
                            }
                            app.append(duration.toMinutes() * 60.0 + duration.toSecondsPart() + duration.toMillisPart() / 1000.0 + (duration.toNanosPart() % 1_000_000) / 1_000_000_000.0);
                        });
                    } else{
                    col = defaultCol;
                    }
            }
            case "byte" -> {
                col = new Table.Column(fieldName, "TINYINT", (obj, app) -> app.append(getBaseObject.apply(obj).getByte(fieldName)));
            }
            case "short" -> {
                col = new Table.Column(fieldName, "SMALLINT", (obj, app) -> app.append(getBaseObject.apply(obj).getShort(fieldName)));
            }
            case "char" -> {
                col = new Table.Column(fieldName, "SMALLINT", (obj, app) -> app.append(getBaseObject.apply(obj).getChar(fieldName)));
            }
            case "int" -> {
                col = new Table.Column(fieldName, "INTEGER", (obj, app) -> app.append(getBaseObject.apply(obj).getInt(fieldName)));
            }
            case "boolean" -> {
                col = new Table.Column(fieldName, "BOOLEAN", (obj, app) -> app.append(getBaseObject.apply(obj).getBoolean(fieldName)));
            }
            case "double" -> {
                col = new Table.Column(fieldName, "DOUBLE", (obj, app) -> app.append(getBaseObject.apply(obj).getDouble(fieldName)));
            }
            case "float" -> {
                col = new Table.Column(fieldName, "FLOAT", (obj, app) -> app.append(getBaseObject.apply(obj).getFloat(fieldName)));
            }
            case "jdk.types.Timestamp" -> {
                col = new Table.Column(fieldName, "TIMESTAMP", (obj, app) -> {
                    var instant = getBaseObject.apply(obj).getInstant(fieldName);
                    app.append(instant != null ? instant.toString() : null);
                });
            }
            case "jdk.types.StackTrace" -> {
                return createStackTraceColumns(descriptor, getBaseObject);
            }
            case "java.lang.ThreadGroup", "jdk.types.Module", "jdk.types.ClassLoader" -> {
                return createStructColumns(descriptor, getBaseObject);
            }
            case "jdk.types.Package" -> {
                if (options.isExcluded(Options.ExcludableItems.PACKAGE_HIERARCHY)) {
                    return List.of(new Table.Column(fieldName, "VARCHAR", (obj, app) -> {
                        RecordedObject pkg = getBaseObject.apply(obj).getValue(fieldName);
                        app.append(pkg != null ? pkg.getString("name") : null);
                    }));
                }
                return createStructColumns(descriptor, getBaseObject);
            }
            default -> {
                if (!descriptor.getFields().isEmpty()) {
                    return createStructColumns(descriptor, getBaseObject);
                }
                if (warned.add(typeName)) {
                    System.out.println("Unknown type " + typeName + " first seen in field " + descriptor);
                }
                return null;
            }
        }
        return List.of(col);
    }

    private ValueDescriptor getField(ValueDescriptor descriptor, String fieldName) {
        return descriptor.getFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Field " + fieldName + " not found in descriptor " + descriptor));
    }

    private ValueDescriptor getField(RecordedObject descriptor, String fieldName) {
        return descriptor.getFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Field " + fieldName + " not found in descriptor " + descriptor));
    }

    /**
     * Map a stack trace field to two columns:
     *
     * <dl>
     *     <dt><code>fieldname$truncated</code></dt>
     *     <dd>BOOLEAN</dd>
     *     <dt><code>fieldname</code></dt>
     *     <dd>INTEGER[]</dd>
     *     <dd>an array of integers representing the IDs of the stack frames in the
     * </dl>
     */
    private List<Table.Column> createStackTraceColumns(ValueDescriptor field, Function<RecordedObject, RecordedObject> getBaseObject) {
        String fieldName = field.getName();
        List<Table.Column> cols = new ArrayList<>();
        BiConsumer<Function<RecordedStackTrace, RecordedFrame>, String> addFrameColumn = (frameObtainer, name) -> {
            cols.add(new Table.Column(fieldName + "$" + name, "INTEGER", (obj, app) -> {
                RecordedStackTrace stackTrace = getBaseObject.apply(obj).getValue(fieldName);
                if (stackTrace == null || stackTrace.getFrames().isEmpty()) {
                    app.append(0);
                    return;
                }
                RecordedFrame frame = frameObtainer.apply(stackTrace);
                if (frame == null) {
                    app.append(0);
                } else {
                    int id = getTableForMiscType(getField(frame, "method")).assumeCaching().insertInto(frame.getMethod());
                    app.append(id);
                }
            }, "Method"));
        };
        addFrameColumn.accept((stackTrace) -> {
            return stackTrace.getFrames().getFirst();
        }, "topMethod");
        addFrameColumn.accept((stackTrace) -> {
            return stackTrace.getFrames().stream().filter(f -> {
                if (f.isJavaFrame()) {
                    RecordedClassLoader cl = f.getMethod().getType().getClassLoader();
                    return cl != null && !"bootstrap".equals(cl.getName());
                }
                return false;
            }).findFirst().orElse(null);
        }, "topApplicationMethod");
        addFrameColumn.accept((stackTrace) -> {
            return stackTrace.getFrames().stream().filter(f -> {
                if (f.isJavaFrame()) {
                    return !"<init>".equals(f.getMethod().getName());
                }
                return false;
            }).findFirst().orElse(null);
        }, "topNonInitMethod");
        cols.add(new Table.Column(fieldName + "$length", "SHORT", (obj, app) -> {
            RecordedStackTrace stackTrace = getBaseObject.apply(obj).getValue(fieldName);
            app.append(stackTrace != null ? (short) stackTrace.getFrames().size() : 0);
        }));
        cols.add(new Table.Column(fieldName + "$truncated", "BOOLEAN", (obj, app) -> {
            RecordedStackTrace stackTrace = getBaseObject.apply(obj).getValue(fieldName);
            app.append(stackTrace != null && (stackTrace.isTruncated() || stackTrace.getFrames().size() > options.getMaxStackTraceDepth()));
        }));
        cols.add(new Table.Column(fieldName + "$methods", "INTEGER[" + options.getMaxStackTraceDepth() + "]", (obj, app) -> {
            RecordedStackTrace stackTrace = getBaseObject.apply(obj).getValue(fieldName);
            int[] arr = new int[options.getMaxStackTraceDepth()];
            for (int i = 0; i < options.getMaxStackTraceDepth(); i++) {
                if (stackTrace == null || i >= stackTrace.getFrames().size()) {
                    arr[i] = 0;
                } else {
                    RecordedFrame frame = stackTrace.getFrames().get(i);
                    int id = getTableForMiscType(getField(frame, "method")).assumeCaching().insertInto(frame.getMethod());
                    arr[i] = id;
                }
            }
            app.append(arr);
        }, "Method"));
        return cols;
    }

    /**
     * Rules:
     * If the struct only consists of numeric values {@link #isNumericField(ValueDescriptor)} then inline struct into columns.
     * This is also true if the struct only has one field.
     * Else: Create a separate table for the struct and reference it by ID ({@link Table#assumeCaching()}.
     */
    private List<Table.Column> createStructColumns(ValueDescriptor descriptor, Function<RecordedObject, RecordedObject> getBaseObject) {
        boolean isObjectReference = (descriptor.getFields().size() > 1 &&
                !descriptor.getFields().stream().allMatch(this::isNumericField));
        if (isObjectReference) {
            Table table = getTableForMiscType(descriptor);
            if (table == null) {
                //System.out.println("Breaking recursion for type " + descriptor.getTypeName());
                return List.of();
            }
            // create a table for the struct type, that does use caching, like with stack frames
            return List.of(new Table.Column(descriptor.getName(), "INTEGER", (obj, app) -> {
                RecordedObject struct = getBaseObject.apply(obj).getValue(descriptor.getName());
                if (struct == null) {
                    app.appendNull();
                } else {
                    app.append(table.insertInto(struct));
                }
            }, table.name) );
        }
        return descriptor.getFields().stream()
                .flatMap(f -> createColumnsForType(f, (o) -> getBaseObject.apply(o).getValue(descriptor.getName())).stream())
                .map(c -> c.prependName(descriptor.getName() + "$"))
                .toList();
    }

    private Table getTableForMiscType(ValueDescriptor descriptor) {
        if (descriptor.getTypeName().contains("StackFrame")) {
            throw new IllegalArgumentException("StackFrame types should be handled separately");
        }
        if (!structTypeToTable.containsKey(descriptor.getTypeName())) {
            structTypeToTable.put(descriptor.getTypeName(), null);
            structTypeToTable.put(descriptor.getTypeName(), createTable(descriptor.getTypeName(), descriptor.getFields(), true));
        }
        return structTypeToTable.get(descriptor.getTypeName());
    }

    private List<Table.Column> additionalColumns(String name, List<ValueDescriptor> fields) {
        switch (name) {
            case "java.lang.Class" -> {
                // add a javaName column that contains the fully qualified name of the class
                // in human readable form (with dots)
                return List.of(new Table.Column("javaName", "VARCHAR", (obj, app) -> {
                        RecordedClass cls = (RecordedClass) obj;
                        app.append(decodeBytecodeClassName(cls.getName()));
                    }));
            }
            case "jdk.types.ClassLoader" -> {
                // I still want to know the class loader name
                // but don't want a class reference cycle
                return List.of(
                        new Table.Column("javaName", "VARCHAR", (obj, app) -> {
                            RecordedObject cl = getField(obj, "type").getTypeName().equals("java.lang.Class") ?
                                    obj.getValue("type") : null;
                            app.append(cl != null ? decodeBytecodeClassName(cl.getString("name")) : "null-bootstrap");
                        })
                );
            }
            default -> {
                return List.of();
            }
        }
    }

    private Table createTable(String name, List<ValueDescriptor> fields, boolean cache) {
        List<Table.Column> columns = Stream.concat(fields.stream()
                .flatMap(f -> createColumnsForType(f, IDENTITY_FUNCTION).stream()),
                additionalColumns(name, fields).stream())
                .toList();
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("Type " + name + " has no mappable fields, cannot create table");
        }
        String tableName = normalizeTableName(name);
        try {
            return new Table(tableName, columns, connectionSupplier, cache);
        } catch (SQLException e) {
            throw new RuntimeSQLException("Failed to create table for type " + name, e);
        }
    }

    private Table createTableForEventType(EventType eventType) {
        String tableName = eventType.getName().startsWith("jdk.") ? eventType.getName().substring(4) :
                eventType.getName();
        return createTable(tableName, eventType.getFields(), false);
    }

    private void writeEventCounts() {
        try {
            Table table = new Table("Events", List.of(
                    new Table.Column("name", "VARCHAR", null),
                    new Table.Column("count", "INTEGER", null)
            ), connectionSupplier, false);
            for (Map.Entry<EventType, Integer> entry : eventCount.entrySet().stream().sorted(Comparator.comparing(e -> -e.getValue())).toList()) {
                table.appender.beginRow();
                table.appender.append(normalizeTableName(entry.getKey().getName()));
                table.appender.append(entry.getValue());
                table.appender.endRow();
            }
            Table eventIdTable = new Table("EventIDs", List.of(
                    new Table.Column("name", "VARCHAR", null),
                    new Table.Column("id", "LONG", null)
            ), connectionSupplier, false);
            Set<EventType> eventTypes = new HashSet<>(eventTypeToTable.keySet());
            eventTypes.addAll(FlightRecorder.getFlightRecorder().getEventTypes());
            for (EventType eventType : eventTypes) {
                eventIdTable.appender.beginRow();
                eventIdTable.appender.append(normalizeTableName(eventType.getName()));
                eventIdTable.appender.append(eventType.getId());
                eventIdTable.appender.endRow();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeEventLabels() {
        try {
            Table table = new Table("EventLabels", List.of(
                    new Table.Column("name", "VARCHAR", null),
                    new Table.Column("label", "VARCHAR", null)
            ), connectionSupplier, false);
            for (EventType eventType : eventTypeToTable.keySet()) {
                table.appender.beginRow();
                table.appender.append(normalizeTableName(eventType.getName()));
                table.appender.append(eventType.getLabel());
                table.appender.endRow();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void importRecording(Stream<RecordedEvent> eventStream) throws IOException {
        eventStream.forEach(event -> {
            Table table = getTableForEventType(event.getEventType());
            table.insertInto(event);
            eventCount.merge(event.getEventType(), 1, Integer::sum);
            recordingInfo.processEvent(event);
        });
        writeEventCounts();
        writeEventLabels();
        for (Table table : eventTypeToTable.values()) {
            table.close();
        }
        for (Table table : structTypeToTable.values()) {
            table.close();
        }
        try {
            recordingInfo.store(connectionSupplier);
        } catch (SQLException e) {
            throw new RuntimeSQLException("Failed to store recording info", e);
        }
        try {
            addMacrosAndViews(connectionSupplier.get());
        } catch (SQLException e) {
            throw new RuntimeSQLException("Failed to get a connection", e);
        }
    }

    public record CreationResult(String llmDescription) {}

    public static CreationResult createFile(Path jfrFile, Path dbFile, Options options) throws IOException, SQLException {
        List<DuckDBConnection> conns = new ArrayList<>();
        var importer = new BasicParallelImporter(() -> {
            try {
                DuckDBConnection duckDBConnection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + dbFile);
                conns.add(duckDBConnection);
                return duckDBConnection;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, options);
        importer.importRecording(jfrFile);
        String llmDescription = importer.getLLMDescription(options.useExamplesForLLM());
        for (DuckDBConnection duckDBConnection : conns) {
            duckDBConnection.close();
        }
        return new CreationResult(llmDescription);
    }

    public static CreationResult importIntoConnection(Path jfrFile, DuckDBConnection connection, Options options) throws IOException, SQLException {
        List<DuckDBConnection> conns = new ArrayList<>();
        var importer = new BasicParallelImporter(() -> {
            try {
                DuckDBConnection duckDBConnection = (DuckDBConnection) connection.duplicate();
                conns.add(duckDBConnection);
                return duckDBConnection;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, options);
        importer.importRecording(jfrFile);
        String llmDescription = importer.getLLMDescription(options.useExamplesForLLM());
        for (DuckDBConnection duckDBConnection : conns) {
            duckDBConnection.close();
        }
        return new CreationResult(llmDescription);
    }

    public static void addMacrosAndViews(DuckDBConnection connection) {
        try {
            MacroCollection.addToDatabase(connection);
        } catch (SQLException e) {
            throw new RuntimeSQLException("Failed to add macros to database", e);
        }
        try {
            ViewCollection.addToDatabase(connection);
        } catch (SQLException e) {
            throw new RuntimeSQLException("Failed to add views to database", e);
        }
    }

    /**
     * Returns a description of the database schema in SQL format, suitable for LLM input.
     *
     * @param addExamples whether to add example queries and results for each table
     *                    (this should improve LLM performance, see <a href="https://arxiv.org/abs/2204.00498">Rajkumar et al</a>)
     */
    public String getLLMDescription(boolean addExamples) throws SQLException {
        StringBuilder sb = new StringBuilder();
        sb.append("There are the following tables in the database (described via the SQL definitions):\n\n");
        List<Table> tables = new ArrayList<>(eventTypeToTable.values());
        tables.addAll(structTypeToTable.values());
        var connection = connectionSupplier.get();
        for (Table table : tables) {
            sb.append(table.getLLMDescription()).append("\n");
            if (addExamples) {
                String query = "SELECT * FROM '" + table.name + "' LIMIT 1";
                sb.append("  Example query: ").append(query).append("\n");
                String result = new QueryExecutor(connection).executeQuery(query, QueryExecutor.OutputFormat.CSV, 10);
                sb.append("  Example result csv:\n").append(result.lines().map(l -> "    " + l).collect(Collectors.joining("\n"))).append("\n");
            }
        }
        sb.append("\nAdditionally, there are the following views and macros:\n\n");
        DuckDBConnection conn = connectionSupplier.get();
        sb.append(MacroCollection.getLLMDescription(conn)).append("\n\n");
        sb.append(ViewCollection.getLLMDescription(conn, addExamples));
        return sb.toString();
    }
}