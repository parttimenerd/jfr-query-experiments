package me.bechberger.jfr.duckdb;

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import jdk.jfr.EventType;
import jdk.jfr.FlightRecorder;
import jdk.jfr.ValueDescriptor;
import jdk.jfr.consumer.*;
import me.bechberger.jfr.duckdb.definitions.MacroCollection;
import me.bechberger.jfr.duckdb.definitions.ViewCollection;
import me.bechberger.jfr.duckdb.wrapper.AppenderWrapper;
import me.bechberger.jfr.duckdb.wrapper.DuckDBAppenderWrapper;
import me.bechberger.jfr.duckdb.wrapper.NoOpAppenderWrapper;
import org.duckdb.DuckDBConnection;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static me.bechberger.jfr.duckdb.util.JFRUtil.decodeBytecodeClassName;

/**
 * Imports a JFR recording into a DuckDB database and creates the database tables
 * <p/>
 * Creates one table per event type and one table per struct type (if needed).
 * Uses one connection per table type to use appenders in parallel.
 * <p/>
 * Does no advanced checking and reads the recording sequentially once. Doesn't store the events.
 * <p/>
 * <b>Limitations: Does not support non StackFrame arrays and breaks recursive objects</b>
 * <p/>
 * Durations are stored as long (microseconds)
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
        void appendTo(RecordedObject object, AppenderWrapper appender) throws SQLException;
    }

    @FunctionalInterface
    interface ObjectCacheFunction {
        ObjectCacheFunction IDENTITY_BASED = System::identityHashCode;

        long getCachedId(RecordedObject object);
    }

    /**
     * Represents a table in the database with its columns and an appender to insert data into it
     */
    static class Table {
        final String name;
        final List<Column> columns;
        final AppenderWrapper appender;
        private final AtomicInteger counter = new AtomicInteger(0);
        private final Long2IntMap longToIndex = new Long2IntOpenHashMap();
        private final @Nullable ObjectCacheFunction cacheFunction;

        Table(String name, List<Column> columns, ConnectionSupplier connectionSupplier, @Nullable ObjectCacheFunction cacheFunction) throws SQLException {
            this.name = name;
            this.columns = columns;
            var connection = connectionSupplier.get();
            this.cacheFunction = cacheFunction;
            createTable(connection);
            // list appenders are not yet supported in the DuckDBAppender
            // https://github.com/duckdb/duckdb-java/issues/344
            this.appender = hasVariableLengthArrayType() ? createNoAppenderWrapper(connection, name, columns, cacheFunction != null) :
                    new DuckDBAppenderWrapper(connection.createAppender(name), name);
        }

        static NoOpAppenderWrapper createNoAppenderWrapper(DuckDBConnection connection, String tableName, List<Column> columns, boolean caching) throws SQLException {
            String idPrefix = caching ? "_id, " : "";
            String sql = "INSERT INTO \"" + tableName + "\" (" + idPrefix + String.join(", ", columns.stream().map(c -> "\"" + c.name + "\"").toList()) + ") VALUES (" +
                    String.join(", ", Collections.nCopies(columns.size() + (caching ? 1 : 0), "?")) + ");";
            PreparedStatement ps = connection.prepareStatement(sql);
            return new NoOpAppenderWrapper(ps);
        }

        boolean hasVariableLengthArrayType() {
            return columns.stream().anyMatch(Column::hasVariableLengthArrayType);
        }

        /**
         * Defines a column in a table
         *
         * @param name   the name of the column (and the field in the RecordedObject)
         * @param type   the SQL type of the column
         * @param append a function that appends the value of the field to the DuckDBAppender
         */
        record Column(String name, String type, AppendFunction append) {
            @Override
            public String toString() {
                return "\"" + name + "\" " + type;
            }

            public Column prependName(String prefix) {
                return new Column(prefix + name, type, append);
            }

            boolean hasVariableLengthArrayType() {
                return type.endsWith("[]");
            }
        }

        @Override
        public String toString() {
            String idPrefix = doesUseCaching() ? "_id INTEGER PRIMARY KEY, " : "";
            return "CREATE TABLE IF NOT EXISTS \"" + name + "\" (" + idPrefix + String.join(", ", columns.stream().map(Column::toString).toList()) + ");";
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
            if (cacheFunction != null) {
                long cachedId = cacheFunction.getCachedId(object);
                if (longToIndex.containsKey(cachedId)) {
                    return longToIndex.get(cachedId);
                }
                int index = counter.incrementAndGet();
                longToIndex.put(cachedId, index);
            }
            try {
                insertIntoWithoutCaching(object);
            } catch (SQLException e) {
                throw new RuntimeSQLException("Failed to insert into table " + name + " at row " + counter.get(), e);
            }
            return counter.get();
        }

        public boolean doesUseCaching() {
            return cacheFunction != null;
        }

        public Table assumeCaching() {
            if (!doesUseCaching()) {
                throw new IllegalStateException("Table " + name + " does not use caching");
            }
            return this;
        }

        private void insertIntoWithoutCaching(RecordedObject object) throws SQLException {
            appender.begin();
            if (doesUseCaching()) {
                appender.append(counter.get());
            }
            for (Column column : columns) {
                column.append.appendTo(object, appender);
            }
            appender.end();
        }

        public void close() {
            try {
                appender.close();
            } catch (SQLException e) {
                throw new RuntimeSQLException("Failed to close appender for table " + name, e);
            }
        }
    }

    private final Map<EventType, Table> eventTypeToTable = new HashMap<>();
    private final Map<String, Table> structTypeToTable = new HashMap<>();
    private final Map<EventType, Integer> eventCount = new HashMap<>();

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
                switch (descriptor.getContentType()) {
                    case "jdk.jfr.Timestamp" -> {
                        col = new Table.Column(fieldName, "TIMESTAMP", (obj, app) -> {
                            var instant = getBaseObject.apply(obj).getInstant(fieldName);
                            if (instant.getEpochSecond() < 0) {
                                app.append(Instant.EPOCH);
                                return;
                            }
                            app.append(instant);
                        });
                    }
                    case "jdk.jfr.Timespan" -> {
                        col = new Table.Column(fieldName, "DOUBLE", (obj, app) -> {
                            var duration = getBaseObject.apply(obj).getDuration(fieldName);
                            if (duration.toHours() > 24 * 365 * 10) {
                                // avoid overflow for very large durations
                                app.append(Double.POSITIVE_INFINITY);
                                return;
                            }
                            app.append(duration.toMinutes() * 60.0 + duration.toSecondsPart() + duration.toMillisPart() / 1000.0 + (duration.toNanosPart() % 1_000_000) / 1_000_000_000.0);
                        });
                    }
                    case "jdk.jfr.Unsigned", "jdk.jfr.Frequency" -> {
                        col = defaultCol; // -1 is a valid value here
                    }
                    default -> {
                        col = defaultCol;
                    }
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
            case "jdk.types.Timestamp", "jdk.types.TickSpan", "jdk.types.Ticks" -> {
                col = new Table.Column(fieldName, "TIMESTAMP", (obj, app) -> {
                    var instant = getBaseObject.apply(obj).getInstant(fieldName);
                    app.append(instant != null ? instant.toString() : null);
                });
            }
            case "jdk.types.StackTrace" -> {
                return createStackTraceColumns(descriptor, getBaseObject);
            }
            case "java.lang.ThreadGroup" -> {
                if (options.isExcluded(Options.ExcludableItems.THREAD_GROUPS)) {
                    return List.of();
                }
                return createStructColumns(descriptor, getBaseObject);
            }
            case "jdk.types.Module" -> {
                if (options.isExcluded(Options.ExcludableItems.MODULES)) {
                    return List.of();
                }
                return createStructColumns(descriptor, getBaseObject);
            }
            case "jdk.types.ClassLoader" -> {
                if (options.isExcluded(Options.ExcludableItems.CLASS_LOADERS)) {
                    return List.of();
                }
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
        cols.addAll(List.of(new Table.Column(fieldName + "$topClass", "INTEGER", (obj, app) -> {
            RecordedStackTrace stackTrace = getBaseObject.apply(obj).getValue(fieldName);
            if (stackTrace == null || stackTrace.getFrames().isEmpty()) {
                app.appendNull();
            } else {
                RecordedFrame topFrame = stackTrace.getFrames().getFirst();
                RecordedMethod topMethod = topFrame.getMethod();
                if (topMethod == null) {
                    app.appendNull();
                    return;
                }
                app.append(getTableForMiscType(getField(topMethod, "type")).assumeCaching().insertInto(topMethod.getType()));
            }
        }), new Table.Column(fieldName + "$topMethod", "VARCHAR", (obj, app) -> {
            RecordedStackTrace stackTrace = getBaseObject.apply(obj).getValue(fieldName);
            if (stackTrace == null || stackTrace.getFrames().isEmpty()) {
                app.appendNull();
            } else {
                RecordedFrame topFrame = stackTrace.getFrames().getFirst();
                app.append(topFrame.getMethod() != null ? topFrame.getMethod().getName() : null);
            }
        })));
        if (options.isExcluded(Options.ExcludableItems.STACK_TRACES)) {
            return cols;
        }
        var frames = getTableForMiscType(getField(field, "frames")).assumeCaching();
        cols.addAll(List.of(new Table.Column(fieldName + "$length", "SHORT", (obj, app) -> {
            RecordedStackTrace stackTrace = getBaseObject.apply(obj).getValue(fieldName);
            app.append(stackTrace != null ? (short) stackTrace.getFrames().size() : 0);
        }), new Table.Column(fieldName + "$truncated", "BOOLEAN", (obj, app) -> {
            RecordedStackTrace stackTrace = getBaseObject.apply(obj).getValue(fieldName);
            app.append(stackTrace != null && (stackTrace.isTruncated() || stackTrace.getFrames().size() > options.limitStackTraceDepth));
        }), new Table.Column(fieldName, "INTEGER[" + options.limitStackTraceDepth + "]", (obj, app) -> {
            RecordedStackTrace stackTrace = getBaseObject.apply(obj).getValue(fieldName);
            if (stackTrace == null) {
                app.appendNull();
            } else {
                List<RecordedFrame> stackFrames = new ArrayList<>();
                if (options.limitStackTraceDepth < stackTrace.getFrames().size()) {
                    if (options.limitFromTop) {
                        stackFrames.addAll(stackTrace.getFrames().subList(0, options.limitStackTraceDepth));
                    } else {
                        stackFrames.addAll(stackTrace.getFrames().subList(stackTrace.getFrames().size() - options.limitStackTraceDepth, stackTrace.getFrames().size()));
                    }
                } else {
                    stackFrames = stackTrace.getFrames();
                }
                int[] arr = new int[options.limitStackTraceDepth];
                for (int i = 0; i < stackFrames.size(); i++) {
                    arr[i] = frames.insertInto(stackFrames.get(i));
                }
                for (int i = stackFrames.size(); i < options.limitStackTraceDepth; i++) {
                    arr[i] = 0;
                }
                app.append(arr);
            }
        })));
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
                System.out.println("Breaking recursion for type " + descriptor.getTypeName());
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
            }));
        }
        return descriptor.getFields().stream()
                .flatMap(f -> createColumnsForType(f, (o) -> getBaseObject.apply(o).getValue(descriptor.getName())).stream())
                .map(c -> c.prependName(descriptor.getName() + "$"))
                .toList();
    }

    private Table getTableForMiscType(ValueDescriptor descriptor) {
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
            return new Table(tableName, columns, connectionSupplier, cache ? ObjectCacheFunction.IDENTITY_BASED : null);
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
            ), connectionSupplier, null);
            for (Map.Entry<EventType, Integer> entry : eventCount.entrySet().stream().sorted(Comparator.comparing(e -> -e.getValue())).toList()) {
                table.appender.begin();
                table.appender.append(normalizeTableName(entry.getKey().getName()));
                table.appender.append(entry.getValue());
                table.appender.end();
            }
            Table eventIdTable = new Table("EventIDs", List.of(
                    new Table.Column("name", "VARCHAR", null),
                    new Table.Column("id", "LONG", null)
            ), connectionSupplier, null);
            Set<EventType> eventTypes = new HashSet<>(eventTypeToTable.keySet());
            eventTypes.addAll(FlightRecorder.getFlightRecorder().getEventTypes());
            for (EventType eventType : eventTypes) {
                eventIdTable.appender.begin();
                eventIdTable.appender.append(normalizeTableName(eventType.getName()));
                eventIdTable.appender.append(eventType.getId());
                eventIdTable.appender.end();
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
            ), connectionSupplier, null);
            for (EventType eventType : eventTypeToTable.keySet()) {
                table.appender.begin();
                table.appender.append(normalizeTableName(eventType.getName()));
                table.appender.append(eventType.getLabel());
                table.appender.end();
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
            MacroCollection.addToDatabase(connectionSupplier.get());
        } catch (SQLException e) {
            throw new RuntimeSQLException("Failed to add macros to database", e);
        }
        try {
            ViewCollection.addToDatabase(connectionSupplier.get());
        } catch (SQLException e) {
            throw new RuntimeSQLException("Failed to add views to database", e);
        }
    }

    public static void createFile(Path jfrFile, Path dbFile, Options options) throws IOException, SQLException {
        List<DuckDBConnection> conns = new ArrayList<>();
        new BasicParallelImporter(() -> {
            try {
                DuckDBConnection duckDBConnection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:" + dbFile);
                conns.add(duckDBConnection);
                return duckDBConnection;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, options).importRecording(jfrFile);
        for (DuckDBConnection duckDBConnection : conns) {
            duckDBConnection.close();
        }
    }

    public static void importIntoConnection(Path jfrFile, DuckDBConnection connection, Options options) throws IOException, SQLException {
        List<DuckDBConnection> conns = new ArrayList<>();
        new BasicParallelImporter(() -> {
            try {
                DuckDBConnection duckDBConnection = (DuckDBConnection) connection.duplicate();
                conns.add(duckDBConnection);
                return duckDBConnection;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, options).importRecording(jfrFile);
        for (DuckDBConnection duckDBConnection : conns) {
            duckDBConnection.close();
        }
    }
}