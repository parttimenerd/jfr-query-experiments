package me.bechberger.jfr.duckdb;

import jdk.jfr.consumer.RecordedEvent;
import org.duckdb.DuckDBConnection;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

public abstract class AbstractImporter {

    final ConnectionSupplier connectionSupplier;
    final Options options;

    @FunctionalInterface
    public interface ConnectionSupplier {
        DuckDBConnection get() throws SQLException;
    }

    public AbstractImporter(ConnectionSupplier connectionSupplier, Options options) {
        this.connectionSupplier = connectionSupplier;
        this.options = options;
    }

    public final void importRecording(Path path) throws IOException {
        importRecording(JFRUtil.streamEvents(path));
    }

    public final void importRecording(List<RecordedEvent> events) throws IOException {
        importRecording(events.stream());
    }

    abstract void importRecording(Stream<RecordedEvent> eventStream) throws IOException;

    public String normalizeTableName(String name) {
        return name.replaceAll("^(java|jfr|jdk)(\\.[a-z]+)*\\.", "jdk\\$");
    }
}