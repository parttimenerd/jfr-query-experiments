package me.bechberger.jfr.duckdb;

import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class JFRUtil {
    public static Stream<RecordedEvent> streamEvents(Path path) throws IOException {
        RecordingFile recordingFile = new RecordingFile(path);
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(new Iterator<RecordedEvent>() {
                    @Override
                    public boolean hasNext() {
                        return recordingFile.hasMoreEvents();
                    }

                    @Override
                    public RecordedEvent next() {
                        try {
                            return recordingFile.readEvent();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }, 0), false
        ).onClose(() -> {
            try {
                recordingFile.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
}