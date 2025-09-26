package me.bechberger.jfr.duckdb.util;

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

    public static String decodeBytecodeClassName(String name) {
        if (name == null) {
            return null;
        }

        int depth = 0;
        int len = name.length();
        while (depth < len && name.charAt(depth) == '[') {
            depth++;
        }

        String rest = name.substring(depth);
        String base;

        if (rest.isEmpty()) {
            base = "";
        } else if (rest.charAt(0) == 'L' && rest.endsWith(";")) {
            // object descriptor like Ljava/lang/String;
            base = rest.substring(1, rest.length() - 1).replace('/', '.').replace('$', '.');
        } else {
            switch (rest) {
                case "B": base = "byte"; break;
                case "C": base = "char"; break;
                case "D": base = "double"; break;
                case "F": base = "float"; break;
                case "I": base = "int"; break;
                case "J": base = "long"; break;
                case "S": base = "short"; break;
                case "Z": base = "boolean"; break;
                case "V": base = "void"; break;
                default:
                    // Already slash-separated or dotted class name (e.g. java/lang/String or java.lang.String)
                    base = rest.replace('/', '.');
                    break;
            }
        }

        if (depth == 0) {
            return base;
        }

        StringBuilder sb = new StringBuilder(base);
        for (int i = 0; i < depth; i++) {
            sb.append("[]");
        }
        return sb.toString();
    }
}