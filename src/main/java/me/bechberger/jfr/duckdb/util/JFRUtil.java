package me.bechberger.jfr.duckdb.util;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;

public class JFRUtil {

    public static Stream<RecordedEvent> streamEvents(Path path) throws IOException {
        RecordingFile recordingFile = new RecordingFile(path);
        return StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(
                                new Iterator<RecordedEvent>() {
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
                                },
                                0),
                        false)
                .onClose(
                        () -> {
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
            base =
                    switch (rest) {
                        case "B" -> "byte";
                        case "C" -> "char";
                        case "D" -> "double";
                        case "F" -> "float";
                        case "I" -> "int";
                        case "J" -> "long";
                        case "S" -> "short";
                        case "Z" -> "boolean";
                        case "V" -> "void";
                        default ->
                                // Already slash-separated or dotted class name (e.g.
                                // java/lang/String or
                                // java.lang.String)
                                rest.replace('/', '.');
                    };
        }

        if (depth == 0) {
            return base;
        }

        StringBuilder sb = new StringBuilder(base);
        sb.append("[]".repeat(depth));
        return sb.toString();
    }

    /**
     * Simplifies a bytecode method descriptor by removing package names and the return type and
     * formatting it into Java style. E.g. (Ljava/lang/String;I)V becomes (String, int) If the
     * descriptor is invalid, returns the original descriptor.
     *
     * @param descriptor the bytecode method descriptor
     * @return the simplified descriptor
     */
    public static String simplifyDescriptor(String descriptor) {
        if (descriptor == null) {
            return null;
        }
        if (!descriptor.startsWith("(")) {
            return descriptor; // not a valid descriptor
        }
        int endParams = descriptor.indexOf(')');
        if (endParams == -1) {
            return descriptor; // not a valid descriptor
        }
        String params = descriptor.substring(1, endParams);
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        int i = 0;
        boolean first = true;
        while (i < params.length()) {
            if (!first) {
                sb.append(", ");
            } else {
                first = false;
            }
            int arrayDepth = 0;
            while (i < params.length() && params.charAt(i) == '[') {
                arrayDepth++;
                i++;
            }
            if (i >= params.length()) {
                return descriptor; // not a valid descriptor
            }
            char c = params.charAt(i);
            String type;
            if (c == 'L') {
                int semicolonIndex = params.indexOf(';', i);
                if (semicolonIndex == -1) {
                    return descriptor; // not a valid descriptor
                }
                String className = params.substring(i + 1, semicolonIndex);
                if (className.isEmpty()) {
                    // Invalid class descriptor, return original
                    return descriptor;
                }
                int lastSlash = className.lastIndexOf('/');
                if (lastSlash != -1) {
                    className = className.substring(lastSlash + 1);
                }
                type = className.replace('$', '.');
                i = semicolonIndex + 1;
            } else {
                type = decodeBytecodeClassName(String.valueOf(c));
                i++;
            }
            sb.append(type);
            sb.append("[]".repeat(Math.max(0, arrayDepth)));
        }
        sb.append(')');
        return sb.toString();
    }
}
