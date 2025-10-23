package me.bechberger.jfr.duckdb.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import jdk.jfr.ValueDescriptor;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.jetbrains.annotations.Nullable;

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

    private static boolean isValidDescriptionPart(String entityName, String part) {
        if (part == null || part.isBlank()) {
            return false;
        }
        if (part.length() < 10) {
            return false;
        }
        String withoutJava = part.replace("Java ", "");
        String[] words = withoutJava.split(" +");
        // count the words in the table name
        int wordsInTableName = entityName.split("[A-Z]+").length - (Character.isUpperCase(entityName.charAt(0)) ? 1 : 0);
        return words.length > wordsInTableName;
    }

    /**
     * Combines label and description into a single description and returns null if both are null or
     * too simple.
     */
    public static @Nullable String combineDescription(String entityName, String label, String description) {
        if ((description == null || description.isBlank()) && (label == null || label.isBlank())) {
            return null;
        }
        List<String> parts = new ArrayList<>();
        if (label != null && !label.isBlank()) {
            parts.add(label);
        }
        if (description != null && !description.isBlank()) {
            parts.add(description);
        }
        String joined = parts.stream().filter(p -> isValidDescriptionPart(entityName, p))
                .collect(Collectors.joining(". ")).replace("..", ".");
        if (!joined.isBlank()) {
            return joined;
        }
        return null;
    }

    private static Method getTypeMethod;
    private static Method getLabelMethod;
    private static Method getDescriptionMethod;

    /**
     * Returns the combined description of the type of the given value descriptor.
     * <p>
     * The problem: {@link ValueDescriptor#getType()} is package private, so we cannot access it directly.
     * But hey, let's use reflection to get it anyway.
     * @param descriptor
     * @return
     */
    public static String getCombinedTypeDescription(ValueDescriptor descriptor) {
        if (getTypeMethod == null) {
            try {
                getTypeMethod = ValueDescriptor.class.getDeclaredMethod("getType");
                getTypeMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
        if (getLabelMethod == null) {
            try {
                getLabelMethod = ValueDescriptor.class.getDeclaredMethod("getLabel");
                getLabelMethod.setAccessible(true);
                getDescriptionMethod = ValueDescriptor.class.getDeclaredMethod("getDescription");
                getDescriptionMethod.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            Object type = getTypeMethod.invoke(descriptor);
            String label = (String) getLabelMethod.invoke(descriptor);
            String description = (String) getDescriptionMethod.invoke(descriptor);
            return combineDescription(descriptor.getTypeName(), label, description);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}