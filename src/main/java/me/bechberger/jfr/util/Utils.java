/*
 * Copyright (c) 2016, 2023, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package me.bechberger.jfr.util;

import jdk.internal.module.Checks;
import jdk.jfr.*;
import jdk.jfr.internal.JVM;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.annotation.Annotation;
import java.lang.annotation.Repeatable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.*;

public final class Utils {

    private static final Object flushObject = new Object();
    private static final String INFINITY = "infinity";
    private static final String OFF = "off";
    public static final String EVENTS_PACKAGE_NAME = "jdk.jfr.events";
    public static final String INSTRUMENT_PACKAGE_NAME = "jdk.jfr.internal.instrument";
    public static final String EVENT_PACKAGE_NAME = "jdk.jfr.internal.event";
    public static final String REGISTER_EVENT = "registerEvent";
    public static final String ACCESS_FLIGHT_RECORDER = "accessFlightRecorder";
    private static final String LEGACY_EVENT_NAME_PREFIX = "com.oracle.jdk.";

    private static final Duration MICRO_SECOND = Duration.ofNanos(1_000);
    private static final Duration SECOND = Duration.ofSeconds(1);
    private static final Duration MINUTE = Duration.ofMinutes(1);
    private static final Duration HOUR = Duration.ofHours(1);
    private static final Duration DAY = Duration.ofDays(1);
    private static final int NANO_SIGNIFICANT_FIGURES = 9;
    private static final int MILL_SIGNIFICANT_FIGURES = 3;
    private static final int DISPLAY_NANO_DIGIT = 3;
    private static final int BASE = 10;
    private static long THROTTLE_OFF = -2;

    /*
     * This field will be lazily initialized and the access is not synchronized.
     * The possible data race is benign and is worth of not introducing any contention here.
     */
    private static Instant lastTimestamp;

    private static enum TimespanUnit {
        NANOSECONDS("ns", 1000), MICROSECONDS("us", 1000), MILLISECONDS("ms", 1000), SECONDS("s", 60), MINUTES("m", 60), HOURS("h", 24), DAYS("d", 7);

        final String text;
        final long amount;

        TimespanUnit(String unit, long amount) {
            this.text = unit;
            this.amount = amount;
        }
    }

    private static String formatDataAmount(String formatter, long amount) {
        if (amount == Long.MIN_VALUE) {
            return "N/A";
        }
        int exp = (int) (Math.log(Math.abs(amount)) / Math.log(1024));
        char unit = "kMGTPE".charAt(exp - 1);
        return String.format(formatter, amount / Math.pow(1024, exp), unit);
    }

    public static String formatBytesCompact(long bytes) {
        if (bytes < 1024) {
            return String.valueOf(bytes);
        }
        return formatDataAmount("%.1f%cB", bytes);
    }

    public static String formatBits(long bits) {
        if (bits == 1 || bits == -1) {
            return bits + " bit";
        }
        if (bits < 1024 && bits > -1024) {
            return bits + " bits";
        }
        return formatDataAmount("%.1f %cbit", bits);
    }

    public static String formatBytes(long bytes) {
        if (bytes == 1 || bytes == -1) {
            return bytes + " byte";
        }
        if (bytes < 1024 && bytes > -1024) {
            return bytes + " bytes";
        }
        return formatDataAmount("%.1f %cB", bytes);
    }

    public static String formatBytesPerSecond(long bytes) {
        if (bytes < 1024 && bytes > -1024) {
            return bytes + " byte/s";
        }
        return formatDataAmount("%.1f %cB/s", bytes);
    }

    public static String formatBitsPerSecond(long bits) {
        if (bits < 1024 && bits > -1024) {
            return bits + " bps";
        }
        return formatDataAmount("%.1f %cbps", bits);
    }
    public static String formatTimespan(Duration dValue, String separation) {
        if (dValue == null) {
            return "0";
        }
        long value = dValue.toNanos();
        TimespanUnit result = TimespanUnit.NANOSECONDS;
        for (TimespanUnit unit : TimespanUnit.values()) {
            result = unit;
            long amount = unit.amount;
            if (result == TimespanUnit.DAYS || value < amount || value % amount != 0) {
                break;
            }
            value /= amount;
        }
        return String.format("%d%s%s", value, separation, result.text);
    }

    // This method reduces the number of loaded classes
    // compared to DateTimeFormatter
    public static String formatDateTime(LocalDateTime time) {
        StringBuilder sb = new StringBuilder(19);
        sb.append(time.getYear() / 100);
        appendPadded(sb, time.getYear() % 100, true);
        appendPadded(sb, time.getMonth().getValue(), true);
        appendPadded(sb, time.getDayOfMonth(), true);
        appendPadded(sb, time.getHour(), true);
        appendPadded(sb, time.getMinute(), true);
        appendPadded(sb, time.getSecond(), false);
        return sb.toString();
    }

    private static void appendPadded(StringBuilder text, int number, boolean separator) {
        if (number < 10) {
            text.append('0');
        }
        text.append(number);
        if (separator) {
            text.append('_');
        }
    }

    enum ThrottleUnit {
        NANOSECONDS("ns", TimeUnit.SECONDS.toNanos(1), TimeUnit.SECONDS.toMillis(1)),
        MICROSECONDS("us", TimeUnit.SECONDS.toNanos(1) / 1000, TimeUnit.SECONDS.toMillis(1)),
        MILLISECONDS("ms", TimeUnit.SECONDS.toMillis(1), TimeUnit.SECONDS.toMillis(1)),
        SECONDS("s", 1, TimeUnit.SECONDS.toMillis(1)),
        MINUTES("m", 1, TimeUnit.MINUTES.toMillis(1)),
        HOUR("h", 1, TimeUnit.HOURS.toMillis(1)),
        DAY("d", 1, TimeUnit.DAYS.toMillis(1));

        private final String text;
        private final long factor;
        private final long millis;

        ThrottleUnit(String t, long factor, long millis) {
            this.text = t;
            this.factor = factor;
            this.millis = millis;
        }

        private static ThrottleUnit parse(String s) {
            if (s.equals(OFF)) {
                return MILLISECONDS;
            }
            return unit(parseThrottleString(s, false));
        }

        private static ThrottleUnit unit(String s) {
            if (s.endsWith("ns") || s.endsWith("us") || s.endsWith("ms")) {
                return value(s.substring(s.length() - 2));
            }
            if (s.endsWith("s") || s.endsWith("m") || s.endsWith("h") || s.endsWith("d")) {
                return value(s.substring(s.length() - 1));
            }
            throw new NumberFormatException("'" + s + "' is not a valid time unit.");
        }

        private static ThrottleUnit value(String s) {
            for (ThrottleUnit t : values()) {
                if (t.text.equals(s)) {
                    return t;
                }
            }
            throw new NumberFormatException("'" + s + "' is not a valid time unit.");
        }

        static long asMillis(String s) {
            return parse(s).millis;
        }

        static long normalizeValueAsMillis(long value, String s) {
            return value * parse(s).factor;
        }
    }

    private static void throwThrottleNumberFormatException(String s) {
        throw new NumberFormatException("'" + s + "' is not valid. Should be a non-negative numeric value followed by a delimiter. i.e. '/', and then followed by a unit e.g. 100/s.");
    }

    // Expected input format is "x/y" where x is a non-negative long
    // and y is a time unit. Split the string at the delimiter.
    private static String parseThrottleString(String s, boolean value) {
        String[] split = s.split("/");
        if (split.length != 2) {
            throwThrottleNumberFormatException(s);
        }
        return value ? split[0].trim() : split[1].trim();
    }

    public static long parseThrottleValue(String s) {
        if (s.equals(OFF)) {
            return THROTTLE_OFF;
        }
        String parsedValue = parseThrottleString(s, true);
        long normalizedValue = 0;
        try {
            normalizedValue = ThrottleUnit.normalizeValueAsMillis(Long.parseLong(parsedValue), s);
        } catch (NumberFormatException nfe) {
            throwThrottleNumberFormatException(s);
        }
        return normalizedValue;
    }

    public static long parseThrottleTimeUnit(String s) {
        return ThrottleUnit.asMillis(s);
    }

    public static long parseTimespanWithInfinity(String s) {
        if (INFINITY.equals(s)) {
            return Long.MAX_VALUE;
        }
        return parseTimespan(s);
    }

    public static long parseTimespan(String s) {
        if (s.endsWith("ns")) {
            return Long.parseLong(s.substring(0, s.length() - 2).trim());
        }
        if (s.endsWith("us")) {
            return MICROSECONDS.toNanos(Long.parseLong(s.substring(0, s.length() - 2).trim()));
        }
        if (s.endsWith("ms")) {
            return MILLISECONDS.toNanos(Long.parseLong(s.substring(0, s.length() - 2).trim()));
        }
        if (s.endsWith("s")) {
            return SECONDS.toNanos(Long.parseLong(s.substring(0, s.length() - 1).trim()));
        }
        if (s.endsWith("m")) {
            return MINUTES.toNanos(Long.parseLong(s.substring(0, s.length() - 1).trim()));
        }
        if (s.endsWith("h")) {
            return HOURS.toNanos(Long.parseLong(s.substring(0, s.length() - 1).trim()));
        }
        if (s.endsWith("d")) {
            return DAYS.toNanos(Long.parseLong(s.substring(0, s.length() - 1).trim()));
        }

        try {
            Long.parseLong(s);
        } catch (NumberFormatException nfe) {
            throw new NumberFormatException("'" + s + "' is not a valid timespan. Should be numeric value followed by a unit, i.e. 20 ms. Valid units are ns, us, s, m, h and d.");
        }
        // Only accept values with units
        throw new NumberFormatException("Timespan + '" + s + "' is missing unit. Valid units are ns, us, s, m, h and d.");
    }

    /**
     * Return all annotations as they are visible in the source code
     *
     * @param clazz class to return annotations from
     *
     * @return list of annotation
     *
     */
    static List<Annotation> getAnnotations(Class<?> clazz) {
        List<Annotation> annos = new ArrayList<>();
        for (Annotation a : clazz.getAnnotations()) {
            annos.addAll(getAnnotation(a));
        }
        return annos;
    }

    private static List<? extends Annotation> getAnnotation(Annotation a) {
        Class<?> annotated = a.annotationType();
        Method valueMethod = getValueMethod(annotated);
        if (valueMethod != null) {
            Class<?> returnType = valueMethod.getReturnType();
            if (returnType.isArray()) {
                Class<?> candidate = returnType.getComponentType();
                Repeatable r = candidate.getAnnotation(Repeatable.class);
                if (r != null) {
                    Class<?> repeatClass = r.value();
                    if (annotated == repeatClass) {
                        return getAnnotationValues(a, valueMethod);
                    }
                }
            }
        }
        return List.of(a);
    }

    static boolean isAfter(RecordingState stateToTest, RecordingState b) {
        return stateToTest.ordinal() > b.ordinal();
    }

    static boolean isBefore(RecordingState stateToTest, RecordingState b) {
        return stateToTest.ordinal() < b.ordinal();
    }

    static boolean isState(RecordingState stateToTest, RecordingState... states) {
        for (RecordingState s : states) {
            if (s == stateToTest) {
                return true;
            }
        }
        return false;
    }

    private static List<Annotation> getAnnotationValues(Annotation a, Method valueMethod) {
        try {
            return Arrays.asList((Annotation[]) valueMethod.invoke(a, new Object[0]));
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            return new ArrayList<>();
        }
    }

    private static Method getValueMethod(Class<?> annotated) {
        try {
            return annotated.getMethod("value", new Class<?>[0]);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    public static void touch(Path dumpFile) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(dumpFile.toFile(), "rw");
        raf.close();
    }

    public static Class<?> unboxType(Class<?> t) {
        if (t == Integer.class) {
            return int.class;
        }
        if (t == Long.class) {
            return long.class;
        }
        if (t == Float.class) {
            return float.class;
        }
        if (t == Double.class) {
            return double.class;
        }
        if (t == Byte.class) {
            return byte.class;
        }
        if (t == Short.class) {
            return short.class;
        }
        if (t == Boolean.class) {
            return boolean.class;
        }
        if (t == Character.class) {
            return char.class;
        }
        return t;
    }

    public static Map<String, String> sanitizeNullFreeStringMap(Map<String, String> settings) {
        HashMap<String, String> map = HashMap.newHashMap(settings.size());
        for (Map.Entry<String, String> e : settings.entrySet()) {
            String key = e.getKey();
            if (key == null) {
                throw new NullPointerException("Null key is not allowed in map");
            }
            String value = e.getValue();
            if (value == null) {
                throw new NullPointerException("Null value is not allowed in map");
            }
            map.put(key, value);
        }
        return map;
    }

    public static <T> List<T> sanitizeNullFreeList(List<T> elements, Class<T> clazz) {
        List<T> sanitized = new ArrayList<>(elements.size());
        for (T element : elements) {
            if (element == null) {
                throw new NullPointerException("Null is not an allowed element in list");
            }
            if (element.getClass() != clazz) {
                throw new ClassCastException();
            }
            sanitized.add(element);
        }
        return sanitized;
    }

    static List<Field> getVisibleEventFields(Class<?> clazz) {
        Utils.ensureValidEventSubclass(clazz);
        List<Field> fields = new ArrayList<>();
        for (Class<?> c = clazz; c != jdk.internal.event.Event.class; c = c.getSuperclass()) {
            for (Field field : c.getDeclaredFields()) {
                // skip private field in base classes
                if (c == clazz || !Modifier.isPrivate(field.getModifiers())) {
                    fields.add(field);
                }
            }
        }
        return fields;
    }

    public static void ensureValidEventSubclass(Class<?> eventClass) {
        if (jdk.internal.event.Event.class.isAssignableFrom(eventClass) && Modifier.isAbstract(eventClass.getModifiers())) {
            throw new IllegalArgumentException("Abstract event classes are not allowed");
        }
        if (eventClass == Event.class || eventClass == jdk.internal.event.Event.class || !jdk.internal.event.Event.class.isAssignableFrom(eventClass)) {
            throw new IllegalArgumentException("Must be a subclass to " + Event.class.getName());
        }
    }

    public static Object makePrimitiveArray(String typeName, List<Object> values) {
        int length = values.size();
        switch (typeName) {
        case "int":
            int[] ints = new int[length];
            for (int i = 0; i < length; i++) {
                ints[i] = (int) values.get(i);
            }
            return ints;
        case "long":
            long[] longs = new long[length];
            for (int i = 0; i < length; i++) {
                longs[i] = (long) values.get(i);
            }
            return longs;

        case "float":
            float[] floats = new float[length];
            for (int i = 0; i < length; i++) {
                floats[i] = (float) values.get(i);
            }
            return floats;

        case "double":
            double[] doubles = new double[length];
            for (int i = 0; i < length; i++) {
                doubles[i] = (double) values.get(i);
            }
            return doubles;

        case "short":
            short[] shorts = new short[length];
            for (int i = 0; i < length; i++) {
                shorts[i] = (short) values.get(i);
            }
            return shorts;
        case "char":
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = (char) values.get(i);
            }
            return chars;
        case "byte":
            byte[] bytes = new byte[length];
            for (int i = 0; i < length; i++) {
                bytes[i] = (byte) values.get(i);
            }
            return bytes;
        case "boolean":
            boolean[] booleans = new boolean[length];
            for (int i = 0; i < length; i++) {
                booleans[i] = (boolean) values.get(i);
            }
            return booleans;
        case "java.lang.String":
            String[] strings = new String[length];
            for (int i = 0; i < length; i++) {
                strings[i] = (String) values.get(i);
            }
            return strings;
        }
        return null;
    }

    public static String formatDuration(Duration d) {
        Duration roundedDuration = roundDuration(d);
        if (roundedDuration.equals(Duration.ZERO)) {
            return "0 s";
        } else if(roundedDuration.isNegative()){
            return "-" + formatPositiveDuration(roundedDuration.abs());
        } else {
            return formatPositiveDuration(roundedDuration);
        }
    }

    private static String formatPositiveDuration(Duration d){
        if (d.compareTo(MICRO_SECOND) < 0) {
            // 0.000001 ms - 0.000999 ms
            double outputMs = (double) d.toNanosPart() / 1_000_000;
            return String.format("%.6f ms",  outputMs);
        } else if (d.compareTo(SECOND) < 0) {
            // 0.001 ms - 999 ms
            int valueLength = countLength(d.toNanosPart());
            int outputDigit = NANO_SIGNIFICANT_FIGURES - valueLength;
            double outputMs = (double) d.toNanosPart() / 1_000_000;
            return String.format("%." + outputDigit + "f ms",  outputMs);
        } else if (d.compareTo(MINUTE) < 0) {
            // 1.00 s - 59.9 s
            int valueLength = countLength(d.toSecondsPart());
            int outputDigit = MILL_SIGNIFICANT_FIGURES - valueLength;
            double outputSecond = d.toSecondsPart() + (double) d.toMillisPart() / 1_000;
            return String.format("%." + outputDigit + "f s",  outputSecond);
        } else if (d.compareTo(HOUR) < 0) {
            // 1 m 0 s - 59 m 59 s
            return String.format("%d m %d s",  d.toMinutesPart(), d.toSecondsPart());
        } else if (d.compareTo(DAY) < 0) {
            // 1 h 0 m - 23 h 59 m
            return String.format("%d h %d m",  d.toHoursPart(), d.toMinutesPart());
        } else {
            // 1 d 0 h -
            return String.format("%d d %d h",  d.toDaysPart(), d.toHoursPart());
        }
    }

    private static int countLength(long value){
        return (int) Math.log10(value) + 1;
    }

    private static Duration roundDuration(Duration d) {
        if (d.equals(Duration.ZERO)) {
            return d;
        } else if(d.isNegative()){
            Duration roundedPositiveDuration = roundPositiveDuration(d.abs());
            return roundedPositiveDuration.negated();
        } else {
            return roundPositiveDuration(d);
        }
    }

    private static Duration roundPositiveDuration(Duration d){
        if (d.compareTo(MICRO_SECOND) < 0) {
            // No round
            return d;
        } else if (d.compareTo(SECOND) < 0) {
            // Round significant figures to three digits
            int valueLength = countLength(d.toNanosPart());
            int roundValue = (int) Math.pow(BASE, valueLength - DISPLAY_NANO_DIGIT);
            long roundedNanos = Math.round((double) d.toNanosPart() / roundValue) * roundValue;
            return d.truncatedTo(ChronoUnit.SECONDS).plusNanos(roundedNanos);
        } else if (d.compareTo(MINUTE) < 0) {
            // Round significant figures to three digits
            int valueLength = countLength(d.toSecondsPart());
            int roundValue = (int) Math.pow(BASE, valueLength);
            long roundedMills = Math.round((double) d.toMillisPart() / roundValue) * roundValue;
            return d.truncatedTo(ChronoUnit.SECONDS).plusMillis(roundedMills);
        } else if (d.compareTo(HOUR) < 0) {
            // Round for more than 500 ms or less
            return d.plusMillis(SECOND.dividedBy(2).toMillisPart()).truncatedTo(ChronoUnit.SECONDS);
        } else if (d.compareTo(DAY) < 0) {
            // Round for more than 30 seconds or less
            return d.plusSeconds(MINUTE.dividedBy(2).toSecondsPart()).truncatedTo(ChronoUnit.MINUTES);
        } else {
            // Round for more than 30 minutes or less
            return d.plusMinutes(HOUR.dividedBy(2).toMinutesPart()).truncatedTo(ChronoUnit.HOURS);
        }
    }


    public static void takeNap(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // ok
        }
    }

    public static void notifyFlush() {
        synchronized (flushObject) {
            flushObject.notifyAll();
        }
    }

    public static void waitFlush(long timeOut) {
        synchronized (flushObject) {
            try {
                flushObject.wait(timeOut);
            } catch (InterruptedException e) {
                // OK
            }
        }

    }

    public static Instant epochNanosToInstant(long epochNanos) {
        return Instant.ofEpochSecond(0, epochNanos);
    }

    public static long timeToNanos(Instant timestamp) {
        return timestamp.getEpochSecond() * 1_000_000_000L + timestamp.getNano();
    }

    public static String validTypeName(String typeName, String defaultTypeName) {
        if (Checks.isClassName(typeName)) {
            return typeName;
        } else {
            return defaultTypeName;
        }
    }

    public static String validJavaIdentifier(String identifier, String defaultIdentifier) {
        if (Checks.isJavaIdentifier(identifier)) {
            return identifier;
        } else {
            return defaultIdentifier;
        }
    }

    public static void ensureJavaIdentifier(String name) {
        if (!Checks.isJavaIdentifier(name)) {
            throw new IllegalArgumentException("'" + name + "' is not a valid Java identifier");
        }
    }

    private static void awaitUniqueTimestamp() {
        if (lastTimestamp == null) {
            lastTimestamp = Instant.now(); // lazy initialization
        }
        while (true) {
            Instant time = Instant.now();
            if (!time.equals(lastTimestamp)) {
                lastTimestamp = time;
                return;
            }
            try {
                Thread.sleep(0, 100);
            } catch (InterruptedException iex) {
                // ignore
            }
        }
    }

    public static String makeSimpleName(EventType type) {
      return makeSimpleName(type.getName());
    }

    public static String makeSimpleName(String qualified) {
        return qualified.substring(qualified.lastIndexOf(".") + 1);
    }
}