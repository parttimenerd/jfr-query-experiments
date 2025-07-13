package me.bechberger.jfr.extended.table;

import me.bechberger.jfr.extended.ast.ASTNodes.BinaryOperator;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.stream.Collectors;

/**
 * Represents a cell value in a JFR table with specific type information.
 * <p>
 * Supports multiple types: String, Number, Duration, Timestamp, MemorySize, Rate, Boolean, Float, Array, and Null.
 * Provides type-safe operations, conversion, and comparison utilities for table cell values.
 * <p>
 * Use {@link CellValue#of(Object)} to create instances from raw values.
 * <p>
 * All numeric operations are type-preserving and support type promotion where necessary.
 */
public sealed interface CellValue permits 
    CellValue.StringValue,
    CellValue.NumberValue, 
    CellValue.DurationValue,
    CellValue.TimestampValue,
    CellValue.MemorySizeValue,
    CellValue.RateValue,
    CellValue.BooleanValue,
    CellValue.FloatValue,
    CellValue.ArrayValue,
    CellValue.NullValue {
    
    /**
     * Get the raw value
     */
    Object getValue();
    
    /**
     * Get the type of this cell value
     */
    CellType getType();
    
    /**
     * Convert to string representation
     */
    String toString();
    
    /**
     * Map function for numeric operations
     * All numeric values are treated as double internally, but type is preserved
     */
    default CellValue mapNumeric(Function<Double, Double> mapper) {
        if (!isNumeric()) {
            throw new IllegalArgumentException("Cannot perform numeric operation on: " + this.getType());
        }
        
        double value = extractNumericValue();
        double result = mapper.apply(value);
        
        return switch (this) {
            case NumberValue n -> new NumberValue(result);
            case FloatValue f -> new FloatValue(result);
            case DurationValue d -> new DurationValue(Duration.ofNanos((long) (result * 1_000_000.0)));
            case TimestampValue t -> new TimestampValue(Instant.ofEpochMilli((long) result));
            case MemorySizeValue m -> new MemorySizeValue((long) result);
            case RateValue r -> new RateValue(result, r.timeUnit());
            default -> throw new IllegalArgumentException("Cannot perform numeric operation on: " + this.getType());
        };
    }
    
    /**
     * Binary operation mapping for two values with type compatibility checking
     * Handles special cases like timestamp operations and type promotion
     */
    default CellValue mapBinary(CellValue other, BinaryOperator operator) {
        // Check type compatibility first
        if (!isCompatibleForBinaryOperation(other, operator)) {
            throw new IllegalArgumentException(
                "Cannot perform " + operator + " operation between " + this.getType() + " and " + other.getType()
            );
        }
        
        double thisValue = extractNumericValue();
        double otherValue = other.extractNumericValue();
        
        // Perform the operation
        double result = switch (operator) {
            case ADD -> thisValue + otherValue;
            case SUBTRACT -> thisValue - otherValue;
            case MULTIPLY -> thisValue * otherValue;
            case DIVIDE -> {
                if (otherValue == 0.0) {
                    throw new ArithmeticException("Division by zero");
                }
                yield thisValue / otherValue;
            }
            case MODULO -> {
                if (otherValue == 0.0) {
                    throw new ArithmeticException("Modulo by zero");
                }
                yield thisValue % otherValue;
            }
            default -> throw new IllegalArgumentException("Unsupported binary operator: " + operator);
        };
        
        // Determine result type based on operation and operands
        return determineResultType(other, operator, result);
    }
    
    /**
     * Check if two values are compatible for binary operations
     */
    default boolean isCompatibleForBinaryOperation(CellValue other, BinaryOperator operator) {
        // STRING, BOOLEAN, NULL cannot participate in arithmetic operations
        if (!this.isNumeric() || !other.isNumeric()) {
            return false;
        }
        
        CellType thisType = this.getType();
        CellType otherType = other.getType();
        
        // Same types are always compatible
        if (thisType == otherType) {
            return true;
        }
        
        // NUMBER and FLOAT can operate with all numeric types
        if (thisType == CellType.NUMBER || thisType == CellType.FLOAT ||
            otherType == CellType.NUMBER || otherType == CellType.FLOAT) {
            return true;
        }
        
        // TIMESTAMP can operate with DURATION
        if ((thisType == CellType.TIMESTAMP && otherType == CellType.DURATION) ||
            (thisType == CellType.DURATION && otherType == CellType.TIMESTAMP)) {
            return true;
        }
        
        // All other cross-type operations are prohibited
        return false;
    }
    
    /**
     * Determine the result type based on operands and operation
     */
    default CellValue determineResultType(CellValue other, BinaryOperator operator, double result) {
        CellType thisType = this.getType();
        CellType otherType = other.getType();
        
        // Special case: timestamp operations
        if (thisType == CellType.TIMESTAMP && otherType == CellType.TIMESTAMP) {
            // timestamp - timestamp = duration
            return new DurationValue(Duration.ofMillis((long) result));
        }
        
        if ((thisType == CellType.TIMESTAMP && otherType == CellType.DURATION) ||
            (thisType == CellType.DURATION && otherType == CellType.TIMESTAMP)) {
            // timestamp +/- duration = timestamp
            // duration +/- timestamp = timestamp (commutative for addition)
            if (operator == BinaryOperator.ADD || 
                (operator == BinaryOperator.SUBTRACT && thisType == CellType.TIMESTAMP)) {
                return new TimestampValue(Instant.ofEpochMilli((long) result));
            } else {
                return new DurationValue(Duration.ofMillis((long) result));
            }
        }
        
        // For same types, preserve the type
        if (thisType == otherType) {
            return switch (thisType) {
                case NUMBER -> new NumberValue(result);
                case FLOAT -> new FloatValue(result);
                case DURATION -> new DurationValue(Duration.ofMillis((long) result));
                case TIMESTAMP -> new TimestampValue(Instant.ofEpochMilli((long) result));
                case MEMORY_SIZE -> new MemorySizeValue((long) result);
                case RATE -> new RateValue(result, ((RateValue) this).timeUnit());
                default -> throw new IllegalArgumentException("Cannot determine result type for: " + thisType);
            };
        }
        
        // For mixed NUMBER/FLOAT operations, promote to FLOAT
        if ((thisType == CellType.NUMBER || thisType == CellType.FLOAT) &&
            (otherType == CellType.NUMBER || otherType == CellType.FLOAT)) {
            return new FloatValue(result);
        }
        
        // For operations with NUMBER/FLOAT and other numeric types, preserve the non-NUMBER/FLOAT type
        if (thisType == CellType.NUMBER || thisType == CellType.FLOAT) {
            return switch (otherType) {
                case DURATION -> new DurationValue(Duration.ofMillis((long) result));
                case TIMESTAMP -> new TimestampValue(Instant.ofEpochMilli((long) result));
                case MEMORY_SIZE -> new MemorySizeValue((long) result);
                case RATE -> new RateValue(result, ((RateValue) other).timeUnit());
                default -> new FloatValue(result);
            };
        }
        
        if (otherType == CellType.NUMBER || otherType == CellType.FLOAT) {
            return switch (thisType) {
                case DURATION -> new DurationValue(Duration.ofMillis((long) result));
                case TIMESTAMP -> new TimestampValue(Instant.ofEpochMilli((long) result));
                case MEMORY_SIZE -> new MemorySizeValue((long) result);
                case RATE -> new RateValue(result, ((RateValue) this).timeUnit());
                default -> new FloatValue(result);
            };
        }
        
        // Fallback to preserving left operand type
        return this.mapNumeric(x -> result);
    }
    
    /**
     * Check if this value is numeric
     */
    default boolean isNumeric() {
        return switch (this.getType()) {
            case NUMBER, FLOAT, DURATION, TIMESTAMP, MEMORY_SIZE, RATE -> true;
            default -> false;
        };
    }
    
    /**
     * Extract numeric value for calculations (all as double now)
     */
    default double extractNumericValue() {
        return switch (this) {
            case NumberValue n -> n.value();
            case FloatValue f -> f.value();
            case DurationValue d -> d.value().toNanos() / 1_000_000.0; // Convert to milliseconds
            case TimestampValue t -> (double) t.value().toEpochMilli();
            case MemorySizeValue m -> (double) m.value();
            case RateValue r -> r.count();
            default -> throw new IllegalArgumentException("Cannot extract numeric value from: " + this.getType());
        };
    }
    
    /**
     * Static utility function for mapping a list of CellValues to a single numeric result.
     * Automatically handles numeric validation and type preservation.
     * 
     * @param values List of CellValues to process
     * @param mapper Function that takes a list of doubles and returns a single double
     * @return CellValue with the same type as the first input value, or NumberValue if mixed types
     */
    static CellValue mapDouble(List<CellValue> values, Function<List<Double>, Double> mapper) {
        if (values.isEmpty()) {
            return new NullValue();
        }
        
        // Check that all values are numeric
        for (CellValue value : values) {
            if (!(value instanceof NullValue) && !value.isNumeric()) {
                throw new IllegalArgumentException("All arguments must be numeric, found: " + value.getType());
            }
        }
        
        // Filter out null values and extract numeric values
        List<Double> numericValues = values.stream()
                .filter(v -> !(v instanceof NullValue))
                .map(CellValue::extractNumericValue)
                .collect(Collectors.toList());
        
        if (numericValues.isEmpty()) {
            return new NullValue();
        }
        
        // Apply the mapping function
        double result = mapper.apply(numericValues);
        
        // Determine the result type based on the first non-null input
        CellValue firstNonNull = values.stream()
                .filter(v -> !(v instanceof NullValue))
                .findFirst()
                .orElse(new NumberValue(result));
        
        // Return result with the same type as the first non-null input
        return firstNonNull.mapNumeric(x -> result);
    }
    
    /**
     * String cell value
     */
    record StringValue(String value) implements CellValue {
        @Override
        public Object getValue() { return value; }
        
        @Override
        public CellType getType() { return CellType.STRING; }
        
        @Override
        public String toString() { return value != null ? value : "N/A"; }
    }
    
    /**
     * Number cell value (uses double internally for all calculations)
     */
    record NumberValue(double value) implements CellValue {
        @Override
        public Object getValue() { return value; }
        
        @Override
        public CellType getType() { return CellType.NUMBER; }
        
        @Override
        public String toString() { 
            // Display as integer if it's a whole number
            if (value == Math.floor(value) && !Double.isInfinite(value)) {
                return String.valueOf((long) value);
            } else {
                return String.valueOf(value);
            }
        }
        
        @Override
        public int hashCode() {
            return Double.hashCode(value);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof NumberValue other)) return false;
            return Double.compare(value, other.value) == 0;
        }
    }
    
    /**
     * Duration cell value
     */
    record DurationValue(Duration value) implements CellValue {
        @Override
        public Object getValue() { return value; }
        
        @Override
        public CellType getType() { return CellType.DURATION; }
         @Override
        public String toString() { 
            if (value == null) return "N/A";
            return DurationFormatter.format(value);
        }
    }

    /**
     * Timestamp cell value (stores both the parsed Instant and original format string)
     */
    record TimestampValue(Instant value, String originalFormat) implements CellValue {
        // Constructor for backward compatibility
        public TimestampValue(Instant value) {
            this(value, value != null ? value.toString() : "N/A");
        }
        
        @Override
        public Object getValue() { return value; }
        
        @Override
        public CellType getType() { return CellType.TIMESTAMP; }
        
        @Override
        public String toString() { 
            return originalFormat != null ? originalFormat : (value != null ? value.toString() : "N/A"); 
        }
    }
    
    /**
     * Memory size cell value (bytes stored as long)
     */
    record MemorySizeValue(long value) implements CellValue {
        @Override
        public Object getValue() { return value; }
        
        @Override
        public CellType getType() { return CellType.MEMORY_SIZE; }
        
        @Override
        public String toString() {
            long bytes = value;
            if (bytes < 1024) {
                return bytes + "B";
            } else if (bytes < 1024 * 1024) {
                long kb = bytes / 1024;
                if (bytes % 1024 == 0) {
                    return kb + "KB";
                } else {
                    return String.format(java.util.Locale.US, "%.1fKB", bytes / 1024.0);
                }
            } else if (bytes < 1024L * 1024 * 1024) {
                long mb = bytes / (1024 * 1024);
                if (bytes % (1024 * 1024) == 0) {
                    return mb + "MB";
                } else {
                    return String.format(java.util.Locale.US, "%.1fMB", bytes / (1024.0 * 1024));
                }
            } else if (bytes < 1024L * 1024 * 1024 * 1024) {
                long gb = bytes / (1024L * 1024 * 1024);
                if (bytes % (1024L * 1024 * 1024) == 0) {
                    return gb + "GB";
                } else {
                    return String.format(java.util.Locale.US, "%.1fGB", bytes / (1024.0 * 1024 * 1024));
                }
            } else {
                long tb = bytes / (1024L * 1024 * 1024 * 1024);
                if (bytes % (1024L * 1024 * 1024 * 1024) == 0) {
                    return tb + "TB";
                } else {
                    return String.format(java.util.Locale.US, "%.1fTB", bytes / (1024.0 * 1024 * 1024 * 1024));
                }
            }
        }
        
        @Override
        public int hashCode() {
            return Long.hashCode(value);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof MemorySizeValue other)) return false;
            return value == other.value;
        }
    }
    
    /**
     * Rate cell value (count per time unit)
     */
    record RateValue(double count, Duration timeUnit) implements CellValue {
        @Override
        public Object getValue() { return count; }
        
        @Override
        public CellType getType() { return CellType.RATE; }
        
        @Override
        public String toString() {
            String unit = "s";
            if (timeUnit.equals(Duration.ofMinutes(1))) {
                unit = "min";
            } else if (timeUnit.equals(Duration.ofHours(1))) {
                unit = "h";
            }
            return count + "/" + unit;
        }
    }
    
    /**
     * Boolean cell value
     */
    record BooleanValue(boolean value) implements CellValue {
        @Override
        public Object getValue() { return value; }
        
        @Override
        public CellType getType() { return CellType.BOOLEAN; }
        
        @Override
        public String toString() { return String.valueOf(value); }
        
         
        @Override
        public int hashCode() {
            return Boolean.hashCode(value);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof BooleanValue other)) return false;
            return value == other.value;
        }
    }
    
    /**
     * Floating point cell value
     */
    record FloatValue(double value) implements CellValue {
        @Override
        public Object getValue() { return value; }
        
        @Override
        public CellType getType() { return CellType.FLOAT; }
        
        @Override
        public String toString() { return String.valueOf(value); }
        
        @Override
        public int hashCode() {
            return Double.hashCode(value);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof FloatValue other)) return false;
            return Double.compare(value, other.value) == 0;
        }
    }
    
    /**
     * Array cell value with optimized contains operation
     */
    final class ArrayValue implements CellValue, List<CellValue> {
        private final List<CellValue> elements;
        private Set<CellValue> containsCache;
        private static final int CACHE_THRESHOLD = 10;
        
        public ArrayValue(List<CellValue> elements) {
            this.elements = List.copyOf(elements); // Make immutable
        }
        
        public List<CellValue> elements() {
            return elements;
        }
        
        /**
         * Optimized contains check with caching for large arrays
         */
        public boolean contains(CellValue value) {
            if (elements.size() <= CACHE_THRESHOLD) {
                // For small arrays, linear search is faster
                return elements.contains(value);
            } else {
                // For large arrays, use cached HashSet
                if (containsCache == null) {
                    containsCache = new HashSet<>(elements);
                }
                return containsCache.contains(value);
            }
        }
        
        @Override
        public Object getValue() { 
            return elements; 
        }
        
        @Override
        public CellType getType() { 
            return CellType.ARRAY; 
        }
        
        @Override
        public String toString() { 
            return "[" + elements.stream()
                .map(CellValue::toString)
                .collect(Collectors.joining(", ")) + "]";
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof ArrayValue other)) return false;
            return elements.equals(other.elements);
        }
        
        @Override
        public int hashCode() {
            return elements.hashCode();
        }
        
        // List<CellValue> implementation delegates to the underlying list
        @Override
        public int size() {
            return elements.size();
        }
        
        @Override
        public boolean isEmpty() {
            return elements.isEmpty();
        }
        
        @Override
        public boolean contains(Object o) {
            if (o instanceof CellValue cellValue) {
                return contains(cellValue);
            }
            return elements.contains(o);
        }
        
        @Override
        public Iterator<CellValue> iterator() {
            return elements.iterator();
        }
        
        @Override
        public Object[] toArray() {
            return elements.toArray();
        }
        
        @Override
        public <T> T[] toArray(T[] a) {
            return elements.toArray(a);
        }
        
        @Override
        public boolean add(CellValue cellValue) {
            throw new UnsupportedOperationException("ArrayValue is immutable");
        }
        
        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("ArrayValue is immutable");
        }
        
        @Override
        public boolean containsAll(Collection<?> c) {
            return elements.containsAll(c);
        }
        
        @Override
        public boolean addAll(Collection<? extends CellValue> c) {
            throw new UnsupportedOperationException("ArrayValue is immutable");
        }
        
        @Override
        public boolean addAll(int index, Collection<? extends CellValue> c) {
            throw new UnsupportedOperationException("ArrayValue is immutable");
        }
        
        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("ArrayValue is immutable");
        }
        
        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("ArrayValue is immutable");
        }
        
        @Override
        public void clear() {
            throw new UnsupportedOperationException("ArrayValue is immutable");
        }
        
        @Override
        public CellValue get(int index) {
            return elements.get(index);
        }
        
        @Override
        public CellValue set(int index, CellValue element) {
            throw new UnsupportedOperationException("ArrayValue is immutable");
        }
        
        @Override
        public void add(int index, CellValue element) {
            throw new UnsupportedOperationException("ArrayValue is immutable");
        }
        
        @Override
        public CellValue remove(int index) {
            throw new UnsupportedOperationException("ArrayValue is immutable");
        }
        
        @Override
        public int indexOf(Object o) {
            return elements.indexOf(o);
        }
        
        @Override
        public int lastIndexOf(Object o) {
            return elements.lastIndexOf(o);
        }
        
        @Override
        public ListIterator<CellValue> listIterator() {
            return elements.listIterator();
        }
        
        @Override
        public ListIterator<CellValue> listIterator(int index) {
            return elements.listIterator(index);
        }
        
        @Override
        public List<CellValue> subList(int fromIndex, int toIndex) {
            // Return a new ArrayValue for the sublist to maintain type consistency
            return new ArrayValue(elements.subList(fromIndex, toIndex));
        }
    }
    
    /**
     * Null/missing cell value
     */
    record NullValue() implements CellValue {
        @Override
        public Object getValue() { return null; }
        
        @Override
        public CellType getType() { return CellType.NULL; }
        
        @Override
        public String toString() { return "N/A"; }
    }
    
    /**
     * Factory methods for creating cell values
     */
    static CellValue of(Object value) {
        if (value == null) return new NullValue();
        if (value instanceof String s) return new StringValue(s);
        if (value instanceof Integer i) return new NumberValue(i.longValue());
        if (value instanceof Long l) return new NumberValue(l);
        if (value instanceof Double d) return new FloatValue(d);
        if (value instanceof Float f) return new FloatValue(f.doubleValue());
        if (value instanceof Boolean b) return new BooleanValue(b);
        if (value instanceof Duration d) return new DurationValue(d);
        if (value instanceof Instant i) return new TimestampValue(i);
        if (value instanceof List<?> list) {
            List<CellValue> elements = list.stream()
                .map(CellValue::of)
                .collect(Collectors.toList());
            return new ArrayValue(elements);
        }
        if (value instanceof Object[] array) {
            List<CellValue> elements = new ArrayList<>();
            for (Object element : array) {
                elements.add(CellValue.of(element));
            }
            return new ArrayValue(elements);
        }
        
        // Default to string representation
        return new StringValue(value.toString());
    }
    
    /**
     * Factory method for creating array values from CellValue lists
     */
    static ArrayValue arrayOf(List<CellValue> elements) {
        return new ArrayValue(elements);
    }
    
    /**
     * Factory method for creating array values from object arrays
     */
    static ArrayValue arrayOf(Object... elements) {
        List<CellValue> cellValues = new ArrayList<>();
        for (Object element : elements) {
            cellValues.add(CellValue.of(element));
        }
        return new ArrayValue(cellValues);
    }
    
    /**
     * Compare cell values for sorting and ordering
     */
    static int compare(CellValue a, CellValue b) {
        if (a instanceof NullValue && b instanceof NullValue) return 0;
        if (a instanceof NullValue) return -1;
        if (b instanceof NullValue) return 1;
        
        if (a.getType() != b.getType()) {
            return a.getType().compareTo(b.getType());
        }
        
        return switch (a.getType()) {
            case STRING -> ((StringValue) a).value().compareTo(((StringValue) b).value());
            case NUMBER -> Double.compare(((NumberValue) a).value(), ((NumberValue) b).value());
            case FLOAT -> Double.compare(((FloatValue) a).value(), ((FloatValue) b).value());
            case BOOLEAN -> Boolean.compare(((BooleanValue) a).value(), ((BooleanValue) b).value());
            case DURATION -> ((DurationValue) a).value().compareTo(((DurationValue) b).value());
            case TIMESTAMP -> ((TimestampValue) a).value().compareTo(((TimestampValue) b).value());
            case MEMORY_SIZE -> Long.compare(((MemorySizeValue) a).value(), ((MemorySizeValue) b).value());
            case RATE -> Double.compare(((RateValue) a).count(), ((RateValue) b).count());
            case ARRAY -> {
                ArrayValue arrayA = (ArrayValue) a;
                ArrayValue arrayB = (ArrayValue) b;
                // Compare arrays lexicographically
                int minSize = Math.min(arrayA.elements().size(), arrayB.elements().size());
                for (int i = 0; i < minSize; i++) {
                    int result = compare(arrayA.elements().get(i), arrayB.elements().get(i));
                    if (result != 0) yield result;
                }
                yield Integer.compare(arrayA.elements().size(), arrayB.elements().size());
            }
            case NULL -> 0;
        };
    }
}
