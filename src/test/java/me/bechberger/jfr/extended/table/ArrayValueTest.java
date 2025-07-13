package me.bechberger.jfr.extended.table;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive test suite for ArrayValue functionality including optimized contains operations.
 * 
 * Tests cover:
 * - Basic array creation and operations
 * - Optimized contains operation with caching for large arrays
 * - Factory methods for array creation
 * - Comparison and equality operations
 * - Performance characteristics for small vs large arrays
 */
public class ArrayValueTest {

    @Test
    void testBasicArrayCreation() {
        List<CellValue> elements = List.of(
            new CellValue.NumberValue(1),
            new CellValue.NumberValue(2),
            new CellValue.NumberValue(3)
        );
        
        CellValue.ArrayValue array = new CellValue.ArrayValue(elements);
        
        assertEquals(CellType.ARRAY, array.getType());
        assertEquals(elements, array.getValue());
        assertEquals(3, array.elements().size());
        assertEquals("[1, 2, 3]", array.toString());
    }

    @Test
    void testArrayFactoryMethods() {
        // Test arrayOf with CellValue list
        List<CellValue> cellElements = List.of(
            new CellValue.StringValue("hello"),
            new CellValue.NumberValue(42),
            new CellValue.BooleanValue(true)
        );
        CellValue.ArrayValue array1 = CellValue.arrayOf(cellElements);
        assertEquals(3, array1.elements().size());
        
        // Test arrayOf with object varargs
        CellValue.ArrayValue array2 = CellValue.arrayOf("hello", 42, true);
        assertEquals(3, array2.elements().size());
        assertEquals("[hello, 42, true]", array2.toString());
        
        // Test CellValue.of with List
        CellValue array3 = CellValue.of(List.of(1, 2, 3));
        assertTrue(array3 instanceof CellValue.ArrayValue);
        assertEquals("[1, 2, 3]", array3.toString());
        
        // Test CellValue.of with Object array
        CellValue array4 = CellValue.of(new Object[]{1, 2, 3});
        assertTrue(array4 instanceof CellValue.ArrayValue);
        assertEquals("[1, 2, 3]", array4.toString());
    }

    @Test
    void testSmallArrayContains() {
        // Small array (≤ 10 elements) should use linear search
        CellValue.ArrayValue array = CellValue.arrayOf(1, 2, 3, 4, 5);
        
        assertTrue(array.contains(new CellValue.NumberValue(3)));
        assertTrue(array.contains(new CellValue.NumberValue(1)));
        assertTrue(array.contains(new CellValue.NumberValue(5)));
        assertFalse(array.contains(new CellValue.NumberValue(6)));
        assertFalse(array.contains(new CellValue.StringValue("3")));
    }

    @Test
    void testLargeArrayContainsCaching() {
        // Large array (> 10 elements) should use cached HashSet
        List<CellValue> elements = new ArrayList<>();
        for (int i = 1; i <= 15; i++) {
            elements.add(new CellValue.NumberValue(i));
        }
        
        CellValue.ArrayValue array = new CellValue.ArrayValue(elements);
        
        // First contains call should build the cache
        assertTrue(array.contains(new CellValue.NumberValue(10)));
        assertTrue(array.contains(new CellValue.NumberValue(1)));
        assertTrue(array.contains(new CellValue.NumberValue(15)));
        
        // Subsequent calls should use the cache
        assertFalse(array.contains(new CellValue.NumberValue(16)));
        assertFalse(array.contains(new CellValue.NumberValue(0)));
        assertTrue(array.contains(new CellValue.NumberValue(7)));
    }

    @ParameterizedTest
    @ValueSource(ints = {5, 10, 15, 20, 50})
    void testContainsPerformance(int arraySize) {
        // Test contains operation for various array sizes
        List<CellValue> elements = new ArrayList<>();
        for (int i = 1; i <= arraySize; i++) {
            elements.add(new CellValue.NumberValue(i));
        }
        
        CellValue.ArrayValue array = new CellValue.ArrayValue(elements);
        
        // Test contains for existing element
        assertTrue(array.contains(new CellValue.NumberValue(arraySize / 2)));
        
        // Test contains for non-existing element
        assertFalse(array.contains(new CellValue.NumberValue(arraySize + 1)));
        
        // Multiple contains calls should be efficient
        for (int i = 1; i <= Math.min(arraySize, 10); i++) {
            assertTrue(array.contains(new CellValue.NumberValue(i)));
        }
    }

    @Test
    void testArrayEquality() {
        CellValue.ArrayValue array1 = CellValue.arrayOf(1, 2, 3);
        CellValue.ArrayValue array2 = CellValue.arrayOf(1, 2, 3);
        CellValue.ArrayValue array3 = CellValue.arrayOf(1, 2, 4);
        
        assertEquals(array1, array2);
        assertNotEquals(array1, array3);
        assertEquals(array1.hashCode(), array2.hashCode());
        assertNotEquals(array1.hashCode(), array3.hashCode());
    }

    @Test
    void testArrayComparison() {
        CellValue.ArrayValue array1 = CellValue.arrayOf(1, 2, 3);
        CellValue.ArrayValue array2 = CellValue.arrayOf(1, 2, 3);
        CellValue.ArrayValue array3 = CellValue.arrayOf(1, 2, 4);
        CellValue.ArrayValue array4 = CellValue.arrayOf(1, 2);
        CellValue.ArrayValue array5 = CellValue.arrayOf(1, 2, 3, 4);
        
        // Equal arrays
        assertEquals(0, CellValue.compare(array1, array2));
        
        // Lexicographic comparison
        assertTrue(CellValue.compare(array1, array3) < 0); // [1,2,3] < [1,2,4]
        assertTrue(CellValue.compare(array3, array1) > 0); // [1,2,4] > [1,2,3]
        
        // Size comparison when prefix matches
        assertTrue(CellValue.compare(array4, array1) < 0); // [1,2] < [1,2,3]
        assertTrue(CellValue.compare(array1, array5) < 0); // [1,2,3] < [1,2,3,4]
    }

    @Test
    void testMixedTypeArray() {
        CellValue.ArrayValue array = CellValue.arrayOf(
            "hello", 
            42, 
            true, 
            java.time.Duration.ofMillis(1000)
        );
        
        assertEquals(4, array.elements().size());
        assertTrue(array.contains(new CellValue.StringValue("hello")));
        assertTrue(array.contains(new CellValue.NumberValue(42)));
        assertTrue(array.contains(new CellValue.BooleanValue(true)));
        assertFalse(array.contains(new CellValue.StringValue("world")));
    }

    @Test
    void testEmptyArray() {
        CellValue.ArrayValue emptyArray = CellValue.arrayOf();
        
        assertEquals(0, emptyArray.elements().size());
        assertEquals("[]", emptyArray.toString());
        assertFalse(emptyArray.contains(new CellValue.NumberValue(1)));
    }

    @Test
    void testNestedArrays() {
        CellValue.ArrayValue innerArray1 = CellValue.arrayOf(1, 2);
        CellValue.ArrayValue innerArray2 = CellValue.arrayOf(3, 4);
        
        List<CellValue> outerElements = List.of(innerArray1, innerArray2);
        CellValue.ArrayValue outerArray = new CellValue.ArrayValue(outerElements);
        
        assertEquals(2, outerArray.elements().size());
        assertTrue(outerArray.contains(innerArray1));
        assertTrue(outerArray.contains(innerArray2));
        assertEquals("[[1, 2], [3, 4]]", outerArray.toString());
    }

    @Test
    void testArrayWithNullValues() {
        CellValue.ArrayValue array = CellValue.arrayOf(1, null, 3);
        
        assertEquals(3, array.elements().size());
        assertTrue(array.contains(new CellValue.NumberValue(1)));
        assertTrue(array.contains(new CellValue.NullValue()));
        assertTrue(array.contains(new CellValue.NumberValue(3)));
        assertEquals("[1, N/A, 3]", array.toString());
    }

    @Test
    void testImmutability() {
        List<CellValue> originalElements = new ArrayList<>();
        originalElements.add(new CellValue.NumberValue(1));
        originalElements.add(new CellValue.NumberValue(2));
        
        CellValue.ArrayValue array = new CellValue.ArrayValue(originalElements);
        
        // Modifying original list should not affect the array
        originalElements.add(new CellValue.NumberValue(3));
        assertEquals(2, array.elements().size());
        
        // The returned elements list should be immutable
        assertThrows(UnsupportedOperationException.class, () -> {
            array.elements().add(new CellValue.NumberValue(3));
        });
    }
}
