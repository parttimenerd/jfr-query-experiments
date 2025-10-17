package me.bechberger.jfr.duckdb.query;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class TablePrinterTest {

    private static List<TablePrinter.Column> cols(TablePrinter.Column... c) {
        List<TablePrinter.Column> list = new ArrayList<>();
        for (TablePrinter.Column col : c) list.add(col);
        return list;
    }

    private static List<List<String>> rows(List<String>... r) {
        List<List<String>> list = new ArrayList<>();
        for (List<String> row : r) list.add(row);
        return list;
    }

    private static String[] lines(String s) {
        if (s == null || s.isEmpty()) return new String[0];
        return s.split("\r?\n");
    }

    private static String rtrim(String s) {
        return s == null ? null : s.replaceFirst("\\s+$", "");
    }

    @Test
    void testBasicPrint() {
        List<TablePrinter.Column> columns = cols(new TablePrinter.Column("Method", true), new TablePrinter.Column("Samples", false));
        List<List<String>> rows = rows(List.of("java.lang.Foo.bar()", "123"));
        TablePrinter tp = new TablePrinter(columns, rows);

        String out = tp.toString(80);
        String[] ls = lines(out);
        // header, separator, 1 row
        assertEquals(3, ls.length, "should print header, separator and one row");
        assertTrue(ls[0].contains("Method"));
        assertTrue(ls[0].contains("Samples"));
        assertTrue(ls[2].contains("java.lang.Foo.bar()"));
        assertTrue(ls[2].contains("123"));
    }

    @Test
    void testTruncationAndMaxWidth() {
        // create very long header and a long cell
        String longHeader = "ThisIsAnExceedinglyLongHeaderThatShouldBeTruncatedByThePrinterBecauseItExceedsMaxWidth";
        String longCell = "this_is_a_very_long_cell_value_that_will_not_fit_in_the_column_and_needs_truncation";
        List<TablePrinter.Column> columns = cols(new TablePrinter.Column(longHeader, true), new TablePrinter.Column("N", false));
        List<List<String>> rows = rows(List.of(longCell, "999999"));
        TablePrinter tp = new TablePrinter(columns, rows);

        int maxWidth = 30;
        String out = tp.toString(maxWidth);
        String[] ls = lines(out);
        // header + separator + row
        assertEquals(3, ls.length);
        for (String line : ls) {
            assertTrue(line.length() <= maxWidth, () -> "line longer than maxWidth (" + line.length() + "): '" + line + "'");
        }
        // at least one ellipsis should appear due to truncation
        assertTrue(out.contains("…"), "Expected an ellipsis in the truncated output");
    }

    @Test
    void testEmptyRowsProducesOnlyHeaderAndSeparator() {
        List<TablePrinter.Column> columns = cols(new TablePrinter.Column("ColA", true), new TablePrinter.Column("ColB", false));
        List<List<String>> rows = List.of();
        TablePrinter tp = new TablePrinter(columns, rows);

        String out = tp.toString(40);
        String[] ls = lines(out);
        assertEquals(2, ls.length, "With no rows should only print header and separator");
        // separator should be same length as header
        assertEquals(ls[0].length(), ls[1].length(), "separator should match header length");
        assertTrue(ls[1].matches("[- ]+"), "separator should only contain dashes and spaces");
    }

    // --- New tests added below ---

    @Test
    void testUnicodeEmojiTruncation() {
        String longHeader = "🌟🌟🌟🌟🌟SuperEmojiHeader🌟🌟🌟🌟";
        String longCell = "🔴🔴🔴verylongemojisequence🔴🔴🔴🔴🔴🔴";
        List<TablePrinter.Column> columns = cols(new TablePrinter.Column(longHeader, true), new TablePrinter.Column("Count", false));
        List<List<String>> rows = rows(List.of(longCell, "42"));
        TablePrinter tp = new TablePrinter(columns, rows);

        int maxWidth = 20;
        String out = tp.toString(maxWidth);
        String[] ls = lines(out);
        // header+sep+row
        assertEquals(3, ls.length);
        for (String line : ls) {
            assertTrue(line.length() <= maxWidth, () -> "line longer than maxWidth: '" + line + "'");
        }
        assertTrue(out.contains("…"), "Expected truncation ellipsis for emoji content");
    }

    @Test
    void testNullAndUnevenRows() {
        List<TablePrinter.Column> columns = cols(new TablePrinter.Column("A", true), new TablePrinter.Column("B", true), new TablePrinter.Column("C", true));
        // List.of(null, ...) would throw; use Arrays.asList to allow null
        List<List<String>> rows = rows(Arrays.asList("onlyA"), Arrays.asList((String) null, "bval", "cval"));
        TablePrinter tp = new TablePrinter(columns, rows);

        String out = tp.toString(80);
        String[] ls = lines(out);
        // header + separator + 2 rows
        assertEquals(4, ls.length);
        // rows should contain the provided values or empty placeholders
        assertTrue(out.contains("onlyA"));
        assertTrue(out.contains("bval"));
        assertTrue(out.contains("cval"));
        // ensure null didn't cause literal 'null' to appear
        assertFalse(out.contains("null\n"));
    }

    @Test
    void testManyColumnsDistributionFitsWidth() {
        List<TablePrinter.Column> columns = cols(
                new TablePrinter.Column("C1", true),
                new TablePrinter.Column("C2", false),
                new TablePrinter.Column("C3", true),
                new TablePrinter.Column("C4", false),
                new TablePrinter.Column("C5", true),
                new TablePrinter.Column("C6", false)
        );
        List<List<String>> rows = rows(
                List.of("one", "2", "three", "4", "five", "6"),
                List.of("longercontent", "12345", "x", "999", "y", "0")
        );
        TablePrinter tp = new TablePrinter(columns, rows);

        int maxWidth = 50;
        String out = tp.toString(maxWidth);
        String[] ls = lines(out);
        assertTrue(ls.length >= 3);
        for (String line : ls) {
            assertTrue(line.length() <= maxWidth, () -> "line exceeds maxWidth: '" + line + "'");
        }
    }

    @Test
    void testTinyMaxWidthDoesNotThrowAndTruncates() {
        List<TablePrinter.Column> columns = cols(new TablePrinter.Column("LongHeader", true), new TablePrinter.Column("N", false));
        List<List<String>> rows = rows(List.of("verylongcellvalue", "1234567890"));
        TablePrinter tp = new TablePrinter(columns, rows);

        String originalA = "verylongcellvalue";
        String originalB = "1234567890";
        boolean detected = false;
        for (int maxWidth : new int[]{0, 1, 2, 3, 4}) {
            String out = tp.toString(maxWidth);
            // ensure it didn't throw and produced some output
            assertNotNull(out);
            // detect truncation either via ellipsis or by absence of the original full strings
            if (out.contains("…") || !out.contains(originalA) || !out.contains(originalB)) {
                detected = true;
            }
        }
        assertTrue(detected, "Expected truncation or removal of full original strings for tiny widths");
    }

    @Test
    void testRightAlignedColumnsShrinkWithEllipsis() {
        // create two right-aligned columns (alignLeft = false)
        List<TablePrinter.Column> columns = cols(new TablePrinter.Column("NumA", false), new TablePrinter.Column("NumB", false));
        List<List<String>> rows = rows(List.of("123456789012345", "987654321098765"));
        TablePrinter tp = new TablePrinter(columns, rows);

        int maxWidth = 20; // intentionally small
        String out = tp.toString(maxWidth);
        String[] ls = lines(out);
        assertEquals(3, ls.length);
        // should have truncation somewhere
        assertTrue(out.contains("…"), "Expected ellipsis when right-aligned columns had to be shrunk");
    }

    // --- Parameterized whole-table tests using text blocks ---
    @ParameterizedTest(name = "table case {index}")
    @MethodSource("tableProvider")
    void testWholeTableOutput(List<TablePrinter.Column> columns, List<List<String>> rowsData, int maxWidth, String expected) {
        TablePrinter tp = new TablePrinter(columns, rowsData);
        String out = tp.toString(maxWidth);
        // Compare line-by-line after trimming trailing spaces to avoid constant differences from padding
        String[] got = lines(out);
        String[] want = lines(expected);
        assertEquals(want.length, got.length, "line count mismatch\nEXPECTED:\n" + expected + "\nGOT:\n" + out);
        for (int i = 0; i < want.length; i++) {
            assertEquals(rtrim(want[i]), rtrim(got[i]), "mismatch on line " + i + "\nEXPECTED:'" + want[i] + "'\nGOT:     '" + got[i] + "'");
        }
    }

    static Stream<Arguments> tableProvider() {
        return Stream.of(
                // single column
                Arguments.of(
                        cols(new TablePrinter.Column("H", true)),
                        rows(List.of("a"), List.of("bb")),
                        5,
                        """
                        H 
                        --
                        a 
                        bb
                        """
                ),
                // two left columns
                Arguments.of(
                        cols(new TablePrinter.Column("A", true), new TablePrinter.Column("B", true)),
                        rows(List.of("x", "y")),
                        10,
                        """
                        A B
                        - -
                        x y
                        """
                ),
                // left + right mix
                Arguments.of(
                        cols(new TablePrinter.Column("L", true), new TablePrinter.Column("R", false)),
                        rows(List.of("foo", "7")),
                        6,
                        """
                        L   R
                        --- -
                        foo 7
                        """
                ),
                // edge: many short left columns to force distribution
                Arguments.of(
                        cols(new TablePrinter.Column("c1", true), new TablePrinter.Column("c2", true), new TablePrinter.Column("c3", true), new TablePrinter.Column("c4", true)),
                        rows(List.of("1", "2", "3", "4")),
                        10,
                        """
                        c1 c2 c3 c
                        -- -- -- -
                        1  2  3  4
                        """
                ),
                // edge: left column needs to be wider than header because of content
                Arguments.of(
                        cols(new TablePrinter.Column("H", true), new TablePrinter.Column("N", false)),
                        rows(List.of("longcontent", "9")),
                        15,
                        """
                        H           N
                        ----------- -
                        longcontent 9
                        """
                )
        );
    }

    // New param test to inspect computed column widths via the separator line
    @ParameterizedTest(name = "widths {index}")
    @MethodSource("widthsProvider")
    void testComputedColumnWidths(List<TablePrinter.Column> columns, List<List<String>> rowsData, int maxWidth, int[] expectedWidths) {
        TablePrinter tp = new TablePrinter(columns, rowsData);
        String out = tp.toString(maxWidth);
        String[] ls = lines(out);
        assertTrue(ls.length >= 2, "expected at least header and separator");
        String sep = ls[1];
        // split on single spaces (columns separated by single space)
        String[] groups = sep.split(" ");
        assertEquals(expectedWidths.length, groups.length, "separator groups count");
        for (int i = 0; i < expectedWidths.length; i++) {
            assertEquals(expectedWidths[i], groups[i].length(), "width mismatch for column " + i + "\nSEPARATOR: " + sep);
        }
    }

    static Stream<Arguments> widthsProvider() {
        return Stream.of(
                Arguments.of(cols(new TablePrinter.Column("H", true)), rows(List.of("a"), List.of("bb")), 5, new int[]{2}),
                Arguments.of(cols(new TablePrinter.Column("L", true), new TablePrinter.Column("R", false)), rows(List.of("foo", "7")), 6, new int[]{3,1}),
                Arguments.of(cols(new TablePrinter.Column("A", true), new TablePrinter.Column("B", true)), rows(List.of("x", "y")), 10, new int[]{1,1}),
                // many short left columns
                Arguments.of(cols(new TablePrinter.Column("c1", true), new TablePrinter.Column("c2", true), new TablePrinter.Column("c3", true), new TablePrinter.Column("c4", true)), rows(List.of("1", "2", "3", "4")), 10, new int[]{2,2,2,1}),
                // left column wider than header because of content ("longcontent" length 11)
                Arguments.of(cols(new TablePrinter.Column("H", true), new TablePrinter.Column("N", false)), rows(List.of("longcontent", "9")), 15, new int[]{11,1})
        );
    }
}