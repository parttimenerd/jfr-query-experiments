package me.bechberger.jfr.duckdb.util;

import java.util.Set;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class SQLUtilTest {

    @ParameterizedTest
    @CsvSource({
            "'SELECT * FROM table1, table2 WHERE table1.id = table2.id', table1|table2",
            "'SELECT a, b FROM   table3  ,   table4  , table5', table3|table4|table5",
            "'SELECT * FROM single_table, single_table, single_table', single_table",
            "'SELECT * FROM    spaced_table   ,   another_table   ', spaced_table|another_table",
            "'SELECT * FROM table_with_alias t1, another_table a2', table_with_alias|another_table",
            "'SELECT * FROM \"weird-table-name\", normal_table', weird-table-name|normal_table",
            "'SELECT * FROM (SELECT * FROM sub_table) AS subquery, main_table', sub_table|main_table",
    })
    public void testGetReferencedTables(String query, String expectedTables) {
        Set<String> result = SQLUtil.getReferencedTables(query);
        Set<String> expected = Set.of(expectedTables.split("\\|"));
        assertEquals(expected, result);
    }
}