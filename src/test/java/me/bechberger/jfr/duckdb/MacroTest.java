package me.bechberger.jfr.duckdb;

import org.assertj.db.type.AssertDbConnection;
import org.assertj.db.type.AssertDbConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;

import static org.assertj.db.api.Assertions.assertThat;

public class MacroTest {

    private static Path tmpDBFile = null;
    private static AssertDbConnection conn;

    @BeforeEach
    public void setup() throws IOException, SQLException {
        tmpDBFile = Files.createTempFile("duckdb_test", ".db");
        // make sure the file is empty
        Files.deleteIfExists(tmpDBFile);
        // import the jfr_files/renaissance-dotty_default_G1.jfr file
        Path jfrFile = Path.of("jfr_files/default.jfr");
        if (!Files.exists(jfrFile)) {
            throw new IOException("Test file not found: " + jfrFile.toAbsolutePath());
        }
        BasicParallelImporter.createFile(jfrFile, tmpDBFile.toAbsolutePath(), new Options());
        conn = AssertDbConnectionFactory.of("jdbc:duckdb:" + tmpDBFile.toAbsolutePath(), "", "").create();
    }

    @AfterEach
    public void teardown() throws IOException {
        if (tmpDBFile != null) {
            Files.deleteIfExists(tmpDBFile);
            tmpDBFile = null;
        }
    }

    @Test
    public void testP90Macro() throws SQLException {
        var table = conn.request("SELECT p90(value) AS p90 FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)) AS T(value)").build();
        assertThat(table).hasNumberOfRows(1);
        assertThat(table).row(0)
                .value("p90").isEqualTo(9);
        System.out.println(table.getRow(0).toString());
    }

    @Test
    public void testNormalized() {
        var table = conn.request("""
                SELECT value,
                      normalized(value) AS normalized
                FROM (VALUES (1), (2), (3), (4)) AS T(value)
                ORDER BY value
                """).build();
        assertThat(table).hasNumberOfRows(4);
        for (int i = 0; i < 4; i++) {
            assertThat(table).row(i)
                    .value("value").isEqualTo(i + 1)
                    .value("normalized").isEqualTo((i + 1) / 4.0);
        }
    }

    @Test
    public void testDiff() {
        var table = conn.request("""
                SELECT value,
                       diff(value) AS diff
                FROM (VALUES (1), (2), (4)) AS T(value)
                ORDER BY value
                """).build();
        assertThat(table).hasNumberOfRows(3);
        assertThat(table).row(0)
                .value("value").isEqualTo(1)
                .value("diff").isNull();
        assertThat(table).row(1)
                .value("value").isEqualTo(2)
                .value("diff").isEqualTo(1);
        assertThat(table).row(2)
                .value("value").isEqualTo(4)
                .value("diff").isEqualTo(2);
    }

    @Test
    public void testCountUnique() {
        var table = conn.request("""
                SELECT value,
                       count_unique(value) AS count_unique
                FROM (VALUES (1), (2), (2), (3), (3), (3)) AS T(value)
                GROUP BY value
                ORDER BY value
                """).build();
        assertThat(table).hasNumberOfRows(3);
        assertThat(table).row(0)
                .value("value").isEqualTo(1)
                .value("count_unique").isEqualTo(1);
        assertThat(table).row(1)
                .value("value").isEqualTo(2)
                .value("count_unique").isEqualTo(1);
        assertThat(table).row(2)
                .value("value").isEqualTo(3)
                .value("count_unique").isEqualTo(1);
    }

    @Test
    public void testFormatMemory() {
        var table = conn.request("""
                SELECT value,
                       format_memory(value) AS formatted,
                       format_memory(value, decimals := 0) AS formatted_0
                FROM (VALUES (1023), (1024), (1024 * 1.5), (1024 * 1024)) AS T(value)
                ORDER BY value
                """).build();
        assertThat(table).hasNumberOfRows(4);
        assertThat(table).row(0)
                .value("formatted").isEqualTo("1023.00 B")
                .value("formatted_0").isEqualTo("1023 B");
        assertThat(table).row(1)
                .value("formatted").isEqualTo("1.00 KB");
        assertThat(table).row(2)
                .value("formatted").isEqualTo("1.50 KB");
        assertThat(table).row(3)
                .value("formatted").isEqualTo("1.00 MB");
    }

    @Test
    public void testFormatHumanDuration() {
        var table = conn.request("""
                SELECT value,
                       format_human_duration(value) AS formatted
                FROM (VALUES (0.999), (1), (1.5)) AS T(value)
                ORDER BY value
                """).build();
        assertThat(table).hasNumberOfRows(3);
        assertThat(table).row(0)
                .value("formatted").isEqualTo("999ms");
        assertThat(table).row(1)
                .value("formatted").isEqualTo("1s");
        assertThat(table).row(2)
                .value("formatted").isEqualTo("1s 500ms");
    }

    @Test
    public void testFormatDuration() {
        var table = conn.request("""
                SELECT value,
                       format_duration(value) AS formatted
                FROM (VALUES (0.999), (1), (1.5), (60), (0)) AS T(value)
                ORDER BY value
                """).build();
        assertThat(table).hasNumberOfRows(5);
        assertThat(table).row(0)
                .value("formatted").isEqualTo("0s");
        assertThat(table).row(1)
                .value("formatted").isEqualTo("999.00ms");
        assertThat(table).row(2)
                .value("formatted").isEqualTo("1.00s");
        assertThat(table).row(3)
                .value("formatted").isEqualTo("1.50s");
        assertThat(table).row(4)
                .value("formatted").isEqualTo("60.00s");
    }
}