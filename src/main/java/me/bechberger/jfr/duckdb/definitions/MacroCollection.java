package me.bechberger.jfr.duckdb.definitions;

import static me.bechberger.jfr.duckdb.util.SQLUtil.getTableNames;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import me.bechberger.jfr.duckdb.RuntimeSQLException;
import org.duckdb.DuckDBConnection;

public class MacroCollection {

    /**
     * A SQL macro definition.
     *
     * @param name the name of the macro
     * @param description description of the macro
     * @param sampleUsages example usages of the macro
     * @param definition SQL definition of the macro (including "CREATE MACRO ... AS ...")
     */
    public record Macro(
            String name,
            String description,
            String sampleUsages,
            String definition,
            String... referencedTables) {

        public boolean isValid(Set<String> availableTables) {
            return availableTables.containsAll(List.of(referencedTables));
        }

        public String nameWithArgs() {
            String args =
                    definition.substring(definition.indexOf('(') + 1, definition.indexOf(')'));
            return name + "(" + args + ")";
        }
    }

    private static final Macro[] macros =
            new Macro[] {

                // ==========================================
                // STATISTICAL FUNCTIONS AND PERCENTILES
                // ==========================================

                new Macro(
                        "P90",
                        "90th percentile of a column.",
                        "SELECT P90(duration) FROM GCPhasePause;",
                        "CREATE MACRO P90(col) AS quantile(col, 0.90)"),
                new Macro(
                        "P95",
                        "95th percentile of a column.",
                        "SELECT P95(duration) FROM GCPhasePause;",
                        "CREATE MACRO P95(col) AS quantile(col, 0.95)"),
                new Macro(
                        "P99",
                        "99th percentile of a column.",
                        "SELECT P99(duration) FROM GCPhasePause;",
                        "CREATE MACRO P99(col) AS quantile(col, 0.99)"),
                new Macro(
                        "P999",
                        "99.9th percentile of a column.",
                        "SELECT P999(duration) FROM GCPhasePause;",
                        "CREATE MACRO P999(col) AS quantile(col, 0.999)"),
                new Macro(
                        "normalized",
                        "Normalize a value to [0,1] over entire input, by comparing with max value, might have problems with LIMIT",
                        "SELECT normalized(duration) FROM GCPhasePause LIMIT 10;",
                        """
                    CREATE MACRO normalized(x) AS (
                      x / NULLIF(MAX(x) OVER(), 0)
                    )
                    """),

                // ==========================================
                // JFR AGGREGATE FUNCTIONS
                // ==========================================

                new Macro(
                        "diff",
                        "Row delta: col - LAG(col) OVER(ORDER BY col).",
                        "SELECT diff(duration) FROM GCPhasePause;",
                        "CREATE MACRO diff(col) AS (col - LAG(col) OVER (ORDER BY col))"),
                new Macro(
                        "COUNT_UNIQUE",
                        "Count distinct values (JFR UNIQUE(x)).",
                        "SELECT COUNT_UNIQUE(eventThread) FROM JavaMonitorEnter; -- Contention analysis: GROUP BY monitorClass",
                        "CREATE MACRO COUNT_UNIQUE(x) AS count(DISTINCT x)"),

                // ==========================================
                // UNIT CONVERSION FUNCTIONS
                // ==========================================
                new Macro(
                        "format_decimals",
                        "Format number with fixed number of decimals.",
                        "SELECT format_decimals(PI(), 4);",
                        """
            CREATE MACRO format_decimals(num, decimals) AS (
                CASE
                    WHEN decimals == 0 THEN FLOOR(num)::VARCHAR
                    ELSE format('{:.' || decimals || 'f}', num)
                END)
            """),
                new Macro(
                        "format_percentage",
                        "Format a number as percentage with fixed number of decimals.",
                        "SELECT format_percentage(0.123456, 2);",
                        """
                    CREATE MACRO format_percentage(num, decimals := 2) AS (
                      format_decimals(num * 100.0, decimals) || '%'
                    )
                    """),
                new Macro(
                        "format_memory",
                        "Format bytes into human readable string (B, KB, MB, GB, TB).",
                        "SELECT format_memory(123456789);",
                        """
                    CREATE MACRO format_memory(bytes, decimals := 2) AS (
                      CASE
                        WHEN bytes IS NULL THEN NULL
                        ELSE
                          (CASE WHEN bytes < 0 THEN '-' ELSE '' END) ||
                          (CASE
                             WHEN abs(bytes) >= 1099511627776 THEN format_decimals(abs(bytes)/1099511627776.0, decimals) || ' TB'
                             WHEN abs(bytes) >= 1073741824 THEN format_decimals(abs(bytes)/1073741824.0, decimals) || ' GB'
                             WHEN abs(bytes) >= 1048576 THEN format_decimals(abs(bytes)/1048576.0, decimals) || ' MB'
                             WHEN abs(bytes) >= 1024 THEN format_decimals(abs(bytes)/1024.0, decimals) || ' KB'
                             ELSE format_decimals(abs(bytes) * 1.0, decimals) || ' B'
                          END)
                      END
                    )
                    """),
                new Macro(
                        "format_human_duration",
                        "Format seconds into human readable string).",
                        "SELECT format_human_duration(60.1);",
                        """
                            CREATE MACRO format_human_duration(sec) AS (
                               CASE
                                  WHEN sec IS NULL THEN NULL
                                  ELSE
                                    (CASE WHEN sec < 0 THEN '-' ELSE '' END) ||
                                    (
                                      WITH vals AS (
                                        SELECT
                                          -- total nanoseconds (rounded)
                                          CAST(ROUND(sec * 1000000000.0) AS BIGINT) AS total_ns
                                      ),
                                      a AS (
                                        SELECT
                                          ABS(total_ns) AS ns
                                        FROM vals
                                      ),
                                      u AS (
                                        SELECT
                                          CAST(floor(ns / 86400000000000.0) AS BIGINT) AS days,
                                          CAST(floor((ns % 86400000000000.0) / 3600000000000.0) AS BIGINT) AS hours,
                                          CAST(floor((ns % 3600000000000.0) / 60000000000.0) AS BIGINT) AS minutes,
                                          CAST(floor((ns % 60000000000.0) / 1000000000.0) AS BIGINT) AS seconds,
                                          CAST(floor((ns % 1000000000.0) / 1000000.0) AS BIGINT) AS milliseconds,
                                          CAST(floor((ns % 1000000.0) / 1000.0) AS BIGINT) AS microseconds,
                                          ns % 1000 AS nanoseconds,
                                          ns AS total_ns
                                        FROM a
                                      )
                                      SELECT
                                        CASE
                                          WHEN total_ns = 0 THEN '0s'
                                          WHEN days > 0 THEN CAST(days AS VARCHAR) || 'd' || (CASE WHEN hours > 0 THEN ' ' || CAST(hours AS VARCHAR) || 'h' ELSE '' END)
                                          WHEN hours > 0 THEN CAST(hours AS VARCHAR) || 'h' || (CASE WHEN minutes > 0 THEN ' ' || CAST(minutes AS VARCHAR) || 'm' ELSE '' END)
                                          WHEN minutes > 0 THEN CAST(minutes AS VARCHAR) || 'm' || (CASE WHEN seconds > 0 THEN ' ' || CAST(seconds AS VARCHAR) || 's' ELSE '' END)
                                          WHEN seconds > 0 THEN CAST(seconds AS VARCHAR) || 's' || (CASE WHEN milliseconds > 0 THEN ' ' || CAST(milliseconds AS VARCHAR) || 'ms' ELSE '' END)
                                          WHEN milliseconds > 0 THEN CAST(milliseconds AS VARCHAR) || 'ms' || (CASE WHEN microseconds > 0 THEN ' ' || CAST(microseconds AS VARCHAR) || 'us' ELSE '' END)
                                          WHEN microseconds > 0 THEN CAST(microseconds AS VARCHAR) || 'us' || (CASE WHEN nanoseconds > 0 THEN ' ' || CAST(nanoseconds AS VARCHAR) || 'ns' ELSE '' END)
                                          ELSE CAST(nanoseconds AS VARCHAR) || 'ns'
                                        END
                                      FROM u
                                    )
                                END
                                  )
                            """),
                new Macro(
                        "format_duration",
                        "Format seconds using SI units (s, ms, us, ns) with specified decimal places. Does not go larger than seconds.",
                        "SELECT format_duration(40), format_duration(0.4), format_duration(0.0004), format_duration(0.0000004);",
                        """
                    CREATE MACRO format_duration(seconds, decimals := 2) AS (
                      CASE
                        WHEN seconds IS NULL THEN NULL
                        WHEN abs(seconds) > 1000.0 * 365 * 24 * 3600 THEN NULL  -- more than 1000 years
                        ELSE
                          (CASE WHEN seconds < 0 THEN '-' ELSE '' END) ||
                          (CASE
                             WHEN seconds = 0 THEN '0s'
                             WHEN abs(seconds) >= 1 THEN format_decimals(abs(seconds) * 1.0, decimals) || 's'
                             WHEN abs(seconds) >= 0.001 THEN format_decimals(abs(seconds) * 1000.0, decimals) || 'ms'
                             WHEN abs(seconds) >= 0.000001 THEN format_decimals(abs(seconds) * 1000000.0, decimals) || 'us'
                             ELSE format_decimals(abs(seconds) * 1000000000.0, decimals) || 'ns'
                          END)
                      END
                    )
                    """),
                new Macro(
                        "format_hex",
                        "Format integer as hex string (with 0x prefix).",
                        "SELECT format_hex(255), format_hex(-255);",
                        "CREATE MACRO format_hex(i) AS format('0x{:x}', i)"),

                // ==========================================
                // GARBAGE COLLECTION ANALYSIS
                // ==========================================

                new Macro(
                        "before_gc",
                        "GC id of the last GC before the event, or -1. Slow on large tables.",
                        "SELECT before_gc(startTime) FROM ExecutionSample LIMIT 10;",
                        """
                    CREATE MACRO before_gc(ts) AS (
                      COALESCE(
                        (SELECT gcId
                         FROM GarbageCollection
                         WHERE startTime <= ts
                         ORDER BY startTime DESC
                         LIMIT 1),
                        -1
                      )
                    )
                    """,
                        "GarbageCollection"),
                new Macro(
                        "after_gc",
                        "GC id of the first GC after the event, or -1. Slow on large tables.",
                        "SELECT after_gc(startTime) FROM ExecutionSample LIMIT 10;",
                        """
                    CREATE MACRO after_gc(ts) AS (
                      COALESCE(
                        (SELECT gcId
                         FROM GarbageCollection
                         WHERE startTime > ts
                         ORDER BY startTime ASC
                         LIMIT 1),
                        -1
                      )
                    )
                    """,
                        "GarbageCollection"),
                new Macro(
                        "duration_since_last_gc",
                        "Duration since the last GC before the event, or -1.",
                        "SELECT duration_since_last_gc(startTime) FROM ExecutionSample LIMIT 10;",
                        """
                    CREATE MACRO duration_since_last_gc(ts) AS (
                      COALESCE(
                        (SELECT ts - startTime
                         FROM GarbageCollection
                         WHERE startTime <= ts
                         ORDER BY startTime DESC
                         LIMIT 1),
                        -1
                      )
                    )
                    """,
                        "GarbageCollection"),
                new Macro(
                        "HEAP_BEFORE_GC",
                        "Get heap usage before GC for a given GC ID.",
                        "SELECT gcId, HEAP_BEFORE_GC(gcId) as heap_before FROM GarbageCollection;",
                        """
                    CREATE MACRO HEAP_BEFORE_GC(gc_id) AS (
                      (SELECT heapUsed
                       FROM GCHeapSummary
                       WHERE gcId = gc_id AND "when" = 'Before GC'
                       LIMIT 1)
                    )
                    """,
                        "GCHeapSummary"),
                new Macro(
                        "HEAP_AFTER_GC",
                        "Get heap usage after GC for a given GC ID.",
                        "SELECT gcId, HEAP_AFTER_GC(gcId) as heap_after FROM GarbageCollection;",
                        """
                    CREATE MACRO HEAP_AFTER_GC(gc_id) AS (
                      (SELECT heapUsed
                       FROM GCHeapSummary
                       WHERE gcId = gc_id AND "when" = 'After GC'
                       LIMIT 1)
                    )
                    """,
                        "GCHeapSummary"),
                new Macro(
                        "GC_TYPE",
                        "Get GC type for a given GC ID (Young/Old/Unknown).",
                        "SELECT gcId, GC_TYPE(gcId) as gc_type FROM GarbageCollection;",
                        """
                    CREATE MACRO GC_TYPE(gc_id) AS (
                      COALESCE(
                        (SELECT 'Young' FROM YoungGarbageCollection WHERE gcId = gc_id LIMIT 1),
                        (SELECT 'Old' FROM OldGarbageCollection WHERE gcId = gc_id LIMIT 1),
                        'Unknown'
                      )
                    )
                    """,
                        "YoungGarbageCollection",
                        "OldGarbageCollection"),

                // ==========================================
                // JFR FIELD ACCESSORS
                // ==========================================

                new Macro(
                        "EVENT_TYPE_LABEL",
                        "Get the event label for the event table name.",
                        "SELECT EVENT_TYPE_LABEL('GarbageCollection');",
                        "CREATE MACRO EVENT_TYPE_LABEL(et) AS (SELECT label FROM EventLabels WHERE name = et LIMIT 1)",
                        "EventLabels"),
                new Macro(
                        "EVENT_NAME_FOR_ID",
                        "Get the event table name for the event ID.",
                        "SELECT EVENT_NAME_FOR_ID(1);",
                        "CREATE MACRO EVENT_NAME_FOR_ID(_id) AS (SELECT name FROM EventIDs event WHERE id = _id LIMIT 1)",
                        "EventIDs"),
                // ============================================
                // Misc database utility functions
                // ============================================
                new Macro(
                        "macro_sql",
                        "Get the SQL definition of a macro by name.",
                        "SELECT macro_sql('P90');",
                        "CREATE MACRO macro_sql(macro_name) AS (SELECT macro_definition FROM duckdb_functions() WHERE function_name = macro_name AND function_type = 'macro' AND NOT internal LIMIT 1)"),
                new Macro(
                        "view_sql",
                        "Get the SQL definition of a view by name.",
                        "SELECT view_sql('SomeView');",
                        "CREATE MACRO view_sql(name) AS (SELECT sql FROM duckdb_views() WHERE view_name = name LIMIT 1)")
            };

    public static List<Macro> getMacros() {
        return List.of(macros);
    }

    public static void addToDatabase(DuckDBConnection connection) throws SQLException {
        Set<String> tableNames = getTableNames(connection);
        // remove all existing macros
        try (ResultSet rs =
                connection
                        .createStatement()
                        .executeQuery(
                                "SELECT function_name FROM duckdb_functions() WHERE function_type = 'macro' and not internal")) {
            while (rs.next()) {
                String macroName = rs.getString(1);
                try (var dropStmt = connection.createStatement()) {
                    dropStmt.execute("DROP MACRO IF EXISTS " + macroName + ";");
                } catch (SQLException e) {
                    throw new RuntimeSQLException(
                            "Error dropping existing macro " + macroName + ": " + e.getMessage(),
                            e);
                }
            }
        }
        try (var stmt = connection.createStatement()) {
            for (Macro macro : macros) {
                try {
                    if (!macro.isValid(tableNames)) {
                        System.out.println(
                                "Skipping macro "
                                        + macro.name()
                                        + " because it references missing tables: "
                                        + Arrays.stream(macro.referencedTables)
                                                .filter(t -> !tableNames.contains(t))
                                                .collect(Collectors.joining(", ")));
                        continue;
                    }
                    stmt.execute(macro.definition());
                    String description =
                            macro.description() + "\nExample usage:\n" + macro.sampleUsages();
                    stmt.execute(
                            "COMMENT ON MACRO "
                                    + macro.name()
                                    + " IS "
                                    + stmt.enquoteLiteral(description)
                                    + ";");
                    // System.out.println("Created macro " + macro.nameWithArgs() + ": " +
                    // macro.description());
                } catch (SQLException e) {
                    throw new RuntimeSQLException(
                            "Error creating macro " + macro.name() + ": " + e.getMessage(), e);
                }
            }
            stmt.execute(
                    """
                CREATE TABLE IF NOT EXISTS macros (
                  name VARCHAR,
                  description VARCHAR,
                  sampleUsage VARCHAR
                );
                """);
            stmt.execute("DELETE FROM macros;");
        }
        try (var appender = connection.createAppender("macros")) {
            for (Macro macro : macros) {
                appender.beginRow();
                appender.append(macro.name());
                appender.append(macro.description());
                appender.append(macro.sampleUsages());
                appender.endRow();
            }
        }
    }
}
