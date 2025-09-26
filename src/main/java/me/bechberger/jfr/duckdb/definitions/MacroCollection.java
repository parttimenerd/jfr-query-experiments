package me.bechberger.jfr.duckdb.definitions;

import org.duckdb.DuckDBConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static me.bechberger.jfr.duckdb.util.SQLUtil.getReferencedTables;
import static me.bechberger.jfr.duckdb.util.SQLUtil.getTableNames;

public class MacroCollection {

    /**
     * A SQL macro definition.
     * @param name the name of the macro
     * @param description description of the macro
     * @param sampleUsages example usages of the macro
     * @param definition SQL definition of the macro (including "CREATE MACRO ... AS ...")
     */
    public record Macro(String name, String description, String sampleUsages, String definition) {

        public boolean isValid(Set<String> availableTables) {
            return availableTables.containsAll(getReferencedTables(definition));
        }

        public String nameWithArgs() {
            String args = definition.substring(definition.indexOf('(') + 1, definition.indexOf(')'));
            return name + "(" + args + ")";
        }
    }

    private static final Macro[] macros = new Macro[] {

            // ==========================================
            // STATISTICAL FUNCTIONS AND PERCENTILES
            // ==========================================

            new Macro(
                    "P90",
                    "90th percentile of a column.",
                    "SELECT P90(duration) FROM GCPhasePause;",
                    "CREATE MACRO P90(col) AS quantile(col, 0.90)"
            ),
            new Macro(
                    "P95",
                    "95th percentile of a column.",
                    "SELECT P95(duration) FROM GCPhasePause;",
                    "CREATE MACRO P95(col) AS quantile(col, 0.95)"
            ),
            new Macro(
                    "P99",
                    "99th percentile of a column.",
                    "SELECT P99(duration) FROM GCPhasePause;",
                    "CREATE MACRO P99(col) AS quantile(col, 0.99)"
            ),
            new Macro(
                    "P999",
                    "99.9th percentile of a column.",
                    "SELECT P999(duration) FROM GCPhasePause;",
                    "CREATE MACRO P999(col) AS quantile(col, 0.999)"
            ),
            new Macro(
                    "normalized",
                    "Normalize a value to [0,1] over entire input, by comparing with max value, might have problems with LIMIT",
                    "SELECT normalized(duration) FROM GCPhasePause LIMIT 10;",
                    """
                    CREATE MACRO normalized(x) AS (
                      x / NULLIF(MAX(x) OVER(), 0)
                    )
                    """
            ),

            // ==========================================
            // JFR AGGREGATE FUNCTIONS
            // ==========================================

            new Macro(
                    "diff",
                    "Row delta: col - LAG(col) OVER(ORDER BY col).",
                    "SELECT diff(duration) FROM GCPhasePause;",
                    "CREATE MACRO diff(col) AS (col - LAG(col) OVER (ORDER BY col))"
            ),
            new Macro(
                    "JFR_UNIQUE",
                    "Count distinct values (JFR UNIQUE(x)).",
                    "SELECT COUNT_UNIQUE(eventThread) FROM JavaMonitorEnter; -- Contention analysis: GROUP BY monitorClass",
                    "CREATE MACRO COUNT_UNIQUE(x) AS count(DISTINCT x)"
            ),

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
                """
            ),

            new Macro(
                    "format_percentage",
                    "Format a number as percentage with fixed number of decimals.",
                    "SELECT format_percentage(0.123456, 2);",
                    """
                    CREATE MACRO format_percentage(num, decimals := 2) AS (
                      format_decimals(num * 100.0, decimals) || ' %'
                    )
                    """
            ),

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
                             ELSE format_decimals(abs(bytes), decimals) || ' B'
                          END)
                      END
                    )
                    """
            ),
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
                    """
            ),
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
                    """
            ),
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
                    """
            ),
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
                    """
            ),
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
                    """
            ),
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
                    """
            ),

            // ==========================================
            // JFR FIELD ACCESSORS
            // ==========================================

            new Macro(
                    "EVENT_TYPE_LABEL",
                    "Get the event label for the event table name.",
                    "SELECT EVENT_TYPE_LABEL('GarbageCollection');",
                    "CREATE MACRO EVENT_TYPE_LABEL(et) AS (SELECT label FROM EventLabels WHERE name = et LIMIT 1)"
            ),
            new Macro("EVENT_NAME_FOR_ID",
                    "Get the event table name for the event ID.",
                    "SELECT EVENT_NAME_FOR_ID(1);",
                    "CREATE MACRO EVENT_NAME_FOR_ID(_id) AS (SELECT name FROM EventIDs event WHERE id = _id LIMIT 1)"
            )
    };

    public static List<Macro> getMacros() {
        return List.of(macros);
    }

    public static void addToDatabase(DuckDBConnection connection) throws SQLException {
        Set<String> tableNames = getTableNames(connection);
        try (var stmt = connection.createStatement()) {
            for (Macro macro : macros) {
                try {
                    if (!macro.isValid(tableNames)) {
                        System.out.println("Skipping macro " + macro.name() + " because it references missing tables: " +
                                getReferencedTables(macro.definition()).stream()
                                        .filter(t -> !tableNames.contains(t))
                                        .collect(Collectors.joining(", ")));
                        continue;
                    }
                    stmt.execute(macro.definition());
                } catch (SQLException e) {
                    throw new SQLException("Error creating macro " + macro.name() + ": " + e.getMessage(), e);
                }
            }
            stmt.execute("""
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