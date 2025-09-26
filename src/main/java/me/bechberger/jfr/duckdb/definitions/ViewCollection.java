package me.bechberger.jfr.duckdb.definitions;

import org.duckdb.DuckDBConnection;
import org.jetbrains.annotations.Nullable;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static me.bechberger.jfr.duckdb.util.SQLUtil.getReferencedTables;
import static me.bechberger.jfr.duckdb.util.SQLUtil.getTableNames;

/**
 * Implement a subset of JFR views as DuckDB views.
 */
public class ViewCollection {

    public record View(String name, String category, String label, @Nullable String relatedJFRView, String definition, String... referencedTables) {
        String viewName() {
            return "jfr$" + name;
        }

        public List<String> properlyReferencedTables() {
            if (referencedTables != null && referencedTables.length > 0) {
                return Arrays.asList(referencedTables);
            } else {
                return getReferencedTables(definition).stream().toList();
            }
        }

        public boolean isValid(Set<String> existingTables) {
            return existingTables.containsAll(properlyReferencedTables());
        }
    }

    private static final View[] views = new View[] {
            new View(
                    "active-recordings",
                    "environment",
                    "Active Recordings",
                    "active-recordings",
                    """
                    CREATE VIEW "active-recordings" AS
                    SELECT
                        LAST(recordingStart) AS "Start",
                        LAST(recordingDuration) AS "Duration",
                        LAST(name) AS "Name",
                        LAST(destination) AS "Destination",
                        LAST(maxAge) AS "Max Age",
                        LAST(maxSize) AS "Max Size"
                    FROM ActiveRecording
                    GROUP BY id
                    """
            ),
            new View(
                    "active-settings",
                    "environment",
                    "Active Settings",
                    "active-settings",
                    """
                    CREATE VIEW "active-settings" AS
                    SELECT
                        EVENT_NAME_FOR_ID(id) AS "Event Type",
                        MAX(CASE WHEN name = 'enabled' THEN value END) AS "Enabled",
                        MAX(CASE WHEN name = 'threshold' THEN value END) AS "Threshold",
                        MAX(CASE WHEN name = 'stackTrace' THEN value END) AS "Stack Trace",
                        MAX(CASE WHEN name = 'period' THEN value END) AS "Period",
                        MAX(CASE WHEN name = 'cutoff' THEN value END) AS "Cutoff",
                        MAX(CASE WHEN name = 'throttle' THEN value END) AS "Throttle"
                    FROM ActiveSetting
                    GROUP BY id
                    ORDER BY "Event Type"
                    """
            ),
            new View(
                    "allocation-by-class",
                    "application",
                    "Allocation by Class",
                    "allocation-by-class",
                    """
                         CREATE VIEW "allocation-by-class" AS
                         SELECT cls.javaName as "Object Type", format_percentage(pressure) as "Allocation Pressure" FROM (SELECT
                             objectClass AS _objectType,
                             SUM(weight) / (SELECT SUM(weight) FROM ObjectAllocationSample) AS pressure
                         FROM ObjectAllocationSample
                         GROUP BY objectClass
                         ORDER BY pressure DESC
                         LIMIT 25), Class cls
                         WHERE _objectType = cls._id
                         ORDER BY pressure DESC
                    """,
                    "ObjectAllocationSample", "Class"
            ),
            /**
             * [application.allocation-by-thread]
             * label = "Allocation by Thread"
             * table = "COLUMN 'Thread', 'Allocation Pressure'
             *          FORMAT none, normalized
             *          SELECT eventThread AS T, SUM(weight) AS W
             *          FROM ObjectAllocationSample GROUP BY T ORDER BY W DESC LIMIT 25"
            */
            new View("allocation-by-thread",
                    "application",
                    "Allocation by Thread",
                    "allocation-by-thread",
                    """
                         CREATE VIEW "allocation-by-thread" AS
                         SELECT th.javaName AS "Thread", format_percentage(pressure) AS "Allocation Pressure" FROM (SELECT
                             eventThread AS _thread,
                             SUM(weight) / (SELECT SUM(weight) FROM ObjectAllocationSample) AS pressure
                         FROM ObjectAllocationSample
                         GROUP BY eventThread
                         ORDER BY pressure DESC
                         LIMIT 25), Thread th
                         WHERE _thread = th._id
                         ORDER BY pressure DESC
                    """,
                    "ObjectAllocationSample", "Thread"
            ),
            /**
             * [application.allocation-by-site]
             * label = "Allocation by Site"
             * table = "COLUMN 'Method', 'Allocation Pressure'
             *          FORMAT none, normalized
             *          SELECT stackTrace.topFrame AS S, SUM(weight) AS W
             *          FROM ObjectAllocationSample
             *          GROUP BY S
             *          ORDER BY W DESC LIMIT 25"
            */
            new View("allocation-by-site",
                    "application",
                    "Allocation by Method (disregarding line number and descriptor)",
                    "allocation-by-site",
                    """
                         CREATE VIEW "allocation-by-site" AS
                         SELECT (c.javaName || topMethod) AS "Method", format_percentage(pressure) AS "Allocation Pressure" FROM (SELECT
                             stackTrace$topClass AS topClass,
                             stackTrace$topMethod AS topMethod,
                             SUM(weight) / (SELECT SUM(weight) FROM ObjectAllocationSample) AS pressure
                         FROM ObjectAllocationSample
                         GROUP BY topClass, topMethod
                         ORDER BY pressure DESC
                         LIMIT 25
                         ), Class c
                         WHERE topClass = c._id
                         ORDER BY pressure DESC
                    """,
                    "ObjectAllocationSample", "Class"
            ),
    };

    public static List<View> getViews() {
        return List.of(views);
    }

    public static void addToDatabase(DuckDBConnection connection) throws SQLException {
        Set<String> existingTables = getTableNames(connection);
        for (View view : views) {
            if (!view.isValid(existingTables)) {
                System.err.println("Skipping view " + view.name() + " because it references missing tables: " +
                        getReferencedTables(view.definition()).stream()
                                .filter(t -> !existingTables.contains(t))
                                .collect(Collectors.joining(", ")));
                continue;
            }
            connection.createStatement().execute(view.definition());
        }
        connection.createStatement().execute("""
                CREATE TABLE IF NOT EXISTS jfr$views (
                    name VARCHAR PRIMARY KEY,
                    category VARCHAR,
                    label VARCHAR,
                    definition VARCHAR
                )
                """);
        try (var appender = connection.createAppender("jfr$views")) {
            for (View view : views) {
                appender.beginRow();
                appender.append(view.name());
                appender.append(view.category());
                appender.append(view.label());
                appender.append(view.definition());
                appender.endRow();
            }
        }
    }

    public static Map<String, List<View>> getViewsByCategory() {
        return Arrays.stream(views)
                .collect(Collectors.groupingBy(View::category));
    }

    public static View getView(String name) {
        return Arrays.stream(views)
                .filter(v -> v.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No such view: " + name));
    }
}