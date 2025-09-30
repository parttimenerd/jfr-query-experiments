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
            new View("class-loaders",
                    "application",
                    "Class Loaders",
                    "class-loaders",
                    """
                         CREATE VIEW "class-loaders" AS
                         SELECT
                             cl.javaName AS "Class Loader",
                             LAST(hiddenClassCount) AS "Hidden Classes",
                             LAST(classCount) AS "Classes"
                         FROM ClassLoaderStatistics cls
                         LEFT JOIN ClassLoader cl ON cls.classLoader = cl._id
                         GROUP BY classLoader, cl.javaName
                         ORDER BY "Classes" DESC
                    """
            ),
            new View("class-modifications", // TODO: test
                    "jvm",
                    "Class Modifications",
                    "class-modifications",
                    """
                         CREATE VIEW "class-modifications" AS
                         SELECT
                             duration AS "Time",
                             (c.javaName || combined.stackTrace$topApplicationMethod) AS "Requested By",
                             CASE
                                 WHEN eventType = 'redefine' THEN 'Redefine'
                                 WHEN eventType = 'retransform' THEN 'Retransform'
                                 ELSE eventType
                             END AS "Operation",
                             classCount AS "Classes"
                         FROM (
                             SELECT
                                 id AS redefinitionId,
                                 'redefine' AS eventType,
                                 duration,
                                 stackTrace$topApplicationClass,
                                 stackTrace$topApplicationMethod,
                                 classCount
                             FROM RedefineClasses
                             UNION ALL
                             SELECT
                                 id AS redefinitionId,
                                 'retransform' AS eventType,
                                 duration,
                                 stackTrace$topApplicationClass,
                                 stackTrace$topApplicationMethod,
                                 classCount
                             FROM RetransformClasses
                         ) AS combined
                         LEFT JOIN Class c on c._id = combined.topApplicationFrame$topClass
                         ORDER BY duration DESC
                    """,
                    "RedefineClasses", "RetransformClasses", "Class"
            ),
            new View("compiler-configuration",
                    "jvm",
                    "Compiler Configuration",
                    "compiler-configuration",
                    """
                         CREATE VIEW "compiler-configuration" AS
                         SELECT
                             LAST(threadCount) AS "Compiler Threads",
                             LAST(dynamicCompilerThreadCount) AS "Dynamic Compiler Threads",
                             LAST(tieredCompilation) AS "Tiered Compilation"
                         FROM CompilerConfiguration
                    """
            ),
            new View("compiler-statistics",
                    "jvm",
                    "Compiler Statistics",
                    "compiler-statistics",
                    """
                         CREATE VIEW "compiler-statistics" AS
                         SELECT
                             LAST(compileCount) AS "Compiled Methods",
                             format_duration(LAST(peakTimeSpent)) AS "Peak Time",
                             format_duration(LAST(totalTimeSpent)) AS "Total Time",
                             LAST(bailoutCount) AS "Bailouts",
                             LAST(osrCompileCount) AS "OSR Compilations",
                             LAST(standardCompileCount) AS "Standard Compilations",
                             format_memory(LAST(osrBytesCompiled)) AS "OSR Bytes Compiled",
                             format_memory(LAST(standardBytesCompiled)) AS "Standard Bytes Compiled",
                             format_memory(LAST(nmethodsSize)) AS "Compilation Resulting Size",
                             format_memory(LAST(nmethodCodeSize)) AS "Compilation Resulting Code Size"
                         FROM CompilerStatistics
                    """
            ),
            /**
             * [jvm.compiler-phases]
             * label = "Concurrent Compiler Phases"
             * table = "COLUMN 'Level', 'Phase', 'Average',
             *                 'P95', 'Longest', 'Count',
             *                 'Total'
             *          SELECT phaseLevel AS L, phase AS P, AVG(duration),
             *                 P95(duration),  MAX(duration), COUNT(*),
             *                 SUM(duration) AS S
             *          FROM CompilerPhase
             *          GROUP BY P ORDER BY L ASC, S DESC"
            */
            new View("compiler-phases", // TODO: test
                    "jvm",
                    "Concurrent Compiler Phases",
                    "compiler-phases",
                    """
                         CREATE VIEW "compiler-phases" AS
                         SELECT
                             phaseLevel AS "Level",
                             phase AS "Phase",
                             format_duration(AVG(duration)) AS "Average",
                             format_duration(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration)) AS "P95",
                             format_duration(MAX(duration)) AS "Longest",
                             COUNT(*) AS "Count",
                             format_duration(SUM(duration)) AS "Total"
                         FROM CompilerPhase
                         GROUP BY phase, phaseLevel
                         ORDER BY phaseLevel ASC, SUM(duration) DESC
                    """),
            /**
             * [environment.container-configuration]
             * label = "Container Configuration"
             * form = "SELECT LAST(containerType), LAST(cpuSlicePeriod), LAST(cpuQuota), LAST(cpuShares),
             *                LAST(effectiveCpuCount), LAST(memorySoftLimit), LAST(memoryLimit),
             *                LAST(swapMemoryLimit), LAST(hostTotalMemory)
             *                FROM ContainerConfiguration"
            */
            new View("container-configuration", // TODO: test
                    "environment",
                    "Container Configuration",
                    "container-configuration",
                    """
                         CREATE VIEW "container-configuration" AS
                         SELECT
                             LAST(containerType) AS "Container Type",
                             LAST(cpuSlicePeriod) AS "CPU Slice Period",
                             LAST(cpuQuota) AS "CPU Quota",
                             LAST(cpuShares) AS "CPU Shares",
                             LAST(effectiveCpuCount) AS "Effective CPU Count",
                             format_memory(LAST(memorySoftLimit)) AS "Memory Soft Limit",
                             format_memory(LAST(memoryLimit)) AS "Memory Limit",
                             format_memory(LAST(swapMemoryLimit)) AS "Swap Memory Limit",
                             format_memory(LAST(hostTotalMemory)) AS "Host Total Memory"
                         FROM ContainerConfiguration
                    """
            ),
            /**
             * [environment.container-cpu-usage]
             * label = "Container CPU Usage"
             * form = "SELECT LAST(cpuTime), LAST(cpuUserTime), LAST(cpuSystemTime) FROM ContainerCPUUsage"
            */
            new View("container-cpu-usage", // TODO: test
                    "environment",
                    "Container CPU Usage",
                    "container-cpu-usage",
                    """
                         CREATE VIEW "container-cpu-usage" AS
                         SELECT
                             LAST(cpuTime) AS "CPU Time",
                             LAST(cpuUserTime) AS "CPU User Time",
                             LAST(cpuSystemTime) AS "CPU System Time"
                         FROM ContainerCPUUsage
                    """
            ),
            /**
             * [environment.container-memory-usage]
             * label = "Container Memory Usage"
             * form = "SELECT LAST(memoryFailCount), LAST(memoryUsage), LAST(swapMemoryUsage) FROM ContainerMemoryUsage"
            */
            new View("container-memory-usage", // TODO: test
                    "environment",
                    "Container Memory Usage",
                    "container-memory-usage",
                    """
                         CREATE VIEW "container-memory-usage" AS
                         SELECT
                             LAST(memoryFailCount) AS "Memory Fail Count",
                             format_memory(LAST(memoryUsage)) AS "Memory Usage",
                             format_memory(LAST(swapMemoryUsage)) AS "Swap Memory Usage"
                         FROM ContainerMemoryUsage
                    """
            ),
            /**
             * [environment.container-io-usage]
             * label = "Container I/O Usage"
             * form = "SELECT LAST(serviceRequests), LAST(dataTransferred) FROM ContainerIOUsage"
            */
            new View("container-io-usage", // TODO: test
                    "environment",
                    "Container I/O Usage",
                    "container-io-usage",
                    """
                         CREATE VIEW "container-io-usage" AS
                         SELECT
                             LAST(serviceRequests) AS "Service Requests",
                             format_memory(LAST(dataTransferred)) AS "Data Transferred"
                         FROM ContainerIOUsage
                    """
            ),
            /**
             * [environment.container-cpu-throttling]
             * label = "Container CPU Throttling"
             * form = "SELECT LAST(cpuElapsedSlices), LAST(cpuThrottledSlices), LAST(cpuThrottledTime) FROM ContainerCPUThrottling"
            */
            new View("container-cpu-throttling", // TODO: test
                    "environment",
                    "Container CPU Throttling",
                    "container-cpu-throttling",
                    """
                         CREATE VIEW "container-cpu-throttling" AS
                         SELECT
                             LAST(cpuElapsedSlices) AS "CPU Elapsed Slices",
                             LAST(cpuThrottledSlices) AS "CPU Throttled Slices",
                             format_duration(LAST(cpuThrottledTime)) AS "CPU Throttled Time"
                         FROM ContainerCPUThrottling
                    """
            ),
            /**
             * [application.contention-by-thread]
             * label = "Contention by Thread"
             * table = "COLUMN 'Thread', 'Count', 'Avg', 'P90', 'Max.'
             *          SELECT eventThread, COUNT(*), AVG(duration), P90(duration), MAX(duration) AS M
             *          FROM JavaMonitorEnter GROUP BY eventThread ORDER BY M"
            */
            new View("contention-by-thread", // TODO: test
                    "application",
                    "Contention by Thread",
                    "contention-by-thread",
                    """
                         CREATE VIEW "contention-by-thread" AS
                         SELECT
                             th.javaName AS "Thread",
                             COUNT(*) AS "Count",
                             format_duration(AVG(duration)) AS "Avg",
                             format_duration(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY duration)) AS "P90",
                             format_duration(MAX(duration)) AS "Max."
                         FROM JavaMonitorEnter jme
                         JOIN Thread th ON jme.eventThread = th._id
                         GROUP BY eventThread, th.javaName
                         ORDER BY MAX(duration) DESC
                    """,
                    "JavaMonitorEnter", "Thread"
            )
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