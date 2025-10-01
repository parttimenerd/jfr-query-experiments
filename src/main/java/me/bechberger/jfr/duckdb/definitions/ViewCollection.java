/**
 *
 * Copyright (c) 2025, SAP SE or an SAP affiliate company. All rights reserved.
 * Copyright (c) 2023, Oracle and/or its affiliates. All rights reserved.
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
package me.bechberger.jfr.duckdb.definitions;

import me.bechberger.jfr.duckdb.RuntimeSQLException;
import org.duckdb.DuckDBConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static me.bechberger.jfr.duckdb.util.SQLUtil.getReferencedTables;
import static me.bechberger.jfr.duckdb.util.SQLUtil.getTableNames;

/**
 * Implement all JFR views as defined in the OpenJDK (till 25. September 2025).
 */
public class ViewCollection {

    private static final View[] views = new View[]{
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
            new View("class-modifications",
                    "jvm",
                    "Class Modifications",
                    "class-modifications",
                    """
                                 CREATE VIEW "class-modifications" AS
                                 SELECT
                                     format_duration(duration) AS "Time",
                                     (c.javaName || combined.stackTrace$topApplicationMethod) AS "Requested By",
                                     CASE
                                         WHEN eventType = 'redefine' THEN 'Redefine Classes'
                                         WHEN eventType = 'retransform' THEN 'Retransform Classes'
                                         ELSE eventType
                                     END AS "Operation",
                                     classCount AS "Classes"
                                 FROM (
                                     SELECT
                                         'redefine' AS eventType,
                                         duration,
                                         stackTrace$topApplicationClass,
                                         stackTrace$topApplicationMethod,
                                         classCount
                                     FROM RedefineClasses
                                     UNION ALL
                                     SELECT
                                         'retransform' AS eventType,
                                         duration,
                                         stackTrace$topApplicationClass,
                                         stackTrace$topApplicationMethod,
                                         classCount
                                     FROM RetransformClasses
                                 ) AS combined
                                 LEFT JOIN Class c on c._id = stackTrace$topApplicationClass
                                 ORDER BY duration DESC
                            """,
                    "RedefineClasses", "RetransformClasses", "Class"
            ).addUnionAlternatives(),
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
            new View("compiler-phases",
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
            new View("container-configuration",
                    "environment",
                    "Container Configuration",
                    "container-configuration",
                    """
                                 CREATE VIEW "container-configuration" AS
                                 SELECT
                                     LAST(containerType) AS "Container Type",
                                     format_duration(LAST(cpuSlicePeriod)) AS "CPU Slice Period",
                                     format_duration(LAST(cpuQuota)) AS "CPU Quota",
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
            new View("container-cpu-usage",
                    "environment",
                    "Container CPU Usage",
                    "container-cpu-usage",
                    """
                                 CREATE VIEW "container-cpu-usage" AS
                                 SELECT
                                     format_duration(LAST(cpuTime)) AS "CPU Time",
                                     format_duration(LAST(cpuUserTime)) AS "CPU User Time",
                                     format_duration(LAST(cpuSystemTime)) AS "CPU System Time"
                                 FROM ContainerCPUUsage
                            """
            ),
            /**
             * [environment.container-memory-usage]
             * label = "Container Memory Usage"
             * form = "SELECT LAST(memoryFailCount), LAST(memoryUsage), LAST(swapMemoryUsage) FROM ContainerMemoryUsage"
            */
            new View("container-memory-usage",
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
            new View("container-io-usage",
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
            new View("container-cpu-throttling",
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
            new View("contention-by-thread",
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
            ),
            /**
             * [application.contention-by-class]
             * label = "Contention by Lock Class"
             * table = "COLUMN 'Lock Class', 'Count', 'Avg.', 'P90', 'Max.'
             *          SELECT monitorClass, COUNT(*), AVG(duration), P90(duration), MAX(duration) AS M
             *          FROM JavaMonitorEnter GROUP BY monitorClass ORDER BY M"
            */
            new View("contention-by-class",
                    "application",
                    "Contention by Lock Class",
                    "contention-by-class",
                    """
                                 CREATE VIEW "contention-by-class" AS
                                 SELECT
                                     c.javaName AS "Lock Class",
                                     COUNT(*) AS "Count",
                                     format_duration(AVG(duration)) AS "Avg.",
                                     format_duration(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY duration)) AS "P90",
                                     format_duration(MAX(duration)) AS "Max."
                                 FROM JavaMonitorEnter jme
                                 JOIN Class c ON jme.monitorClass = c._id
                                 GROUP BY monitorClass, c.javaName
                                 ORDER BY MAX(duration) DESC
                            """,
                    "JavaMonitorEnter", "Class"
            ),
            /**
             * [application.contention-by-site]
             * label = "Contention by Site"
             * table = "COLUMN 'StackTrace', 'Count', 'Avg.', 'Max.'
             *          SELECT stackTrace AS S, COUNT(*), AVG(duration), MAX(duration) AS M
             *          FROM JavaMonitorEnter GROUP BY S ORDER BY M"
            */
            new View("contention-by-site",
                    "application",
                    "Contention by Site",
                    "contention-by-site",
                    """
                                 CREATE VIEW "contention-by-site" AS
                                 SELECT
                                     (c.javaName || '.' || jme.stackTrace$topMethod) AS "StackTrace",
                                     COUNT(*) AS "Count",
                                     format_duration(AVG(duration)) AS "Avg.",
                                     format_duration(MAX(duration)) AS "Max."
                                 FROM JavaMonitorEnter jme
                                 JOIN Class c ON jme.stackTrace$topClass = c._id
                                 GROUP BY c.javaName, jme.stackTrace$topMethod
                                 ORDER BY MAX(duration) DESC
                            """,
                    "JavaMonitorEnter", "Class"
            ),
            /**
             * [application.contention-by-address]
             * label = "Contention by Monitor Address"
             * table = "COLUMN 'Monitor Address', 'Class', 'Threads', 'Max Duration'
             *          SELECT address, FIRST(monitorClass), UNIQUE(*), MAX(duration) AS M
             *          FROM JavaMonitorEnter
             *          GROUP BY monitorClass ORDER BY M"
            */
            new View("contention-by-address",
                    "application",
                    "Contention by Monitor Address",
                    "contention-by-address",
                    """
                                 CREATE VIEW "contention-by-address" AS
                                 SELECT
                                     format_hex(jme.address) AS "Monitor Address",
                                     c.javaName AS "Class",
                                     COUNT(DISTINCT eventThread) AS "Threads",
                                     format_duration(MAX(duration)) AS "Max Duration"
                                 FROM JavaMonitorEnter jme
                                 JOIN Class c ON jme.monitorClass = c._id
                                 GROUP BY jme.address, c.javaName
                                 ORDER BY MAX(duration) DESC
                            """,
                    "JavaMonitorEnter", "Class"
            ),
            /**
             *
            [environment.cpu-information]
            label ="CPU Information"
            form = "SELECT cpu, sockets, cores, hwThreads, description FROM CPUInformation"
            */
            new View("cpu-information",
                    "environment",
                    "CPU Information",
                    "cpu-information",
                    """
                                 CREATE VIEW "cpu-information" AS
                                 SELECT
                                     cpu AS "CPU",
                                     sockets AS "Sockets",
                                     cores AS "Cores",
                                     hwThreads AS "Hardware Threads",
                                     description AS "Description"
                                 FROM CPUInformation
                                 GROUP BY cpu, sockets, cores, hwThreads, description
                            """
            ),
            /**
             * [environment.cpu-load]
             * label = "CPU Load Statistics"
             * form = "COLUMN
             *         'JVM User (Minimum)',
             *         'JVM User (Average)',
             *         'JVM User (Maximum)',
             *         'JVM System (Minimum)',
             *         'JVM System (Average)',
             *         'JVM System (Maximum)',
             *         'Machine Total (Minimum)',
             *         'Machine Total (Average)',
             *         'Machine Total (Maximum)'
             *         SELECT MIN(jvmUser), AVG(jvmUser), MAX(jvmUser),
             *                MIN(jvmSystem), AVG(jvmSystem), MAX(jvmSystem),
             *                MIN(machineTotal), AVG(machineTotal), MAX(machineTotal)
             *                FROM CPULoad"
            */
            new View("cpu-load",
                    "environment",
                    "CPU Load Statistics",
                    "cpu-load",
                    """
                                 CREATE VIEW "cpu-load" AS
                                 SELECT
                                     format_percentage(MIN(jvmUser)) AS "JVM User (Minimum)",
                                     format_percentage(AVG(jvmUser)) AS "JVM User (Average)",
                                     format_percentage(MAX(jvmUser)) AS "JVM User (Maximum)",
                                     format_percentage(MIN(jvmSystem)) AS "JVM System (Minimum)",
                                     format_percentage(AVG(jvmSystem)) AS "JVM System (Average)",
                                     format_percentage(MAX(jvmSystem)) AS "JVM System (Maximum)",
                                     format_percentage(MIN(machineTotal)) AS "Machine Total (Minimum)",
                                     format_percentage(AVG(machineTotal)) AS "Machine Total (Average)",
                                     format_percentage(MAX(machineTotal)) AS "Machine Total (Maximum)"
                                 FROM CPULoad
                            """
            ),
            /**
             * [environment.cpu-load-samples]
             * label = "CPU Load"
             * table = "SELECT startTime, jvmUser, jvmSystem, machineTotal FROM CPULoad"
            */
            new View("cpu-load-samples",
                    "environment",
                    "CPU Load",
                    "cpu-load-samples",
                    """
                                 CREATE VIEW "cpu-load-samples" AS
                                 SELECT
                                     startTime AS "Time",
                                     format_percentage(jvmUser) AS "JVM User",
                                     format_percentage(jvmSystem) AS "JVM System",
                                     format_percentage(machineTotal) AS "Machine Total"
                                 FROM CPULoad
                                 ORDER BY startTime
                            """
            ),
            /*
            [environment.cpu-tsc]
label ="CPU Time Stamp Counter"
form = "SELECT LAST(fastTimeAutoEnabled), LAST(fastTimeEnabled),
               LAST(fastTimeFrequency), LAST(osFrequency)
        FROM CPUTimeStampCounter"
             */
            new View("cpu-tsc",
                    "environment",
                    "CPU Time Stamp Counter",
                    "cpu-tsc",
                    """
                                 CREATE VIEW "cpu-tsc" AS
                                 SELECT
                                     LAST(fastTimeAutoEnabled) AS "Fast Time Auto Enabled",
                                     LAST(fastTimeEnabled) AS "Fast Time Enabled",
                                     LAST(fastTimeFrequency) || ' Hz' AS "Fast Time Frequency",
                                     LAST(osFrequency) || ' Hz' AS "OS Frequency"
                                 FROM CPUTimeStampCounter
                            """),
            /**
             * [jvm.deoptimizations-by-reason]
             * label = "Deoptimization by Reason"
             * table = "SELECT reason, COUNT(reason) AS C
             *          FROM Deoptimization GROUP BY reason ORDER BY C DESC"
            */
            new View("deoptimizations-by-reason",
                    "jvm",
                    "Deoptimization by Reason",
                    "deoptimizations-by-reason",
                    """
                                 CREATE VIEW "deoptimizations-by-reason" AS
                                 SELECT
                                     reason AS "Reason",
                                     COUNT(reason) AS "Count"
                                 FROM Deoptimization
                                 GROUP BY reason
                                 ORDER BY COUNT(reason) DESC
                            """
            ),
            /**
             * [jvm.deoptimizations-by-site]
             * label = "Deoptimization by Site"
             * table = "SELECT method, lineNumber, bci, COUNT(reason) AS C
             *          FROM Deoptimization GROUP BY method ORDER BY C DESC"
            */
            new View("deoptimizations-by-site",
                    "jvm",
                    "Deoptimization by Site",
                    "deoptimizations-by-site",
                    """
                                 CREATE VIEW "deoptimizations-by-site" AS
                                 SELECT
                                     (c.javaName || '.' || m.name || m.descriptor) AS "Method",
                                     d.lineNumber AS "Line Number",
                                     d.bci AS "BCI",
                                     COUNT(d.reason) AS "Count"
                                 FROM Deoptimization d
                                 JOIN Method m ON d.method = m._id
                                 JOIN Class c ON m.type = c._id
                                 GROUP BY d.method, d.lineNumber, d.bci, c.javaName, m.name, m.descriptor
                                 ORDER BY COUNT(d.reason) DESC
                            """,
                    "Deoptimization", "Class"
            ),
            /**
             * [environment.events-by-count]
             * label = "Event Types by Count"
             * table = "SELECT eventType.label AS E, COUNT(*) AS C FROM * GROUP BY E ORDER BY C DESC"
            */
            new View("events-by-count",
                    "environment",
                    "Event Types by Count",
                    "events-by-count",
                    """
                                 CREATE VIEW "events-by-count" AS
                                 SELECT
                                     label AS "Event Label",
                                     count AS "Count"
                                 FROM Events
                                 JOIN EventLabels ON Events.name = EventLabels.name
                                 ORDER BY count DESC
                                 """),
            /**
            [environment.events-by-name]
            label = "Event Types by Name"
            table = "SELECT eventType.label AS E, COUNT(*) AS C FROM * GROUP BY E ORDER BY E ASC"
            */
            new View("events-by-name",
                    "environment",
                    "Event Types by Name",
                    "events-by-name",
                    """
                                 CREATE VIEW "events-by-name" AS
                                 SELECT
                                     label AS "Event Label",
                                     count AS "Count"
                                 FROM Events
                                 JOIN EventLabels ON Events.name = EventLabels.name
                                 ORDER BY Events.name ASC
                            """),
            /**
             * [environment.environment-variables]
             * label = "Environment Variables"
             * table = "FORMAT none, cell-height:20
             *          SELECT LAST(key) AS K, LAST(value)
             *          FROM InitialEnvironmentVariable GROUP BY key ORDER BY K"
            */
            new View("environment-variables",
                    "environment",
                    "Environment Variables",
                    "environment-variables",
                    """
                                 CREATE VIEW "environment-variables" AS
                                 SELECT
                                     key AS "Key",
                                     value AS "Value"
                                 FROM InitialEnvironmentVariable
                                 GROUP BY key, value
                                 ORDER BY key
                            """
            ),
            /**

            [application.exception-count]
            label ="Exception Statistics"
            form = "COLUMN 'Exceptions Thrown' SELECT DIFF(throwables) FROM ExceptionStatistics"

            */
            new View("exception-count",
                    "application",
                    "Exception Statistics",
                    "exception-count",
                    """
                                 CREATE VIEW "exception-count" AS
                                 SELECT
                                     LAST(throwables) - FIRST(throwables) AS "Exceptions Thrown"
                                 FROM ExceptionStatistics
                            """
            ),
            /**
             * [application.exception-by-type]
             * label ="Exceptions by Type"
             * table = "COLUMN 'Class', 'Count'
             *          SELECT thrownClass AS T, COUNT(thrownClass) AS C
             *          FROM JavaErrorThrow, JavaExceptionThrow GROUP BY T ORDER BY C DESC"
            */
            new View("exception-by-type",
                    "application",
                    "Exceptions by Type",
                    "exception-by-type",
                    """
                                 CREATE VIEW "exception-by-type" AS
                                 SELECT
                                     c.javaName AS "Class",
                                     COUNT(*) AS "Count"
                                 FROM (
                                     SELECT thrownClass FROM JavaErrorThrow
                                     UNION ALL
                                     SELECT thrownClass FROM JavaExceptionThrow
                                 ) AS combined
                                 JOIN Class c ON combined.thrownClass = c._id
                                 GROUP BY combined.thrownClass, c.javaName
                                    ORDER BY COUNT(*) DESC
                            """).addUnionAlternatives(),
            /**
             *
            [application.exception-by-message]
            label ="Exceptions by Message"
            table = "COLUMN 'Message', 'Count'
            SELECT message AS M, COUNT(message) AS C
            FROM JavaErrorThrow, JavaExceptionThrow GROUP BY M ORDER BY C DESC"
            */
            new View("exception-by-message",
                    "application",
                    "Exceptions by Message",
                    "exception-by-message",
                    """
                                 CREATE VIEW "exception-by-message" AS
                                 SELECT
                                     message AS "Message",
                                     COUNT(*) AS "Count"
                                 FROM (
                                     SELECT message FROM JavaErrorThrow
                                     UNION ALL
                                     SELECT message FROM JavaExceptionThrow
                                 ) AS combined
                                 GROUP BY message
                                 ORDER BY COUNT(*) DESC
                            """).addUnionAlternatives(),
            /**
             * [application.exception-by-site]
             * label ="Exceptions by Site"
             * table = "COLUMN 'Method', 'Count'
             *          SELECT stackTrace.notInit AS S, COUNT(startTime) AS C
             *          FROM JavaErrorThrow, JavaExceptionThrow GROUP BY S ORDER BY C DESC"
            */
            new View("exception-by-site",
                    "application",
                    "Exceptions by Site",
                    "exception-by-site",
                    """
                                 CREATE VIEW "exception-by-site" AS
                                 SELECT
                                     (c.javaName || '.' || combined.stackTrace$topNonInitMethod) AS "Method",
                                     COUNT(*) AS "Count"
                                 FROM (
                                     SELECT stackTrace$topNonInitClass, stackTrace$topNonInitMethod FROM JavaErrorThrow
                                     UNION ALL
                                     SELECT stackTrace$topNonInitClass, stackTrace$topNonInitMethod FROM JavaExceptionThrow
                                 ) AS combined
                                 JOIN Class c ON combined.stackTrace$topNonInitClass = c._id
                                 GROUP BY combined.stackTrace$topNonInitClass, combined.stackTrace$topNonInitMethod, c.javaName
                                 ORDER BY COUNT(*) DESC
                            """,
                    "JavaErrorThrow", "JavaExceptionThrow", "Class"
            ).addUnionAlternatives(),
            /**
             * [application.file-reads-by-path]
             * label = "File Reads by Path"
             * table = "COLUMN 'Path', 'Reads', 'Total Read'
             *          FORMAT cell-height:5, none, none
             *          SELECT path, COUNT(*), SUM(bytesRead) AS S FROM FileRead
             *          GROUP BY path ORDER BY S DESC"
            */
            new View("file-reads-by-path",
                    "application",
                    "File Reads by Path",
                    "file-reads-by-path",
                    """
                                 CREATE VIEW "file-reads-by-path" AS
                                 SELECT
                                     path AS "Path",
                                     COUNT(*) AS "Reads",
                                     format_memory(SUM(bytesRead)) AS "Total Read"
                                 FROM FileRead
                                 GROUP BY path
                                 ORDER BY SUM(bytesRead) DESC
                            """),
            /**
             * [application.file-writes-by-path]
             * label = "File Writes by Path"
             * table = "COLUMN 'Path', 'Writes', 'Total Written'
             *          FORMAT cell-height:5, none, none
             *          SELECT path, COUNT(bytesWritten), SUM(bytesWritten) AS S FROM FileWrite
             *          GROUP BY path ORDER BY S DESC"
            */
            new View("file-writes-by-path",
                    "application",
                    "File Writes by Path",
                    "file-writes-by-path",
                    """
                                 CREATE VIEW "file-writes-by-path" AS
                                 SELECT
                                     path AS "Path",
                                     COUNT(*) AS "Writes",
                                     format_memory(SUM(bytesWritten)) AS "Total Written"
                                 FROM FileWrite
                                 GROUP BY path
                                 ORDER BY SUM(bytesWritten) DESC
                            """),
            /**
             *
            [application.finalizers]
            label = "Finalizers"
            table = "SELECT finalizableClass, LAST_BATCH(objects) AS O, LAST_BATCH(totalFinalizersRun)
            FROM FinalizerStatistics GROUP BY finalizableClass ORDER BY O DESC"
            */
            new View("finalizers", // TODO: test
                    "application",
                    "Finalizers",
                    "finalizers",
                    """
                                 CREATE VIEW "finalizers" AS
                                 SELECT
                                     c.javaName AS "Finalizable Class",
                                     LAST(objects) AS "Objects",
                                     LAST(totalFinalizersRun) AS "Total Finalizers Run"
                                 FROM FinalizerStatistics fs
                                 JOIN Class c ON fs.finalizableClass = c._id
                                 GROUP BY fs.finalizableClass, c.javaName
                                 ORDER BY LAST(objects) DESC
                            """,
                    "FinalizerStatistics", "Class"
            ),
            /**
             * [jvm.gc]
             * label = "Garbage Collections"
             * table = "COLUMN 'Start', 'GC ID', 'GC Name', 'Heap Before GC', 'Heap After GC', 'Longest Pause'
             *          FORMAT none, none, missing:Unknown, none, none, none
             *          SELECT G.startTime, gcId, G.name,
             *                 B.heapUsed, A.heapUsed, longestPause
             *          FROM
             *                 GarbageCollection AS G,
             *                 GCHeapSummary AS B,
             *                 GCHeapSummary AS A
             *          WHERE B.when = 'Before GC' AND A.when = 'After GC'
             *          GROUP BY gcId ORDER BY gcId"
             */
            new View("gc",
                    "jvm",
                    "Garbage Collections",
                    "gc",
                    """
                            CREATE VIEW "gc" AS
                            SELECT
                                G.startTime                          AS "Start",
                                G.gcId                               AS "GC ID",
                                COALESCE(G.name, 'Unknown')          AS "GC Name",
                                format_memory(B.heapUsed)            AS "Heap Before GC",
                                format_memory(A.heapUsed)            AS "Heap After GC",
                                format_duration(G.longestPause)      AS "Longest Pause"
                            FROM GarbageCollection G
                            JOIN GCHeapSummary B ON G.gcId = B.gcId AND B.when = 'Before GC'
                            JOIN GCHeapSummary A ON G.gcId = A.gcId AND A.when = 'After GC'
                            ORDER BY G.gcId;
                            """
            ).addUnionAlternatives(),
            /**
             * [jvm.gc-concurrent-phases]
             * label = "Concurrent GC Phases"
             * table = "COLUMN 'Name', 'Average', 'P95',
             *                 'Longest', 'Count', 'Total'
             *          SELECT name,  AVG(duration),  P95(duration),
             *                 MAX(duration), COUNT(*), SUM(duration) AS S
             *          FROM   GCPhaseConcurrent, GCPhaseConcurrentLevel1
             *          GROUP BY name ORDER BY S"
             */
            new View("gc-concurrent-phases",
                    "jvm",
                    "Concurrent GC Phases",
                    "gc-concurrent-phases",
                    """
                                 CREATE VIEW "gc-concurrent-phases" AS
                                 SELECT
                                     name AS "Name",
                                     format_duration(AVG(duration)) AS "Average",
                                     format_duration(P95(duration)) AS "P95",
                                     format_duration(MAX(duration)) AS "Longest",
                                     COUNT(*) AS "Count",
                                     format_duration(SUM(duration)) AS "Total"
                                 FROM GCPhaseConcurrent
                                 GROUP BY name
                                 ORDER BY SUM(duration) DESC
                            """),
            /**
             * [jvm.gc-configuration]
             * label = 'GC Configuration'
             * form = "COLUMN 'Young GC', 'Old GC',
             *                'Parallel GC Threads','Concurrent GC Threads',
             *                'Dynamic GC Threads', 'Concurrent Explicit GC',
             *                'Disable Explicit GC', 'Pause Target',
             *                'GC Time Ratio'
             *         SELECT LAST(youngCollector), LAST(oldCollector),
             *                LAST(parallelGCThreads), LAST(concurrentGCThreads),
             *                LAST(usesDynamicGCThreads), LAST(isExplicitGCConcurrent),
             *                LAST(isExplicitGCDisabled), LAST(pauseTarget),
             *                LAST(gcTimeRatio)
             *         FROM   GCConfiguration"
             */
            new View("gc-configuration",
                    "jvm",
                    "GC Configuration",
                    "gc-configuration",
                    """
                                 CREATE VIEW "gc-configuration" AS
                                 SELECT
                                     LAST(youngCollector) AS "Young GC",
                                     LAST(oldCollector) AS "Old GC",
                                     LAST(parallelGCThreads) AS "Parallel GC Threads",
                                     LAST(concurrentGCThreads) AS "Concurrent GC Threads",
                                     LAST(usesDynamicGCThreads) AS "Dynamic GC Threads",
                                     LAST(isExplicitGCConcurrent) AS "Concurrent Explicit GC",
                                     LAST(isExplicitGCDisabled) AS "Disable Explicit GC",
                                     format_duration(LAST(pauseTarget)) AS "Pause Target",
                                     LAST(gcTimeRatio) AS "GC Time Ratio"
                                 FROM GCConfiguration
                            """),
            /**
             * [jvm.gc-references]
             * label = "GC References"
             * table = "COLUMN 'Time', 'GC ID', 'Soft Ref.', 'Weak Ref.', 'Phantom Ref.', 'Final Ref.', 'Total Count'
             *          SELECT G.startTime, G.gcId, S.count, W.count, P.count, F.count, SUM(G.count)
             *          FROM GCReferenceStatistics AS S,
             *               GCReferenceStatistics AS W,
             *               GCReferenceStatistics AS P,
             *               GCReferenceStatistics AS F,
             *               GCReferenceStatistics AS G
             *          WHERE S.type = 'Soft reference' AND
             *                W.type = 'Weak reference' AND
             *                P.type = 'Phantom reference' AND
             *                F.type = 'Final reference'
             *          GROUP BY gcId ORDER By G.gcId ASC"
            */
            new View("gc-references",
                    "jvm",
                    "GC References",
                    "gc-references",
                    """
                                 CREATE VIEW "gc-references" AS
                                 SELECT
                                     FIRST(G.startTime) AS "Time",
                                     G.gcId AS "GC ID",
                                     S.count AS "Soft Ref.",
                                     W.count AS "Weak Ref.",
                                     P.count AS "Phantom Ref.",
                                     F.count AS "Final Ref.",
                                     (S.count + W.count + P.count + F.count) AS "Total Count"
                                 FROM GCReferenceStatistics S
                                 JOIN GCReferenceStatistics W ON S.gcId = W.gcId
                                 JOIN GCReferenceStatistics P ON S.gcId = P.gcId
                                 JOIN GCReferenceStatistics F ON S.gcId = F.gcId
                                 JOIN GCReferenceStatistics G ON S.gcId = G.gcId
                                 WHERE S.type = 'Soft reference'
                                   AND W.type = 'Weak reference'
                                   AND P.type = 'Phantom reference'
                                   AND F.type = 'Final reference'
                                 GROUP BY G.gcId, S.count, W.count, P.count, F.count
                                 ORDER BY G.gcId ASC
                            """,
                    "GCReferenceStatistics"
            ),
            /**
             * [jvm.gc-pause-phases]
             * label = "GC Pause Phases"
             * table = "COLUMN 'Type', 'Name', 'Average',
             *                 'P95', 'Longest', 'Count', 'Total'
             *          SELECT eventType.label AS T, name,  AVG(duration),
             *                 P95(duration), MAX(duration), COUNT(*), SUM(duration) AS S
             *          FROM   GCPhasePause, GCPhasePauseLevel1, GCPhasePauseLevel2,
             *                 GCPhasePauseLevel3, GCPhasePauseLevel4 GROUP BY name
             *          ORDER BY T ASC, S"
             */
            new View("gc-pause-phases",
                    "jvm",
                    "GC Pause Phases",
                    "gc-pause-phases",
                    """
                            CREATE VIEW "gc-pause-phases" AS
                            SELECT
                                eventTypeLabel AS "Type",
                                name AS "Name",
                                format_duration(AVG(duration)) AS "Average",
                                format_duration(P95(duration)) AS "P95",
                                format_duration(MAX(duration)) AS "Longest",
                                COUNT(*) AS "Count",
                                format_duration(SUM(duration)) AS "Total"
                            FROM (
                                SELECT 'GC Phase Pause' as eventTypeLabel, name, duration FROM GCPhasePause
                                UNION ALL
                                SELECT 'GC Phase Pause Level 1' as eventTypeLabel, name, duration FROM GCPhasePauseLevel1
                                UNION ALL
                                SELECT 'GC Phase Pause Level 2' as eventTypeLabel, name, duration FROM GCPhasePauseLevel2
                                UNION ALL
                                SELECT 'GC Phase Pause Level 3' as eventTypeLabel, name, duration FROM GCPhasePauseLevel3
                                UNION ALL
                                SELECT 'GC Phase Pause Level 4' as eventTypeLabel, name, duration FROM GCPhasePauseLevel4
                            ) phases
                            GROUP BY eventTypeLabel, name
                            ORDER BY eventTypeLabel ASC, SUM(duration) DESC;
                    """,
                    "GCPhasePause", "GCPhasePauseLevel1", "GCPhasePauseLevel2", "GCPhasePauseLevel3", "GCPhasePauseLevel4", "EventType"
            ).addUnionAlternatives(),
            /**
             * [jvm.gc-pauses]
             * label = "GC Pauses"
             * form = "COLUMN 'Total Pause Time','Number of Pauses', 'Minimum Pause Time',
             *                'Median Pause Time', 'Average Pause Time', 'P90 Pause Time',
             *                'P95 Pause Time', 'P99 Pause Time', 'P99.9% Pause Time',
             *                'Maximum Pause Time'
             *         SELECT SUM(duration), COUNT(duration), MIN(duration),
             *                MEDIAN(duration), AVG(duration), P90(duration),
             *                P95(duration), P99(duration), P999(duration),
             *                MAX(duration)
             *         FROM GCPhasePause"
             */
            new View("gc-pauses",
                    "jvm",
                    "GC Pauses",
                    "gc-pauses",
                    """
                                 CREATE VIEW "gc-pauses" AS
                                 SELECT
                                     format_duration(SUM(duration)) AS "Total Pause Time",
                                     COUNT(duration) AS "Number of Pauses",
                                     format_duration(MIN(duration)) AS "Minimum Pause Time",
                                     format_duration(MEDIAN(duration)) AS "Median Pause Time",
                                     format_duration(AVG(duration)) AS "Average Pause Time",
                                     format_duration(P90(duration)) AS "P90 Pause Time",
                                     format_duration(P95(duration)) AS "P95 Pause Time",
                                     format_duration(P99(duration)) AS "P99 Pause Time",
                                     format_duration(P999(duration)) AS "P99.9% Pause Time",
                                     format_duration(MAX(duration)) AS "Maximum Pause Time"
                                 FROM GCPhasePause
                            """),
            /**
             * [jvm.gc-allocation-trigger]
             * label = "GC Allocation Trigger"
             * table = "COLUMN 'Trigger Method (Non-JDK)', 'Count', 'Total Requested'
             *          SELECT stackTrace.topApplicationFrame AS S, COUNT(*), SUM(size)
             *          FROM AllocationRequiringGC GROUP BY S"
             */
            new View("gc-allocation-trigger",
                    "jvm",
                    "GC Allocation Trigger",
                    "gc-allocation-trigger",
                    """
                                 CREATE VIEW "gc-allocation-trigger" AS
                                 SELECT
                                     (c.javaName || '.' || ar.stackTrace$topApplicationMethod) AS "Trigger Method (Non-JDK)",
                                     COUNT(*) AS "Count",
                                     format_memory(SUM(ar.size)) AS "Total Requested"
                                 FROM AllocationRequiringGC ar
                                 JOIN Class c ON ar.stackTrace$topApplicationClass = c._id
                                 GROUP BY ar.stackTrace$topApplicationMethod, ar.stackTrace$topApplicationClass, c.javaName
                                 ORDER BY COUNT(*) DESC, SUM(ar.size) DESC
                            """,
                    "AllocationRequiringGC", "Class"
            ),
            /**
             * [jvm.gc-cpu-time]
             * label = "GC CPU Time"
             * form = "COLUMN 'GC User Time', 'GC System Time',
             *                'GC Wall Clock Time', 'Total Time',
             *                'GC Count'
             *         SELECT SUM(userTime), SUM(systemTime),
             *                SUM(realTime), DIFF(startTime), COUNT(*)
             *         FROM GCCPUTime"
             */
            new View("gc-cpu-time",
                    "jvm",
                    "GC CPU Time",
                    "gc-cpu-time",
                    """
                                 CREATE VIEW "gc-cpu-time" AS
                                 SELECT
                                     format_duration(SUM(userTime)) AS "GC User Time",
                                     format_duration(SUM(systemTime)) AS "GC System Time",
                                     format_duration(SUM(realTime)) AS "GC Wall Clock Time",
                                     format_duration(epoch(MAX(startTime) - MIN(startTime))) AS "Total Time",
                                     COUNT(*) AS "GC Count"
                                 FROM GCCPUTime
                            """),
            /**
             * [jvm.heap-configuration]
             * label = "Heap Configuration"
             * form = "SELECT LAST(initialSize), LAST(minSize), LAST(maxSize),
             *                LAST(usesCompressedOops), LAST(compressedOopsMode)
             *                FROM GCHeapConfiguration"
             */
            new View("heap-configuration",
                    "jvm",
                    "Heap Configuration",
                    "heap-configuration",
                    """
                                 CREATE VIEW "heap-configuration" AS
                                 SELECT
                                     format_memory(LAST(initialSize)) AS "Initial Size",
                                     format_memory(LAST(minSize)) AS "Minimum Size",
                                     format_memory(LAST(maxSize)) AS "Maximum Size",
                                     LAST(usesCompressedOops) AS "If Compressed Oops Are Used",
                                     LAST(compressedOopsMode) AS "Compressed Oops Mode"
                                 FROM GCHeapConfiguration
                            """),
            /**
             * [application.hot-methods]
             * label = "Java Methods that Executes the Most"
             * table = "COLUMN 'Method', 'Samples', 'Percent'
             *          FORMAT none, none, normalized
             *          SELECT stackTrace.topFrame AS T, COUNT(*), COUNT(*)
             *          FROM ExecutionSample GROUP BY T LIMIT 25"
             */
            new View("hot-methods",
                    "application",
                    "Java Methods that Execute the Most",
                    "hot-methods",
                    """
                                 CREATE VIEW "hot-methods" AS
                                 SELECT
                                     (c.javaName || '.' || es.stackTrace$topMethod) AS "Method",
                                     COUNT(*) AS "Samples",
                                     format_percentage(COUNT(*) / (SELECT COUNT(*) FROM ExecutionSample)) AS "Percent"
                                 FROM ExecutionSample es
                                 JOIN Class c ON es.stackTrace$topClass = c._id
                                 GROUP BY es.stackTrace$topMethod, es.stackTrace$topClass, c.javaName
                                 ORDER BY COUNT(*) DESC
                                 LIMIT 25
                            """),
            /**
             * [environment.jvm-flags]
             * label = "Command Line Flags"
             * table = "SELECT name AS N, LAST(value)
             *          FROM IntFlag, UnsignedIntFlag, BooleanFlag,
             *          LongFlag, UnsignedLongFlag,
             *          DoubleFlag, StringFlag,
             *          IntFlagChanged, UnsignedIntFlagChanged, BooleanFlagChanged,
             *          LongFlagChanged, UnsignedLongFlagChanged,
             *          DoubleFlagChanged, StringFlagChanged
             *          GROUP BY name ORDER BY name ASC"
             */
            new View("jvm-flags",
                    "environment",
                    "Command Line Flags",
                    "jvm-flags",
                    """
                                 CREATE VIEW "jvm-flags" AS
                                 SELECT
                                     name AS "Name",
                                     value AS "Value"
                                 FROM (
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM IntFlag
                                     UNION ALL
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM UnsignedIntFlag
                                     UNION ALL
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM BooleanFlag
                                     UNION ALL
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM LongFlag
                                     UNION ALL
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM UnsignedLongFlag
                                     UNION ALL
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM DoubleFlag
                                     UNION ALL
                                     SELECT name, value FROM StringFlag
                                     UNION ALL
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM IntFlagChanged
                                     UNION ALL
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM UnsignedIntFlagChanged
                                     UNION ALL
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM BooleanFlagChanged
                                     UNION ALL
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM LongFlagChanged
                                     UNION ALL
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM UnsignedLongFlagChanged
                                     UNION ALL
                                     SELECT name, CAST(value AS VARCHAR) AS value FROM DoubleFlagChanged
                                     UNION ALL
                                     SELECT name, value FROM StringFlagChanged
                                 ) flags
                                 GROUP BY name, value
                                 ORDER BY name ASC
                            """
            ).addUnionAlternatives(),
            /**
             * [jvm.jvm-information]
             * label = "JVM Information"
             * form = "COLUMN
             *                'PID', 'VM Start', 'Name', 'Version',
             *                'VM Arguments', 'Program Arguments'
             *         SELECT LAST(pid), LAST(jvmStartTime), LAST(jvmName), LAST(jvmVersion),
             *                LAST(jvmArguments), LAST(javaArguments) FROM JVMInformation"
             */
            new View("jvm-information",
                    "jvm",
                    "JVM Information",
                    "jvm-information",
                    """
                                 CREATE VIEW "jvm-information" AS
                                 SELECT
                                     LAST(pid) AS "PID",
                                     LAST(jvmStartTime) AS "VM Start",
                                     LAST(jvmName) AS "Name",
                                     LAST(jvmVersion) AS "Version",
                                     LAST(jvmArguments) AS "VM Arguments",
                                     LAST(javaArguments) AS "Program Arguments"
                                 FROM JVMInformation
                            """),
            /**
             * [application.latencies-by-type]
             * label = "Latencies by Type"
             * table = "COLUMN 'Event Type', 'Count', 'Average', 'P 99', 'Longest', 'Total'
             *          SELECT eventType.label AS T, COUNT(*), AVG(duration),  P99(duration), MAX(duration), SUM(duration)
             *          FROM JavaMonitorWait, JavaMonitorEnter, ThreadPark, ThreadSleep,
             *          SocketRead, SocketWrite, FileWrite, FileRead GROUP BY T"
             */
            new View("latencies-by-type",
                    "application",
                    "Latencies by Type",
                    "latencies-by-type",
                    """
                                 CREATE VIEW "latencies-by-type" AS
                                 SELECT
                                     eventType AS "Event Type",
                                     COUNT(*) AS "Count",
                                     format_duration(AVG(duration)) AS "Average",
                                     format_duration(P99(duration)) AS "P 99",
                                     format_duration(MAX(duration)) AS "Longest",
                                     format_duration(SUM(duration)) AS "Total"
                                 FROM (
                                     SELECT 'Java Monitor Wait' AS eventType, duration FROM JavaMonitorWait
                                     UNION ALL
                                     SELECT 'Java Monitor Enter' AS eventType, duration FROM JavaMonitorEnter
                                     UNION ALL
                                     SELECT 'Thread Park' AS eventType, duration FROM ThreadPark
                                     UNION ALL
                                     SELECT 'Thread Sleep' AS eventType, duration FROM ThreadSleep
                                     UNION ALL
                                     SELECT 'Socket Read' AS eventType, duration FROM SocketRead
                                     UNION ALL
                                     SELECT 'Socket Write' AS eventType, duration FROM SocketWrite
                                     UNION ALL
                                     SELECT 'File Write' AS eventType, duration FROM FileWrite
                                     UNION ALL
                                     SELECT 'File Read' AS eventType, duration FROM FileRead
                                 ) latencies
                                 GROUP BY eventType
                                 ORDER BY SUM(duration) DESC
                            """).addUnionAlternatives(),
            /**
             * [application.memory-leaks-by-class]
             * label = "Memory Leak Candidates by Class"
             * table = "COLUMN 'Alloc. Time', 'Object Class', 'Object Age', 'Heap Usage'
             *          SELECT LAST_BATCH(allocationTime), LAST_BATCH(object.type), LAST_BATCH(objectAge),
             *          LAST_BATCH(lastKnownHeapUsage) FROM OldObjectSample GROUP BY object.type ORDER BY allocationTime"
             */
            new View("memory-leaks-by-class",
                    "application",
                    "Memory Leak Candidates by Class",
                    "memory-leaks-by-class",
                    """
                                 CREATE VIEW "memory-leaks-by-class" AS
                                 SELECT
                                     LAST(allocationTime) AS "Alloc. Time",
                                     c.javaName AS "Object Class",
                                     format_duration(LAST(objectAge)) AS "Object Age",
                                     format_memory(LAST(lastKnownHeapUsage)) AS "Heap Usage"
                                 FROM OldObjectSample os
                                 JOIN OldObject o ON os.object = o._id
                                 JOIN Class c ON o.type = c._id
                                 GROUP BY c.javaName
                                 ORDER BY LAST(allocationTime) ASC
                            """,
                    "OldObjectSample", "Class"),
            /**
             * [application.memory-leaks-by-site]
             * label = "Memory Leak Candidates by Site"
             * table = "COLUMN 'Alloc. Time', 'Application Method', 'Object Age', 'Heap Usage'
             *          SELECT LAST_BATCH(allocationTime), LAST_BATCH(stackTrace.topApplicationFrame), LAST_BATCH(objectAge),
             *          LAST_BATCH(lastKnownHeapUsage) FROM OldObjectSample GROUP BY stackTrace.topApplicationFrame ORDER BY allocationTime"
             */
            new View("memory-leaks-by-site",
                    "application",
                    "Memory Leak Candidates by Site",
                    "memory-leaks-by-site",
                    """
                                 CREATE VIEW "memory-leaks-by-site" AS
                                 SELECT
                                     LAST(allocationTime) AS "Alloc. Time",
                                     (c.javaName || '.' || os.stackTrace$topApplicationMethod) AS "Application Method",
                                     format_duration(LAST(objectAge)) AS "Object Age",
                                     format_memory(LAST(lastKnownHeapUsage)) AS "Heap Usage"
                                 FROM OldObjectSample os
                                 JOIN Class c ON os.stackTrace$topApplicationClass = c._id
                                 GROUP BY os.stackTrace$topApplicationMethod, os.stackTrace$topApplicationClass, c.javaName
                                 ORDER BY LAST(allocationTime) ASC
                            """,
                    "OldObjectSample", "Class"),
            /**
             * [application.modules]
             * label = "Modules"
             * table = "SELECT LAST(source.name) AS S FROM ModuleRequire GROUP BY source.name ORDER BY S"
             */
            new View("modules",
                    "application",
                    "Modules",
                    "modules",
                    """
                                 CREATE VIEW "modules" AS
                                 SELECT
                                     LAST(m.name) AS "Module Name"
                                 FROM ModuleRequire
                                 JOIN Module m ON ModuleRequire.source = m._id
                                 GROUP BY source
                                 ORDER BY "Module Name" ASC
                            """,
                    "ModuleRequire", "Module"
            ),
            /**
             * [application.monitor-inflation]
             * label = "Monitor Inflation"
             * table = "SELECT stackTrace, monitorClass, COUNT(*), SUM(duration) AS S
             *          FROM jdk.JavaMonitorInflate GROUP BY stackTrace, monitorClass ORDER BY S"
             */
            new View("monitor-inflation",
                    "application",
                    "Monitor Inflation",
                    "monitor-inflation",
                    """
                                 CREATE VIEW "monitor-inflation" AS
                                 SELECT
                                     (c.javaName || '.' || jmi.stackTrace$topMethod) AS "Method",
                                     mc.javaName AS "Monitor Class",
                                     COUNT(*) AS "Count",
                                     format_duration(SUM(jmi.duration)) AS "Total Duration"
                                 FROM JavaMonitorInflate jmi
                                 JOIN Class c ON jmi.stackTrace$topClass = c._id
                                 JOIN Class mc ON jmi.monitorClass = mc._id
                                 GROUP BY jmi.stackTrace$topMethod, c.javaName, mc.javaName
                                 ORDER BY SUM(jmi.duration) DESC
                            """,
                    "JavaMonitorInflate", "Class"
            ),
            /**
             * [environment.native-libraries]
             * label = "Native Libraries"
             * table = "FORMAT cell-height:2, none, none
             *          SELECT name AS N, baseAddress, topAddress FROM NativeLibrary GROUP BY name ORDER BY N"
             */
            new View("native-libraries",
                    "environment",
                    "Native Libraries",
                    "native-libraries",
                    """
                                 CREATE VIEW "native-libraries" AS
                                 SELECT
                                     name AS "Name",
                                     format_hex(baseAddress) AS "Base Address",
                                     format_hex(topAddress) AS "Top Address"
                                 FROM NativeLibrary
                                 GROUP BY name, baseAddress, topAddress
                                 ORDER BY name ASC
                            """,
                    "NativeLibrary"
            ),
            /**
             * [jvm.native-memory-committed]
             * label = "Native Memory Committed"
             * table = "COLUMN 'Memory Type', 'First Observed', 'Average', 'Last Observed', 'Maximum'
             *          SELECT type, FIRST(committed), AVG(committed), LAST(committed), MAX(committed) AS M
             *          FROM NativeMemoryUsage GROUP BY type ORDER BY M DESC"
             */
            new View("native-memory-committed", // TODO test
                    "jvm",
                    "Native Memory Committed",
                    "native-memory-committed",
                    """
                                 CREATE VIEW "native-memory-committed" AS
                                 SELECT
                                     type AS "Memory Type",
                                     FIRST(committed) AS "First Observed",
                                     format_memory(AVG(committed)) AS "Average",
                                     LAST(committed) AS "Last Observed",
                                     format_memory(MAX(committed)) AS "Maximum"
                                 FROM NativeMemoryUsage
                                 GROUP BY type
                                 ORDER BY MAX(committed) DESC
                            """
            ),
            /**
             * [jvm.native-memory-reserved]
             * label = "Native Memory Reserved"
             * table = "COLUMN 'Memory Type', 'First Observed', 'Average', 'Last Observed', 'Maximum'
             *          SELECT type, FIRST(reserved), AVG(reserved), LAST(reserved), MAX(reserved) AS M
             *          FROM NativeMemoryUsage GROUP BY type ORDER BY M DESC"
             */
            new View("native-memory-reserved", // TODO test
                    "jvm",
                    "Native Memory Reserved",
                    "native-memory-reserved",
                    """
                                 CREATE VIEW "native-memory-reserved" AS
                                 SELECT
                                     type AS "Memory Type",
                                     FIRST(reserved) AS "First Observed",
                                     format_memory(AVG(reserved)) AS "Average",
                                     LAST(reserved) AS "Last Observed",
                                     format_memory(MAX(reserved)) AS "Maximum"
                                 FROM NativeMemoryUsage
                                 GROUP BY type
                                 ORDER BY MAX(reserved) DESC
                            """),
            /**
             * [application.native-methods]
             * label = "Waiting or Executing Native Methods"
             * table = "COLUMN 'Method', 'Samples', 'Percent'
             *          FORMAT none, none, normalized
             *          SELECT stackTrace.topFrame AS T, COUNT(*), COUNT(*)
             *          FROM NativeMethodSample GROUP BY T"
             */
            new View("native-methods",
                    "application",
                    "Waiting or Executing Native Methods",
                    "native-methods",
                    """
                                 CREATE VIEW "native-methods" AS
                                 SELECT
                                     (c.javaName || '.' || nms.stackTrace$topMethod) AS "Method",
                                     COUNT(*) AS "Samples",
                                     format_percentage(COUNT(*) / (SELECT COUNT(*) FROM NativeMethodSample)) AS "Percent"
                                 FROM NativeMethodSample nms
                                 JOIN Class c ON nms.stackTrace$topClass = c._id
                                 GROUP BY nms.stackTrace$topMethod, nms.stackTrace$topClass, c.javaName
                                 ORDER BY COUNT(*) DESC
                            """,
                    "NativeMethodSample", "Class"),
            /**
             * [environment.network-utilization]
             * label = "Network Utilization"
             * table = "SELECT networkInterface, AVG(readRate), MAX(readRate), AVG(writeRate), MAX(writeRate)
             *          FROM NetworkUtilization GROUP BY networkInterface"
             */
            new View("network-utilization",
                    "environment",
                    "Network Utilization",
                    "network-utilization",
                    """
                                 CREATE VIEW "network-utilization" AS
                                 SELECT
                                     networkInterface AS "Network Interface",
                                     format_memory(AVG(readRate) / 8) || '/s' AS "Avg. Read Rate",
                                     format_memory(MAX(readRate) / 8) || '/s' AS "Max. Read Rate",
                                     format_memory(AVG(writeRate) / 8) || '/s' AS "Avg. Write Rate",
                                     format_memory(MAX(writeRate) / 8) || '/s' AS "Max. Write Rate"
                                 FROM NetworkUtilization
                                 GROUP BY networkInterface
                                 ORDER BY networkInterface ASC
                            """),
            /**
             * [application.object-statistics]
             * label = "Objects Occupying More than 1%"
             * table = "COLUMN 'Class', 'Count', 'Heap Space', 'Increase'
             *          SELECT
             *           LAST_BATCH(objectClass), LAST_BATCH(count),
             *           LAST_BATCH(totalSize), DIFF(totalSize)
             *          FROM ObjectCountAfterGC, ObjectCount
             *          GROUP BY objectClass
             *          ORDER BY totalSize DESC"
             */
            new View("object-statistics",
                    "application",
                    "Objects Occupying More than 1%",
                    "object-statistics",
                    """
                            CREATE VIEW "object-statistics" AS
                            SELECT "Class", "Count", "Heap Space", "Increase"
                            FROM
                            (SELECT
                                c.javaName AS "Class",
                                LAST(count) AS "Count",
                                format_memory(LAST(totalSize)) AS "Heap Space",
                                LAST(totalSize) as h,
                                format_memory(MAX(totalSize) - MIN(totalSize)) AS "Increase"
                            FROM (
                                SELECT objectClass, count, totalSize FROM ObjectCountAfterGC
                                UNION ALL
                                SELECT objectClass, count, totalSize FROM ObjectCount
                            ) ocg
                            JOIN Class c ON ocg.objectClass = c._id
                            GROUP BY c.javaName)
                            ORDER BY h DESC
                            """,
                    "ObjectCountAfterGC", "ObjectCount", "Class"
            ).addUnionAlternatives(),
            /**
             * [application.pinned-threads]
             * label = "Pinned Virtual Threads"
             * table = "COLUMN 'Method', 'Pinned Count',  'Longest Pinning', 'Total Time Pinned'
             *          SELECT stackTrace.topApplicationFrame AS S, COUNT(*),
             *                 MAX(duration), SUM(duration) AS T FROM VirtualThreadPinned
             *          GROUP BY S
             *          ORDER BY T DESC"
             */
            new View("pinned-threads", // TODO test
                    "application",
                    "Pinned Virtual Threads",
                    "pinned-threads",
                    """
                            CREATE VIEW "pinned-threads" AS
                            SELECT
                                (c.javaName || '.' || vtp.stackTrace$topApplicationMethod) AS "Method",
                                COUNT(*) AS "Pinned Count",
                                format_duration(MAX(vtp.duration)) AS "Longest Pinning",
                                format_duration(SUM(vtp.duration)) AS "Total Time Pinned"
                            FROM VirtualThreadPinned vtp
                            JOIN Class c ON vtp.stackTrace$topApplicationClass = c._id
                            GROUP BY vtp.stackTrace$topApplicationMethod, vtp.stackTrace$topApplicationClass, c.javaName
                            ORDER BY SUM(vtp.duration) DESC
                            """),
            /**
             * [application.thread-count]
             * label ="Java Thread Statistics"
             * table = "SELECT * FROM JavaThreadStatistics"
             */
            new View("thread-count",
                    "application",
                    "Java Thread Statistics",
                    "thread-count",
                    """
                            CREATE VIEW "thread-count" AS
                            SELECT
                            startTime AS "Start Time",
                            activeCount AS "Active Threads",
                            daemonCount AS "Daemon Threads",
                            accumulatedCount AS "Accumulated Threads",
                            peakCount AS "Peak Threads"
                            FROM JavaThreadStatistics
                            ORDER BY startTime ASC
                            """),
            /**
             * [environment.recording]
             * label = "Recording Information"
             * form = "COLUMN 'Event Count', 'First Recorded Event', 'Last Recorded Event',
             *                  'Length of Recorded Events', 'Dump Reason'
             *         SELECT   COUNT(startTime), FIRST(startTime), LAST(startTime),
             *                  DIFF(startTime), LAST(jdk.Shutdown.reason)
             *         FROM *
             *
             *                    new Table.Column("eventCount", "INTEGER", null),
             *                     new Table.Column("firstEvent", "TIMESTAMP", null),
             *                     new Table.Column("lastEvent", "TIMESTAMP", null),
             *                     new Table.Column("eventDurationSeconds", "DOUBLE", null),
             *                     new Table.Column("dumpReason", "VARCHAR", null)
             */
            new View("recording",
                    "environment",
                    "Recording Information",
                    "recording",
                    """
                            CREATE VIEW "recording" AS
                            SELECT
                                eventCount AS "Event Count",
                                firstEvent AS "First Recorded Event",
                                lastEvent AS "Last Recorded Event",
                                format_duration(eventDurationSeconds) AS "Length of Recorded Events",
                                dumpReason AS "Dump Reason"
                            FROM RecordingInfo
                            """),
            /**
             * [jvm.safepoints]
             * label = "Safepoints"
             * table = "COLUMN  'Start Time', 'Duration',
             *                    'State Syncronization', 'Cleanup',
             *                    'JNI Critical Threads', 'Total Threads'
             *          SELECT    B.startTime,  DIFF([B|E].startTime),
             *                    S.duration, C.duration,
             *                    jniCriticalThreadCount, totalThreadCount
             *          FROM SafepointBegin AS B, SafepointEnd AS E,
             *               SafepointCleanup AS C, SafepointStateSynchronization AS S
             *          GROUP BY safepointId ORDER BY B.startTime"
             */
            new View("safepoints", // TODO test
                    "jvm",
                    "Safepoints",
                    "safepoints",
                    """
                            CREATE VIEW "safepoints" AS
                            SELECT
                                B.startTime AS "Start Time",
                                format_duration(epoch(E.startTime - B.startTime)) AS "Duration",
                                format_duration(S.duration) AS "State Synchronization",
                                format_duration(C.duration) AS "Cleanup",
                                jniCriticalThreadCount AS "JNI Critical Threads",
                                totalThreadCount AS "Total Threads"
                            FROM SafepointBegin B
                            JOIN SafepointEnd E ON B.safepointId = E.safepointId
                            LEFT JOIN SafepointStateSynchronization S ON B.safepointId = S.safepointId
                            LEFT JOIN SafepointCleanup C ON B.safepointId = C.safepointId
                            ORDER BY B.startTime ASC
                            """),
            /**
             * [jvm.longest-compilations]
             * label = "Longest Compilations"
             * table = "SELECT startTime, duration AS D, method, compileLevel, succeded
             *          FROM Compilation ORDER BY D LIMIT 25"
             */
            new View("longest-compilations",
                    "jvm",
                    "Longest Compilations",
                    "longest-compilations",
                    """
                            CREATE VIEW "longest-compilations" AS
                            SELECT
                                startTime AS "Start Time",
                                format_duration(duration) AS "Duration",
                                (c.javaName || '.' || m.name) AS "Method",
                                compileLevel AS "Compile Level",
                                Compilation.succeded AS "Succeeded"
                            FROM Compilation
                            JOIN Method m ON Compilation.method = m._id
                            JOIN Class c ON m.type = c._id
                            ORDER BY duration DESC
                            LIMIT 25
                            """),
            /**
             * [application.longest-class-loading]
             * label = "Longest Class Loading"
             * table = "COLUMN 'Time', 'Loaded Class', 'Load Time'
             *          SELECT startTime,loadedClass, duration AS D
             *          FROM ClassLoad ORDER BY D DESC LIMIT 25"
             */
            new View("longest-class-loading",
                    "application",
                    "Longest Class Loading",
                    "longest-class-loading",
                    """
                            CREATE VIEW "longest-class-loading" AS
                            SELECT
                                startTime AS "Time",
                                c.javaName AS "Loaded Class",
                                format_duration(duration) AS "Load Time"
                            FROM ClassLoad cl
                            JOIN Class c ON cl.loadedClass = c._id
                            ORDER BY duration DESC
                            LIMIT 25
                            """),
            /**
             * [environment.system-properties]
             * label = "System Properties at Startup"
             * table = "FORMAT none, cell-height:25
             *         SELECT key AS K, value FROM InitialSystemProperty GROUP BY key ORDER by K"
             */
            new View("system-properties",
                    "environment",
                    "System Properties at Startup",
                    "system-properties",
                    """
                            CREATE VIEW "system-properties" AS
                            SELECT
                                key AS "Key",
                                value AS "Value"
                            FROM InitialSystemProperty
                            GROUP BY key, value
                            ORDER BY key ASC
                            """),
            /**
             * [application.socket-writes-by-host]
             * label = "Socket Writes by Host"
             * table = "COLUMN 'Host', 'Writes', 'Total Written'
             *          FORMAT cell-height:2, none, none
             *          SELECT host, COUNT(*), SUM(bytesWritten) AS S FROM SocketWrite
             *          GROUP BY host ORDER BY S DESC"
             */
            new View("socket-writes-by-host", // TODO test
                    "application",
                    "Socket Writes by Host",
                    "socket-writes-by-host",
                    """
                            CREATE VIEW "socket-writes-by-host" AS
                            SELECT
                                host AS "Host",
                                COUNT(*) AS "Writes",
                                format_memory(SUM(bytesWritten)) AS "Total Written"
                            FROM SocketWrite
                            GROUP BY host
                            ORDER BY SUM(bytesWritten) DESC
                            """),
            /**
             * [application.socket-reads-by-host]
             * label = "Socket Reads by Host"
             * table = "COLUMN 'Host', 'Reads', 'Total Read'
             *          FORMAT cell-height:2, none, none
             *          SELECT host, COUNT(*), SUM(bytesRead) AS S FROM SocketRead
             *          GROUP BY host ORDER BY S DESC"
             */
            new View("socket-reads-by-host", // TODO test
                    "application",
                    "Socket Reads by Host",
                    "socket-reads-by-host",
                    """
                            CREATE VIEW "socket-reads-by-host" AS
                            SELECT
                                host AS "Host",
                                COUNT(*) AS "Reads",
                                format_memory(SUM(bytesRead)) AS "Total Read"
                            FROM SocketRead
                            GROUP BY host
                            ORDER BY SUM(bytesRead) DESC
                            """),
            /**
             * [environment.system-information]
             * label = "System Information"
             * form = "COLUMN 'Total Physical Memory Size', 'OS Version', 'CPU Type',
             *                  'Number of Cores', 'Number of Hardware Threads',
             *                  'Number of Sockets', 'CPU Description'
             *         SELECT LAST(totalSize), LAST(osVersion), LAST(cpu),
             *                LAST(cores), LAST(hwThreads),
             *                LAST(sockets), LAST(description)
             *         FROM CPUInformation, PhysicalMemory, OSInformation"
             */
            new View("system-information",
                    "environment",
                    "System Information",
                    "system-information",
                    """
                            CREATE VIEW "system-information" AS
                            SELECT
                                format_memory(LAST(pm.totalSize)) AS "Total Physical Memory Size",
                                LAST(osi.osVersion) AS "OS Version",
                                LAST(cii.cpu) AS "CPU Type",
                                LAST(cii.cores) AS "Number of Cores",
                                LAST(cii.hwThreads) AS "Number of Hardware Threads",
                                LAST(cii.sockets) AS "Number of Sockets",
                                LAST(cii.description) AS "CPU Description"
                            FROM PhysicalMemory pm, OSInformation osi, CPUInformation cii
                            """),
            /**
             * [environment.system-processes]
             * label = "System Processes"
             * table = "COLUMN 'First Observed', 'Last Observed', 'PID', 'Command Line'
             *          SELECT FIRST(startTime), LAST(startTime),
             *                 FIRST(pid), FIRST(commandLine)
             *          FROM SystemProcess GROUP BY pid"
             */
            new View("system-processes",
                    "environment",
                    "System Processes",
                    "system-processes",
                    """
                            CREATE VIEW "system-processes" AS
                            SELECT
                                FIRST(startTime) AS "First Observed",
                                LAST(startTime) AS "Last Observed",
                                pid AS "PID",
                                FIRST(commandLine) AS "Command Line"
                            FROM SystemProcess
                            GROUP BY pid
                            ORDER BY FIRST(startTime) ASC
                            """),
            /**
             * [jvm.tlabs]
             * label = "Thread Local Allocation Buffers"
             * form = "COLUMN 'Inside TLAB Count', 'Inside TLAB Minimum Size', 'Inside TLAB Average Size',
             *                'Inside TLAB Maximum Size', 'Inside TLAB Total Allocation',
             *                'Outside TLAB Count',  'OutSide TLAB Minimum Size', 'Outside TLAB Average Size',
             *                'Outside TLAB Maximum Size', 'Outside TLAB Total Allocation'
             *         SELECT  COUNT(I.tlabSize), MIN(I.tlabSize), AVG(I.tlabSize),
             *                 MAX(I.tlabSize), SUM(I.tlabSize),
             *                 COUNT(O.allocationSize), MIN(O.allocationSize), AVG(O.allocationSize),
             *                 MAX(O.allocationSize), SUM(O.allocationSize)
             *         FROM ObjectAllocationInNewTLAB AS I, ObjectAllocationOutsideTLAB AS O"
             */
            new View("tlabs",
                    "jvm",
                    "Thread Local Allocation Buffers",
                    "tlabs",
                    """
                            CREATE VIEW "tlabs" AS
                            SELECT * FROM (SELECT
                                COUNT(tlabSize) AS "Inside Count",
                                format_memory(MIN(tlabSize)) AS "Inside Minimum Size",
                                format_memory(AVG(tlabSize)) AS "Inside Average Size",
                                format_memory(MAX(tlabSize)) AS "Inside Maximum Size",
                                format_memory(SUM(tlabSize)) AS "Inside Total Allocation"
                            FROM ObjectAllocationInNewTLAB),
                            (SELECT
                                COUNT(allocationSize) AS "Outside Count",
                                format_memory(MIN(allocationSize)) AS "Outside Minimum Size",
                                format_memory(AVG(allocationSize)) AS "Outside Average Size",
                                format_memory(MAX(allocationSize)) AS "Outside Maximum Size",
                                format_memory(SUM(allocationSize)) AS "Outside Total Allocation"
                            FROM ObjectAllocationOutsideTLAB)
                            """),
            /**
             * [application.thread-allocation]
             * label = "Thread Allocation Statistics"
             * table = "COLUMN 'Thread', 'Allocated', 'Percentage'
             *          FORMAT none, none, normalized
             *          SELECT thread, LAST(allocated), LAST(allocated) AS A FROM ThreadAllocationStatistics
             *          GROUP BY thread ORDER BY A DESC"
             */
            new View("thread-allocation", // TODO test
                    "application",
                    "Thread Allocation Statistics",
                    "thread-allocation",
                    """
                            CREATE VIEW thread_allocation_statistics AS
                            SELECT
                                thread AS "Thread",
                                LAST(allocated) AS "Allocated",
                                format_percentage(
                                    LAST(allocated) * 1.0 / SUM(LAST(allocated)) OVER ()
                                ) AS "Percentage"
                            FROM ThreadAllocationStatistics
                            GROUP BY thread
                            ORDER BY "Allocated" DESC
                            """),
            /**
             * [application.thread-cpu-load]
             * label = "Thread CPU Load"
             * table = "COLUMN 'Thread', 'System', 'User'
             *          SELECT eventThread AS E, LAST(system), LAST(user) AS U
             *          FROM ThreadCPULoad GROUP BY E ORDER BY U DESC"
             */
            new View("thread-cpu-load",
                    "application",
                    "Thread CPU Load",
                    "thread-cpu-load",
                    """
                            CREATE VIEW "thread-cpu-load" AS
                            SELECT
                                t.javaName AS "Thread",
                                format_percentage(LAST(system)) AS "System",
                                format_percentage(LAST(user)) AS "User"
                            FROM ThreadCPULoad
                            JOIN Thread t ON ThreadCPULoad.eventThread = t._id
                            GROUP BY t.javaName
                            ORDER BY LAST(user) DESC, LAST(system) DESC
                            """),
            /**
             * [application.thread-start]
             * label = "Platform Thread Start by Method"
             * table = "COLUMN 'Start Time','Stack Trace', 'Thread', 'Duration'
             *          SELECT S.startTime, S.stackTrace, eventThread, DIFF(startTime) AS D
             *          FROM ThreadStart AS S, ThreadEnd AS E GROUP
             *          by eventThread ORDER BY D DESC"
             */
            new View("thread-start",
                    "application",
                    "Platform Thread Start by Method",
                    "thread-start",
                    """
                            CREATE VIEW "thread-start" AS
                            SELECT
                                CASE
                                    WHEN j.ts_start IS NOT NULL THEN j.ts_start
                                    ELSE NULL
                                END AS "Start Time",
                                CASE
                                    WHEN c.javaName IS NULL THEN j.stackTrace$topMethod
                                    ELSE (c.javaName || '.' || j.stackTrace$topMethod)
                                END AS "Stack Trace",
                                t.javaName AS "Thread",
                                CASE
                                    WHEN j.ts_start IS NULL THEN 'unknown'        -- only End, can't compute
                                    WHEN j.te_start IS NULL THEN 'infinity'      -- no End -> infinite
                                    ELSE format_duration(epoch(j.te_start - j.ts_start))
                                END AS "Duration"
                            FROM Thread t
                            JOIN (
                                SELECT
                                    COALESCE(ts.eventThread, te.eventThread) AS eventThread,
                                    ts.startTime AS ts_start,
                                    te.startTime AS te_start,
                                    ts.stackTrace$topMethod,
                                    ts.stackTrace$topClass
                                FROM ThreadStart ts
                                FULL OUTER JOIN ThreadEnd te
                                  ON ts.eventThread = te.eventThread
                            ) j ON j.eventThread = t._id
                            LEFT JOIN Class c ON j.stackTrace$topClass = c._id
                            WHERE t.javaName IS NOT NULL
                            QUALIFY ROW_NUMBER() OVER (
                                PARTITION BY j.eventThread
                                ORDER BY
                                    CASE
                                        WHEN j.ts_start IS NOT NULL AND j.te_start IS NOT NULL
                                             AND j.te_start >= j.ts_start THEN 0  -- prefer valid duration
                                        WHEN j.ts_start IS NOT NULL AND j.te_start IS NULL THEN 1 -- infinity
                                        WHEN j.ts_start IS NULL AND j.te_start IS NOT NULL THEN 2 -- only end
                                        ELSE 3
                                    END
                            ) = 1
                            ORDER BY
                                CASE
                                    WHEN j.te_start IS NULL AND j.ts_start IS NOT NULL THEN 0 -- infinity first
                                    ELSE 1
                                END,
                                (j.te_start - j.ts_start) DESC NULLS LAST, j.ts_start ASC;
                            """),
            /**
             * [jvm.vm-operations]
             * label = "VM Operations"
             * table = "COLUMN 'VM Operation', 'Average Duration', 'Longest Duration', 'Count' , 'Total Duration'
             *          SELECT operation,  AVG(duration), MAX(duration), COUNT(*), SUM(duration)
             *          FROM jdk.ExecuteVMOperation GROUP BY operation"
             */
            new View("vm-operations",
                    "jvm",
                    "VM Operations",
                    "vm-operations",
                    """
                            CREATE VIEW "vm-operations" AS
                            SELECT
                                operation AS "VM Operation",
                                format_duration(AVG(duration)) AS "Average Duration",
                                format_duration(MAX(duration)) AS "Longest Duration",
                                COUNT(*) AS "Count",
                                format_duration(SUM(duration)) AS "Total Duration"
                            FROM ExecuteVMOperation
                            GROUP BY operation
                            ORDER BY SUM(duration) DESC
                            """),

    };

    public static List<View> getViews() {
        return List.of(views);
    }

    public static void addToDatabase(DuckDBConnection connection) throws SQLException {
        Set<String> existingTables = getTableNames(connection);
        // Remove existing views
        try (ResultSet rs = connection.createStatement().executeQuery("SELECT view_name FROM duckdb_views;")) {
            while (rs.next()) {
                String viewName = rs.getString(1);
                connection.createStatement().execute("DROP VIEW IF EXISTS \"" + viewName + "\"");
            }
        }
        for (View view : views) {
            if (!view.isValid(existingTables)) {
                String alternative = view.getBestMatchingQuery(existingTables);
                if (alternative != null) {
                    try {
                        connection.createStatement().execute(alternative);
                    } catch (SQLException e) {
                        throw new RuntimeSQLException(alternative, e);
                    }
                } else {
                    System.err.println("Skipping view " + view.name() + " because it references missing tables: " +
                                       getReferencedTables(view.definition()).stream()
                                               .filter(t -> !existingTables.contains(t))
                                               .collect(Collectors.joining(", ")));
                }
            } else {
                try {
                    connection.createStatement().execute(view.definition());
                } catch (SQLException e) {
                    throw new RuntimeSQLException("Error creating view " + view.name(), e);
                }
            }
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